package cli

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/urfave/cli/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/moostackhq/moostack/internal/appfile"
	"github.com/moostackhq/moostack/internal/buildx"
	"github.com/moostackhq/moostack/internal/cluster"
	"github.com/moostackhq/moostack/internal/interrupt"
	"github.com/moostackhq/moostack/internal/orchestrate"
)

// deployCommand is mooctl deploy.
func deployCommand(env Env) *cli.Command {
	return &cli.Command{
		Name:      "deploy",
		Usage:     "deploy the app whose app.yml is in dir, the current directory if none is given, to a node of the cluster, or remove it with --remove",
		ArgsUsage: "[dir]",
		Description: "The app runs on the node that runs it already, or else on the node that runs the fewest apps. " +
			"The node pulls the app's image itself, and deploy checks a few seconds after the app's container started that it still runs. " +
			"An app file with build in place of image names the build context, relative to the app's directory, with a Dockerfile at its root: " +
			"deploy builds it first with docker buildx, which must be on Docker's containerd image store, for each architecture the cluster's nodes have, " +
			"before it contacts a node, and the node imports the build for its architecture. " +
			"Builds are reproducible: SOURCE_DATE_EPOCH is 0 for the build, so the same source builds the same image, its files dated 1970, on any machine. " +
			"Deploying an app that runs already replaces it, unless it runs the same build of its image, which changes nothing. " +
			"--remove removes the app from every node that runs it, with its image unless another container runs from it, and its log; it needs only the app file's name. " +
			"Where an app runs is kept on the nodes alone, so deploy doesn't change the cluster directory.",
		Flags: []cli.Flag{forceFlag, &cli.BoolFlag{Name: removeFlag, Usage: "remove the app from every node that runs it, rather than deploy it"}},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			if cmd.NArg() > 1 {
				return fmt.Errorf("deploy takes the app's directory, or nothing for the current one, not %d arguments; see --help", cmd.NArg())
			}
			// An empty one, as an unset variable gives, names no directory,
			// rather than the current one.
			if cmd.NArg() == 1 && cmd.Args().First() == "" {
				return errors.New("deploy was given an empty directory; give the app's directory, or nothing for the current one; nothing was contacted")
			}
			if cmd.Bool(removeFlag) {
				return withHint(removeApp(ctx, env, cmd))
			}
			return withHint(deploy(ctx, env, cmd))
		},
	}
}

// removeFlag is deploy's flag that removes the app instead, which a
// removal takes the lock with as part of its operation's name.
const removeFlag = "remove"

// removeApp is mooctl deploy --remove. It reads the app file for the
// app's name alone, before it contacts a node.
func removeApp(ctx context.Context, env Env, cmd *cli.Command) error {
	app, err := appfile.ReadName(cmp.Or(cmd.Args().First(), "."))
	if err != nil {
		return fmt.Errorf("%w; nothing was contacted", err)
	}
	d, err := openCluster(cmd)
	if err != nil {
		return err
	}
	// A signal ends the preflight or the wait for a node's removal, and
	// the removal gives the lock back, rather than ending mooctl.
	ctx, stop := interrupt.Context(ctx, env.Exit)
	defer stop()
	c, err := preflight(ctx, env, cmd, d, "", nil)
	if err != nil {
		return err
	}
	defer c.Close()
	removed, err := c.RemoveApp(c.Context(), app, func(node string) {
		fmt.Fprintf(env.Stdout, "Removed %s from %s\n", app, node)
	})
	if err != nil {
		return removeError(app, removed, err)
	}
	if len(removed) == 0 {
		fmt.Fprintf(env.Stdout, "%s isn't deployed; nothing changed\n", app)
	}
	return nil
}

// removeError is err, why the removal of the app called app failed once
// it was removed from the nodes removed, as the operator reads it: what
// failed, and how to finish the removal.
func removeError(app string, removed []string, err error) error {
	// What running the removal again does: say whether the app is
	// deployed, of a removal that may have happened, and remove it from
	// the nodes after, of one that stopped partway.
	says := fmt.Sprintf("Run mooctl deploy --remove again: it says whether %s is deployed", app)
	finishes := fmt.Sprintf("Run mooctl deploy --remove again: it removes %s from every node it is still on", app)
	var re *orchestrate.RemoveError
	switch {
	case errors.As(err, &re) && re.Stopped:
		return fmt.Errorf("mooctl stopped waiting for %s to remove %s, which may go on on the node: %v\n%s", re.Node, app, re.Err, says)
	// Unavailable comes from a node that crashed, or whose connection was
	// cut, once it had the call, which may have removed the app all the
	// same; from a call that was never sent; and from the agent itself
	// while containerd doesn't answer, saying what it changed.
	case errors.As(err, &re) && status.Code(re.Err) == codes.Unavailable:
		return fmt.Errorf("mooctl can't tell whether %s removed %s: %s\n%s", re.Node, app, status.Convert(re.Err).Message(), says)
	case errors.As(err, &re):
		return fmt.Errorf("%s failed to remove %s: %s\n%s", re.Node, app, status.Convert(re.Err).Message(), finishes)
	// One stopped, or that lost the lock, between two nodes leaves the app
	// on those after.
	case len(removed) > 0:
		return fmt.Errorf("%w\n%s", err, finishes)
	}
	return err
}

// deploy is mooctl deploy. Everything it checks of the app file it
// checks before it contacts a node.
func deploy(ctx context.Context, env Env, cmd *cli.Command) error {
	app, err := appfile.Read(cmp.Or(cmd.Args().First(), "."))
	if err != nil {
		return fmt.Errorf("%w; nothing was contacted", err)
	}
	d, err := openCluster(cmd)
	if err != nil {
		return err
	}
	target := orchestrate.App{Name: app.Name, Image: app.Image}
	exit := env.Exit
	if app.Build != "" {
		// The builds' archives are kept until the deploy ends, however it
		// ends, a second signal included.
		dir, err := os.MkdirTemp("", "mooctl-build-")
		if err != nil {
			return err
		}
		defer os.RemoveAll(dir)
		exit = func(code int) {
			os.RemoveAll(dir)
			env.Exit(code)
		}
		target, err = buildApp(ctx, env, exit, d, app, dir)
		if err != nil {
			return err
		}
	}
	// A signal ends the preflight, the pull or the wait for the app, and
	// the deploy gives the lock back, rather than ending mooctl.
	ctx, stop := interrupt.Context(ctx, exit)
	defer stop()
	c, err := preflight(ctx, env, cmd, d, "", nil)
	if err != nil {
		return err
	}
	defer c.Close()
	deployed, err := c.Deploy(c.Context(), target, func(s orchestrate.DeployStep) {
		switch {
		case s.Fetching && target.Archive != nil:
			fmt.Fprintf(env.Stdout, "Importing %s on %s\n", target.Image, s.Node)
		case s.Fetching:
			fmt.Fprintf(env.Stdout, "Pulling %s on %s\n", target.Image, s.Node)
		case s.RunsThere:
			fmt.Fprintf(env.Stdout, "%s: %s (runs there)\n", app.Name, s.Node)
		default:
			fmt.Fprintf(env.Stdout, "%s: %s (fewest apps)\n", app.Name, s.Node)
		}
	})
	if err != nil {
		return deployError(app.Name, err)
	}
	if deployed.Unchanged {
		fmt.Fprintf(env.Stdout, "%s runs %s on %s already; nothing changed\n", app.Name, deployed.Image, deployed.Node)
		return nil
	}
	fmt.Fprintf(env.Stdout, "%s runs %s on %s\n", app.Name, deployed.Image, deployed.Node)
	return nil
}

// buildApp builds app's image, from its build context, into dir, before
// any node is contacted, once for each architecture the nodes of d
// have, and returns the app as a deploy runs it, which imports the build
// for a node's architecture. A signal ends the build, and a second one
// calls exit.
func buildApp(ctx context.Context, env Env, exit func(int), d *cluster.Dir, app *appfile.App, dir string) (orchestrate.App, error) {
	nodes, err := d.Nodes()
	if err != nil {
		return orchestrate.App{}, err
	}
	var arches []string
	for _, n := range nodes {
		arches = append(arches, n.Architecture)
	}
	docker := buildx.Docker{Path: env.Docker, Output: env.Stderr}
	ctx, stop := interrupt.Context(ctx, exit)
	defer stop()
	daemon, err := docker.Check(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return orchestrate.App{}, fmt.Errorf("mooctl stopped checking Docker, before it built %s; nothing changed on the cluster", app.Name)
		}
		return orchestrate.App{}, fmt.Errorf("mooctl can't build %s: %v\nmooctl installs nothing; nothing was built, and nothing changed on the cluster", app.Name, err)
	}
	docker.Builder = daemon.Builder
	now := time.Now
	if env.Now != nil {
		now = env.Now
	}
	tag := "moostack.local/" + app.Name + ":" + now().UTC().Format("20060102T150405Z")
	archive := func(arch string) string { return filepath.Join(dir, arch+".tar") }
	for _, a := range slices.Compact(slices.Sorted(slices.Values(arches))) {
		fmt.Fprintf(env.Stdout, "Building %s as %s for linux/%s\n", app.Name, tag, a)
		if err := docker.Build(ctx, app.Build, tag, a, archive(a)); err != nil {
			return orchestrate.App{}, buildError(app.Name, a, daemon.Arch, err)
		}
	}
	return orchestrate.App{Name: app.Name, Image: tag, Archive: func(node string) (io.ReadSeekCloser, error) {
		// The cluster directory is read again, as the preflight read it:
		// a node may have joined it while the app was built.
		nodes, err := d.Nodes()
		if err != nil {
			return nil, err
		}
		i := slices.IndexFunc(nodes, func(n cluster.Node) bool { return n.Name == node })
		if i < 0 {
			return nil, fmt.Errorf("the cluster directory no longer lists %s; deploy again", node)
		}
		a := nodes[i].Architecture
		if !slices.Contains(arches, a) {
			return nil, fmt.Errorf("%s, of linux/%s, joined the cluster directory while %s was built, so mooctl built nothing for it; deploy again", node, a, app.Name)
		}
		return os.Open(archive(a))
	}}, nil
}

// buildError is err, why the build of the app called app for linux on
// arch failed, on a Docker whose machine is of dockerArch, as the
// operator reads it, below buildx's own output: that nothing changed;
// for "exec format error" in a build for another architecture than
// Docker's machine's, how to build for it; and why buildx ended, if it
// didn't end as a failed build does, saying why itself.
func buildError(app, arch, dockerArch string, err error) error {
	if errors.Is(err, context.Canceled) {
		return fmt.Errorf("mooctl stopped the build of %s; nothing changed on the cluster", app)
	}
	msg := fmt.Sprintf("the build of %s failed; nothing changed on the cluster", app)
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 1 {
		msg += fmt.Sprintf("\ndocker buildx ended: %v", errors.Unwrap(err))
	}
	var be *buildx.BuildError
	if errors.As(err, &be) && be.ExecFormat && arch != dockerArch {
		msg += fmt.Sprintf("\nThe build said \"exec format error\": a step ran a program this machine can't run, as one for linux/%s is without emulation. "+
			"On Linux, register QEMU for it once with docker run --privileged --rm tonistiigi/binfmt --install %s, or install your distribution's qemu-user-static with its binfmt entries; "+
			"Docker Desktop has it already. Or build the program for linux/%s in a stage that runs on this machine's platform, FROM --platform=$BUILDPLATFORM.", arch, arch, arch)
	}
	return errors.New(msg)
}

// deployError is err, why the deploy of the app called app failed, as
// the operator reads it: what failed, and what it means for the cluster.
func deployError(app string, err error) error {
	var (
		pull      *orchestrate.PullError
		imported  *orchestrate.ImportError
		run       *orchestrate.RunError
		unchecked *orchestrate.UncheckedError
	)
	switch {
	case errors.As(err, &pull):
		return pullError(pull)
	case errors.As(err, &imported):
		return importError(imported)
	case errors.As(err, new(*orchestrate.DuplicateAppError)):
		return fmt.Errorf("%w\nRun mooctl deploy --remove, which removes %s from every node it runs on, then deploy it again", err, app)
	case errors.As(err, &run):
		var what string
		switch run.Runs {
		case orchestrate.RunUnknown:
			what = fmt.Sprintf("mooctl can't tell whether %s runs on %s.", app, run.Node)
		case orchestrate.RunsAsBefore:
			what = fmt.Sprintf("%s still runs on %s as it ran before.", app, run.Node)
		case orchestrate.RunsAsAsked:
			what = fmt.Sprintf("%s runs %s on %s all the same, as the run asked.", app, run.Image, run.Node)
		case orchestrate.RunsOther:
			what = fmt.Sprintf("%s runs %s on %s, which neither ran before nor was asked for.", app, run.Image, run.Node)
		default:
			what = fmt.Sprintf("%s is not running on %s.", app, run.Node)
		}
		if run.Stopped {
			return fmt.Errorf("mooctl stopped waiting for %s to run %s, which may go on on the node: %v\nRun mooctl deploy again: it says whether %s runs", run.Node, app, run.Err, app)
		}
		return fmt.Errorf("%s failed to run %s: %s\n%s", run.Node, app, status.Convert(run.Err).Message(), what)
	case errors.As(err, &unchecked):
		return fmt.Errorf("%w\nRun mooctl deploy again: it says whether %s runs", err, app)
	}
	return err
}

// importError is why the import e failed, as the operator reads it:
// what the node said, and that nothing changed.
func importError(e *orchestrate.ImportError) error {
	if errors.Is(e.Err, orchestrate.ErrLockLost) || errors.Is(e.Err, context.Canceled) {
		return fmt.Errorf("mooctl stopped the import of %s on %s: %v; nothing changed", e.Image, e.Node, e.Err)
	}
	msg := status.Convert(e.Err).Message()
	if status.Code(e.Err) == codes.FailedPrecondition {
		// A build for another architecture than the node's: the cluster
		// directory says the node is of the architecture built for.
		if strings.Contains(msg, "for this node's") {
			return fmt.Errorf("%s refused %s: %s; nothing changed\nCheck the architecture that %s's node file in the cluster directory gives it", e.Node, e.Image, msg, e.Node)
		}
		return fmt.Errorf("%s refused %s: %s; nothing changed\nRun mooctl deploy again", e.Node, e.Image, msg)
	}
	// An archive the node can't read is so whenever it is sent.
	if status.Code(e.Err) == codes.InvalidArgument {
		return fmt.Errorf("%s couldn't import %s: %s; nothing changed", e.Node, e.Image, msg)
	}
	return fmt.Errorf("%s couldn't import %s: %s; nothing changed\nRun mooctl deploy again", e.Node, e.Image, msg)
}

// pullError is why the pull e failed, as the operator reads it: what it
// means, by the node's class of failure, that nothing changed, and what
// the node said.
func pullError(e *orchestrate.PullError) error {
	if errors.Is(e.Err, orchestrate.ErrLockLost) || errors.Is(e.Err, context.Canceled) {
		return fmt.Errorf("mooctl stopped the pull of %s on %s: %v; nothing changed", e.Image, e.Node, e.Err)
	}
	var means string
	switch status.Code(e.Err) {
	case codes.NotFound:
		means = "the registry doesn't have it; check its name and tag"
	case codes.PermissionDenied:
		means = "the registry denied access to it, which Docker Hub answers for a private repository and for one that doesn't exist alike; check its name, and that it is public"
	case codes.ResourceExhausted:
		means = fmt.Sprintf("the registry rate limits %s; Docker Hub allows 100 anonymous pulls from each IPv4 address or IPv6 /64 within a window of time, which the nodes behind one address share; wait, then deploy again", e.Node)
	case codes.Unavailable:
		means = fmt.Sprintf("%s can't reach the registry, or the registry answers that it is down; check %s's network, or try again later", e.Node, e.Node)
	case codes.Unimplemented:
		return fmt.Errorf("%s can't pull images: its agent is of a release before deploy; update the cluster with mooctl cluster update; nothing changed", e.Node)
	default:
		return fmt.Errorf("%s couldn't pull %s: %s; nothing changed", e.Node, e.Image, status.Convert(e.Err).Message())
	}
	return fmt.Errorf("%s couldn't pull %s: %s\nThe node said: %s\nNothing changed.", e.Node, e.Image, means, status.Convert(e.Err).Message())
}
