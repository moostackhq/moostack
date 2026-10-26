package cli

import (
	"cmp"
	"context"
	"errors"
	"fmt"

	"github.com/urfave/cli/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/moostackhq/moostack/internal/appfile"
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
	// A signal ends the preflight, the pull or the wait for the app, and
	// the deploy gives the lock back, rather than ending mooctl.
	ctx, stop := interrupt.Context(ctx, env.Exit)
	defer stop()
	c, err := preflight(ctx, env, cmd, d, "", nil)
	if err != nil {
		return err
	}
	defer c.Close()
	deployed, err := c.Deploy(c.Context(), orchestrate.App{Name: app.Name, Image: app.Image}, func(s orchestrate.DeployStep) {
		switch {
		case s.Pulling:
			fmt.Fprintf(env.Stdout, "Pulling %s on %s\n", app.Image, s.Node)
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

// deployError is err, why the deploy of the app called app failed, as
// the operator reads it: what failed, and what it means for the cluster.
func deployError(app string, err error) error {
	var (
		pull      *orchestrate.PullError
		run       *orchestrate.RunError
		unchecked *orchestrate.UncheckedError
	)
	switch {
	case errors.As(err, &pull):
		return pullError(pull)
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
