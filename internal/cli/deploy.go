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
		Usage:     "deploy the app whose app.yml is in dir, the current directory if none is given, to a node of the cluster",
		ArgsUsage: "[dir]",
		Description: "The app runs on the node that runs it already, or else on the node that runs the fewest apps. " +
			"The node pulls the app's image itself, and deploy checks a few seconds after the app's container started that it still runs. " +
			"Deploying an app that runs already replaces it, unless it runs the same build of its image, which changes nothing. " +
			"Where an app runs is kept on the nodes alone, so deploy doesn't change the cluster directory.",
		Flags: []cli.Flag{forceFlag},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			if cmd.NArg() > 1 {
				return fmt.Errorf("deploy takes the app's directory, or nothing for the current one, not %d arguments; see --help", cmd.NArg())
			}
			return withHint(deploy(ctx, env, cmd))
		},
	}
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
