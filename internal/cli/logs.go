package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"

	"github.com/urfave/cli/v3"

	"github.com/moostackhq/moostack/internal/deployfile"
	"github.com/moostackhq/moostack/internal/orchestrator"
)

func logsCommand(env Env) *cli.Command {
	return &cli.Command{
		Name:      "logs",
		Usage:     "print the logs of an app's pods: the app in the current directory, or the one named",
		ArgsUsage: "[app]",
		Flags: []cli.Flag{
			&cli.BoolFlag{Name: "follow", Aliases: []string{"f"}, Usage: "keep printing new lines"},
			&cli.IntFlag{Name: "tail", Value: 100, Usage: "lines of each container's log to start with; -1 for all"},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error { return logs(ctx, env, cmd) },
	}
}

func logs(ctx context.Context, env Env, cmd *cli.Command) error {
	if cmd.NArg() > 1 {
		return wantArgs(cmd, 1)
	}
	app := cmd.Args().First()
	if app == "" {
		_, name, err := deployfile.ReadRef(".")
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("which app? name it, or run mooctl logs in its directory")
		}
		if err != nil {
			return err
		}
		app = name
	}
	if err := deployfile.CheckName(app); err != nil {
		return err
	}
	tail := cmd.Int("tail")
	if tail < -1 {
		return fmt.Errorf("--tail %d: it is a number of lines, or -1 for all", tail)
	}
	d, err := openCluster(env, cmd)
	if err != nil {
		return err
	}
	cp, ok, err := d.ControlPlane()
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("cluster %s has no nodes", d.Cluster.Ref)
	}
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt)
	defer stop()
	c, err := dialNode(ctx, env, cp)
	if err != nil {
		return err
	}
	defer c.Close()
	err = orchestratorThrough(env, c).Logs(ctx, app, orchestrator.LogOptions{Tail: tail, Follow: cmd.Bool("follow")}, env.Stdout)
	switch {
	case ctx.Err() != nil:
		return nil
	case errors.Is(err, orchestrator.ErrNoApp):
		return fmt.Errorf("there is no app %s on %s", app, d.Cluster.Ref)
	case errors.Is(err, orchestrator.ErrNoReplicas):
		return fmt.Errorf("%s has no pods", app)
	case err != nil:
		return err
	}
	if cmd.Bool("follow") {
		fmt.Fprintln(env.Stderr, "Stopped: the pods followed have ended.")
	}
	return nil
}
