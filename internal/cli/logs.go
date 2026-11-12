package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"

	"github.com/urfave/cli/v3"

	"github.com/moostackhq/moostack/internal/deployfile"
	"github.com/moostackhq/moostack/internal/objects"
	"github.com/moostackhq/moostack/internal/sshclient"
)

// logsCommand is mooctl logs.
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

// logs is mooctl logs.
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
	ns := sshclient.ShellQuote(app)
	out, err := run(ctx, c, "k3s kubectl get namespaces -l "+sshclient.ShellQuote(objects.AppLabel+"="+app)+" -o name --request-timeout=30s", "")
	if err != nil {
		return fmt.Errorf("looking for %s: %w", app, err)
	}
	if strings.TrimSpace(out) == "" {
		return fmt.Errorf("there is no app %s on %s", app, d.Cluster.Ref)
	}
	pods, err := run(ctx, c, "k3s kubectl get pods -n "+ns+" -o name --request-timeout=30s", "")
	if err != nil {
		return fmt.Errorf("listing %s's pods: %w", app, err)
	}
	if strings.TrimSpace(pods) == "" {
		return fmt.Errorf("%s has no pods", app)
	}
	// Every pod of the namespace: no pod has the label moostack.dev/none.
	script := "k3s kubectl logs -n " + ns + " -l '!moostack.dev/none' --all-containers --prefix --ignore-errors --max-log-requests=50 --tail=" + strconv.Itoa(cmd.Int("tail"))
	if cmd.Bool("follow") {
		script += " --follow"
	}
	code, err := c.Stream(ctx, script, nil, env.Stdout, env.Stderr)
	if ctx.Err() != nil {
		return nil
	}
	if err != nil {
		return err
	}
	if code != 0 {
		return fmt.Errorf("k3s kubectl logs exited %d", code)
	}
	if cmd.Bool("follow") {
		fmt.Fprintln(env.Stderr, "Stopped: the pods followed have ended.")
	}
	return nil
}
