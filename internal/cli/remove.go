package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"

	"github.com/urfave/cli/v3"

	"github.com/moostackhq/moostack/internal/deployfile"
	"github.com/moostackhq/moostack/internal/interrupt"
	"github.com/moostackhq/moostack/internal/model"
	"github.com/moostackhq/moostack/internal/orchestrator"
	"github.com/moostackhq/moostack/internal/registry"
)

// removeApp is mooctl deploy --remove: it removes the app named, or the
// current directory's, from the cluster, and before that its images from
// the cluster's registry.
func removeApp(ctx context.Context, env Env, cmd *cli.Command) error {
	var name string
	switch cmd.NArg() {
	case 0:
		_, n, err := deployfile.ReadRef(".")
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("which app? name it, or run mooctl deploy --remove in its directory")
		}
		if err != nil {
			return err
		}
		name = n
	case 1:
		name = cmd.Args().First()
		if err := deployfile.CheckName(name); err != nil {
			return err
		}
	default:
		return wantArgs(cmd, 1)
	}
	ctx, stop := interrupt.Context(ctx, env.Exit)
	defer stop()
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
	cpClient, err := dialNode(ctx, env, cp)
	if err != nil {
		return err
	}
	defer cpClient.Close()
	ctx, release, err := hold(ctx, cpClient)
	if err != nil {
		return err
	}
	defer release()
	orch := orchestratorThrough(env, cpClient)
	apps, err := orch.Apps(ctx)
	if err != nil {
		return fmt.Errorf("looking for %s: %w", name, err)
	}
	noApp := fmt.Errorf("there is no app %s on %s", name, d.Cluster.Ref)
	if !slices.ContainsFunc(apps, func(a model.AppState) bool { return a.Name == name }) {
		return noApp
	}
	say(env, "Removing %s from %s...", name, d.Cluster.Ref)
	if err := registry.Remove(ctx, registryAddr(cp), name, cpClient.DialTCP); err != nil {
		return err
	}
	if err := orch.Remove(ctx, name); errors.Is(err, orchestrator.ErrNoApp) {
		return noApp
	} else if err != nil {
		return err
	}
	fmt.Fprintf(env.Stdout, "Removed %s from %s\n", name, d.Cluster.Ref)
	return nil
}
