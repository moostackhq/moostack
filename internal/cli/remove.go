package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/urfave/cli/v3"

	"github.com/moostackhq/moostack/internal/deployfile"
	"github.com/moostackhq/moostack/internal/interrupt"
	"github.com/moostackhq/moostack/internal/registry"
	"github.com/moostackhq/moostack/internal/sshclient"
)

// appLabel is the label that marks an app's namespace with its name.
const appLabel = "moostack.dev/app"

// removeApp is mooctl deploy --remove: it removes the app named, or the
// current directory's, from the cluster: its HelmChart, which uninstalls
// its chart, its namespace, with everything in it, and its images.
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
	label := sshclient.ShellQuote(appLabel + "=" + name)
	out, err := run(ctx, cpClient, "k3s kubectl get namespaces -l "+label+" -o name --request-timeout=30s", "")
	if err != nil {
		return fmt.Errorf("looking for %s: %w", name, err)
	}
	if strings.TrimSpace(out) == "" {
		return fmt.Errorf("there is no app %s on %s", name, d.Cluster.Ref)
	}
	say(env, "Removing %s from %s...", name, d.Cluster.Ref)
	if err := registry.Remove(ctx, registryAddr(cp), name, cpClient.DialTCP); err != nil {
		return err
	}
	script := "set -e\n" +
		"k3s kubectl delete helmchart -n kube-system -l " + label + " --ignore-not-found --timeout=300s >/dev/null\n" +
		"k3s kubectl delete namespace " + sshclient.ShellQuote(name) + " --ignore-not-found --timeout=300s >/dev/null"
	if _, err := run(ctx, cpClient, script, ""); err != nil {
		return fmt.Errorf("removing %s: %w", name, err)
	}
	fmt.Fprintf(env.Stdout, "Removed %s from %s\n", name, d.Cluster.Ref)
	return nil
}
