//go:build !release

package cli

import (
	"cmp"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/urfave/cli/v3"

	"github.com/moostackhq/moostack/internal/clusterdir"
	"github.com/moostackhq/moostack/internal/deployfile"
	"github.com/moostackhq/moostack/internal/interrupt"
)

func init() {
	devCommands = func(env Env) []*cli.Command {
		return []*cli.Command{{
			Name:   "dev",
			Usage:  "develop Moostack",
			Hidden: true,
			Commands: []*cli.Command{{
				Name:  "console",
				Usage: "develop the console",
				Commands: []*cli.Command{{
					Name:      "deploy",
					Usage:     "build the console of a Moostack checkout, the current directory's without one, and run it on the cluster",
					ArgsUsage: "[checkout]",
					Action:    func(ctx context.Context, cmd *cli.Command) error { return devConsoleDeploy(ctx, env, cmd) },
				}},
			}},
		}}
	}
}

// devConsoleDeploy is mooctl dev console deploy.
func devConsoleDeploy(ctx context.Context, env Env, cmd *cli.Command) error {
	checkout := "."
	switch cmd.NArg() {
	case 0:
	case 1:
		checkout = cmd.Args().First()
	default:
		return fmt.Errorf("%s takes at most 1 argument, not %d; see --help", cmd.FullName(), cmd.NArg())
	}
	dockerfile := filepath.Join(checkout, "cmd", "console", "Dockerfile")
	if _, err := os.Stat(dockerfile); err != nil {
		return fmt.Errorf("%s is no Moostack checkout: it has no cmd/console/Dockerfile", checkout)
	}
	ctx, stop := interrupt.Context(ctx, env.Exit)
	defer stop()
	d, err := openCluster(env, cmd)
	if err != nil {
		return err
	}
	if err := updateFromRemote(ctx, d.Path); err != nil {
		return err
	}
	if d, err = clusterdir.Open(d.Path); err != nil {
		return err
	}
	cp, ok, err := d.ControlPlane()
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("cluster %s has no nodes yet; add one with mooctl cluster node-add", d.Cluster.Ref)
	}
	say(env, "Deploying the console of %s to %s", checkout, d.Cluster.Ref)
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

	// The image's build context: the binary, the licence and the
	// Dockerfile.
	build, err := os.MkdirTemp("", "mooctl-console-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(build)
	say(env, "Compiling the console for linux/%s...", cp.Architecture)
	compile := exec.CommandContext(ctx, cmp.Or(env.Go, "go"), "build", "-trimpath", "-ldflags=-s", "-o", filepath.Join(build, "console"), "./cmd/console")
	compile.Dir = checkout
	compile.Env = append(os.Environ(), "GOOS=linux", "GOARCH="+cp.Architecture, "CGO_ENABLED=0")
	if out, err := compile.CombinedOutput(); err != nil {
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		return fmt.Errorf("compiling the console: %w\n%s", err, strings.TrimSpace(string(out)))
	}
	if err := os.Chmod(filepath.Join(build, "console"), 0o755); err != nil {
		return err
	}
	for name, from := range map[string]string{"Dockerfile": dockerfile, "LICENSE": filepath.Join(checkout, "LICENSE")} {
		content, err := os.ReadFile(from)
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(build, name), content, 0o644); err != nil {
			return err
		}
	}
	image, err := buildAndPush(ctx, env, consoleRepository, "the console", deployfile.Build{Dockerfile: build}, []string{cp.Architecture}, cp, cpClient)
	if err != nil {
		return err
	}
	say(env, "Waiting for the console to be ready...")
	_, digest, _ := strings.Cut(image, "@sha256:")
	if err := runConsole(ctx, orchestratorThrough(env, cpClient), cpClient, cp, digest); err != nil {
		return err
	}
	fmt.Fprintf(env.Stdout, "Deployed the console to %s\nImage: %s\n", d.Cluster.Ref, image)
	return nil
}
