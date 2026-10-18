package cli

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/urfave/cli/v3"
	"golang.org/x/crypto/ssh"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/cluster"
	"github.com/moostackhq/moostack/internal/nodeclient"
	"github.com/moostackhq/moostack/internal/sshclient"
)

// clusterCommand is mooctl cluster.
func clusterCommand(env Env) *cli.Command {
	return &cli.Command{
		Name:  "cluster",
		Usage: "create and inspect the cluster",
		Commands: []*cli.Command{
			{
				Name:  "init",
				Usage: "make the cluster directory, pinned to this mooctl's release",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "name",
						Usage: "the cluster's name, instead of the directory's",
					},
				},
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if err := wantArgs(cmd, 0); err != nil {
						return err
					}
					return clusterInit(env, cmd.String("dir"), cmd.String("name"))
				},
			},
			{
				Name:  "status",
				Usage: "show each node's mode and node components, next to the pinned release",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if err := wantArgs(cmd, 0); err != nil {
						return err
					}
					return clusterStatus(ctx, env, cmd)
				},
			},
		},
	}
}

// clusterInit is mooctl cluster init.
func clusterInit(env Env, dir, name string) error {
	if env.Version == "" {
		return errors.New("this mooctl has no release to pin, since it wasn't built by the release build")
	}
	if name == "" {
		abs, err := filepath.Abs(dir)
		if err != nil {
			return err
		}
		name = filepath.Base(abs)
		if err := cluster.CheckName(name); err != nil {
			return fmt.Errorf("the directory's name can't name the cluster: %w; choose a name with --name", err)
		}
	}
	d, err := cluster.Init(dir, cluster.Cluster{Name: name, Release: env.Version})
	if err != nil {
		return err
	}
	fmt.Fprintf(env.Stdout, "Created cluster %s on release %s in %s\n", d.Cluster.Name, d.Cluster.Release, dir)
	return nil
}

// statusTimeout bounds reaching one node and asking for its status.
const statusTimeout = 30 * time.Second

// clusterStatus is mooctl cluster status.
func clusterStatus(ctx context.Context, env Env, cmd *cli.Command) error {
	d, err := cluster.Open(cmd.String("dir"))
	if err != nil {
		return err
	}
	rel, err := openRelease(cmd, d)
	if err != nil {
		return err
	}
	nodes, err := d.Nodes()
	if err != nil {
		return err
	}
	fmt.Fprintf(env.Stdout, "Cluster %s on release %s\n\n", d.Cluster.Name, rel.Version)
	w := tabwriter.NewWriter(env.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "NODE\tREACHABLE\tMODE\tCOMPONENT\tRUNNING\tPINNED\tHEALTH")
	var problems []string
	for _, n := range nodes {
		status, err := nodeStatus(ctx, env, n)
		if err != nil {
			fmt.Fprintf(w, "%s\tno\t-\t-\t-\t-\t-\n", n.Name)
			problems = append(problems, fmt.Sprintf("%s: %v", n.Name, err))
			continue
		}
		mode := nodeclient.ModeName(status.Mode)
		for _, c := range status.Components {
			pinned := "-"
			if p, ok := rel.Component(c.Name); ok {
				pinned = p.Version
			}
			running := c.Version
			if running == "" {
				running = "-"
			}
			fmt.Fprintf(w, "%s\tyes\t%s\t%s\t%s\t%s\t%s\n", n.Name, mode, c.Name, running, pinned, nodeclient.HealthName(c.Health))
			if c.Problem != "" {
				problems = append(problems, fmt.Sprintf("%s: %s: %s", n.Name, c.Name, c.Problem))
			}
		}
	}
	if err := w.Flush(); err != nil {
		return err
	}
	if len(problems) > 0 {
		fmt.Fprintf(env.Stdout, "\n%s\n", strings.Join(problems, "\n"))
	}
	return nil
}

// nodeStatus is n's status, from its agent over SSH.
func nodeStatus(ctx context.Context, env Env, n cluster.Node) (*nodev1.StatusResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, statusTimeout)
	defer cancel()
	c, err := sshclient.Dial(ctx, sshclient.Node{Target: n.User + "@" + n.Address, HostKeys: []ssh.PublicKey{n.HostKey}}, sshOptions(env))
	if err != nil {
		return nil, err
	}
	defer c.Close()
	agent, err := nodeclient.DialAgent(c)
	if err != nil {
		return nil, err
	}
	defer agent.Close()
	return agent.Status(ctx, &nodev1.StatusRequest{})
}
