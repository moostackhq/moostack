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
	"github.com/moostackhq/moostack/internal/release"
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
		for _, row := range componentRows(rel, status.Components) {
			fmt.Fprintf(w, "%s\tyes\t%s\t%s\t%s\t%s\t%s\n", n.Name, mode, row.name, row.running, row.pinned, row.health)
			if row.problem != "" {
				problems = append(problems, fmt.Sprintf("%s: %s: %s", n.Name, row.name, row.problem))
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

// componentRow is a component's row in cluster status, with the
// problem to print below the table.
type componentRow struct{ name, running, pinned, health, problem string }

// componentRows are the rows of a node that reported components: one for
// each component rel pins, in rel's order, then one for each component
// it doesn't. A pinned component the node didn't report is of unknown
// health.
func componentRows(rel *release.Release, components []*nodev1.Component) []componentRow {
	reported := map[string]*nodev1.Component{}
	for _, c := range components {
		reported[c.Name] = c
	}
	var rows []componentRow
	for _, p := range rel.Components {
		c, ok := reported[p.Name]
		if !ok {
			rows = append(rows, componentRow{name: p.Name, running: "-", pinned: p.Version, health: nodeclient.HealthName(nodev1.Health_HEALTH_UNSPECIFIED)})
			continue
		}
		rows = append(rows, reportedRow(c, p.Version))
	}
	for _, c := range components {
		if _, ok := rel.Component(c.Name); !ok {
			rows = append(rows, reportedRow(c, "-"))
		}
	}
	return rows
}

// reportedRow is the row of the reported component c, pinned at pinned.
func reportedRow(c *nodev1.Component, pinned string) componentRow {
	running := c.Version
	if running == "" {
		running = "-"
	}
	return componentRow{name: c.Name, running: running, pinned: pinned, health: nodeclient.HealthName(c.Health), problem: c.Problem}
}

// nodeStatus is n's status, from env.NodeStatus or else over SSH. It
// fails only if the node can't be reached.
func nodeStatus(ctx context.Context, env Env, n cluster.Node) (nodeclient.Status, error) {
	ctx, cancel := context.WithTimeout(ctx, statusTimeout)
	defer cancel()
	if env.NodeStatus != nil {
		return env.NodeStatus(ctx, n)
	}
	c, err := sshclient.Dial(ctx, sshclient.Node{Target: n.User + "@" + n.Address, HostKeys: []ssh.PublicKey{n.HostKey}}, sshOptions(env))
	if err != nil {
		return nodeclient.Status{}, err
	}
	defer c.Close()
	node, err := nodeclient.Dial(c)
	if err != nil {
		return nodeclient.Status{}, err
	}
	defer node.Close()
	return node.Status(ctx), nil
}
