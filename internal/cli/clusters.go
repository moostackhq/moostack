package cli

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"text/tabwriter"

	"github.com/urfave/cli/v3"

	"github.com/moostackhq/moostack/internal/clusterdir"
	"github.com/moostackhq/moostack/internal/interrupt"
)

// clusterList is mooctl cluster list.
func clusterList(env Env, cmd *cli.Command) error {
	if err := wantArgs(cmd, 0); err != nil {
		return err
	}
	r, err := root(env)
	if err != nil {
		return err
	}
	dirs, err := clusterDirs(r)
	if err != nil {
		return err
	}
	if len(dirs) == 0 {
		fmt.Fprintln(env.Stdout, "No clusters.")
		return nil
	}
	def, err := defaultCluster(env)
	if err != nil {
		return err
	}
	w := tabwriter.NewWriter(env.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "CLUSTER\tNODES\tDEFAULT")
	for _, d := range dirs {
		nodes, err := d.Nodes()
		if err != nil {
			return fmt.Errorf("%s: %w", d.Path, err)
		}
		mark := ""
		if d.Cluster.Ref == def {
			mark = "yes"
		}
		fmt.Fprintf(w, "%s\t%d\t%s\n", d.Cluster.Ref, len(nodes), mark)
	}
	return w.Flush()
}

// clusterDirs are the cluster directories under root, at
// <prefix>/<name>, in the order of their paths.
func clusterDirs(root string) ([]*clusterdir.Dir, error) {
	paths, err := filepath.Glob(filepath.Join(root, "*", "*"))
	if err != nil {
		return nil, err
	}
	var dirs []*clusterdir.Dir
	for _, p := range paths {
		if info, err := os.Stat(p); err != nil || !info.IsDir() {
			continue
		}
		d, err := clusterdir.Open(p)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("%s: %w", p, err)
		}
		dirs = append(dirs, d)
	}
	return dirs, nil
}

// clusterRemove is mooctl cluster remove: it uninstalls k3s from every
// node of a cluster, the control plane last, and deletes its cluster
// directory, once the operator has typed the cluster's name. It holds
// the operation lock from before it reads the nodes, which it takes from
// the directory brought up to its remote.
func clusterRemove(ctx context.Context, env Env, cmd *cli.Command) error {
	if err := wantArgs(cmd, 1); err != nil {
		return err
	}
	ref, dir, err := refDir(env, cmd, cmd.Args().First())
	if err != nil {
		return err
	}
	d, err := clusterdir.Open(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("there is no cluster directory at %s", dir)
	}
	if err != nil {
		return err
	}
	if d.Cluster.Ref != ref.String() {
		return fmt.Errorf("%s is cluster %s, not %s", dir, d.Cluster.Ref, ref)
	}
	ctx, stop := interrupt.Context(ctx, env.Exit)
	defer stop()

	conns := map[string]remote{}
	var reached, gone []string
	var dialErrs []error
	dial := func(n clusterdir.Node) {
		c, err := dialNode(ctx, env, n)
		if err != nil {
			gone = append(gone, nodeAt(n))
			dialErrs = append(dialErrs, err)
			return
		}
		conns[n.Name] = c
		reached = append(reached, nodeAt(n))
	}
	defer func() {
		for _, c := range conns {
			c.Close()
		}
	}()
	cp, hasCP, err := d.ControlPlane()
	if err != nil {
		return err
	}
	var lockOn lockHost
	if hasCP {
		dial(cp)
		if c, ok := conns[cp.Name]; ok {
			lockOn = remoteHost{c}
		}
	}
	op, ctx, err := begin(ctx, env, dir, lockOn, true)
	if err != nil {
		return err
	}
	defer op.release()
	if d, err = clusterdir.Open(dir); err != nil {
		return err
	}
	if !hasCP {
		cp = clusterdir.Node{}
	}
	if err := sameControlPlane(d, cp); err != nil {
		return err
	}
	nodes, err := d.Nodes()
	if err != nil {
		return err
	}
	// The agents come first, the control plane last.
	var ordered []clusterdir.Node
	for _, n := range nodes {
		if !n.ControlPlane {
			ordered = append(ordered, n)
			dial(n)
		}
	}
	if hasCP {
		// The control plane is named after the agents.
		at := nodeAt(cp)
		for _, list := range []*[]string{&reached, &gone} {
			if i := slices.Index(*list, at); i >= 0 {
				*list = append(slices.Delete(*list, i, i+1), at)
			}
		}
		ordered = append(ordered, cp)
	}
	if len(gone) > 0 && !cmd.Bool("unreachable") {
		return fmt.Errorf("%w\nPass --unreachable to remove the cluster anyway, leaving k3s on what can't be reached.", errors.Join(dialErrs...))
	}

	t, closeTerminal := openTerminal(env)
	defer closeTerminal()
	if t == nil {
		return errors.New("cluster remove asks for the cluster's name on a terminal, and there is none")
	}
	fmt.Fprintf(t, "Removing cluster %s:\n", ref)
	if len(reached) > 0 {
		fmt.Fprintf(t, "- k3s is uninstalled from %s: every app there stops, and its volumes are deleted\n", strings.Join(reached, ", "))
	}
	if len(gone) > 0 {
		fmt.Fprintf(t, "- %s can't be reached: k3s stays there\n", strings.Join(gone, ", "))
	}
	fmt.Fprintf(t, "- the cluster directory %s is deleted, with its secret store\n", dir)
	if op.remote {
		fmt.Fprintln(t, "- its git remote keeps what was pushed")
	}
	fmt.Fprintf(t, "This can't be undone. Type the cluster's name to remove it: ")
	typed, err := t.ReadLine()
	if err != nil {
		return fmt.Errorf("reading the cluster's name: %w", err)
	}
	if strings.TrimSpace(typed) != ref.String() {
		return fmt.Errorf("that isn't %s; nothing was removed", ref)
	}

	orch := orchestratorOf(env, d.Cluster, cp.PrivateAddress, conns[cp.Name])
	for _, n := range ordered {
		c, ok := conns[n.Name]
		if !ok {
			continue
		}
		say(env, "Uninstalling k3s from %s...", n.Name)
		if err := uninstall(ctx, orch, c, n); err != nil {
			return fmt.Errorf("uninstalling from %s: %w; run cluster remove again", n.Name, err)
		}
	}
	if err := os.RemoveAll(dir); err != nil {
		return err
	}
	fmt.Fprintf(env.Stdout, "Removed cluster %s\n", ref)
	if r, err := root(env); err == nil && ref.Dir(r) == dir {
		// The prefix's directory goes with its last cluster.
		os.Remove(filepath.Dir(dir))
		if def, err := defaultCluster(env); err == nil && def == ref.String() {
			path, err := defaultFile(env)
			if err != nil {
				return err
			}
			if err := os.Remove(path); err != nil {
				return err
			}
			fmt.Fprintln(env.Stdout, "Default cluster: none")
		}
	}
	for _, g := range gone {
		fmt.Fprintf(env.Stderr, "Warning: %s couldn't be reached; if k3s still runs there, uninstall it there.\n", g)
	}
	return nil
}

// nodeAt is a node's name with its SSH target.
func nodeAt(n clusterdir.Node) string {
	return fmt.Sprintf("%s (%s)", n.Name, n.Target)
}
