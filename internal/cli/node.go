package cli

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/netip"
	"os"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/urfave/cli/v3"

	"github.com/moostackhq/moostack/internal/clusterdir"
	"github.com/moostackhq/moostack/internal/interrupt"
	"github.com/moostackhq/moostack/internal/model"
	"github.com/moostackhq/moostack/internal/orchestrator"
	"github.com/moostackhq/moostack/internal/registry"
	"github.com/moostackhq/moostack/internal/release"
)

// undoTimeout bounds undoing a failed node-add.
const undoTimeout = 5 * time.Minute

// roleName is how the operator sees what a node is in its cluster.
func roleName(controlPlane bool) string {
	if controlPlane {
		return "control plane"
	}
	return "node"
}

// modelNode is n as an orchestrator takes it.
func modelNode(n clusterdir.Node) model.Node {
	return model.Node{
		Name:           n.Name,
		ControlPlane:   n.ControlPlane,
		PrivateAddress: n.PrivateAddress,
		Interface:      n.Interface,
		PublicAddress:  n.PublicAddress,
		Architecture:   n.Architecture,
		MemoryMiB:      n.MemoryMiB,
		GOGC:           n.GOGC,
		GOMEMLIMIT:     n.GOMEMLIMIT,
	}
}

// nodeAdd is mooctl cluster node-add.
func nodeAdd(ctx context.Context, env Env, cmd *cli.Command) error {
	if err := wantArgs(cmd, 1); err != nil {
		return err
	}
	target := cmd.Args().First()
	var override netip.Addr
	if s := cmd.String("private-address"); s != "" {
		a, err := netip.ParseAddr(s)
		if err != nil {
			return fmt.Errorf("--private-address: %w", err)
		}
		override = a
	}
	name := cmd.String("name")
	if name != "" {
		if err := clusterdir.CheckName(name); err != nil {
			return fmt.Errorf("--name: %w", err)
		}
	}
	ctx, stop := interrupt.Context(ctx, env.Exit)
	defer stop()

	d, err := openCluster(env, cmd)
	if err != nil {
		return err
	}
	cp, hasCP, err := d.ControlPlane()
	if err != nil {
		return err
	}
	listed, err := d.Nodes()
	if err != nil {
		return err
	}
	if err := notANode(d, listed, target); err != nil {
		return err
	}
	host, key, err := connect(ctx, env, target, nil)
	if err != nil {
		return err
	}
	defer host.Close()
	lockOn := host
	var cpClient remote
	if hasCP {
		if cpClient, err = dialNode(ctx, env, cp); err != nil {
			return fmt.Errorf("%w\nIf %s is gone, remove it with mooctl cluster node-remove --unreachable %s.", err, cp.Name, cp.Name)
		}
		defer cpClient.Close()
		lockOn = cpClient
	}

	op, ctx, err := begin(ctx, env, d.Path, remoteHost{lockOn}, hasCP)
	if err != nil {
		return err
	}
	defer op.release()
	if d, err = clusterdir.Open(d.Path); err != nil {
		return err
	}
	if err := sameControlPlane(d, cp); err != nil {
		return err
	}
	nodes, err := d.Nodes()
	if err != nil {
		return err
	}
	orch := orchestratorOf(env, d.Cluster, cp.PrivateAddress, cpClient)
	var known []string
	if hasCP {
		if known, err = clusterNodeNames(ctx, orch); err != nil {
			return err
		}
	}
	if name == "" {
		name = freeName(nodes, known)
	} else if slices.Contains(known, name) && !slices.ContainsFunc(nodes, func(n clusterdir.Node) bool { return n.Name == name }) {
		return fmt.Errorf("the cluster has a Kubernetes node %s that isn't in the cluster directory; remove it with mooctl cluster node-remove %s, or choose another name", name, name)
	}
	for _, n := range nodes {
		if n.Name == name {
			return fmt.Errorf("the cluster has a node %s already; choose another name with --name", name)
		}
	}
	if err := notANode(d, nodes, target); err != nil {
		return err
	}

	found, err := orch.Preflight(ctx, orchestratorHost{host}, orchestrator.NodeRequest{Name: name, ControlPlane: !hasCP, PrivateAddress: override})
	if err != nil {
		return fmt.Errorf("%s: %w", target, err)
	}
	node := clusterdir.Node{
		Name:           name,
		Target:         target,
		HostKey:        key,
		PrivateAddress: found.PrivateAddress,
		Interface:      found.Interface,
		PublicAddress:  found.PublicAddress,
		Architecture:   found.Architecture,
		MemoryMiB:      found.MemoryMiB,
		GOGC:           found.GOGC,
		GOMEMLIMIT:     found.GOMEMLIMIT,
		ControlPlane:   !hasCP,
	}

	say(env, "Adding %s to %s (%s, %s, %d MiB, %s)", name, d.Cluster.Ref, roleName(node.ControlPlane), node.Architecture, node.MemoryMiB, node.PrivateAddress)
	var console string
	if node.ControlPlane {
		say(env, "Downloading the console...")
		dir, err := os.MkdirTemp("", "mooctl-console-")
		if err != nil {
			return err
		}
		defer os.RemoveAll(dir)
		client := &http.Client{Timeout: 10 * time.Minute}
		if console, err = release.Fetch(ctx, client, cmp.Or(env.Release, release.URL(commit)), "console-linux-"+node.Architecture+".tar", dir); err != nil {
			return err
		}
	}

	if err := add(ctx, env, orch, host, node, console); err != nil {
		return err
	}
	if err := d.WriteNode(node); err != nil {
		return err
	}
	recordOn := cpClient
	if node.ControlPlane {
		recordOn = host
	}
	if err := op.finish(ctx, "node-add "+name, remoteHost{recordOn}); err != nil {
		return err
	}
	fmt.Fprintf(env.Stdout, "Added %s to %s\n", name, d.Cluster.Ref)
	if hasCP && !slices.ContainsFunc(nodes, func(n clusterdir.Node) bool { return n.Architecture == node.Architecture }) {
		if apps, err := orch.Apps(ctx); err == nil && len(apps) > 0 {
			fmt.Fprintf(env.Stderr, "Warning: %s is the cluster's first %s node; deploy its built apps again to run them there.\n", name, node.Architecture)
		}
	}
	return nil
}

// notANode refuses target if it is the target of one of nodes, the nodes
// of the cluster of d.
func notANode(d *clusterdir.Dir, nodes []clusterdir.Node, target string) error {
	for _, n := range nodes {
		if n.Target == target {
			return fmt.Errorf("%s is node %s of %s already", target, n.Name, d.Cluster.Ref)
		}
	}
	return nil
}

// clusterNodeNames are the names of the nodes the cluster has.
func clusterNodeNames(ctx context.Context, orch orchestrator.Orchestrator) ([]string, error) {
	nodes, err := orch.Nodes(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing the cluster's nodes: %w", err)
	}
	var names []string
	for _, n := range nodes {
		names = append(names, n.Name)
	}
	return names, nil
}

// freeName is the first of node1, node2 and so on that neither a node of
// nodes nor a node of known, those the cluster has, is called.
func freeName(nodes []clusterdir.Node, known []string) string {
	for i := 1; ; i++ {
		name := fmt.Sprintf("node%d", i)
		if !slices.Contains(known, name) && !slices.ContainsFunc(nodes, func(n clusterdir.Node) bool { return n.Name == name }) {
			return name
		}
	}
}

// ownUninstallScript removes from a control plane what the commands put
// there themselves: the registry and the applied commit.
var ownUninstallScript = registry.ZotUninstallScript + "\nrm -f " + AppliedCommitFile + "\nrmdir " + path.Dir(AppliedCommitFile) + " 2>/dev/null\ntrue"

// uninstall removes the node n from host: what the orchestrator put
// there, and from a control plane the registry and the applied commit
// as well. It fails if the orchestrator's part does.
func uninstall(ctx context.Context, orch orchestrator.Orchestrator, host remote, n clusterdir.Node) error {
	err := orch.Uninstall(ctx, orchestratorHost{host}, modelNode(n))
	if n.ControlPlane {
		if _, oerr := run(ctx, host, ownUninstallScript, ""); oerr != nil {
			err = errors.Join(err, oerr)
		}
	}
	return err
}

// add makes host the node, and a control plane the host of the registry
// and of the console, whose image archive is at console. If it fails,
// it leaves nothing of them on host.
func add(ctx context.Context, env Env, orch orchestrator.Orchestrator, host remote, node clusterdir.Node, console string) error {
	uninstalling := false
	added, err := install(ctx, env, orch, host, node, &uninstalling)
	if err == nil && node.ControlPlane {
		err = installConsole(ctx, env, orch, host, node, console)
	}
	if err == nil {
		return nil
	}
	undo, cancel := context.WithTimeout(context.WithoutCancel(ctx), undoTimeout)
	defer cancel()
	if !uninstalling && (added || node.ControlPlane) {
		say(env, "Uninstalling...")
	}
	if added {
		if uerr := orch.Uninstall(undo, orchestratorHost{host}, modelNode(node)); uerr != nil {
			err = errors.Join(err, fmt.Errorf("uninstalling from %s: %w", node.Target, uerr))
		}
	}
	if node.ControlPlane {
		if _, uerr := run(undo, host, ownUninstallScript, ""); uerr != nil {
			err = errors.Join(err, fmt.Errorf("uninstalling from %s: %w", node.Target, uerr))
		}
	}
	return err
}

// install has the orchestrator make host the node, and installs the
// registry on a control plane once the orchestrator starts it. It
// reports whether the orchestrator added the node, and sets
// uninstalling if the orchestrator told that it uninstalls.
func install(ctx context.Context, env Env, orch orchestrator.Orchestrator, host remote, node clusterdir.Node, uninstalling *bool) (added bool, err error) {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	zot := make(chan error, 1)
	started := false
	step := func(s orchestrator.Step) {
		switch s {
		case orchestrator.Installing:
			if node.ControlPlane {
				say(env, "Installing k3s and zot...")
			} else {
				say(env, "Installing k3s...")
			}
		case orchestrator.Starting:
			if !node.ControlPlane {
				return
			}
			started = true
			go func() {
				_, err := runStoppable(ctx, host, registry.ZotScript(node.Architecture, node.PrivateAddress))
				if err != nil {
					err = fmt.Errorf("installing zot: %w", err)
					cancel(err)
				}
				zot <- err
			}()
		case orchestrator.WaitingForNode:
			say(env, "Waiting for %s to be Ready...", node.Name)
		case orchestrator.Uninstalling:
			*uninstalling = true
			say(env, "Uninstalling...")
		}
	}
	err = orch.AddNode(ctx, orchestratorHost{host}, modelNode(node), step)
	added = err == nil
	if started {
		if zerr := <-zot; zerr != nil && !errors.Is(err, zerr) {
			err = errors.Join(err, zerr)
		}
	}
	return added, err
}

// consoleRepository is the console's repository in the cluster's
// registry, under a name no app can have.
const consoleRepository = "kube-system/console"

// installConsole pushes the console's image, the archive at console, to
// the registry on node, the control plane, at host, and runs it.
func installConsole(ctx context.Context, env Env, orch orchestrator.Orchestrator, host remote, node clusterdir.Node, console string) error {
	say(env, "Installing the console...")
	digest, err := registry.Push(ctx, map[string]string{node.Architecture: console}, registryAddr(node), consoleRepository, host.DialTCP, nil)
	if err != nil {
		return fmt.Errorf("the console's image: %w", err)
	}
	return runConsole(ctx, orch, host, node, digest)
}

// runConsole has the orchestrator run the console from its image of
// digest, which is in the registry on node, the control plane, at host,
// on the node's addresses. It then marks the image as the one that
// runs.
func runConsole(ctx context.Context, orch orchestrator.Orchestrator, host remote, node clusterdir.Node, digest string) error {
	addresses := []netip.Addr{node.PrivateAddress}
	if node.PublicAddress.IsValid() {
		addresses = []netip.Addr{node.PublicAddress, node.PrivateAddress}
	}
	if err := orch.RunConsole(ctx, registry.Image(consoleRepository, digest), addresses); err != nil {
		return err
	}
	if err := registry.Tag(ctx, registryAddr(node), consoleRepository, digest, registry.LiveTag, host.DialTCP); err != nil {
		return fmt.Errorf("the console's image: %w", err)
	}
	return nil
}

// nodeRemove is mooctl cluster node-remove.
func nodeRemove(ctx context.Context, env Env, cmd *cli.Command) error {
	if err := wantArgs(cmd, 1); err != nil {
		return err
	}
	name := cmd.Args().First()
	if err := clusterdir.CheckName(name); err != nil {
		return err
	}
	unreachable := cmd.Bool("unreachable")
	ctx, stop := interrupt.Context(ctx, env.Exit)
	defer stop()
	d, err := openCluster(env, cmd)
	if err != nil {
		return err
	}
	cp, hasCP, err := d.ControlPlane()
	if err != nil {
		return err
	}
	nodes, err := d.Nodes()
	if err != nil {
		return err
	}
	_, inDir := findNode(nodes, name)
	if !hasCP {
		if inDir {
			return errors.New("the cluster directory has nodes but no control plane")
		}
		return fmt.Errorf("the cluster has no node %s", name)
	}
	if inDir && len(nodes) == 1 && !cmd.Bool("yes") {
		return lastNode(name)
	}

	cpClient, err := dialNode(ctx, env, cp)
	if err != nil {
		if !unreachable {
			return fmt.Errorf("%w\nPass --unreachable to remove nodes without the control plane.", err)
		}
		return removeWithoutControlPlane(ctx, env, d, cp, name, cmd.Bool("yes"))
	}
	defer cpClient.Close()
	op, ctx, err := begin(ctx, env, d.Path, remoteHost{cpClient}, true)
	if err != nil {
		return err
	}
	defer op.release()
	if d, err = clusterdir.Open(d.Path); err != nil {
		return err
	}
	if err := sameControlPlane(d, cp); err != nil {
		return err
	}
	if nodes, err = d.Nodes(); err != nil {
		return err
	}
	n, inDir := findNode(nodes, name)
	lastControlPlane := inDir && n.ControlPlane && len(nodes) == 1
	orch := orchestratorOf(env, d.Cluster, cp.PrivateAddress, cpClient)
	known, err := clusterNodeNames(ctx, orch)
	if err != nil && !lastControlPlane {
		return err
	}
	onCluster := slices.Contains(known, name)
	if !inDir && !onCluster {
		return fmt.Errorf("the cluster has no node %s", name)
	}
	if lastControlPlane && !cmd.Bool("yes") {
		return lastNode(name)
	}

	say(env, "Removing %s from %s", name, d.Cluster.Ref)
	var recordOn lockHost = remoteHost{cpClient}
	switch {
	case inDir && n.ControlPlane:
		var others []string
		for _, o := range nodes {
			if o.Name != name {
				others = append(others, o.Name)
			}
		}
		for _, k := range known {
			if k != name && !slices.Contains(others, k) {
				others = append(others, k)
			}
		}
		if len(others) > 0 {
			return fmt.Errorf("%s is the control plane, which goes last; remove %s first", name, strings.Join(others, ", "))
		}
		say(env, "Uninstalling...")
		if err := uninstall(ctx, orch, cpClient, n); err != nil {
			return fmt.Errorf("uninstalling from %s: %w; run node-remove again", name, err)
		}
		recordOn = nil
	default:
		var host remote
		if inDir {
			if host, err = dialNode(ctx, env, n); err != nil {
				if !unreachable {
					return fmt.Errorf("%w\nPass --unreachable to remove it from the cluster anyway, leaving k3s on it.", err)
				}
				host = nil
			} else {
				defer host.Close()
			}
		}
		var on orchestrator.Host
		if host != nil {
			on = orchestratorHost{host}
		}
		var last orchestrator.Step
		step := func(s orchestrator.Step) {
			last = s
			switch s {
			case orchestrator.Draining:
				say(env, "Draining %s...", name)
			case orchestrator.Uninstalling:
				say(env, "Uninstalling...")
			}
		}
		err := orch.RemoveNode(ctx, name, on, orchestrator.RemoveOptions{LoseVolumes: cmd.Bool("lose-volumes"), Unreachable: inDir && host == nil}, step)
		var held *orchestrator.VolumesError
		switch {
		case errors.As(err, &held):
			return fmt.Errorf("%w; pass --lose-volumes to remove it anyway", err)
		case err != nil && last == orchestrator.Uninstalling:
			return fmt.Errorf("%w; run node-remove again", err)
		case err != nil:
			return err
		}
		switch {
		case inDir && host == nil:
			fmt.Fprintf(env.Stderr, "Warning: %s can't be reached; if k3s still runs there, it registers again: run k3s-agent-uninstall.sh on it.\n", name)
		case !inDir:
			fmt.Fprintf(env.Stderr, "Warning: if k3s still runs on %s's host, it registers again: run k3s-agent-uninstall.sh there.\n", name)
		}
	}
	if inDir {
		if err := d.RemoveNode(name); err != nil {
			return err
		}
	}
	if err := op.finish(ctx, "node-remove "+name, recordOn); err != nil {
		return err
	}
	fmt.Fprintf(env.Stdout, "Removed %s from %s\n", name, d.Cluster.Ref)
	return nil
}

// findNode is the node called name in nodes, and whether there is one.
func findNode(nodes []clusterdir.Node, name string) (clusterdir.Node, bool) {
	i := slices.IndexFunc(nodes, func(n clusterdir.Node) bool { return n.Name == name })
	if i < 0 {
		return clusterdir.Node{}, false
	}
	return nodes[i], true
}

// lastNode is the refusal to remove the cluster's last node, name,
// without --yes.
func lastNode(name string) error {
	return fmt.Errorf("%s is the cluster's last node: removing it removes the cluster's k3s and everything running on it; pass --yes to remove it", name)
}

// removeWithoutControlPlane removes the node called name from the
// cluster directory d, whose control plane cp can't be reached: an
// agent, uninstalled if its host answers, or the control plane once it
// is the last node, with yes. It takes no lock.
func removeWithoutControlPlane(ctx context.Context, env Env, d *clusterdir.Dir, cp clusterdir.Node, name string, yes bool) error {
	op, ctx, err := begin(ctx, env, d.Path, nil, false)
	if err != nil {
		return err
	}
	defer op.release()
	if d, err = clusterdir.Open(d.Path); err != nil {
		return err
	}
	if err := sameControlPlane(d, cp); err != nil {
		return err
	}
	nodes, err := d.Nodes()
	if err != nil {
		return err
	}
	n, ok := findNode(nodes, name)
	if !ok {
		return fmt.Errorf("the cluster has no node %s", name)
	}
	if n.ControlPlane && len(nodes) > 1 {
		return fmt.Errorf("%s is the control plane, which goes last; remove the other %s first, with --unreachable", name, plural(len(nodes)-1, "node"))
	}
	if len(nodes) == 1 && !yes {
		return lastNode(name)
	}
	say(env, "Removing %s from %s", name, d.Cluster.Ref)
	if host, err := dialNode(ctx, env, n); err == nil {
		say(env, "Uninstalling...")
		err = uninstall(ctx, orchestratorOf(env, d.Cluster, cp.PrivateAddress, nil), host, n)
		host.Close()
		if err != nil {
			return fmt.Errorf("uninstalling from %s: %w; run node-remove again", name, err)
		}
	} else {
		fmt.Fprintf(env.Stderr, "Warning: %s can't be reached; if k3s still runs there, uninstall it there.\n", name)
	}
	if err := d.RemoveNode(name); err != nil {
		return err
	}
	if err := op.finish(ctx, "node-remove "+name, nil); err != nil {
		return err
	}
	fmt.Fprintf(env.Stdout, "Removed %s from %s\n", name, d.Cluster.Ref)
	return nil
}
