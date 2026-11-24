package cli

import (
	"cmp"
	"context"
	"crypto/rand"
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
	"github.com/moostackhq/moostack/internal/k3s"
	"github.com/moostackhq/moostack/internal/registry"
	"github.com/moostackhq/moostack/internal/release"
	"github.com/moostackhq/moostack/internal/sshclient"
)

// undoTimeout bounds undoing a failed node-add.
const undoTimeout = 5 * time.Minute

// uncordonTimeout bounds putting a node back after a failed drain.
const uncordonTimeout = 30 * time.Second

// appLabel is the label that marks an app's namespace with its name.
const appLabel = "moostack.dev/app"

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
	var known []string
	if hasCP {
		if known, err = kubeNodeNames(ctx, cpClient); err != nil {
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

	// Preflight.
	role := k3s.RoleOf(!hasCP)
	out, err := run(ctx, host, k3s.FactsScript, "")
	if err != nil {
		return fmt.Errorf("%s: %w", target, err)
	}
	facts, err := k3s.ParseFacts(out)
	if err != nil {
		return fmt.Errorf("%s: %w", target, err)
	}
	if _, err := k3s.InstallScript(d.Cluster.K3s, role); err != nil {
		return err
	}
	if err := facts.Check(role); err != nil {
		return fmt.Errorf("%s can't be a %s: %w", target, role, err)
	}
	private, err := facts.PrivateAddress(override)
	if err != nil {
		return fmt.Errorf("%s: %w", target, err)
	}
	gogc, gomemlimit := k3s.Memory(facts.MemoryMiB, role)
	var token string
	if hasCP {
		if _, err := run(ctx, host, k3s.ReachScript(cp.PrivateAddress), ""); err != nil {
			return fmt.Errorf("%s: %w", target, err)
		}
		v, err := run(ctx, cpClient, k3s.VersionScript, "")
		if err != nil {
			return fmt.Errorf("reading k3s's version on %s: %w", cp.Name, err)
		}
		if v = strings.TrimSpace(v); v != d.Cluster.K3s {
			return fmt.Errorf("%s runs k3s %s, and the cluster file pins %s", cp.Name, v, d.Cluster.K3s)
		}
		out, err := run(ctx, cpClient, "cat "+k3s.AgentTokenFile, "")
		if err != nil {
			return fmt.Errorf("reading the join token on %s: %w", cp.Name, err)
		}
		token = strings.TrimSpace(out)
	} else {
		token = rand.Text()
	}
	node := clusterdir.Node{
		Name:           name,
		Target:         target,
		HostKey:        key,
		PrivateAddress: private.Addr,
		Interface:      private.Interface,
		PublicAddress:  facts.PublicAddress(),
		Architecture:   facts.Architecture,
		MemoryMiB:      facts.MemoryMiB,
		GOGC:           gogc,
		GOMEMLIMIT:     gomemlimit,
		ControlPlane:   role.ControlPlane,
	}

	say(env, "Adding %s to %s (%s, %s, %d MiB, %s)", name, d.Cluster.Ref, role, facts.Architecture, facts.MemoryMiB, private.Addr)
	var console string
	if role.ControlPlane {
		say(env, "Downloading the console...")
		dir, err := os.MkdirTemp("", "mooctl-console-")
		if err != nil {
			return err
		}
		defer os.RemoveAll(dir)
		client := &http.Client{Timeout: 10 * time.Minute}
		if console, err = release.Fetch(ctx, client, cmp.Or(env.Release, release.URL), "console-linux-"+facts.Architecture+".tar", dir); err != nil {
			return err
		}
	}

	// Install, undoing it all if it fails.
	if err := install(ctx, env, d, cp, host, cpClient, node, token, console); err != nil {
		say(env, "Uninstalling...")
		undoCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), undoTimeout)
		defer cancel()
		if _, uerr := run(undoCtx, host, uninstallScript(role), ""); uerr != nil {
			err = errors.Join(err, fmt.Errorf("uninstalling from %s: %w", target, uerr))
		}
		if hasCP {
			if _, uerr := run(undoCtx, cpClient, "k3s kubectl delete node "+sshclient.ShellQuote(name)+" --ignore-not-found --request-timeout=60s", ""); uerr != nil {
				err = errors.Join(err, uerr)
			}
		}
		return err
	}
	if err := d.WriteNode(node); err != nil {
		return err
	}
	recordOn := cpClient
	if role.ControlPlane {
		recordOn = host
	}
	if err := op.finish(ctx, "node-add "+name, remoteHost{recordOn}); err != nil {
		return err
	}
	fmt.Fprintf(env.Stdout, "Added %s to %s\n", name, d.Cluster.Ref)
	if hasCP && !slices.ContainsFunc(nodes, func(n clusterdir.Node) bool { return n.Architecture == node.Architecture }) {
		if apps, err := run(ctx, cpClient, "k3s kubectl get namespaces -l "+appLabel+" -o name --request-timeout=30s", ""); err == nil && strings.TrimSpace(apps) != "" {
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

// kubeNodeNames are the names of the cluster's Kubernetes nodes, read on
// its control plane cp.
func kubeNodeNames(ctx context.Context, cp remote) ([]string, error) {
	out, err := run(ctx, cp, k3s.NodeNamesScript, "")
	if err != nil {
		return nil, fmt.Errorf("listing the cluster's nodes: %w", err)
	}
	var names []string
	for _, line := range strings.Fields(out) {
		names = append(names, strings.TrimPrefix(line, "node/"))
	}
	return names, nil
}

// freeName is the first of node1, node2 and so on that neither a node of
// nodes nor a Kubernetes node of known has.
func freeName(nodes []clusterdir.Node, known []string) string {
	for i := 1; ; i++ {
		name := fmt.Sprintf("node%d", i)
		if !slices.Contains(known, name) && !slices.ContainsFunc(nodes, func(n clusterdir.Node) bool { return n.Name == name }) {
			return name
		}
	}
}

// uninstallScript removes k3s from a node of role, and from the control
// plane the registry and the applied commit as well, and fails if k3s's
// uninstall fails.
func uninstallScript(role k3s.Role) string {
	script := k3s.UninstallScript(role)
	if !role.ControlPlane {
		return script
	}
	return "(\n" + script + "\n)\ns=$?\n" + registry.ZotUninstallScript + "\nrm -f " + AppliedCommitFile + "\nrmdir " + path.Dir(AppliedCommitFile) + " 2>/dev/null\nexit $s"
}

// install installs k3s on host as node, joining the control plane cp at
// cpClient with token, or, if node is the control plane, with token as
// its agent token, beside zot, Traefik's config, cert-manager, its
// issuer and the console, whose image archive is at console.
func install(ctx context.Context, env Env, d *clusterdir.Dir, cp clusterdir.Node, host, cpClient remote, node clusterdir.Node, token, console string) error {
	role := k3s.RoleOf(node.ControlPlane)
	kn := k3s.Node{Name: node.Name, PrivateAddress: node.PrivateAddress, Interface: node.Interface, PublicAddress: node.PublicAddress}
	cpAddr := cp.PrivateAddress
	if role.ControlPlane {
		cpAddr = node.PrivateAddress
	}
	files := k3s.Written{Node: kn, Role: role, MemoryMiB: node.MemoryMiB, GOGC: node.GOGC, GOMEMLIMIT: node.GOMEMLIMIT, ControlPlane: cpAddr, Token: token, Registry: registry.Name, RegistryPort: registry.Port, Email: d.Cluster.Email}.Files()
	for _, f := range files {
		if err := upload(ctx, host, f.Path, f.Content, 0o600); err != nil {
			return fmt.Errorf("writing %s: %w", f.Path, err)
		}
	}
	script, err := k3s.InstallScript(d.Cluster.K3s, role)
	if err != nil {
		return err
	}
	zot := make(chan error, 1)
	if role.ControlPlane {
		say(env, "Installing k3s and zot...")
		go func() {
			if _, err := runStoppable(ctx, host, registry.ZotScript(node.Architecture, node.PrivateAddress, k3s.DownloadedScript)); err != nil {
				zot <- fmt.Errorf("installing zot: %w", err)
				return
			}
			zot <- nil
		}()
	} else {
		say(env, "Installing k3s...")
		zot <- nil
	}
	if _, err = runStoppable(ctx, host, script); err != nil {
		err = fmt.Errorf("installing k3s: %w", err)
	}
	if err := errors.Join(err, <-zot); err != nil {
		return err
	}
	say(env, "Waiting for %s to be Ready...", node.Name)
	wait := k3s.WaitReadyScript(node.Name, node.PrivateAddress)
	if role.ControlPlane {
		if _, err = runStoppable(ctx, host, wait); err != nil {
			return err
		}
		return installConsole(ctx, env, host, node, console)
	}
	// The wait for Ready ends the wait for the start, whose failure ends
	// both.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	started, ready := make(chan error, 1), make(chan error, 1)
	go func() {
		_, err := runStoppable(ctx, host, k3s.StartScript)
		started <- err
	}()
	go func() {
		_, err := runStoppable(ctx, cpClient, wait)
		ready <- err
	}()
	select {
	case err := <-ready:
		return err
	case err := <-started:
		if err != nil {
			cancel()
			<-ready
			return fmt.Errorf("starting k3s: %w", err)
		}
		return <-ready
	}
}

// installConsole pushes the console's image, the archive at console, to
// the registry on node, the control plane, at host, and runs it.
func installConsole(ctx context.Context, env Env, host remote, node clusterdir.Node, console string) error {
	say(env, "Installing the console...")
	digest, err := registry.Push(ctx, map[string]string{node.Architecture: console}, registryAddr(node), k3s.ConsoleRepository, host.DialTCP, nil)
	if err != nil {
		return fmt.Errorf("the console's image: %w", err)
	}
	return runConsole(ctx, host, node, digest)
}

// runConsole has k3s on node, the control plane, at host, apply the
// console's objects with its image of digest, which is in the node's
// registry, and waits for the console to run it and be ready. It then
// marks the image as the one that runs.
func runConsole(ctx context.Context, host remote, node clusterdir.Node, digest string) error {
	addresses := []netip.Addr{node.PrivateAddress}
	if node.PublicAddress.IsValid() {
		addresses = []netip.Addr{node.PublicAddress, node.PrivateAddress}
	}
	if err := upload(ctx, host, k3s.AutoDeployFile("console"), k3s.Console(registry.Image(k3s.ConsoleRepository, digest), addresses), 0o600); err != nil {
		return fmt.Errorf("writing the console's objects: %w", err)
	}
	for deadline := time.Now().Add(consoleAppliedTimeout); ; {
		image, err := run(ctx, host, k3s.ConsoleImageScript, "")
		if err == nil && strings.TrimSpace(image) == registry.Image(k3s.ConsoleRepository, digest) {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("the console's objects weren't applied: %w", cmp.Or(err, fmt.Errorf("the console runs %s", strings.TrimSpace(image))))
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-time.After(2 * time.Second):
		}
	}
	if _, err := runStoppable(ctx, host, k3s.ConsoleReadyScript); err != nil {
		return fmt.Errorf("the console isn't ready: %w", err)
	}
	if err := registry.Tag(ctx, registryAddr(node), k3s.ConsoleRepository, digest, registry.LiveTag, host.DialTCP); err != nil {
		return fmt.Errorf("the console's image: %w", err)
	}
	return nil
}

// consoleAppliedTimeout is how long k3s gets to apply the console's
// objects.
const consoleAppliedTimeout = 2 * time.Minute

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
	known, err := kubeNodeNames(ctx, cpClient)
	if err != nil && !lastControlPlane {
		return err
	}
	inKube := slices.Contains(known, name)
	if !inDir && !inKube {
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
		if _, err := run(ctx, cpClient, uninstallScript(k3s.ControlPlane), ""); err != nil {
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
		// A node whose host can't be reached isn't drained: its pods go
		// with its Node.
		if inKube && (host != nil || !inDir) {
			if err := drain(ctx, env, cpClient, name, cmd.Bool("lose-volumes")); err != nil {
				return err
			}
		}
		if host != nil {
			say(env, "Uninstalling...")
			if _, err := run(ctx, host, uninstallScript(k3s.Agent), ""); err != nil {
				return fmt.Errorf("uninstalling from %s: %w; run node-remove again", name, err)
			}
		}
		if inKube {
			if _, err := run(ctx, cpClient, "k3s kubectl delete node "+sshclient.ShellQuote(name)+" --ignore-not-found --request-timeout=60s", ""); err != nil {
				return fmt.Errorf("deleting node %s: %w", name, err)
			}
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

// drain drains the Kubernetes node called name on the control plane cp,
// refusing, unless loseVolumes, a node that holds local volumes.
func drain(ctx context.Context, env Env, cp remote, name string, loseVolumes bool) error {
	q := sshclient.ShellQuote(name)
	if !loseVolumes {
		out, err := run(ctx, cp, `k3s kubectl get pv --request-timeout=30s -o jsonpath='{range .items[*]}{.metadata.name} {.spec.nodeAffinity.required.nodeSelectorTerms[*].matchExpressions[*].values[*]}{"\n"}{end}'`, "")
		if err != nil {
			return fmt.Errorf("listing volumes: %w", err)
		}
		var held []string
		for _, line := range strings.Split(out, "\n") {
			if f := strings.Fields(line); len(f) > 1 && slices.Contains(f[1:], name) {
				held = append(held, f[0])
			}
		}
		if len(held) > 0 {
			return fmt.Errorf("%s holds the volumes %s, which go with it; pass --lose-volumes to remove it anyway", name, strings.Join(held, ", "))
		}
	}
	say(env, "Draining %s...", name)
	if _, err := run(ctx, cp, "k3s kubectl drain "+q+" --ignore-daemonsets --delete-emptydir-data --timeout=180s", ""); err != nil {
		uctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), uncordonTimeout)
		defer cancel()
		if _, uerr := run(uctx, cp, "k3s kubectl uncordon "+q+" --request-timeout=20s", ""); uerr != nil {
			err = errors.Join(err, fmt.Errorf("uncordoning %s: %w", name, uerr))
		}
		return fmt.Errorf("draining %s: %w", name, err)
	}
	return nil
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
		_, err = run(ctx, host, uninstallScript(k3s.RoleOf(n.ControlPlane)), "")
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
