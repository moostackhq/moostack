package cli

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net/netip"
	"slices"
	"strings"
	"time"

	"github.com/urfave/cli/v3"

	"github.com/moostackhq/moostack/internal/clusterdir"
	"github.com/moostackhq/moostack/internal/interrupt"
	"github.com/moostackhq/moostack/internal/k3s"
	"github.com/moostackhq/moostack/internal/sshclient"
)

// undoTimeout bounds undoing a failed node-add.
const undoTimeout = 5 * time.Minute

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

	// Install, undoing it all if it fails.
	say(env, "Adding %s to %s (%s, %s, %d MiB, %s)", name, d.Cluster.Ref, role, facts.Architecture, facts.MemoryMiB, private.Addr)
	if err := install(ctx, env, d, cp, host, cpClient, node, token); err != nil {
		say(env, "Uninstalling...")
		undoCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), undoTimeout)
		defer cancel()
		if _, uerr := run(undoCtx, host, k3s.UninstallScript(role), ""); uerr != nil {
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

// install installs k3s on host as node, joining the control plane cp at
// cpClient with token, or, if node is the control plane, with token as
// its agent token, beside zot, Traefik's config, cert-manager and its
// issuer.
func install(ctx context.Context, env Env, d *clusterdir.Dir, cp clusterdir.Node, host, cpClient remote, node clusterdir.Node, token string) error {
	role := k3s.RoleOf(node.ControlPlane)
	kn := k3s.Node{Name: node.Name, PrivateAddress: node.PrivateAddress, Interface: node.Interface, PublicAddress: node.PublicAddress}
	registry := cp.PrivateAddress
	if role.ControlPlane {
		registry = node.PrivateAddress
	}
	files := k3s.Written{Node: kn, Role: role, MemoryMiB: node.MemoryMiB, GOGC: node.GOGC, GOMEMLIMIT: node.GOMEMLIMIT, ControlPlane: registry, Token: token, Email: d.Cluster.Email}.Files()
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
			if _, err := runStoppable(ctx, host, k3s.ZotScript(node.Architecture, node.PrivateAddress)); err != nil {
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
		_, err = runStoppable(ctx, host, wait)
		return err
	}
	// The node is Ready before k3s says it has started: the wait for
	// Ready ends the wait for the start, whose failure ends both.
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
