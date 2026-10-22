package cli_test

import (
	"context"
	"errors"
	"maps"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/cluster"
	"github.com/moostackhq/moostack/internal/nodeclient"
)

// nodeRemoveRefused runs node remove with args, which must fail with an
// error containing each of want, without changing the cluster directory,
// recording a generation or removing an identity.
func nodeRemoveRefused(t *testing.T, o *operator, args []string, want ...string) result {
	t.Helper()
	before := snapshot(t, o.dir)
	r := o.run(append([]string{"node", "remove"}, args...)...)
	if r.err == nil {
		t.Fatal("node remove succeeded")
	}
	wantContains(t, "node remove's error", r.err.Error(), want...)
	if after := snapshot(t, o.dir); !maps.Equal(before, after) {
		t.Error("the refused node remove changed the cluster directory")
	}
	wantReminder(t, "the refused node remove", r, false)
	for name, n := range o.nodes {
		if g := n.generations(); len(g) > 0 {
			t.Errorf("%s recorded the generations %v", name, g)
		}
		if ids := n.removedIdentities(); len(ids) > 0 {
			t.Errorf("%s holds the identities %v as removed", name, ids)
		}
		if l := n.heldLock(); l != nil && l.Lease != bobsLock.Lease {
			t.Errorf("after the refused node remove, %s has the lock %+v", name, l)
		}
	}
	return r
}

// removedIdentities are the identities the operator's cluster directory
// lists as removed.
func (o *operator) removedIdentities() []cluster.RemovedIdentity {
	o.t.Helper()
	d, err := cluster.Open(o.dir)
	if err != nil {
		o.t.Fatal(err)
	}
	removed, err := d.RemovedIdentities()
	if err != nil {
		o.t.Fatal(err)
	}
	return removed
}

func TestNodeRemoveRefusesAnUnknownNode(t *testing.T) {
	o := newOperator(t, "op")
	preflightCluster(t, o)
	nodeRemoveRefused(t, o, []string{"node4"}, "the cluster has no node called node4")
	for name, n := range o.nodes {
		if n.dialed() > 0 {
			t.Errorf("node remove of an unknown node reached %s", name)
		}
	}
}

func TestNodeRemoveRefusesAnyNodeThatFailsThePreflight(t *testing.T) {
	o := newOperator(t, "op")
	preflightCluster(t, o)
	o.nodes["node2"] = &fakeNode{err: errors.New("dial tcp: no route to host")}
	r := nodeRemoveRefused(t, o, []string{"node3"}, "node2: dial tcp: no route to host")
	if strings.Contains(r.err.Error(), "--unreachable") {
		t.Errorf("node remove of node3 suggests --unreachable for node2: %v", r.err)
	}
}

func TestNodeRemoveOfAnUnreachableNodeSuggestsUnreachable(t *testing.T) {
	o := newOperator(t, "op")
	preflightCluster(t, o)
	o.nodes["node3"] = &fakeNode{err: errors.New("dial tcp: no route to host")}
	nodeRemoveRefused(t, o, []string{"node3"}, "node3: dial tcp: no route to host", "node remove node3 --unreachable")
}

func TestNodeRemoveSuggestsUnreachableOnlyForANodeThatCantBeReached(t *testing.T) {
	for name, n := range map[string]*fakeNode{
		"locked by another operation": healthyNode(1).locked(bobsLock),
		"in control plane mode":       {status: nodeclient.Status{Mode: nodev1.Mode_MODE_CONTROL_PLANE, Generation: 1}},
	} {
		t.Run(name, func(t *testing.T) {
			o := newOperator(t, "op")
			preflightCluster(t, o)
			o.nodes["node3"] = n
			r := nodeRemoveRefused(t, o, []string{"node3"}, "node3: ")
			if strings.Contains(r.err.Error(), "--unreachable") {
				t.Errorf("node remove of a node that can be reached suggests --unreachable: %v", r.err)
			}
		})
	}
	o := newOperator(t, "op")
	preflightCluster(t, o)
	o.nodes["node3"] = &fakeNode{status: nodeclient.Status{Mode: nodev1.Mode_MODE_DIRECT, AgentErr: errors.New("connection refused")}}
	nodeRemoveRefused(t, o, []string{"node3"}, "the agent doesn't answer", "node remove node3 --unreachable")
}

func TestForcedNodeRemoveUnreachableDoesntCallTheSkippedNodeUnlisted(t *testing.T) {
	o := newOperator(t, "op")
	preflightCluster(t, o)
	later := healthyNode(5)
	later.status.Nodes = []string{"node1", "node2", "node3", "node4"}
	o.nodes["node2"] = later
	o.nodes["node3"] = &fakeNode{err: errors.New("dial tcp: no route to host")}
	r := o.mustRun("node", "remove", "node3", "--unreachable", "--force")
	wantContains(t, "the forced node remove's warnings", r.stderr, "Generation 5 lists node4, which this cluster directory doesn't")
}

func TestNodeRemoveCleansTheHostBeforeChangingTheClusterDirectory(t *testing.T) {
	o := newOperator(t, "op")
	preflightCluster(t, o)
	// node3's node API answers, but its host can't be reached over SSH to
	// clean it.
	nodeRemoveRefused(t, o, []string{"node3"}, "node3.invalid", "node3 stays in the cluster directory")
}

func TestNodeRemoveUnreachableRemovesTheNodeWithoutReachingIt(t *testing.T) {
	o := newOperator(t, "op")
	preflightCluster(t, o)
	gone := &fakeNode{err: errors.New("dial tcp: no route to host")}
	o.nodes["node3"] = gone
	r := o.mustRun("node", "remove", "node3", "--unreachable")
	if gone.dialed() > 0 {
		t.Error("node remove --unreachable reached node3")
	}
	if _, err := os.Stat(filepath.Join(o.dir, "nodes", "node3.yml")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("node3's node file: %v, want it gone", err)
	}
	if got, want := o.removedIdentities(), []cluster.RemovedIdentity{{Node: "node3", Identity: identityOf("node3")}}; !slices.Equal(got, want) {
		t.Errorf("the cluster directory lists the removed identities %v, want %v", got, want)
	}
	d, err := cluster.Open(o.dir)
	if err != nil {
		t.Fatal(err)
	}
	if d.Cluster.Generation != 2 {
		t.Errorf("the cluster directory is at generation %d, want 2", d.Cluster.Generation)
	}
	for _, name := range []string{"node1", "node2"} {
		n := o.nodes[name]
		if g := n.generations(); !slices.Equal(g, []int64{2}) {
			t.Errorf("%s recorded the generations %v, want [2]", name, g)
		}
		if ids := n.removedIdentities(); !slices.Equal(ids, []string{identityOf("node3")}) {
			t.Errorf("%s holds the identities %v as removed, want node3's", name, ids)
		}
		if l := n.heldLock(); l != nil {
			t.Errorf("after node remove, %s has the lock %+v", name, l)
		}
	}
	wantContains(t, "node remove's output", r.stdout, "Removed node node3, at generation 2 of the cluster directory", "node3's host was left as it is")
	wantReminder(t, "node remove", r, true)
}

func TestNodeRemoveUnreachableSkipsOnlyTheNamedNode(t *testing.T) {
	o := newOperator(t, "op")
	preflightCluster(t, o)
	o.nodes["node2"] = &fakeNode{err: errors.New("dial tcp: no route to host")}
	o.nodes["node3"] = &fakeNode{err: errors.New("dial tcp: connection timed out")}
	r := nodeRemoveRefused(t, o, []string{"node3", "--unreachable"}, "node2: dial tcp: no route to host")
	if strings.Contains(r.err.Error(), "node3") {
		t.Errorf("node remove node3 --unreachable still checked node3: %v", r.err)
	}
}

func TestEveryOperationPushesTheRemovedIdentitiesToANodeThatMissedThem(t *testing.T) {
	o := newOperator(t, "op")
	source := statusCluster(t, o, "node1", "node2", "node3")
	o.nodes = map[string]*fakeNode{"node1": healthyNode(1), "node2": healthyNode(1), "node3": {err: errors.New("dial tcp: no route to host")}}
	o.nodes["node1"].refuseNewRemovals = true
	r := o.run("node", "remove", "node3", "--unreachable")
	if r.err == nil {
		t.Fatal("node remove passed with node1 refusing the removed identity")
	}
	wantContains(t, "node remove's error", r.err.Error(), "removed node3", "node1 didn't take the removed identities")
	if strings.Contains(r.err.Error(), "node2") {
		t.Errorf("node remove's error names node2, which took them: %v", r.err)
	}
	// The cluster directory changed all the same.
	wantReminder(t, "node remove that node1 missed", r, true)
	if ids := o.nodes["node2"].removedIdentities(); !slices.Equal(ids, []string{identityOf("node3")}) {
		t.Errorf("node2 holds the identities %v as removed, want node3's", ids)
	}

	// The next operation pushes them to node1 before it changes
	// anything.
	o.nodes["node1"].refuseNewRemovals = false
	nodeAddPassesPreflight(t, o, source)
	if ids := o.nodes["node1"].removedIdentities(); !slices.Equal(ids, []string{identityOf("node3")}) {
		t.Errorf("after the next operation, node1 holds the identities %v as removed, want node3's", ids)
	}
}

func TestCtrlCStopsNodeRemoveBeforeItsUninstallAndGivesBackTheLock(t *testing.T) {
	interrupt := func(ctx context.Context) {
		syscall.Kill(os.Getpid(), syscall.SIGINT)
		<-ctx.Done()
	}
	for name, c := range map[string]struct {
		set  func(o *operator)
		want string
	}{
		"the preflight": {func(o *operator) { o.nodes["node2"].dialing = interrupt }, "node remove interrupted"},
		// node3's host accepts the connection and never answers.
		"reaching the host": {func(o *operator) {
			l, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { l.Close() })
			go func() {
				for {
					c, err := l.Accept()
					if err != nil {
						return
					}
					t.Cleanup(func() { c.Close() })
					syscall.Kill(os.Getpid(), syscall.SIGINT)
				}
			}()
			_, port, _ := net.SplitHostPort(l.Addr().String())
			writeFile(t, filepath.Join(o.home, ".ssh", "config"), []byte("Host node3.invalid\n  HostName 127.0.0.1\n  Port "+port+"\n"))
		}, "reaching node3's host to clean it, which stays in the cluster directory: interrupted"},
	} {
		t.Run(name, func(t *testing.T) {
			o := newOperator(t, "op")
			source := preflightCluster(t, o)
			c.set(o)
			before := snapshot(t, o.dir)
			r := o.run("--release-source", source, "node", "remove", "node3")
			if r.err == nil {
				t.Fatal("node remove succeeded")
			}
			wantContains(t, "node remove's error", r.err.Error(), c.want)
			if got := o.exitCodes(); len(got) > 0 {
				t.Errorf("mooctl exited with %v, want it to return", got)
			}
			for name, n := range o.nodes {
				if l := n.heldLock(); l != nil {
					t.Errorf("after the interrupted node remove, %s holds the lock %+v", name, l)
				}
			}
			wantSnapshot(t, o.dir, before)
		})
	}
}

func TestCtrlCOnceTheLockIsHeldStopsNodeRemoveUnreachableBeforeAnythingChanges(t *testing.T) {
	o := newOperator(t, "op")
	source := preflightCluster(t, o)
	// The signal comes once node1 has taken the removed identities the
	// preflight pushes, before it answers.
	o.nodes["node1"].pushed = func(ctx context.Context, push int) {
		if push == 1 {
			syscall.Kill(os.Getpid(), syscall.SIGINT)
			<-ctx.Done()
		}
	}
	before := snapshot(t, o.dir)
	r := o.run("--release-source", source, "node", "remove", "node3", "--unreachable")
	if r.err == nil || !strings.Contains(r.err.Error(), "interrupted before removing node3") {
		t.Fatalf("node remove --unreachable interrupted once the lock is held: %v, want it to stop before removing node3", r.err)
	}
	wantSnapshot(t, o.dir, before)
	if got := o.exitCodes(); len(got) > 0 {
		t.Errorf("mooctl exited with %v, want it to return", got)
	}
	for name, n := range o.nodes {
		if l := n.heldLock(); l != nil {
			t.Errorf("after the interrupted node remove, %s holds the lock %+v", name, l)
		}
	}
}

func TestCtrlCOnceANodeIsRemovedStillTellsTheNodes(t *testing.T) {
	o := newOperator(t, "op")
	source := preflightCluster(t, o)
	// The signal comes as node remove starts to tell the nodes that node3
	// is removed, their second push, and every node's push waits for it
	// to land before it finds whether its context has ended.
	for name, n := range o.nodes {
		n.removing = func(push int) {
			if push != 2 {
				return
			}
			if name == "node1" {
				syscall.Kill(os.Getpid(), syscall.SIGINT)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
	r := o.run("--release-source", source, "node", "remove", "node3", "--unreachable")
	if r.err == nil || !strings.Contains(r.err.Error(), "node remove interrupted once node3 was removed, which the nodes were told") {
		t.Errorf("node remove interrupted as it told the nodes: %v, want it to say so once they were told", r.err)
	}
	wantContains(t, "node remove's stderr", r.stderr, "Interrupted, but node3 is removed: mooctl tells the nodes first. Interrupt again to stop now")
	for _, name := range []string{"node1", "node2"} {
		n := o.nodes[name]
		if !slices.Contains(n.removedIdentities(), identityOf("node3")) {
			t.Errorf("%s holds the removed identities %v, want node3's", name, n.removedIdentities())
		}
		if !slices.Contains(n.generations(), 2) {
			t.Errorf("%s recorded %v, want generation 2", name, n.generations())
		}
	}
	if got := o.exitCodes(); len(got) > 0 {
		t.Errorf("mooctl exited with %v, want it to return", got)
	}
}
