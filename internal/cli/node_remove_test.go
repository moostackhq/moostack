package cli_test

import (
	"context"
	"errors"
	"maps"
	"net"
	"os"
	"os/exec"
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
	nodeRemoveRefused(t, o, []string{"node3"}, "node3.invalid", "node3 stays in the cluster directory", "mooctl -C "+o.dir+" node remove node3 --unreachable.")
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
		if nodes := n.removedNodes(); !slices.Equal(nodes, []string{"node3"}) {
			t.Errorf("%s was told the removed nodes %v, want node3", name, nodes)
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
	r := nodeRemoveRefused(t, o, []string{"node3", "--unreachable"}, "node2: dial tcp: no route to host",
		"If node3, node2 can't be reached, remove them without reaching them with mooctl -C "+o.dir+" node remove node3 node2 --unreachable.")
	if strings.Contains(r.err.Error(), "node3: ") {
		t.Errorf("node remove node3 --unreachable still checked node3: %v", r.err)
	}
}

// A plain node remove of a lost node, with another lost, names the command
// that gets past both.
func TestNodeRemoveNamesEveryLostNodeInItsHint(t *testing.T) {
	o := newOperator(t, "op")
	preflightCluster(t, o)
	o.nodes["node2"] = &fakeNode{err: errors.New("dial tcp: no route to host")}
	o.nodes["node3"] = &fakeNode{err: errors.New("dial tcp: connection timed out")}
	nodeRemoveRefused(t, o, []string{"node3"}, "If node3, node2 can't be reached, remove them without reaching them with mooctl -C "+o.dir+" node remove node3 node2 --unreachable.")
}

// A node remove --unreachable that fails for another reason, with no
// other node lost, names no command.
func TestNodeRemoveOfSeveralHintsNothingForAFailureOfAnotherKind(t *testing.T) {
	o := newOperator(t, "op")
	preflightCluster(t, o)
	o.nodes["node1"] = healthyNode(5)
	r := o.run("node", "remove", "node2", "node3", "--unreachable")
	if r.err == nil {
		t.Fatal("node remove passed with node1 at a later generation")
	}
	if strings.Contains(r.err.Error(), "If node") {
		t.Errorf("node remove's error suggests the command that failed:\n%v", r.err)
	}
	wantContains(t, "node remove's error", r.err.Error(), "Pull the latest cluster directory, or pass --force")
}

func TestNodeRemoveOfSeveralNamesAnotherLostNodeInItsHint(t *testing.T) {
	o := newOperator(t, "op")
	preflightCluster(t, o)
	o.nodes["node1"] = &fakeNode{err: errors.New("dial tcp: no route to host")}
	nodeRemoveRefused(t, o, []string{"node2", "node3", "--unreachable"}, "If node2, node3, node1 can't be reached, remove them without reaching them with mooctl -C "+o.dir+" node remove node2 node3 node1 --unreachable.")
}

// The cluster directory outlives a lost only node: the hint's command
// takes the node out, and a node added then starts from the directory.
func TestALostOnlyNodeIsRemovedAndReplaced(t *testing.T) {
	o := newOperator(t, "op")
	source := statusCluster(t, o, "node1")
	o.nodes = map[string]*fakeNode{"node1": {err: errors.New("dial tcp: no route to host")}}
	r := nodeRemoveRefused(t, o, []string{"node1"}, "node remove node1 --unreachable")
	if want := "the preflight failed, so nothing changed:\n  node1: dial tcp: no route to host\nIf node1 can't be reached, remove it without reaching it with mooctl -C " + o.dir + " node remove node1 --unreachable."; r.err.Error() != want {
		t.Errorf("node remove's error is\n%s\nwant\n%s", r.err, want)
	}
	o.mustRun("node", "remove", "node1", "--unreachable")
	if got, want := o.removedIdentities(), []cluster.RemovedIdentity{{Node: "node1", Identity: identityOf("node1")}}; !slices.Equal(got, want) {
		t.Errorf("the directory lists the removed identities %v, want %v", got, want)
	}
	want := "Cluster prod on release 1.0.0\nOperation lock: free\nUpdate: none needed\n\nThe cluster has no nodes: add one with mooctl node add\n"
	if got := o.mustRun("--release-source", source, "cluster", "status").stdout; got != want {
		t.Errorf("cluster status of no nodes printed\n%s\nwant\n%s", got, want)
	}
	nodeAddPassesPreflight(t, o, source)
}

// A hint's command holds nothing that opens the release source: one with
// a user, a query or a form url.Parse refuses is left out.
func TestNodeRemovesHintLeavesOutAReleaseSourcesSecret(t *testing.T) {
	for name, source := range map[string]string{
		"user":       "https://op:s3cret@host.invalid/r",
		"bad escape": "https://op:s3cret%zz@host.invalid/r",
		"bad path":   "https://op:s3cret/x@host.invalid/r",
		"query":      "https://host.invalid/r?token=s3cret",
	} {
		t.Run(name, func(t *testing.T) {
			o := newOperator(t, "op")
			preflightCluster(t, o)
			o.nodes["node3"] = &fakeNode{err: errors.New("dial tcp: no route to host")}
			r := o.run("--release-source", source, "node", "remove", "node3")
			if r.err == nil {
				t.Fatal("node remove passed with node3 lost")
			}
			wantContains(t, "node remove's error", r.err.Error(), "node remove node3 --unreachable.")
			if strings.Contains(r.err.Error(), "s3cret") {
				t.Errorf("node remove's error shows the release source's secret:\n%v", r.err)
			}
		})
	}
}

// A release source that is a directory stays in the hint, whatever its
// name holds.
func TestNodeRemovesHintKeepsADirectorySource(t *testing.T) {
	for _, source := range []string{"rel?1", "100%/rel", "rel:eases"} {
		t.Run(source, func(t *testing.T) {
			o := newOperator(t, "op")
			preflightCluster(t, o)
			o.nodes["node3"] = &fakeNode{err: errors.New("dial tcp: no route to host")}
			r := o.run("--release-source", source, "node", "remove", "node3")
			if r.err == nil {
				t.Fatal("node remove passed with node3 lost")
			}
			wantContains(t, "node remove's error", r.err.Error(), "--release-source")
		})
	}
}

// The command of a hint, read back by a shell, runs: from a directory, a
// key and a release source with a space and a quote in them.
func TestNodeRemovesHintPastesBack(t *testing.T) {
	for _, shell := range []string{"sh", "zsh"} {
		t.Run(shell, func(t *testing.T) {
			if _, err := exec.LookPath(shell); err != nil {
				t.Skip(err)
			}
			o := newOperator(t, "op")
			o.dir = filepath.Join(t.TempDir(), "my 'cluster")
			if err := os.Mkdir(o.dir, 0o755); err != nil {
				t.Fatal(err)
			}
			preflightCluster(t, o)
			for _, n := range o.nodes {
				n.err = errors.New("dial tcp: no route to host")
			}
			key := filepath.Join(t.TempDir(), "ops key")
			writeFile(t, key, []byte("x"))
			source := filepath.Join(t.TempDir(), "rel eases")
			r := o.run("--release-source", source, "--ssh-key", key, "node", "remove", "node1", "--force")
			if r.err == nil {
				t.Fatal("node remove passed with node1 lost")
			}
			_, line, _ := strings.Cut(r.err.Error(), "without reaching them with mooctl ")
			out, err := exec.Command(shell, "-c", "printf '%s\\n' "+strings.TrimSuffix(line, ".")).Output()
			if err != nil {
				t.Fatal(err)
			}
			args := strings.Split(strings.TrimSuffix(string(out), "\n"), "\n")
			want := []string{"-C", o.dir, "--release-source", source, "--ssh-key", key, "node", "remove", "node1", "node2", "node3", "--unreachable", "--force"}
			if !slices.Equal(args, want) {
				t.Errorf("%s reads the hint as %q, want %q", shell, args, want)
			}
			if r := o.runWithoutDir(args...); r.err != nil {
				t.Fatalf("the hint's command failed: %v", r.err)
			}
			if got := o.removedIdentities(); len(got) != 3 {
				t.Errorf("the directory lists the removed identities %v, want all three", got)
			}
		})
	}
}

// A hint of another failure of the preflight stays beside node remove's.
func TestNodeRemovesHintKeepsThePreflightsHint(t *testing.T) {
	o := newOperator(t, "op")
	updateCluster(t, o)
	o.version, o.previous = "1.2.0", "1.1.0"
	o.nodes["node2"] = &fakeNode{err: errors.New("dial tcp: no route to host")}
	o.nodes["node3"] = &fakeNode{err: errors.New("dial tcp: no route to host")}
	r := o.run("node", "remove", "node3")
	if r.err == nil {
		t.Fatal("node remove passed with node3 lost")
	}
	wantContains(t, "node remove's error", r.err.Error(), "node remove node3 node2 --unreachable.", "Operate the cluster with the mooctl of the release it runs")
}

func TestForcedNodeRemoveUnreachableTakesEveryNode(t *testing.T) {
	o := newOperator(t, "op")
	source := preflightCluster(t, o)
	for _, n := range o.nodes {
		n.err = errors.New("dial tcp: no route to host")
	}
	o.mustRun("node", "remove", "node1", "node2", "node3", "--unreachable", "--force")
	nodeAddPassesPreflight(t, o, source)
}

func TestClusterStatusOfNodesThatDontAnswerDoesntCallThemNone(t *testing.T) {
	o := newOperator(t, "op")
	source := preflightCluster(t, o)
	for _, n := range o.nodes {
		n.err = errors.New("dial tcp: no route to host")
	}
	if r := o.mustRun("--release-source", source, "cluster", "status"); strings.Contains(r.stdout, "no nodes") {
		t.Errorf("cluster status of unreachable nodes says there are none:\n%s", r.stdout)
	}
}

func TestNodeRemoveUnreachableTakesEveryLostNode(t *testing.T) {
	o := newOperator(t, "op")
	preflightCluster(t, o)
	for _, n := range o.nodes {
		n.err = errors.New("dial tcp: no route to host")
	}
	nodeRemoveRefused(t, o, []string{"node3", "--unreachable"}, "node remove node3 node1 node2 --unreachable")
	o.mustRun("node", "remove", "node3", "node1", "node2", "--unreachable")
	if got := o.removedIdentities(); len(got) != 3 {
		t.Errorf("the directory lists the removed identities %v, want all three", got)
	}
}

// The hint's command runs as the operator's did: in its cluster directory,
// and with --force if it had it.
func TestNodeRemovesHintRunsAsTheOperatorRan(t *testing.T) {
	o := newOperator(t, "op")
	preflightCluster(t, o)
	o.nodes["node1"] = healthyNode(5)
	o.nodes["node2"] = &fakeNode{err: errors.New("dial tcp: no route to host")}
	o.nodes["node3"] = &fakeNode{err: errors.New("dial tcp: no route to host")}
	r := o.run("node", "remove", "node3", "--force")
	if r.err == nil {
		t.Fatal("node remove passed with node3 lost")
	}
	wantContains(t, "node remove's error", r.err.Error(), "with mooctl -C "+o.dir+" node remove node3 node2 --unreachable --force.")
	r = o.run("node", "remove", "node3", "--unreachable", "--force")
	if r.err == nil {
		t.Fatal("node remove passed with node2 lost")
	}
	wantContains(t, "node remove's error", r.err.Error(), "with mooctl -C "+o.dir+" node remove node3 node2 --unreachable --force.")
}

func TestNodeRemoveOfSeveralSaysThemOnce(t *testing.T) {
	o := newOperator(t, "op")
	preflightCluster(t, o)
	o.nodes["node1"].refuseRecording = true
	o.nodes["node2"] = &fakeNode{err: errors.New("dial tcp: no route to host")}
	o.nodes["node3"] = &fakeNode{err: errors.New("dial tcp: no route to host")}
	r := o.run("node", "remove", "node2", "node3", "--unreachable")
	if r.err == nil {
		t.Fatal("node remove passed with node1 refusing the recording")
	}
	wantContains(t, "node remove's error", r.err.Error(), "removed node2, node3, but the next operation has to finish telling these nodes")
}

func TestForcedNodeRemoveOfSeveralDoesntCallThemUnlisted(t *testing.T) {
	o := newOperator(t, "op")
	preflightCluster(t, o)
	later := healthyNode(5)
	later.status.Nodes = []string{"node1", "node2", "node3", "node4"}
	o.nodes["node1"] = later
	o.nodes["node2"] = &fakeNode{err: errors.New("dial tcp: no route to host")}
	o.nodes["node3"] = &fakeNode{err: errors.New("dial tcp: no route to host")}
	r := o.mustRun("node", "remove", "node2", "node3", "--unreachable", "--force")
	wantContains(t, "the forced node remove's warnings", r.stderr, "Generation 5 lists node4, which this cluster directory doesn't")
	if strings.Contains(r.stderr, "node2, ") || strings.Contains(r.stderr, "node3, ") {
		t.Errorf("the forced node remove calls a removed node unlisted:\n%s", r.stderr)
	}
}

// A removal of several cut short leaves a name gone and its identity
// listed.
func TestARerunOfSeveralSaysWhichIsGone(t *testing.T) {
	o := newOperator(t, "op")
	preflightCluster(t, o)
	o.writeRemoved(t, "node2")
	if err := os.Remove(filepath.Join(o.dir, "nodes", "node2.yml")); err != nil {
		t.Fatal(err)
	}
	nodeRemoveRefused(t, o, []string{"node2", "node3", "--unreachable"}, "node2 is removed already: leave it out")
	nodeRemoveRefused(t, o, []string{"node3", "node2", "--unreachable"}, "node2 is removed already: leave it out")
}

func TestARerunOfOneSaysItIsGone(t *testing.T) {
	o := newOperator(t, "op")
	preflightCluster(t, o)
	o.writeRemoved(t, "node2")
	if err := os.Remove(filepath.Join(o.dir, "nodes", "node2.yml")); err != nil {
		t.Fatal(err)
	}
	nodeRemoveRefused(t, o, []string{"node2", "--unreachable"}, "node2 is removed already: leave it out")
}

func TestARemovalOfSeveralItCantRecordNamesThem(t *testing.T) {
	o := newOperator(t, "op")
	preflightCluster(t, o)
	nodes := filepath.Join(o.dir, "nodes")
	if err := os.Chmod(nodes, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(nodes, 0o755) })
	r := o.run("node", "remove", "node2", "node3", "--unreachable")
	if r.err == nil {
		t.Fatal("node remove passed with the nodes directory read-only")
	}
	wantContains(t, "node remove's error", r.err.Error(), "recording that node2, node3 are removed: ")
}

// The hint of a host that can't be reached to clean it runs as the
// operator ran.
func TestNodeRemoveOfAHostItCantCleanHintsAsItRan(t *testing.T) {
	o := newOperator(t, "op")
	preflightCluster(t, o)
	nodeRemoveRefused(t, o, []string{"node3", "--force"}, "node3 stays in the cluster directory", "node remove node3 --unreachable --force.")
}

func TestANodeFileItCantReadIsNotCalledRemoved(t *testing.T) {
	o := newOperator(t, "op")
	preflightCluster(t, o)
	o.writeRemoved(t, "node2")
	if err := os.WriteFile(filepath.Join(o.dir, "nodes", "node2.yml"), []byte("name: ["), 0o644); err != nil {
		t.Fatal(err)
	}
	r := nodeRemoveRefused(t, o, []string{"node2", "--unreachable"}, "node2")
	if strings.Contains(r.err.Error(), "removed already") {
		t.Errorf("node remove calls a node whose file it can't read removed: %v", r.err)
	}
}

func TestCtrlCAsSeveralAreToldSaysTheyWereRemoved(t *testing.T) {
	o := newOperator(t, "op")
	source := preflightCluster(t, o)
	o.nodes["node1"].removing = func(push int) {
		if push == 2 {
			syscall.Kill(os.Getpid(), syscall.SIGINT)
			time.Sleep(200 * time.Millisecond)
		}
	}
	r := o.run("--release-source", source, "node", "remove", "node2", "node3", "--unreachable")
	if r.err == nil || !strings.Contains(r.err.Error(), "node remove interrupted once node2, node3 were removed, which the nodes were told") {
		t.Errorf("node remove interrupted as it told the nodes: %v", r.err)
	}
	wantContains(t, "node remove's stderr", r.stderr, "Interrupted, but node2, node3 are removed: mooctl tells the nodes first.")
}

func TestNodeRemoveUnreachableTakesSeveralNodes(t *testing.T) {
	o := newOperator(t, "op")
	preflightCluster(t, o)
	o.nodes["node2"] = &fakeNode{err: errors.New("dial tcp: no route to host")}
	o.nodes["node3"] = &fakeNode{err: errors.New("dial tcp: connection timed out")}
	r := o.mustRun("node", "remove", "node2", "node3", "--unreachable")
	wantContains(t, "node remove's output", r.stdout,
		"Removed node node2, at generation 2 of the cluster directory\n", "node2's host was left as it is",
		"Removed node node3, at generation 2 of the cluster directory\n", "node3's host was left as it is")
	if got, want := o.removedIdentities(), []cluster.RemovedIdentity{{Node: "node2", Identity: identityOf("node2")}, {Node: "node3", Identity: identityOf("node3")}}; !slices.Equal(got, want) {
		t.Errorf("the directory lists the removed identities %v, want %v", got, want)
	}
	n := o.nodes["node1"]
	if g := n.generations(); !slices.Equal(g, []int64{2}) {
		t.Errorf("node1 recorded the generations %v, want [2]", g)
	}
	if got := n.removedNodes(); !slices.Equal(got, []string{"node2", "node3"}) {
		t.Errorf("node1 was told the removed nodes %v, want node2 and node3", got)
	}
	var listed []string
	for _, l := range n.lastRecorded() {
		listed = append(listed, l.Name)
	}
	if !slices.Equal(listed, []string{"node1"}) {
		t.Errorf("node1 recorded the nodes %v, want node1 alone", listed)
	}
	wantReminder(t, "node remove", r, true)
}

// Each refusal comes before anything is reached.
func TestNodeRemoveRefusesTheNamesItCantTake(t *testing.T) {
	for name, c := range map[string]struct {
		args []string
		says string
	}{
		"none":                {[]string{"--unreachable"}, "takes the nodes to remove"},
		"none, plainly":       {nil, "takes the nodes to remove"},
		"several, reached":    {[]string{"node2", "node3"}, "node remove reaches one host at a time: give it one node, or several with --unreachable"},
		"one given twice":     {[]string{"node3", "node3", "--unreachable"}, "node3 is given twice"},
		"one it doesn't have": {[]string{"node3", "node9", "--unreachable"}, "node9"},
	} {
		t.Run(name, func(t *testing.T) {
			o := newOperator(t, "op")
			preflightCluster(t, o)
			nodeRemoveRefused(t, o, c.args, c.says)
			for name, n := range o.nodes {
				if n.dialed() != 0 {
					t.Errorf("%s was reached", name)
				}
			}
		})
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
	if nodes := o.nodes["node1"].removedNodes(); !slices.Equal(nodes, []string{"node3"}) {
		t.Errorf("after the next operation, node1 was told the removed nodes %v, want node3", nodes)
	}
}

// The operation after a removal half told finishes telling, though it
// changes nothing of the directory's.
func TestAnOperationBringsUpANodeThatMissedTheRecordingOfARemoval(t *testing.T) {
	o := newOperator(t, "op")
	statusCluster(t, o, "node1", "node2", "node3")
	o.nodes = map[string]*fakeNode{"node1": healthyNode(1), "node2": healthyNode(1), "node3": {err: errors.New("dial tcp: no route to host")}}
	o.nodes["node1"].refuseRecording = true
	r := o.run("node", "remove", "node3", "--unreachable")
	if r.err == nil {
		t.Fatal("node remove passed with node1 refusing the recording")
	}
	if want := "removed node3, but the next operation has to finish telling these nodes: node1 didn't record generation 2: recording the generation: the disk is full"; r.err.Error() != want {
		t.Errorf("node remove's error is\n%s\nwant\n%s", r.err, want)
	}
	o.nodes["node1"].refuseRecording = false
	next := o.run("deploy", "--remove", appDir(t, "name: absent\n"))
	if next.err != nil {
		t.Fatalf("deploy --remove: %v\n%s", next.err, next.stderr)
	}
	if want := "node1 was behind this cluster directory, and recorded its generation, 2.\n"; !strings.Contains(next.stderr, want) {
		t.Errorf("deploy --remove printed to stderr\n%s\nwant %q", next.stderr, want)
	}
	if g := o.nodes["node1"].generations(); !slices.Equal(g, []int64{2}) {
		t.Errorf("node1 recorded the generations %v, want [2]", g)
	}
	if g := o.nodes["node2"].generations(); !slices.Equal(g, []int64{2}) {
		t.Errorf("node2, told already, recorded the generations %v, want [2]", g)
	}
}

// node remove --unreachable leaves the node out of the preflight, whose
// nodes are then no generation of the directory's, and records one of its
// own.
func TestNodeRemoveUnreachableRecordsOnlyItsOwnGeneration(t *testing.T) {
	o := newOperator(t, "op")
	statusCluster(t, o, "node1", "node2", "node3")
	o.nodes = map[string]*fakeNode{"node1": healthyNode(0), "node2": healthyNode(1), "node3": {err: errors.New("dial tcp: no route to host")}}
	o.mustRun("node", "remove", "node3", "--unreachable")
	if g := o.nodes["node1"].generations(); !slices.Equal(g, []int64{2}) {
		t.Errorf("node1, behind, recorded the generations %v, want the removal's alone", g)
	}
}

// Every operation goes through the preflight, and a node that can't
// record stops none of them.
func TestANodeThatCantRecordStopsNoOperationAndIsNamed(t *testing.T) {
	o := newOperator(t, "op")
	statusCluster(t, o, "node1", "node2")
	o.nodes = map[string]*fakeNode{"node1": healthyNode(0), "node2": healthyNode(1)}
	o.nodes["node1"].refuseRecording = true
	r := o.run("deploy", "--remove", appDir(t, "name: absent\n"))
	if r.err != nil {
		t.Fatalf("deploy --remove: %v\n%s", r.err, r.stderr)
	}
	wantContains(t, "deploy --remove's stderr", r.stderr, "Going on, but node1 didn't record generation 1: recording the generation: the disk is full.")
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

// writeRemoved lists an identity of the node called name as removed in
// the cluster directory.
func (o *operator) writeRemoved(t *testing.T, name string) {
	t.Helper()
	entry := "- node: " + name + "\n  identity: " + identityOf(name) + "\n"
	if err := os.WriteFile(filepath.Join(o.dir, cluster.RemovedFileName), []byte(entry), 0o600); err != nil {
		t.Fatal(err)
	}
}
