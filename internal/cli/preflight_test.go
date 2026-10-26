package cli_test

import (
	"context"
	"errors"
	"maps"
	"strings"
	"sync"
	"testing"
	"time"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/nodeclient"
)

// preflightCluster makes the operator's cluster directory a cluster of
// node1, node2 and node3, each answering as healthyNode(1), and returns
// the release source.
func preflightCluster(t *testing.T, o *operator) string {
	t.Helper()
	source := statusCluster(t, o, "node1", "node2", "node3")
	o.nodes = map[string]*fakeNode{"node1": healthyNode(1), "node2": healthyNode(1), "node3": healthyNode(1)}
	return source
}

// nodeAddRefused runs node add, which must fail in the preflight with an
// error containing each of want, before reaching the new host and
// without changing the cluster directory or recording a generation.
func nodeAddRefused(t *testing.T, o *operator, source string, want ...string) {
	t.Helper()
	before := snapshot(t, o.dir)
	r := o.run("--release-source", source, "node", "add", unreachable)
	if r.err == nil {
		t.Fatal("node add succeeded")
	}
	wantContains(t, "node add's error", r.err.Error(), want...)
	if strings.Contains(r.err.Error(), unreachable) {
		t.Errorf("node add reached the new host after a failed preflight: %v", r.err)
	}
	if after := snapshot(t, o.dir); !maps.Equal(before, after) {
		t.Error("the refused node add changed the cluster directory")
	}
	wantReminder(t, "the refused node add", r, false)
	for name, n := range o.nodes {
		if g := n.generations(); len(g) > 0 {
			t.Errorf("%s recorded the generations %v", name, g)
		}
	}
}

// nodeAddPassesPreflight runs node add with args, whose preflight must
// pass, so that it fails reaching the new host, and returns what it
// printed.
func nodeAddPassesPreflight(t *testing.T, o *operator, source string, args ...string) result {
	t.Helper()
	r := o.run(append([]string{"--release-source", source, "node", "add", unreachable}, args...)...)
	if r.err == nil || !strings.Contains(r.err.Error(), unreachable) {
		t.Errorf("node add: %v, want it to pass the preflight and fail reaching %s", r.err, unreachable)
	}
	wantReminder(t, "node add that failed reaching the new host", r, false)
	return r
}

func TestNodeAddPassesThePreflightOfAClusterItCanOperate(t *testing.T) {
	o := newOperator(t, "op")
	source := preflightCluster(t, o)
	nodeAddPassesPreflight(t, o, source)
}

func TestThePreflightRefusesAnUnreachableNode(t *testing.T) {
	o := newOperator(t, "op")
	source := preflightCluster(t, o)
	o.nodes["node2"] = &fakeNode{err: errors.New("dial tcp: no route to host")}
	nodeAddRefused(t, o, source, "node2: dial tcp: no route to host")
}

func TestThePreflightRefusesANodeWhoseAgentDoesntAnswer(t *testing.T) {
	o := newOperator(t, "op")
	source := preflightCluster(t, o)
	o.nodes["node3"] = &fakeNode{status: nodeclient.Status{AgentErr: errors.New("connection refused")}}
	nodeAddRefused(t, o, source, "node3", "the agent doesn't answer", "connection refused")
}

func TestThePreflightRefusesANodeInControlPlaneMode(t *testing.T) {
	o := newOperator(t, "op")
	source := preflightCluster(t, o)
	o.nodes["node1"] = &fakeNode{status: nodeclient.Status{Mode: nodev1.Mode_MODE_CONTROL_PLANE, Generation: 1}}
	nodeAddRefused(t, o, source, "node1", "control plane mode", "agent detach")
}

// A node whose agent is detached, but whose updater still serves the
// control plane, as for a second after a detach, isn't operated either.
func TestThePreflightRefusesANodeWhoseUpdaterIsInControlPlaneMode(t *testing.T) {
	o := newOperator(t, "op")
	source := preflightCluster(t, o)
	n := healthyNode(1)
	n.status.UpdaterMode = nodev1.Mode_MODE_CONTROL_PLANE
	o.nodes["node2"] = n
	nodeAddRefused(t, o, source, "node2", "its updater is in control plane mode")
	// An updater of a release before it said its mode is operated.
	n.status.UpdaterMode = nodev1.Mode_MODE_UNSPECIFIED
	nodeAddPassesPreflight(t, o, source)
}

func TestThePreflightRefusesANodeThatAppliedALaterGeneration(t *testing.T) {
	o := newOperator(t, "op")
	source := preflightCluster(t, o)
	o.nodes["node2"] = healthyNode(5)
	nodeAddRefused(t, o, source, "node2", "generation 5", "this cluster directory's 1", "--force")
}

func TestForceAppliesAnOlderGeneration(t *testing.T) {
	o := newOperator(t, "op")
	source := preflightCluster(t, o)
	o.nodes["node2"] = healthyNode(5)
	r := nodeAddPassesPreflight(t, o, source, "--force")
	wantContains(t, "node add --force's warnings", r.stderr, "node2: it has applied generation 5, later than this cluster directory's 1")
	if strings.Contains(r.stderr, "doesn't, so mooctl can't reach them") {
		t.Errorf("node add --force warned about unlisted nodes when there are none:\n%s", r.stderr)
	}
}

func TestForceNamesTheNodesTheLatestGenerationListsButTheDirectoryDoesnt(t *testing.T) {
	o := newOperator(t, "op")
	source := preflightCluster(t, o)
	later := healthyNode(5)
	later.status.Nodes = []string{"node1", "node2", "node3", "node4", "node5"}
	o.nodes["node2"] = later
	r := nodeAddPassesPreflight(t, o, source, "--force")
	wantContains(t, "node add --force's warnings", r.stderr, "Generation 5 lists node4, node5, which this cluster directory doesn't, so mooctl can't reach them, and they keep the generation they have applied.")
}

func TestThePreflightRefusesANodeThatCantTellItsGenerationUnlessForced(t *testing.T) {
	o := newOperator(t, "op")
	source := preflightCluster(t, o)
	o.nodes["node3"] = generationUnknown("reading /var/lib/moostack/generation: invalid character")
	nodeAddRefused(t, o, source, "node3: it can't tell the generation it has applied: reading /var/lib/moostack/generation")
	r := nodeAddPassesPreflight(t, o, source, "--force")
	wantContains(t, "node add --force's warnings", r.stderr, "node3: it can't tell the generation it has applied")
}

func TestForceWarnsOnlyWhenItOverridesALaterGeneration(t *testing.T) {
	o := newOperator(t, "op")
	source := preflightCluster(t, o)
	if r := nodeAddPassesPreflight(t, o, source, "--force"); r.stderr != "" {
		t.Errorf("node add --force, overriding nothing, warned:\n%s", r.stderr)
	}
}

func TestForceDoesntSkipTheOtherChecks(t *testing.T) {
	o := newOperator(t, "op")
	source := preflightCluster(t, o)
	o.nodes["node1"] = &fakeNode{err: errors.New("dial tcp: no route to host")}
	before := snapshot(t, o.dir)
	r := o.run("--release-source", source, "node", "add", unreachable, "--force")
	if r.err == nil || !strings.Contains(r.err.Error(), "no route to host") || strings.Contains(r.err.Error(), unreachable) {
		t.Errorf("node add --force with an unreachable node: %v, want the preflight to fail", r.err)
	}
	if after := snapshot(t, o.dir); !maps.Equal(before, after) {
		t.Error("the refused node add changed the cluster directory")
	}
}

func TestThePreflightNamesEveryNodeItRefuses(t *testing.T) {
	o := newOperator(t, "op")
	source := preflightCluster(t, o)
	o.nodes["node1"] = &fakeNode{err: errors.New("host key mismatch for node1")}
	o.nodes["node3"] = &fakeNode{status: nodeclient.Status{Mode: nodev1.Mode_MODE_CONTROL_PLANE}}
	nodeAddRefused(t, o, source, "node1: host key mismatch", "node3: it is in control plane mode")
}

// barrier makes every node's dial wait until all of them have started,
// failing t if they don't within a few seconds, as when mooctl reaches
// nodes one at a time.
func barrier(t *testing.T, nodes map[string]*fakeNode) {
	var (
		mu     sync.Mutex
		dialed int
	)
	all := make(chan struct{})
	for _, n := range nodes {
		n.dialing = func(context.Context) {
			mu.Lock()
			if dialed++; dialed == len(nodes) {
				close(all)
			}
			mu.Unlock()
			select {
			case <-all:
			case <-time.After(5 * time.Second):
				t.Error("mooctl didn't reach the nodes at once")
			}
		}
	}
}

func TestThePreflightReachesEveryNodeAtOnce(t *testing.T) {
	o := newOperator(t, "op")
	source := preflightCluster(t, o)
	barrier(t, o.nodes)
	nodeAddPassesPreflight(t, o, source)
}

func TestThePreflightRefusesANodeInAnUnknownMode(t *testing.T) {
	o := newOperator(t, "op")
	source := preflightCluster(t, o)
	o.nodes["node2"] = &fakeNode{status: nodeclient.Status{Generation: 1}}
	nodeAddRefused(t, o, source, "node2", "not direct mode")
}

func TestThePreflightRefusesANodeWhoseUpdaterDoesntAnswer(t *testing.T) {
	o := newOperator(t, "op")
	source := preflightCluster(t, o)
	o.nodes["node2"] = &fakeNode{status: nodeclient.Status{Mode: nodev1.Mode_MODE_DIRECT, Generation: 1, UpdaterErr: errors.New("connection refused")}}
	nodeAddRefused(t, o, source, "node2: the updater doesn't answer: connection refused")
}

func TestThePreflightSuggestsForceOnlyWhenItWouldPass(t *testing.T) {
	o := newOperator(t, "op")
	source := preflightCluster(t, o)
	o.nodes["node2"] = healthyNode(5)
	o.nodes["node3"] = &fakeNode{err: errors.New("dial tcp: no route to host")}
	r := o.run("--release-source", source, "node", "add", unreachable)
	if r.err == nil || strings.Contains(r.err.Error(), "--force") {
		t.Errorf("node add with an unreachable node suggests --force: %v", r.err)
	}
}

func TestThePreflightSuggestsForceForANodeThatCantTellItsGeneration(t *testing.T) {
	o := newOperator(t, "op")
	source := preflightCluster(t, o)
	o.nodes["node3"] = generationUnknown("unreadable")
	nodeAddRefused(t, o, source, "node3: it can't tell the generation", "pass --force to go on from it anyway; the next operation that writes it has every node record its generation")
}

func TestThePreflightSuggestsForceForEachKindOfRefusalItWouldPass(t *testing.T) {
	o := newOperator(t, "op")
	source := preflightCluster(t, o)
	o.nodes["node2"] = healthyNode(5)
	o.nodes["node3"] = generationUnknown("unreadable")
	nodeAddRefused(t, o, source,
		"Pull the latest cluster directory, or pass --force to go on from this one anyway",
		"pass --force to go on from it anyway; the next operation that writes it has every node record its generation")
}

func TestOnlyNodeAddTakesForce(t *testing.T) {
	o := newOperator(t, "op")
	source := preflightCluster(t, o)
	for _, args := range [][]string{
		{"--force", "node", "add", unreachable},
		{"cluster", "status", "--force"},
		{"secret", "recipients", "ls", "--force"},
	} {
		r := o.run(append([]string{"--release-source", source}, args...)...)
		if r.err == nil || !strings.Contains(r.err.Error(), "flag provided but not defined: -force") {
			t.Errorf("mooctl %s: %v, want --force refused as unknown", strings.Join(args, " "), r.err)
		}
	}
}

func TestForceSaysWhatItPasses(t *testing.T) {
	o := newOperator(t, "op")
	r := o.mustRun("node", "add", "--help")
	wantContains(t, "node add --help", r.stdout, "even if a node has applied a later generation of it, or can't tell which generation it applied")
}
