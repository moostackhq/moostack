package orchestrate_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/nodeclient"
	"github.com/moostackhq/moostack/internal/orchestrate"
)

// node is a fake node in direct mode.
type node struct {
	generation int64
	// nodes are the nodes of the cluster directory at generation.
	nodes []string
	// problem, if set, says why the node can't tell its generation.
	problem string
	// recordErr, if set, is why recording a generation fails.
	recordErr error
	// hangs says the node never answers once reached: a status call or a
	// recording waits until its context ends.
	hangs bool

	mu       sync.Mutex
	recorded []int64
	// recordedNodes are the nodes given with the last recording.
	recordedNodes []string
	closed        bool
}

func (n *node) Status(ctx context.Context) nodeclient.Status {
	if n.hangs {
		<-ctx.Done()
		return nodeclient.Status{AgentErr: ctx.Err(), UpdaterErr: ctx.Err()}
	}
	return nodeclient.Status{Mode: nodev1.Mode_MODE_DIRECT, Generation: n.generation, Nodes: n.nodes, GenerationUnknown: n.problem}
}

func (n *node) RecordGeneration(ctx context.Context, g int64, nodes []string) error {
	if n.hangs {
		<-ctx.Done()
		return ctx.Err()
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.recordErr != nil {
		return n.recordErr
	}
	n.recorded = append(n.recorded, g)
	n.recordedNodes = nodes
	return nil
}

func (n *node) Close() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.closed = true
	return nil
}

func (n *node) isClosed() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.closed
}

// fakes is a cluster of fake nodes, by name.
type fakes map[string]*node

// names are the names of the cluster's nodes.
func (f fakes) names() []string {
	var names []string
	for name := range f {
		names = append(names, name)
	}
	return names
}

// nodes are the cluster's nodes, with extra names, as orchestrate takes
// them.
func (f fakes) nodes(extra ...string) orchestrate.Nodes {
	return orchestrate.Nodes{Names: append(f.names(), extra...), Dial: f.dial}
}

func (f fakes) dial(_ context.Context, name string) (orchestrate.Node, error) {
	if fake, ok := f[name]; ok {
		return fake, nil
	}
	return nil, errors.New("no route to host")
}

func TestTheNextGenerationIsLaterThanTheDirectorysAndEveryNodes(t *testing.T) {
	for _, c := range []struct {
		name         string
		dir          int64
		applied      []int64
		force        bool
		wantNext     int64
		wantRefusing bool
	}{
		{name: "every node applied the directory", dir: 4, applied: []int64{4, 4}, wantNext: 5},
		{name: "a node applied an older one", dir: 4, applied: []int64{2, 4}, wantNext: 5},
		{name: "a new cluster", dir: 1, wantNext: 2},
		{name: "a node applied a later one", dir: 4, applied: []int64{4, 6}, wantRefusing: true},
		{name: "a later one forced", dir: 4, applied: []int64{4, 6}, force: true, wantNext: 7},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := fakes{}
			for i, g := range c.applied {
				f[string(rune('a'+i))] = &node{generation: g}
			}
			cl, err := orchestrate.Preflight(t.Context(), f.nodes(), c.dir, c.force)
			if c.wantRefusing {
				if err == nil {
					t.Fatal("the preflight passed")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer cl.Close()
			if got := cl.Next(c.dir); got != c.wantNext {
				t.Errorf("Next(%d) is %d, want %d", c.dir, got, c.wantNext)
			}
		})
	}
}

func TestRecordRecordsOnEveryNodeTheAddedOneToo(t *testing.T) {
	f := fakes{"node1": {generation: 1}, "node2": {generation: 1}}
	cl, err := orchestrate.Preflight(t.Context(), f.nodes(), 1, false)
	if err != nil {
		t.Fatal(err)
	}
	added := &node{}
	cl.Add("node3", added)
	if err := cl.Record(t.Context(), 2); err != nil {
		t.Fatal(err)
	}
	for name, n := range map[string]*node{"node1": f["node1"], "node2": f["node2"], "node3": added} {
		if len(n.recorded) != 1 || n.recorded[0] != 2 {
			t.Errorf("%s recorded %v, want [2]", name, n.recorded)
		}
	}
	cl.Close()
	for name, n := range map[string]*node{"node1": f["node1"], "node2": f["node2"], "node3": added} {
		if !n.isClosed() {
			t.Errorf("Close left the connection to %s open", name)
		}
	}
}

func TestRecordNamesEveryNodeThatDidntRecord(t *testing.T) {
	f := fakes{
		"node1": {generation: 1, recordErr: errors.New("it is full")},
		"node2": {generation: 1},
		"node3": {generation: 1, recordErr: errors.New("it is stuck")},
	}
	cl, err := orchestrate.Preflight(t.Context(), f.nodes(), 1, false)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	err = cl.Record(t.Context(), 2)
	if err == nil {
		t.Fatal("Record succeeded")
	}
	for _, want := range []string{"node1 didn't record generation 2: it is full", "node3 didn't record generation 2: it is stuck"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("Record's error is %v, want it to contain %q", err, want)
		}
	}
	if strings.Contains(err.Error(), "node2") {
		t.Errorf("Record's error names node2, which recorded: %v", err)
	}
	if len(f["node2"].recorded) != 1 {
		t.Errorf("node2 recorded %v, want [2]", f["node2"].recorded)
	}
}

func TestAFailedPreflightClosesTheConnectionsItMade(t *testing.T) {
	f := fakes{"node1": {generation: 1}, "node2": {generation: 9}}
	if _, err := orchestrate.Preflight(t.Context(), f.nodes("gone"), 1, false); err == nil {
		t.Fatal("the preflight passed")
	}
	for name, n := range f {
		if !n.isClosed() {
			t.Errorf("the failed preflight left the connection to %s open", name)
		}
	}
}

func TestSurveyClosesTheConnectionsItMade(t *testing.T) {
	f := fakes{"node1": {generation: 1}, "node2": {generation: 9}}
	reached := orchestrate.Survey(t.Context(), f.nodes("gone"))
	if len(reached) != 3 {
		t.Fatalf("Survey reported %d nodes, want 3", len(reached))
	}
	for _, r := range reached {
		if (r.Err == nil) != (r.Name != "gone") {
			t.Errorf("Survey reports %s with the error %v", r.Name, r.Err)
		}
	}
	for name, n := range f {
		if !n.isClosed() {
			t.Errorf("Survey left the connection to %s open", name)
		}
	}
}

func TestRecordGivesUpOnANodeThatDoesntAnswer(t *testing.T) {
	t.Parallel()
	f := fakes{"node1": {generation: 1}, "node2": {generation: 1}}
	nodes := f.nodes()
	nodes.RecordTimeout = 100 * time.Millisecond
	cl, err := orchestrate.Preflight(t.Context(), nodes, 1, false)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	// node2 stops answering after the preflight.
	f["node2"].hangs = true
	done := make(chan error, 1)
	go func() { done <- cl.Record(context.Background(), 2) }()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "node2 didn't record generation 2") || !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Record's error is %v, want node2 timing out", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Record still waits for node2")
	}
	if len(f["node1"].recorded) != 1 {
		t.Errorf("node1 recorded %v, want [2]", f["node1"].recorded)
	}
}

func TestThePreflightGivesUpOnANodeThatDoesntAnswer(t *testing.T) {
	t.Parallel()
	f := fakes{"node1": {generation: 1}, "node2": {generation: 1, hangs: true}}
	nodes := f.nodes()
	nodes.ReachTimeout = 100 * time.Millisecond
	done := make(chan error, 1)
	go func() {
		_, err := orchestrate.Preflight(context.Background(), nodes, 1, false)
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "node2: the agent doesn't answer") || !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("the preflight's error is %v, want node2 timing out", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the preflight still waits for node2")
	}
}

func TestRecordRecordsTheNodesOfTheClusterDirectory(t *testing.T) {
	f := fakes{"node2": {generation: 1}, "node1": {generation: 1}}
	cl, err := orchestrate.Preflight(t.Context(), f.nodes(), 1, false)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	added := &node{}
	cl.Add("node3", added)
	if err := cl.Record(t.Context(), 2); err != nil {
		t.Fatal(err)
	}
	want := []string{"node1", "node2", "node3"}
	for name, n := range map[string]*node{"node1": f["node1"], "node2": f["node2"], "node3": added} {
		if !slices.Equal(n.recordedNodes, want) {
			t.Errorf("%s recorded the nodes %v, want %v", name, n.recordedNodes, want)
		}
	}
}

func TestThePreflightFindsTheNodesTheLatestGenerationListsButTheDirectoryDoesnt(t *testing.T) {
	f := fakes{
		// The directory lists node1 and node2. node1 has applied
		// generation 3, of node1, node2, node3 and node4; node2 an older
		// one, of node2 and node5.
		"node1": {generation: 3, nodes: []string{"node1", "node2", "node3", "node4"}},
		"node2": {generation: 2, nodes: []string{"node2", "node5"}},
	}
	cl, err := orchestrate.Preflight(t.Context(), f.nodes(), 2, true)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if want := []string{"node3", "node4"}; !slices.Equal(cl.Unlisted, want) {
		t.Errorf("the unlisted nodes are %v, want %v", cl.Unlisted, want)
	}
	if cl.Latest != 3 {
		t.Errorf("the latest generation is %d, want 3", cl.Latest)
	}
}

func TestThePreflightListsNoNodesWhenTheDirectoryListsThemAll(t *testing.T) {
	f := fakes{"node1": {generation: 2, nodes: []string{"node1", "node2"}}, "node2": {generation: 2, nodes: []string{"node1", "node2"}}}
	cl, err := orchestrate.Preflight(t.Context(), f.nodes(), 2, false)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if len(cl.Unlisted) != 0 || len(cl.Forced) != 0 {
		t.Errorf("the unlisted nodes are %v and the forced problems %v, want none", cl.Unlisted, cl.Forced)
	}
}

func TestThePreflightRefusesANodeThatCantTellItsGenerationUnlessForced(t *testing.T) {
	f := fakes{"node1": {generation: 1}, "node2": {problem: "reading /var/lib/moostack/generation: invalid character"}}
	_, err := orchestrate.Preflight(t.Context(), f.nodes(), 1, false)
	if err == nil || !strings.Contains(err.Error(), "node2: it can't tell the generation it has applied: reading /var/lib/moostack/generation") {
		t.Errorf("the preflight's error is %v, want node2 refused for not telling its generation", err)
	}
	cl, err := orchestrate.Preflight(t.Context(), f.nodes(), 1, true)
	if err != nil {
		t.Fatalf("the forced preflight failed: %v", err)
	}
	defer cl.Close()
	if len(cl.Forced) != 1 || !strings.Contains(cl.Forced[0].Error(), "node2: it can't tell the generation") {
		t.Errorf("the forced problems are %v, want node2's", cl.Forced)
	}
}

func TestAnUnforcedPreflightLooksForNoUnlistedNodes(t *testing.T) {
	// node1 lists node3, which the directory doesn't, at the directory's
	// generation, as after a removal it didn't hear of.
	f := fakes{"node1": {generation: 2, nodes: []string{"node1", "node2", "node3"}}, "node2": {generation: 2, nodes: []string{"node1", "node2"}}}
	cl, err := orchestrate.Preflight(t.Context(), f.nodes(), 2, false)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if len(cl.Unlisted) != 0 {
		t.Errorf("an unforced preflight found the unlisted nodes %v", cl.Unlisted)
	}
}

func TestANodeThatWasntReachedHasNoKnownGeneration(t *testing.T) {
	f := fakes{"node1": {generation: 3}}
	for _, r := range orchestrate.Survey(t.Context(), f.nodes("gone")) {
		got, ok := r.KnownGeneration()
		if want := r.Name == "node1"; ok != want || (ok && got != 3) {
			t.Errorf("%s's known generation is %d, %t", r.Name, got, ok)
		}
	}
}

func TestAPreflightErrorSaysWhetherForceWouldPassIt(t *testing.T) {
	for _, c := range []struct {
		name string
		f    fakes
		// unreachable are nodes the directory lists that can't be reached.
		unreachable []string
		forceable   bool
	}{
		{name: "a later generation", f: fakes{"node1": {generation: 5}}, forceable: true},
		{name: "a node that can't tell its generation", f: fakes{"node1": {problem: "unreadable"}}, forceable: true},
		{name: "a later generation and an unreachable node", f: fakes{"node1": {generation: 5}}, unreachable: []string{"gone"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err := orchestrate.Preflight(t.Context(), c.f.nodes(c.unreachable...), 1, false)
			var perr *orchestrate.PreflightError
			if !errors.As(err, &perr) {
				t.Fatalf("the preflight's error is %v, want a PreflightError", err)
			}
			if perr.Forceable() != c.forceable {
				t.Errorf("Forceable is %t, want %t, for %v", perr.Forceable(), c.forceable, err)
			}
		})
	}
}
