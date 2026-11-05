package orchestrate_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/moostackhq/moostack/internal/hostlayout"
	"github.com/moostackhq/moostack/internal/orchestrate"
)

// removedNode3 is a removed node's identity.
var removedNode3 = []hostlayout.RemovedIdentity{{Node: "node3", Identity: "sha256:" + strings.Repeat("a", 64)}}

func TestThePreflightPushesTheRemovedIdentitiesToEveryNode(t *testing.T) {
	f := fakes{"node1": {generation: 1}, "node2": {generation: 1}}
	nodes := f.nodes()
	nodes.Removed = append(slices.Clone(removedNode3), hostlayout.RemovedIdentity{Node: "node4", Identity: "sha256:" + strings.Repeat("b", 64)})
	cl, err := orchestrate.Preflight(t.Context(), nodes, 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	for name, n := range f {
		if got := n.removedIdentities(); !slices.Equal(got, nodes.Removed) {
			t.Errorf("%s holds %v as removed, want %v", name, got, nodes.Removed)
		}
	}
}

func TestThePreflightKeepsTheLeaseWhileItPushesTheRemovedIdentities(t *testing.T) {
	ttl := 600 * time.Millisecond
	f := fakes{"node1": {generation: 1, removeDelay: 2 * ttl}, "node2": {generation: 1}}
	nodes := f.nodes()
	nodes.LeaseTTL = ttl
	cl, err := orchestrate.Preflight(t.Context(), nodes, 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	for name, n := range f {
		if l := n.heldLock(); l == nil {
			t.Errorf("after a push that took two lease times, %s doesn't hold the lock for the operation", name)
		}
	}
	if err := cl.CheckLock(); err != nil {
		t.Errorf("after the push, the operation's lease: %v", err)
	}
}

func TestAPreflightThatLosesTheLockWhileItPushesFails(t *testing.T) {
	ttl := 600 * time.Millisecond
	f := fakes{"node1": {generation: 1, stolenOnRemove: "bobs", removeDelay: ttl}, "node2": {generation: 1}}
	nodes := f.nodes()
	nodes.LeaseTTL = ttl
	cl, err := orchestrate.Preflight(t.Context(), nodes, 1, false, holder)
	if err == nil {
		cl.Close()
		t.Fatal("the preflight passed although node1's lock was taken by another operation during its push")
	}
	if !errors.Is(err, orchestrate.ErrLockLost) {
		t.Errorf("the preflight failed with %v, want ErrLockLost", err)
	}
	if l := f["node2"].heldLock(); l != nil {
		t.Errorf("after the failed preflight, node2 has the lock %+v, want it handed back", l)
	}
	for name, n := range f {
		if !n.isClosed() {
			t.Errorf("the failed preflight left the connection to %s open", name)
		}
	}
}

func TestThePreflightFailsNamingANodeThatDoesntTakeTheRemovedIdentities(t *testing.T) {
	f := fakes{"node1": {generation: 1}, "node2": {generation: 1, removeErr: errors.New("its disk is full")}}
	nodes := f.nodes()
	nodes.Removed = removedNode3
	_, err := orchestrate.Preflight(t.Context(), nodes, 1, false, holder)
	if err == nil {
		t.Fatal("the preflight passed")
	}
	wantContains(t, "the preflight's error", err.Error(), "node2 didn't take the removed identities: its disk is full")
	if strings.Contains(err.Error(), "node1") {
		t.Errorf("the preflight's error names node1, which took them: %v", err)
	}
	for name, n := range f {
		if l := n.heldLock(); l != nil {
			t.Errorf("after the failed preflight, %s has the lock %+v, want it handed back", name, l)
		}
		if !n.isClosed() {
			t.Errorf("the failed preflight left the connection to %s open", name)
		}
	}
}

func TestRemoveIdentitiesPushesToEveryNodeAndNamesThoseThatDidntTakeThem(t *testing.T) {
	f := fakes{"node1": {generation: 1}, "node2": {generation: 1}, "node3": {generation: 1}}
	cl, err := orchestrate.Preflight(t.Context(), f.nodes(), 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	f["node3"].mu.Lock()
	f["node3"].removeErr = errors.New("its disk is full")
	f["node3"].mu.Unlock()
	err = cl.RemoveIdentities(t.Context(), removedNode3)
	if err == nil {
		t.Fatal("RemoveIdentities passed with node3 refusing them")
	}
	wantContains(t, "RemoveIdentities' error", err.Error(), "node3 didn't take the removed identities: its disk is full")
	for _, name := range []string{"node1", "node2"} {
		if strings.Contains(err.Error(), name) {
			t.Errorf("RemoveIdentities' error names %s, which took them: %v", name, err)
		}
		if got := f[name].removedIdentities(); !slices.Equal(got, removedNode3) {
			t.Errorf("%s holds %v as removed, want [sha256:aa]", name, got)
		}
	}
}

func TestTheOperationsLastNodeKeepsTheLockWhenRemoved(t *testing.T) {
	f := fakes{"node1": {generation: 1}}
	cl, err := orchestrate.Preflight(t.Context(), f.nodes(), 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	cl.Remove("node1")
	if f["node1"].heldLock() == nil {
		t.Error("after Remove, the last node has no lock, want it kept")
	}
	if !f["node1"].isClosed() {
		t.Error("Remove left the connection to node1 open")
	}
	if err := cl.CheckLock(); err != nil {
		t.Errorf("after Remove, the operation's lease: %v", err)
	}
}

func TestARemovedNodeIsLeftOutOfTheOperation(t *testing.T) {
	f := fakes{"node1": {generation: 1}, "node2": {generation: 1}}
	cl, err := orchestrate.Preflight(t.Context(), f.nodes(), 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	cl.Remove("node2")
	if l := f["node2"].heldLock(); l != nil {
		t.Errorf("after Remove, node2 has the lock %+v, want it released", l)
	}
	if !f["node2"].isClosed() {
		t.Error("Remove left the connection to node2 open")
	}
	// The node going away doesn't cost the operation its lease.
	if err := cl.CheckLock(); err != nil {
		t.Errorf("after Remove, the operation's lease: %v", err)
	}
	if err := cl.RemoveIdentities(t.Context(), removedNode3); err != nil {
		t.Fatal(err)
	}
	if err := cl.Record(t.Context(), 2); err != nil {
		t.Fatal(err)
	}
	if got := f["node2"].removedIdentities(); len(got) != 0 {
		t.Errorf("the removed node2 got the removed identities %v", got)
	}
	if got := f["node2"].recorded; len(got) != 0 {
		t.Errorf("the removed node2 recorded %v", got)
	}
	if got := recorded(f["node1"].recordedNodes); !slices.Equal(got, []string{"node1 at " + privateAddress("node1").String()}) {
		t.Errorf("node1 recorded the nodes %v, want node1 alone", got)
	}
}

func TestThePreflightHasANodeBehindRecordTheDirectorysGeneration(t *testing.T) {
	f := fakes{"node1": {generation: 3}, "node2": {generation: 5}}
	nodes := f.nodes()
	cl, err := orchestrate.Preflight(t.Context(), nodes, 5, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if got := f["node1"].recorded; !slices.Equal(got, []int64{5}) {
		t.Errorf("node1, behind, recorded %v, want generation 5", got)
	}
	var listed []string
	for _, n := range f["node1"].recordedNodes {
		listed = append(listed, n.Name+" at "+n.PrivateAddress)
	}
	want := []string{"node1 at " + nodes.PrivateAddresses["node1"].String(), "node2 at " + nodes.PrivateAddresses["node2"].String()}
	if !slices.Equal(listed, want) {
		t.Errorf("node1 recorded the nodes %v, want %v", listed, want)
	}
	if got := f["node2"].recorded; len(got) != 0 {
		t.Errorf("node2, at the directory's generation, recorded %v", got)
	}
}

func TestAForcedPreflightRecordsNothing(t *testing.T) {
	f := fakes{"node1": {generation: 3}, "node2": {generation: 7}}
	cl, err := orchestrate.Preflight(t.Context(), f.nodes(), 5, true, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	for name, n := range f {
		if len(n.recorded) != 0 {
			t.Errorf("%s recorded %v", name, n.recorded)
		}
	}
}

func TestAPreflightGoesOnPastANodeThatCantRecordAndSaysSo(t *testing.T) {
	f := fakes{"node1": {generation: 3, recordErr: errors.New("disk full")}, "node2": {generation: 3}, "node3": {generation: 5}}
	cl, err := orchestrate.Preflight(t.Context(), f.nodes(), 5, false, holder)
	if err != nil {
		t.Fatalf("the preflight failed on a node that can't record: %v", err)
	}
	defer cl.Close()
	if !slices.Equal(cl.CaughtUp, []string{"node2"}) {
		t.Errorf("it caught up %v, want node2", cl.CaughtUp)
	}
	if len(cl.Behind) != 1 || !strings.Contains(cl.Behind[0].Error(), "node1 didn't record generation 5") {
		t.Errorf("it left behind %v, want node1", cl.Behind)
	}
}

func TestThePreflightBringsANodeUpBeforeItPushes(t *testing.T) {
	f := fakes{"node1": {generation: 3}}
	nodes := f.nodes()
	nodes.Removed = removedNode3
	cl, err := orchestrate.Preflight(t.Context(), nodes, 5, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if f["node1"].recordedAtPush != 1 {
		t.Errorf("the push came after %d recordings, want 1", f["node1"].recordedAtPush)
	}
}

func TestAPreflightOfSomeOfTheNodesRecordsNothing(t *testing.T) {
	f := fakes{"node1": {generation: 3}}
	nodes := f.nodes()
	nodes.Partial = true
	cl, err := orchestrate.Preflight(t.Context(), nodes, 5, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if got := f["node1"].recorded; len(got) != 0 {
		t.Errorf("node1 recorded %v", got)
	}
}

func TestAPreflightThatLosesTheLockWhileItBringsANodeUpFailsAtOnce(t *testing.T) {
	ttl := 600 * time.Millisecond
	f := fakes{"node1": {generation: 3, stolenOnRecord: "bobs", recordDelay: 20 * ttl}, "node2": {generation: 5}}
	nodes := f.nodes()
	nodes.LeaseTTL = ttl
	start := time.Now()
	cl, err := orchestrate.Preflight(t.Context(), nodes, 5, false, holder)
	if err == nil {
		cl.Close()
		t.Fatal("the preflight passed although node1's lock was taken while it recorded")
	}
	if !errors.Is(err, orchestrate.ErrLockLost) {
		t.Errorf("the preflight failed with %v, want ErrLockLost", err)
	}
	if took := time.Since(start); took > 10*ttl {
		t.Errorf("the preflight took %s to fail, so the lost lease didn't end the recording", took)
	}
}

func TestThePreflightKeepsTheLeaseWhileItBringsANodeUp(t *testing.T) {
	ttl := 600 * time.Millisecond
	f := fakes{"node1": {generation: 3, recordDelay: 2 * ttl}, "node2": {generation: 5}}
	nodes := f.nodes()
	nodes.LeaseTTL = ttl
	cl, err := orchestrate.Preflight(t.Context(), nodes, 5, false, holder)
	if err != nil {
		t.Fatalf("a recording that took two lease times: %v", err)
	}
	defer cl.Close()
	if err := cl.CheckLock(); err != nil {
		t.Errorf("after the recording, the operation's lease: %v", err)
	}
}

func TestACancelledPreflightStopsBringingANodeUp(t *testing.T) {
	f := fakes{"node1": {generation: 3, recordDelay: time.Hour}}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	time.AfterFunc(200*time.Millisecond, cancel)
	start := time.Now()
	cl, err := orchestrate.Preflight(ctx, f.nodes(), 5, false, holder)
	if err == nil {
		cl.Close()
		t.Fatal("the preflight passed although it was cancelled")
	}
	if took := time.Since(start); took > 10*time.Second {
		t.Errorf("the cancelled preflight took %s", took)
	}
}

func TestAPreflightGoesOnPastANodeHungOnItsRecording(t *testing.T) {
	f := fakes{"node1": {generation: 3, recordDelay: time.Hour}, "node2": {generation: 5}}
	nodes := f.nodes()
	nodes.RecordTimeout = 100 * time.Millisecond
	cl, err := orchestrate.Preflight(t.Context(), nodes, 5, false, holder)
	if err != nil {
		t.Fatalf("the preflight failed on a node hung on its recording: %v", err)
	}
	defer cl.Close()
	if len(cl.Behind) != 1 || !strings.Contains(cl.Behind[0].Error(), "node1 didn't record generation 5") {
		t.Errorf("it left behind %v, want node1", cl.Behind)
	}
}

func TestAPreflightThatFailsItsPushSaysWhatTheNodesKeep(t *testing.T) {
	failing := func() *node { return &node{generation: 5, removeErr: errors.New("disk full")} }
	for name, c := range map[string]struct {
		nodes   fakes
		removed []hostlayout.RemovedIdentity
		says    string
	}{
		"one took them": {fakes{"node1": failing(), "node2": {generation: 5}}, removedNode3,
			"the operation changed nothing, but the nodes that took the removed identities keep them:"},
		"none took them": {fakes{"node1": failing(), "node2": failing()}, removedNode3, "so nothing changed:"},
		"one caught up": {fakes{"node1": failing(), "node2": {generation: 3}}, nil,
			"the operation changed nothing, but node2 recorded the cluster directory's generation:"},
		"one caught up and took them": {fakes{"node1": failing(), "node2": {generation: 3}}, removedNode3,
			"but node2 recorded the cluster directory's generation, and the nodes that took the removed identities keep them:"},
	} {
		t.Run(name, func(t *testing.T) {
			nodes := c.nodes.nodes()
			nodes.Removed = c.removed
			cl, err := orchestrate.Preflight(t.Context(), nodes, 5, false, holder)
			if err == nil {
				cl.Close()
				t.Fatal("the preflight passed although a push failed")
			}
			if !strings.Contains(err.Error(), c.says) {
				t.Errorf("the preflight's error doesn't say %q: %v", c.says, err)
			}
		})
	}
}

func TestAPreflightThatLosesTheLockSaysWhoRecorded(t *testing.T) {
	ttl := 600 * time.Millisecond
	f := fakes{"node1": {generation: 5, stolenOnRemove: "bobs", removeDelay: ttl}, "node2": {generation: 3}}
	nodes := f.nodes()
	nodes.LeaseTTL = ttl
	cl, err := orchestrate.Preflight(t.Context(), nodes, 5, false, holder)
	if err == nil {
		cl.Close()
		t.Fatal("the preflight passed although node1's lock was taken")
	}
	if !errors.Is(err, orchestrate.ErrLockLost) || !strings.Contains(err.Error(), "node2 recorded the cluster directory's generation") {
		t.Errorf("the preflight failed with %v, want ErrLockLost saying node2 recorded", err)
	}
}
