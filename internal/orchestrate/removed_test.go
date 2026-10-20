package orchestrate_test

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/moostackhq/moostack/internal/orchestrate"
)

func TestThePreflightPushesTheRemovedIdentitiesToEveryNode(t *testing.T) {
	f := fakes{"node1": {generation: 1}, "node2": {generation: 1}}
	nodes := f.nodes()
	nodes.Removed = []string{"sha256:aa", "sha256:bb"}
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
	nodes.Removed = []string{"sha256:aa"}
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
	err = cl.RemoveIdentities(t.Context(), []string{"sha256:aa"})
	if err == nil {
		t.Fatal("RemoveIdentities passed with node3 refusing them")
	}
	wantContains(t, "RemoveIdentities' error", err.Error(), "node3 didn't take the removed identities: its disk is full")
	for _, name := range []string{"node1", "node2"} {
		if strings.Contains(err.Error(), name) {
			t.Errorf("RemoveIdentities' error names %s, which took them: %v", name, err)
		}
		if got := f[name].removedIdentities(); !slices.Equal(got, []string{"sha256:aa"}) {
			t.Errorf("%s holds %v as removed, want [sha256:aa]", name, got)
		}
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
	if err := cl.RemoveIdentities(t.Context(), []string{"sha256:aa"}); err != nil {
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
	if got := f["node1"].recordedNodes; !slices.Equal(got, []string{"node1"}) {
		t.Errorf("node1 recorded the nodes %v, want node1 alone", got)
	}
}
