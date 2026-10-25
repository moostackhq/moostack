package orchestrate_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/moostackhq/moostack/internal/nodeclient"
	"github.com/moostackhq/moostack/internal/orchestrate"
)

// holder is who the tests' operations run for.
var holder = orchestrate.Holder{Who: "alice", Machine: "laptop"}

func TestThePreflightTakesTheLockOnEveryNodeAndCloseReleasesIt(t *testing.T) {
	f := fakes{"node1": {generation: 1}, "node2": {generation: 1}}
	cl, err := orchestrate.Preflight(t.Context(), f.nodes(), 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	l1, l2 := f["node1"].heldLock(), f["node2"].heldLock()
	if l1 == nil || l2 == nil || l1.Lease != l2.Lease || l1.Lease == "" {
		t.Fatalf("after the preflight, node1 has the lock %+v and node2 %+v, want both held by one lease", l1, l2)
	}
	if l1.Holder != "alice" || l1.Machine != "laptop" {
		t.Errorf("the lock is held by %s on %s, want alice on laptop", l1.Holder, l1.Machine)
	}
	if err := cl.Record(t.Context(), 2); err != nil {
		t.Errorf("recording as the lock's holder: %v", err)
	}
	cl.Close()
	for name, n := range f {
		if l := n.heldLock(); l != nil {
			t.Errorf("after Close, %s still has the lock %+v", name, l)
		}
	}
}

func TestAPreflightOfALockedClusterFailsNamingTheHolderAndHandsBackWhatItGot(t *testing.T) {
	f := fakes{"node1": {generation: 1}, "node2": {generation: 1}, "node3": {generation: 1}}
	f["node2"].lockFor("bobs", time.Minute)
	_, err := orchestrate.Preflight(t.Context(), f.nodes(), 1, true, holder)
	if err == nil {
		t.Fatal("the preflight of a locked cluster passed")
	}
	var held *orchestrate.LockHeldError
	if !errors.As(err, &held) || held.Lock.Holder != "bob" {
		t.Errorf("the preflight failed with %v, want a LockHeldError of bob's lock", err)
	}
	wantContains(t, "the preflight's error", err.Error(), "node2: ", "bob", "desktop", "2026-10-19")
	if perr := (*orchestrate.PreflightError)(nil); errors.As(err, &perr) && perr.Forceable() {
		t.Error("the preflight's error says force would pass it")
	}
	for _, name := range []string{"node1", "node3"} {
		if l := f[name].heldLock(); l != nil {
			t.Errorf("after the refused preflight, %s has the lock %+v, want it handed back", name, l)
		}
	}
	if l := f["node2"].heldLock(); l == nil || l.Lease != "bobs" {
		t.Errorf("after the refused preflight, node2 has the lock %+v, want bob's still", l)
	}
}

func TestAGrantWhoseAnswerIsLostIsHandedBack(t *testing.T) {
	f := fakes{"node1": {generation: 1}, "node2": {generation: 1, answerLost: true}}
	nodes := f.nodes()
	nodes.ReachTimeout = 200 * time.Millisecond
	if _, err := orchestrate.Preflight(t.Context(), nodes, 1, false, holder); err == nil {
		t.Fatal("the preflight passed without node2's answer")
	} else {
		wantContains(t, "the preflight's error", err.Error(), "node2: taking the operation lock")
	}
	for name, n := range f {
		if l := n.heldLock(); l != nil {
			t.Errorf("after the failed preflight, %s has the lock %+v, want it handed back", name, l)
		}
	}
}

func TestThePreflightChecksTheGenerationsWhileHoldingTheLock(t *testing.T) {
	// Another operation records generation 2 on node2 after the preflight
	// first asks for its status, and ends before it takes the lock.
	f := fakes{"node1": {generation: 1}, "node2": {generation: 1, laterGeneration: 2}}
	_, err := orchestrate.Preflight(t.Context(), f.nodes(), 1, false, holder)
	if !errors.As(err, new(*orchestrate.LaterGenerationError)) {
		t.Fatalf("the preflight failed with %v, want node2's later generation", err)
	}
	for name, n := range f {
		if l := n.heldLock(); l != nil {
			t.Errorf("after the refused preflight, %s has the lock %+v, want it handed back", name, l)
		}
	}
	// Forced, the next generation is later than the one recorded since.
	f["node2"].statusCalls = 0
	cl, err := orchestrate.Preflight(t.Context(), f.nodes(), 1, true, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if got := cl.Next(1); got != 3 {
		t.Errorf("Next(1) is %d, want 3, later than node2's generation 2", got)
	}
}

func TestAPreflightWhoseLeaseRanOutBeforeItsGrantArrivedFails(t *testing.T) {
	f := fakes{"node1": {generation: 1}, "node2": {generation: 1, answerDelay: 250 * time.Millisecond}}
	nodes := f.nodes()
	nodes.LeaseTTL = 100 * time.Millisecond
	nodes.ReachTimeout = time.Second
	cl, err := orchestrate.Preflight(t.Context(), nodes, 1, false, holder)
	if err == nil {
		cl.Close()
		t.Fatal("the preflight passed with node2's lease run out before its grant arrived")
	}
	if !errors.Is(err, orchestrate.ErrLockLost) {
		t.Errorf("the preflight failed with %v, want ErrLockLost", err)
	}
	for name, n := range f {
		if l := n.heldLock(); l != nil {
			t.Errorf("after the failed preflight, %s has the lock %+v, want it handed back", name, l)
		}
	}
}

func TestOnlyOneOfTwoPreflightsHoldsTheLock(t *testing.T) {
	f := fakes{"node1": {generation: 1}, "node2": {generation: 1}}
	first, err := orchestrate.Preflight(t.Context(), f.nodes(), 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if _, err := orchestrate.Preflight(t.Context(), f.nodes(), 1, false, orchestrate.Holder{Who: "carol", Machine: "tablet"}); err == nil {
		t.Fatal("a second preflight passed while the first held the lock")
	} else {
		wantContains(t, "the second preflight's error", err.Error(), "alice", "laptop")
	}
}

func TestSurveyTakesNoLock(t *testing.T) {
	f := fakes{"node1": {generation: 1}}
	f["node1"].lockFor("bobs", time.Minute)
	reached := orchestrate.Survey(t.Context(), f.nodes())
	if l := reached[0].Status.Lock; l == nil || l.Holder != "bob" {
		t.Errorf("Survey found the lock %+v, want bob's", l)
	}
	if n := f["node1"].acquired; n != 0 {
		t.Errorf("Survey asked for the lock %d times", n)
	}
}

func TestTheLeaseIsRenewedWhileTheOperationRuns(t *testing.T) {
	f := fakes{"node1": {generation: 1}, "node2": {generation: 1}}
	nodes := f.nodes()
	nodes.LeaseTTL = 300 * time.Millisecond
	cl, err := orchestrate.Preflight(t.Context(), nodes, 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	time.Sleep(time.Second)
	if err := context.Cause(cl.Context()); err != nil {
		t.Fatalf("the operation was cancelled: %v", err)
	}
	if err := cl.Record(t.Context(), 2); err != nil {
		t.Errorf("recording after three lease times: %v", err)
	}
}

func TestAnOperationThatLosesItsLeaseIsCancelledBeforeAnotherCanTakeIt(t *testing.T) {
	for _, c := range []struct {
		name string
		lose func(n *node)
		want string
	}{
		{"the lock is broken and taken", func(n *node) { n.BreakLock(context.Background()); n.lockFor("bobs", time.Minute) }, "bob"},
		{"the lock is broken", func(n *node) { n.BreakLock(context.Background()) }, "node2: its lock was broken or ran out"},
		{"the node stops answering", func(n *node) {
			n.mu.Lock()
			n.hanging = true
			n.mu.Unlock()
		}, "node2"},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := fakes{"node1": {generation: 1}, "node2": {generation: 1}}
			nodes := f.nodes()
			nodes.LeaseTTL = 600 * time.Millisecond
			nodes.ReachTimeout = 100 * time.Millisecond
			cl, err := orchestrate.Preflight(t.Context(), nodes, 1, false, holder)
			if err != nil {
				t.Fatal(err)
			}
			defer cl.Close()
			c.lose(f["node2"])
			// The node frees the lock a lease time after it got the last
			// renewal, and the operation must have given up by then.
			expires := f["node2"].leaseExpires()
			select {
			case <-cl.Context().Done():
			case <-time.After(2 * time.Second):
				t.Fatal("the operation wasn't cancelled")
			}
			if cancelled := time.Now(); !cancelled.Before(expires) {
				t.Errorf("the operation was cancelled %s after node2's lease ran out, want before", cancelled.Sub(expires))
			}
			cause := context.Cause(cl.Context())
			if !errors.Is(cause, orchestrate.ErrLockLost) {
				t.Errorf("the operation was cancelled by %v, want ErrLockLost", cause)
			}
			wantContains(t, "the cause", cause.Error(), c.want)
		})
	}
}

func TestCloseDoesntWaitForARenewalANodeHangsOn(t *testing.T) {
	f := fakes{"node1": {generation: 1}, "node2": {generation: 1}}
	nodes := f.nodes()
	nodes.LeaseTTL = 3 * time.Second
	nodes.ReachTimeout = 100 * time.Millisecond
	cl, err := orchestrate.Preflight(t.Context(), nodes, 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	f["node2"].mu.Lock()
	f["node2"].hanging = true
	f["node2"].mu.Unlock()
	// The renewal a second in waits on node2 for up to a second.
	time.Sleep(1200 * time.Millisecond)
	start := time.Now()
	cl.Close()
	if took := time.Since(start); took > 500*time.Millisecond {
		t.Errorf("Close took %s, want it not to wait for the renewal node2 hangs on", took)
	}
}

func TestAnAddedNodeTakesTheLease(t *testing.T) {
	f := fakes{"node1": {generation: 1}}
	cl, err := orchestrate.Preflight(t.Context(), f.nodes(), 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	added := &node{}
	cl.Add("node2", added)
	if l, want := added.heldLock(), f["node1"].heldLock(); l == nil || l.Lease != want.Lease {
		t.Errorf("the added node has the lock %+v, want the operation's %+v", l, want)
	}
	cl.Close()
	if l := added.heldLock(); l != nil {
		t.Errorf("after Close, the added node still has the lock %+v", l)
	}
}

func TestAnAddedNodeThatRefusesTheLeaseDoesntRecord(t *testing.T) {
	f := fakes{"node1": {generation: 1}}
	cl, err := orchestrate.Preflight(t.Context(), f.nodes(), 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	added := &node{}
	added.lockFor("bobs", time.Minute)
	cl.Add("node2", added)
	err = cl.Record(t.Context(), 2)
	if err == nil {
		t.Fatal("Record passed with node2 locked by another")
	}
	wantContains(t, "Record's error", err.Error(), "node2 didn't record generation 2", "bob")
	if strings.Contains(err.Error(), "node1") {
		t.Errorf("Record says node1 didn't record: %v", err)
	}
}

func TestUnlockBreaksTheLockOnEveryNodeItReaches(t *testing.T) {
	f := fakes{"node1": {generation: 1}, "node2": {generation: 1}}
	f["node1"].lockFor("bobs", time.Minute)
	unlocked := orchestrate.Unlock(t.Context(), f.nodes("node3"))
	got := map[string]orchestrate.Unlocked{}
	for _, u := range unlocked {
		got[u.Name] = u
	}
	if u := got["node1"]; u.Err != nil || u.Broken == nil || u.Broken.Holder != "bob" {
		t.Errorf("unlocking node1: %+v, want bob's lock broken", u)
	}
	if u := got["node2"]; u.Err != nil || u.Broken != nil {
		t.Errorf("unlocking node2: %+v, want nothing broken", u)
	}
	if u := got["node3"]; u.Err == nil {
		t.Errorf("unlocking node3, which can't be reached: %+v, want an error", u)
	}
	if l := f["node1"].heldLock(); l != nil {
		t.Errorf("after Unlock, node1 has the lock %+v", l)
	}
}

// wantContains fails t unless s contains each of want.
func wantContains(t *testing.T, what, s string, want ...string) {
	t.Helper()
	for _, w := range want {
		if !strings.Contains(s, w) {
			t.Errorf("%s doesn't contain %q: %s", what, w, s)
		}
	}
}

var _ orchestrate.Node = (*node)(nil)

func TestPreflightTakesTheLockForTheOperationItRuns(t *testing.T) {
	f := fakes{"node1": {generation: 1}}
	cl, err := orchestrate.Preflight(t.Context(), inOrder(f, "node1"), 1, false, orchestrate.Holder{Who: "alice", Machine: "laptop", Operation: "cluster update"})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	l := f["node1"].heldLock()
	if l == nil || l.Operation != "cluster update" {
		t.Fatalf("node1's lock is %+v, want it held for cluster update", l)
	}
	if got := orchestrate.DescribeLock(*l); !strings.HasSuffix(got, ", for cluster update") {
		t.Errorf("DescribeLock says %q, want it to end with the operation", got)
	}
}

// Once the preflight has passed, what ends the caller's context, such
// as a signal, ends the operation's work but not its lease: the lease is
// renewed, a new node is locked and the nodes record a generation in the
// held context, which only the lock's loss ends.
func TestTheLeaseOutlivesTheCallersContext(t *testing.T) {
	f := fakes{"node1": {generation: 1}}
	nodes := f.nodes()
	nodes.LeaseTTL = 300 * time.Millisecond
	ctx, cancel := context.WithCancel(t.Context())
	cl, err := orchestrate.Preflight(ctx, nodes, 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	cancel()
	if cl.Context().Err() == nil {
		t.Error("the operation's context goes on after its caller's ended")
	}
	time.Sleep(3 * nodes.LeaseTTL)
	if err := cl.Held().Err(); err != nil {
		t.Fatalf("the held context ended with its caller's: %v", context.Cause(cl.Held()))
	}
	if err := cl.CheckLock(); err != nil {
		t.Fatalf("three lease times after its caller's context ended, the operation lost its lease: %v", err)
	}
	added := &node{generation: 0}
	cl.Add("node2", added)
	if l := added.heldLock(); l == nil {
		t.Error("a node added after the caller's context ended wasn't locked")
	}
	if err := cl.Record(cl.Held(), 2); err != nil {
		t.Errorf("recording in the held context: %v", err)
	}
	for name, n := range map[string]*node{"node1": f["node1"], "node2": added} {
		if got := n.recorded; !slices.Contains(got, 2) {
			t.Errorf("%s recorded %v, want generation 2", name, got)
		}
	}

	// Another operation takes node1's lock, as when the lock is broken.
	f["node1"].lockFor("bobs", time.Minute)
	select {
	case <-cl.Held().Done():
		if cause := context.Cause(cl.Held()); !errors.Is(cause, orchestrate.ErrLockLost) {
			t.Errorf("the held context ended with %v, want ErrLockLost", cause)
		}
	case <-time.After(5 * time.Second):
		t.Error("the held context went on after the operation lost the lock")
	}
}

// A node being installed, whose lock Lock took, holds it for the
// operation for as long as the install goes on, however long, and the
// lease runs out once the install lets go of it.
func TestLockKeepsTheLockOfANodeBeingInstalledUntilItIsLetGo(t *testing.T) {
	f := fakes{"node1": {generation: 1}}
	nodes := f.nodes()
	nodes.LeaseTTL = 300 * time.Millisecond
	cl, err := orchestrate.Preflight(t.Context(), nodes, 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	installed := &node{}
	lease, release, err := cl.Lock(t.Context(), installed)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(3 * nodes.LeaseTTL)
	if l := installed.heldLock(); l == nil || l.Lease != lease {
		t.Errorf("the node being installed holds the lock for %v after three lease times, want %s", l, lease)
	}
	release()
	time.Sleep(2 * nodes.LeaseTTL)
	if l := installed.heldLock(); l != nil {
		t.Errorf("the node holds the lock for %v two lease times after the install let go of it, want it free", l)
	}
}

// renewCounter is a node that counts the renewals of its lock it is
// asked for.
type renewCounter struct {
	*node
	renewals atomic.Int32
}

func (r *renewCounter) RenewLock(ctx context.Context, lease string, ttl time.Duration) (*nodeclient.Lock, error) {
	r.renewals.Add(1)
	return r.node.RenewLock(ctx, lease, ttl)
}

// The renewals of a node being installed end once the operation ends,
// as when it is closed or loses its lock, though the install never let go
// of it, and once the node answers that another lease holds its lock or
// none does.
func TestLockStopsRenewingOnceTheOperationEndsOrTheNodeRefuses(t *testing.T) {
	for what, end := range map[string]string{"the operation closed": "close", "another lease holding the lock": "take", "the lock freed": "free"} {
		t.Run(what, func(t *testing.T) {
			f := fakes{"node1": {generation: 1}}
			nodes := f.nodes()
			nodes.LeaseTTL = 300 * time.Millisecond
			cl, err := orchestrate.Preflight(t.Context(), nodes, 1, false, holder)
			if err != nil {
				t.Fatal(err)
			}
			if end != "close" {
				defer cl.Close()
			}
			installed := &renewCounter{node: &node{}}
			_, release, err := cl.Lock(t.Context(), installed)
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			time.Sleep(nodes.LeaseTTL)
			if installed.renewals.Load() == 0 {
				t.Fatal("the lock wasn't renewed within a lease time")
			}
			switch end {
			case "close":
				cl.Close()
			case "take":
				installed.lockFor("other", time.Hour)
			case "free":
				if _, err := installed.BreakLock(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			// The renewal that finds it ended comes within a third of a
			// lease time, and may still be counted.
			time.Sleep(nodes.LeaseTTL)
			after := installed.renewals.Load()
			time.Sleep(2 * nodes.LeaseTTL)
			if got := installed.renewals.Load(); got != after {
				t.Errorf("the lock was renewed %d times more over two lease times after %s", got-after, what)
			}
			// The node being installed takes no part in whether the
			// operation holds its lock.
			if end != "close" {
				if err := cl.CheckLock(); err != nil || cl.Context().Err() != nil {
					t.Errorf("after %s, the operation is lost: %v, %v, want it to hold its lock on the other nodes", what, err, cl.Context().Err())
				}
			}
		})
	}
}
