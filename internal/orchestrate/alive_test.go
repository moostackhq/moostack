package orchestrate_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/orchestrate"
)

const alive = nodev1.MemberState_MEMBER_STATE_ALIVE

// holds is a view of each of members, alive.
func holds(members ...string) func() map[string]nodev1.MemberState {
	return func() map[string]nodev1.MemberState {
		view := map[string]nodev1.MemberState{}
		for _, m := range members {
			view[m] = alive
		}
		return view
	}
}

// withNode3 is a cluster of f, with added added to it as node3.
func withNode3(t *testing.T, f fakes, added *node) *orchestrate.Cluster {
	t.Helper()
	cl, err := orchestrate.Preflight(t.Context(), f.nodes(), 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cl.Close() })
	cl.Add("node3", privateAddress("node3"), added)
	return cl
}

func TestWaitAliveEndsOnceTheNewNodeAndAnExistingNodeHoldEachOtherAlive(t *testing.T) {
	var seen atomic.Bool
	f := fakes{
		"node1": {generation: 1, view: holds("node1", "node2")},
		"node2": {generation: 1, view: func() map[string]nodev1.MemberState {
			if !seen.Swap(true) {
				return holds("node1", "node2")()
			}
			return holds("node1", "node2", "node3")()
		}},
	}
	cl := withNode3(t, f, &node{view: holds("node2", "node3")})
	seen.Store(false)
	if err := cl.WaitAlive(t.Context(), "node3", time.Minute, false); err != nil {
		t.Errorf("WaitAlive: %v", err)
	}
	if !seen.Load() {
		t.Error("WaitAlive didn't read node2's view")
	}
}

func TestWaitAliveFailsNamingTheNodesAndThePort(t *testing.T) {
	for name, c := range map[string]struct {
		existing, added []string
		heldOut         bool
	}{
		"neither sees the other":                 {[]string{"node1"}, []string{"node3"}, false},
		"only the new node sees the other":       {[]string{"node1"}, []string{"node1", "node3"}, false},
		"only the existing node sees the other":  {[]string{"node1", "node3"}, []string{"node3"}, false},
		"held out, and the new node sees nobody": {[]string{"node1", "node3"}, []string{"node3"}, true},
	} {
		t.Run(name, func(t *testing.T) {
			f := fakes{"node1": {generation: 1, view: holds(c.existing...)}}
			cl := withNode3(t, f, &node{view: holds(c.added...)})
			err := cl.WaitAlive(t.Context(), "node3", 50*time.Millisecond, c.heldOut)
			want := "node3 and node1 can't reach each other over UDP on the peer port, 7443"
			if err == nil || err.Error() != want {
				t.Errorf("WaitAlive: %v, want %q", err, want)
			}
		})
	}
}

// A problem takes UDP's place when it stands between the new node and
// every other that answered, and otherwise sits beside it.
func TestWaitAliveNamesTheProblemsNodesReport(t *testing.T) {
	udp := "node3 and node1, node2 can't reach each other over UDP on the peer port, 7443"
	for name, c := range map[string]struct {
		node1, node2, added string
		want                string
	}{
		"the new node's": {added: "bad", want: "node3 and node1, node2 can't reach each other:\nnode3: bad"},
		"every other's":  {node1: "bad", node2: "worse", want: "node3 and node1, node2 can't reach each other:\nnode1: bad\nnode2: worse"},
		"both ends'":     {node2: "bad", added: "worse", want: "node3 and node1, node2 can't reach each other:\nnode2: bad\nnode3: worse"},
		"another's only": {node1: "bad", want: udp + "\nnode1: bad"},
		"nobody's":       {want: udp},
	} {
		t.Run(name, func(t *testing.T) {
			f := fakes{
				"node1": {generation: 1, view: holds("node1"), membershipProblem: c.node1},
				"node2": {generation: 1, view: holds("node2"), membershipProblem: c.node2},
			}
			cl := withNode3(t, f, &node{view: holds("node3"), membershipProblem: c.added})
			err := cl.WaitAlive(t.Context(), "node3", 50*time.Millisecond, false)
			if err == nil || err.Error() != c.want {
				t.Errorf("WaitAlive: %v, want %q", err, c.want)
			}
		})
	}
}

// A node that doesn't report isn't counted among those with a problem.
func TestWaitAliveBlamesUDPBesideAProblemAndASilentNode(t *testing.T) {
	f := fakes{
		"node1": {generation: 1, view: holds("node1"), membershipProblem: "bad"},
		"node2": {generation: 1, view: holds("node2")},
		"node4": {generation: 1, view: holds("node4")},
	}
	cl := withNode3(t, f, &node{view: holds("node3")})
	f["node4"].stopAnswering()
	err := cl.WaitAlive(t.Context(), "node3", 50*time.Millisecond, false)
	want := "node3 and node1, node2 can't reach each other over UDP on the peer port, 7443\nnode1: bad\nNo membership reported by:\nnode4: it didn't answer"
	if err == nil || err.Error() != want {
		t.Errorf("WaitAlive: %v, want %q", err, want)
	}
}

// The new node's problem counts though no other node reported.
func TestWaitAliveNamesTheNewNodesProblemWithNoOtherAnswering(t *testing.T) {
	f := fakes{"node1": {generation: 1, view: holds("node1")}}
	cl := withNode3(t, f, &node{view: holds("node3"), membershipProblem: "bad"})
	f["node1"].stopAnswering()
	err := cl.WaitAlive(t.Context(), "node3", 50*time.Millisecond, false)
	if err == nil || !strings.Contains(err.Error(), "node3: bad") {
		t.Errorf("WaitAlive: %v, want the new node's problem", err)
	}
}

// A problem gone by the last answer isn't named.
func TestWaitAliveDoesntNameAProblemThatCleared(t *testing.T) {
	added := &node{view: holds("node3")}
	f := fakes{"node1": {generation: 1, view: holds("node1")}}
	cl := withNode3(t, f, added)
	var reads atomic.Int32
	added.view = func() map[string]nodev1.MemberState {
		if reads.Add(1) == 1 {
			added.mu.Lock()
			added.membershipProblem = "bad"
			added.mu.Unlock()
		} else {
			added.mu.Lock()
			added.membershipProblem = ""
			added.mu.Unlock()
		}
		return holds("node3")()
	}
	err := cl.WaitAlive(t.Context(), "node3", 500*time.Millisecond, false)
	want := "node3 and node1 can't reach each other over UDP on the peer port, 7443"
	if err == nil || err.Error() != want {
		t.Errorf("WaitAlive: %v, want %q", err, want)
	}
}

func TestWaitAliveOfSeveralNodesNamesThemAll(t *testing.T) {
	f := fakes{"node1": {generation: 1, view: holds("node1")}, "node2": {generation: 1, view: holds("node2")}}
	cl := withNode3(t, f, &node{view: holds("node3")})
	err := cl.WaitAlive(t.Context(), "node3", 50*time.Millisecond, false)
	if want := "node3 and node1, node2 can't reach each other over UDP on the peer port, 7443"; err == nil || err.Error() != want {
		t.Errorf("WaitAlive: %v, want %q", err, want)
	}
}

// The other nodes' views don't count for a name held out.
func TestWaitAliveOfAHeldOutNameTakesTheNewNodesViewAlone(t *testing.T) {
	f := fakes{"node1": {generation: 1, view: holds("node1")}}
	cl := withNode3(t, f, &node{view: holds("node1", "node3")})
	if err := cl.WaitAlive(t.Context(), "node3", time.Minute, true); err != nil {
		t.Errorf("WaitAlive: %v", err)
	}
}

func TestWaitAliveDoesntCountAMemberThatIsntAlive(t *testing.T) {
	for _, state := range []nodev1.MemberState{nodev1.MemberState_MEMBER_STATE_SUSPECT, nodev1.MemberState_MEMBER_STATE_DEAD, nodev1.MemberState_MEMBER_STATE_LEFT} {
		holding := func(member string) func() map[string]nodev1.MemberState {
			return func() map[string]nodev1.MemberState {
				return map[string]nodev1.MemberState{member: state}
			}
		}
		for name, c := range map[string]struct {
			existing, added func() map[string]nodev1.MemberState
			heldOut         bool
		}{
			"in the existing node's view":          {holding("node3"), holds("node1"), false},
			"in the new node's view":               {holds("node3"), holding("node1"), false},
			"in the new node's view, and held out": {holds("node1"), holding("node1"), true},
		} {
			t.Run(state.String()+" "+name, func(t *testing.T) {
				f := fakes{"node1": {generation: 1, view: c.existing}}
				cl := withNode3(t, f, &node{view: c.added})
				if err := cl.WaitAlive(t.Context(), "node3", 50*time.Millisecond, c.heldOut); err == nil {
					t.Error("WaitAlive took a member that isn't alive")
				}
			})
		}
	}
}

// The new node holding node1 alive and node2 holding the new node alive
// are two halves of two pairs, and neither pair holds each other alive.
func TestWaitAliveTakesOnlyTwoNodesThatHoldEachOtherAlive(t *testing.T) {
	f := fakes{
		"node1": {generation: 1, view: holds("node1")},
		"node2": {generation: 1, view: holds("node2", "node3")},
	}
	cl := withNode3(t, f, &node{view: holds("node1", "node3")})
	if err := cl.WaitAlive(t.Context(), "node3", 50*time.Millisecond, false); err == nil {
		t.Error("WaitAlive took two views that don't pair up")
	}
}

// A node alive while another node hangs is seen at once.
func TestWaitAliveOutlastsANodeThatHangs(t *testing.T) {
	var met atomic.Bool
	f := fakes{"node1": {generation: 1, view: func() map[string]nodev1.MemberState {
		if met.Load() {
			return holds("node3")()
		}
		return holds("node1")()
	}}, "node2": {generation: 1}}
	cl := withNode3(t, f, &node{view: holds("node1")})
	f["node2"].stopAnswering()
	done := make(chan error, 1)
	go func() { done <- cl.WaitAlive(t.Context(), "node3", time.Minute, false) }()
	time.Sleep(500 * time.Millisecond)
	met.Store(true)
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("WaitAlive: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Error("a node that hangs held WaitAlive up")
	}
}

// A node that doesn't report its membership isn't blamed on UDP.
func TestWaitAliveDoesntBlameUDPForANodeThatDoesntAnswer(t *testing.T) {
	for name, c := range map[string]struct {
		f     fakes
		added *node
		want  string
	}{
		"the new node": {fakes{"node1": {generation: 1, view: holds("node1")}}, &node{},
			"node3 didn't report its membership within 50ms: it reports no membership"},
		"the other node": {fakes{"node1": {generation: 1}}, &node{view: holds("node3")},
			"no other node reported its membership within 50ms:\nnode1: it reports no membership"},
		"every other node": {fakes{"node1": {generation: 1}, "node2": {generation: 1, view: func() map[string]nodev1.MemberState { return nil }}}, &node{view: holds("node3")},
			"no other node reported its membership within 50ms:\nnode1: it reports no membership\nnode2: it reports no membership"},
	} {
		t.Run(name, func(t *testing.T) {
			cl := withNode3(t, c.f, c.added)
			err := cl.WaitAlive(t.Context(), "node3", 50*time.Millisecond, false)
			if err == nil || err.Error() != c.want {
				t.Errorf("WaitAlive: %v, want %q", err, c.want)
			}
		})
	}
}

// A node that reported its membership once counts as having reported it.
func TestWaitAliveBlamesUDPOnceTheNodesHaveAnswered(t *testing.T) {
	var reads atomic.Int32
	f := fakes{"node1": {generation: 1, view: func() map[string]nodev1.MemberState {
		if reads.Add(1) > 1 {
			return nil
		}
		return holds("node1")()
	}}}
	cl := withNode3(t, f, &node{view: holds("node3")})
	reads.Store(0)
	err := cl.WaitAlive(t.Context(), "node3", 500*time.Millisecond, false)
	if want := "node3 and node1 can't reach each other over UDP on the peer port, 7443"; err == nil || err.Error() != want {
		t.Errorf("WaitAlive: %v, want %q", err, want)
	}
}

// A node that answered is blamed on UDP, and one that didn't is named apart.
func TestWaitAliveNamesOnlyTheNodesThatAnswered(t *testing.T) {
	f := fakes{"node1": {generation: 1, view: holds("node1")}, "node2": {generation: 1}}
	cl := withNode3(t, f, &node{view: holds("node3")})
	err := cl.WaitAlive(t.Context(), "node3", 50*time.Millisecond, false)
	want := "node3 and node1 can't reach each other over UDP on the peer port, 7443\nNo membership reported by:\nnode2: it reports no membership"
	if err == nil || err.Error() != want {
		t.Errorf("WaitAlive: %v, want %q", err, want)
	}
}

func TestWaitAliveStopsWhenItsContextEnds(t *testing.T) {
	f := fakes{"node1": {generation: 1, view: holds("node1")}}
	cl := withNode3(t, f, &node{view: holds("node3")})
	stop := errors.New("stopped")
	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(stop)
	if err := cl.WaitAlive(ctx, "node3", time.Minute, false); !errors.Is(err, stop) {
		t.Errorf("WaitAlive: %v, want the context's cause", err)
	}
}

func TestRecordOnHasOnlyThatNodeRecordEveryNode(t *testing.T) {
	f := fakes{"node1": {generation: 1}}
	added := &node{}
	cl := withNode3(t, f, added)
	if err := cl.RecordOn(t.Context(), "node3", 2); err != nil {
		t.Fatal(err)
	}
	if got, want := added.recorded, []int64{2}; !slices.Equal(got, want) {
		t.Errorf("node3 recorded the generations %v, want %v", got, want)
	}
	if got := f["node1"].recorded; len(got) != 0 {
		t.Errorf("node1 recorded the generations %v, want none", got)
	}
	var listed []string
	for _, n := range added.recordedNodes {
		listed = append(listed, n.Name)
	}
	if want := []string{"node1", "node3"}; !slices.Equal(listed, want) {
		t.Errorf("node3 recorded the nodes %v, want %v", listed, want)
	}
}

func TestRecordOnFailsWithTheNodesReason(t *testing.T) {
	refused := errors.New("the disk is full")
	cl := withNode3(t, fakes{"node1": {generation: 1}}, &node{recordErr: refused})
	if err := cl.RecordOn(t.Context(), "node3", 2); !errors.Is(err, refused) {
		t.Errorf("RecordOn: %v, want %v", err, refused)
	}
}

func TestRecordOnOfANodeTheOperationHasntFails(t *testing.T) {
	cl := withNode3(t, fakes{"node1": {generation: 1}}, &node{})
	want := "the operation has no node node4"
	if err := cl.RecordOn(t.Context(), "node4", 2); err == nil || err.Error() != want {
		t.Errorf("RecordOn: %v, want %q", err, want)
	}
}

func TestEmptySaysWhetherTheOperationHasANode(t *testing.T) {
	cl, err := orchestrate.Preflight(t.Context(), fakes{}.nodes(), 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if !cl.Empty() {
		t.Error("a cluster of no nodes isn't Empty")
	}
	cl.Add("node1", privateAddress("node1"), &node{})
	if cl.Empty() {
		t.Error("a cluster of a node is Empty")
	}
}
