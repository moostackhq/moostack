package orchestrate_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/orchestrate"
)

// releaseOf gives each node the agent at 2.0.0, the updater at 2.0.1
// and containerd at 3.0.0, each of whose binaries names the node.
func releaseOf(name string) (orchestrate.NodeRelease, error) {
	return orchestrate.NodeRelease{
		"agent":      {Version: "2.0.0", Files: map[string][]byte{"agent": []byte("agent for " + name)}},
		"updater":    {Version: "2.0.1", Files: map[string][]byte{"updater": []byte("updater for " + name)}},
		"containerd": {Version: "3.0.0", Files: map[string][]byte{"containerd": []byte("containerd for " + name), "shim": []byte("shim for " + name)}},
	}, nil
}

// update has cl update its nodes to releaseOf's, telling nothing.
func update(cl *orchestrate.Cluster) error {
	return cl.UpdateNodes(cl.Context(), releaseOf, func(orchestrate.Step) {})
}

// inOrder are fakes' nodes in the order of names.
func inOrder(f fakes, names ...string) orchestrate.Nodes {
	return orchestrate.Nodes{Names: names, Dial: f.dial}
}

func TestUpdateNodesUpdatesEachNodesAgentUpdaterAndContainerdBeforeTheNextNode(t *testing.T) {
	f := fakes{}
	names := []string{"node1", "node2", "node3"}
	for _, name := range names {
		f[name] = &node{generation: 1, updateDelay: 50 * time.Millisecond}
	}
	nodes := inOrder(f, names...)
	nodes.LeaseTTL = 10 * time.Second
	cl, err := orchestrate.Preflight(t.Context(), nodes, 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	var told []string
	err = cl.UpdateNodes(cl.Context(), releaseOf, func(s orchestrate.Step) {
		told = append(told, s.Node+" "+s.Component+" "+s.Version)
	})
	if err != nil {
		t.Fatal(err)
	}
	var all []componentUpdate
	for _, name := range names {
		all = append(all, f[name].componentUpdates("")...)
	}
	slices.SortFunc(all, func(a, b componentUpdate) int { return a.started.Compare(b.started) })
	var got []string
	var previous time.Time
	for _, u := range all {
		got = append(got, u.component+" "+u.version+" "+u.binary)
		if u.started.Before(previous) {
			t.Errorf("the update of %s started before the previous update ended", u.binary)
		}
		previous = u.ended
		// Each has a fifth of a lease time to become healthy.
		if u.healthTimeout != 2*time.Second {
			t.Errorf("%s's health timeout is %s, want a fifth of the lease time, 2s", u.binary, u.healthTimeout)
		}
	}
	var want, wantTold []string
	for _, name := range names {
		want = append(want,
			"agent 2.0.0 agent for "+name,
			"updater 2.0.1 updater for "+name,
			"containerd 3.0.0 containerd: containerd for "+name+", shim: shim for "+name)
		wantTold = append(wantTold, name+" the agent 2.0.0", name+" the updater 2.0.1", name+" containerd 3.0.0")
	}
	if !slices.Equal(got, want) {
		t.Errorf("the nodes were updated in the order\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	if !slices.Equal(told, wantTold) {
		t.Errorf("the update told %q, want %q", told, wantTold)
	}
}

// A rerun reaches containerd on a node whose new containerd doesn't
// answer: the new agent would be refused while containerd is unhealthy,
// but the node runs it and the updater at the target already.
func TestUpdateNodesSkipsTheAgentAndTheUpdaterWhereTheNodeRunsThemAlready(t *testing.T) {
	healthy, unhealthy := nodev1.Health_HEALTH_HEALTHY, nodev1.Health_HEALTH_UNHEALTHY
	f := fakes{
		"node1": {generation: 1, components: []*nodev1.Component{
			{Name: "agent", Version: "2.0.0", Health: healthy},
			{Name: "containerd", Health: unhealthy},
			{Name: "updater", Version: "2.0.1", Health: healthy},
		}},
		// node2's updater runs the target, but doesn't answer.
		"node2": {generation: 1, components: []*nodev1.Component{
			{Name: "agent", Version: "1.0.0", Health: healthy},
			{Name: "updater", Version: "2.0.1", Health: unhealthy},
		}},
	}
	cl, err := orchestrate.Preflight(t.Context(), inOrder(f, "node1", "node2"), 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	var told []string
	err = cl.UpdateNodes(cl.Context(), releaseOf, func(s orchestrate.Step) {
		told = append(told, fmt.Sprintf("%s %s %s %v", s.Node, s.Component, s.Version, s.Already))
	})
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string][]string{"node1": {"containerd"}, "node2": {"agent", "updater", "containerd"}} {
		var got []string
		for _, u := range f[name].componentUpdates("") {
			got = append(got, u.component)
		}
		if !slices.Equal(got, want) {
			t.Errorf("%s updated %v, want %v", name, got, want)
		}
	}
	want := []string{
		"node1 the agent 2.0.0 true", "node1 the updater 2.0.1 true", "node1 containerd 3.0.0 false",
		"node2 the agent 2.0.0 false", "node2 the updater 2.0.1 false", "node2 containerd 3.0.0 false",
	}
	if !slices.Equal(told, want) {
		t.Errorf("the update told\n%s\nwant\n%s", strings.Join(told, "\n"), strings.Join(want, "\n"))
	}
}

func TestUpdateNodesStopsAtTheFirstStepThatFailsNamingTheNodeAndTheStep(t *testing.T) {
	for _, c := range []struct {
		failing string
		// want is in the error, and done are the steps node2 took.
		want string
		done []string
	}{
		{"agent", "updating the agent to 2.0.0", []string{"agent"}},
		{"updater", "updating the updater to 2.0.1", []string{"agent", "updater"}},
		{"containerd", "updating containerd to 3.0.0", []string{"agent", "updater", "containerd"}},
	} {
		t.Run(c.failing, func(t *testing.T) {
			f := fakes{"node1": {generation: 1}, "node2": {generation: 1, failing: c.failing, updateErr: errors.New("it broke")}, "node3": {generation: 1}}
			cl, err := orchestrate.Preflight(t.Context(), inOrder(f, "node1", "node2", "node3"), 1, false, holder)
			if err != nil {
				t.Fatal(err)
			}
			defer cl.Close()
			err = update(cl)
			var ne *orchestrate.NodeError
			if !errors.As(err, &ne) || ne.Node != "node2" {
				t.Fatalf("UpdateNodes: %v, want a NodeError for node2", err)
			}
			wantContains(t, "the error", err.Error(), c.want+": it broke")
			if n := len(f["node1"].componentUpdates("")); n != 3 {
				t.Errorf("node1 had %d updates, want 3", n)
			}
			var done []string
			for _, u := range f["node2"].componentUpdates("") {
				done = append(done, u.component)
			}
			if !slices.Equal(done, c.done) {
				t.Errorf("node2 updated %v, want %v", done, c.done)
			}
			if n := len(f["node3"].componentUpdates("")); n != 0 {
				t.Errorf("node3 had %d updates after node2 failed, want none", n)
			}
		})
	}
}

func TestUpdateNodesStopsBeforeTheNextStepOnceTheLockIsLost(t *testing.T) {
	f := fakes{"node1": {generation: 1, updateDelay: 500 * time.Millisecond}}
	f["node1"].onUpdateOf = map[string]func(){"updater": func() { f["node1"].BreakLock(context.Background()) }}
	nodes := inOrder(f, "node1")
	nodes.LeaseTTL = 600 * time.Millisecond
	cl, err := orchestrate.Preflight(t.Context(), nodes, 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if err := update(cl); !errors.Is(err, orchestrate.ErrLockLost) {
		t.Fatalf("UpdateNodes: %v, want ErrLockLost", err)
	}
	if n := len(f["node1"].componentUpdates("containerd")); n != 0 {
		t.Errorf("containerd was updated %d times once the lock was lost, want never", n)
	}
}

func TestUpdateNodesFailsNamingANodeWithoutItsAgent(t *testing.T) {
	f := fakes{"node1": {generation: 1}}
	cl, err := orchestrate.Preflight(t.Context(), inOrder(f, "node1"), 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	err = cl.UpdateNodes(cl.Context(), func(string) (orchestrate.NodeRelease, error) {
		return orchestrate.NodeRelease{}, errors.New("no agent for riscv64")
	}, func(orchestrate.Step) {})
	var ne *orchestrate.NodeError
	if !errors.As(err, &ne) || ne.Node != "node1" {
		t.Fatalf("UpdateNodes: %v, want a NodeError for node1", err)
	}
	if n := len(f["node1"].componentUpdates("")); n != 0 {
		t.Errorf("node1 was updated %d times, want never", n)
	}
}

func TestUpdateNodesRenewsTheLeaseOnANodeOnceItsAgentRestarts(t *testing.T) {
	f := fakes{"node1": {generation: 1, restartFor: 300 * time.Millisecond}}
	nodes := inOrder(f, "node1")
	nodes.LeaseTTL = time.Minute
	cl, err := orchestrate.Preflight(t.Context(), nodes, 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if err := update(cl); err != nil {
		t.Fatal(err)
	}
	// keepLease renews only every twenty seconds, so a lease that runs
	// out a minute after the agent came back was renewed by the update.
	ended := f["node1"].agentUpdates()[0].ended
	if got, want := f["node1"].leaseExpires(), ended.Add(time.Minute+300*time.Millisecond); got.Before(want) {
		t.Errorf("node1's lease runs out %s after its update ended, want it renewed once the agent came back", got.Sub(ended))
	}
}

func TestUpdateNodesFailsWhenARestartedAgentHoldsAnotherLease(t *testing.T) {
	f := fakes{"node1": {generation: 1}, "node2": {generation: 1}}
	f["node1"].onUpdate = func() { f["node1"].lockFor("bobs", time.Minute) }
	cl, err := orchestrate.Preflight(t.Context(), inOrder(f, "node1", "node2"), 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	err = update(cl)
	if !errors.Is(err, orchestrate.ErrLockLost) {
		t.Fatalf("UpdateNodes: %v, want ErrLockLost", err)
	}
	if n := len(f["node2"].agentUpdates()); n != 0 {
		t.Errorf("node2 was updated %d times once the lock was lost, want never", n)
	}
}

func TestUpdateNodesStopsBeforeTheNextNodeOnceTheLockIsLost(t *testing.T) {
	f := fakes{"node1": {generation: 1, updateDelay: 500 * time.Millisecond}, "node2": {generation: 1}}
	f["node1"].onUpdate = func() { f["node2"].BreakLock(context.Background()) }
	nodes := inOrder(f, "node1", "node2")
	nodes.LeaseTTL = 600 * time.Millisecond
	cl, err := orchestrate.Preflight(t.Context(), nodes, 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	err = update(cl)
	if !errors.Is(err, orchestrate.ErrLockLost) {
		t.Fatalf("UpdateNodes: %v, want ErrLockLost", err)
	}
	if n := len(f["node2"].agentUpdates()); n != 0 {
		t.Errorf("node2 was updated %d times once the lock was lost, want never", n)
	}
}

func TestUpdateNodesFailsNamingANodeWhoseRestartedAgentDoesntAnswer(t *testing.T) {
	f := fakes{"node1": {generation: 1, restartFor: time.Hour}, "node2": {generation: 1}}
	nodes := inOrder(f, "node1", "node2")
	nodes.LeaseTTL = 3 * time.Second
	cl, err := orchestrate.Preflight(t.Context(), nodes, 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	err = update(cl)
	var ne *orchestrate.NodeError
	if !errors.As(err, &ne) || ne.Node != "node1" {
		t.Fatalf("UpdateNodes: %v, want a NodeError for node1", err)
	}
	wantContains(t, "the error", err.Error(), "doesn't answer mooctl", "the agent is restarting")
	if n := len(f["node2"].agentUpdates()); n != 0 {
		t.Errorf("node2 was updated %d times, want never", n)
	}
}

func TestUpdateNodesFailsWhenTheLastNodesRestartedAgentHoldsAnotherLease(t *testing.T) {
	f := fakes{"node1": {generation: 1}}
	f["node1"].onUpdate = func() { f["node1"].lockFor("bobs", time.Minute) }
	cl, err := orchestrate.Preflight(t.Context(), inOrder(f, "node1"), 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if err := update(cl); !errors.Is(err, orchestrate.ErrLockLost) {
		t.Errorf("UpdateNodes: %v, want ErrLockLost", err)
	}
}

// The agent stops answering renewals a third of a lease time before its
// restart, as when the last renewal before it was answered just after
// the preflight. The swap and the rollback both run out their time, and
// the lease must outlast them, so that the update fails with why, and
// not with the lock lost.
func TestARollbackEndsBeforeTheLeaseCanRunOut(t *testing.T) {
	f := fakes{"node1": {generation: 1, rollsBack: true}}
	nodes := inOrder(f, "node1")
	nodes.LeaseTTL = 1500 * time.Millisecond
	cl, err := orchestrate.Preflight(t.Context(), nodes, 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	f["node1"].mu.Lock()
	f["node1"].downUntil = time.Now().Add(time.Hour)
	f["node1"].mu.Unlock()
	slowly := func(name string) (orchestrate.NodeRelease, error) {
		time.Sleep(450 * time.Millisecond)
		return releaseOf(name)
	}
	err = cl.UpdateNodes(cl.Context(), slowly, func(orchestrate.Step) {})
	if errors.Is(err, orchestrate.ErrLockLost) || err == nil || !strings.Contains(err.Error(), "rolled back to the agent it replaced") {
		t.Errorf("UpdateNodes: %v, want the rollback's error, before the lease may run out", err)
	}
}
