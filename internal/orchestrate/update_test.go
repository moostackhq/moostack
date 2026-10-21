package orchestrate_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/moostackhq/moostack/internal/orchestrate"
)

// agentOf gives each node an agent whose binary names the node.
func agentOf(name string) ([]byte, error) { return []byte("agent for " + name), nil }

// inOrder are fakes' nodes in the order of names.
func inOrder(f fakes, names ...string) orchestrate.Nodes {
	return orchestrate.Nodes{Names: names, Dial: f.dial}
}

func TestUpdateAgentsUpdatesOneNodeAtATimeInOrder(t *testing.T) {
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
	err = cl.UpdateAgents(cl.Context(), "2.0.0", agentOf, func(name string) { told = append(told, name) })
	if err != nil {
		t.Fatal(err)
	}
	var previous time.Time
	for i, name := range names {
		updates := f[name].agentUpdates()
		if len(updates) != 1 {
			t.Fatalf("%s's agent was updated %d times, want once", name, len(updates))
		}
		u := updates[0]
		if u.version != "2.0.0" || u.binary != "agent for "+name {
			t.Errorf("%s got the agent %s, %q, want 2.0.0, its own", name, u.version, u.binary)
		}
		// The new agent has a fifth of a lease time to become healthy.
		if u.healthTimeout != 2*time.Second {
			t.Errorf("%s's health timeout is %s, want a fifth of the lease time, 2s", name, u.healthTimeout)
		}
		if u.started.Before(previous) {
			t.Errorf("%s's update started before the previous node's ended", name)
		}
		previous = u.ended
		if told[i] != name {
			t.Errorf("the update told %v, want each node in order", told)
		}
	}
}

func TestUpdateAgentsStopsAtTheFirstNodeThatFails(t *testing.T) {
	f := fakes{"node1": {generation: 1}, "node2": {generation: 1, updateErr: errors.New("the new agent didn't become healthy")}, "node3": {generation: 1}}
	cl, err := orchestrate.Preflight(t.Context(), inOrder(f, "node1", "node2", "node3"), 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	err = cl.UpdateAgents(cl.Context(), "2.0.0", agentOf, func(string) {})
	var ne *orchestrate.NodeError
	if !errors.As(err, &ne) || ne.Node != "node2" {
		t.Fatalf("UpdateAgents: %v, want a NodeError for node2", err)
	}
	wantContains(t, "the error", err.Error(), "updating the agent to 2.0.0", "the new agent didn't become healthy")
	if n := len(f["node1"].agentUpdates()); n != 1 {
		t.Errorf("node1 was updated %d times, want once", n)
	}
	if n := len(f["node3"].agentUpdates()); n != 0 {
		t.Errorf("node3 was updated %d times after node2 failed, want never", n)
	}
}

func TestUpdateAgentsFailsNamingANodeWithoutItsAgent(t *testing.T) {
	f := fakes{"node1": {generation: 1}}
	cl, err := orchestrate.Preflight(t.Context(), inOrder(f, "node1"), 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	err = cl.UpdateAgents(cl.Context(), "2.0.0", func(string) ([]byte, error) { return nil, errors.New("no agent for riscv64") }, func(string) {})
	var ne *orchestrate.NodeError
	if !errors.As(err, &ne) || ne.Node != "node1" {
		t.Fatalf("UpdateAgents: %v, want a NodeError for node1", err)
	}
	if n := len(f["node1"].agentUpdates()); n != 0 {
		t.Errorf("node1 was updated %d times, want never", n)
	}
}

func TestUpdateAgentsRenewsTheLeaseOnANodeOnceItsAgentRestarts(t *testing.T) {
	f := fakes{"node1": {generation: 1, restartFor: 300 * time.Millisecond}}
	nodes := inOrder(f, "node1")
	nodes.LeaseTTL = time.Minute
	cl, err := orchestrate.Preflight(t.Context(), nodes, 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if err := cl.UpdateAgents(cl.Context(), "2.0.0", agentOf, func(string) {}); err != nil {
		t.Fatal(err)
	}
	// keepLease renews only every twenty seconds, so a lease that runs
	// out a minute after the agent came back was renewed by the update.
	ended := f["node1"].agentUpdates()[0].ended
	if got, want := f["node1"].leaseExpires(), ended.Add(time.Minute+300*time.Millisecond); got.Before(want) {
		t.Errorf("node1's lease runs out %s after its update ended, want it renewed once the agent came back", got.Sub(ended))
	}
}

func TestUpdateAgentsFailsWhenARestartedAgentHoldsAnotherLease(t *testing.T) {
	f := fakes{"node1": {generation: 1}, "node2": {generation: 1}}
	f["node1"].onUpdate = func() { f["node1"].lockFor("bobs", time.Minute) }
	cl, err := orchestrate.Preflight(t.Context(), inOrder(f, "node1", "node2"), 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	err = cl.UpdateAgents(cl.Context(), "2.0.0", agentOf, func(string) {})
	if !errors.Is(err, orchestrate.ErrLockLost) {
		t.Fatalf("UpdateAgents: %v, want ErrLockLost", err)
	}
	if n := len(f["node2"].agentUpdates()); n != 0 {
		t.Errorf("node2 was updated %d times once the lock was lost, want never", n)
	}
}

func TestUpdateAgentsStopsBeforeTheNextNodeOnceTheLockIsLost(t *testing.T) {
	f := fakes{"node1": {generation: 1, updateDelay: 500 * time.Millisecond}, "node2": {generation: 1}}
	f["node1"].onUpdate = func() { f["node2"].BreakLock(context.Background()) }
	nodes := inOrder(f, "node1", "node2")
	nodes.LeaseTTL = 600 * time.Millisecond
	cl, err := orchestrate.Preflight(t.Context(), nodes, 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	err = cl.UpdateAgents(cl.Context(), "2.0.0", agentOf, func(string) {})
	if !errors.Is(err, orchestrate.ErrLockLost) {
		t.Fatalf("UpdateAgents: %v, want ErrLockLost", err)
	}
	if n := len(f["node2"].agentUpdates()); n != 0 {
		t.Errorf("node2 was updated %d times once the lock was lost, want never", n)
	}
}

func TestUpdateAgentsFailsNamingANodeWhoseRestartedAgentDoesntAnswer(t *testing.T) {
	f := fakes{"node1": {generation: 1, restartFor: time.Hour}, "node2": {generation: 1}}
	nodes := inOrder(f, "node1", "node2")
	nodes.LeaseTTL = 3 * time.Second
	cl, err := orchestrate.Preflight(t.Context(), nodes, 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	err = cl.UpdateAgents(cl.Context(), "2.0.0", agentOf, func(string) {})
	var ne *orchestrate.NodeError
	if !errors.As(err, &ne) || ne.Node != "node1" {
		t.Fatalf("UpdateAgents: %v, want a NodeError for node1", err)
	}
	wantContains(t, "the error", err.Error(), "doesn't answer mooctl", "the agent is restarting")
	if n := len(f["node2"].agentUpdates()); n != 0 {
		t.Errorf("node2 was updated %d times, want never", n)
	}
}

func TestUpdateAgentsFailsWhenTheLastNodesRestartedAgentHoldsAnotherLease(t *testing.T) {
	f := fakes{"node1": {generation: 1}}
	f["node1"].onUpdate = func() { f["node1"].lockFor("bobs", time.Minute) }
	cl, err := orchestrate.Preflight(t.Context(), inOrder(f, "node1"), 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if err := cl.UpdateAgents(cl.Context(), "2.0.0", agentOf, func(string) {}); !errors.Is(err, orchestrate.ErrLockLost) {
		t.Errorf("UpdateAgents: %v, want ErrLockLost", err)
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
	slowly := func(name string) ([]byte, error) {
		time.Sleep(450 * time.Millisecond)
		return agentOf(name)
	}
	err = cl.UpdateAgents(cl.Context(), "2.0.0", slowly, func(string) {})
	if errors.Is(err, orchestrate.ErrLockLost) || err == nil || !strings.Contains(err.Error(), "rolled back to the agent it replaced") {
		t.Errorf("UpdateAgents: %v, want the rollback's error, before the lease may run out", err)
	}
}
