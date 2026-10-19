package cli_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestNodeAddIsRefusedWhileAnotherOperationHoldsTheLock(t *testing.T) {
	o := newOperator(t, "op")
	source := preflightCluster(t, o)
	o.nodes["node2"].locked(bobsLock)
	nodeAddRefused(t, o, source, "node2: it is locked by bob on desktop since 2026-10-19", "and expires at 2026-10-19 "+bobsLock.Until.Local().Format("15:04:05 MST unless renewed"), "mooctl cluster unlock")
	for _, name := range []string{"node1", "node3"} {
		if l := o.nodes[name].heldLock(); l != nil {
			t.Errorf("after the refused node add, %s has the lock %+v, want it handed back", name, l)
		}
	}
	if l := o.nodes["node2"].heldLock(); l == nil || l.Lease != bobsLock.Lease {
		t.Errorf("after the refused node add, node2 has the lock %+v, want bob's", l)
	}
}

func TestNodeAddTakesTheLockAsTheOperatorOnThisMachineAndReleasesIt(t *testing.T) {
	o := newOperator(t, "op")
	source := preflightCluster(t, o)
	nodeAddPassesPreflight(t, o, source)
	for name, n := range o.nodes {
		granted := n.granted()
		if len(granted) != 1 || granted[0].Holder != "alice" || granted[0].Machine != "laptop" {
			t.Errorf("%s granted the locks %+v, want one to alice on laptop", name, granted)
		}
		if l := n.heldLock(); l != nil {
			t.Errorf("after node add ended, %s still has the lock %+v", name, l)
		}
	}
}

func TestClusterStatusShowsWhoHoldsTheLockWithoutTakingIt(t *testing.T) {
	o := newOperator(t, "op")
	source := statusCluster(t, o, "node1", "node2", "node3")
	o.nodes = map[string]*fakeNode{"node1": healthyNode(1), "node2": healthyNode(1), "node3": healthyNode(1)}
	r := o.mustRun("--release-source", source, "cluster", "status")
	wantContains(t, "cluster status of a free cluster", r.stdout, "\nOperation lock: free\n")

	for _, n := range o.nodes {
		n.locked(bobsLock)
	}
	r = o.mustRun("--release-source", source, "cluster", "status")
	wantContains(t, "cluster status of a locked cluster", r.stdout, "\nOperation lock: held by bob on desktop since 2026-10-19")
	if strings.Contains(r.stdout, "node1, node2") {
		t.Errorf("cluster status lists the nodes of a lock every node holds:\n%s", r.stdout)
	}

	// A lock that only some nodes hold says which.
	o.nodes["node2"].lock = nil
	r = o.mustRun("--release-source", source, "cluster", "status", "--json")
	var view struct {
		Lock []struct {
			Holder, Machine, Since string
			Nodes                  []string
		}
	}
	if err := json.Unmarshal([]byte(r.stdout), &view); err != nil {
		t.Fatal(err)
	}
	if len(view.Lock) != 1 || view.Lock[0].Holder != "bob" || view.Lock[0].Machine != "desktop" ||
		!strings.HasPrefix(view.Lock[0].Since, "2026-10-19T09:30:00") || strings.Join(view.Lock[0].Nodes, " ") != "node1 node3" {
		t.Errorf("cluster status --json shows the lock %+v, want bob's on node1 and node3", view.Lock)
	}
	r = o.mustRun("--release-source", source, "cluster", "status")
	wantContains(t, "cluster status of a partly locked cluster", r.stdout, "since 2026-10-19", " on node1, node3\n")

	for name, n := range o.nodes {
		if n.acquired > 0 {
			t.Errorf("cluster status asked %s for the lock", name)
		}
	}
}

func TestClusterUnlockBreaksTheLockAndSaysWhoHeldIt(t *testing.T) {
	o := newOperator(t, "op")
	source := statusCluster(t, o, "node1", "node2", "node3")
	o.nodes = map[string]*fakeNode{"node1": healthyNode(1).locked(bobsLock), "node2": healthyNode(1).locked(bobsLock), "node3": healthyNode(1)}
	r := o.mustRun("--release-source", source, "cluster", "unlock")
	wantContains(t, "cluster unlock's output", r.stdout, "Broke the operation lock held by bob on desktop since 2026-10-19", "on node1, node2")
	wantContains(t, "cluster unlock's warning", r.stderr, "If that mooctl is still running", "until 2026-10-19 "+bobsLock.Until.Local().Format("15:04:05 MST at the latest"), "overlap")
	for name, n := range o.nodes {
		if l := n.heldLock(); l != nil {
			t.Errorf("after cluster unlock, %s has the lock %+v", name, l)
		}
	}
	r = o.mustRun("--release-source", source, "cluster", "unlock")
	wantContains(t, "cluster unlock's output on a free cluster", r.stdout, "No node held the operation lock.")
	if r.stderr != "" {
		t.Errorf("cluster unlock of a free cluster warned:\n%s", r.stderr)
	}
}

func TestClusterUnlockNamesTheNodesItCouldntReach(t *testing.T) {
	o := newOperator(t, "op")
	source := statusCluster(t, o, "node1", "node2")
	o.nodes = map[string]*fakeNode{"node1": healthyNode(1).locked(bobsLock), "node2": {err: errors.New("no route to host")}}
	r := o.run("--release-source", source, "cluster", "unlock")
	if r.err == nil {
		t.Fatal("cluster unlock succeeded with node2 unreachable")
	}
	wantContains(t, "cluster unlock's error", r.err.Error(), "node2: no route to host")
	wantContains(t, "cluster unlock's output", r.stdout, "Broke the operation lock held by bob", "on node1")
	if l := o.nodes["node1"].heldLock(); l != nil {
		t.Errorf("after cluster unlock, node1 has the lock %+v", l)
	}
}
