//go:build linux

package agent_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/hostlayout"
	"github.com/moostackhq/moostack/internal/identity"
	"github.com/moostackhq/moostack/internal/nodetest"
)

// These tests remove an identity issued under another node's name, not
// the identity that node's agent runs with: the node keeps sending while
// the removal has it forgotten.

// holdFor is how long a test watches a view. A member gossiped or heard
// from comes back within it.
const holdFor = 5 * time.Second

// joined starts a cluster of the nodes called names, each seeing the
// others alive at generation 1, and holds each one's operation lock as
// the lease "op".
func joined(t *testing.T, names ...string) []*nodetest.Node {
	t.Helper()
	nodes := []*nodetest.Node{nodetest.Start(t)}
	for _, name := range names {
		nodes = append(nodes, nodetest.StartIn(t, nodes[0], name))
	}
	var listing []*nodev1.GenerationNode
	want := map[string]string{}
	for _, n := range nodes {
		n.EventuallyHealthy(t)
		listing = append(listing, nodeOf(n))
		want[n.NodeName] = alive(n)
	}
	for _, n := range nodes {
		acquire(t, n, "op", time.Hour)
		recordAs(t, n, "op", 1, listing...)
	}
	for _, n := range nodes {
		eventuallyMembers(t, n, 30*time.Second, want)
	}
	return nodes
}

// removeAs has n hold ids as removed, as the operation of the lease "op",
// which must succeed.
func removeAs(t *testing.T, n *nodetest.Node, ids ...*identity.Identity) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if _, err := n.Agent.RemoveIdentities(ctx, &nodev1.RemoveIdentitiesRequest{Identities: removedOf(ids...), Lease: "op"}); err != nil {
		t.Fatalf("removing %v: %v", removedOf(ids...), err)
	}
}

// openMembership connects to n's membership as id, and keeps the
// connection until the test ends.
func openMembership(t *testing.T, n *nodetest.Node, id *identity.Identity) *quic.Conn {
	t.Helper()
	cfg := id.ClientTLS(n.NodeName, nil)
	cfg.NextProtos = []string{membershipALPN}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	conn, err := quic.DialAddr(ctx, n.PeerAddress.String(), cfg, &quic.Config{EnableDatagrams: true, KeepAlivePeriod: time.Second})
	if err != nil {
		t.Fatalf("connecting as %s: %v", id.Node, err)
	}
	t.Cleanup(func() { conn.CloseWithError(0, "") })
	return conn
}

func TestRemovingAnIdentityCutsOffItsMembershipConnections(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	removed := nodetest.PeerIdentity(t, n.CA, "node-2")
	kept := nodetest.PeerIdentity(t, n.CA, "node-3")
	// The old identity of node-4 has no connection open.
	oldIdentity := nodetest.PeerIdentity(t, n.CA, "node-4")
	sameName := nodetest.PeerIdentity(t, n.CA, "node-4")
	removedConn := openMembership(t, n, removed)
	sameNameConn := openMembership(t, n, sameName)
	keptConn := openMembership(t, n, kept)

	removeIdentities(t, n, removed, oldIdentity)
	select {
	case <-removedConn.Context().Done():
	case <-time.After(10 * time.Second):
		t.Fatal("a removed identity's membership connection, open before the removal, is still open")
	}
	var cut *quic.ApplicationError
	if !errors.As(context.Cause(removedConn.Context()), &cut) || cut.ErrorMessage != "removed" {
		t.Errorf("the connection ended with %v, want the node cutting it off as removed", context.Cause(removedConn.Context()))
	}
	if err := checkMembership(t, n, removed.ClientTLS(n.NodeName, nil)); err == nil {
		t.Error("a removed identity connected again")
	}
	select {
	case <-sameNameConn.Context().Done():
		t.Errorf("a new identity under the removed name was cut off: %v", context.Cause(sameNameConn.Context()))
	case <-keptConn.Context().Done():
		t.Errorf("another node's identity was cut off: %v", context.Cause(keptConn.Context()))
	case <-time.After(2 * time.Second):
	}
	if err := checkMembership(t, n, sameName.ClientTLS(n.NodeName, nil)); err != nil {
		t.Errorf("a new identity under the removed name can't connect: %v", err)
	}
}

// Whichever of the two reaches the node first, it holds the removed node
// out once it has both, while the node keeps sending.
func TestARemovedNodeIsForgottenWhicheverOrderItIsTold(t *testing.T) {
	for name, removeFirst := range map[string]bool{"the removal first": true, "the recording first": false} {
		t.Run(name, func(t *testing.T) {
			nodes := joined(t, "node-2")
			n1, n2 := nodes[0], nodes[1]
			old := nodetest.PeerIdentity(t, n1.CA, n2.NodeName)
			if removeFirst {
				removeAs(t, n1, old)
				recordAs(t, n1, "op", 2, nodeOf(n1))
			} else {
				recordAs(t, n1, "op", 2, nodeOf(n1))
				removeAs(t, n1, old)
			}
			keepsMembers(t, n1, holdFor, map[string]string{n1.NodeName: alive(n1)})
		})
	}
}

// A removed name the generation lists stays until a recording leaves it
// out.
func TestARemovedNameTheGenerationListsStaysUntilARecordingLeavesItOut(t *testing.T) {
	nodes := joined(t, "node-2")
	n1, n2 := nodes[0], nodes[1]
	both := map[string]string{n1.NodeName: alive(n1), n2.NodeName: alive(n2)}
	removeAs(t, n1, nodetest.PeerIdentity(t, n1.CA, n2.NodeName))
	keepsMembers(t, n1, holdFor, both)
	recordAs(t, n1, "op", 2, nodeOf(n1), nodeOf(n2))
	keepsMembers(t, n1, holdFor, both)
	recordAs(t, n1, "op", 3, nodeOf(n1))
	keepsMembers(t, n1, holdFor, map[string]string{n1.NodeName: alive(n1)})
}

// A node that forgot a name takes it back from a recording that lists it,
// as when a node is added again.
func TestARecordingThatListsAForgottenNameBringsItBack(t *testing.T) {
	nodes := joined(t, "node-2")
	n1, n2 := nodes[0], nodes[1]
	removeAs(t, n1, nodetest.PeerIdentity(t, n1.CA, n2.NodeName))
	recordAs(t, n1, "op", 2, nodeOf(n1))
	keepsMembers(t, n1, time.Second, map[string]string{n1.NodeName: alive(n1)})
	recordAs(t, n1, "op", 3, nodeOf(n1), nodeOf(n2))
	eventuallyMembers(t, n1, 30*time.Second, map[string]string{n1.NodeName: alive(n1), n2.NodeName: alive(n2)})
}

// Only node-1 is told. node-2 keeps gossiping node-3 alive to it, and
// node-3 keeps sending.
func TestAForgottenNodeStaysOutWhileAnotherGossipsItAlive(t *testing.T) {
	nodes := joined(t, "node-2", "node-3")
	n1, n2, n3 := nodes[0], nodes[1], nodes[2]
	removeAs(t, n1, nodetest.PeerIdentity(t, n1.CA, n3.NodeName))
	recordAs(t, n1, "op", 2, nodeOf(n1), nodeOf(n2))
	keepsMembers(t, n1, 2*holdFor, map[string]string{n1.NodeName: alive(n1), n2.NodeName: alive(n2)})
	if got, err := members(t, n2); err != nil {
		t.Fatal(err)
	} else if got[n3.NodeName] != alive(n3) {
		t.Errorf("node-2 holds node-3 as %q, so it gossiped nothing of it alive", got[n3.NodeName])
	}
}

func TestARestartForgetsRemovedNamesAgainButNotOneTheGenerationLists(t *testing.T) {
	nodes := joined(t, "node-2", "node-3")
	n1, n2, n3 := nodes[0], nodes[1], nodes[2]
	removeAs(t, n1, nodetest.PeerIdentity(t, n1.CA, n2.NodeName), nodetest.PeerIdentity(t, n1.CA, n3.NodeName))
	// node-2 is gone from the generation, and node-3 is a node added again
	// under its name.
	recordAs(t, n1, "op", 2, nodeOf(n1), nodeOf(n3))
	n1.Restart(t, "agent")
	n1.EventuallyHealthy(t)
	want := map[string]string{n1.NodeName: alive(n1), n3.NodeName: alive(n3)}
	eventuallyMembers(t, n1, 30*time.Second, want)
	keepsMembers(t, n1, 2*holdFor, want)
}

// A generation that doesn't list the node itself, with its own name on
// the list of removed identities, leaves it in its view.
func TestRemovingTheNodesOwnNameLeavesItInItsView(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	record(t, n, 1)
	releaseLock(t, n, "record")
	removeIdentities(t, n, nodetest.PeerIdentity(t, n.CA, n.NodeName))
	keepsMembers(t, n, time.Second, map[string]string{n.NodeName: alive(n)})
	n.Restart(t, "agent")
	n.EventuallyHealthy(t)
	keepsMembers(t, n, time.Second, map[string]string{n.NodeName: alive(n)})
}

// A node that can't read its generation forgets nothing until a recording.
func TestAnUnreadableGenerationForgetsNothingUntilARecording(t *testing.T) {
	both := func(n1, n2 *nodetest.Node) map[string]string {
		return map[string]string{n1.NodeName: alive(n1), n2.NodeName: alive(n2)}
	}
	t.Run("at a removal", func(t *testing.T) {
		nodes := joined(t, "node-2")
		n1, n2 := nodes[0], nodes[1]
		n1.Exec(t, "root", "sh", "-c", "echo garbage > "+hostlayout.GenerationFile)
		removeAs(t, n1, nodetest.PeerIdentity(t, n1.CA, n2.NodeName))
		keepsMembers(t, n1, holdFor, both(n1, n2))
		recordAs(t, n1, "op", 2, nodeOf(n1))
		keepsMembers(t, n1, time.Second, map[string]string{n1.NodeName: alive(n1)})
	})
	t.Run("at start", func(t *testing.T) {
		nodes := joined(t, "node-2")
		n1, n2 := nodes[0], nodes[1]
		removeAs(t, n1, nodetest.PeerIdentity(t, n1.CA, n2.NodeName))
		n1.Exec(t, "root", "sh", "-c", "echo garbage > "+hostlayout.GenerationFile)
		n1.Restart(t, "agent")
		n1.EventuallyHealthy(t)
		// node-2 joins it again, as it holds node-1 as a seed.
		eventuallyMembers(t, n1, 30*time.Second, both(n1, n2))
		keepsMembers(t, n1, holdFor, both(n1, n2))
	})
}

// A new node holds the removed identities from its install, a node added
// again under one of the names among them. It has recorded no generation.
func TestANewNodeForgetsNothingBeforeItsFirstRecording(t *testing.T) {
	n1 := nodetest.Start(t)
	n2 := nodetest.StartIn(t, n1, "node-2")
	n1.EventuallyHealthy(t)
	n2.EventuallyHealthy(t)
	removeIdentities(t, n1, nodetest.PeerIdentity(t, n1.CA, n2.NodeName))
	n1.Restart(t, "agent")
	n1.EventuallyHealthy(t)
	record(t, n2, 1, nodeOf(n1), nodeOf(n2))
	want := map[string]string{n1.NodeName: alive(n1), n2.NodeName: alive(n2)}
	eventuallyMembers(t, n1, 30*time.Second, want)
	keepsMembers(t, n1, holdFor, want)
}

// A crash between the list's write and what follows it leaves a name the
// hook never saw: the agent forgets it as it starts.
func TestACrashAfterTheListIsWrittenForgetsAtTheNextStart(t *testing.T) {
	nodes := joined(t, "node-2")
	n1, n2 := nodes[0], nodes[1]
	recordAs(t, n1, "op", 2, nodeOf(n1))
	old := nodetest.PeerIdentity(t, n1.CA, n2.NodeName)
	list := hostlayout.MarshalRemovedIdentities([]hostlayout.RemovedIdentity{{Node: old.Node, Identity: old.Fingerprint()}})
	n1.Exec(t, "root", "sh", "-c", "printf %s '"+string(list)+"' > "+hostlayout.RemovedIdentitiesFile)
	n1.Crash(t, "agent")
	n1.EventuallyHealthy(t)
	keepsMembers(t, n1, holdFor, map[string]string{n1.NodeName: alive(n1)})
}

func TestTheAgentRefusesARemovedIdentityItCantUse(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	acquire(t, n, "remove", time.Minute)
	id := nodetest.PeerIdentity(t, n.CA, "node-2")
	goodID := nodetest.PeerIdentity(t, n.CA, "node-3")
	good := &nodev1.RemovedIdentity{Node: goodID.Node, Identity: goodID.Fingerprint()}
	for name, bad := range map[string]*nodev1.RemovedIdentity{
		"no node":               {Identity: id.Fingerprint()},
		"no fingerprint":        {Node: "node-2"},
		"neither":               {},
		"no fingerprint's form": {Node: "node-2", Identity: "not a fingerprint"},
	} {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		_, err := n.Agent.RemoveIdentities(ctx, &nodev1.RemoveIdentitiesRequest{Identities: []*nodev1.RemovedIdentity{good, bad}, Lease: "remove"})
		cancel()
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: %v, want InvalidArgument", name, err)
		}
	}
	// Nothing of a refused request is taken, the good entry beside the bad
	// one included.
	for _, peer := range []*identity.Identity{id, goodID} {
		if err := checkPeer(t, n, peer.ClientTLS(n.NodeName, nil)); err != nil {
			t.Errorf("after the refused requests, %s was refused: %v", peer.Node, err)
		}
		if err := checkMembership(t, n, peer.ClientTLS(n.NodeName, nil)); err != nil {
			t.Errorf("after the refused requests, membership refused %s: %v", peer.Node, err)
		}
	}
}
