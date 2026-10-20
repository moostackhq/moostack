//go:build linux

package agent_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/hostlayout"
	"github.com/moostackhq/moostack/internal/identity"
	"github.com/moostackhq/moostack/internal/nodetest"
)

func TestThePeerListenerRefusesARemovedIdentity(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	removed := nodetest.PeerIdentity(t, n.CA, "node-2")
	kept := nodetest.PeerIdentity(t, n.CA, "node-3")
	removeIdentities(t, n, removed)
	if err := checkPeer(t, n, removed.ClientTLS(n.NodeName, nil)); err == nil {
		t.Error("a peer whose identity is removed was accepted")
	}
	if err := checkPeer(t, n, kept.ClientTLS(n.NodeName, nil)); err != nil {
		t.Errorf("a peer whose identity isn't removed was refused: %v", err)
	}
}

func TestRemovingAnIdentityCutsOffItsOpenConnections(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	removed := nodetest.PeerIdentity(t, n.CA, "node-2")
	kept := nodetest.PeerIdentity(t, n.CA, "node-3")
	removedConn, removedWatch := watchHealth(t, n, removed)
	keptConn, keptWatch := watchHealth(t, n, kept)

	removeIdentities(t, n, removed)
	ended := make(chan error, 1)
	go func() {
		for {
			if _, err := removedWatch.Recv(); err != nil {
				ended <- err
				return
			}
		}
	}()
	select {
	case <-ended:
	case <-time.After(10 * time.Second):
		t.Fatal("a removed peer's health watch, open before the removal, still runs")
	}
	// The connection the removed peer had open takes no more calls, and
	// it can't connect again.
	if err := checkConn(t, removedConn); err == nil {
		t.Error("a removed peer's connection, open before the removal, took a call after it")
	}
	// The kept peer's connection and watch are untouched.
	if err := checkConn(t, keptConn); err != nil {
		t.Errorf("a kept peer's connection was cut off: %v", err)
	}
	go func() {
		_, err := keptWatch.Recv()
		ended <- err
	}()
	select {
	case err := <-ended:
		t.Errorf("a kept peer's health watch ended: %v", err)
	case <-time.After(time.Second):
	}
}

func TestTheAgentRemovesIdentitiesOnlyForTheLeaseThatHoldsTheLock(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	removed := nodetest.PeerIdentity(t, n.CA, "node-2")
	acquire(t, n, "a", time.Minute)
	for _, lease := range []string{"", "b"} {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		_, err := n.Agent.RemoveIdentities(ctx, &nodev1.RemoveIdentitiesRequest{Identities: []string{removed.Fingerprint()}, Lease: lease})
		cancel()
		if status.Code(err) != codes.FailedPrecondition {
			t.Errorf("removing an identity with lease %q while a holds the lock: %v, want FailedPrecondition", lease, err)
		}
	}
	if err := checkPeer(t, n, removed.ClientTLS(n.NodeName, nil)); err != nil {
		t.Errorf("after the refused removals, the peer was refused: %v", err)
	}
}

func TestTheRemovedIdentitiesOnlyGrowAndOutliveARestart(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	first := nodetest.PeerIdentity(t, n.CA, "node-2")
	second := nodetest.PeerIdentity(t, n.CA, "node-3")
	removeIdentities(t, n, first)
	// A list without the first, as an older cluster directory has it,
	// adds the second and keeps the first; the same list again changes
	// nothing.
	removeIdentities(t, n, second)
	removeIdentities(t, n, second)
	n.Restart(t, "agent")
	n.EventuallyHealthy(t)
	for _, id := range []*identity.Identity{first, second} {
		if err := checkPeer(t, n, id.ClientTLS(n.NodeName, nil)); err == nil {
			t.Errorf("after a restart, %s, whose identity is removed, was accepted", id.Node)
		}
	}
	if err := checkPeer(t, n, nodetest.PeerIdentity(t, n.CA, "node-4").ClientTLS(n.NodeName, nil)); err != nil {
		t.Errorf("after a restart, a peer whose identity isn't removed was refused: %v", err)
	}
}

func TestPushingRemovedIdentitiesTheNodeHoldsChangesNothing(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	removed := nodetest.PeerIdentity(t, n.CA, "node-2")
	removeIdentities(t, n, removed)
	// A directory takes the list's place, which no file can replace, so a
	// push that tried to write the list would fail.
	n.Exec(t, "root", "sh", "-c", "rm "+hostlayout.RemovedIdentitiesFile+" && mkdir -p "+hostlayout.RemovedIdentitiesFile+"/x")
	removeIdentities(t, n, removed)
	removeIdentities(t, n)
	if err := checkPeer(t, n, removed.ClientTLS(n.NodeName, nil)); err == nil {
		t.Error("after pushing it again, the removed identity was accepted")
	}
	// A push that adds one has to write, and fails.
	acquire(t, n, "remove", time.Minute)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, err := n.Agent.RemoveIdentities(ctx, &nodev1.RemoveIdentitiesRequest{Identities: []string{nodetest.PeerIdentity(t, n.CA, "node-3").Fingerprint()}, Lease: "remove"})
	if err == nil {
		t.Error("a push that adds an identity succeeded with a directory in the list's place")
	}
}

func TestTheAgentWontServeWithARemovedIdentitiesListItCantRead(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	removeIdentities(t, n, nodetest.PeerIdentity(t, n.CA, "node-2"))
	n.Exec(t, "root", "sh", "-c", "echo garbage > "+hostlayout.RemovedIdentitiesFile)
	n.Restart(t, "agent")
	nodetest.Eventually(t, 10*time.Second, func() error {
		if out, _ := n.Output("root", "cat", "/var/log/agent.log"); !strings.Contains(out, "removed identities") {
			return fmt.Errorf("the agent's log doesn't say it can't read the removed identities:\n%s", out)
		}
		return nil
	})
	if _, err := n.Status(t); err == nil {
		t.Error("the agent serves with a list of removed identities it can't read")
	}
}

// removeIdentities has n's agent hold ids' identities as removed, as an
// operation whose lease holds the lock, which must succeed.
func removeIdentities(t *testing.T, n *nodetest.Node, ids ...*identity.Identity) {
	t.Helper()
	acquire(t, n, "remove", time.Minute)
	var fps []string
	for _, id := range ids {
		fps = append(fps, id.Fingerprint())
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if _, err := n.Agent.RemoveIdentities(ctx, &nodev1.RemoveIdentitiesRequest{Identities: fps, Lease: "remove"}); err != nil {
		t.Fatalf("removing %v: %v", fps, err)
	}
	releaseLock(t, n, "remove")
}

// watchHealth connects to n's peer listener as id, and watches its
// health until the test ends.
func watchHealth(t *testing.T, n *nodetest.Node, id *identity.Identity) (*grpc.ClientConn, healthpb.Health_WatchClient) {
	t.Helper()
	conn, err := grpc.NewClient(n.PeerAddress.String(), grpc.WithTransportCredentials(credentials.NewTLS(id.ClientTLS(n.NodeName, nil))))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	watch, err := healthpb.NewHealthClient(conn).Watch(t.Context(), &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := watch.Recv(); err != nil {
		t.Fatal(err)
	}
	return conn, watch
}

// checkConn asks the peer listener's health service over conn whether it
// serves.
func checkConn(t *testing.T, conn *grpc.ClientConn) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	_, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	return err
}
