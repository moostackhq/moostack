//go:build linux

package agent_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/moostackhq/moostack/internal/agent"
	"github.com/moostackhq/moostack/internal/hostlayout"
	"github.com/moostackhq/moostack/internal/identity"
	"github.com/moostackhq/moostack/internal/nodetest"
)

// These tests call the agent's peer listener on the node's private
// address, as another node would.

func TestThePeerListenerAcceptsAnIdentityOfTheCluster(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	peer := nodetest.PeerIdentity(t, n.CA, "node-2")
	if err := checkPeer(t, n, peer.ClientTLS(n.NodeName)); err != nil {
		t.Errorf("a peer of the cluster was refused: %v", err)
	}
}

func TestThePeerListenerProvesTheNodesIdentity(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	peer := nodetest.PeerIdentity(t, n.CA, "node-2")
	// The node's identity is node-1's, so a peer that expects another
	// node refuses it.
	err := checkPeer(t, n, peer.ClientTLS("node-3"))
	if err == nil || !strings.Contains(err.Error(), "node-3") {
		t.Errorf("a peer expecting node-3 at node-1's address: %v, want a certificate error naming node-3", err)
	}
}

func TestThePeerListenerRefusesAnIdentityOfAnotherCluster(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	other, err := identity.NewCA("other")
	if err != nil {
		t.Fatal(err)
	}
	peer := nodetest.PeerIdentity(t, other, "node-2")
	cfg := peer.ClientTLS(n.NodeName)
	// The peer trusts this cluster's CA, so only the node can refuse it.
	cfg.RootCAs = caPool(t, n.CA)
	if err := checkPeer(t, n, cfg); err == nil {
		t.Error("a peer of another cluster was accepted")
	}
}

func TestThePeerListenerRefusesAPeerWithoutACertificate(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	cfg := &tls.Config{RootCAs: caPool(t, n.CA), ServerName: n.NodeName, MinVersion: tls.VersionTLS13}
	if err := checkPeer(t, n, cfg); err == nil {
		t.Error("a peer without a certificate was accepted")
	}
}

func TestTheAgentListensForPeersOnlyOnThePrivateAddress(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	out, err := n.Output("root", "bash", "-c", fmt.Sprintf("echo > /dev/tcp/127.0.0.1/%d", hostlayout.PeerPort))
	if err == nil {
		t.Errorf("the agent listens for peers on 127.0.0.1 too: %s", out)
	}
}

func TestTheAgentWontServeWithoutAValidIdentity(t *testing.T) {
	nodetest.CheckRefusesInvalidIdentity(t, hostlayout.Agent)
}

func TestRequestIdentityNeverReplacesTheNodesKey(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	before, err := n.Output("root", "cat", hostlayout.Identity.Key)
	if err != nil {
		t.Fatal(err)
	}
	if out, err := n.Output("root", hostlayout.Agent, "request-identity"); err == nil {
		t.Errorf("a second request-identity succeeded:\n%s", out)
	}
	if after, err := n.Output("root", "cat", hostlayout.Identity.Key); err != nil || after != before {
		t.Errorf("a second request-identity changed the node's key: %v", err)
	}
}

func TestAnOpenHealthWatchDoesntKeepTheAgentFromStopping(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	peer := nodetest.PeerIdentity(t, n.CA, "node-2")
	conn, err := grpc.NewClient(n.PeerAddress.String(), grpc.WithTransportCredentials(credentials.NewTLS(peer.ClientTLS(n.NodeName))))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	watch, err := healthpb.NewHealthClient(conn).Watch(t.Context(), &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := watch.Recv(); err != nil {
		t.Fatal(err)
	}

	n.Kill(t, "agent")
	ended := make(chan error, 1)
	go func() {
		for {
			if _, err := watch.Recv(); err != nil {
				ended <- err
				return
			}
		}
	}()
	select {
	case <-ended:
	case <-time.After(agent.StopTimeout + 10*time.Second):
		t.Fatal("the agent still serves a peer's health watch after it was told to stop")
	}
	nodetest.Eventually(t, 10*time.Second, func() error {
		// The agent's parent doesn't reap it, so a stopped agent is a
		// zombie.
		out, err := n.Output("root", "sh", "-c", "awk '{print $3}' /proc/$(cat /run/agent.pid)/stat")
		if err == nil && strings.TrimSpace(out) != "Z" {
			return fmt.Errorf("the agent is still running: state %s", out)
		}
		return nil
	})
}

// checkPeer asks the peer listener's health service whether it serves,
// connecting with cfg.
func checkPeer(t *testing.T, n *nodetest.Node, cfg *tls.Config) error {
	t.Helper()
	conn, err := grpc.NewClient(n.PeerAddress.String(), grpc.WithTransportCredentials(credentials.NewTLS(cfg)))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	res, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		return err
	}
	if res.Status != healthpb.HealthCheckResponse_SERVING {
		return fmt.Errorf("the peer listener is %s", res.Status)
	}
	return nil
}

// caPool is a pool of ca's certificate alone.
func caPool(t *testing.T, ca *identity.CA) *x509.CertPool {
	t.Helper()
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca.CertPEM()) {
		t.Fatal("the CA's certificate doesn't parse")
	}
	return pool
}
