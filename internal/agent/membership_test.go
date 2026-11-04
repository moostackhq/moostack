//go:build linux

package agent_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"maps"
	"math/big"
	"strings"
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

// These tests run the agents of several nodes of one cluster, and call
// a node's membership on UDP at its private address, as another node
// would.

// membershipALPN is the ALPN membership's transport takes.
const membershipALPN = "hivemind-membership/1"

// members are the members n's agent reports, each as "state at
// address", by name.
func members(t *testing.T, n *nodetest.Node) (map[string]string, error) {
	t.Helper()
	s, err := n.Status(t)
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	for _, m := range s.Membership.GetMembers() {
		out[m.Node] = fmt.Sprintf("%s at %s", m.State, m.Address)
	}
	return out, nil
}

// alive is n as another node sees it alive.
func alive(n *nodetest.Node) string {
	return fmt.Sprintf("%s at %s", nodev1.MemberState_MEMBER_STATE_ALIVE, n.PeerAddress)
}

// eventuallyMembers waits until n's agent reports want as its members.
func eventuallyMembers(t *testing.T, n *nodetest.Node, timeout time.Duration, want map[string]string) {
	t.Helper()
	nodetest.Eventually(t, timeout, func() error {
		got, err := members(t, n)
		if err == nil && !maps.Equal(got, want) {
			err = fmt.Errorf("%s reports the members %v, want %v", n.NodeName, got, want)
		}
		return err
	})
}

// keepsMembers checks that n's agent reports want as its members, and
// nothing else, for d.
func keepsMembers(t *testing.T, n *nodetest.Node, d time.Duration, want map[string]string) {
	t.Helper()
	for end := time.Now().Add(d); time.Now().Before(end); time.Sleep(200 * time.Millisecond) {
		got, err := members(t, n)
		if err != nil {
			t.Fatal(err)
		}
		if !maps.Equal(got, want) {
			t.Fatalf("%s reports the members %v, want %v", n.NodeName, got, want)
		}
	}
}

func TestTwoNodesJoinThroughARecording(t *testing.T) {
	n1 := nodetest.Start(t)
	n2 := nodetest.StartIn(t, n1, "node-2")
	n1.EventuallyHealthy(t)
	n2.EventuallyHealthy(t)
	// Only node-1 is told of the other; node-2 learns of node-1 when it
	// joins.
	record(t, n1, 1, nodeOf(n1), nodeOf(n2))
	want := map[string]string{"node-1": alive(n1), "node-2": alive(n2)}
	eventuallyMembers(t, n1, 20*time.Second, want)
	eventuallyMembers(t, n2, 20*time.Second, want)
}

// A node joins through the nodes of the generation file when its agent
// starts, though nothing else tells either node of the other.
func TestANodeJoinsThroughItsGenerationFileWhenItStarts(t *testing.T) {
	n1 := nodetest.Start(t)
	n2 := nodetest.StartIn(t, n1, "node-2")
	n1.EventuallyHealthy(t)
	n2.EventuallyHealthy(t)
	record(t, n1, 1, nodeOf(n1), nodeOf(n2))
	want := map[string]string{"node-1": alive(n1), "node-2": alive(n2)}
	eventuallyMembers(t, n2, 20*time.Second, want)
	// node-2 recorded nothing, so once restarted it knows node-1 no more.
	n1.Kill(t, "agent")
	n2.Restart(t, "agent")
	n2.EventuallyHealthy(t)
	eventuallyMembers(t, n2, 10*time.Second, map[string]string{"node-2": alive(n2)})
	n1.Restart(t, "agent")
	eventuallyMembers(t, n1, 20*time.Second, want)
	eventuallyMembers(t, n2, 20*time.Second, want)
}

// An agent told to stop leaves: the other node holds it as left, not
// dead, and keeps holding it so.
func TestAnAgentThatStopsIsLeft(t *testing.T) {
	n1 := nodetest.Start(t)
	n2 := nodetest.StartIn(t, n1, "node-2")
	n1.EventuallyHealthy(t)
	n2.EventuallyHealthy(t)
	record(t, n1, 1, nodeOf(n1), nodeOf(n2))
	eventuallyMembers(t, n1, 20*time.Second, map[string]string{"node-1": alive(n1), "node-2": alive(n2)})
	n2.Kill(t, "agent")
	want := map[string]string{"node-1": alive(n1), "node-2": fmt.Sprintf("%s at %s", nodev1.MemberState_MEMBER_STATE_LEFT, n2.PeerAddress)}
	eventuallyMembers(t, n1, 10*time.Second, want)
	keepsMembers(t, n1, 8*time.Second, want)
}

// A node whose agent crashed and restarted is reached again at once by
// a node that knew it, though the restarted one has no node to join
// through: the reset key, the same across its runs, ends the other's
// connection to its run before.
func TestACrashedAgentIsReachedAgainAtOnceOnceRestarted(t *testing.T) {
	n1 := nodetest.Start(t)
	n2 := nodetest.StartIn(t, n1, "node-2")
	n1.EventuallyHealthy(t)
	n2.EventuallyHealthy(t)
	record(t, n1, 1, nodeOf(n1), nodeOf(n2))
	want := map[string]string{"node-1": alive(n1), "node-2": alive(n2)}
	eventuallyMembers(t, n2, 20*time.Second, want)
	n2.Crash(t, "agent")
	n2.EventuallyHealthy(t)
	// node-1 probes node-2 every second or so.
	eventuallyMembers(t, n2, 5*time.Second, want)
}

// Every message a node sends goes only to the node it is meant for: a
// node at the address the generation gives node-2 hears nothing from
// node-1, and node-1 takes nothing from it, until a generation names it.
func TestANodeAtAnotherNodesAddressIsNotReached(t *testing.T) {
	n1 := nodetest.Start(t)
	impostor := nodetest.StartIn(t, n1, "node-3")
	n1.EventuallyHealthy(t)
	impostor.EventuallyHealthy(t)
	record(t, n1, 1, nodeOf(n1), listed("node-2", impostor.PeerAddress.Addr().String()))
	keepsMembers(t, n1, 5*time.Second, map[string]string{"node-1": alive(n1)})
	keepsMembers(t, impostor, time.Second, map[string]string{"node-3": alive(impostor)})
	record(t, n1, 2, nodeOf(n1), nodeOf(impostor))
	eventuallyMembers(t, impostor, 20*time.Second, map[string]string{"node-1": alive(n1), "node-3": alive(impostor)})
}

// A generation file the agent can't use gives it no seeds: it serves, says
// why in its status, and a recording repairs it.
func TestAnAgentStartsOnAGenerationFileItCantUse(t *testing.T) {
	for name, content := range map[string]string{
		"nodes by name alone":    `{"generation":1,"nodes":["node-1","node-2"]}`,
		"a node with no name":    `{"generation":1,"nodes":[{"name":"","private_address":"192.0.2.2"}]}`,
		"a node with no address": `{"generation":1,"nodes":[{"name":"node-2"}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			n := nodetest.Start(t)
			n.EventuallyHealthy(t)
			n.Kill(t, "agent")
			n.Exec(t, "root", "sh", "-c", "echo '"+content+"' > "+hostlayout.GenerationFile)
			n.Restart(t, "agent")
			if s := n.EventuallyHealthy(t); s.GenerationUnknown == "" {
				t.Errorf("the agent reports generation %d of %v, want it to say it can't tell", s.Generation, s.Nodes)
			}
			eventuallyMembers(t, n, 10*time.Second, map[string]string{"node-1": alive(n)})
			record(t, n, 1, nodeOf(n))
			if s := n.EventuallyHealthy(t); s.GenerationUnknown != "" {
				t.Errorf("after a recording, the agent says %q", s.GenerationUnknown)
			}
		})
	}
}

func TestAUDPSocketThatCantOpenFailsServe(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.Kill(t, "agent")
	// Another program holds the UDP port at the private address.
	n.Exec(t, "root", "sh", "-c", fmt.Sprintf(`perl -MIO::Socket::INET -e 'my $s = IO::Socket::INET->new(LocalAddr => "%s", Proto => "udp") or die $!; sleep' >/dev/null 2>&1 & echo $! >/run/holder.pid`, n.PeerAddress))
	n.Restart(t, "agent")
	nodetest.Eventually(t, 10*time.Second, func() error {
		if out, _ := n.Output("root", "cat", "/var/log/agent.log"); !strings.Contains(out, "membership: listening on UDP "+n.PeerAddress.String()) {
			return fmt.Errorf("the agent's log doesn't say membership can't listen on UDP:\n%s", out)
		}
		return nil
	})
	if _, err := n.Status(t); err == nil {
		t.Error("the agent serves without membership")
	}
	n.Exec(t, "root", "sh", "-c", "kill $(cat /run/holder.pid)")
	n.Restart(t, "agent")
	n.EventuallyHealthy(t)
}

// Membership refuses whom the peer listener refuses, and takes whom it
// takes: only an identity of the cluster's CA, neither expired nor
// removed.
func TestMembershipAndThePeerListenerTakeOnlyTheClustersIdentities(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	other, err := identity.NewCA("other")
	if err != nil {
		t.Fatal(err)
	}
	removed := nodetest.PeerIdentity(t, n.CA, "node-4")
	removeIdentities(t, n, removed)
	// Each dialler trusts the cluster's CA, so only the node can refuse
	// it.
	dialler := func(cert ...tls.Certificate) *tls.Config {
		return &tls.Config{Certificates: cert, RootCAs: caPool(t, n.CA), ServerName: n.NodeName, MinVersion: tls.VersionTLS13}
	}
	withCA := func(cfg *tls.Config) *tls.Config { cfg.RootCAs = caPool(t, n.CA); return cfg }
	for _, c := range []struct {
		name  string
		cfg   *tls.Config
		taken bool
	}{
		{"an identity of the cluster", nodetest.PeerIdentity(t, n.CA, "node-2").ClientTLS(n.NodeName, nil), true},
		{"an identity of another cluster", withCA(nodetest.PeerIdentity(t, other, "node-2").ClientTLS(n.NodeName, nil)), false},
		{"no certificate", dialler(), false},
		{"an expired identity", dialler(expiredIdentity(t, n.CA, "node-3")), false},
		{"a removed identity", removed.ClientTLS(n.NodeName, nil), false},
	} {
		t.Run(c.name, func(t *testing.T) {
			for path, check := range map[string]func(*testing.T, *nodetest.Node, *tls.Config) error{"the peer listener": checkPeer, "membership": checkMembership} {
				if err := check(t, n, c.cfg.Clone()); c.taken && err != nil {
					t.Errorf("%s refused it: %v", path, err)
				} else if !c.taken && err == nil {
					t.Errorf("%s took it", path)
				}
			}
		})
	}
}

// checkMembership connects to n's membership on UDP with cfg, and says
// whether the node took the connection: whether it is still open a
// second after the handshake.
func checkMembership(t *testing.T, n *nodetest.Node, cfg *tls.Config) error {
	t.Helper()
	cfg.NextProtos = []string{membershipALPN}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	conn, err := quic.DialAddr(ctx, n.PeerAddress.String(), cfg, &quic.Config{EnableDatagrams: true})
	if err != nil {
		return err
	}
	defer conn.CloseWithError(0, "")
	select {
	case <-conn.Context().Done():
		return context.Cause(conn.Context())
	case <-time.After(time.Second):
		return nil
	}
}

// expiredIdentity is a certificate and key for the node called name,
// from ca, that expired an hour ago.
func expiredIdentity(t *testing.T, ca *identity.CA, name string) tls.Certificate {
	t.Helper()
	caCert, err := tls.X509KeyPair(ca.CertPEM(), ca.KeyPEM())
	if err != nil {
		t.Fatal(err)
	}
	issuer, err := x509.ParseCertificate(caCert.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.CreateCertificate(rand.Reader, &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: name},
		DNSNames:     []string{name},
		NotBefore:    time.Now().Add(-2 * time.Hour),
		NotAfter:     time.Now().Add(-time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}, issuer, pub, caCert.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := tls.X509KeyPair(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pkcs8(t, key))
	if err != nil {
		t.Fatal(err)
	}
	return cert
}

// pkcs8 is key as PEM, PKCS #8.
func pkcs8(t *testing.T, key ed25519.PrivateKey) []byte {
	t.Helper()
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
}

func TestTheAgentRefusesAGenerationOfANodeWithNoNameOrAddress(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	acquire(t, n, "record", time.Minute)
	for _, bad := range []*nodev1.GenerationNode{listed("", "192.0.2.2"), listed("node-2", ""), listed("node-2", "node-2.example")} {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		_, err := n.Agent.RecordGeneration(ctx, &nodev1.RecordGenerationRequest{Generation: 1, Nodes: []*nodev1.GenerationNode{nodeOf(n), bad}, Lease: "record"})
		cancel()
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("recording %v: %v, want InvalidArgument", bad, err)
		}
	}
	if got := n.EventuallyHealthy(t).Generation; got != 0 {
		t.Errorf("after the refusals, the node reports generation %d, want 0", got)
	}
	if out, err := n.Output("root", "test", "-e", hostlayout.GenerationFile); err == nil {
		t.Errorf("a refused recording wrote the generation file: %s", out)
	}
}
