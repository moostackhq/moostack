//go:build linux

package hivemindadapter

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/nodetest"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/member"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/msg"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/transport"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/transport/quic"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/wire"
)

// A member of the cluster that sends a node a message saying it is
// another member has it dropped, through the agent: the node counts it
// as impersonated, and holds the other member as it was.
func TestTheAgentDropsAMemberSpeakingAsAnother(t *testing.T) {
	n1 := nodetest.Start(t)
	n2 := nodetest.StartIn(t, n1, "node-2")
	n1.EventuallyHealthy(t)
	n2.EventuallyHealthy(t)
	recordGeneration(t, n1, n1, n2)
	nodetest.Eventually(t, 20*time.Second, func() error { return holds(t, n1, "node-2", nodev1.MemberState_MEMBER_STATE_ALIVE) })

	// node-3 proves its own identity, from this machine's address on the
	// nodes' network, and says it is node-2, leaving.
	forger, err := quic.Listen(transportConfig(Config{Identity: nodetest.PeerIdentity(t, n1.CA, "node-3"), Address: localAddress(t, n1.PeerAddress)}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { forger.Close() })
	forged, err := wire.EncodePacket(msg.Packet{From: member.Member{ID: "node-2", Addr: n2.PeerAddress.String(), Incarnation: member.MaxIncarnation, State: member.Left}, Body: msg.Gossip{}})
	if err != nil {
		t.Fatal(err)
	}
	nodetest.Eventually(t, 10*time.Second, func() error {
		forger.SendPacket(transport.Dest{Addr: n1.PeerAddress.String(), Peer: "node-1"}, forged)
		s, err := n1.Status(t)
		if err != nil {
			return err
		}
		if d := s.Membership.Dropped["impersonated"]; d == 0 {
			return fmt.Errorf("node-1 counts no message dropped as impersonated: %v", s.Membership.Dropped)
		}
		return nil
	})
	if err := holds(t, n1, "node-2", nodev1.MemberState_MEMBER_STATE_ALIVE); err != nil {
		t.Error(err)
	}
}

// holds says whether n's agent holds the node called name in state.
func holds(t *testing.T, n *nodetest.Node, name string, state nodev1.MemberState) error {
	t.Helper()
	s, err := n.Status(t)
	if err != nil {
		return err
	}
	for _, m := range s.Membership.GetMembers() {
		if m.Node == name {
			if m.State != state {
				return fmt.Errorf("%s holds %s %s, want %s", n.NodeName, name, m.State, state)
			}
			return nil
		}
	}
	return fmt.Errorf("%s doesn't know %s", n.NodeName, name)
}

// recordGeneration has n record generation 1 of nodes, each at its
// private address.
func recordGeneration(t *testing.T, n *nodetest.Node, nodes ...*nodetest.Node) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if _, err := n.Agent.AcquireLock(ctx, &nodev1.AcquireLockRequest{Lock: &nodev1.Lock{Lease: "record"}, Ttl: durationpb.New(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	var listed []*nodev1.GenerationNode
	for _, m := range nodes {
		listed = append(listed, &nodev1.GenerationNode{Name: m.NodeName, PrivateAddress: m.PeerAddress.Addr().String()})
	}
	if _, err := n.Agent.RecordGeneration(ctx, &nodev1.RecordGenerationRequest{Generation: 1, Nodes: listed, Lease: "record"}); err != nil {
		t.Fatal(err)
	}
}

// localAddress is this machine's address on the way to to, with a port
// of its own.
func localAddress(t *testing.T, to netip.AddrPort) netip.AddrPort {
	t.Helper()
	c, err := net.Dial("udp", to.String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	return netip.AddrPortFrom(c.LocalAddr().(*net.UDPAddr).AddrPort().Addr(), 0)
}
