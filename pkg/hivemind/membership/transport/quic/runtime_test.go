package quic_test

import (
	"testing"
	"time"

	"github.com/moostackhq/moostack/pkg/hivemind/membership/member"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/msg"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/node"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/runtime"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/transport"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/wire"
)

func waitConverged(t *testing.T, c *benchCluster) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !c.converged(len(c.nodes)) {
		if time.Now().After(deadline) {
			t.Fatal("the cluster didn't converge")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func stateOf(rt *runtime.Runtime, id member.ID) member.State {
	for _, m := range rt.Members() {
		if m.ID == id {
			return m.State
		}
	}
	return member.Dead
}

// A node that leaves and closes its transport at once is known as left,
// not dead, every time.
func TestALeaveThenACloseIsKnownAsLeft(t *testing.T) {
	for range 20 {
		c := startCluster(t, 2, nil)
		waitConverged(t, c)
		if err := c.nodes[1].Leave(); err != nil {
			t.Fatal(err)
		}
		c.nodes[1].Close()
		c.trs[1].Close()
		deadline := time.Now().Add(2 * time.Second)
		for s := stateOf(c.nodes[0], "node-001"); s != member.Left; s = stateOf(c.nodes[0], "node-001") {
			if time.Now().After(deadline) {
				t.Fatalf("node-000 has node-001 %v after it left and closed; its transport's stats %+v", s, c.trs[1].Stats())
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// A member that sends a packet whose sender entry is another member has
// it dropped, and reported, over the transport.
func TestAMemberSpeakingAsAnotherIsDropped(t *testing.T) {
	events := make(chan node.Event, 16)
	c := startCluster(t, 3, func(i int) runtime.Options {
		if i > 0 {
			return runtime.Options{}
		}
		return runtime.Options{OnEvent: func(e node.Event) {
			if e.Kind == node.Impersonated {
				events <- e
			}
		}}
	})
	waitConverged(t, c)
	var other member.Member
	for _, m := range c.nodes[0].Members() {
		if m.ID == "node-001" {
			other = m
		}
	}
	other.State, other.Incarnation = member.Left, other.Incarnation+1
	b, err := wire.EncodePacket(msg.Packet{From: other, Body: msg.Gossip{}})
	if err != nil {
		t.Fatal(err)
	}
	c.trs[2].SendPacket(transport.Dest{Addr: c.trs[0].Addr(), Peer: "node-000"}, b)
	select {
	case e := <-events:
		if e.Member != "node-001" || e.By != "node-002" {
			t.Fatalf("event %+v; want node-002 speaking as node-001", e)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no Impersonated event")
	}
	if s := stateOf(c.nodes[0], "node-001"); s != member.Alive {
		t.Fatalf("node-000 has node-001 %v", s)
	}
}
