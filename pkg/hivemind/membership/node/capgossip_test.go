package node_test

import (
	"testing"
	"time"

	"github.com/moostackhq/moostack/pkg/hivemind/membership/member"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/msg"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/node"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/wire"
)

// gossipSent counts the gossip packets a node sent, by address.
type gossipSent map[string]int

func (g gossipSent) add(o gossipSent) gossipSent {
	for a, k := range o {
		g[a] += k
	}
	return g
}

// A gossip packet goes only to a member that reads them (CapGossip),
// whether it is sent in a gossip round, as a correction of a sender's stale
// entries, or as a goodbye.
func TestGossipPacketsGoOnlyToMembersThatReadThem(t *testing.T) {
	old := peer("b")
	old.Incarnation, old.Caps = 5, member.Of(member.CapNack)
	reads := peer("c")
	reads.Incarnation = 5
	stale := func(m member.Member) member.Member { m.Incarnation--; return m }
	gossip := func(from member.Member, about ...member.Member) node.Receive {
		p := msg.Packet{From: from, Body: msg.Gossip{}}
		for _, m := range about {
			p.Gossip = append(p.Gossip, msg.Update{Member: m})
		}
		return node.Receive{Addr: from.Addr, Packet: p}
	}

	for _, tc := range []struct {
		name   string
		rounds bool // the node's own gossip rounds on
		do     func(step func(node.Input) gossipSent) gossipSent
	}{
		{"gossip rounds", true, func(step func(node.Input) gossipSent) gossipSent {
			sent := step(gossip(reads, peer("z")))
			for range 10 {
				sent.add(step(node.Tick{}))
			}
			return sent
		}},
		{"corrections", false, func(step func(node.Input) gossipSent) gossipSent {
			return step(gossip(old, stale(reads))).add(step(gossip(reads, stale(old))))
		}},
		{"goodbyes", false, func(step func(node.Input) gossipSent) gossipSent {
			return step(node.Leave{})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := node.Defaults("a", "a")
			c.Seed = 1
			if !tc.rounds {
				c.GossipInterval = 0
			}
			n := node.New(c, wire.Sizer{}, t0)
			now := t0
			step := func(in node.Input) gossipSent {
				now = now.Add(100 * time.Millisecond)
				sent := gossipSent{}
				for _, p := range n.Step(now, in).Packets {
					if _, ok := p.Packet.Body.(msg.Gossip); ok {
						sent[p.Addr]++
					}
				}
				return sent
			}
			for _, m := range []member.Member{old, reads} {
				step(node.Receive{Addr: m.Addr, Packet: msg.Packet{From: m, Body: msg.Ping{Seq: 1}}})
			}
			sent := tc.do(step)
			if sent["b"] != 0 {
				t.Errorf("sent %d gossip packets to b, which doesn't read them", sent["b"])
			}
			if sent["c"] == 0 {
				t.Errorf("sent no gossip packet to c, which reads them")
			}
		})
	}
}
