package node_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/moostackhq/moostack/pkg/hivemind/membership/member"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/msg"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/node"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/wire"
)

// bigNode is a node that knows n live members and, as after crashes, a
// tenth as many dead ones.
func bigNode(b *testing.B, n int) (*node.Node, time.Time) {
	c := node.Defaults("self", "self")
	c.MaxMembers = 0
	nd := node.New(c, wire.Sizer{}, t0)
	var ups []msg.Update
	for i := range n + n/10 {
		m := member.Member{ID: member.ID(fmt.Sprintf("m%06d", i)), Addr: fmt.Sprintf("a%06d", i), Caps: c.Caps, MinVersion: 1, Version: 1}
		if i >= n {
			m.State = member.Dead
		}
		ups = append(ups, msg.Update{Member: m})
		if len(ups) == 500 {
			nd.Step(t0, node.Receive{Addr: "m000000", Packet: msg.Packet{From: ups[0].Member, Body: msg.Ack{}, Gossip: ups}})
			ups = nil
		}
	}
	if len(ups) > 0 {
		nd.Step(t0, node.Receive{Addr: "m000000", Packet: msg.Packet{From: ups[0].Member, Body: msg.Ack{}, Gossip: ups}})
	}
	return nd, t0
}

// BenchmarkTick: the cost of the wake-ups a node asks for, in a cluster of
// n, per wake-up.
func BenchmarkTick(b *testing.B) {
	for _, n := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			nd, now := bigNode(b, n)
			wake := now
			b.ResetTimer()
			for range b.N {
				r := nd.Step(wake, node.Tick{})
				wake = r.Wake
			}
		})
	}
}

// BenchmarkReceive: the cost of a ping with ten updates on it, in a
// cluster of n.
func BenchmarkReceive(b *testing.B) {
	for _, n := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			nd, now := bigNode(b, n)
			from := member.Member{ID: "m000001", Addr: "a000001", Caps: member.Of(member.CapNack, member.CapGossip), MinVersion: 1, Version: 1}
			var ups []msg.Update
			for i := range 10 {
				ups = append(ups, msg.Update{Member: member.Member{ID: member.ID(fmt.Sprintf("m%06d", i+2)), Addr: fmt.Sprintf("a%06d", i+2), Incarnation: 1}})
			}
			p := msg.Packet{From: from, Body: msg.Ping{Seq: 1, Target: "self"}, Gossip: ups}
			b.ResetTimer()
			for i := range b.N {
				nd.Step(now.Add(time.Duration(i)*time.Microsecond), node.Receive{Addr: "a000001", Packet: p})
			}
		})
	}
}
