package runtime_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/moostackhq/moostack/pkg/hivemind/membership/member"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/msg"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/node"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/runtime"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/transport/memnet"
)

func fast(id string, seed uint64) node.Config {
	c := node.Defaults(member.ID(id), "")
	c.Probe.Interval = 50 * time.Millisecond
	c.Probe.Timeout = 15 * time.Millisecond
	c.SuspicionMult = 2
	c.LateLimit = 0 // no pause detection
	c.Seed = seed
	return c
}

func start(t *testing.T, net *memnet.Network, n int) []*runtime.Runtime {
	var rs []*runtime.Runtime
	for i := range n {
		id := fmt.Sprintf("n%d", i)
		r, err := runtime.Start(context.Background(), fast(id, uint64(i+1)), net.ListenAs(id, id), runtime.Options{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(r.Close)
		rs = append(rs, r)
	}
	for _, r := range rs[1:] {
		r.Join("n0")
	}
	return rs
}

func waitFor(t *testing.T, what string, d time.Duration, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("%s: not within %v", what, d)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func sees(r *runtime.Runtime, id string, s member.State) bool {
	for _, m := range r.Members() {
		if string(m.ID) == id {
			return m.State == s
		}
	}
	return false
}

func TestNodesOnMemnet(t *testing.T) {
	net := memnet.New(1, time.Millisecond)
	rs := start(t, net, 5)
	waitFor(t, "every node sees the others", 5*time.Second, func() bool {
		for _, r := range rs {
			for i := range rs {
				if !sees(r, fmt.Sprintf("n%d", i), member.Alive) {
					return false
				}
			}
		}
		return true
	})

	rs[3].Close()
	waitFor(t, "n3 declared dead", 10*time.Second, func() bool {
		return sees(rs[0], "n3", member.Dead) && sees(rs[1], "n3", member.Dead)
	})

	rs[4].Leave()
	waitFor(t, "n4 left", 5*time.Second, func() bool { return sees(rs[0], "n4", member.Left) })
	if d := rs[0].Counts(); d != (runtime.Counts{}) {
		t.Fatalf("dropped %+v", d)
	}
}

func TestPartitionAndHealOnMemnet(t *testing.T) {
	net := memnet.New(2, time.Millisecond)
	rs := start(t, net, 4)
	waitFor(t, "converged", 5*time.Second, func() bool { return sees(rs[3], "n0", member.Alive) && sees(rs[0], "n3", member.Alive) })
	side := map[string]int{"n0": 0, "n1": 0, "n2": 1, "n3": 1}
	net.Cut(func(from, to string) bool { return side[from] != side[to] })
	waitFor(t, "the split is found", 15*time.Second, func() bool { return sees(rs[0], "n3", member.Dead) })
	net.Cut(nil)
	waitFor(t, "healed", 15*time.Second, func() bool { return sees(rs[0], "n3", member.Alive) && sees(rs[3], "n0", member.Alive) })
}

func msgAck() msg.Packet { return msg.Packet{From: member.Member{ID: "x"}, Body: msg.Ack{Seq: 1}} }
