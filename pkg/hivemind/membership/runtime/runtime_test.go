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
	"github.com/moostackhq/moostack/pkg/hivemind/membership/transport"
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
		r, err := runtime.Start(context.Background(), fast(id, uint64(i+1)), pinned{t, net.ListenAs(id, id)}, runtime.Options{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(r.Close)
		rs = append(rs, r)
	}
	for _, r := range rs[1:] {
		r.Join(seed("n0"))
	}
	return rs
}

// seed is the seed of the member id, listening at the address id.
func seed(id string) node.Seed { return node.Seed{ID: member.ID(id), Addr: id} }

// pinned is a transport that fails the test for a message that names no
// member to hand it to.
type pinned struct {
	t *testing.T
	transport.Transport
}

func (p pinned) SendPacket(to transport.Dest, b []byte) { p.check(to); p.Transport.SendPacket(to, b) }
func (p pinned) SendStream(to transport.Dest, b []byte) { p.check(to); p.Transport.SendStream(to, b) }

func (p pinned) check(to transport.Dest) {
	if to.Peer == "" {
		p.t.Errorf("%s sent to %s, naming no member", p.Addr(), to.Addr)
	}
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

// A seed with no member ID or no address is refused; no seeds, or the node
// itself alone, are taken. The node joins nothing.
func TestJoinRefusesASeedWithNoID(t *testing.T) {
	net := memnet.New(3, time.Millisecond)
	rs := start(t, net, 1)
	b, err := runtime.Start(context.Background(), fast("b", 9), pinned{t, net.ListenAs("b", "b")}, runtime.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(b.Close)
	if err := b.Join(seed("n0"), node.Seed{Addr: "n0"}); err == nil {
		t.Fatal("joined through a seed with no ID")
	}
	if err := b.Join(seed("n0"), node.Seed{ID: "n0"}); err == nil {
		t.Fatal("joined through a seed with no address")
	}
	if err := b.Join(); err != nil {
		t.Fatalf("Join of no seeds: %v", err)
	}
	if err := b.Join(seed("b")); err != nil {
		t.Fatalf("Join naming only itself: %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	if len(rs[0].Members()) != 1 || len(b.Members()) != 1 {
		t.Fatalf("members %v and %v after a refused join", rs[0].Members(), b.Members())
	}
}

// Forget removes a member at once and reports it, and the member stays out
// while it runs on and the others gossip it alive, and when it restarts at
// a higher incarnation, until a Join names it. The node can't forget
// itself or the empty ID, and forgetting a member it doesn't know changes
// nothing in the view.
func TestForgetThroughTheRuntime(t *testing.T) {
	net := memnet.New(4, time.Millisecond)
	forgot := make(chan member.Member, 4)
	var rs []*runtime.Runtime
	for i := range 3 {
		id := fmt.Sprintf("n%d", i)
		o := runtime.Options{}
		if i == 0 {
			o.OnForget = func(m member.Member) { forgot <- m }
		}
		r, err := runtime.Start(context.Background(), fast(id, uint64(i+1)), pinned{t, net.ListenAs(id, id)}, o)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(r.Close)
		rs = append(rs, r)
	}
	rs[1].Join(seed("n0"))
	rs[2].Join(seed("n0"))
	waitFor(t, "converged", 5*time.Second, func() bool { return sees(rs[0], "n2", member.Alive) && sees(rs[1], "n2", member.Alive) })
	if err := rs[0].Forget("n2"); err != nil {
		t.Fatal(err)
	}
	if sees(rs[0], "n2", member.Alive) || len(rs[0].Members()) != 2 {
		t.Fatalf("n2 still in n0's view: %v", rs[0].Members())
	}
	select {
	case m := <-forgot:
		if m.ID != "n2" {
			t.Fatalf("forgot %v", m)
		}
	case <-time.After(time.Second):
		t.Fatal("OnForget not called")
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if len(rs[0].Members()) != 2 {
			t.Fatalf("n2 back in n0's view: %v", rs[0].Members())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !sees(rs[1], "n2", member.Alive) {
		t.Fatal("n1 lost n2")
	}
	// A new n2, at a higher incarnation, stays out until a Join names it.
	rs[2].Close()
	n2, err := runtime.Start(context.Background(), fast("n2", 9), pinned{t, net.ListenAs("n2", "n2")}, runtime.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(n2.Close)
	n2.Join(seed("n1"))
	waitFor(t, "n1 has the new n2", 5*time.Second, func() bool {
		m, ok := member.Member{}, false
		for _, x := range rs[1].Members() {
			if x.ID == "n2" {
				m, ok = x, true
			}
		}
		return ok && m.State == member.Alive && m.Incarnation == n2.Self().Incarnation
	})
	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if len(rs[0].Members()) != 2 {
			t.Fatalf("the new n2 in n0's view before a Join named it: %v", rs[0].Members())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := rs[0].Join(seed("n2")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "n0 takes n2 back", 5*time.Second, func() bool { return sees(rs[0], "n2", member.Alive) })
	if err := rs[0].Forget("n0"); err == nil {
		t.Fatal("n0 forgot itself")
	}
	if err := rs[0].Forget(""); err == nil {
		t.Fatal("n0 forgot the empty ID")
	}
	if err := rs[0].Forget("nobody"); err != nil {
		t.Fatal(err)
	}
	if len(rs[0].Members()) != 3 || rs[0].Self().State != member.Alive {
		t.Fatalf("members %v, self %v", rs[0].Members(), rs[0].Self())
	}
}
