package hivemindadapter

import (
	"context"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	goruntime "runtime"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/moostackhq/moostack/pkg/hivemind/membership/member"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/msg"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/runtime"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/transport"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/transport/memnet"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/transport/quic"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/wire"
)

// These tests run membership over memnet's authenticating endpoints,
// each proving the node it is named for.

func states(m *Membership) map[string]State {
	out := map[string]State{}
	for _, e := range m.Members() {
		out[e.NodeID] = e.State
	}
	return out
}

func waitFor(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// startOn starts membership for nodeID at addr on net, stopped when the
// test ends.
func startOn(t *testing.T, ctx context.Context, net *memnet.Network, nodeID, addr string) *Membership {
	t.Helper()
	m, err := start(ctx, nodeID, net.ListenAs(addr, nodeID), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Stop)
	return m
}

// cluster starts nodes node-0 and on, at 10.0.0.<i>:7443, each seeded
// with node-0, and waits until each sees all alive.
func cluster(t *testing.T, net *memnet.Network, n int) []*Membership {
	t.Helper()
	var nodes []*Membership
	for i := range n {
		m := startOn(t, t.Context(), net, fmt.Sprint("node-", i), fmt.Sprintf("10.0.0.%d:7443", i))
		if i > 0 {
			if err := m.SetSeeds([]Seed{{NodeID: "node-0", Address: "10.0.0.0:7443"}}); err != nil {
				t.Fatal(err)
			}
		}
		nodes = append(nodes, m)
	}
	for _, m := range nodes {
		waitFor(t, "all alive", func() bool {
			s := states(m)
			if len(s) != n {
				return false
			}
			for _, st := range s {
				if st != Alive {
					return false
				}
			}
			return true
		})
	}
	return nodes
}

func TestThreeNodesJoinAndAStoppedOneIsLeft(t *testing.T) {
	t.Parallel()
	nodes := cluster(t, memnet.New(1, time.Millisecond), 3)
	want := []Member{
		{NodeID: "node-0", Address: "10.0.0.0:7443", State: Alive},
		{NodeID: "node-1", Address: "10.0.0.1:7443", State: Alive},
		{NodeID: "node-2", Address: "10.0.0.2:7443", State: Alive},
	}
	if got := nodes[1].Members(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("node-1's members are %v, want %v", got, want)
	}
	nodes[2].Stop()
	for _, m := range nodes[:2] {
		waitFor(t, "node-2 left", func() bool { return states(m)["node-2"] == Left })
	}
}

// A node that stops without leaving is declared dead.
func TestACrashedNodeIsDead(t *testing.T) {
	t.Parallel()
	net := memnet.New(1, time.Millisecond)
	a := startOn(t, t.Context(), net, "a", "10.0.0.1:7443")
	ctx, crash := context.WithCancel(t.Context())
	b := startOn(t, ctx, net, "b", "10.0.0.2:7443")
	if err := b.SetSeeds([]Seed{{NodeID: "a", Address: "10.0.0.1:7443"}}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "b alive", func() bool { return states(a)["b"] == Alive })
	crash()
	waitFor(t, "b dead", func() bool { return states(a)["b"] == Dead })
}

// SetSeeds called again and again while a node stops never brings it
// back between its leave and its close: the others hold it as left, and
// keep holding it so past its suspicion's timeout.
func TestSetSeedsRacingStopLeavesTheNodeLeft(t *testing.T) {
	t.Parallel()
	for i := range 5 {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Parallel()
			nodes := cluster(t, memnet.New(uint64(i), time.Millisecond), 2)
			seeds := []Seed{{NodeID: "node-0", Address: "10.0.0.0:7443"}}
			done := make(chan struct{})
			go func() {
				defer close(done)
				for nodes[1].SetSeeds(seeds) == nil {
				}
			}()
			time.Sleep(10 * time.Millisecond)
			nodes[1].Stop()
			<-done
			waitFor(t, "node-1 left", func() bool { return states(nodes[0])["node-1"] == Left })
			// A node brought back and closed would be suspected within a
			// few probe periods, and dead after the suspicion's timeout.
			for end := time.Now().Add(8 * time.Second); time.Now().Before(end); time.Sleep(50 * time.Millisecond) {
				if s := states(nodes[0])["node-1"]; s != Left {
					t.Fatalf("node-0 holds the stopped node-1 %s", s)
				}
			}
		})
	}
}

func TestCallsAfterStopAreRefused(t *testing.T) {
	t.Parallel()
	m := startOn(t, t.Context(), memnet.New(1, time.Millisecond), "a", "10.0.0.1:7443")
	m.Stop()
	m.Stop()
	if err := m.SetSeeds([]Seed{{NodeID: "b", Address: "10.0.0.2:7443"}}); !errors.Is(err, ErrStopped) {
		t.Errorf("SetSeeds after Stop: %v, want ErrStopped", err)
	}
	if err := m.Forget("b"); !errors.Is(err, ErrStopped) {
		t.Errorf("Forget after Stop: %v, want ErrStopped", err)
	}
}

// A forgotten node is out of the view at once, and stays out while
// another node still holds it alive, until SetSeeds names it again.
func TestForgetKeepsANodeOutUntilSetSeedsNamesIt(t *testing.T) {
	t.Parallel()
	nodes := cluster(t, memnet.New(1, time.Millisecond), 3)
	if err := nodes[0].Forget("node-2"); err != nil {
		t.Fatal(err)
	}
	if _, ok := states(nodes[0])["node-2"]; ok {
		t.Fatal("node-2 is still in node-0's view once forgotten")
	}
	// node-1 still gossips node-2 alive, and node-2 still probes node-0.
	for end := time.Now().Add(3 * time.Second); time.Now().Before(end); time.Sleep(50 * time.Millisecond) {
		if s, ok := states(nodes[0])["node-2"]; ok {
			t.Fatalf("the forgotten node-2 is back in node-0's view, %s", s)
		}
	}
	if err := nodes[0].SetSeeds([]Seed{{NodeID: "node-2", Address: "10.0.0.2:7443"}}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "node-2 back", func() bool { return states(nodes[0])["node-2"] == Alive })
	if err := nodes[0].Forget("node-0"); err == nil || errors.Is(err, ErrStopped) {
		t.Errorf("a node forgetting itself: %v, want it refused, not ErrStopped", err)
	}
}

// Status gives the members, the health, and the drops by reason: the
// runtime's, an impersonation the node refused, and the transport's.
func TestStatusCountsTheDropsByReason(t *testing.T) {
	t.Parallel()
	net := memnet.New(1, time.Millisecond)
	a, err := start(t.Context(), "a", net.ListenAs("10.0.0.1:7443", "a"), func() map[string]uint64 {
		return map[string]uint64{"queue_full": 2, "unsent": 0}
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(a.Stop)
	b := startOn(t, t.Context(), net, "b", "10.0.0.2:7443")
	if err := b.SetSeeds([]Seed{{NodeID: "a", Address: "10.0.0.1:7443"}}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "b alive", func() bool { return states(a)["b"] == Alive })

	to := transport.Dest{Addr: "10.0.0.1:7443", Peer: "a"}
	// c proves it is c, but says it is b leaving.
	forged, err := wire.EncodePacket(msg.Packet{From: member.Member{ID: "b", Addr: "10.0.0.2:7443", Incarnation: 1 << 50, State: member.Left}, Body: msg.Gossip{}})
	if err != nil {
		t.Fatal(err)
	}
	net.ListenAs("10.0.0.3:7443", "c").SendPacket(to, forged)
	// An endpoint that proves nobody, and one that sends what isn't in a
	// wire version a reads.
	net.Listen("10.0.0.4:7443").SendPacket(to, forged)
	net.ListenAs("10.0.0.5:7443", "e").SendPacket(to, []byte("garbage"))

	want := map[string]uint64{"impersonated": 1, "unauthenticated": 1, "wrong_version": 1, "queue_full": 2}
	waitFor(t, "the drops counted", func() bool { return maps.Equal(a.Status().Dropped, want) })
	s := a.Status()
	if s.HealthScore != 0 || fmt.Sprint(s.Members) != fmt.Sprint([]Member{{"a", "10.0.0.1:7443", Alive}, {"b", "10.0.0.2:7443", Alive}}) {
		t.Errorf("a's status is %+v, want a and b alive at health 0", s)
	}
}

func TestEveryStateIsNamed(t *testing.T) {
	for state, want := range map[member.State]State{member.Alive: Alive, member.Suspect: Suspect, member.Dead: Dead, member.Left: Left} {
		if got := convert(member.Member{ID: "a", Addr: "b", State: state}); got != (Member{NodeID: "a", Address: "b", State: want}) {
			t.Errorf("%v converts to %+v, want %s", state, got, want)
		}
	}
}

func TestEveryDropOfTheTransportHasItsReason(t *testing.T) {
	got := quicDrops(quic.Drops{Unpinned: 1, Oversize: 2, QueueFull: 3, Unsent: 4, Closed: 5, ReceiveFull: 6, ReadFails: 7})
	want := map[string]uint64{"unpinned": 1, "oversize": 2, "queue_full": 3, "unsent": 4, "closed": 5, "receive_full": 6, "read_failed": 7}
	if !maps.Equal(got, want) {
		t.Errorf("the drops are %v, want %v", got, want)
	}
}

func TestACertificateThatNamesNoNodeIsNoPeer(t *testing.T) {
	if id, err := peerID(&x509.Certificate{}); err == nil {
		t.Errorf("a certificate with no common name is the node %q", id)
	}
	if id, err := peerID(&x509.Certificate{Subject: pkix.Name{CommonName: "node-1"}}); err != nil || id != "node-1" {
		t.Errorf("a certificate of node-1 is %q, %v", id, err)
	}
}

// hungTransport is a transport whose Close waits for release.
type hungTransport struct {
	transport.Transport
	release chan struct{}
}

func (h hungTransport) Close() error {
	<-h.release
	return h.Transport.Close()
}

// A Stop while another is stuck returns at once, and the calls it would
// race are refused.
func TestASecondStopDoesntWaitForAStuckFirst(t *testing.T) {
	t.Parallel()
	h := hungTransport{memnet.New(1, time.Millisecond).ListenAs("10.0.0.1:7443", "a"), make(chan struct{})}
	m, err := start(t.Context(), "a", h, nil)
	if err != nil {
		t.Fatal(err)
	}
	first := make(chan struct{})
	go func() {
		m.Stop()
		close(first)
	}()
	waitFor(t, "the first Stop to begin", func() bool { return m.SetSeeds(nil) != nil })
	second := make(chan struct{})
	go func() {
		m.Stop()
		close(second)
	}()
	select {
	case <-second:
	case <-time.After(5 * time.Second):
		t.Error("a second Stop waits for the first")
	}
	if err := m.Forget("b"); !errors.Is(err, ErrStopped) {
		t.Errorf("Forget during a Stop: %v, want ErrStopped", err)
	}
	close(h.release)
	<-first
}

// Stopped memberships leave no goroutine behind.
func TestAStoppedMembershipLeavesNoGoroutine(t *testing.T) {
	before := goruntime.NumGoroutine()
	net := memnet.New(1, time.Millisecond)
	for i := range 5 {
		m, err := start(context.Background(), "a", net.ListenAs(fmt.Sprintf("10.0.0.%d:7443", i), "a"), nil)
		if err != nil {
			t.Fatal(err)
		}
		m.Stop()
	}
	waitFor(t, "the goroutines to end", func() bool { return goruntime.NumGoroutine() <= before })
}

// Any sequence of calls keeps to a model of what the view holds and which
// calls are taken, up to and past Stop.
func TestCallsAgainstAModel(t *testing.T) {
	t.Parallel()
	for seed := range uint64(5) {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			t.Parallel()
			r := rand.New(rand.NewPCG(seed, 7))
			nodes := cluster(t, memnet.New(seed, time.Millisecond), 3)
			m := nodes[0]
			address := map[string]string{"node-1": "10.0.0.1:7443", "node-2": "10.0.0.2:7443"}
			forgotten := map[string]bool{}
			stopped := false
			for range 40 {
				id := fmt.Sprint("node-", 1+r.IntN(2))
				var err error
				op := r.IntN(12)
				refused := stopped && op != 0
				switch op {
				case 0:
					m.Stop()
					stopped = true
				case 1, 2, 3, 4, 5:
					if err = m.Forget(id); err == nil {
						forgotten[id] = true
					}
				default:
					if err = m.SetSeeds([]Seed{{NodeID: id, Address: address[id]}}); err == nil {
						delete(forgotten, id)
					}
				}
				if err != nil && !errors.Is(err, ErrStopped) || errors.Is(err, ErrStopped) != refused {
					t.Fatalf("a call returned %v, with stopped %v", err, stopped)
				}
				if stopped {
					continue
				}
				for id := range forgotten {
					if s, ok := states(m)[id]; ok {
						t.Fatalf("the forgotten %s is in the view, %s", id, s)
					}
				}
				for id := range address {
					if !forgotten[id] {
						waitFor(t, id+" taken back", func() bool { _, ok := states(m)[id]; return ok })
					}
				}
			}
			m.Stop()
			if err := m.SetSeeds(nil); !errors.Is(err, ErrStopped) {
				t.Errorf("SetSeeds after Stop: %v", err)
			}
		})
	}
}

// A member can gossip an entry whose name isn't UTF-8: it spreads, and
// the status of each node that holds it still goes out.
func TestAnEntryOfNoValidNameDoesntBreakAStatus(t *testing.T) {
	t.Parallel()
	net := memnet.New(1, time.Millisecond)
	a := startOn(t, t.Context(), net, "a", "10.0.0.1:7443")
	b := startOn(t, t.Context(), net, "b", "10.0.0.2:7443")
	if err := b.SetSeeds([]Seed{{NodeID: "a", Address: "10.0.0.1:7443"}}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "b alive", func() bool { return states(a)["b"] == Alive })
	entry := func(id member.ID, addr string) member.Member {
		return member.Member{ID: id, Addr: addr, Incarnation: 1, State: member.Alive, MinVersion: 1, Version: 1}
	}
	p, err := wire.EncodePacket(msg.Packet{From: entry("c", "10.0.0.3:7443"), Body: msg.Gossip{}, Gossip: []msg.Update{{Member: entry("\xff\xfe", "10.0.0.9:\xff")}}})
	if err != nil {
		t.Fatal(err)
	}
	net.ListenAs("10.0.0.3:7443", "c").SendPacket(transport.Dest{Addr: "10.0.0.1:7443", Peer: "a"}, p)
	for _, m := range []*Membership{a, b} {
		waitFor(t, "the entry in the view", func() bool { _, ok := states(m)["\uFFFD"]; return ok })
		for _, e := range m.Status().Members {
			if !utf8.ValidString(e.NodeID) || !utf8.ValidString(e.Address) {
				t.Errorf("the status holds %q at %q", e.NodeID, e.Address)
			}
		}
	}
}

func TestEveryDropOfTheRuntimeHasItsReason(t *testing.T) {
	got := runtimeDrops(runtime.Counts{Malformed: 1, Unknown: 2, WrongVersion: 3, Unsent: 4, Unauthenticated: 5})
	want := map[string]uint64{"malformed": 1, "unknown_kind": 2, "wrong_version": 3, "unencodable": 4, "unauthenticated": 5}
	if !maps.Equal(got, want) {
		t.Errorf("the drops are %v, want %v", got, want)
	}
}

// A node that hears nothing from the others shows it in its health score.
func TestStatusHasTheHealthScore(t *testing.T) {
	t.Parallel()
	net := memnet.New(1, time.Millisecond)
	nodes := cluster(t, net, 3)
	if s := nodes[0].Status().HealthScore; s != 0 {
		t.Errorf("a health score of %d in a whole cluster", s)
	}
	net.Cut(func(from, to string) bool { return from == "10.0.0.0:7443" || to == "10.0.0.0:7443" })
	waitFor(t, "the health score to rise", func() bool { return nodes[0].Status().HealthScore > 0 })
}

// Once Start's context ended, calls say membership stopped.
func TestCallsAfterTheContextEndsAreRefused(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	m := startOn(t, ctx, memnet.New(1, time.Millisecond), "a", "10.0.0.1:7443")
	cancel()
	waitFor(t, "the context to end it", func() bool { return errors.Is(m.SetSeeds(nil), ErrStopped) })
	if err := m.Forget("b"); !errors.Is(err, ErrStopped) {
		t.Errorf("Forget after the context ended: %v, want ErrStopped", err)
	}
}

func TestSetSeedsTakesNoneAndItselfAndRefusesAHalfSeed(t *testing.T) {
	t.Parallel()
	m := startOn(t, t.Context(), memnet.New(1, time.Millisecond), "a", "10.0.0.1:7443")
	for _, seeds := range [][]Seed{nil, {{NodeID: "a", Address: "10.0.0.1:7443"}}} {
		if err := m.SetSeeds(seeds); err != nil {
			t.Errorf("SetSeeds(%v): %v", seeds, err)
		}
	}
	for _, seed := range []Seed{{Address: "10.0.0.2:7443"}, {NodeID: "b"}} {
		if err := m.SetSeeds([]Seed{seed}); err == nil || errors.Is(err, ErrStopped) {
			t.Errorf("SetSeeds of %+v: %v, want it refused, not ErrStopped", seed, err)
		}
	}
}

// Whatever a member gossips as a name or an address, what leaves the
// adapter is valid UTF-8, and valid text is left as it is.
func FuzzConvertIsValidUTF8(f *testing.F) {
	f.Add("node-1", "10.0.0.1:7443")
	f.Add("\xff\xfe", "10.0.0.9:\xff")
	f.Add("", "\xc0")
	f.Fuzz(func(t *testing.T, id, addr string) {
		m := convert(member.Member{ID: member.ID(id), Addr: addr, State: member.Alive})
		if !utf8.ValidString(m.NodeID) || !utf8.ValidString(m.Address) {
			t.Fatalf("%q at %q converts to %q at %q", id, addr, m.NodeID, m.Address)
		}
		if utf8.ValidString(id) && m.NodeID != id || utf8.ValidString(addr) && m.Address != addr {
			t.Fatalf("%q at %q converts to %q at %q, want them as they were", id, addr, m.NodeID, m.Address)
		}
	})
}
