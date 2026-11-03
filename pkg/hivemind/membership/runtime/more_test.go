package runtime_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/moostackhq/moostack/pkg/hivemind/membership/member"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/msg"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/node"
	hmruntime "github.com/moostackhq/moostack/pkg/hivemind/membership/runtime"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/transport"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/transport/memnet"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/wire"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/wire/wiretest"
)

func TestStartRefusesWhatCantRun(t *testing.T) {
	net := memnet.New(1, time.Millisecond)
	for name, edit := range map[string]func(*node.Config){
		"versions 0":                     func(c *node.Config) { c.MinVersion, c.Version = 0, 0 },
		"a version without a format":     func(c *node.Config) { c.Version = 2 },
		"a long ID":                      func(c *node.Config) { c.ID = member.ID(strings.Repeat("x", 256)) },
		"no probe interval":              func(c *node.Config) { c.Probe.Interval = 0 },
		"an address not the transport's": func(c *node.Config) { c.Addr = "elsewhere" },
	} {
		c := fast("n", 1)
		edit(&c)
		if r, err := hmruntime.Start(context.Background(), c, net.Listen("n-"+name), hmruntime.Options{}); err == nil {
			r.Close()
			t.Errorf("%s: started", name)
		}
	}
	// The transport's own address, given, is no error.
	c := fast("n", 1)
	tr := net.Listen("n-own")
	c.Addr = tr.Addr()
	r, err := hmruntime.Start(context.Background(), c, tr, hmruntime.Options{})
	if err != nil {
		t.Fatal(err)
	}
	r.Close()
}

// A hook may call the runtime, Close included, without stopping the node.
func TestHooksMayCallTheRuntime(t *testing.T) {
	net := memnet.New(2, time.Millisecond)
	var r *hmruntime.Runtime
	left := make(chan struct{}, 1)
	opts := hmruntime.Options{OnChange: func(m member.Member) {
		if m.ID == "n1" && m.State == member.Alive {
			r.Leave()
			r.Close()
			left <- struct{}{}
		}
	}}
	var err error
	r, err = hmruntime.Start(context.Background(), fast("n0", 1), net.ListenAs("n0", "n0"), opts)
	if err != nil {
		t.Fatal(err)
	}
	other, _ := hmruntime.Start(context.Background(), fast("n1", 2), net.ListenAs("n1", "n1"), hmruntime.Options{})
	t.Cleanup(other.Close)
	other.Join("n0")
	select {
	case <-left:
	case <-time.After(5 * time.Second):
		t.Fatal("a hook calling Leave and Close never returned")
	}
	if err := r.Join("n1"); !errors.Is(err, hmruntime.ErrClosed) {
		t.Fatalf("Join after Close: %v", err)
	}
}

// closing is a transport whose Receive channel closes, as a transport that
// failed may.
type closing struct{ transport.Transport }

func (c closing) Receive() <-chan transport.Message {
	ch := make(chan transport.Message)
	close(ch)
	return ch
}

// A closed Receive channel ends receiving; the loop doesn't spin on it.
func TestAClosedReceiveDoesntSpin(t *testing.T) {
	net := memnet.New(3, time.Millisecond)
	r, err := hmruntime.Start(context.Background(), fast("n0", 1), closing{net.ListenAs("n0", "n0")}, hmruntime.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	time.Sleep(200 * time.Millisecond)
	if c := r.Counts(); c.Malformed != 0 {
		t.Fatalf("counted %d messages from a closed channel", c.Malformed)
	}
}

// Start and Close leave no goroutine behind.
func TestNothingIsLeftAfterClose(t *testing.T) {
	before := runtime.NumGoroutine()
	net := memnet.New(4, time.Millisecond)
	var rs []*hmruntime.Runtime
	for _, id := range []string{"a", "b", "c"} {
		r, err := hmruntime.Start(context.Background(), fast(id, 1), net.ListenAs(id, id), hmruntime.Options{OnEvent: func(node.Event) {}})
		if err != nil {
			t.Fatal(err)
		}
		rs = append(rs, r)
	}
	rs[1].Join("a")
	rs[2].Join("a")
	time.Sleep(300 * time.Millisecond)
	for _, r := range rs {
		r.Close()
	}
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > before {
		t.Fatalf("%d goroutines after Close, %d before", n, before)
	}
}

// Leave returns once the goodbyes are handed over: a node that leaves and
// closes at once reads as left, not dead, even with every stream lost.
func TestLeaveThenCloseIsALeave(t *testing.T) {
	net := memnet.New(5, time.Millisecond)
	rs := start(t, net, 5)
	waitFor(t, "converged", 5*time.Second, func() bool {
		for _, r := range rs {
			for i := range rs {
				if !sees(r, fmt.Sprintf("n%d", i), member.Alive) {
					return false
				}
			}
		}
		return true
	})
	rs[4].Leave()
	rs[4].Close()
	defer func() {
		if t.Failed() {
			for _, r := range rs[:4] {
				m, _ := r.Self(), 0
				for _, e := range r.Members() {
					if e.ID == "n4" {
						t.Log(m.ID, "has", e)
					}
				}
			}
		}
	}()
	waitFor(t, "n4 left everywhere", 5*time.Second, func() bool {
		for _, r := range rs[:4] {
			if !sees(r, "n4", member.Left) {
				return false
			}
		}
		return true
	})
}

// Nodes of different wire versions on memnet: the runtime picks each
// peer's version, answers what it can't read, and counts it.
func TestVersionsThroughTheRuntime(t *testing.T) {
	net := memnet.New(6, time.Millisecond)
	formats := []wire.Format{wiretest.Breaking(2)}
	start := func(id string, lo, hi uint8, seed uint64) *hmruntime.Runtime {
		c := fast(id, seed)
		c.MinVersion, c.Version = lo, hi
		r, err := hmruntime.Start(context.Background(), c, net.ListenAs(id, id), hmruntime.Options{Formats: formats})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(r.Close)
		return r
	}
	a := start("a", 1, 2, 1)
	b := start("b", 1, 2, 2)
	c := start("c", 1, 1, 3)
	d := start("d", 2, 2, 4)
	b.Join("a")
	c.Join("a")
	waitFor(t, "a, b and c converge", 5*time.Second, func() bool {
		return sees(a, "c", member.Alive) && sees(c, "b", member.Alive) && sees(b, "c", member.Alive)
	})
	d.Join("c") // c reads only 1, d only 2
	waitFor(t, "c counts what it can't read", 5*time.Second, func() bool { return c.Counts().WrongVersion > 0 })
	if got := a.Counts(); got.Malformed != 0 {
		t.Fatalf("a counted %+v", got)
	}
}

func TestDecode(t *testing.T) {
	c, _ := wire.NewCodec(1, 1)
	ack, _ := wire.EncodePacket(msgAck())
	if in, err := hmruntime.Decode(c, transport.Message{From: "x", Data: ack}); err != nil {
		t.Fatal(err)
	} else if _, ok := in.(node.Receive); !ok {
		t.Fatalf("%T", in)
	}
	newer := append([]byte{3}, ack[1:]...)
	in, err := hmruntime.Decode(c, transport.Message{From: "x", Peer: "p", Data: newer})
	if u, ok := in.(node.Unreadable); err != nil || !ok || u.Addr != "x" || u.Peer != "p" || u.Version != 3 {
		t.Fatalf("%#v, %v", in, err)
	}
	if _, err := hmruntime.Decode(c, transport.Message{Data: []byte{1, 99}}); !errors.Is(err, wire.ErrUnknownKind) {
		t.Fatalf("unknown kind: %v", err)
	}
	// The peer the transport proved goes with the input, packet or sync.
	if in, err := hmruntime.Decode(c, transport.Message{From: "x", Peer: "p", Data: ack}); err != nil || in.(node.Receive).Peer != "p" {
		t.Fatalf("%#v, %v", in, err)
	}
	sync, _ := wire.EncodeSync(msg.Sync{From: member.Member{ID: "a"}})
	if in, err := hmruntime.Decode(c, transport.Message{From: "x", Peer: "p", Data: sync, Stream: true}); err != nil || in.(node.ReceiveSync).Peer != "p" {
		t.Fatalf("%#v, %v", in, err)
	}
}

func TestAnUnauthenticatedClusterFormsWhenAllowed(t *testing.T) {
	net := memnet.New(12, time.Millisecond)
	var rs []*hmruntime.Runtime
	for i := range 3 {
		id := fmt.Sprintf("n%d", i)
		r, err := hmruntime.Start(context.Background(), fast(id, uint64(i+1)), net.Listen(id), hmruntime.Options{Unauthenticated: true})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(r.Close)
		rs = append(rs, r)
	}
	for _, r := range rs[1:] {
		r.Join("n0")
	}
	waitFor(t, "the cluster formed", 10*time.Second, func() bool {
		return len(rs[0].Members()) == 3 && len(rs[1].Members()) == 3 && len(rs[2].Members()) == 3
	})
}

// What a node means for a member reaches no other at the member's address:
// the runtime hands the transport the member it means (transport.Dest),
// and an endpoint there under another identity gets nothing.
func TestWhatIsMeantForAMemberReachesNoOtherAtItsAddress(t *testing.T) {
	net := memnet.New(13, time.Millisecond)
	var rs []*hmruntime.Runtime
	for i := range 2 {
		id := fmt.Sprintf("n%d", i)
		c := fast(id, uint64(i+1))
		// A sync every period: syncs go to n1's address too before n1 is
		// found dead.
		c.SyncEvery, c.SeedSyncEvery = 1, 0
		r, err := hmruntime.Start(context.Background(), c, net.ListenAs(id, id), hmruntime.Options{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(r.Close)
		rs = append(rs, r)
	}
	rs[1].Join("n0")
	waitFor(t, "n0 sees n1", 10*time.Second, func() bool { return sees(rs[0], "n1", member.Alive) })
	// n1 stops, and another identity takes its address.
	rs[1].Close()
	other := net.ListenAs("n1", "intruder")
	t.Cleanup(func() { other.Close() })
	select {
	case m := <-other.Receive():
		t.Fatalf("the intruder at n1's address got a message from %s", m.From)
	case <-time.After(500 * time.Millisecond):
	}
}

// Over a transport that proves who sends, a cluster forms as over one
// that doesn't, and a member that speaks as another is dropped: the node
// reports it, and its claim changes nothing.
func TestAnAuthenticatedClusterDropsImpersonation(t *testing.T) {
	net := memnet.New(11, time.Millisecond)
	impersonated := make(chan node.Event, 16)
	var rs []*hmruntime.Runtime
	for i := range 3 {
		id := fmt.Sprintf("n%d", i)
		opts := hmruntime.Options{}
		if i == 0 {
			opts.OnEvent = func(e node.Event) {
				if e.Kind == node.Impersonated {
					impersonated <- e
				}
			}
		}
		r, err := hmruntime.Start(context.Background(), fast(id, uint64(i+1)), net.ListenAs(id, id), opts)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(r.Close)
		rs = append(rs, r)
	}
	for _, r := range rs[1:] {
		r.Join("n0")
	}
	deadline := time.Now().Add(10 * time.Second)
	for len(rs[0].Members()) < 3 || len(rs[1].Members()) < 3 || len(rs[2].Members()) < 3 {
		if time.Now().After(deadline) {
			t.Fatal("the authenticated cluster didn't form")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// n2, admitted, says n1 left, in n1's name.
	var n1 member.Member
	for _, m := range rs[0].Members() {
		if m.ID == "n1" {
			n1 = m
		}
	}
	n1.State, n1.Incarnation = member.Left, n1.Incarnation+1
	b, err := wire.EncodePacket(msg.Packet{From: n1, Body: msg.Gossip{}})
	if err != nil {
		t.Fatal(err)
	}
	mallory := net.ListenAs("n2-again", "n2")
	t.Cleanup(func() { mallory.Close() })
	mallory.SendPacket(transport.Dest{Addr: "n0", Peer: "n0"}, b)
	select {
	case e := <-impersonated:
		if e.Member != "n1" || e.By != "n2" {
			t.Fatalf("Impersonated %+v, want n1 by n2", e)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no Impersonated event")
	}
	for _, m := range rs[0].Members() {
		if m.ID == "n1" && m.State == member.Left {
			t.Fatal("n0 took n1's leaving from n2")
		}
	}
}

// The forget hook sees a member DeadTimeout after it died, and a fresh
// start takes its incarnation from the clock.
func TestForgetHookAndClockIncarnation(t *testing.T) {
	net := memnet.New(7, time.Millisecond)
	var forgot atomic.Bool
	c := fast("a", 1)
	c.DeadTimeout = 300 * time.Millisecond
	a, err := hmruntime.Start(context.Background(), c, net.ListenAs("a", "a"), hmruntime.Options{OnForget: func(m member.Member) {
		if m.ID == "b" {
			forgot.Store(true)
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(a.Close)
	if inc := a.Self().Incarnation; inc < uint64(time.Now().Add(-time.Minute).UnixMilli()) {
		t.Fatalf("incarnation %d, not from the clock", inc)
	}
	b, _ := hmruntime.Start(context.Background(), fast("b", 2), net.ListenAs("b", "b"), hmruntime.Options{})
	b.Join("a")
	waitFor(t, "b joined", 5*time.Second, func() bool { return sees(a, "b", member.Alive) })
	b.Close()
	waitFor(t, "b forgotten", 10*time.Second, forgot.Load)
}
