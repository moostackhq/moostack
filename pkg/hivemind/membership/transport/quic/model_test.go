package quic_test

import (
	"flag"
	"fmt"
	"math/rand/v2"
	goruntime "runtime"
	"testing"
	"time"

	"github.com/moostackhq/moostack/pkg/hivemind/membership/transport"
	. "github.com/moostackhq/moostack/pkg/hivemind/membership/transport/quic"
)

var seed = flag.Uint64("seed", 0, "the seed of TestSequencesKeepTheModel's runs; 0 picks one")

// model is what a, the transport under test, has been asked to do, and
// what its peers must have received.
type model struct {
	sent    uint64          // messages a was handed
	removed map[string]bool // members a cut off
	closed  bool
	toPeer  map[string][]string // what each peer must receive from a, in no order
	never   map[string]bool     // what no peer may receive: sent after its removal
}

// Generated sequences of sends to two members and an unpinned
// destination, the removal of one member and Close, from a and to it,
// keep the model: a member receives what was sent to it before Close,
// unless it is removed, and from its removal nothing more in either
// direction; and every message a was handed is sent or counted once as
// dropped.
func TestSequencesKeepTheModel(t *testing.T) {
	s := *seed
	if s == 0 {
		s = uint64(time.Now().UnixNano())
	}
	t.Logf("seed %d: rerun with -seed %d", s, s)
	r := rand.New(rand.NewPCG(s, s))
	for run := range 10 {
		t.Run(fmt.Sprint(run), func(t *testing.T) { runSequence(t, r) })
	}
}

func (m *model) expect(name, body string) {
	switch {
	case m.removed[name]:
		m.never[body] = true
	case !m.closed:
		m.toPeer[name] = append(m.toPeer[name], body)
	}
}

func runSequence(t *testing.T, r *rand.Rand) {
	c := newCluster(t)
	a := listen(t, c.Node("a"))
	ids := map[string]ident{"b": c.Node("b"), "c": c.Node("c")}
	peers := map[string]*Transport{"b": listen(t, ids["b"]), "c": listen(t, ids["c"])}
	m := model{removed: map[string]bool{}, toPeer: map[string][]string{}, never: map[string]bool{}}
	names := []string{"b", "c"}
	for step := range 30 {
		name := names[r.IntN(2)]
		body := fmt.Sprintf("%s-%d", name, step)
		switch op := r.IntN(10); {
		case op < 4:
			a.SendPacket(dest(peers[name], name), []byte(body))
			m.sent++
			m.expect(name, body)
		case op < 7:
			a.SendStream(dest(peers[name], name), []byte(body))
			m.sent++
			m.expect(name, body)
		case op == 7:
			a.SendPacket(transport.Dest{Addr: peers[name].Addr()}, []byte(body))
			m.sent++
		case op == 8 && !m.removed[name]:
			c.Remove(ids[name])
			a.CutOff(c.IsRemoved)
			m.removed[name] = true
			// What a still had queued for it may never go.
			m.toPeer[name] = nil
			// What the member sends a from then on never arrives.
			peers[name].SendPacket(dest(a, "a"), []byte("from "+name))
		case op == 9 && r.IntN(5) == 0:
			a.Close()
			m.closed = true
		}
	}
	a.Close()
	for _, name := range names {
		want := map[string]bool{}
		for _, b := range m.toPeer[name] {
			want[b] = true
		}
		deadline := time.After(3 * time.Second)
		for done := false; !done; {
			wait := 100 * time.Millisecond
			if len(want) > 0 {
				wait = 3 * time.Second
			}
			select {
			case got := <-peers[name].Receive():
				if got.Peer != "a" {
					t.Fatalf("%s received %q from %q", name, got.Data, got.Peer)
				}
				if m.never[string(got.Data)] {
					t.Fatalf("%s received %q, sent after its removal", name, got.Data)
				}
				delete(want, string(got.Data))
			case <-deadline:
				t.Fatalf("%s didn't receive %v; a's stats %+v", name, want, a.Stats())
			case <-time.After(wait):
				done = len(want) == 0
			}
		}
	}
	for got := range a.Receive() {
		if m.removed[got.Peer] {
			t.Fatalf("a received %q from %s, removed", got.Data, got.Peer)
		}
	}
	s := a.Stats()
	d := s.Dropped
	if n := s.PacketsOut + s.StreamsOut + d.Unpinned + d.Oversize + d.QueueFull + d.Unsent + d.Closed; n != m.sent {
		t.Fatalf("a was handed %d messages, and sent or dropped %d: %+v", m.sent, n, s)
	}
	if s.Links != 0 || s.Conns != 0 {
		t.Fatalf("a closed with %d links and %d connections", s.Links, s.Conns)
	}
}

// Transports that opened, talked and closed leave no goroutine behind.
func TestClosedTransportsLeaveNoGoroutine(t *testing.T) {
	before := goruntime.NumGoroutine()
	func() {
		c := newCluster(t)
		a, err := Listen(c.Node("a").Config("127.0.0.1:0"))
		if err != nil {
			t.Fatal(err)
		}
		b, err := Listen(c.Node("b").Config("127.0.0.1:0"))
		if err != nil {
			t.Fatal(err)
		}
		a.SendPacket(dest(b, "b"), []byte("ping"))
		a.SendStream(dest(b, "b"), []byte("view"))
		a.SendPacket(transport.Dest{Addr: "127.0.0.1:9", Peer: "gone"}, []byte("x"))
		for range 2 {
			if _, ok := next(b, 2*time.Second); !ok {
				t.Fatal("b received nothing")
			}
		}
		a.Close()
		b.Close()
	}()
	deadline := time.Now().Add(5 * time.Second)
	for goruntime.NumGoroutine() > before {
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<20)
			t.Fatalf("%d goroutines more than before:\n%s", goruntime.NumGoroutine()-before, buf[:goruntime.Stack(buf, true)])
		}
		time.Sleep(10 * time.Millisecond)
	}
}
