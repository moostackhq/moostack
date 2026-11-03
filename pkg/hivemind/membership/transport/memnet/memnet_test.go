package memnet

import (
	"testing"
	"time"

	"github.com/moostackhq/moostack/pkg/hivemind/membership/transport"
)

func recv(t *testing.T, e *Endpoint, within time.Duration) (transport.Message, bool) {
	t.Helper()
	select {
	case m := <-e.Receive():
		return m, true
	case <-time.After(within):
		return transport.Message{}, false
	}
}

func TestDeliversAfterTheDelayACopy(t *testing.T) {
	n := New(1, 30*time.Millisecond)
	a, b := n.Listen("a"), n.Listen("b")
	buf := []byte("hello")
	start := time.Now()
	a.SendPacket(transport.Dest{Addr: "b"}, buf)
	buf[0] = 'j'
	m, ok := recv(t, b, time.Second)
	if !ok || string(m.Data) != "hello" || m.From != "a" || m.Stream {
		t.Fatalf("got %+v, %v", m, ok)
	}
	if took := time.Since(start); took < 30*time.Millisecond {
		t.Fatalf("delivered after %v, before the delay", took)
	}
}

// An endpoint listening as a peer stamps its messages with that identity,
// packets and streams alike; one listening without stamps none.
func TestListenAsCarriesThePeer(t *testing.T) {
	n := New(1, 0)
	a, b := n.ListenAs("a", "node-a"), n.Listen("b")
	a.SendPacket(transport.Dest{Addr: "b"}, []byte("p"))
	a.SendStream(transport.Dest{Addr: "b"}, []byte("s"))
	for range 2 {
		if m, ok := recv(t, b, time.Second); !ok || m.Peer != "node-a" || m.From != "a" {
			t.Fatalf("got %+v, %v", m, ok)
		}
	}
	b.SendPacket(transport.Dest{Addr: "a"}, []byte("p"))
	if m, ok := recv(t, a, time.Second); !ok || m.Peer != "" {
		t.Fatalf("got %+v, %v", m, ok)
	}
}

// An endpoint listening as a peer takes no message meant for another; one
// meant for it, or for nobody in particular, it takes. An endpoint listening as nobody takes all.
func TestAMessageMeantForAnotherPeerIsDropped(t *testing.T) {
	n := New(1, 0)
	a, b, c := n.Listen("a"), n.ListenAs("b", "node-b"), n.Listen("c")
	a.SendPacket(transport.Dest{Addr: "b", Peer: "node-x"}, []byte("wrong"))
	if m, ok := recv(t, b, 50*time.Millisecond); ok {
		t.Fatalf("took %q, meant for another peer", m.Data)
	}
	a.SendPacket(transport.Dest{Addr: "b", Peer: "node-b"}, []byte("right"))
	a.SendStream(transport.Dest{Addr: "b"}, []byte("unpinned"))
	got := map[string]bool{}
	for range 2 {
		if m, ok := recv(t, b, time.Second); ok {
			got[string(m.Data)] = true
		}
	}
	if !got["right"] || !got["unpinned"] {
		t.Fatalf("got %v, want the pinned and the unpinned message", got)
	}
	a.SendPacket(transport.Dest{Addr: "c", Peer: "node-x"}, []byte("any"))
	if _, ok := recv(t, c, time.Second); !ok {
		t.Fatal("an endpoint listening as nobody dropped a pinned message")
	}
	if n.Lost() != 1 {
		t.Fatalf("Lost = %d, want 1", n.Lost())
	}
}

func TestLoss(t *testing.T) {
	n := New(2, 0)
	a, b := n.Listen("a"), n.Listen("b")
	n.SetLoss(0.5)
	for range 400 {
		a.SendPacket(transport.Dest{Addr: "b"}, []byte{1})
	}
	got := 0
	for {
		if _, ok := recv(t, b, 50*time.Millisecond); !ok {
			break
		}
		got++
	}
	if got < 120 || got > 280 || n.Lost() != 400-got {
		t.Fatalf("%d of 400 arrived at loss 0.5, %d counted lost", got, n.Lost())
	}
	a.SendStream(transport.Dest{Addr: "b"}, []byte{2})
	if _, ok := recv(t, b, time.Second); !ok {
		t.Fatal("a stream was lost")
	}
}

func TestJitter(t *testing.T) {
	n := New(3, 0)
	n.SetDelay(5*time.Millisecond, 40*time.Millisecond)
	a, b := n.Listen("a"), n.Listen("b")
	for i := range 50 {
		a.SendPacket(transport.Dest{Addr: "b"}, []byte{byte(i)})
	}
	inOrder := true
	for i := range 50 {
		m, ok := recv(t, b, time.Second)
		if !ok {
			t.Fatal("lost with no loss set")
		}
		if m.Data[0] != byte(i) {
			inOrder = false
		}
	}
	if inOrder {
		t.Fatal("jitter of 40ms over 50 packets kept every one in order")
	}
}

// A cut drops both ways it says, packets and streams, those in flight too,
// and healing restores them.
func TestCut(t *testing.T) {
	n := New(4, 20*time.Millisecond)
	a, b := n.Listen("a"), n.Listen("b")
	a.SendPacket(transport.Dest{Addr: "b"}, []byte{1}) // in flight when the cut comes
	n.Cut(func(from, to string) bool { return from == "a" })
	a.SendStream(transport.Dest{Addr: "b"}, []byte{2})
	if _, ok := recv(t, b, 100*time.Millisecond); ok {
		t.Fatal("a message crossed the cut")
	}
	b.SendPacket(transport.Dest{Addr: "a"}, []byte{3})
	if _, ok := recv(t, a, time.Second); !ok {
		t.Fatal("the other way was cut too")
	}
	n.Cut(nil)
	a.SendPacket(transport.Dest{Addr: "b"}, []byte{4})
	if _, ok := recv(t, b, time.Second); !ok {
		t.Fatal("healing didn't restore the link")
	}
}

func TestAFullBufferDropsPacketsNotStreams(t *testing.T) {
	n := New(5, 0)
	a, b := n.Listen("a"), n.Listen("b")
	for range 1100 {
		a.SendStream(transport.Dest{Addr: "b"}, []byte{1})
	}
	time.Sleep(50 * time.Millisecond)
	got := 0
	for {
		if _, ok := recv(t, b, 100*time.Millisecond); !ok {
			break
		}
		got++
	}
	if got != 1100 {
		t.Fatalf("%d of 1100 streams arrived", got)
	}
}

func TestClose(t *testing.T) {
	n := New(6, 0)
	a, b := n.Listen("a"), n.Listen("b")
	b.Close()
	b.Close()
	a.SendPacket(transport.Dest{Addr: "b"}, []byte{1})
	a.SendStream(transport.Dest{Addr: "b"}, []byte{1})
	if n.Lost() != 2 {
		t.Fatalf("Lost = %d after sending to a closed endpoint", n.Lost())
	}
	b2 := n.Listen("b")
	a.SendPacket(transport.Dest{Addr: "b"}, []byte{2})
	if m, ok := recv(t, b2, time.Second); !ok || m.Data[0] != 2 {
		t.Fatal("listening again didn't receive")
	}
}

// A packet in flight when a cut comes counts as lost, as one dropped at
// sending does.
func TestACutInFlightCountsAsLost(t *testing.T) {
	n := New(4, 20*time.Millisecond)
	a, b := n.Listen("a"), n.Listen("b")
	a.SendPacket(transport.Dest{Addr: "b"}, []byte{1})
	n.Cut(func(from, to string) bool { return true })
	if _, ok := recv(t, b, 100*time.Millisecond); ok {
		t.Fatal("a packet crossed the cut")
	}
	if got := n.Lost(); got != 1 {
		t.Fatalf("Lost = %d, want 1", got)
	}
}

// A loss of p drops about p of the packets, not 1 − p.
func TestLossIsTheShareDropped(t *testing.T) {
	n := New(3, 0)
	a, b := n.Listen("a"), n.Listen("b")
	n.SetLoss(0.1)
	for range 400 {
		a.SendPacket(transport.Dest{Addr: "b"}, []byte{1})
	}
	got := 0
	for {
		if _, ok := recv(t, b, 50*time.Millisecond); !ok {
			break
		}
		got++
	}
	if got < 320 {
		t.Fatalf("%d of 400 arrived at loss 0.1", got)
	}
}
