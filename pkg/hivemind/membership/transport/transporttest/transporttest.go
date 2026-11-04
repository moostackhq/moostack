// Package transporttest checks a transport against the promises of
// transport.Transport and transport.Dest, the same for every
// implementation that proves who sends.
package transporttest

import (
	"testing"
	"time"

	"github.com/moostackhq/moostack/pkg/hivemind/membership/transport"
)

// Pair is two endpoints of one network: A proves it is the member "a", and
// B the member "b".
type Pair struct{ A, B transport.Transport }

// Run checks the transports newPair makes against the port's promises,
// each on a pair of its own.
func Run(t *testing.T, newPair func(t *testing.T) Pair) {
	for name, check := range map[string]func(t *testing.T, p Pair){
		"a message meant for its member arrives, from its proven sender": meantFor,
		"a message meant for no member is dropped":                       noMember,
		"a message meant for another member at the address is dropped":   anotherMember,
		"a send takes a copy":                  sendCopies,
		"sending never blocks":                 neverBlocks,
		"Close ends Receive":                   closeEnds,
		"Close returns while sends to it wait": closeWhileSent,
	} {
		t.Run(name, func(t *testing.T) { check(t, newPair(t)) })
	}
}

func send(tr transport.Transport, to transport.Dest, b []byte, stream bool) {
	if stream {
		tr.SendStream(to, b)
	} else {
		tr.SendPacket(to, b)
	}
}

func next(tr transport.Transport, d time.Duration) (transport.Message, bool) {
	select {
	case m, ok := <-tr.Receive():
		return m, ok
	case <-time.After(d):
		return transport.Message{}, false
	}
}

func meantFor(t *testing.T, p Pair) {
	for _, stream := range []bool{false, true} {
		send(p.A, transport.Dest{Addr: p.B.Addr(), Peer: "b"}, []byte("hello"), stream)
		m, ok := next(p.B, 2*time.Second)
		if !ok || string(m.Data) != "hello" || m.Peer != "a" || m.From != p.A.Addr() || m.Stream != stream {
			t.Fatalf("stream %v: got %+v, %v; want hello from a at %s", stream, m, ok, p.A.Addr())
		}
	}
}

func noMember(t *testing.T, p Pair) {
	for _, stream := range []bool{false, true} {
		send(p.A, transport.Dest{Addr: p.B.Addr()}, []byte("nobody"), stream)
	}
	if m, ok := next(p.B, time.Second); ok {
		t.Fatalf("got %+v, meant for no member", m)
	}
}

func anotherMember(t *testing.T, p Pair) {
	for _, stream := range []bool{false, true} {
		send(p.A, transport.Dest{Addr: p.B.Addr(), Peer: "c"}, []byte("for c"), stream)
	}
	if m, ok := next(p.B, time.Second); ok {
		t.Fatalf("got %+v, meant for c", m)
	}
}

func sendCopies(t *testing.T, p Pair) {
	for _, stream := range []bool{false, true} {
		buf := []byte("first")
		send(p.A, transport.Dest{Addr: p.B.Addr(), Peer: "b"}, buf, stream)
		copy(buf, "other")
		if m, ok := next(p.B, 2*time.Second); !ok || string(m.Data) != "first" {
			t.Fatalf("stream %v: got %q, %v; want what was sent, \"first\"", stream, m.Data, ok)
		}
	}
}

func neverBlocks(t *testing.T, p Pair) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range 10000 {
			stream := i%2 == 1
			send(p.A, transport.Dest{Addr: "127.0.0.1:9", Peer: "nobody"}, []byte("lost"), stream)
			send(p.A, transport.Dest{Addr: p.B.Addr(), Peer: "b"}, []byte("flood"), stream)
		}
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("20000 packets and streams, to nobody and to a member that doesn't read, didn't return within 10s")
	}
}

func closeEnds(t *testing.T, p Pair) {
	p.B.Close()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case _, ok := <-p.B.Receive():
			if !ok {
				send(p.A, transport.Dest{Addr: p.B.Addr(), Peer: "b"}, []byte("after"), false)
				return
			}
		case <-deadline:
			t.Fatal("Receive still open 5s after Close")
		}
	}
}

func closeWhileSent(t *testing.T, p Pair) {
	for range 2000 {
		send(p.A, transport.Dest{Addr: p.B.Addr(), Peer: "b"}, []byte("waiting"), true)
	}
	time.Sleep(100 * time.Millisecond)
	closed := make(chan struct{})
	go func() {
		p.B.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close didn't return within 5s while streams to it waited")
	}
}
