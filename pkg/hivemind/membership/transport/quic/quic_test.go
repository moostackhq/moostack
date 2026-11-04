package quic_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	goruntime "runtime"
	"sync"
	"testing"
	"time"

	quicgo "github.com/quic-go/quic-go"

	"github.com/moostackhq/moostack/pkg/hivemind/membership/transport"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/transport/transporttest"

	. "github.com/moostackhq/moostack/pkg/hivemind/membership/transport/quic"
)

func listen(t *testing.T, id ident) *Transport {
	t.Helper()
	tr, err := Listen(id.Config("127.0.0.1:0"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tr.Close() })
	return tr
}

func dest(tr *Transport, peer string) transport.Dest {
	return transport.Dest{Addr: tr.Addr(), Peer: peer}
}

// next returns the next message tr receives within d, or false.
func next(tr *Transport, d time.Duration) (transport.Message, bool) {
	select {
	case m, ok := <-tr.Receive():
		return m, ok
	case <-time.After(d):
		return transport.Message{}, false
	}
}

func TestPacketsAndStreamsCarryTheProvenSenderAndItsListenAddress(t *testing.T) {
	c := newCluster(t)
	a, b := listen(t, c.Node("a")), listen(t, c.Node("b"))
	a.SendPacket(dest(b, "b"), []byte("ping"))
	m, ok := next(b, 2*time.Second)
	if !ok || m.Peer != "a" || m.From != a.Addr() || m.Stream || string(m.Data) != "ping" {
		t.Fatalf("got %+v, %v; want a packet from a at %s", m, ok, a.Addr())
	}
	// The answer goes back on the connection a opened.
	b.SendStream(dest(a, "a"), []byte("view"))
	m, ok = next(a, 2*time.Second)
	if !ok || m.Peer != "b" || m.From != b.Addr() || !m.Stream || string(m.Data) != "view" {
		t.Fatalf("got %+v, %v; want a stream from b at %s", m, ok, b.Addr())
	}
	if s := b.Stats(); s.Dials != 0 || s.Accepts != 1 {
		t.Errorf("b dialled %d and accepted %d; want one connection, a's", s.Dials, s.Accepts)
	}
}

func TestAMessageForOneMemberDoesNotReachAnotherAtItsAddress(t *testing.T) {
	c := newCluster(t)
	a, b := listen(t, c.Node("a")), listen(t, c.Node("b"))
	a.SendPacket(dest(b, "c"), []byte("for c"))
	if m, ok := next(b, time.Second); ok {
		t.Fatalf("b received %+v, meant for c", m)
	}
	if s := a.Stats(); s.DialFails != 1 || s.PacketsOut != 0 {
		t.Errorf("stats %+v; want one failed dial and nothing sent", s)
	}
}

func TestAMessageWithNoMemberToPinIsDropped(t *testing.T) {
	c := newCluster(t)
	a, b := listen(t, c.Node("a")), listen(t, c.Node("b"))
	a.SendPacket(transport.Dest{Addr: b.Addr()}, []byte("to whoever"))
	if m, ok := next(b, time.Second); ok {
		t.Fatalf("b received %+v", m)
	}
	if s := a.Stats(); s.Dropped.Unpinned != 1 || s.Dials != 0 {
		t.Errorf("stats %+v; want one unpinned and no dial", s)
	}
}

// dialRaw dials tr's member a with QUIC directly, trusting the CA
// certificates in pool, presenting certs, and sends one datagram: an
// attacker that knows the CA certificate.
func dialRaw(t *testing.T, tr *Transport, pool *x509.CertPool, certs []tls.Certificate) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	conn, err := quicgo.DialAddr(ctx, tr.Addr(), &tls.Config{
		MinVersion: tls.VersionTLS13, RootCAs: pool, ServerName: "a", NextProtos: []string{ALPN},
		Certificates: certs,
	}, &quicgo.Config{EnableDatagrams: true})
	if err == nil {
		// TLS 1.3 clients finish before the server checks them: a refusal
		// comes as the connection closing.
		conn.SendDatagram([]byte("let me in"))
		select {
		case <-conn.Context().Done():
		case <-time.After(time.Second):
		}
		conn.CloseWithError(0, "")
	}
}

func TestANodeOfAnotherClusterCanNotSend(t *testing.T) {
	c := newCluster(t)
	a := listen(t, c.Node("a"))
	stranger := newCluster(t).Node("s")
	dialRaw(t, a, c.Pool, []tls.Certificate{stranger.Cert})
	if m, ok := next(a, 500*time.Millisecond); ok {
		t.Fatalf("a received %+v from another cluster", m)
	}
	if s := a.Stats(); s.Accepts != 0 {
		t.Errorf("a accepted %d connections", s.Accepts)
	}
}

func TestANodeOfAnotherClusterIsNotReached(t *testing.T) {
	a := listen(t, newCluster(t).Node("a"))
	stranger := listen(t, newCluster(t).Node("s"))
	a.SendPacket(dest(stranger, "s"), []byte("hello"))
	if m, ok := next(stranger, time.Second); ok {
		t.Fatalf("another cluster's node received %+v", m)
	}
	if s := a.Stats(); s.DialFails != 1 {
		t.Errorf("stats %+v; want a failed dial", s)
	}
}

// Same cluster, same name, but its certificate has expired.
func TestAnExpiredIdentityCanNotSend(t *testing.T) {
	c := newCluster(t)
	a := listen(t, c.Node("a"))
	old := listen(t, c.Issue("b", time.Now().Add(-time.Minute)))
	old.SendPacket(dest(a, "a"), []byte("still me"))
	if m, ok := next(a, time.Second); ok {
		t.Fatalf("a received %+v from an expired identity", m)
	}
}

func TestADialerWithNoCertificateIsRefused(t *testing.T) {
	c := newCluster(t)
	a := listen(t, c.Node("a"))
	dialRaw(t, a, c.Pool, nil)
	if m, ok := next(a, 500*time.Millisecond); ok {
		t.Fatalf("a received %+v from a dialer with no certificate", m)
	}
	if s := a.Stats(); s.Accepts != 0 {
		t.Errorf("a accepted %d connections", s.Accepts)
	}
}

func TestARemovedIdentityIsCutOffAndCanNotComeBack(t *testing.T) {
	c := newCluster(t)
	bid := c.Node("b")
	a, b := listen(t, c.Node("a")), listen(t, bid)
	b.SendPacket(dest(a, "a"), []byte("1"))
	if _, ok := next(a, 2*time.Second); !ok {
		t.Fatal("a received nothing before the removal")
	}
	c.Remove(bid)
	a.CutOff(c.IsRemoved)
	deadline := time.Now().Add(2 * time.Second)
	for a.Stats().Conns != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("a still has %d connections", a.Stats().Conns)
		}
		time.Sleep(10 * time.Millisecond)
	}
	for range 3 {
		b.SendPacket(dest(a, "a"), []byte("2"))
		a.SendPacket(dest(b, "b"), []byte("3"))
	}
	if m, ok := next(a, time.Second); ok {
		t.Fatalf("a received %+v after the removal", m)
	}
	if m, ok := next(b, 100*time.Millisecond); ok {
		t.Fatalf("b received %+v after the removal", m)
	}
}

// Membership's packets are at most node.Defaults' MaxPacket, 1150 bytes.
func TestAFullPacketFitsADatagram(t *testing.T) {
	c := newCluster(t)
	a, b := listen(t, c.Node("a")), listen(t, c.Node("b"))
	a.SendPacket(dest(b, "b"), bytes.Repeat([]byte{1}, 1150))
	if m, ok := next(b, 2*time.Second); !ok || len(m.Data) != 1150 {
		t.Fatalf("got %d bytes, %v; want 1150", len(m.Data), ok)
	}
	largest := 0
	for size := 1150; size <= 1500; size++ {
		a.SendPacket(dest(b, "b"), make([]byte, size))
		m, ok := next(b, 200*time.Millisecond)
		if !ok {
			break
		}
		largest = len(m.Data)
	}
	t.Logf("the largest datagram on a fresh loopback connection: %d bytes", largest)
}

func TestTheLargestSyncArrivesAndALargerOneDoesNot(t *testing.T) {
	c := newCluster(t)
	a, b := listen(t, c.Node("a")), listen(t, c.Node("b"))
	a.SendStream(dest(b, "b"), bytes.Repeat([]byte{2}, MaxStream))
	if m, ok := next(b, 5*time.Second); !ok || len(m.Data) != MaxStream {
		t.Fatalf("got %d bytes, %v; want %d", len(m.Data), ok, MaxStream)
	}
	a.SendStream(dest(b, "b"), make([]byte, MaxStream+1))
	if m, ok := next(b, time.Second); ok {
		t.Fatalf("b received %d bytes", len(m.Data))
	}
	if s := a.Stats(); s.Dropped.Oversize != 1 {
		t.Errorf("a counted %d oversize; want 1", s.Dropped.Oversize)
	}
}

func TestSendingNeverBlocks(t *testing.T) {
	c := newCluster(t)
	a := listen(t, c.Node("a"))
	start := time.Now()
	for range 10000 {
		a.SendPacket(transport.Dest{Addr: "127.0.0.1:9", Peer: "gone"}, []byte("x"))
	}
	if d := time.Since(start); d > 100*time.Millisecond {
		t.Fatalf("10,000 sends to an unreachable member took %s", d)
	}
	// The link queues 64, and its sender holds one while it dials.
	if s := a.Stats(); s.Dropped.QueueFull < 10000-65 {
		t.Errorf("%d of 10,000 dropped; want all but the queue's", s.Dropped.QueueFull)
	}
}

func TestAClosedTransportDropsWhatItIsSent(t *testing.T) {
	c := newCluster(t)
	a, b := listen(t, c.Node("a")), listen(t, c.Node("b"))
	a.SendPacket(dest(b, "b"), []byte("before"))
	if _, ok := next(b, 2*time.Second); !ok {
		t.Fatal("b received nothing before a closed")
	}
	a.Close()
	select {
	case _, ok := <-a.Receive():
		if ok {
			t.Error("a's Receive delivered after Close")
		}
	case <-time.After(time.Second):
		t.Error("a's Receive is open after Close")
	}
	a.SendPacket(dest(b, "b"), []byte("after"))
	a.SendStream(dest(b, "b"), []byte("after"))
	if s := a.Stats(); s.Dropped.Closed != 2 || s.Links != 0 || s.Conns != 0 {
		t.Errorf("stats %+v; want two dropped, and no link or connection", s)
	}
	if m, ok := next(b, 500*time.Millisecond); ok {
		t.Fatalf("b received %+v from a closed transport", m)
	}
}

// A node that crashed and restarted at the same address with the same
// reset key is reached again at once; with another key, after the idle
// timeout.
func TestARestartedNodeIsReachedAgainAtOnceWithTheSameResetKey(t *testing.T) {
	for _, same := range []bool{true, false} {
		c := newCluster(t)
		a := listen(t, c.Node("a"))
		bid := c.Node("b")
		cfg := bid.Config("127.0.0.1:0")
		cfg.IdleTimeout = 5 * time.Second
		b, err := Listen(cfg)
		if err != nil {
			t.Fatal(err)
		}
		a.SendPacket(dest(b, "b"), []byte("before"))
		if _, ok := next(b, 2*time.Second); !ok {
			t.Fatal("b received nothing before its restart")
		}
		cfg.Addr = b.Addr()
		b.Crash()
		if !same {
			cfg.ResetKey[0]++
		}
		b2, err := Listen(cfg)
		if err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		for {
			a.SendPacket(dest(b2, "b"), []byte("after"))
			if _, ok := next(b2, 100*time.Millisecond); ok {
				break
			}
			if time.Since(start) > 10*time.Second {
				t.Fatal("never reached the restarted node")
			}
		}
		took := time.Since(start)
		t.Logf("same reset key %v: reached again after %s", same, took.Round(10*time.Millisecond))
		if same && took > time.Second {
			t.Errorf("with the same reset key it took %s", took)
		}
		if !same && took < 3*time.Second {
			t.Errorf("with another reset key it took %s, within the idle timeout of 5 s", took)
		}
		b2.Close()
	}
}

// A packet sent right after the largest sync arrives within 5 ms: a sync
// on its stream doesn't hold back the packets behind it.
func TestASyncDoesNotHoldBackThePacketsBehindIt(t *testing.T) {
	c := newCluster(t)
	a, b := listen(t, c.Node("a")), listen(t, c.Node("b"))
	a.SendPacket(dest(b, "b"), []byte("connect"))
	if _, ok := next(b, 2*time.Second); !ok {
		t.Fatal("no connection")
	}
	a.SendStream(dest(b, "b"), make([]byte, MaxStream))
	sent := time.Now()
	a.SendPacket(dest(b, "b"), []byte("probe"))
	for {
		m, ok := next(b, 5*time.Second)
		if !ok {
			t.Fatal("the probe never arrived")
		}
		if !m.Stream {
			t.Logf("the probe arrived after %s", time.Since(sent).Round(time.Millisecond))
			if d := time.Since(sent); d > 5*time.Millisecond {
				t.Fatalf("the probe waited %s behind the sync", d)
			}
			return
		}
	}
}

// A link to a member that went away ends after the idle timeout.
func TestLinksToAMemberThatIsGoneAreReaped(t *testing.T) {
	c := newCluster(t)
	cfg := c.Node("a").Config("127.0.0.1:0")
	cfg.IdleTimeout = 200 * time.Millisecond
	a, err := Listen(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	for i := range 20 {
		a.SendPacket(transport.Dest{Addr: fmt.Sprintf("127.0.0.1:%d", 30000+i), Peer: "gone"}, []byte("x"))
	}
	if n := a.Stats().Links; n != 20 {
		t.Fatalf("%d links, want 20", n)
	}
	deadline := time.Now().Add(5 * time.Second)
	for a.Stats().Links != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("%d links left", a.Stats().Links)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// An impostor at a member's address, with the member's name in a
// certificate from another CA and taking any client, isn't reached.
func TestAnImpostorWithTheMembersNameIsNotReached(t *testing.T) {
	a := listen(t, newCluster(t).Node("a"))
	fake := newCluster(t).Node("b")
	ln, err := quicgo.ListenAddr("127.0.0.1:0", &tls.Config{
		MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{fake.Cert},
		ClientAuth: tls.RequireAnyClientCert, NextProtos: []string{ALPN},
	}, &quicgo.Config{EnableDatagrams: true})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	got := make(chan []byte, 1)
	go func() {
		conn, err := ln.Accept(t.Context())
		if err != nil {
			return
		}
		if b, err := conn.ReceiveDatagram(t.Context()); err == nil {
			got <- b
		}
	}()
	a.SendPacket(transport.Dest{Addr: ln.Addr().String(), Peer: "b"}, []byte("for b"))
	select {
	case b := <-got:
		t.Fatalf("the impostor received %q", b)
	case <-time.After(time.Second):
	}
	if s := a.Stats(); s.DialFails != 1 {
		t.Errorf("stats %+v; want a failed dial", s)
	}
}

// dialAs dials tr as the node id, which it calls peer, offering alpn.
func dialAs(t *testing.T, tr *Transport, id ident, peer, alpn string) (*quicgo.Conn, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	cfg := id.Config("").Client(peer)
	cfg.NextProtos = []string{alpn}
	conn, err := quicgo.DialAddr(ctx, tr.Addr(), cfg, &quicgo.Config{EnableDatagrams: true})
	if err == nil {
		t.Cleanup(func() { conn.CloseWithError(0, "") })
	}
	return conn, err
}

func TestADialerWithAnotherALPNIsRefused(t *testing.T) {
	c := newCluster(t)
	a := listen(t, c.Node("a"))
	if _, err := dialAs(t, a, c.Node("b"), "a", "hivemind-membership/2"); err == nil {
		t.Fatal("a member offering another ALPN connected")
	}
	if _, err := dialAs(t, a, c.Node("b"), "a", "hivemind-membership/1"); err != nil {
		t.Fatalf("a member offering %s: %v", ALPN, err)
	}
}

// A member that writes a stream longer than the largest sync has it
// refused: the receiver reads no more than MaxStream and one byte.
func TestAStreamLongerThanTheLargestSyncIsRefused(t *testing.T) {
	c := newCluster(t)
	a := listen(t, c.Node("a"))
	conn, err := dialAs(t, a, c.Node("b"), "a", ALPN)
	if err != nil {
		t.Fatal(err)
	}
	s, err := conn.OpenUniStream()
	if err != nil {
		t.Fatal(err)
	}
	s.Write(make([]byte, MaxStream+1))
	s.Close()
	if m, ok := next(a, 2*time.Second); ok {
		t.Fatalf("a received %d bytes", len(m.Data))
	}
	if st := a.Stats(); st.Dropped.Oversize != 1 || st.StreamsIn != 0 {
		t.Errorf("stats %+v; want one oversize and no stream in", st)
	}
}

// fuzzPair is a pair of transports of one cluster, a sending to b.
func fuzzPair(f *testing.F) (a, b *Transport) {
	c := newCluster(f)
	open := func(id ident) *Transport {
		tr, err := Listen(id.Config("127.0.0.1:0"))
		if err != nil {
			f.Fatal(err)
		}
		f.Cleanup(func() { tr.Close() })
		return tr
	}
	return open(c.Node("a")), open(c.Node("b"))
}

// await waits for a message carrying data, skipping others, and says
// whether it came.
func await(tr *Transport, data []byte, stream bool, d time.Duration) bool {
	deadline := time.After(d)
	for {
		select {
		case m := <-tr.Receive():
			if m.Stream == stream && bytes.Equal(m.Data, data) {
				if m.Peer != "a" {
					panic("a message from a with Peer " + m.Peer)
				}
				return true
			}
		case <-deadline:
			return false
		}
	}
}

// Whatever a member sends as a packet arrives as it was sent, if a
// datagram holds it.
func FuzzDatagramReader(f *testing.F) {
	a, b := fuzzPair(f)
	for _, seed := range [][]byte{nil, []byte("ping"), make([]byte, 1150), make([]byte, 1500)} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		a.SendPacket(dest(b, "b"), data)
		if !await(b, data, false, time.Second) && len(data) <= 1150 {
			t.Fatalf("a packet of %d bytes didn't arrive", len(data))
		}
	})
}

// Whatever a member writes on a stream and ends arrives as it was
// written; a stream it resets doesn't arrive.
func FuzzStreamReader(f *testing.F) {
	c := newCluster(f)
	b, err := Listen(c.Node("b").Config("127.0.0.1:0"))
	if err != nil {
		f.Fatal(err)
	}
	f.Cleanup(func() { b.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cfg := c.Node("a").Config("").Client("b")
	cfg.NextProtos = []string{ALPN}
	conn, err := quicgo.DialAddr(ctx, b.Addr(), cfg, &quicgo.Config{EnableDatagrams: true})
	if err != nil {
		f.Fatal(err)
	}
	f.Cleanup(func() { conn.CloseWithError(0, "") })
	for _, seed := range [][]byte{nil, []byte("view"), make([]byte, 64<<10)} {
		f.Add(seed, false)
		f.Add(seed, true)
	}
	f.Fuzz(func(t *testing.T, data []byte, reset bool) {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()
		s, err := conn.OpenUniStreamSync(ctx)
		if err != nil {
			t.Fatal(err)
		}
		s.Write(data)
		if reset {
			s.CancelWrite(0)
			if await(b, data, true, 100*time.Millisecond) && len(data) > 0 {
				t.Fatalf("a reset stream of %d bytes arrived", len(data))
			}
			return
		}
		s.Close()
		if !await(b, data, true, 2*time.Second) {
			t.Fatalf("a stream of %d bytes didn't arrive", len(data))
		}
	})
}

// Syncs sent back to back, more than a peer may have open at once, all
// arrive: each waits for a stream.
func TestSyncsSentBackToBackAllArrive(t *testing.T) {
	c := newCluster(t)
	a, b := listen(t, c.Node("a")), listen(t, c.Node("b"))
	for i := range 10 {
		a.SendStream(dest(b, "b"), []byte{byte(i)})
	}
	got := map[byte]bool{}
	for len(got) < 10 {
		m, ok := next(b, 5*time.Second)
		if !ok {
			t.Fatalf("%d of 10 syncs arrived; a's stats %+v", len(got), a.Stats())
		}
		got[m.Data[0]] = true
	}
}

// rawMember listens as the member id over QUIC directly, taking dials as
// id would, and returns its address and the connections it accepts.
func rawMember(t *testing.T, id ident, qconf *quicgo.Config) (addr string, accepted <-chan *quicgo.Conn) {
	t.Helper()
	cfg := id.Config("")
	server := cfg.Server.Clone()
	server.NextProtos = []string{ALPN}
	ln, err := quicgo.ListenAddr("127.0.0.1:0", server, qconf)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	conns := make(chan *quicgo.Conn, 8)
	go func() {
		for {
			c, err := ln.Accept(context.Background())
			if err != nil {
				return
			}
			conns <- c
		}
	}()
	return ln.Addr().String(), conns
}

// openPartial opens a stream on c and writes part of a message on it.
func openPartial(t *testing.T, c *quicgo.Conn) *quicgo.SendStream {
	t.Helper()
	s, err := c.OpenUniStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write([]byte("partial")); err != nil {
		t.Fatal(err)
	}
	return s
}

// A peer may have 4 streams read at once on all its connections to a
// node: a fifth, on another connection, waits until one of them ends.
func TestAPeerMayHaveFourStreamsReadAtOnce(t *testing.T) {
	c := newCluster(t)
	a := listen(t, c.Node("a"))
	bid := c.Node("b")
	addr, accepted := rawMember(t, bid, &quicgo.Config{EnableDatagrams: true})
	a.SendPacket(transport.Dest{Addr: addr, Peer: "b"}, []byte("dial"))
	var dialled *quicgo.Conn
	select {
	case dialled = <-accepted:
	case <-time.After(2 * time.Second):
		t.Fatal("a didn't dial b")
	}
	var open []*quicgo.SendStream
	for range 4 {
		open = append(open, openPartial(t, dialled))
	}
	if _, err := dialled.OpenUniStream(); err == nil {
		t.Fatal("a fifth stream opened on one connection")
	}
	dialling, err := dialAs(t, a, bid, "a", ALPN)
	if err != nil {
		t.Fatal(err)
	}
	fifth, err := dialling.OpenUniStream()
	if err != nil {
		t.Fatal(err)
	}
	fifth.Write([]byte("fifth"))
	fifth.Close()
	if m, ok := next(a, time.Second); ok {
		t.Fatalf("a read %q while b had four streams being read", m.Data)
	}
	open[0].Close()
	got := map[string]bool{}
	for len(got) < 2 {
		m, ok := next(a, 2*time.Second)
		if !ok {
			t.Fatalf("got %v; want the first stream's message and then the fifth", got)
		}
		got[string(m.Data)] = true
	}
	if !got["partial"] || !got["fifth"] {
		t.Fatalf("got %v; want the first stream's message and the fifth", got)
	}
}

func TestAPeerCanNotOpenABidirectionalStream(t *testing.T) {
	c := newCluster(t)
	a := listen(t, c.Node("a"))
	conn, err := dialAs(t, a, c.Node("b"), "a", ALPN)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.OpenStream(); err == nil {
		t.Fatal("a peer opened a bidirectional stream")
	}
}

// However often a member dials, a node keeps one connection it accepted
// from it, and no link.
func TestANodeKeepsOneConnectionAMemberDialled(t *testing.T) {
	c := newCluster(t)
	a := listen(t, c.Node("a"))
	bid := c.Node("b")
	var last *quicgo.Conn
	for range 20 {
		conn, err := dialAs(t, a, bid, "a", ALPN)
		if err != nil {
			t.Fatal(err)
		}
		last = conn
	}
	deadline := time.Now().Add(2 * time.Second)
	for s := a.Stats(); s.Conns != 1 || s.Accepts != 20; s = a.Stats() {
		if time.Now().After(deadline) {
			t.Fatalf("stats %+v; want 20 accepted and one connection open", s)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if s := a.Stats(); s.Links != 0 {
		t.Errorf("stats %+v; want no link", s)
	}
	last.SendDatagram([]byte("latest"))
	if m, ok := next(a, 2*time.Second); !ok || string(m.Data) != "latest" {
		t.Fatalf("got %+v, %v; want the latest connection's packet", m, ok)
	}
}

// A Dest naming the member in another spelling that TLS's name check
// takes, another case or a trailing dot, doesn't reach it.
func TestAnotherSpellingOfTheMemberIsNotReached(t *testing.T) {
	c := newCluster(t)
	a, b := listen(t, c.Node("a")), listen(t, c.Node("b"))
	for _, peer := range []string{"B", "b."} {
		a.SendPacket(dest(b, peer), []byte(peer))
		if m, ok := next(b, time.Second); ok {
			t.Fatalf("a message for %q reached b: %+v", peer, m)
		}
	}
	if s := a.Stats(); s.DialFails != 2 {
		t.Errorf("stats %+v; want two failed dials", s)
	}
}

// A certificate of the cluster that PeerID names no member is refused,
// dialling and dialled.
func TestACertificateThatNamesNoMemberIsRefused(t *testing.T) {
	c := newCluster(t)
	cfg := c.Node("a").Config("127.0.0.1:0")
	cfg.PeerID = func(cert *x509.Certificate) (string, error) {
		if cert.Subject.CommonName == "b" {
			return "", errors.New("no member")
		}
		return cert.Subject.CommonName, nil
	}
	a, err := Listen(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b := listen(t, c.Node("b"))
	b.SendPacket(dest(a, "a"), []byte("in"))
	if m, ok := next(a, time.Second); ok {
		t.Fatalf("a accepted b and received %+v", m)
	}
	a.SendPacket(dest(b, "b"), []byte("out"))
	if m, ok := next(b, time.Second); ok {
		t.Fatalf("a dialled b and b received %+v", m)
	}
	if s := a.Stats(); s.Accepts != 0 || s.AcceptFails == 0 || s.DialFails == 0 {
		t.Errorf("stats %+v; want failed accepts and a failed dial", s)
	}
}

// A handshake Server refuses counts as a failed accept: from another
// cluster, with no certificate, or from a removed identity.
func TestRefusedHandshakesAreCounted(t *testing.T) {
	c := newCluster(t)
	a := listen(t, c.Node("a"))
	dialRaw(t, a, c.Pool, []tls.Certificate{newCluster(t).Node("s").Cert})
	dialRaw(t, a, c.Pool, nil)
	bid := c.Node("b")
	c.Remove(bid)
	dialRaw(t, a, c.Pool, []tls.Certificate{bid.Cert})
	deadline := time.Now().Add(2 * time.Second)
	for a.Stats().AcceptFails != 3 {
		if time.Now().After(deadline) {
			t.Fatalf("stats %+v; want three failed accepts", a.Stats())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if s := a.Stats(); s.Accepts != 0 {
		t.Errorf("stats %+v; want no accept", s)
	}
}

// A member removed after a handshake can't resume that session.
func TestARemovedMemberCanNotResumeItsSession(t *testing.T) {
	c := newCluster(t)
	a := listen(t, c.Node("a"))
	bid := c.Node("b")
	cache := tls.NewLRUClientSessionCache(1)
	dial := func() (*quicgo.Conn, error) {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()
		cfg := bid.Config("").Client("a")
		cfg.NextProtos = []string{ALPN}
		cfg.ClientSessionCache = cache
		return quicgo.DialAddr(ctx, a.Addr(), cfg, &quicgo.Config{EnableDatagrams: true})
	}
	for i := range 2 {
		conn, err := dial()
		if err != nil {
			t.Fatal(err)
		}
		if i == 1 && !conn.ConnectionState().TLS.DidResume {
			t.Fatal("the second dial didn't resume the first's session")
		}
		conn.SendDatagram([]byte("before"))
		if _, ok := next(a, 2*time.Second); !ok {
			t.Fatal("a received nothing before the removal")
		}
		// The session ticket comes after the handshake.
		time.Sleep(100 * time.Millisecond)
		conn.CloseWithError(0, "")
	}
	c.Remove(bid)
	a.CutOff(c.IsRemoved)
	conn, err := dial()
	if err != nil {
		return
	}
	defer conn.CloseWithError(0, "")
	conn.SendDatagram([]byte("after"))
	if m, ok := next(a, time.Second); ok {
		t.Fatalf("a received %q from b, removed, resuming its session", m.Data)
	}
}

// With a Server that refuses no removed identity, a member cut off can't
// connect again.
func TestACutOffMemberCanNotConnectAgain(t *testing.T) {
	c := newCluster(t)
	cfg := c.Node("a").Config("127.0.0.1:0")
	cfg.Server.VerifyConnection = nil
	a, err := Listen(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	bid := c.Node("b")
	c.Remove(bid)
	a.CutOff(c.IsRemoved)
	b := listen(t, bid)
	b.SendPacket(dest(a, "a"), []byte("after"))
	if m, ok := next(a, time.Second); ok {
		t.Fatalf("a received %q from b, cut off", m.Data)
	}
	if s := a.Stats(); s.Accepts != 0 || s.AcceptFails == 0 {
		t.Errorf("stats %+v; want a failed accept", s)
	}
}

// blockingPeerID returns a PeerID that blocks the first time it names b,
// until release closes, and says on reached that it has.
func blockingPeerID() (peerID func(*x509.Certificate) (string, error), reached, release chan struct{}) {
	reached, release = make(chan struct{}), make(chan struct{})
	var once sync.Once
	return func(cert *x509.Certificate) (string, error) {
		if cert.Subject.CommonName == "b" {
			once.Do(func() { close(reached); <-release })
		}
		return cert.Subject.CommonName, nil
	}, reached, release
}

// A connection whose handshake ended before CutOff, but which the
// transport takes on only after it, is cut off too: accepted or dialled.
func TestCutOffReachesAConnectionBeingTakenOn(t *testing.T) {
	for _, side := range []string{"accepted", "dialled"} {
		t.Run(side, func(t *testing.T) {
			c := newCluster(t)
			bid := c.Node("b")
			cfg := c.Node("a").Config("127.0.0.1:0")
			var reached, release chan struct{}
			cfg.PeerID, reached, release = blockingPeerID()
			a, err := Listen(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer a.Close()
			b := listen(t, bid)
			if side == "accepted" {
				b.SendPacket(dest(a, "a"), []byte("before"))
			} else {
				a.SendPacket(dest(b, "b"), []byte("before"))
			}
			<-reached
			c.Remove(bid)
			a.CutOff(c.IsRemoved)
			close(release)
			for range 3 {
				b.SendPacket(dest(a, "a"), []byte("after"))
			}
			deadline := time.After(time.Second)
			for {
				select {
				case m := <-a.Receive():
					t.Fatalf("a received %q from b, cut off", m.Data)
				case <-deadline:
					if s := a.Stats(); s.Conns != 0 {
						t.Fatalf("stats %+v; want no connection", s)
					}
					return
				}
			}
		})
	}
}

func TestListenRefusesAnIncompleteConfig(t *testing.T) {
	full := newCluster(t).Node("a").Config("127.0.0.1:0")
	for name, change := range map[string]func(*Config){
		"no Server":   func(c *Config) { c.Server = nil },
		"no Client":   func(c *Config) { c.Client = nil },
		"no PeerID":   func(c *Config) { c.PeerID = nil },
		"no ResetKey": func(c *Config) { c.ResetKey = [32]byte{} },
		"a bad Addr":  func(c *Config) { c.Addr = "nowhere" },
		"no host":     func(c *Config) { c.Addr = ":0" },
		"IPv4's any":  func(c *Config) { c.Addr = "0.0.0.0:0" },
		"IPv6's any":  func(c *Config) { c.Addr = "[::]:0" },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := full
			change(&cfg)
			if tr, err := Listen(cfg); err == nil {
				tr.Close()
				t.Fatal("Listen took it")
			}
		})
	}
}

// A packet and a sync sent just before Close arrive, every time.
func TestWhatIsSentBeforeCloseArrives(t *testing.T) {
	c := newCluster(t)
	b := listen(t, c.Node("b"))
	for i := range 20 {
		a := listen(t, c.Node("a"))
		if i%2 == 0 {
			// Half the runs connect first.
			a.SendPacket(dest(b, "b"), []byte("hello"))
			if _, ok := next(b, 2*time.Second); !ok {
				t.Fatal("no connection")
			}
		}
		a.SendPacket(dest(b, "b"), []byte("bye"))
		a.SendStream(dest(b, "b"), []byte("view"))
		a.Close()
		var packet, stream bool
		for !packet || !stream {
			m, ok := next(b, 2*time.Second)
			if !ok {
				t.Fatalf("run %d: packet %v, sync %v arrived; a's stats %+v", i, packet, stream, a.Stats())
			}
			packet = packet || string(m.Data) == "bye"
			stream = stream || string(m.Data) == "view"
		}
	}
}

// A member that allows no stream holds no more than a link's queue of 64
// and the syncs being written, and Close ends within its bound with
// syncs still waiting.
func TestSyncsWaitingForAMemberAreBounded(t *testing.T) {
	const maxWritten = 4
	c := newCluster(t)
	a := listen(t, c.Node("a"))
	addr, _ := rawMember(t, c.Node("b"), &quicgo.Config{EnableDatagrams: true, MaxIncomingUniStreams: -1})
	to := transport.Dest{Addr: addr, Peer: "b"}
	before := goruntime.NumGoroutine()
	for range 20 {
		for range 32 {
			a.SendStream(to, []byte("view"))
		}
		time.Sleep(10 * time.Millisecond)
	}
	if n := goruntime.NumGoroutine() - before; n > 20 {
		t.Errorf("%d goroutines more for one member's syncs", n)
	}
	if s := a.Stats(); s.Dropped.QueueFull < 640-64-maxWritten {
		t.Errorf("stats %+v; want all but the queue's syncs and those being written dropped", s)
	}
	start := time.Now()
	a.Close()
	if d := time.Since(start); d > 3*time.Second {
		t.Errorf("Close took %s", d)
	}
}

// A dial the other end refuses once it is made, as a TLS 1.3 server
// refuses a client, counts as a failed dial at one end and a failed
// accept at the other.
func TestARefusedDialIsCountedAtBothEnds(t *testing.T) {
	c := newCluster(t)
	a := listen(t, c.Node("a"))
	bid := c.Node("b")
	c.Remove(bid)
	b := listen(t, bid)
	b.SendPacket(dest(a, "a"), []byte("let me in"))
	deadline := time.Now().Add(2 * time.Second)
	for b.Stats().DialFails == 0 || a.Stats().AcceptFails == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("b's stats %+v, a's %+v; want a failed dial and a failed accept", b.Stats(), a.Stats())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A stream a peer stalls on is given up after the read timeout, and the
// peer may open another.
func TestAStalledStreamIsGivenUp(t *testing.T) {
	defer SetStreamReadTimeout(200 * time.Millisecond)()
	c := newCluster(t)
	a := listen(t, c.Node("a"))
	conn, err := dialAs(t, a, c.Node("b"), "a", ALPN)
	if err != nil {
		t.Fatal(err)
	}
	for range 4 {
		openPartial(t, conn)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	s, err := conn.OpenUniStreamSync(ctx)
	if err != nil {
		t.Fatalf("no fifth stream once the stalled ones were given up: %v", err)
	}
	s.Write([]byte("fifth"))
	s.Close()
	if m, ok := next(a, 2*time.Second); !ok || string(m.Data) != "fifth" {
		t.Fatalf("got %+v, %v; want the fifth stream", m, ok)
	}
	if st := a.Stats(); st.Dropped.ReadFails != 4 {
		t.Errorf("stats %+v; want four failed reads", st)
	}
}

// What arrives while Receive is full is dropped and counted.
func TestAFullReceiveDrops(t *testing.T) {
	c := newCluster(t)
	a, b := listen(t, c.Node("a")), listen(t, c.Node("b"))
	for i := range 2000 {
		a.SendPacket(dest(b, "b"), []byte("x"))
		if i%50 == 0 {
			time.Sleep(time.Millisecond)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for s := b.Stats(); s.Dropped.ReceiveFull == 0 || s.PacketsIn != s.Dropped.ReceiveFull+uint64(len(b.Receive())); s = b.Stats() {
		if time.Now().After(deadline) {
			t.Fatalf("stats %+v with %d waiting; want what didn't fit dropped", s, len(b.Receive()))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Sends that race the reaping of their link are each sent or counted:
// with an idle timeout of a nanosecond, a link is ready to be reaped
// whenever it has a message to send.
func TestSendsRacingTheReapingOfTheirLinkAreCounted(t *testing.T) {
	c := newCluster(t)
	cfg := c.Node("a").Config("127.0.0.1:0")
	cfg.IdleTimeout = time.Nanosecond
	a, err := Listen(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	const senders, each = 8, 5000
	var wg sync.WaitGroup
	for range senders {
		wg.Go(func() {
			for i := range each {
				a.SendPacket(transport.Dest{Addr: "127.0.0.1:99999", Peer: "x"}, []byte{byte(i)})
				if i%100 == 0 {
					time.Sleep(time.Millisecond)
				}
			}
		})
	}
	wg.Wait()
	deadline := time.Now().Add(5 * time.Second)
	for s := a.Stats(); s.Dropped.Unsent+s.Dropped.QueueFull != senders*each || s.Links != 0; s = a.Stats() {
		if time.Now().After(deadline) {
			t.Fatalf("stats %+v; want all %d counted and no link", s, senders*each)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestTheIdleTimeoutIsFiveMinutesByDefault(t *testing.T) {
	a := listen(t, newCluster(t).Node("a"))
	if d := a.IdleTimeout(); d != 5*time.Minute {
		t.Errorf("the idle timeout is %s", d)
	}
}

// A Dest's address is an IP address: a host name isn't looked up.
func TestAHostNameIsNotLookedUp(t *testing.T) {
	c := newCluster(t)
	a, b := listen(t, c.Node("a")), listen(t, c.Node("b"))
	_, port, _ := net.SplitHostPort(b.Addr())
	a.SendPacket(transport.Dest{Addr: net.JoinHostPort("localhost", port), Peer: "b"}, []byte("by name"))
	if m, ok := next(b, time.Second); ok {
		t.Fatalf("b received %+v", m)
	}
	if s := a.Stats(); s.DialFails != 1 || s.Dials != 0 {
		t.Errorf("stats %+v; want a failed dial before any dial", s)
	}
}

// A sync the other end doesn't take within the sync timeout is given up
// and counted.
func TestASyncNotTakenInTimeIsGivenUp(t *testing.T) {
	defer SetSyncTimeout(200 * time.Millisecond)()
	c := newCluster(t)
	a := listen(t, c.Node("a"))
	addr, accepted := rawMember(t, c.Node("b"), &quicgo.Config{EnableDatagrams: true})
	a.SendStream(transport.Dest{Addr: addr, Peer: "b"}, make([]byte, MaxStream))
	select {
	case <-accepted:
	case <-time.After(2 * time.Second):
		t.Fatal("a didn't dial b")
	}
	deadline := time.Now().Add(2 * time.Second)
	for a.Stats().Dropped.Unsent != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("stats %+v; want the sync given up", a.Stats())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if s := a.Stats(); s.StreamsOut != 0 {
		t.Errorf("stats %+v; want no stream sent", s)
	}
}

// A transport that closes tells the other ends of its connections, which
// drop them at once.
func TestCloseTellsTheOtherEnds(t *testing.T) {
	c := newCluster(t)
	a, b := listen(t, c.Node("a")), listen(t, c.Node("b"))
	a.SendPacket(dest(b, "b"), []byte("hello"))
	if _, ok := next(b, 2*time.Second); !ok {
		t.Fatal("no connection")
	}
	a.Close()
	deadline := time.Now().Add(time.Second)
	for b.Stats().Conns != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("b still has %d connections after a closed", b.Stats().Conns)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// An honest member's large syncs, sent in bursts, all arrive: each new
// stream the receiver allows is read.
func TestBurstsOfLargeSyncsAllArrive(t *testing.T) {
	for range 3 {
		c := newCluster(t)
		a, b := listen(t, c.Node("a")), listen(t, c.Node("b"))
		for i := range 40 {
			a.SendStream(dest(b, "b"), make([]byte, 256<<10))
			if i%8 == 7 {
				time.Sleep(5 * time.Millisecond)
			}
		}
		for i := range 40 {
			if _, ok := next(b, 5*time.Second); !ok {
				t.Fatalf("%d of 40 syncs arrived; a's stats %+v, b's %+v", i, a.Stats(), b.Stats())
			}
		}
	}
}

// A connection whose dialler closes it while the transport names its
// member counts once, as an accept or a failed one.
func TestAConnectionEndingWhileTakenOnCountsOnce(t *testing.T) {
	c := newCluster(t)
	cfg := c.Node("a").Config("127.0.0.1:0")
	var reached, release chan struct{}
	cfg.PeerID, reached, release = blockingPeerID()
	a, err := Listen(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	conn, err := dialAs(t, a, c.Node("b"), "a", ALPN)
	if err != nil {
		t.Fatal(err)
	}
	<-reached
	conn.CloseWithError(0, "bye")
	time.Sleep(200 * time.Millisecond)
	close(release)
	deadline := time.Now().Add(2 * time.Second)
	for s := a.Stats(); s.Accepts+s.AcceptFails == 0; s = a.Stats() {
		if time.Now().After(deadline) {
			t.Fatalf("stats %+v; want the connection counted", s)
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)
	if s := a.Stats(); s.Accepts+s.AcceptFails != 1 {
		t.Errorf("stats %+v; want one connection counted once", s)
	}
}

// A dial the other end's transport refuses once it is made, naming no
// member or cut off, counts as a failed dial.
func TestADialTheOtherTransportRefusesIsAFailedDial(t *testing.T) {
	for _, how := range []string{"names no member", "cut off"} {
		t.Run(how, func(t *testing.T) {
			c := newCluster(t)
			cfg := c.Node("a").Config("127.0.0.1:0")
			cfg.Server.VerifyConnection = nil
			if how == "names no member" {
				cfg.PeerID = func(cert *x509.Certificate) (string, error) {
					if cert.Subject.CommonName == "b" {
						return "", errors.New("no member")
					}
					return cert.Subject.CommonName, nil
				}
			}
			a, err := Listen(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer a.Close()
			if how == "cut off" {
				a.CutOff(func(cert *x509.Certificate) bool { return cert.Subject.CommonName == "b" })
			}
			b := listen(t, c.Node("b"))
			b.SendPacket(dest(a, "a"), []byte("in"))
			deadline := time.Now().Add(2 * time.Second)
			for b.Stats().DialFails != 1 {
				if time.Now().After(deadline) {
					t.Fatalf("b's stats %+v; want a failed dial", b.Stats())
				}
				time.Sleep(10 * time.Millisecond)
			}
		})
	}
}

// Once Close's drain runs out of time, what is still queued for a member
// that doesn't answer is dropped without dialling.
func TestCloseDialsNoMoreOnceItsDrainEnds(t *testing.T) {
	c := newCluster(t)
	a, err := Listen(c.Node("a").Config("127.0.0.1:0"))
	if err != nil {
		t.Fatal(err)
	}
	hole, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer hole.Close()
	to := transport.Dest{Addr: hole.LocalAddr().String(), Peer: "x"}
	for range 64 {
		a.SendPacket(to, []byte("x"))
	}
	a.Close()
	if s := a.Stats(); s.Dials > 2 || s.Dropped.Closed+s.Dropped.Unsent != 64 {
		t.Errorf("stats %+v; want at most the dials the drain made, and every message dropped", s)
	}
}

// A stream waiting for a slot when its connection ends is given up at
// once and counted as a failed read.
func TestAStreamWaitingWhenItsConnectionEndsIsCounted(t *testing.T) {
	c := newCluster(t)
	a := listen(t, c.Node("a"))
	bid := c.Node("b")
	addr, accepted := rawMember(t, bid, &quicgo.Config{EnableDatagrams: true})
	a.SendPacket(transport.Dest{Addr: addr, Peer: "b"}, []byte("dial"))
	var dialled *quicgo.Conn
	select {
	case dialled = <-accepted:
	case <-time.After(2 * time.Second):
		t.Fatal("a didn't dial b")
	}
	for range 4 {
		openPartial(t, dialled)
	}
	dialling, err := dialAs(t, a, bid, "a", ALPN)
	if err != nil {
		t.Fatal(err)
	}
	s, err := dialling.OpenUniStream()
	if err != nil {
		t.Fatal(err)
	}
	s.Write([]byte("fifth"))
	s.Close()
	time.Sleep(100 * time.Millisecond)
	dialling.CloseWithError(0, "bye")
	deadline := time.Now().Add(time.Second)
	for a.Stats().Dropped.ReadFails != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("stats %+v; want the waiting stream counted as a failed read", a.Stats())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A connection that ends by the other end's Close or CutOff is neither a
// failed dial nor a failed accept.
func TestAConnectionEndedByTheOtherEndIsNoFailure(t *testing.T) {
	for _, how := range []string{"Close", "CutOff"} {
		t.Run(how, func(t *testing.T) {
			c := newCluster(t)
			a, b := listen(t, c.Node("a")), listen(t, c.Node("b"))
			a.SendPacket(dest(b, "b"), []byte("hi"))
			if _, ok := next(b, 2*time.Second); !ok {
				t.Fatal("no connection")
			}
			if how == "Close" {
				b.Close()
			} else {
				b.CutOff(func(cert *x509.Certificate) bool { return cert.Subject.CommonName == "a" })
			}
			deadline := time.Now().Add(2 * time.Second)
			for a.Stats().Conns != 0 {
				if time.Now().After(deadline) {
					t.Fatal("a kept its connection")
				}
				time.Sleep(10 * time.Millisecond)
			}
			time.Sleep(100 * time.Millisecond)
			if s := a.Stats(); s.DialFails != 0 {
				t.Errorf("a's stats %+v; want no failed dial", s)
			}
			if s := b.Stats(); s.Accepts != 1 || s.AcceptFails != 0 {
				t.Errorf("b's stats %+v; want one accept and no failed one", s)
			}
		})
	}
}

// A member's stream slots are forgotten once its connections end.
func TestStreamSlotsAreForgottenWithTheirMember(t *testing.T) {
	c := newCluster(t)
	a := listen(t, c.Node("a"))
	bid := c.Node("b")
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 5 {
				conn, err := dialAs(t, a, bid, "a", ALPN)
				if err != nil {
					continue
				}
				for range 3 {
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					s, err := conn.OpenUniStreamSync(ctx)
					cancel()
					if err != nil {
						break
					}
					s.Write([]byte("x"))
					s.Close()
				}
				conn.CloseWithError(0, "")
			}
		})
	}
	wg.Wait()
	deadline := time.Now().Add(3 * time.Second)
	for a.StreamSlots() != 0 || a.Stats().Conns != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("slots kept for %d members; stats %+v", a.StreamSlots(), a.Stats())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A dial whose handshake ends once Close's drain has run out is no failed
// dial: the message is dropped as closed.
func TestADialCloseCutsShortIsNoFailure(t *testing.T) {
	c := newCluster(t)
	cfg := c.Node("a").Config("127.0.0.1:0")
	var reached, release chan struct{}
	cfg.PeerID, reached, release = blockingPeerID()
	a, err := Listen(cfg)
	if err != nil {
		t.Fatal(err)
	}
	b := listen(t, c.Node("b"))
	a.SendPacket(dest(b, "b"), []byte("x"))
	<-reached
	closed := make(chan struct{})
	go func() {
		a.Close()
		close(closed)
	}()
	time.Sleep(2500 * time.Millisecond)
	close(release)
	<-closed
	if s := a.Stats(); s.DialFails != 0 || s.Dropped.Closed != 1 {
		t.Errorf("stats %+v; want no failed dial and the message dropped as closed", s)
	}
}

// What is sent is what arrives, though the caller reuses its buffer at
// once.
func TestASentBufferCanBeReusedAtOnce(t *testing.T) {
	c := newCluster(t)
	a, b := listen(t, c.Node("a")), listen(t, c.Node("b"))
	for _, stream := range []bool{false, true} {
		buf := []byte("first")
		if stream {
			a.SendStream(dest(b, "b"), buf)
		} else {
			a.SendPacket(dest(b, "b"), buf)
		}
		copy(buf, "other")
		m, ok := next(b, 2*time.Second)
		if !ok || string(m.Data) != "first" {
			t.Fatalf("stream %v: got %q, %v; want what was sent, \"first\"", stream, m.Data, ok)
		}
	}
}

// The QUIC transport keeps the port's promises.
func TestThePortContract(t *testing.T) {
	transporttest.Run(t, func(t *testing.T) transporttest.Pair {
		c := newCluster(t)
		return transporttest.Pair{A: listen(t, c.Node("a")), B: listen(t, c.Node("b"))}
	})
}
