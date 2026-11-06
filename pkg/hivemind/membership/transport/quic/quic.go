// Package quic is membership's transport over QUIC: one UDP socket that
// listens and dials, TLS 1.3 with certificates both ways, packets as
// datagrams and each stream message on a unidirectional stream of its own.
//
// It proves who sends and pins who receives, as transport.Transport asks:
// every message carries the member ID of the certificate it came under,
// and a message goes only to an other end whose certificate is the member
// it is meant for. A message with no member to pin is dropped.
package quic

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	quicgo "github.com/quic-go/quic-go"

	"github.com/moostackhq/moostack/pkg/hivemind/membership/transport"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/wire"
)

// ALPN names the transport's framing in the handshake: a datagram is one
// packet, a stream one message. Messages carry their wire versions themselves.
const ALPN = "hivemind-membership/1"

// MaxStream is the most bytes a stream message has: the largest sync.
const MaxStream = wire.MaxSyncBytes

// maxStreams is how many streams a peer may have open to a node at once,
// and how many syncs a node writes to one member at one address at once.
const maxStreams = 4

// The codes the transport closes connections with: Close's, CutOff's, a
// connection's it refuses once its handshake is done, and a connection's
// a newer one from its member replaces.
const (
	closeClosing quicgo.ApplicationErrorCode = iota
	closeCutOff
	closeRefused
	closeReplaced
)

// drainTimeout bounds how long Close sends what was handed to it before.
const drainTimeout = 2 * time.Second

// handshakeTimeout is how long a handshake goes without hearing from the
// peer before it fails.
const handshakeTimeout = 5 * time.Second

// streamReadTimeout bounds how long a stream from a peer is read, and
// syncTimeout how long a sync waits for a stream and is written.
var (
	streamReadTimeout = 10 * time.Second
	syncTimeout       = 5 * time.Second
)

// DefaultIdleTimeout is the idle timeout of a Config that sets none.
const DefaultIdleTimeout = 5 * time.Minute

// Config is what the transport needs from the node it serves.
type Config struct {
	// Addr is the UDP address it listens and dials from: an address
	// other than a wildcard, and a port; a host name is resolved once by
	// Listen.
	Addr string
	// Server presents the node's certificate and verifies a dialling
	// member's, refusing removed ones. Client returns the configuration
	// that dials member peer: it verifies the other end is that member,
	// and refuses removed ones. The transport adds its ALPN to both.
	Server *tls.Config
	Client func(peer string) *tls.Config
	// PeerID names the member a verified certificate is.
	PeerID func(*x509.Certificate) (string, error)
	// ResetKey keys the transport's stateless resets: a process that
	// restarts at the same address with the same key ends its peers'
	// connections to the run before at once. It must be the same across
	// runs, secret to the node, and not zero.
	ResetKey [32]byte
	// IdleTimeout closes a connection with no traffic for this long, and
	// ends a link with no connection and nothing to send for this long.
	// 0 is DefaultIdleTimeout.
	IdleTimeout time.Duration
}

// Stats count what the transport did since it opened. Conns is how many
// connections are open now, and Links how many members at an address it
// keeps a sender for. DialFails counts the sends that found no connection
// to their member: a Dest whose Addr is not an IP address and port, a
// dial that failed, reached another member or was refused by CutOff, and
// a dial the other end refused once it was made. AcceptFails counts the
// connections dialled to it that it never took on, refused by Server or
// by the transport.
type Stats struct {
	Conns, Links          int
	Dials, DialFails      uint64
	Accepts, AcceptFails  uint64
	PacketsOut, PacketsIn uint64
	StreamsOut, StreamsIn uint64
	Dropped               Drops
}

// Drops count the messages the transport dropped, by reason.
type Drops struct {
	// Unpinned were sent to a Dest with no Peer, and Oversize were larger
	// than a datagram or MaxStream, sent or received.
	Unpinned, Oversize uint64
	// QueueFull found the link's queue, or its queue of syncs, full.
	// Unsent had no connection to go on, or failed to be written. Closed were sent after Close, or were still waiting
	// for a connection when Close's drain ran out of time; a sync Close
	// cuts short is Unsent.
	QueueFull, Unsent, Closed uint64
	// ReceiveFull arrived while the Receive channel was full, and
	// ReadFails are streams that failed or timed out before they ended.
	ReceiveFull, ReadFails uint64
}

// Transport is one node's endpoint.
type Transport struct {
	cfg    Config
	server *tls.Config
	qconf  *quicgo.Config
	udp    *net.UDPConn
	tr     *quicgo.Transport
	ln     *quicgo.Listener
	addr   string
	in     chan transport.Message
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	// senders counts the links' senders and sync writers,
	// which Close waits for; draining closes when Close begins.
	senders  sync.WaitGroup
	draining chan struct{}
	once     sync.Once
	n        counters

	mu    sync.Mutex
	links map[transport.Dest]*link
	// open is every connection being read; conns the latest to each
	// member at an address, which sends go on.
	open    map[*quicgo.Conn]opened
	conns   map[transport.Dest]*quicgo.Conn
	cut     []func(*x509.Certificate) bool
	streams map[string]*slots
	closing bool
}

// slots are the streams a member may have read at once, on all its
// connections, and how many readers use them.
type slots struct {
	ch      chan struct{}
	readers int
}

// link is what waits to go to one member at one address: the messages,
// and the syncs to be written on the connection each was taken to.
type link struct {
	to      transport.Dest
	out     chan outgoing
	syncs   chan syncing
	writers int // guarded by Transport.mu
}

// syncing is a sync waiting for a writer, and the connection it goes on.
type syncing struct {
	c *quicgo.Conn
	b []byte
}

// opened is a connection's member and address, and whether the member
// dialled it.
type opened struct {
	to       transport.Dest
	accepted bool
}

// handshake is the state of a connection dialled to the transport: taken
// once accept has named its member, and counted by accept from then on,
// or failed if it ends first, counted as a failed accept then.
type handshake struct{ state atomic.Int32 }

const (
	handshaking int32 = iota
	handshakeTaken
	handshakeFailed
)

type handshakeKey struct{}

// outgoing is a message waiting on a link.
type outgoing struct {
	b      []byte
	stream bool
}

// counters are what Stats reports.
type counters struct {
	dials, dialFails, accepts, acceptFails atomic.Uint64
	packetsOut, packetsIn                  atomic.Uint64
	streamsOut, streamsIn                  atomic.Uint64
	unpinned, oversize                     atomic.Uint64
	queueFull, unsent, closed              atomic.Uint64
	receiveFull, readFails                 atomic.Uint64
}

// Listen opens the transport on cfg.Addr.
func Listen(cfg Config) (*Transport, error) {
	if cfg.Server == nil || cfg.Client == nil || cfg.PeerID == nil || cfg.ResetKey == ([32]byte{}) {
		return nil, errors.New("quic: Server, Client, PeerID and ResetKey are required")
	}
	if cfg.IdleTimeout == 0 {
		cfg.IdleTimeout = DefaultIdleTimeout
	}
	ua, err := net.ResolveUDPAddr("udp", cfg.Addr)
	if err != nil {
		return nil, err
	}
	if ua.IP == nil || ua.IP.IsUnspecified() {
		return nil, fmt.Errorf("quic: Addr %q names no address other nodes reach the node at", cfg.Addr)
	}
	udp, err := net.ListenUDP("udp", ua)
	if err != nil {
		return nil, err
	}
	key := quicgo.StatelessResetKey(cfg.ResetKey)
	server := cfg.Server.Clone()
	server.NextProtos = []string{ALPN}
	t := &Transport{
		cfg:    cfg,
		server: server,
		qconf: &quicgo.Config{
			EnableDatagrams: true,
			// Packets stay at QUIC's smallest size.
			InitialPacketSize:       1200,
			DisablePathMTUDiscovery: true,
			HandshakeIdleTimeout:    handshakeTimeout,
			MaxIdleTimeout:          cfg.IdleTimeout,
			// A peer may have at most maxStreams unidirectional streams
			// open on one connection, and no bidirectional stream.
			MaxIncomingUniStreams: maxStreams,
			MaxIncomingStreams:    -1,
		},
		udp:      udp,
		tr:       &quicgo.Transport{Conn: udp, StatelessResetKey: &key},
		addr:     udp.LocalAddr().String(),
		in:       make(chan transport.Message, 1024),
		draining: make(chan struct{}),
		links:    map[transport.Dest]*link{},
		open:     map[*quicgo.Conn]opened{},
		conns:    map[transport.Dest]*quicgo.Conn{},
		streams:  map[string]*slots{},
	}
	t.tr.ConnContext = t.connContext
	t.ctx, t.cancel = context.WithCancel(context.Background())
	if t.ln, err = t.tr.Listen(t.server, t.qconf); err != nil {
		udp.Close()
		return nil, err
	}
	t.wg.Go(t.accept)
	return t, nil
}

// Addr is the address the transport listens at.
func (t *Transport) Addr() string { return t.addr }

// Receive delivers what arrives, until Close.
func (t *Transport) Receive() <-chan transport.Message { return t.in }

// SendPacket queues a copy of b as a datagram to the member to names.
func (t *Transport) SendPacket(to transport.Dest, b []byte) { t.send(to, outgoing{b: b}) }

// SendStream queues a copy of b as a stream message to the member to
// names.
func (t *Transport) SendStream(to transport.Dest, b []byte) {
	t.send(to, outgoing{b: b, stream: true})
}

func (t *Transport) send(to transport.Dest, o outgoing) {
	if to.Peer == "" {
		t.n.unpinned.Add(1)
		return
	}
	if o.stream && len(o.b) > MaxStream {
		t.n.oversize.Add(1)
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closing {
		t.n.closed.Add(1)
		return
	}
	o.b = bytes.Clone(o.b)
	select {
	case t.linkLocked(to).out <- o:
	default:
		t.n.queueFull.Add(1)
	}
}

// CutOff closes every connection whose other end's certificate matches,
// as when its identity is removed, and refuses every connection whose
// certificate matches from then on. Each match is kept, and run under the
// transport's lock on every connection taken on: it mustn't call the
// transport.
func (t *Transport) CutOff(match func(*x509.Certificate) bool) {
	t.mu.Lock()
	t.cut = append(t.cut, match)
	var cut []*quicgo.Conn
	for c := range t.open {
		if match(c.ConnectionState().TLS.PeerCertificates[0]) {
			cut = append(cut, c)
		}
	}
	t.mu.Unlock()
	for _, c := range cut {
		c.CloseWithError(closeCutOff, "removed")
	}
}

// Stats returns the counts so far.
func (t *Transport) Stats() Stats {
	t.mu.Lock()
	conns, links := len(t.open), len(t.links)
	t.mu.Unlock()
	n := &t.n
	return Stats{
		Conns: conns, Links: links,
		Dials: n.dials.Load(), DialFails: n.dialFails.Load(),
		Accepts: n.accepts.Load(), AcceptFails: n.acceptFails.Load(),
		PacketsOut: n.packetsOut.Load(), PacketsIn: n.packetsIn.Load(),
		StreamsOut: n.streamsOut.Load(), StreamsIn: n.streamsIn.Load(),
		Dropped: Drops{
			Unpinned: n.unpinned.Load(), Oversize: n.oversize.Load(),
			QueueFull: n.queueFull.Load(), Unsent: n.unsent.Load(), Closed: n.closed.Load(),
			ReceiveFull: n.receiveFull.Load(), ReadFails: n.readFails.Load(),
		},
	}
}

// Close drops what is sent from then on, and sends what was sent before,
// for at most drainTimeout. Then it closes every connection, telling the
// other end, and the socket, and then the Receive channel.
func (t *Transport) Close() error {
	var err error
	t.once.Do(func() {
		deadline := time.Now().Add(drainTimeout)
		t.mu.Lock()
		t.closing = true
		close(t.draining)
		t.mu.Unlock()
		t.ln.Close()
		sent := make(chan struct{})
		go func() {
			t.senders.Wait()
			close(sent)
		}()
		select {
		case <-sent:
			t.linger(deadline)
		case <-time.After(time.Until(deadline)):
		}
		t.mu.Lock()
		t.cancel()
		var open []*quicgo.Conn
		for c := range t.open {
			open = append(open, c)
		}
		t.mu.Unlock()
		for _, c := range open {
			c.CloseWithError(closeClosing, "closing")
		}
		err = t.tr.Close()
		t.udp.Close()
		t.wg.Wait()
		close(t.in)
	})
	return err
}

// linger sleeps, until deadline at most, twice the longest smoothed RTT
// plus four mean deviations of the open connections, and at least 20 ms.
func (t *Transport) linger(deadline time.Time) {
	d := 20 * time.Millisecond
	t.mu.Lock()
	for c := range t.open {
		st := c.ConnectionStats()
		d = max(d, 2*(st.SmoothedRTT+4*st.MeanDeviation))
	}
	t.mu.Unlock()
	time.Sleep(min(d, time.Until(deadline)))
}

// linkLocked returns the link to to, making it and its sender if there
// is none. t.mu is held.
func (t *Transport) linkLocked(to transport.Dest) *link {
	l, ok := t.links[to]
	if !ok {
		l = &link{to: to, out: make(chan outgoing, 64), syncs: make(chan syncing, 64)}
		t.links[to] = l
		t.senders.Add(1)
		t.wg.Go(func() {
			defer t.senders.Done()
			t.sendLoop(l)
		})
	}
	return l
}

// sendLoop sends l's messages, one dial at a time: packets in order, and
// syncs through writeSyncs, which the packets don't wait for. A link
// with no connection and nothing to send for the idle timeout ends. Once
// Close begins, it sends what is queued and ends.
func (t *Transport) sendLoop(l *link) {
	idle := time.NewTimer(t.cfg.IdleTimeout)
	defer idle.Stop()
	for {
		select {
		case o := <-l.out:
			idle.Reset(t.cfg.IdleTimeout)
			t.sendOne(l, o)
		case <-idle.C:
			if t.reap(l) {
				return
			}
			idle.Reset(t.cfg.IdleTimeout)
		case <-t.draining:
			for {
				select {
				case o := <-l.out:
					t.sendOne(l, o)
				default:
					t.end(l)
					return
				}
			}
		}
	}
}

// sendOne sends packet o on l's connection, or queues sync o for
// writeSyncs on it, dialling one if there is none.
func (t *Transport) sendOne(l *link, o outgoing) {
	c := t.conn(l)
	if c == nil {
		if t.ctx.Err() != nil {
			t.n.closed.Add(1)
		} else {
			t.n.unsent.Add(1)
		}
		return
	}
	if o.stream {
		select {
		case l.syncs <- syncing{c, o.b}:
		default:
			t.n.queueFull.Add(1)
			return
		}
		t.mu.Lock()
		start := l.writers < maxStreams
		if start {
			l.writers++
		}
		t.mu.Unlock()
		if start {
			t.senders.Add(1)
			t.wg.Go(func() {
				defer t.senders.Done()
				t.writeSyncs(l)
			})
		}
		return
	}
	if err := c.SendDatagram(o.b); err != nil {
		if _, ok := errors.AsType[*quicgo.DatagramTooLargeError](err); ok {
			t.n.oversize.Add(1)
		} else {
			t.n.unsent.Add(1)
		}
		return
	}
	t.n.packetsOut.Add(1)
}

// writeSyncs writes l's queued syncs, one at a time, until there are
// none. Up to maxStreams run for a link.
func (t *Transport) writeSyncs(l *link) {
	for {
		select {
		case s := <-l.syncs:
			t.sendStream(s.c, s.b)
		default:
			t.mu.Lock()
			if len(l.syncs) == 0 {
				l.writers--
				t.mu.Unlock()
				return
			}
			t.mu.Unlock()
		}
	}
}

// end removes l once Close has begun.
func (t *Transport) end(l *link) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.links, l.to)
}

// reap removes l if it has no connection and nothing queued, and says so.
func (t *Transport) reap(l *link) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if c := t.conns[l.to]; (c != nil && c.Context().Err() == nil) || len(l.out) > 0 {
		return false
	}
	delete(t.links, l.to)
	return true
}

// sendStream writes b on a stream of its own, waiting for the peer to
// allow one, all within syncTimeout.
func (t *Transport) sendStream(c *quicgo.Conn, b []byte) {
	deadline := time.Now().Add(syncTimeout)
	ctx, cancel := context.WithDeadline(t.ctx, deadline)
	defer cancel()
	s, err := c.OpenUniStreamSync(ctx)
	if err != nil {
		t.n.unsent.Add(1)
		return
	}
	s.SetWriteDeadline(deadline)
	if _, err := s.Write(b); err != nil {
		s.CancelWrite(0)
		t.n.unsent.Add(1)
		return
	}
	s.Close()
	t.n.streamsOut.Add(1)
}

// conn returns l's live connection, dialling one if there is none. A dial
// verifies the other end is l's member twice: Client pins its name, and
// PeerID must name it from the certificate.
func (t *Transport) conn(l *link) *quicgo.Conn {
	t.mu.Lock()
	c := t.conns[l.to]
	t.mu.Unlock()
	if c != nil && c.Context().Err() == nil {
		return c
	}
	if t.ctx.Err() != nil {
		return nil
	}
	ap, err := netip.ParseAddrPort(l.to.Addr)
	if err != nil {
		t.n.dialFails.Add(1)
		return nil
	}
	tc := t.cfg.Client(l.to.Peer).Clone()
	tc.NextProtos = []string{ALPN}
	t.n.dials.Add(1)
	c, err = t.tr.Dial(t.ctx, net.UDPAddrFromAddrPort(ap), tc, t.qconf)
	if err != nil {
		if t.ctx.Err() == nil {
			t.n.dialFails.Add(1)
		}
		return nil
	}
	if id, err := t.peerID(c); err != nil || id != l.to.Peer {
		c.CloseWithError(closeRefused, "not the member dialled")
		t.n.dialFails.Add(1)
		return nil
	}
	if !t.adopt(c, l.to, false) {
		if t.ctx.Err() == nil {
			t.n.dialFails.Add(1)
		}
		return nil
	}
	return c
}

// connContext marks each connection dialled to the transport, and counts
// it as a failed accept if it ends without being taken on.
func (t *Transport) connContext(ctx context.Context, _ *quicgo.ClientInfo) (context.Context, error) {
	h := &handshake{}
	context.AfterFunc(ctx, func() {
		if h.state.CompareAndSwap(handshaking, handshakeFailed) {
			t.n.acceptFails.Add(1)
		}
	})
	return context.WithValue(ctx, handshakeKey{}, h), nil
}

// accept adopts each connection that comes in under a certificate naming
// a member, and refuses the others.
func (t *Transport) accept() {
	for {
		c, err := t.ln.Accept(t.ctx)
		if err != nil {
			return
		}
		id, err := t.peerID(c)
		if err != nil {
			c.CloseWithError(closeRefused, "no member")
			continue
		}
		if !c.Context().Value(handshakeKey{}).(*handshake).state.CompareAndSwap(handshaking, handshakeTaken) {
			continue
		}
		if !t.adopt(c, transport.Dest{Addr: c.RemoteAddr().String(), Peer: id}, true) {
			t.n.acceptFails.Add(1)
			continue
		}
		t.n.accepts.Add(1)
	}
}

// peerID is the member c's peer certificate names.
func (t *Transport) peerID(c *quicgo.Conn) (string, error) {
	certs := c.ConnectionState().TLS.PeerCertificates
	if len(certs) == 0 {
		return "", errors.New("quic: no certificate")
	}
	return t.cfg.PeerID(certs[0])
}

// adopt makes c the connection to its member at its address, and reads
// it until it ends, unless the transport is closed or CutOff matched its
// certificate. A connection the member dialled closes the one it dialled
// before. Two members that dial each other at once each end up with two
// connections, both read and both kept while sends go on the later one;
// once it ends, the next send dials. A dialled connection the other end
// refuses once it is made, by a TLS alert or as the transport refuses
// one, counts as a failed dial. It says whether it took c on.
func (t *Transport) adopt(c *quicgo.Conn, to transport.Dest, accepted bool) bool {
	cert := c.ConnectionState().TLS.PeerCertificates[0]
	t.mu.Lock()
	if t.ctx.Err() != nil || t.isCutLocked(cert) {
		t.mu.Unlock()
		c.CloseWithError(closeRefused, "refused")
		return false
	}
	var replaced []*quicgo.Conn
	if accepted {
		for o, was := range t.open {
			if was.accepted && was.to.Peer == to.Peer {
				replaced = append(replaced, o)
			}
		}
	}
	t.open[c] = opened{to: to, accepted: accepted}
	t.conns[to] = c
	t.mu.Unlock()
	for _, o := range replaced {
		o.CloseWithError(closeReplaced, "replaced")
	}
	t.wg.Go(func() { t.readStreams(c, to) })
	t.wg.Go(func() {
		err := t.readPackets(c, to)
		if !accepted && refused(err) {
			t.n.dialFails.Add(1)
		}
		t.mu.Lock()
		delete(t.open, c)
		if t.conns[to] == c {
			delete(t.conns, to)
		}
		t.mu.Unlock()
	})
	return true
}

// refused says whether err ended a connection the other end refused.
func refused(err error) bool {
	if te, ok := errors.AsType[*quicgo.TransportError](err); ok {
		return te.Remote && te.ErrorCode.IsCryptoError()
	}
	if ae, ok := errors.AsType[*quicgo.ApplicationError](err); ok {
		return ae.Remote && ae.ErrorCode == closeRefused
	}
	return false
}

// isCutLocked says whether a match CutOff was given matches cert. t.mu is
// held.
func (t *Transport) isCutLocked(cert *x509.Certificate) bool {
	for _, match := range t.cut {
		if match(cert) {
			return true
		}
	}
	return false
}

// readPackets delivers c's datagrams until c ends or the transport
// closes, and returns the error that ended it.
func (t *Transport) readPackets(c *quicgo.Conn, from transport.Dest) error {
	for {
		b, err := c.ReceiveDatagram(t.ctx)
		if err != nil {
			return err
		}
		t.n.packetsIn.Add(1)
		t.deliver(transport.Message{From: from.Addr, Peer: from.Peer, Data: b})
	}
}

// readStreams reads each stream c opens as one message of at most
// MaxStream bytes, and refuses a longer one. A stream waits, unread, while
// its member has maxStreams being read on all its connections; one still
// waiting when c ends is given up and counted as a failed read.
func (t *Transport) readStreams(c *quicgo.Conn, from transport.Dest) {
	sl := t.slotsOf(from.Peer, 1)
	defer t.slotsOf(from.Peer, -1)
	for {
		s, err := c.AcceptUniStream(t.ctx)
		if err != nil {
			return
		}
		select {
		case sl.ch <- struct{}{}:
		case <-c.Context().Done():
			s.CancelRead(0)
			t.n.readFails.Add(1)
			return
		}
		t.slotsOf(from.Peer, 1)
		t.wg.Go(func() {
			defer func() {
				<-sl.ch
				t.slotsOf(from.Peer, -1)
			}()
			s.SetReadDeadline(time.Now().Add(streamReadTimeout))
			b, err := io.ReadAll(io.LimitReader(s, MaxStream+1))
			switch {
			case err != nil:
				s.CancelRead(0)
				t.n.readFails.Add(1)
				return
			case len(b) > MaxStream:
				s.CancelRead(0)
				t.n.oversize.Add(1)
				return
			}
			t.n.streamsIn.Add(1)
			t.deliver(transport.Message{From: from.Addr, Peer: from.Peer, Data: b, Stream: true})
		})
	}
}

// slotsOf returns peer's slots, adding d to their readers, and forgets
// them once none is left.
func (t *Transport) slotsOf(peer string, d int) *slots {
	t.mu.Lock()
	defer t.mu.Unlock()
	sl := t.streams[peer]
	if sl == nil {
		sl = &slots{ch: make(chan struct{}, maxStreams)}
		t.streams[peer] = sl
	}
	if sl.readers += d; sl.readers == 0 {
		delete(t.streams, peer)
	}
	return sl
}

// deliver hands m to Receive, or drops it if Receive is full.
func (t *Transport) deliver(m transport.Message) {
	select {
	case t.in <- m:
	case <-t.ctx.Done():
	default:
		t.n.receiveFull.Add(1)
	}
}

var _ transport.Transport = (*Transport)(nil)
