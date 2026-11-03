// Package memnet is an in-memory network for the membership runtime: nodes
// in one process, with delay, loss and partitions set while it runs.
package memnet

import (
	"math/rand/v2"
	"sync"
	"time"

	"github.com/moostackhq/moostack/pkg/hivemind/membership/transport"
)

// Network connects the endpoints listening on it.
type Network struct {
	mu        sync.Mutex
	endpoints map[string]*Endpoint
	rng       *rand.Rand
	delay     time.Duration
	jitter    time.Duration
	loss      float64
	cut       func(from, to string) bool
	lost      int
}

// New returns a network that delivers after delay, with no loss.
func New(seed uint64, delay time.Duration) *Network {
	return &Network{endpoints: map[string]*Endpoint{}, rng: rand.New(rand.NewPCG(seed, 0)), delay: delay}
}

// SetDelay sets the delay of every message, plus up to jitter more.
func (n *Network) SetDelay(delay, jitter time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.delay, n.jitter = delay, jitter
}

// SetLoss drops each packet, never a stream, with probability p.
func (n *Network) SetLoss(p float64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.loss = p
}

// Cut drops every message, packet or stream, for which cut returns true,
// those already on their way included; nil heals. cut runs under the
// network's lock, and mustn't call back into it.
func (n *Network) Cut(cut func(from, to string) bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.cut = cut
}

// Listen returns the endpoint at addr, whose messages carry no peer
// identity.
func (n *Network) Listen(addr string) *Endpoint { return n.ListenAs(addr, "") }

// ListenAs returns the endpoint at addr whose messages carry peer as the
// identity of their sender (transport.Message.Peer), and that takes no
// message meant for another peer (transport.Dest.Peer), as an
// authenticating transport's do.
func (n *Network) ListenAs(addr, peer string) *Endpoint {
	n.mu.Lock()
	defer n.mu.Unlock()
	e := &Endpoint{net: n, addr: addr, peer: peer, in: make(chan transport.Message, 1024), done: make(chan struct{})}
	n.endpoints[addr] = e
	return e
}

// Lost counts the packets loss or a cut dropped.
func (n *Network) Lost() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.lost
}

func (n *Network) send(from, peer string, dest transport.Dest, b []byte, stream bool) {
	to := dest.Addr
	n.mu.Lock()
	dst, ok := n.endpoints[to]
	// An endpoint that listens as one peer doesn't take what is meant for
	// another.
	misdirected := ok && dest.Peer != "" && dst.peer != "" && dst.peer != dest.Peer
	drop := !ok || misdirected || n.cut != nil && n.cut(from, to) || !stream && n.loss > 0 && n.rng.Float64() < n.loss
	if drop {
		n.lost++
	}
	d := n.delay
	if n.jitter > 0 {
		d += time.Duration(n.rng.Int64N(int64(n.jitter)))
	}
	n.mu.Unlock()
	if drop {
		return
	}
	m := transport.Message{From: from, Peer: peer, Data: append([]byte(nil), b...), Stream: stream}
	time.AfterFunc(d, func() {
		n.mu.Lock()
		cut := n.cut != nil && n.cut(from, to)
		if cut {
			n.lost++
		}
		n.mu.Unlock()
		if !cut {
			dst.deliver(m)
		}
	})
}

// Endpoint is one node's place on the network.
type Endpoint struct {
	net  *Network
	addr string
	peer string // the identity its messages carry
	in   chan transport.Message
	once sync.Once
	done chan struct{}
}

var _ transport.Transport = (*Endpoint)(nil)

// deliver hands m to the endpoint. A full buffer drops a packet; a stream
// waits until there is room or the endpoint closes.
func (e *Endpoint) deliver(m transport.Message) {
	if m.Stream {
		select {
		case <-e.done:
		case e.in <- m:
		}
		return
	}
	select {
	case <-e.done:
	case e.in <- m:
	default:
	}
}

func (e *Endpoint) Addr() string                           { return e.addr }
func (e *Endpoint) SendPacket(to transport.Dest, b []byte) { e.net.send(e.addr, e.peer, to, b, false) }
func (e *Endpoint) SendStream(to transport.Dest, b []byte) { e.net.send(e.addr, e.peer, to, b, true) }
func (e *Endpoint) Receive() <-chan transport.Message      { return e.in }

// Close takes the endpoint off the network; what is sent to it is dropped.
func (e *Endpoint) Close() error {
	e.once.Do(func() {
		e.net.mu.Lock()
		if e.net.endpoints[e.addr] == e {
			delete(e.net.endpoints, e.addr)
		}
		e.net.mu.Unlock()
		close(e.done)
	})
	return nil
}
