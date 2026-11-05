// Package runtime drives a membership node with the real clock over a
// transport. It holds membership's goroutines and clock: it feeds the node wake-ups, arriving messages and calls, and carries out
// what the node returns.
package runtime

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/moostackhq/moostack/pkg/hivemind/membership/member"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/node"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/transport"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/wire"
)

// Options are the runtime's hooks. They run on a goroutine of their own,
// one at a time and in the order things happened; a slow hook doesn't hold
// the node up, and a hook may call the runtime, Close included.
type Options struct {
	// OnChange is called with each entry of another member that changed.
	OnChange func(member.Member)
	// OnForget is called with each member the node forgot from its view:
	// DeadTimeout after it died or left, to make room in a full view, or
	// on Forget. It comes once per member and step, after the step's
	// OnChange calls, and not for a member taken back in that step.
	OnForget func(member.Member)
	OnEvent  func(node.Event)
	// Formats are the wire formats of versions after 1 that the node's
	// version range needs.
	Formats []wire.Format
	// Unauthenticated takes messages that carry no proven peer
	// (transport.Message.Peer), from a transport that proves nobody, as
	// memnet's Listen. Without
	// it the runtime drops them and counts them (Counts.Unauthenticated).
	Unauthenticated bool
}

// Counts are what the runtime dropped, by why.
type Counts struct {
	// Malformed messages didn't decode.
	Malformed int
	// Unknown messages were of a kind this version doesn't know.
	Unknown int
	// WrongVersion messages came in a wire version the node doesn't read.
	WrongVersion int
	// Unsent are packets and syncs that didn't encode.
	Unsent int
	// Unauthenticated messages carried no proven peer, and
	// Options.Unauthenticated wasn't set.
	Unauthenticated int
}

type Runtime struct {
	tr    transport.Transport
	opts  Options
	codec *wire.Codec
	calls chan call
	stop  chan struct{}
	once  sync.Once
	done  chan struct{}

	hooks *queue

	mu      sync.Mutex
	members []member.Member
	self    member.Member
	health  int
	counts  Counts
}

type call struct {
	in   node.Input
	done chan struct{} // closed once the step's messages are handed over
}

// ErrClosed is a call on a runtime that stopped.
var ErrClosed = errors.New("runtime: closed")

// Start runs a node with cfg over tr until ctx ends or Close. It fails if
// the configuration is invalid, or the node's wire versions lack a format.
// The node's address is the transport's: cfg.Addr may be left empty, and
// any other address is an error. A Seed of 0 is replaced by a random one,
// and an Incarnation of 0 by the clock in milliseconds: a restart's entry
// supersedes its earlier runs'. The node sizes its packets with the codec
// that encodes them.
func Start(ctx context.Context, cfg node.Config, tr transport.Transport, opts Options) (*Runtime, error) {
	if cfg.Addr != "" && cfg.Addr != tr.Addr() {
		return nil, fmt.Errorf("runtime: an address of %q, but the transport's is %q", cfg.Addr, tr.Addr())
	}
	cfg.Addr = tr.Addr()
	if cfg.Incarnation == 0 {
		cfg.Incarnation = uint64(time.Now().UnixMilli())
	}
	if cfg.Seed == 0 {
		var b [8]byte
		rand.Read(b[:])
		cfg.Seed = binary.LittleEndian.Uint64(b[:])
	}
	if cfg.MinVersion == 0 || cfg.Version < cfg.MinVersion {
		return nil, cfg.Validate(nil)
	}
	codec, err := wire.NewCodec(cfg.MinVersion, cfg.Version, opts.Formats...)
	if err != nil {
		return nil, err
	}
	if err := cfg.Validate(codec); err != nil {
		return nil, err
	}
	r := &Runtime{tr: tr, opts: opts, codec: codec, calls: make(chan call), stop: make(chan struct{}), done: make(chan struct{}), hooks: newQueue()}
	n := node.New(cfg, codec, time.Now())
	r.snapshot(n)
	go r.loop(ctx, n)
	go r.hooks.run()
	return r, nil
}

// Decode turns a message from the network into a node input: a
// node.Unreadable for one in a wire version c doesn't read.
func Decode(c *wire.Codec, m transport.Message) (node.Input, error) {
	var (
		in  node.Input
		err error
	)
	if m.Stream {
		s := node.ReceiveSync{Addr: m.From, Peer: member.ID(m.Peer)}
		s.Sync, err = c.DecodeSync(m.Data)
		in = s
	} else {
		p := node.Receive{Addr: m.From, Peer: member.ID(m.Peer)}
		p.Packet, err = c.DecodePacket(m.Data)
		in = p
	}
	if errors.Is(err, wire.ErrVersion) {
		v, _, _ := wire.PeekKind(m.Data)
		return node.Unreadable{Addr: m.From, Peer: member.ID(m.Peer), Version: v}, nil
	}
	return in, err
}

func (r *Runtime) loop(ctx context.Context, n *node.Node) {
	defer close(r.done)
	defer r.hooks.close()
	defer r.tr.Close()
	timer := time.NewTimer(0)
	defer timer.Stop()
	recv := r.tr.Receive()
	for {
		var (
			in   node.Input
			c    call
			tick bool
		)
		select {
		case <-ctx.Done():
			return
		case <-r.stop:
			return
		case <-timer.C:
			tick = true
		case c = <-r.calls:
			in = c.in
		case m, ok := <-recv:
			if !ok {
				// The transport is done: no more messages, and the node
				// runs on its timers until Close.
				recv = nil
				continue
			}
			in = r.input(m)
			if in == nil {
				continue
			}
		}
		if tick {
			// Messages that queued up while the process didn't run go to
			// the node before its timer does: an ack that waits is read
			// before the probe it answers ends.
			recv = r.drain(n, recv)
			in = node.Tick{}
		}
		res := n.Step(time.Now(), in)
		r.carryOut(n, res)
		if c.done != nil {
			close(c.done)
		}
		select {
		case <-r.stop:
			return
		default:
		}
		r.reset(timer, res.Wake)
	}
}

// reset sets the timer for wake, at least a millisecond away, or stops it
// for a zero wake.
func (r *Runtime) reset(timer *time.Timer, wake time.Time) {
	if wake.IsZero() {
		timer.Stop()
		return
	}
	timer.Reset(max(time.Until(wake), time.Millisecond))
}

// drain steps the node with the messages already waiting, up to a bound,
// and returns the channel, nil once the transport closed it.
func (r *Runtime) drain(n *node.Node, recv <-chan transport.Message) <-chan transport.Message {
	for range 256 {
		select {
		case m, ok := <-recv:
			if !ok {
				return nil
			}
			if in := r.input(m); in != nil {
				r.carryOut(n, n.Step(time.Now(), in))
			}
		default:
			return recv
		}
	}
	return recv
}

// input decodes m, counting what it drops.
func (r *Runtime) input(m transport.Message) node.Input {
	if m.Peer == "" && !r.opts.Unauthenticated {
		r.mu.Lock()
		r.counts.Unauthenticated++
		r.mu.Unlock()
		return nil
	}
	in, err := Decode(r.codec, m)
	r.mu.Lock()
	defer r.mu.Unlock()
	switch {
	case errors.Is(err, wire.ErrUnknownKind):
		r.counts.Unknown++
		return nil
	case err != nil:
		r.counts.Malformed++
		return nil
	}
	if _, ok := in.(node.Unreadable); ok {
		r.counts.WrongVersion++
	}
	return in
}

func (r *Runtime) carryOut(n *node.Node, res node.Result) {
	unsent := 0
	for _, p := range res.Packets {
		if b, err := r.codec.EncodePacket(p.Packet, p.Version); err == nil {
			r.tr.SendPacket(transport.Dest{Addr: p.Addr, Peer: string(p.To)}, b)
		} else {
			unsent++
		}
	}
	for _, s := range res.Syncs {
		if b, err := r.codec.EncodeSync(s.Sync, s.Version); err == nil {
			r.tr.SendStream(transport.Dest{Addr: s.Addr, Peer: string(s.To)}, b)
		} else {
			unsent++
		}
	}
	if f := r.opts.OnChange; f != nil {
		for _, m := range res.Changes {
			r.hooks.push(func() { f(m) })
		}
	}
	if f := r.opts.OnForget; f != nil {
		for _, m := range res.Forgotten {
			r.hooks.push(func() { f(m) })
		}
	}
	if f := r.opts.OnEvent; f != nil {
		for _, e := range res.Events {
			r.hooks.push(func() { f(e) })
		}
	}
	if unsent > 0 {
		r.mu.Lock()
		r.counts.Unsent += unsent
		r.mu.Unlock()
	}
	if len(res.Changes) > 0 || len(res.Forgotten) > 0 || n.Self() != r.Self() || n.Health() != r.Health() {
		r.snapshot(n)
	}
}

func (r *Runtime) snapshot(n *node.Node) {
	members, self, health := n.Members(), n.Self(), n.Health()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.members, r.self, r.health = members, self, health
}

// do hands the loop an input and waits until the node has stepped with it
// and its messages are handed to the transport. It returns ErrClosed when
// the runtime closed before the node took the input.
func (r *Runtime) do(in node.Input) error {
	c := call{in: in, done: make(chan struct{})}
	select {
	case r.calls <- c:
	case <-r.done:
		return ErrClosed
	}
	select {
	case <-c.done:
	case <-r.done:
		select {
		case <-c.done:
		default:
			return ErrClosed
		}
	}
	return nil
}

// Join contacts the seeds, each named by its member ID beside its address,
// keeps them for the periodic seed syncs, and lifts the hold Forget put on
// each seed: entries of it are taken again. A node that left joins again.
// A seed with no ID or no address is refused, and the call does nothing.
func (r *Runtime) Join(seeds ...node.Seed) error {
	for _, s := range seeds {
		if s.ID == "" || s.Addr == "" {
			return fmt.Errorf("runtime: a seed %q at %q, want both a member ID and an address", s.ID, s.Addr)
		}
	}
	return r.do(node.Join{Seeds: slices.Clone(seeds)})
}

// Forget removes the member id from the view at once, reports it to
// OnForget if it was in the view, and ignores every entry of it, at any
// incarnation and from anyone, until Join names it
// as a seed (node.Forget). A member the node doesn't know yet is held out
// too. Forgetting the node itself, or the empty ID, is refused.
func (r *Runtime) Forget(id member.ID) error {
	if id == "" || id == r.Self().ID {
		return fmt.Errorf("runtime: can't forget %q, the node itself or no member", id)
	}
	return r.do(node.Forget{ID: id})
}

// Leave tells the cluster the node is leaving. It returns once the
// goodbyes are handed to the transport; Close may follow at once.
func (r *Runtime) Leave() error { return r.do(node.Leave{}) }

// Members returns every entry the node has, itself included.
func (r *Runtime) Members() []member.Member {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.members)
}

func (r *Runtime) Self() member.Member {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.self
}

func (r *Runtime) Health() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.health
}

func (r *Runtime) Counts() Counts {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.counts
}

// Close stops the node and waits for it. Hooks still queued run after.
func (r *Runtime) Close() {
	r.once.Do(func() { close(r.stop) })
	<-r.done
}

// queue runs functions one at a time on a goroutine of its own, as many
// as are pushed: pushing never blocks.
type queue struct {
	mu     sync.Mutex
	cond   *sync.Cond
	fns    []func()
	closed bool
	done   chan struct{}
}

func newQueue() *queue {
	q := &queue{done: make(chan struct{})}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *queue) push(f func()) {
	q.mu.Lock()
	q.fns = append(q.fns, f)
	q.mu.Unlock()
	q.cond.Signal()
}

// close lets run return once the functions pushed so far have run.
func (q *queue) close() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	q.cond.Signal()
}

func (q *queue) run() {
	defer close(q.done)
	for {
		q.mu.Lock()
		for len(q.fns) == 0 && !q.closed {
			q.cond.Wait()
		}
		if len(q.fns) == 0 {
			q.mu.Unlock()
			return
		}
		f := q.fns[0]
		q.fns = q.fns[1:]
		q.mu.Unlock()
		f()
	}
}
