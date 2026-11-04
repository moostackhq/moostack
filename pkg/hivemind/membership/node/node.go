// Package node is one member of the membership protocol: the place where
// the view, the failure detector, the suspicion timers, the local health
// score and the gossip queue meet. It is a deterministic state machine:
// Step takes the time and one input, and returns what to send, what
// changed, and when to call Step next. It reads no clock, does no I/O and
// starts no goroutine; its randomness comes from Config.Seed.
//
// How the components fit together, per input:
//
//	any input  → view, first after the pause check: graves at GraveTimeout go
//	packet in  → view (sender's entry, gossip) → probe (ack, nack, ping-req)
//	tick       → probe: next probe, indirect probe, failed probe
//	failed     → health (score) and view (suspect) → suspicion (timer)
//	suspicion  → expired → view (dead)
//	view       → changed entries → gossip queue, piggybacked on what probe sends
//	tick       → view: gone members at DeadTimeout are forgotten
//	tick       → pushpull: join retries and periodic whole-view syncs
//	any input  → pause: a call long after its wake-up shifts the probe, suspicion, sync and pacer deadlines
//
// Each part owns its own state; the node owns every transition that
// touches more than one, and keeps them in one place each:
//
//   - Applying an entry (apply) may change the view, the probe order, the
//     suspicion timers and the gossip queue; a full view makes room first,
//     and only for an entry it would take.
//   - Forgetting a member, at DeadTimeout, to make room or on Forget
//     (forgot), drops its round-trip time, queued gossip, incompatibility
//     and unspread entry (discard), and reports it unless the step takes
//     it back; Forget of a member not in the view drops the same and
//     reports nothing.
//   - A grave going, expired, over the cap or lifted (end of Step), drops
//     what is still kept of a member the node no longer remembers.
//   - A pause (stall) shifts the probe's, the suspicions', the syncs' and
//     the gossip pacer's deadlines; graves and DeadTimeout keep theirs.
//   - A packet (sendTo) is sized with the Sizer of the codec that encodes
//     it, and within MaxPacket and msg's limits.
//
// Check reports a state one of these left inconsistent.
package node

import (
	"fmt"
	"hash/fnv"
	"math"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/moostackhq/moostack/pkg/hivemind/membership/gossip"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/health"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/member"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/msg"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/pause"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/probe"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/pushpull"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/suspicion"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/view"
)

// Node is one member.
type Node struct {
	cfg Config
	// rng draws for gossip and leaving; the failure detector and the syncs
	// draw from streams of their own.
	rng    *rand.Rand
	view   *view.View
	probe  *probe.Detector
	susp   *suspicion.Tracker
	health *health.Score
	gossip *gossip.Queue
	syncs  *pushpull.Scheduler
	pacer  *gossip.Pacer
	pause  *pause.Detector
	left   bool
	// incompatible are the members already reported as reading no version
	// the node writes.
	incompatible map[string]bool
	// told is when the node last answered an unreadable message from each
	// address; toldPruned when it last dropped the old ones.
	told       map[string]time.Time
	toldPruned time.Time
	// unspread are the entries taken from syncs and not gossiped on, as
	// they were taken: one that comes again by gossip is news going round,
	// and the node gossips it then (see apply).
	unspread map[member.ID]member.Member
	// sz sizes packets as the codec that encodes them does.
	sz gossip.Sizer
}

// streams returns the node's random streams, from its seed and its ID: two
// nodes of one seed but different IDs draw differently.
func streams(cfg Config) (probe, syncs, rest *rand.Rand) {
	h := fnv.New64a()
	h.Write([]byte(cfg.ID))
	id := h.Sum64()
	return rand.New(rand.NewPCG(cfg.Seed, id)), rand.New(rand.NewPCG(cfg.Seed, id^0x9e3779b97f4a7c15)), rand.New(rand.NewPCG(cfg.Seed, id^0xc2b2ae3d27d4eb4f))
}

// New returns a node that starts at now, knowing only itself, that sizes
// its packets with sz, the sizes of the codec that will encode them. The
// configuration must be valid with sz (Config.Validate).
func New(cfg Config, sz gossip.Sizer, now time.Time) *Node {
	probeRNG, syncRNG, rng := streams(cfg)
	self := member.Member{ID: cfg.ID, Addr: cfg.Addr, Incarnation: cfg.Incarnation, Caps: cfg.Caps, MinVersion: cfg.MinVersion, Version: cfg.Version}
	return &Node{
		cfg:    cfg,
		rng:    rng,
		view:   view.New(self),
		probe:  probe.New(cfg.Probe, probeRNG, now),
		susp:   suspicion.New(),
		health: health.New(cfg.HealthMax, cfg.HealthRule),
		gossip: gossip.New(),
		syncs: pushpull.New(pushpull.Config{
			Every:      cfg.Probe.Interval * time.Duration(cfg.SyncEvery),
			SeedEvery:  cfg.SeedSyncEvery,
			RetryEvery: cfg.Probe.Interval * time.Duration(max(cfg.RetryJoinEvery, 1)),
		}, syncRNG, now),
		pause:        pause.New(cfg.LateLimit),
		pacer:        gossip.NewPacer(cfg.GossipInterval, cfg.GossipNodes, now),
		incompatible: map[string]bool{},
		told:         map[string]time.Time{},
		unspread:     map[member.ID]member.Member{},
		sz:           sz,
	}
}

func (n *Node) Step(now time.Time, in Input) Result {
	var r Result
	score := n.health.Value()
	n.stall(now, &r)
	n.view.Expire(now, n.cfg.GraveTimeout)
	switch in := in.(type) {
	case Receive:
		n.receive(now, in, &r)
	case ReceiveSync:
		n.receiveSync(now, in, &r)
	case Join:
		n.join(now, in, &r)
	case Leave:
		n.leave(&r)
	case Forget:
		n.forget(now, in, &r)
	case Unreadable:
		n.unreadable(now, in, &r)
	}
	n.tick(now, &r)
	if v := n.health.Value(); v != score {
		r.Events = append(r.Events, Event{Kind: HealthChanged, Value: v})
	}
	r.Forgotten = n.stillForgotten(r.Forgotten)
	// Of each member whose grave went: expired, over the cap or lifted,
	// and that wasn't taken back or buried again since, drops the queued
	// gossip, round-trip time and incompatibility.
	for _, id := range n.view.Unburied() {
		if !n.keeps(id) {
			n.probe.Forget(id)
			n.gossip.Forget(id)
			delete(n.incompatible, string(id))
		}
	}
	r.Wake = n.wake()
	n.pause.Expect(r.Wake)
	return r
}

// stall notices that the node was paused: it was handed an input well
// after the wake it asked for.
func (n *Node) stall(now time.Time, r *Result) {
	late, paused := n.pause.Check(now)
	if !paused {
		return
	}
	by := late + n.cfg.Probe.Timeout
	n.probe.Shift(by)
	n.susp.Shift(by)
	n.syncs.Shift(by)
	n.pacer.Shift(by)
	r.Events = append(r.Events, Event{Kind: Stalled, Late: late})
}

func (n *Node) scale() int { return n.health.Scale() }

func (n *Node) receive(now time.Time, in Receive, r *Result) {
	p := in.Packet
	if n.impersonated(in.Peer, p.From.ID, r) {
		return
	}
	// What the node had of the sender before: if it is newer than what the
	// sender says of itself, it goes back to the sender.
	before, knewSender := n.view.Get(p.From.ID)
	n.apply(now, msg.Update{Member: p.From}, false, p.From.ID, r)
	// Entries the sender has older than this node: this node sends it the
	// newer ones.
	var newer []msg.Update
	if knewSender && member.Overrides(before, p.From) {
		newer = append(newer, msg.Update{Member: before})
	}
	for _, u := range p.Gossip {
		if m, ok := n.view.Get(u.Member.ID); ok && member.Overrides(m, u.Member) {
			newer = append(newer, msg.Update{Member: m})
		}
		n.apply(now, u, false, p.From.ID, r)
	}
	from, ok := n.view.Get(p.From.ID)
	if !ok {
		from = p.From
	}
	// Answers go where the packet came from, which may not be the address
	// in the sender's entry yet, in a version the sender says it reads.
	reply := p.From
	reply.Addr = in.Addr
	switch b := p.Body.(type) {
	case msg.Ping:
		if b.Target == n.cfg.ID {
			// A sender first heard now gets no queued gossip.
			n.sendTo(reply, msg.Ack{Seq: b.Seq}, knewSender, r, newer...)
		}
	case msg.Ack:
		n.handle(now, n.probe.Ack(now, from.ID, b.Seq), r)
		// An ack from a member the node keeps nothing of, refused by a full
		// view or whose grave went while it was probed, leaves no round-trip
		// time.
		if !n.keeps(from.ID) {
			n.probe.Forget(from.ID)
		}
	case msg.Nack:
		n.probe.Nack(from.ID, b.Seq)
	case msg.PingReq:
		if !n.left {
			n.handle(now, n.probe.PingReq(now, n.view, from, b, n.scale()), r)
		}
	}
	// A packet that isn't a ping gets no ack to carry the correction: the
	// newer entries go back in a gossip packet of their own, to a sender
	// that reads them and that the view holds. A stranger the view refused
	// gets none.
	if _, ping := p.Body.(msg.Ping); !ping && ok && len(newer) > 0 && p.From.Caps.Has(member.CapGossip) {
		n.send(reply, msg.Gossip{}, r, newer...)
	}
}

// impersonated reports a message whose sender entry names a member other
// than the peer the transport proved sent it, which the node then drops
// whole: one admitted member can't speak as another, refuting for it or
// saying it left. A message with no proven peer is taken as it is.
func (n *Node) impersonated(peer, from member.ID, r *Result) bool {
	if peer == "" || peer == from {
		return false
	}
	r.Events = append(r.Events, Event{Kind: Impersonated, Member: from, By: peer})
	return true
}

func (n *Node) receiveSync(now time.Time, in ReceiveSync, r *Result) {
	s := in.Sync
	if n.impersonated(in.Peer, s.From.ID, r) {
		return
	}
	n.apply(now, msg.Update{Member: s.From}, false, s.From.ID, r)
	// A sync says what its sender knows, not who raised it: a suspicion
	// in it is not the sender's own, and doesn't count as a confirmation.
	known := len(n.view.IDs())
	for _, m := range s.View {
		n.apply(now, msg.Update{Member: m}, true, s.From.ID, r)
	}
	if len(n.view.IDs()) > known {
		n.syncs.Learned(now)
	}
	if s.Reply {
		n.sync(in.Addr, s.From.ID, false, r)
	}
}

func (n *Node) syncMsg(reply bool) msg.Sync {
	return msg.Sync{From: n.view.Self(), View: n.view.All(), Reply: reply}
}

// join contacts the seeds. A node that left joins again, at a new
// incarnation.
func (n *Node) join(now time.Time, in Join, r *Result) {
	if n.left {
		n.left = false
		self := n.view.Self()
		self.State = member.Alive
		self.Incarnation++
		n.view.SetSelf(self)
		n.gossip.Add(msg.Update{Member: self})
	}
	var seeds []member.Member
	for _, s := range in.Seeds {
		if s.ID != "" && s.Addr != "" {
			seeds = append(seeds, member.Member{ID: s.ID, Addr: s.Addr})
		}
	}
	for _, s := range n.syncs.Join(now, n.view.Self(), seeds) {
		n.view.Lift(s.ID)
		n.sync(s.Addr, s.ID, true, r)
	}
}

// forget removes a member at once and holds its grave.
func (n *Node) forget(now time.Time, in Forget, r *Result) {
	if in.ID == n.cfg.ID || in.ID == "" {
		return
	}
	n.syncs.Forget(in.ID)
	n.probe.Abandon(in.ID)
	n.susp.Stop(in.ID)
	if m, ok := n.view.Drop(now, in.ID, n.cfg.GraveTimeout); ok {
		n.forgot(m, r)
	} else {
		n.discard(in.ID)
	}
}

// unreadable answers a message in a version the node doesn't read with its
// own entry, which says which versions it reads: once a period per address,
// and only to members and seeds. The answer is an ack, which every version
// knows and which acknowledges no probe.
func (n *Node) unreadable(now time.Time, in Unreadable, r *Result) {
	r.Events = append(r.Events, Event{Kind: WrongVersion, Member: in.Peer, Value: int(in.Version)})
	// The sender is the proven peer, if the transport proves who sends,
	// and is told apart by it; otherwise whoever the view has at the
	// address.
	m, known := n.view.Get(in.Peer)
	key := "peer " + string(in.Peer)
	if in.Peer == "" {
		m, known = n.view.At(in.Addr)
		key = "addr " + in.Addr
	}
	known = known && m.ID != n.cfg.ID
	seed, isSeed := n.syncs.Seed(in.Peer, in.Addr)
	if !known && !isSeed {
		return
	}
	if t, ok := n.told[key]; ok && now.Sub(t) < n.cfg.Probe.Interval {
		return
	}
	if now.Sub(n.toldPruned) >= n.cfg.Probe.Interval {
		n.toldPruned = now
		for a, t := range n.told {
			if now.Sub(t) >= n.cfg.Probe.Interval {
				delete(n.told, a)
			}
		}
	}
	n.told[key] = now
	v, to := n.cfg.MinVersion, seed.ID
	if known {
		to = m.ID
		if w, ok := n.versionFor(m); ok {
			v = w
		}
	}
	r.Packets = append(r.Packets, Packet{To: to, Addr: in.Addr, Version: v, Packet: msg.Packet{From: n.view.Self(), Body: msg.Ack{}}})
}

func (n *Node) liveOthers() []member.Member {
	var live []member.Member
	for _, m := range n.view.All() {
		if m.ID != n.cfg.ID && !m.State.Gone() {
			live = append(live, m)
		}
	}
	return live
}

// leave says goodbye at once: whole views to a few members, which carry its
// entry, and gossip packets to more.
func (n *Node) leave(r *Result) {
	if n.left {
		return
	}
	n.left = true
	self := n.view.Self()
	self.State = member.Left
	n.view.SetSelf(self)
	n.gossip.Add(msg.Update{Member: self})
	live := n.liveOthers()
	n.rng.Shuffle(len(live), func(i, j int) { live[i], live[j] = live[j], live[i] })
	syncs := min(len(live), max(n.cfg.Probe.Helpers, 1))
	for _, m := range live[:syncs] {
		n.sync(m.Addr, m.ID, false, r)
	}
	fanout := min(len(live), syncs+gossip.Limit(1, len(live)+1)+n.cfg.GossipNodes)
	for _, m := range live[:fanout] {
		if m.Caps.Has(member.CapGossip) {
			n.send(m, msg.Gossip{}, r, msg.Update{Member: self})
		}
	}
}

// apply takes an entry from a packet or a sync into the view, and acts on
// what changed. fromSync entries about members the node didn't know aren't
// gossiped on. But
// if the same entry comes again in a packet from another member, it is
// news going round, and the node gossips it then; the member's own entry,
// which every packet from it carries, isn't.
// sender is the member the entry came from.
func (n *Node) apply(now time.Time, u msg.Update, fromSync bool, sender member.ID, r *Result) {
	m := u.Member
	if m.Incarnation > incarnationBound(now) {
		r.Events = append(r.Events, Event{Kind: Refused, Member: m.ID, By: sender, Incarnation: m.Incarnation})
		return
	}
	if m.ID == n.cfg.ID {
		n.aboutSelf(m, r)
		return
	}
	_, known := n.view.Get(m.ID)
	// A full view makes room for a live member by forgetting the member
	// gone longest. With nobody gone, it refuses the entry, a member's own
	// included. A member kept out gets in once a member fails its probes
	// and dies, as every packet it sends carries its entry. An unknown
	// member's gone entry takes no room: the view ignores it. An entry the
	// view wouldn't take, as one no newer than the member's grave, makes no
	// room.
	if n.cfg.MaxMembers > 0 && len(n.view.IDs()) >= n.cfg.MaxMembers && n.view.Admits(m) {
		old, ok := n.view.ForgetOldest(now)
		if !ok {
			r.Events = append(r.Events, Event{Kind: Refused, Member: m.ID, By: sender})
			return
		}
		n.forgot(old, r)
	}
	switch n.view.Apply(now, u) {
	case view.Changed:
		r.Changes = append(r.Changes, m)
		if known || !fromSync {
			n.gossip.Add(u)
			delete(n.unspread, m.ID)
		} else {
			n.unspread[m.ID] = m
		}
		switch m.State {
		case member.Alive:
			n.susp.Stop(m.ID)
			n.probe.Add(m.ID)
		case member.Suspect:
			by := u.By
			if by == "" {
				by = m.ID // unknown raiser: counts as nobody else
			}
			lo, hi, k := n.suspicionTimes()
			n.susp.Start(now, m.ID, m.Incarnation, by, lo, hi, k)
			r.Events = append(r.Events, Event{Kind: Suspected, Member: m.ID, By: u.By, Incarnation: m.Incarnation})
		case member.Dead, member.Left:
			n.susp.Stop(m.ID)
			r.Events = append(r.Events, Event{Kind: Declared, Member: m.ID, By: u.By, Value: int(m.State), Incarnation: m.Incarnation})
		}
	case view.Reburied:
		// From a sync too.
		n.gossip.Add(u)
	case view.Stale:
		if e, ok := n.unspread[m.ID]; ok && !fromSync && m.ID != sender && e == m {
			n.gossip.Add(u)
			delete(n.unspread, m.ID)
		}
	case view.Confirmed:
		if ok, c := n.susp.Confirm(m.ID, m.Incarnation, u.By); ok {
			r.Events = append(r.Events, Event{Kind: Confirmed, Member: m.ID, By: u.By, Value: c, Incarnation: m.Incarnation})
			// The first Confirmations confirmations are gossiped on.
			if n.cfg.Dogpile && c <= n.cfg.Confirmations {
				n.gossip.Add(u)
			}
		}
	}
}

// refutationSlack is how far past the clock, in milliseconds, an
// incarnation may be.
const refutationSlack = 1 << 32

// incarnationBound is the highest incarnation an entry may carry at now. It
// moves with the clock: a member's refutation of an entry at the bound is
// within it a moment later.
func incarnationBound(now time.Time) uint64 {
	ms := now.UnixMilli()
	if ms < 0 {
		ms = 0
	}
	return min(uint64(ms)+refutationSlack, member.MaxIncarnation)
}

// aboutSelf refutes an entry saying the node is suspect or dead, and
// supersedes one an earlier run of the node left: newer than its own, or
// of its own incarnation but different.
func (n *Node) aboutSelf(m member.Member, r *Result) {
	self := n.view.Self()
	if n.left {
		return
	}
	// An alive entry of the node's own incarnation that differs from its
	// own is one another run of the node left: an earlier one at the same
	// incarnation, with another address, capabilities or wire versions, or
	// a twin with the same ID. The node supersedes it, and says so.
	stale := m.Incarnation == self.Incarnation && m.State == member.Alive &&
		(m.Addr != self.Addr || m.Caps != self.Caps || m.Version != self.Version || m.MinVersion != self.MinVersion)
	if !stale && (m.Incarnation < self.Incarnation || m.Incarnation == self.Incarnation && m.State == member.Alive) {
		return
	}
	self.Incarnation = m.Incarnation + 1
	self.State = member.Alive
	n.view.SetSelf(self)
	n.gossip.Add(msg.Update{Member: self})
	if m.State != member.Alive {
		n.health.Refuted()
		r.Events = append(r.Events, Event{Kind: Refuted, Member: self.ID, Incarnation: self.Incarnation})
	} else {
		r.Events = append(r.Events, Event{Kind: Superseded, Member: self.ID, Incarnation: self.Incarnation})
	}
}

// suspicionTimes are the bounds of a new suspicion's timeout, and the
// confirmations that bring it to the lower one.
func (n *Node) suspicionTimes() (lo, hi time.Duration, k int) {
	live := n.view.Live()
	lo = time.Duration(float64(n.cfg.SuspicionMult) * math.Max(1, math.Log10(float64(live))) * float64(n.cfg.Probe.Interval))
	if !n.cfg.Dogpile {
		return lo, lo, 0
	}
	// Everyone but the suspect and this node can confirm.
	return lo, lo * time.Duration(n.cfg.SuspicionMaxMult), max(min(n.cfg.Confirmations, live-2), 0)
}

func (n *Node) tick(now time.Time, r *Result) {
	if !n.left {
		n.handle(now, n.probe.Tick(now, n.view, n.scale()), r)
	}
	for _, e := range n.susp.Expired(now) {
		if m, ok := n.view.Get(e.ID); ok && m.State == member.Suspect && m.Incarnation == e.Incarnation {
			m.State = member.Dead
			n.apply(now, msg.Update{Member: m, By: n.cfg.ID}, false, n.cfg.ID, r)
		}
	}
	for _, m := range n.view.Forget(now, n.cfg.DeadTimeout, n.cfg.GraveTimeout) {
		n.forgot(m, r)
	}
	if !n.left && n.pacer.Tick(now) && n.gossip.Len() > 0 {
		n.gossipRound(r)
	}
	if !n.left && !n.syncs.Due().IsZero() && !now.Before(n.syncs.Due()) {
		for _, m := range n.syncs.Tick(now, n.view.Live() > 1, n.randomLive(nil)) {
			n.sync(m.Addr, m.ID, true, r)
		}
	}
}

// stillForgotten returns the members of forgotten out of the view, each
// once, as last forgotten, in the order of their last report.
func (n *Node) stillForgotten(forgotten []member.Member) []member.Member {
	var out []member.Member
	for i, m := range forgotten {
		_, back := n.view.Get(m.ID)
		later := slices.ContainsFunc(forgotten[i+1:], func(o member.Member) bool { return o.ID == m.ID })
		if !back && !later {
			out = append(out, m)
		}
	}
	return out
}

// forgot drops what the node keeps of a member the view forgot, at
// DeadTimeout, to make room or on Forget, and reports it.
func (n *Node) forgot(m member.Member, r *Result) {
	n.discard(m.ID)
	r.Forgotten = append(r.Forgotten, m)
}

// discard drops the round-trip time, queued gossip, incompatibility and
// unspread entry the node keeps of id.
func (n *Node) discard(id member.ID) {
	n.probe.Forget(id)
	n.gossip.Forget(id)
	delete(n.incompatible, string(id))
	delete(n.unspread, id)
}

// gossipRound sends the queued updates to a few random members that read
// gossip packets.
func (n *Node) gossipRound(r *Result) {
	chosen := map[member.ID]bool{}
	pick := n.randomLive(func(m member.Member) bool { return m.Caps.Has(member.CapGossip) && !chosen[m.ID] })
	for range n.pacer.Fanout {
		m, ok := pick()
		if !ok {
			return
		}
		chosen[m.ID] = true
		n.send(m, msg.Gossip{}, r)
	}
}

// randomLive returns a function that draws a random live member other
// than the node, one that also passes also if given.
func (n *Node) randomLive(also func(member.Member) bool) func() (member.Member, bool) {
	return func() (member.Member, bool) {
		return n.view.Random(n.rng, 8, func(m member.Member) bool {
			return m.ID != n.cfg.ID && !m.State.Gone() && (also == nil || also(m))
		})
	}
}

// handle carries out what the failure detector asks.
func (n *Node) handle(now time.Time, o probe.Out, r *Result) {
	for range o.Succeeded {
		n.health.ProbeSucceeded()
	}
	for _, f := range o.Failed {
		n.health.ProbeFailed(f.Nackers, f.Nacks)
		r.Events = append(r.Events, Event{Kind: ProbeFailed, Member: f.Target.ID, Value: f.Nacks})
		// The probe failed against the entry it was sent to: a member that
		// restarted meanwhile, at a new incarnation or address, wasn't
		// probed, and isn't suspected for it.
		m, ok := n.view.Get(f.Target.ID)
		if ok && !m.State.Gone() && m.Incarnation == f.Target.Incarnation && m.Addr == f.Target.Addr {
			m.State = member.Suspect
			n.apply(now, msg.Update{Member: m, By: n.cfg.ID}, false, n.cfg.ID, r)
		}
	}
	for _, s := range o.Sends {
		n.send(s.To, s.Body, r)
	}
}

// versionFor is the wire version to write to m: the highest both read.
// A member not yet known by its entry gets the lowest the node writes.
func (n *Node) versionFor(m member.Member) (uint8, bool) {
	if m.Version == 0 {
		return n.cfg.MinVersion, true
	}
	v := min(n.cfg.Version, m.Version)
	if v < n.cfg.MinVersion || v < m.MinVersion {
		return 0, false
	}
	return v, true
}

// incompatibleWith reports, once per member, one that reads no wire
// version the node writes. A stranger, one the
// view refused, is reported each time but not remembered.
func (n *Node) incompatibleWith(key string, m member.Member, r *Result) {
	if n.incompatible[key] {
		return
	}
	if n.keeps(m.ID) {
		n.incompatible[key] = true
	}
	r.Events = append(r.Events, Event{Kind: Incompatible, Member: m.ID, Value: int(m.Version)})
}

// keeps reports whether the node keeps anything of id: an entry in the
// view, or a grave.
func (n *Node) keeps(id member.ID) bool {
	_, ok := n.view.Get(id)
	_, buried := n.view.Buried(id)
	return ok || buried
}

// sync adds a whole-view sync to the member to at addr, unless it reads no
// version the node writes.
func (n *Node) sync(addr string, to member.ID, reply bool, r *Result) {
	v := n.cfg.MinVersion
	if m, ok := n.view.Get(to); ok && m.ID != n.cfg.ID {
		w, ok := n.versionFor(m)
		if !ok {
			n.incompatibleWith(string(m.ID), m, r)
			return
		}
		v = w
	}
	r.Syncs = append(r.Syncs, Sync{To: to, Addr: addr, Version: v, Sync: n.syncMsg(reply)})
}

// send builds a packet to to: the node's own entry, the probe message, and
// as much gossip as fits. An entry saying to is not alive goes first: a
// suspicion with Buddy on, and always the entry of a gone or forgotten
// member. The entries in first go before the gossip queue's, as far as
// they fit. Only a member gets the gossip queue's updates: a packet to
// anyone else spends none of their sends.
//
// The version is the highest the node and to both read, by what to says
// of itself when it carries its versions (a reply to its own packet), by
// the view's entry otherwise.
func (n *Node) send(to member.Member, body msg.Body, r *Result, first ...msg.Update) {
	n.sendTo(to, body, true, r, first...)
}

// sendTo is send, with the gossip queue's updates only when queue is set.
func (n *Node) sendTo(to member.Member, body msg.Body, queue bool, r *Result, first ...msg.Update) {
	known, member_ := n.view.Get(to.ID)
	if !member_ {
		known = to
		if g, buried := n.view.Buried(to.ID); buried {
			known = g
		}
	}
	claim := known
	if to.Version != 0 {
		claim = to
	}
	v, ok := n.versionFor(claim)
	if !ok {
		n.incompatibleWith(string(to.ID), claim, r)
		return
	}
	p := msg.Packet{From: n.view.Self(), Body: body}
	var skip []member.ID
	sz := n.sz
	if m := known; m.State != member.Alive && (m.State.Gone() || n.cfg.Buddy) && sz.PacketSize(p, v)+sz.UpdateSize(msg.Update{Member: m}, v) <= n.cfg.MaxPacket {
		// Without its raiser.
		p.Gossip = append(p.Gossip, msg.Update{Member: m})
		skip = append(skip, to.ID)
	}
	for _, u := range first {
		if slices.Contains(skip, u.Member.ID) || len(p.Gossip) == msg.MaxUpdates || sz.PacketSize(p, v)+sz.UpdateSize(u, v) > n.cfg.MaxPacket {
			continue
		}
		p.Gossip = append(p.Gossip, u)
		skip = append(skip, u.Member.ID)
	}
	if member_ && queue {
		budget := n.cfg.MaxPacket - sz.PacketSize(p, v)
		limit := gossip.Limit(n.cfg.RetransmitMult, n.view.Live())
		p.Gossip = append(p.Gossip, n.gossip.Take(budget, msg.MaxUpdates-len(p.Gossip), limit, sz, v, skip...)...)
	}
	r.Packets = append(r.Packets, Packet{To: to.ID, Addr: to.Addr, Version: v, Packet: p})
}

func (n *Node) wake() time.Time {
	var wake time.Time
	var pacer, syncs time.Time
	if !n.left {
		syncs = n.syncs.Due()
		// The gossip timer only while there are updates and someone to
		// send them to: a node left alone sleeps until its probes.
		if n.gossip.Len() > 0 && n.view.Live() > 1 {
			pacer = n.pacer.Due()
		}
	}
	for _, t := range []time.Time{n.susp.Due(), n.view.NextForget(n.cfg.DeadTimeout), syncs, pacer} {
		if !t.IsZero() && (wake.IsZero() || t.Before(wake)) {
			wake = t
		}
	}
	if !n.left {
		if t := n.probe.Due(); wake.IsZero() || t.Before(wake) {
			wake = t
		}
	}
	return wake
}

// Check reports a broken invariant of the node or its view (view.Check):
// queued gossip, round-trip times or a reported incompatibility of a
// member the node keeps nothing of, a suspicion timer of a member not
// suspect, an unspread entry of a member not in the view, or a view past
// MaxMembers.
func (n *Node) Check() error {
	if err := n.view.Check(); err != nil {
		return err
	}
	if n.cfg.MaxMembers > 0 && len(n.view.IDs()) > n.cfg.MaxMembers {
		return fmt.Errorf("node: %d members, over MaxMembers %d", len(n.view.IDs()), n.cfg.MaxMembers)
	}
	for _, id := range n.gossip.IDs() {
		if !n.keeps(id) {
			return fmt.Errorf("node: gossip queued of %s, which the node keeps nothing of", id)
		}
	}
	for _, id := range n.probe.Measured() {
		if !n.keeps(id) {
			return fmt.Errorf("node: a round-trip time of %s, which the node keeps nothing of", id)
		}
	}
	for id := range n.incompatible {
		if !n.keeps(member.ID(id)) {
			return fmt.Errorf("node: %s reported incompatible, and the node keeps nothing of it", id)
		}
	}
	for _, id := range n.susp.IDs() {
		if m, ok := n.view.Get(id); !ok || m.State != member.Suspect {
			return fmt.Errorf("node: a suspicion timer of %s, %v", id, m.State)
		}
	}
	for id := range n.unspread {
		if _, ok := n.view.Get(id); !ok {
			return fmt.Errorf("node: an unspread entry of %s, not in the view", id)
		}
	}
	return nil
}

func (n *Node) Self() member.Member { return n.view.Self() }

func (n *Node) Member(id member.ID) (member.Member, bool) { return n.view.Get(id) }

// Members returns every entry the node has, itself included, sorted by ID.
func (n *Node) Members() []member.Member { return n.view.All() }

func (n *Node) Health() int { return n.health.Value() }

// RTT returns the smoothed round-trip time to id, 0 if none is measured.
func (n *Node) RTT(id member.ID) time.Duration { return n.probe.RTT(id) }

// Suspected reports whether the node has a suspicion timer running for id.
func (n *Node) Suspected(id member.ID) bool { return n.susp.Suspected(id) }

// Probing returns the member being probed, or false.
func (n *Node) Probing() (member.ID, bool) { return n.probe.Probing() }

// Joining reports whether the node is still trying its seeds, knowing no
// other live member.
func (n *Node) Joining() bool { return n.syncs.Joining() }

func (n *Node) Config() Config { return n.cfg }
