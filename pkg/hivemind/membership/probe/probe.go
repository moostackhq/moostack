// Package probe is SWIM's failure detector (SWIM 3.1, 4.3) with Lifeguard's
// nacks (Lifeguard 4.1). Each protocol period it pings one member, taking
// the members in a random order reshuffled every round. A member that
// doesn't ack within the timeout is pinged through k helpers, and one that
// hasn't acked by the end of the period has failed its probe. It also does
// a helper's part: pinging a target for another member, forwarding the ack,
// and sending a nack when none came.
package probe

import (
	"maps"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/moostackhq/moostack/pkg/hivemind/membership/member"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/msg"
)

// Config is the detector's timing.
type Config struct {
	// Interval is the protocol period T', and Timeout the wait for a direct
	// ack before probing indirectly. Both are stretched by the scale the
	// node passes, from its local health.
	Interval time.Duration
	Timeout  time.Duration
	// Helpers is k, how many members an indirect probe goes through.
	Helpers int
	// RTTMult lets the timeout of a member grow to RTTMult times its
	// smoothed round-trip time, but never past half a period.
	RTTMult int
	// HealEvery sends every HealEvery-th probe to a random gone member. 0
	// turns it off.
	HealEvery int
	// Nacks: as a helper, send a nack to an asker that has CapNack.
	Nacks bool
}

// Members is what the detector reads of the view.
type Members interface {
	Self() member.Member
	IDs() []member.ID
	Get(member.ID) (member.Member, bool)
	// Random returns a member pick accepts, drawn at random.
	Random(rng *rand.Rand, tries int, pick func(member.Member) bool) (member.Member, bool)
	// Grave returns a random forgotten member's last entry.
	Grave(rng *rand.Rand) (member.Member, bool)
}

// Send is a probe message to a member.
type Send struct {
	To   member.Member
	Body msg.Body
}

// Failure is a probe that got no ack by the end of its period.
type Failure struct {
	Target member.Member
	Asked  int
	// Nackers are the helpers asked that can send a nack, and Nacks how
	// many of them did, each counted once.
	Nackers int
	Nacks   int
}

// Out is what a call to the detector asks the node to do.
type Out struct {
	Sends     []Send
	Failed    []Failure
	Succeeded int // probes acked in time
}

func (o *Out) send(to member.Member, b msg.Body) { o.Sends = append(o.Sends, Send{To: to, Body: b}) }

type probe struct {
	seq      uint32
	target   member.Member
	start    time.Time
	indirect time.Time // when to ask helpers
	end      time.Time // end of the period
	asked    []member.ID
	nackers  int
	nacked   map[member.ID]bool
	acked    bool
	noSample bool // the node was paused during it: no round-trip sample
	gone     bool // a healing probe of a gone member: no failure if unanswered
}

type relay struct {
	asker    member.Member
	askerSeq uint32
	target   member.Member
	nackAt   time.Time
	until    time.Time
	sent     time.Time
	done     bool
	noSample bool // the node was paused during it
}

// maxRelays caps the indirect probes a node does for others at once.
const maxRelays = 1024

type Detector struct {
	cfg    Config
	rng    *rand.Rand
	seq    uint32
	order  []member.ID
	pos    int
	ahead  map[member.ID]bool // the members in order[pos:]
	next   time.Time          // when the next probe starts
	cur    *probe
	relays map[uint32]*relay
	rtt    map[member.ID]time.Duration
	count  int
}

// New returns a detector that starts probing at start.
func New(cfg Config, rng *rand.Rand, start time.Time) *Detector {
	return &Detector{cfg: cfg, rng: rng, next: start, relays: map[uint32]*relay{}, rtt: map[member.ID]time.Duration{}, ahead: map[member.ID]bool{}}
}

func (d *Detector) nextSeq() uint32 {
	d.seq++
	return d.seq
}

// RTT returns the smoothed round-trip time to id, 0 if none is measured.
func (d *Detector) RTT(id member.ID) time.Duration { return d.rtt[id] }

// timeout is the wait for a direct ack from target at the given scale.
func (d *Detector) timeout(target member.ID, scale int) time.Duration {
	t := d.cfg.Timeout * time.Duration(scale)
	if r := d.rtt[target] * time.Duration(d.cfg.RTTMult); r > t {
		t = r
	}
	return min(t, d.cfg.Interval*time.Duration(scale)/2)
}

// Add puts a member that joined into a random place in what is left of the
// round, so it is probed this round.
func (d *Detector) Add(id member.ID) {
	if d.ahead[id] {
		return
	}
	d.ahead[id] = true
	d.order = append(d.order, id)
	i := d.pos + d.rng.IntN(len(d.order)-d.pos)
	last := len(d.order) - 1
	d.order[i], d.order[last] = d.order[last], d.order[i]
}

// target picks the next member to probe: the next live one in the round,
// starting a new shuffled round when it ends.
func (d *Detector) target(ms Members) (member.Member, bool) {
	self := ms.Self().ID
	for range 2 {
		for d.pos < len(d.order) {
			id := d.order[d.pos]
			d.pos++
			delete(d.ahead, id)
			if m, ok := ms.Get(id); ok && id != self && !m.State.Gone() {
				return m, true
			}
		}
		d.order, d.pos = slices.Clone(ms.IDs()), 0
		d.rng.Shuffle(len(d.order), func(i, j int) { d.order[i], d.order[j] = d.order[j], d.order[i] })
		clear(d.ahead)
		for _, id := range d.order {
			d.ahead[id] = true
		}
	}
	return member.Member{}, false
}

// goneTarget picks a random gone member, for healing.
// Members that left aren't probed; dead ones are, and so are forgotten
// ones, half the time when the view has both.
func (d *Detector) goneTarget(ms Members) (member.Member, bool) {
	self := ms.Self().ID
	dead, ok := ms.Random(d.rng, 8, func(m member.Member) bool { return m.ID != self && m.State == member.Dead })
	if ok && d.rng.IntN(2) == 0 {
		return dead, true
	}
	if g, buried := ms.Grave(d.rng); buried && g.State == member.Dead {
		return g, true
	}
	return dead, ok
}

// helpers picks up to k live members other than self and the target.
func (d *Detector) helpers(ms Members, target member.ID) []member.Member {
	self := ms.Self().ID
	var out []member.Member
	chosen := map[member.ID]bool{}
	for range d.cfg.Helpers {
		m, ok := ms.Random(d.rng, 8, func(m member.Member) bool {
			return m.ID != self && m.ID != target && m.State == member.Alive && !chosen[m.ID]
		})
		if !ok {
			break
		}
		chosen[m.ID] = true
		out = append(out, m)
	}
	return out
}

// Tick does what is due at now: ends the current probe, starts the next,
// sends indirect probes, and does the helper's part of relays. scale
// stretches the period and timeout of a probe it starts.
func (d *Detector) Tick(now time.Time, ms Members, scale int) Out {
	var o Out
	if p := d.cur; p != nil {
		if !p.acked && p.asked == nil && !now.Before(p.indirect) && !p.gone {
			for _, h := range d.helpers(ms, p.target.ID) {
				p.asked = append(p.asked, h.ID)
				if h.Caps.Has(member.CapNack) {
					p.nackers++
				}
				o.send(h, msg.PingReq{Seq: p.seq, Target: p.target.ID, TargetAddr: p.target.Addr})
			}
			if p.asked == nil {
				p.asked = []member.ID{}
			}
		}
		if !now.Before(p.end) {
			if !p.acked && !p.gone {
				o.Failed = append(o.Failed, Failure{Target: p.target, Asked: len(p.asked), Nackers: p.nackers, Nacks: len(p.nacked)})
			}
			d.cur = nil
		}
	}
	if d.cur == nil && !now.Before(d.next) {
		d.count++
		period := d.cfg.Interval * time.Duration(scale)
		var (
			m    member.Member
			ok   bool
			gone bool
		)
		if d.cfg.HealEvery > 0 && d.count%d.cfg.HealEvery == 0 {
			m, ok = d.goneTarget(ms)
			gone = ok
		}
		if !ok {
			m, ok = d.target(ms)
		}
		// With nobody to probe, the period still passes.
		d.next = now.Add(period)
		if ok {
			p := &probe{seq: d.nextSeq(), target: m, start: now, end: d.next, gone: gone}
			p.indirect = now.Add(d.timeout(m.ID, scale))
			d.cur = p
			o.send(m, msg.Ping{Seq: p.seq, Target: m.ID})
		}
	}
	for _, seq := range slices.Sorted(maps.Keys(d.relays)) {
		r := d.relays[seq]
		if !r.done && !r.nackAt.IsZero() && !now.Before(r.nackAt) {
			r.nackAt = time.Time{}
			o.send(r.asker, msg.Nack{Seq: r.askerSeq})
		}
		if !now.Before(r.until) {
			delete(d.relays, seq)
		}
	}
	return o
}

// Ack takes an ack with seq from a member: for the current probe, directly
// from its target or forwarded by a helper it asked, or for a relay. An
// ack from anyone else, whatever its sequence number, counts for nothing.
func (d *Detector) Ack(now time.Time, from member.ID, seq uint32) Out {
	var o Out
	if p := d.cur; p != nil && p.seq == seq && !p.acked && (from == p.target.ID || slices.Contains(p.asked, from)) {
		p.acked = true
		if !p.gone {
			o.Succeeded++
		}
		// Only a direct ack gives a round-trip sample.
		if from == p.target.ID && !p.noSample {
			d.sample(from, now.Sub(p.start))
		}
	}
	if r, ok := d.relays[seq]; ok && !r.done && from == r.target.ID {
		r.done = true
		if !r.noSample {
			d.sample(from, now.Sub(r.sent))
		}
		o.send(r.asker, msg.Ack{Seq: r.askerSeq})
	}
	return o
}

func (d *Detector) sample(id member.ID, rtt time.Duration) {
	if old, ok := d.rtt[id]; ok {
		rtt = (7*old + rtt) / 8
	}
	d.rtt[id] = rtt
}

// Nack takes a helper's nack for the current probe.
// Nack takes a helper's nack for the current probe: each helper asked
// counts once, however many copies of its nack arrive.
func (d *Detector) Nack(from member.ID, seq uint32) {
	if p := d.cur; p != nil && p.seq == seq && slices.Contains(p.asked, from) {
		if p.nacked == nil {
			p.nacked = map[member.ID]bool{}
		}
		p.nacked[from] = true
	}
}

// Measured returns the members the detector keeps a round-trip time of,
// sorted.
func (d *Detector) Measured() []member.ID { return slices.Sorted(maps.Keys(d.rtt)) }

// Forget drops what the detector keeps of a member the view forgot.
func (d *Detector) Forget(id member.ID) { delete(d.rtt, id) }

// PingReq does a helper's part: ping the target for the asker, forward its
// ack, and nack if none comes within the nack timeout. scale is the node's
// own health scale.
//
// Only for a live member asking about a live member, pinged at the address
// the view has, not one the request names. A copy of a ping-req already
// relayed is relayed once, and no more than maxRelays at once.
func (d *Detector) PingReq(now time.Time, ms Members, asker member.Member, req msg.PingReq, scale int) Out {
	var o Out
	if a, ok := ms.Get(asker.ID); !ok || a.State.Gone() || len(d.relays) >= maxRelays {
		return o
	}
	target, ok := ms.Get(req.Target)
	if !ok || target.State.Gone() || target.ID == ms.Self().ID {
		return o
	}
	for _, r := range d.relays {
		if r.asker.ID == asker.ID && r.askerSeq == req.Seq {
			return o
		}
	}
	seq := d.nextSeq()
	r := &relay{asker: asker, askerSeq: req.Seq, target: target, sent: now, until: now.Add(d.cfg.Interval * time.Duration(scale))}
	if d.cfg.Nacks && asker.Caps.Has(member.CapNack) {
		// By the base timeout, not stretched by this node's health.
		r.nackAt = now.Add(min(d.cfg.Timeout, d.cfg.Interval*2/5))
	}
	d.relays[seq] = r
	o.send(target, msg.Ping{Seq: seq, Target: target.ID})
	return o
}

// Due returns the next time Tick has something to do.
func (d *Detector) Due() time.Time {
	due := d.next
	if p := d.cur; p != nil {
		due = p.end
		if !p.acked && p.asked == nil && !p.gone && p.indirect.Before(due) {
			due = p.indirect
		}
	}
	for _, r := range d.relays {
		if !r.done && !r.nackAt.IsZero() && r.nackAt.Before(due) {
			due = r.nackAt
		}
		if r.until.Before(due) {
			due = r.until
		}
	}
	return due
}

// Shift moves every deadline later by d, as after the node was paused:
// acks that queued up during the pause still count, and the probe in
// flight gives no round-trip sample.
func (d *Detector) Shift(by time.Duration) {
	d.next = d.next.Add(by)
	if p := d.cur; p != nil {
		p.indirect = p.indirect.Add(by)
		p.end = p.end.Add(by)
		p.noSample = true
	}
	for _, r := range d.relays {
		if !r.nackAt.IsZero() {
			r.nackAt = r.nackAt.Add(by)
		}
		r.until = r.until.Add(by)
		r.noSample = true
	}
}

func (d *Detector) Probing() (member.ID, bool) {
	if d.cur == nil {
		return "", false
	}
	return d.cur.target.ID, true
}
