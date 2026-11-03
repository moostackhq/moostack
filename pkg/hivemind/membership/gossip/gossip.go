// Package gossip is the queue of updates a node piggybacks on its probe
// messages (SWIM 4.1). Each update is sent a limited number of times,
// least-sent first, and as many as fit in the packet's byte budget.
package gossip

import (
	"maps"
	"math"
	"slices"
	"time"

	"github.com/moostackhq/moostack/pkg/hivemind/membership/member"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/msg"
)

// Sizer counts bytes the way the codec writes them.
type Sizer interface {
	// PacketSize is the size of p encoded in wire version v.
	PacketSize(p msg.Packet, v uint8) int
	// UpdateSize is what adding u to a packet's gossip in wire version v
	// adds to its size.
	UpdateSize(u msg.Update, v uint8) int
}

// Pacer times the gossip a node sends on its own, apart from its probes:
// every Every, to Fanout members, when it has updates to send.
type Pacer struct {
	every  time.Duration
	Fanout int
	next   time.Time
}

// NewPacer returns a pacer whose first round is every after now. An every
// of 0 turns it off.
func NewPacer(every time.Duration, fanout int, now time.Time) *Pacer {
	return &Pacer{every: every, Fanout: fanout, next: now.Add(every)}
}

// Tick reports whether a round is due at now, and schedules the next.
func (p *Pacer) Tick(now time.Time) bool {
	if p.every <= 0 || p.Fanout <= 0 || now.Before(p.next) {
		return false
	}
	p.next = now.Add(p.every)
	return true
}

// Due returns when the next round is, or the zero time when off.
func (p *Pacer) Due() time.Time {
	if p.every <= 0 || p.Fanout <= 0 {
		return time.Time{}
	}
	return p.next
}

// Shift moves the next round later by d.
func (p *Pacer) Shift(d time.Duration) { p.next = p.next.Add(d) }

// Limit is how many times each update is sent in a cluster of n live
// members: mult × ⌈log10(n+1)⌉, at least 1 (SWIM 4.1, λ log n).
func Limit(mult, n int) int {
	return max(mult*int(math.Ceil(math.Log10(float64(n+1)))), 1)
}

type key struct {
	id member.ID
	by member.ID // set for suspicions: one update per raiser
}

type item struct {
	u    msg.Update
	k    key
	sent int
	gone bool // replaced or sent its limit: left in its bucket until passed
}

// minUpdate is less than any update's encoded size: a budget below it
// takes nothing more.
const minUpdate = 4

// maxMisses is how many updates in a row too big for the packet's budget
// end a Take.
const maxMisses = 16

// Queue holds the updates still to be sent. Its buckets hold them by how
// often they were sent, each in the order they entered it.
type Queue struct {
	items   map[key]*item
	byID    map[member.ID][]key
	buckets [][]*item
}

// New returns an empty queue.
func New() *Queue { return &Queue{items: map[key]*item{}, byID: map[member.ID][]key{}} }

// Add queues u, to be sent anew. An update that isn't a suspicion replaces
// every queued update about the same member; a suspicion replaces the
// member's other updates but suspicions raised by others at the same
// incarnation.
func (q *Queue) Add(u msg.Update) {
	id := u.Member.ID
	k := key{id: id}
	if u.Member.State == member.Suspect {
		k.by = u.By
	}
	keep := q.byID[id][:0]
	for _, old := range q.byID[id] {
		it := q.items[old]
		if k.by == "" || old.by == "" || old == k || it.u.Member.Incarnation != u.Member.Incarnation {
			it.gone = true
			delete(q.items, old)
			continue
		}
		keep = append(keep, old)
	}
	it := &item{u: u, k: k}
	q.items[k] = it
	q.byID[id] = append(keep, k)
	if len(q.buckets) == 0 {
		q.buckets = append(q.buckets, nil)
	}
	q.buckets[0] = append(q.buckets[0], it)
	if len(q.buckets[0]) > 2*len(q.items)+64 {
		q.sweep()
	}
}

func (q *Queue) remove(it *item) {
	it.gone = true
	delete(q.items, it.k)
	keys := slices.DeleteFunc(q.byID[it.k.id], func(k key) bool { return k == it.k })
	if len(keys) == 0 {
		delete(q.byID, it.k.id)
	} else {
		q.byID[it.k.id] = keys
	}
}

// Forget drops every queued update about id.
func (q *Queue) Forget(id member.ID) {
	for _, k := range q.byID[id] {
		q.items[k].gone = true
		delete(q.items, k)
	}
	delete(q.byID, id)
	q.sweep()
}

// sweep drops replaced items from the buckets once they hold more than
// twice the queue.
func (q *Queue) sweep() {
	total := 0
	for _, b := range q.buckets {
		total += len(b)
	}
	if total <= 2*len(q.items)+64 {
		return
	}
	for s := range q.buckets {
		q.buckets[s] = slices.DeleteFunc(q.buckets[s], func(it *item) bool { return it.gone })
	}
}

// IDs returns the members with updates queued, sorted.
func (q *Queue) IDs() []member.ID { return slices.Sorted(maps.Keys(q.byID)) }

func (q *Queue) Len() int { return len(q.items) }

// Take returns at most most updates to send in a packet with budget bytes
// left, least-sent first and then newest first, skipping members in skip.
// Each update taken counts as sent once; one sent limit times leaves the
// queue.
func (q *Queue) Take(budget, most, limit int, sz Sizer, v uint8, skip ...member.ID) []msg.Update {
	var out []msg.Update
	var moved []*item
	misses := 0
	for s := 0; s < len(q.buckets) && budget >= minUpdate && len(out) < most && misses < maxMisses; s++ {
		b := q.buckets[s]
		// Walk from the newest; what was walked is rebuilt without the
		// items taken or gone, and the rest is left as it is.
		i := len(b)
		var kept []*item
		// A run of maxMisses updates too big for what is left ends the
		// take: nothing is taken from more-sent buckets past updates less
		// sent.
		for i > 0 && budget >= minUpdate && len(out) < most && misses < maxMisses {
			i--
			it := b[i]
			if it.gone {
				continue
			}
			if slices.Contains(skip, it.k.id) {
				kept = append(kept, it)
				continue
			}
			n := sz.UpdateSize(it.u, v)
			if n > budget {
				kept = append(kept, it)
				misses++
				continue
			}
			misses = 0
			budget -= n
			out = append(out, it.u)
			it.sent++
			moved = append(moved, it)
		}
		slices.Reverse(kept)
		q.buckets[s] = append(b[:i], kept...)
	}
	q.sweep()
	// In the order they entered their bucket, oldest first.
	slices.Reverse(moved)
	for _, it := range moved {
		if it.sent >= limit {
			q.remove(it)
			continue
		}
		for len(q.buckets) <= it.sent {
			q.buckets = append(q.buckets, nil)
		}
		q.buckets[it.sent] = append(q.buckets[it.sent], it)
	}
	return out
}
