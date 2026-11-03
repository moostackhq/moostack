// Package suspicion keeps a timer for each suspected member. A member that
// doesn't refute before its timer ends is declared dead. With Lifeguard's
// local health aware suspicion, the timer starts long and shortens with
// each other member that independently suspects the same member.
package suspicion

import (
	"maps"
	"math"
	"slices"
	"time"

	"github.com/moostackhq/moostack/pkg/hivemind/membership/member"
)

// Timeout is Lifeguard's suspicion timeout after c independent
// confirmations, of the k expected: max at none, falling logarithmically to
// min at k, and never below min (Lifeguard 4.2). With no confirmations
// expected, k of 0, it is min.
func Timeout(min, max time.Duration, k, c int) time.Duration {
	if k <= 0 || c >= k || max <= min {
		return min
	}
	frac := math.Log(float64(c+1)) / math.Log(float64(k+1))
	return max - time.Duration(frac*float64(max-min))
}

type timer struct {
	inc        uint64
	start      time.Time
	min, max   time.Duration
	k          int
	confirmers []member.ID // the members that raised or confirmed it
}

func (t *timer) deadline() time.Time {
	return t.start.Add(Timeout(t.min, t.max, t.k, len(t.confirmers)-1))
}

// Expiry is a suspicion whose timer ended.
type Expiry struct {
	ID          member.ID
	Incarnation uint64
}

// Tracker holds the suspicion timers of one node.
type Tracker struct {
	timers map[member.ID]*timer
}

// New returns a tracker with no suspicions.
func New() *Tracker { return &Tracker{timers: map[member.ID]*timer{}} }

// Start begins a timer for id at incarnation inc, raised by by. It replaces
// a timer of an older incarnation, and leaves one of the same incarnation.
// With k of 0, or min equal to max, confirmations don't shorten it.
func (t *Tracker) Start(now time.Time, id member.ID, inc uint64, by member.ID, min, max time.Duration, k int) {
	if old, ok := t.timers[id]; ok && old.inc >= inc {
		return
	}
	t.timers[id] = &timer{inc: inc, start: now, min: min, max: max, k: k, confirmers: []member.ID{by}}
}

// Confirm records that by also suspects id at incarnation inc. It reports
// whether by is a new confirmer, and the confirmations counted so far.
func (t *Tracker) Confirm(id member.ID, inc uint64, by member.ID) (bool, int) {
	tm, ok := t.timers[id]
	// Confirmations past k aren't kept.
	if ok && len(tm.confirmers) > max(tm.k, 0) {
		return false, len(tm.confirmers) - 1
	}
	if !ok || tm.inc != inc || by == "" || slices.Contains(tm.confirmers, by) {
		if ok {
			return false, len(tm.confirmers) - 1
		}
		return false, 0
	}
	tm.confirmers = append(tm.confirmers, by)
	return true, len(tm.confirmers) - 1
}

func (t *Tracker) Stop(id member.ID) { delete(t.timers, id) }

// Suspected reports whether id has a timer.
func (t *Tracker) Suspected(id member.ID) bool { _, ok := t.timers[id]; return ok }

// IDs returns the members with a suspicion timer running, sorted.
func (t *Tracker) IDs() []member.ID { return slices.Sorted(maps.Keys(t.timers)) }

// Expired removes and returns the timers that ended by now, sorted by ID.
func (t *Tracker) Expired(now time.Time) []Expiry {
	var out []Expiry
	for id, tm := range t.timers {
		if !now.Before(tm.deadline()) {
			out = append(out, Expiry{ID: id, Incarnation: tm.inc})
		}
	}
	slices.SortFunc(out, func(a, b Expiry) int {
		if a.ID < b.ID {
			return -1
		}
		if a.ID > b.ID {
			return 1
		}
		return 0
	})
	for _, e := range out {
		delete(t.timers, e.ID)
	}
	return out
}

// Due returns the earliest deadline, or the zero time with no timers.
func (t *Tracker) Due() time.Time {
	var due time.Time
	for _, tm := range t.timers {
		if d := tm.deadline(); due.IsZero() || d.Before(due) {
			due = d
		}
	}
	return due
}

// Shift moves every timer later by d, as after the node was paused.
func (t *Tracker) Shift(d time.Duration) {
	for _, tm := range t.timers {
		tm.start = tm.start.Add(d)
	}
}
