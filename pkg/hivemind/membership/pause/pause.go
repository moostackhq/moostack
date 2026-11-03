// Package pause notices that its node was paused: a stopped process, a
// starved VM or a long garbage collection.
package pause

import "time"

// Detector compares when a node is called with when it asked to be.
type Detector struct {
	limit time.Duration
	due   time.Time
}

// New returns a detector that calls a call more than limit late a pause.
// A limit of 0 turns it off.
func New(limit time.Duration) *Detector { return &Detector{limit: limit} }

// Expect records the wake-up the node asked for.
func (d *Detector) Expect(wake time.Time) { d.due = wake }

// Check returns how late a call at now is, if it is later than the limit:
// the node was paused that long. It then expects the node at now.
func (d *Detector) Check(now time.Time) (time.Duration, bool) {
	if d.limit <= 0 || d.due.IsZero() {
		return 0, false
	}
	late := now.Sub(d.due)
	if late <= d.limit {
		return 0, false
	}
	d.due = now
	return late, true
}
