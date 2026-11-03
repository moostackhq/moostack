// Package view is a node's member table. It applies entries by the
// precedence rule and says what each one changed. It keeps no timers but
// two: when a gone member is forgotten, and when a forgotten one's grave
// is.
package view

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/moostackhq/moostack/pkg/hivemind/membership/member"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/msg"
)

// Outcome is what applying an update did.
type Outcome uint8

const (
	// Stale: the view already had this entry or a newer one.
	Stale Outcome = iota
	// Changed: the entry replaced the one in the view, or is new.
	Changed
	// Confirmed: the same suspicion, raised by another member.
	Confirmed
	// Ignored: an entry saying a member the view doesn't know is gone, or
	// an entry of a forgotten member no newer than its grave. A node that
	// has forgotten a member doesn't take it back from one that hasn't yet,
	// nor from one that slept through its death.
	Ignored
	// Reburied: a newer entry saying a forgotten member is gone. Its grave
	// took it, and the caller passes it on.
	Reburied
)

// grave is a forgotten member's last entry.
type grave struct {
	member.Member
	at time.Time // when it was forgotten
	n  uint64    // the order it was dug in
}

// digging is a grave in the order graves were dug. One whose grave has
// gone since, or been dug again, is left for bury to skip.
type digging struct {
	id member.ID
	n  uint64
}

// maxGraves caps the graves a view keeps beyond its members.
const maxGraves = 10000

type entry struct {
	member.Member
	gone time.Time // when it became dead or left
}

// View is the table of every member a node knows, itself included.
type View struct {
	self    member.ID
	members map[member.ID]*entry
	ids     []member.ID // sorted
	byAddr  map[string][]member.ID
	graves  map[member.ID]grave
	order   []digging // oldest first
	dug     uint64
	// unburied are the members whose graves went, expired or over the
	// cap, since Unburied was last called.
	unburied []member.ID
	live     int // members not gone, self included
	gone     int
	// firstGone is when the member gone longest became gone; zero when it
	// needs finding again.
	firstGone time.Time
}

// New returns a view holding only self.
func New(self member.Member) *View {
	v := &View{self: self.ID, members: map[member.ID]*entry{}, byAddr: map[string][]member.ID{}, graves: map[member.ID]grave{}}
	v.put(self, time.Time{})
	return v
}

func (v *View) put(m member.Member, now time.Time) {
	e, ok := v.members[m.ID]
	if !ok {
		e = &entry{}
		v.members[m.ID] = e
		i, _ := slices.BinarySearch(v.ids, m.ID)
		v.ids = slices.Insert(v.ids, i, m.ID)
	}
	wasLive := ok && !e.State.Gone()
	// Self doesn't count among the gone.
	wasGone := ok && e.State.Gone() && m.ID != v.self
	if m.State.Gone() && !wasGone && m.ID != v.self {
		e.gone = now
		if v.gone == 0 {
			v.firstGone = now
		}
	}
	if wasGone && !m.State.Gone() && e.gone.Equal(v.firstGone) {
		v.firstGone = time.Time{}
	}
	if wasLive != !m.State.Gone() {
		if wasLive {
			v.live--
		} else {
			v.live++
		}
	}
	if isGone := m.State.Gone() && m.ID != v.self; wasGone != isGone {
		if wasGone {
			v.gone--
		} else {
			v.gone++
		}
	}
	if !ok || e.Addr != m.Addr {
		if ok {
			v.unindex(e.Addr, m.ID)
		}
		if m.Addr != "" {
			v.byAddr[m.Addr] = append(v.byAddr[m.Addr], m.ID)
		}
	}
	e.Member = m
}

func (v *View) unindex(addr string, id member.ID) {
	ids := slices.DeleteFunc(v.byAddr[addr], func(x member.ID) bool { return x == id })
	if len(ids) == 0 {
		delete(v.byAddr, addr)
	} else {
		v.byAddr[addr] = ids
	}
}

// At returns the member at addr, if the view has one: the one furthest from
// gone, alive before suspect before dead, when a member that failed there
// and one that started there since are both in the view.
func (v *View) At(addr string) (member.Member, bool) {
	var found member.Member
	ok := false
	for _, id := range v.byAddr[addr] {
		if m := v.members[id].Member; !ok || m.State < found.State {
			found, ok = m, true
		}
	}
	return found, ok
}

// Apply takes an update into the view at time now.
func (v *View) Apply(now time.Time, u msg.Update) Outcome {
	m := u.Member
	e, ok := v.members[m.ID]
	if g, buried := v.graves[m.ID]; !ok && buried {
		if !member.Overrides(m, g.Member) {
			return Ignored
		}
		if m.State.Gone() {
			// A newer tombstone replaces the grave's entry; the grave keeps
			// its age.
			g.Member = m
			v.graves[m.ID] = g
			return Reburied
		}
		delete(v.graves, m.ID)
	}
	switch {
	case !ok && m.State.Gone():
		return Ignored
	case !ok || member.Overrides(m, e.Member):
		v.put(m, now)
		return Changed
	case m.State == member.Suspect && e.State == member.Suspect && m.Incarnation == e.Incarnation:
		return Confirmed
	}
	return Stale
}

// SetSelf replaces the node's own entry, which only the node changes. An
// entry of another member is ignored.
func (v *View) SetSelf(m member.Member) {
	if m.ID == v.self {
		v.put(m, time.Time{})
	}
}

func (v *View) Self() member.Member { return v.members[v.self].Member }

func (v *View) Get(id member.ID) (member.Member, bool) {
	e, ok := v.members[id]
	if !ok {
		return member.Member{}, false
	}
	return e.Member, true
}

// All returns every entry, sorted by ID.
func (v *View) All() []member.Member {
	out := make([]member.Member, 0, len(v.ids))
	for _, id := range v.ids {
		out = append(out, v.members[id].Member)
	}
	return out
}

// IDs returns every member's ID, sorted. The slice is the view's own.
func (v *View) IDs() []member.ID { return v.ids }

// Live counts the members that aren't gone, self included.
func (v *View) Live() int { return v.live }

// Common returns the capabilities every live member has.
func (v *View) Common() member.Caps {
	cs, any := ^member.Caps(0), false
	for _, e := range v.members {
		if !e.State.Gone() {
			cs &= e.Caps
			any = true
		}
	}
	if !any {
		return 0
	}
	return cs
}

// Forget removes the members gone for at least after, and returns them.
// Each leaves a grave, its last entry, kept for graves: entries of the
// member no newer than it are ignored, and healing probes can still reach
// it.
func (v *View) Forget(now time.Time, after, graves time.Duration) []member.Member {
	for id, g := range v.graves {
		if !now.Before(g.at.Add(graves)) {
			delete(v.graves, id)
			v.unburied = append(v.unburied, id)
		}
	}
	if next := v.NextForget(after); next.IsZero() || now.Before(next) {
		return nil
	}
	v.firstGone = time.Time{}
	var out []member.Member
	for _, id := range slices.Clone(v.ids) {
		e := v.members[id]
		if id != v.self && e.State.Gone() && !now.Before(e.gone.Add(after)) {
			out = append(out, e.Member)
			v.gone--
			v.unindex(e.Addr, id)
			delete(v.members, id)
			i, _ := slices.BinarySearch(v.ids, id)
			v.ids = slices.Delete(v.ids, i, i+1)
			if graves > 0 {
				v.bury(e.Member, now)
			}
		}
	}
	return out
}

// bury keeps m as the grave of a member forgotten at now. Beyond
// maxGraves, the graves dug first go, whichever way they were dug.
func (v *View) bury(m member.Member, now time.Time) {
	v.dug++
	v.graves[m.ID] = grave{Member: m, at: now, n: v.dug}
	v.order = append(v.order, digging{id: m.ID, n: v.dug})
	for len(v.graves) > maxGraves {
		d := v.order[0]
		v.order = v.order[1:]
		if g, ok := v.graves[d.id]; ok && g.n == d.n {
			delete(v.graves, d.id)
			v.unburied = append(v.unburied, d.id)
		}
	}
	if len(v.order) > 2*len(v.graves)+64 {
		v.order = slices.DeleteFunc(v.order, func(d digging) bool {
			g, ok := v.graves[d.id]
			return !ok || g.n != d.n
		})
	}
}

// Admits reports whether Apply would take m as a new member: m is unknown,
// not gone, and newer than any grave of it.
func (v *View) Admits(m member.Member) bool {
	if _, ok := v.members[m.ID]; ok || m.State.Gone() {
		return false
	}
	g, buried := v.graves[m.ID]
	return !buried || member.Overrides(m, g.Member)
}

// ForgetOldest forgets, at now, the member gone longest, to make room for
// a live one in a full view; it leaves a grave, as Forget does. It returns
// the member forgotten, if there was one.
func (v *View) ForgetOldest(now time.Time) (member.Member, bool) {
	if v.gone == 0 {
		return member.Member{}, false
	}
	oldest := member.ID("")
	for _, id := range v.ids {
		if e := v.members[id]; id != v.self && e.State.Gone() && (oldest == "" || e.gone.Before(v.members[oldest].gone)) {
			oldest = id
		}
	}
	e := v.members[oldest]
	v.gone--
	v.firstGone = time.Time{}
	v.unindex(e.Addr, oldest)
	delete(v.members, oldest)
	i, _ := slices.BinarySearch(v.ids, oldest)
	v.ids = slices.Delete(v.ids, i, i+1)
	v.bury(e.Member, now)
	return e.Member, true
}

// Unburied returns the members whose graves went, expired or over the
// cap, since the last call: nothing is kept of them any more.
func (v *View) Unburied() []member.ID {
	out := v.unburied
	v.unburied = nil
	return out
}

// Check reports a broken invariant of the view: the sorted IDs, the address index, the counts of live and
// gone members, and the graves, capped, gone, of no member in the view,
// and each in the order dug once.
func (v *View) Check() error {
	if len(v.ids) != len(v.members) || !slices.IsSorted(v.ids) {
		return fmt.Errorf("view: %d IDs, sorted %v, for %d members", len(v.ids), slices.IsSorted(v.ids), len(v.members))
	}
	if _, ok := v.members[v.self]; !ok {
		return errors.New("view: no entry of self")
	}
	live, gone, indexed := 0, 0, 0
	for _, id := range v.ids {
		e, ok := v.members[id]
		if !ok {
			return fmt.Errorf("view: ID %s without an entry", id)
		}
		if !e.State.Gone() {
			live++
		} else if id != v.self {
			gone++
		}
		if e.Addr != "" {
			indexed++
			if !slices.Contains(v.byAddr[e.Addr], id) {
				return fmt.Errorf("view: %s not indexed at %q", id, e.Addr)
			}
		}
		if _, buried := v.graves[id]; buried {
			return fmt.Errorf("view: %s both a member and buried", id)
		}
	}
	if live != v.live || gone != v.gone {
		return fmt.Errorf("view: counted %d live and %d gone, kept %d and %d", live, gone, v.live, v.gone)
	}
	n := 0
	for addr, ids := range v.byAddr {
		for _, id := range ids {
			if e, ok := v.members[id]; !ok || e.Addr != addr {
				return fmt.Errorf("view: %s indexed at %q, not its address", id, addr)
			}
		}
		n += len(ids)
	}
	if n != indexed {
		return fmt.Errorf("view: %d indexed, %d members with addresses", n, indexed)
	}
	if len(v.graves) > maxGraves {
		return fmt.Errorf("view: %d graves, over %d", len(v.graves), maxGraves)
	}
	dug := map[member.ID]int{}
	for _, d := range v.order {
		if g, ok := v.graves[d.id]; ok && g.n == d.n {
			dug[d.id]++
		}
	}
	for id, g := range v.graves {
		if !g.State.Gone() {
			return fmt.Errorf("view: the grave of %s holds a %v entry", id, g.State)
		}
		if dug[id] != 1 {
			return fmt.Errorf("view: the grave of %s is in the order dug %d times", id, dug[id])
		}
	}
	return nil
}

// Grave returns a random forgotten member's last entry.
func (v *View) Grave(rng *rand.Rand) (member.Member, bool) {
	if len(v.graves) == 0 {
		return member.Member{}, false
	}
	ids := make([]member.ID, 0, len(v.graves))
	for id := range v.graves {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return v.graves[ids[rng.IntN(len(ids))]].Member, true
}

// Buried returns the last entry of a forgotten member.
func (v *View) Buried(id member.ID) (member.Member, bool) {
	g, ok := v.graves[id]
	return g.Member, ok
}

// NextForget returns when the next gone member is due to be forgotten, or
// the zero time if none is gone.
func (v *View) NextForget(after time.Duration) time.Time {
	if v.gone == 0 {
		return time.Time{}
	}
	if v.firstGone.IsZero() {
		for _, id := range v.ids {
			if e := v.members[id]; id != v.self && e.State.Gone() && (v.firstGone.IsZero() || e.gone.Before(v.firstGone)) {
				v.firstGone = e.gone
			}
		}
	}
	return v.firstGone.Add(after)
}

// Random returns a member chosen by pick from up to tries random draws of
// the view, falling back to a scan when the draws find none.
func (v *View) Random(rng *rand.Rand, tries int, pick func(member.Member) bool) (member.Member, bool) {
	for range tries {
		if e := v.members[v.ids[rng.IntN(len(v.ids))]]; pick(e.Member) {
			return e.Member, true
		}
	}
	var c []member.Member
	for _, id := range v.ids {
		if e := v.members[id]; pick(e.Member) {
			c = append(c, e.Member)
		}
	}
	if len(c) == 0 {
		return member.Member{}, false
	}
	return c[rng.IntN(len(c))], true
}
