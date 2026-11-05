package view

import (
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/moostackhq/moostack/pkg/hivemind/membership/member"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/msg"
)

// model is a view's members and graves, kept as plainly as they can be.
type model struct {
	members map[member.ID]modelEntry
	graves  map[member.ID]modelGrave
}

type modelEntry struct {
	m    member.Member
	gone time.Time
}

type modelGrave struct {
	m    member.Member
	at   time.Time
	held bool
}

func (md *model) apply(now time.Time, m member.Member) Outcome {
	e, ok := md.members[m.ID]
	if g, buried := md.graves[m.ID]; !ok && buried {
		if g.held || !member.Overrides(m, g.m) {
			return Ignored
		}
		if m.State.Gone() {
			g.m = m
			md.graves[m.ID] = g
			return Reburied
		}
		delete(md.graves, m.ID)
	}
	switch {
	case !ok && m.State.Gone():
		return Ignored
	case !ok || member.Overrides(m, e.m):
		gone := e.gone
		if m.State.Gone() && (!ok || !e.m.State.Gone()) {
			gone = now
		}
		md.members[m.ID] = modelEntry{m: m, gone: gone}
		return Changed
	case m.State == member.Suspect && e.m.State == member.Suspect && m.Incarnation == e.m.Incarnation:
		return Confirmed
	}
	return Stale
}

func (md *model) drop(now time.Time, self, id member.ID, graves time.Duration) (member.Member, bool) {
	if id == self || id == "" {
		return member.Member{}, false
	}
	m, ok := member.Member{ID: id}, false
	if g, buried := md.graves[id]; buried {
		m = g.m
	} else if e, known := md.members[id]; known {
		m, ok = e.m, true
		delete(md.members, id)
	}
	if graves > 0 {
		md.graves[id] = modelGrave{m: m, at: now, held: true}
	}
	return m, ok
}

func (md *model) expire(now time.Time, graves time.Duration) {
	for id, g := range md.graves {
		if !g.held && !now.Before(g.at.Add(graves)) {
			delete(md.graves, id)
		}
	}
}

func (md *model) forget(now time.Time, self member.ID, after, graves time.Duration) []member.ID {
	md.expire(now, graves)
	var out []member.ID
	for id, e := range md.members {
		if id != self && e.m.State.Gone() && !now.Before(e.gone.Add(after)) {
			out = append(out, id)
			delete(md.members, id)
			if graves > 0 {
				md.graves[id] = modelGrave{m: e.m, at: now}
			}
		}
	}
	slices.Sort(out)
	return out
}

func (md *model) forgetOldest(now time.Time, self member.ID) (member.ID, bool) {
	oldest := member.ID("")
	for _, id := range slices.Sorted(maps.Keys(md.members)) {
		if e := md.members[id]; id != self && e.m.State.Gone() && (oldest == "" || e.gone.Before(md.members[oldest].gone)) {
			oldest = id
		}
	}
	if oldest == "" {
		return "", false
	}
	md.graves[oldest] = modelGrave{m: md.members[oldest].m, at: now}
	delete(md.members, oldest)
	return oldest, true
}

// Generated sequences of entries, drops, lifts, expiry and forgetting, against the
// model: after every step the view holds the same members and the same
// graves, held or not, admits the same entries, and each call answers as
// the model does.
func TestGravesAgainstAModel(t *testing.T) {
	r := rand.New(rand.NewPCG(13, 14))
	ids := []member.ID{"self", "a", "b", "c", "d", "e", ""}
	const after = 30 * time.Second
	for run := range 300 {
		self := member.Member{ID: "self"}
		v := New(self)
		md := &model{members: map[member.ID]modelEntry{"self": {m: self}}, graves: map[member.ID]modelGrave{}}
		now := t0
		for step := range 300 {
			now = now.Add(time.Duration(r.IntN(8)) * time.Second)
			graves := time.Duration(r.IntN(3)) * time.Minute
			id := ids[1+r.IntN(len(ids)-2)]
			var what string
			switch r.IntN(10) {
			case 0, 1:
				if r.IntN(5) == 0 {
					id = ids[[]int{0, len(ids) - 1}[r.IntN(2)]]
				}
				what = fmt.Sprintf("Drop(%q, %v)", id, graves)
				gm, gok := v.Drop(now, id, graves)
				wm, wok := md.drop(now, "self", id, graves)
				if gok != wok || gok && gm != wm {
					t.Fatalf("run %d step %d: %s = %v, %v; the model %v, %v", run, step, what, gm, gok, wm, wok)
				}
			case 2:
				what = fmt.Sprintf("Lift(%q)", id)
				v.Lift(id)
				if g, ok := md.graves[id]; ok && g.held {
					delete(md.graves, id)
				}
			case 3:
				what = fmt.Sprintf("Forget(%v)", graves)
				var got []member.ID
				for _, m := range v.Forget(now, after, graves) {
					got = append(got, m.ID)
				}
				if want := md.forget(now, "self", after, graves); !slices.Equal(got, want) {
					t.Fatalf("run %d step %d: %s forgot %v, the model %v", run, step, what, got, want)
				}
			case 4:
				if r.IntN(2) == 0 {
					what = fmt.Sprintf("Expire(%v)", graves)
					v.Expire(now, graves)
					md.expire(now, graves)
					break
				}
				what = "ForgetOldest"
				m, ok := v.ForgetOldest(now)
				want, wok := md.forgetOldest(now, "self")
				if ok != wok || ok && m.ID != want {
					t.Fatalf("run %d step %d: ForgetOldest = %v, %v; the model %v, %v", run, step, m.ID, ok, want, wok)
				}
			default:
				m := member.Member{ID: id, Addr: string(id), Incarnation: r.Uint64N(4), State: member.State(r.IntN(4))}
				what = fmt.Sprintf("Apply(%v)", m)
				if got, want := v.Apply(now, msg.Update{Member: m}), md.apply(now, m); got != want {
					t.Fatalf("run %d step %d: %s = %v, the model %v", run, step, what, got, want)
				}
			}
			if err := v.Check(); err != nil {
				t.Fatalf("run %d step %d, after %s: %v", run, step, what, err)
			}
			for _, id := range ids[:len(ids)-1] {
				e, ok := md.members[id]
				if m, got := v.Get(id); got != ok || ok && m != e.m {
					t.Fatalf("run %d step %d, after %s: %s in the view %v, %v; the model %v, %v", run, step, what, id, m, got, e.m, ok)
				}
				g, buried := md.graves[id]
				if m, got := v.Buried(id); got != buried || buried && (m != g.m || v.graves[id].held != g.held) {
					t.Fatalf("run %d step %d, after %s: %s buried %v, %v, held %v; the model %v, %v, held %v", run, step, what, id, m, got, v.graves[id].held, g.m, buried, g.held)
				}
				alive := member.Member{ID: id, Incarnation: 9}
				_, known := md.members[id]
				if want := !known && (!buried || !g.held && member.Overrides(alive, g.m)); id != "self" && v.Admits(alive) != want {
					t.Fatalf("run %d step %d, after %s: Admits(%s) = %v, the model %v", run, step, what, id, !want, want)
				}
			}
		}
	}
}
