package suspicion

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/moostackhq/moostack/pkg/hivemind/membership/member"
)

type modelTimer struct {
	inc        uint64
	start      time.Time
	confirmers []member.ID
}

// The tracker against a model, over random starts, confirmations, stops,
// pauses and expiries: the same timers end at the same times.
func TestTrackerAgainstAModel(t *testing.T) {
	r := rand.New(rand.NewPCG(13, 14))
	const lo, hi, k = 2 * time.Second, 12 * time.Second, 3
	for run := range 300 {
		tr, model := New(), map[member.ID]*modelTimer{}
		now := t0
		for step := range 200 {
			id := member.ID(fmt.Sprint(r.IntN(4)))
			by := member.ID(fmt.Sprint("b", r.IntN(5)))
			inc := r.Uint64N(3)
			switch r.IntN(5) {
			case 0:
				tr.Start(now, id, inc, by, lo, hi, k)
				if m, ok := model[id]; !ok || m.inc < inc {
					model[id] = &modelTimer{inc: inc, start: now, confirmers: []member.ID{by}}
				}
			case 1:
				ok, _ := tr.Confirm(id, inc, by)
				m, has := model[id]
				want := has && m.inc == inc && !slices.Contains(m.confirmers, by) && len(m.confirmers) <= k
				if ok != want {
					t.Fatalf("run %d step %d: Confirm = %v, model %v", run, step, ok, want)
				}
				if want {
					m.confirmers = append(m.confirmers, by)
				}
			case 2:
				tr.Stop(id)
				delete(model, id)
			case 3:
				d := time.Duration(r.IntN(3000)) * time.Millisecond
				tr.Shift(d)
				for _, m := range model {
					m.start = m.start.Add(d)
				}
			case 4:
				now = now.Add(time.Duration(r.IntN(5000)) * time.Millisecond)
				var want []Expiry
				for id, m := range model {
					if !now.Before(m.start.Add(Timeout(lo, hi, k, len(m.confirmers)-1))) {
						want = append(want, Expiry{ID: id, Incarnation: m.inc})
						delete(model, id)
					}
				}
				slices.SortFunc(want, func(a, b Expiry) int {
					if a.ID < b.ID {
						return -1
					}
					return 1
				})
				if got := tr.Expired(now); !slices.Equal(got, want) {
					t.Fatalf("run %d step %d: Expired %v, model %v", run, step, got, want)
				}
			}
			for id := range model {
				if !tr.Suspected(id) {
					t.Fatalf("run %d step %d: %s missing", run, step, id)
				}
			}
		}
	}
}
