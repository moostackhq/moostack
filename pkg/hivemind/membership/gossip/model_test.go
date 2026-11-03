package gossip

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/moostackhq/moostack/pkg/hivemind/membership/member"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/msg"
)

// modelItem is the model's queued update and how often it went out.
type modelItem struct {
	u    msg.Update
	sent int
}

// The queue against a model, over random adds and takes: it holds exactly
// the updates the model holds, sends each at most limit times, never more
// than the budget, and never a more-sent update while a less-sent one that
// fits waits.
func TestQueueAgainstAModel(t *testing.T) {
	r := rand.New(rand.NewPCG(11, 12))
	for run := range 300 {
		q, model := New(), map[key]*modelItem{}
		limit := 1 + r.IntN(4)
		for step := range 200 {
			if r.IntN(3) > 0 {
				u := msg.Update{Member: member.Member{ID: member.ID(fmt.Sprint(r.IntN(6))), Incarnation: r.Uint64N(3), State: member.State(r.IntN(3))}}
				if u.Member.State == member.Suspect {
					u.By = member.ID(fmt.Sprint("b", r.IntN(3)))
				}
				q.Add(u)
				k := key{id: u.Member.ID}
				if u.Member.State == member.Suspect {
					k.by = u.By
				}
				for old, it := range model {
					if old.id == k.id && (k.by == "" || old.by == "" || old == k || it.u.Member.Incarnation != u.Member.Incarnation) {
						delete(model, old)
					}
				}
				model[k] = &modelItem{u: u}
			} else {
				budget := r.IntN(80)
				got := q.Take(budget, msg.MaxUpdates, limit, sizer{}, 1)
				size := 0
				for _, u := range got {
					size += sizer{}.UpdateSize(u, 1)
					k := key{id: u.Member.ID}
					if u.Member.State == member.Suspect {
						k.by = u.By
					}
					it, ok := model[k]
					if !ok || it.u != u {
						t.Fatalf("run %d step %d: took %v, which the model doesn't hold", run, step, u)
					}
					it.sent++
				}
				if size > budget {
					t.Fatalf("run %d step %d: %d bytes of %d", run, step, size, budget)
				}
				// No update went out sent more often, before it was taken,
				// than one left behind that would still have fit.
				left := budget - size
				for _, u := range got {
					before := model[keyOf(u)].sent - 1
					for k, it := range model {
						if it.sent < before && (sizer{}).UpdateSize(it.u, 1) <= left && !taken(got, k) {
							t.Fatalf("run %d step %d: took %v, sent %d times, past %v, sent %d", run, step, u, before, it.u, it.sent)
						}
					}
				}
				for k, it := range model {
					if it.sent >= limit {
						delete(model, k)
					}
				}
			}
			if q.Len() != len(model) {
				t.Fatalf("run %d step %d: queue %d, model %d", run, step, q.Len(), len(model))
			}
		}
	}
}

func keyOf(u msg.Update) key {
	k := key{id: u.Member.ID}
	if u.Member.State == member.Suspect {
		k.by = u.By
	}
	return k
}

func taken(got []msg.Update, k key) bool {
	for _, u := range got {
		if keyOf(u) == k {
			return true
		}
	}
	return false
}
