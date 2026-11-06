package pushpull

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/moostackhq/moostack/pkg/hivemind/membership/member"
)

// Generated sequences of seeds kept, joins, forgotten seeds, ticks and syncs that
// taught something, against a model of the seeds, of whether the scheduler
// is joining and of the syncs left to bring forward after a join: after
// every step the same seeds are found, at their addresses and by their
// IDs, a join retries only the model's seeds, and it is joining exactly
// when the model is; a sync that taught something brings the next within
// RetryEvery give or take half while the model has one left to bring
// forward and isn't joining, and leaves Due as it was otherwise.
func TestSeedsAgainstAModel(t *testing.T) {
	r := rand.New(rand.NewPCG(15, 16))
	self := member.Member{ID: "me", Addr: "me-addr"}
	pool := []member.Member{self, {ID: "me", Addr: "x"}, {ID: "o", Addr: "me-addr"}}
	for i := range 5 {
		id := member.ID(fmt.Sprint("s", i))
		pool = append(pool, member.Member{ID: id, Addr: fmt.Sprint("a", i%3)})
	}
	for run := range 300 {
		s := New(Config{Every: 30 * time.Second, SeedEvery: 2, RetryEvery: time.Second}, rand.New(rand.NewPCG(uint64(run), 1)), t0)
		var seeds []member.Member
		joining := false
		catchUp := 0
		now := t0
		for step := range 100 {
			now = now.Add(time.Duration(r.IntN(3000)) * time.Millisecond)
			var what string
			switch r.IntN(6) {
			case 4:
				var in []member.Member
				for range r.IntN(4) {
					in = append(in, pool[r.IntN(len(pool))])
				}
				what = fmt.Sprintf("Keep(%v)", in)
				kept := s.Keep(self, in)
				seeds = slices.DeleteFunc(slices.Clone(in), func(m member.Member) bool { return m.ID == self.ID || m.Addr == self.Addr })
				if len(seeds) == 0 {
					joining, catchUp = false, 0
				}
				if !slices.Equal(kept, seeds) {
					t.Fatalf("run %d step %d: %s kept %v, the model %v", run, step, what, kept, seeds)
				}
			case 0:
				var in []member.Member
				for range r.IntN(4) {
					in = append(in, pool[r.IntN(len(pool))])
				}
				what = fmt.Sprintf("Reach(%v)", in)
				kept := s.Keep(self, in)
				got := s.Reach(now)
				seeds = slices.DeleteFunc(slices.Clone(in), func(m member.Member) bool { return m.ID == self.ID || m.Addr == self.Addr })
				joining = len(seeds) > 0
				catchUp = 0
				if !slices.Equal(kept, seeds) {
					t.Fatalf("run %d step %d: %s kept %v, the model %v", run, step, what, kept, seeds)
				}
				if len(got) != min(joinSeeds, len(seeds)) {
					t.Fatalf("run %d step %d: %s syncs %v of %v", run, step, what, got, seeds)
				}
				for _, m := range got {
					if !slices.Contains(seeds, m) {
						t.Fatalf("run %d step %d: %s syncs %v, not a seed of the model's %v", run, step, what, m, seeds)
					}
				}
			case 1:
				id := pool[r.IntN(len(pool))].ID
				what = fmt.Sprintf("Forget(%s)", id)
				s.Forget(id)
				seeds = slices.DeleteFunc(seeds, func(m member.Member) bool { return m.ID == id })
				joining = joining && len(seeds) > 0
			case 2:
				what = "Learned"
				before := s.Due()
				s.Learned(now)
				if catchUp > 0 && !joining {
					catchUp--
					if d := s.Due().Sub(now); d > 3*time.Second/2 {
						t.Fatalf("run %d step %d: %s brought the next sync %v on, want within RetryEvery give or take half", run, step, what, d)
					}
				} else if !s.Due().Equal(before) {
					t.Fatalf("run %d step %d: %s moved Due from %v to %v with %d syncs left to bring forward, joining %v", run, step, what, before, s.Due(), catchUp, joining)
				}
			default:
				others := r.IntN(3) == 0
				what = fmt.Sprintf("Tick(others %v)", others)
				got := s.Tick(now, others, none)
				if len(got) > joinSeeds+1 {
					t.Fatalf("run %d step %d: %s synced with %v, more than a few", run, step, what, got)
				}
				for _, m := range got {
					if !slices.Contains(seeds, m) {
						t.Fatalf("run %d step %d: %s synced %v, not a seed of the model's %v", run, step, what, m, seeds)
					}
				}
				if others && joining {
					joining = false
					catchUp = maxCatchUp
				}
			}
			if s.Joining() != joining {
				t.Fatalf("run %d step %d, after %s: joining %v, the model %v", run, step, what, s.Joining(), joining)
			}
			for _, m := range pool {
				i := slices.IndexFunc(seeds, func(x member.Member) bool { return x.Addr == m.Addr && x.ID == m.ID })
				if got, ok := s.Seed(m.ID, m.Addr); ok != (i >= 0) || ok && got != seeds[i] {
					t.Fatalf("run %d step %d, after %s: Seed(%s, %s) = %v, %v; the model's seeds %v", run, step, what, m.ID, m.Addr, got, ok, seeds)
				}
				first := slices.IndexFunc(seeds, func(x member.Member) bool { return x.Addr == m.Addr })
				if got, ok := s.Seed("", m.Addr); ok != (first >= 0) || ok && got != seeds[first] {
					t.Fatalf("run %d step %d, after %s: Seed(\"\", %s) = %v, %v; the model's seeds %v", run, step, what, m.Addr, got, ok, seeds)
				}
			}
		}
	}
}
