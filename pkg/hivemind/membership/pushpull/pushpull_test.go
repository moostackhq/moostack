package pushpull

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/moostackhq/moostack/pkg/hivemind/membership/member"
)

var t0 = time.Unix(1000, 0)

func sched() *Scheduler {
	return New(Config{Every: 30 * time.Second, SeedEvery: 4, RetryEvery: 2 * time.Second}, rand.New(rand.NewPCG(1, 1)), t0)
}

// named returns the seeds at addrs, each named by its address.
func named(addrs ...string) []member.Member {
	var out []member.Member
	for _, a := range addrs {
		out = append(out, member.Member{ID: member.ID(a), Addr: a})
	}
	return out
}

func addrs(ms []member.Member) []string {
	var out []string
	for _, m := range ms {
		out = append(out, m.Addr)
	}
	return out
}

func join(s *Scheduler, now time.Time, seeds []member.Member) []member.Member {
	s.Keep(me, seeds)
	return s.Reach(now)
}

// me is the scheduler's own member.
var me = member.Member{ID: "me", Addr: "me"}

func none() (member.Member, bool) { return member.Member{}, false }

func one(m member.Member) func() (member.Member, bool) {
	return func() (member.Member, bool) { return m, true }
}

// A join attempt takes a few seeds at random, each once.
func TestAJoinAttemptTakesAFewSeeds(t *testing.T) {
	s := sched()
	seeds := named("a", "b", "c", "d", "e", "f", "g", "h")
	seen := map[string]bool{}
	for range 50 {
		got := addrs(join(s, t0, seeds))
		if len(got) != joinSeeds {
			t.Fatalf("Reach = %v", got)
		}
		slices.Sort(got)
		if len(slices.Compact(got)) != joinSeeds {
			t.Fatalf("a seed twice in %v", got)
		}
		for _, a := range got {
			seen[a] = true
		}
	}
	if len(seen) != len(seeds) {
		t.Fatalf("drew only %v", seen)
	}
}

// Keeping seeds starts no join and brings no sync forward.
func TestKeepingSeedsStartsNoJoin(t *testing.T) {
	s := sched()
	due := s.Due()
	s.Keep(me, named("a", "b"))
	if s.Joining() || !s.Due().Equal(due) {
		t.Fatalf("joining %v, due moved from %v to %v", s.Joining(), due, s.Due())
	}
}

// A join with no seeds ends one still trying earlier seeds: nothing is
// left to retry, and the scheduler wakes for nothing.
func TestJoiningNoSeedsEndsTheJoin(t *testing.T) {
	s := New(Config{RetryEvery: 2 * time.Second}, rand.New(rand.NewPCG(1, 1)), t0)
	join(s, t0, named("a"))
	if !s.Joining() {
		t.Fatal("not joining")
	}
	if got := addrs(join(s, t0.Add(time.Second), named("me"))); len(got) != 0 {
		t.Fatalf("Reach = %v", got)
	}
	if s.Joining() || !s.Due().IsZero() {
		t.Fatalf("still joining, due %v, with no seeds", s.Due())
	}
	if got := addrs(s.Tick(t0.Add(time.Hour), false, none)); len(got) != 0 {
		t.Fatalf("Tick = %v with no seeds", got)
	}
}

func TestJoinRetriesUntilAMemberIsKnown(t *testing.T) {
	// Periodic syncs off: Due is the next try.
	s := New(Config{RetryEvery: 2 * time.Second}, rand.New(rand.NewPCG(1, 1)), t0)
	if got := addrs(join(s, t0, named("me", "a", "b"))); !slices.Equal(got, []string{"a", "b"}) {
		t.Fatalf("Reach = %v", got)
	}
	// Retries back off from RetryEvery, each give or take half, up to
	// maxBackoff times it.
	now, back := t0, 2*time.Second
	for i := range 6 {
		due := s.Due()
		if d := due.Sub(now); d < back/2 || d > back*3/2 {
			t.Fatalf("try %d due %v after the last, want %v give or take half", i, d, back)
		}
		if got := addrs(s.Tick(due.Add(-time.Nanosecond), false, none)); len(got) != 0 {
			t.Fatalf("retried early: %v", got)
		}
		now = due
		got := addrs(s.Tick(now, false, none))
		slices.Sort(got)
		if !slices.Equal(got, []string{"a", "b"}) {
			t.Fatalf("retry %d = %v", i, got)
		}
		back = min(2*back, maxBackoff*2*time.Second)
	}
	s.Tick(now.Add(time.Second), true, one(member.Member{ID: "a", Addr: "a"}))
	if s.Joining() {
		t.Fatal("still joining after hearing of a member")
	}
	if got := addrs(s.Tick(now.Add(time.Hour), false, none)); len(got) != 0 {
		t.Fatalf("a joined node retried: %v", got)
	}
}

// Nodes started together try their seeds at spread times.
func TestJoinRetriesAreSpread(t *testing.T) {
	seen := map[time.Time]bool{}
	for seed := range uint64(20) {
		s := New(Config{Every: 30 * time.Second, RetryEvery: 2 * time.Second}, rand.New(rand.NewPCG(seed, 0)), t0)
		join(s, t0, named("seed"))
		seen[s.Due()] = true
	}
	if len(seen) < 15 {
		t.Fatalf("20 nodes retry at %d times", len(seen))
	}
}

// With periodic syncs off, a node that isn't joining has nothing due.
func TestSyncsOff(t *testing.T) {
	s := New(Config{RetryEvery: time.Second}, rand.New(rand.NewPCG(1, 1)), t0)
	if !s.Due().IsZero() {
		t.Fatalf("Due = %v with syncs off", s.Due())
	}
	if got := addrs(s.Tick(t0.Add(time.Hour), true, one(member.Member{ID: "x", Addr: "x"}))); len(got) != 0 {
		t.Fatalf("synced with syncs off: %v", got)
	}
	join(s, t0, named("seed"))
	if s.Due().IsZero() {
		t.Fatal("a join has nothing due")
	}
}

func TestPeriodicSyncsGoToSeedsEverySoOften(t *testing.T) {
	s := sched()
	join(s, t0, named("seed"))
	live := one(member.Member{ID: "x", Addr: "x"})
	var got []string
	now := s.next.Add(-30 * time.Second)
	for range 8 {
		now = now.Add(30 * time.Second)
		got = append(got, addrs(s.Tick(now, true, live))...)
	}
	want := []string{"x", "x", "x", "seed", "x", "x", "x", "seed"}
	if !slices.Equal(got, want) {
		t.Fatalf("syncs %v, want %v", got, want)
	}
}

// Nodes started together spread their first syncs over the interval.
func TestFirstSyncsAreSpread(t *testing.T) {
	seen := map[time.Time]bool{}
	for seed := range uint64(20) {
		s := New(Config{Every: 30 * time.Second}, rand.New(rand.NewPCG(seed, 0)), t0)
		if s.next.Before(t0) || s.next.After(t0.Add(30*time.Second)) {
			t.Fatalf("first sync at %v", s.next.Sub(t0))
		}
		seen[s.next] = true
	}
	if len(seen) < 15 {
		t.Fatalf("20 nodes' first syncs fall on %d times", len(seen))
	}
}

func TestShift(t *testing.T) {
	s := sched()
	join(s, t0, named("seed"))
	before := s.Due()
	s.Shift(5 * time.Second)
	if !s.Due().Equal(before.Add(5 * time.Second)) {
		t.Fatalf("Due after Shift = %v, want %v", s.Due(), before.Add(5*time.Second))
	}
}

// Without seeds, or with seed syncs off, every periodic sync goes to a
// live member.
func TestPeriodicSyncsWithoutSeedSyncs(t *testing.T) {
	for name, tc := range map[string]struct {
		seedEvery int
		seeds     []string
	}{
		"no seeds":        {4, nil},
		"seed syncs off":  {0, []string{"seed"}},
		"only self seeds": {4, []string{"me"}},
	} {
		s := New(Config{Every: 30 * time.Second, SeedEvery: tc.seedEvery, RetryEvery: 2 * time.Second}, rand.New(rand.NewPCG(1, 1)), t0)
		join(s, t0, named(tc.seeds...))
		live := one(member.Member{ID: "x", Addr: "x"})
		now := s.next
		for i := range 8 {
			if got := addrs(s.Tick(now, true, live)); !slices.Equal(got, []string{"x"}) {
				t.Fatalf("%s: sync %d went to %v", name, i, got)
			}
			now = now.Add(30 * time.Second)
		}
	}
}

// With RetryEvery 0, a join tries the next of its seeds at every tick until
// it holds a live member.
func TestJoinWithoutRetryInterval(t *testing.T) {
	s := New(Config{}, rand.New(rand.NewPCG(1, 1)), t0)
	if got := addrs(join(s, t0, named("seed"))); !slices.Equal(got, []string{"seed"}) {
		t.Fatalf("Reach = %v", got)
	}
	if got := addrs(s.Tick(t0, false, none)); !slices.Equal(got, []string{"seed"}) || !s.Joining() {
		t.Fatalf("Tick = %v, joining %v", got, s.Joining())
	}
}

// A node that has joined syncs again within RetryEvery or so, and again
// after each sync that taught it of members it didn't know, maxCatchUp
// times at most; otherwise at Every. A node that never joined keeps to
// Every.
func TestCatchUpAfterAJoin(t *testing.T) {
	s := sched()
	a := one(member.Member{ID: "a", Addr: "a"})
	join(s, t0, named("seed"))
	joined := t0.Add(10 * time.Millisecond)
	s.Tick(joined, true, a)
	soon := func(from time.Time) {
		t.Helper()
		if d := s.Due().Sub(from); d < time.Second || d > 3*time.Second {
			t.Fatalf("next sync %v after %v, want within RetryEvery give or take half", d, from)
		}
	}
	soon(joined)
	now := s.Due()
	if got := addrs(s.Tick(now, true, a)); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("the first catch-up synced with %v", got)
	}
	// One that taught nothing: the next is at Every.
	if d := s.Due().Sub(now); d != 30*time.Second {
		t.Fatalf("after a sync that taught nothing, the next is %v on, want Every", d)
	}
	for i := range maxCatchUp {
		s.Learned(now)
		soon(now)
		now = s.Due()
		// Every SeedEvery-th sync goes to a seed, catching up or not.
		if got := addrs(s.Tick(now, true, a)); len(got) != 1 {
			t.Fatalf("catch-up %d synced with %v", i, got)
		}
	}
	s.Learned(now)
	if d := s.Due().Sub(now); d != 30*time.Second {
		t.Fatalf("after %d catch-ups the next sync is %v on, want Every", maxCatchUp, d)
	}

	// Without a join, learning changes nothing.
	s = sched()
	due := s.Due()
	s.Learned(t0)
	if !s.Due().Equal(due) {
		t.Fatalf("Learned moved an unjoined node's sync from %v to %v", due, s.Due())
	}
}

// A forgotten seed is synced with no more, and is no seed: forgetting the
// last one ends the join.
func TestAForgottenSeedIsNoSeed(t *testing.T) {
	s := sched()
	join(s, t0, named("a", "b"))
	s.Forget("a")
	if _, ok := s.Seed("", "a"); ok {
		t.Fatal("a is still a seed")
	}
	if m, ok := s.Seed("", "b"); !ok || m.ID != "b" {
		t.Fatalf("Seed(b) = %v, %v", m, ok)
	}
	if got := addrs(s.Tick(s.Due(), false, none)); !slices.Equal(got, []string{"b"}) {
		t.Fatalf("retried %v, want b alone", got)
	}
	s.Forget("b")
	if s.Joining() {
		t.Fatal("still joining with no seeds")
	}
}

// A seed is found by its address, with its ID.
func TestASeedIsFoundByItsAddress(t *testing.T) {
	s := sched()
	join(s, t0, []member.Member{{ID: "s1", Addr: "x:1"}})
	if m, ok := s.Seed("", "x:1"); !ok || m.ID != "s1" {
		t.Fatalf("Seed(x:1) = %v, %v", m, ok)
	}
	if _, ok := s.Seed("", "s1"); ok {
		t.Fatal("found a seed by its ID as an address")
	}
}

// A seed is told apart by its ID among seeds at one address, and a seed at
// the node's own address, under any ID, is skipped.
func TestSeedsAtOneAddress(t *testing.T) {
	s := sched()
	got := s.Keep(member.Member{ID: "me", Addr: "here"}, []member.Member{{ID: "old", Addr: "a"}, {ID: "new", Addr: "a"}, {ID: "other", Addr: "here"}})
	if len(got) != 2 {
		t.Fatalf("Reach = %v", got)
	}
	if m, ok := s.Seed("new", "a"); !ok || m.ID != "new" {
		t.Fatalf("Seed(new, a) = %v, %v", m, ok)
	}
	if m, ok := s.Seed("", "a"); !ok || m.ID != "old" {
		t.Fatalf("Seed(\"\", a) = %v, %v", m, ok)
	}
	if _, ok := s.Seed("stranger", "a"); ok {
		t.Fatal("a stranger at a seed's address is a seed")
	}
	if _, ok := s.Seed("", "here"); ok {
		t.Fatal("kept a seed at the node's own address")
	}
}

// Forgetting the seed a node joined through ends no catch-up: the syncs
// after a join go to members, and still come within RetryEvery.
func TestForgettingTheSeedKeepsTheCatchUp(t *testing.T) {
	s := sched()
	a := one(member.Member{ID: "a", Addr: "a"})
	join(s, t0, named("seed"))
	joined := t0.Add(10 * time.Millisecond)
	s.Tick(joined, true, a)
	now := s.Due()
	s.Tick(now, true, a)
	s.Forget("seed")
	s.Learned(now)
	if d := s.Due().Sub(now); d < time.Second || d > 3*time.Second {
		t.Fatalf("after the seed was forgotten, a sync that taught something brings the next %v on, want within RetryEvery give or take half", d)
	}
}

func TestJoiningNoSeedsEndsTheCatchUp(t *testing.T) {
	s := sched()
	a := one(member.Member{ID: "a", Addr: "a"})
	join(s, t0, named("seed"))
	joined := t0.Add(10 * time.Millisecond)
	s.Tick(joined, true, a)
	now := s.Due()
	s.Tick(now, true, a)
	join(s, now, nil)
	s.Learned(now)
	if d := s.Due().Sub(now); d < 15*time.Second {
		t.Fatalf("after a Reach of no seeds, a sync that taught something brings the next %v on, want the period, 30s give or take half", d)
	}
}

func TestARetryTakesTheNextFewSeeds(t *testing.T) {
	var seeds []member.Member
	for i := range 20 {
		seeds = append(seeds, member.Member{ID: member.ID(fmt.Sprint("s", i)), Addr: fmt.Sprint("s", i)})
	}
	s := New(Config{RetryEvery: time.Second}, rand.New(rand.NewPCG(1, 2)), t0)
	tried := map[string]int{}
	for _, m := range join(s, t0, seeds) {
		tried[m.Addr]++
	}
	now := t0
	for attempt := 1; len(tried) < len(seeds); attempt++ {
		if attempt > 6 {
			t.Fatalf("%d attempts tried %d of the seeds", attempt, len(tried))
		}
		now = s.Due()
		got := s.Tick(now, false, none)
		if len(got) == 0 || len(got) > joinSeeds {
			t.Fatalf("attempt %d synced with %v", attempt, addrs(got))
		}
		if d := s.Due().Sub(now); d > 3*time.Second/2 {
			t.Fatalf("attempt %d backed off %v in its first pass", attempt, d)
		}
		for _, m := range got {
			tried[m.Addr]++
		}
	}
	for addr, n := range tried {
		if n != 1 {
			t.Fatalf("%s was tried %d times in a pass", addr, n)
		}
	}
	now = s.Due()
	if got := s.Tick(now, false, none); len(got) != joinSeeds {
		t.Fatalf("the next pass began with %v", addrs(got))
	}
	if d := s.Due().Sub(now); d <= 3*time.Second/2 {
		t.Fatalf("the next attempt is %v away, want backed off after a pass found nobody", d)
	}
}

func TestAForgottenSeedLeavesThePass(t *testing.T) {
	seeds := []member.Member{{ID: "a", Addr: "a"}, {ID: "b", Addr: "b"}, {ID: "c", Addr: "c"}, {ID: "d", Addr: "d"}, {ID: "e", Addr: "e"}}
	s := New(Config{RetryEvery: time.Second}, rand.New(rand.NewPCG(3, 4)), t0)
	first := join(s, t0, seeds)
	for _, m := range seeds {
		if !slices.Contains(first, m) {
			s.Forget(m.ID)
			if got := s.Tick(s.Due(), false, none); slices.Contains(got, m) {
				t.Fatalf("%s was forgotten and tried in %v", m.ID, addrs(got))
			}
			return
		}
	}
}

func TestKeepingSeedsLeavesACatchUp(t *testing.T) {
	seeds := []member.Member{{ID: "a", Addr: "a"}}
	s := New(Config{Every: time.Hour, RetryEvery: time.Second}, rand.New(rand.NewPCG(5, 6)), t0)
	join(s, t0, seeds)
	s.Tick(t0.Add(2*time.Second), true, none)
	s.next = t0.Add(time.Hour)
	s.Keep(me, seeds)
	now := t0.Add(3 * time.Second)
	s.Learned(now)
	if d := s.Due().Sub(now); d > 3*time.Second/2 {
		t.Fatalf("a sync that taught something after a Keep left the next %v away", d)
	}
}

// A join attempt through 200 seeds, as a node's whole generation names them.
func BenchmarkReach(b *testing.B) {
	seeds := make([]member.Member, 200)
	for i := range seeds {
		seeds[i] = member.Member{ID: member.ID(fmt.Sprint("s", i)), Addr: fmt.Sprint("s", i)}
	}
	self := member.Member{ID: "self", Addr: "self"}
	s := New(Config{Every: time.Second, RetryEvery: time.Second}, rand.New(rand.NewPCG(1, 1)), t0)
	for b.Loop() {
		s.Keep(self, seeds)
		s.Reach(t0)
	}
}
