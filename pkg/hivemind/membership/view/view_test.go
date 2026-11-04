package view

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/moostackhq/moostack/pkg/hivemind/membership/member"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/msg"
)

var t0 = time.Unix(1000, 0)

func up(id member.ID, inc uint64, s member.State) msg.Update {
	return msg.Update{Member: member.Member{ID: id, Addr: string(id), Incarnation: inc, State: s}}
}

func TestApply(t *testing.T) {
	v := New(member.Member{ID: "self"})
	steps := []struct {
		u    msg.Update
		want Outcome
	}{
		{up("b", 0, member.Dead), Ignored},
		{up("b", 0, member.Alive), Changed},
		{up("b", 0, member.Alive), Stale},
		{up("b", 0, member.Suspect), Changed},
		{up("b", 0, member.Suspect), Confirmed},
		{up("b", 0, member.Alive), Stale},
		{up("b", 1, member.Alive), Changed},
		{up("b", 1, member.Dead), Changed},
		{up("b", 1, member.Suspect), Stale},
		{up("b", 2, member.Alive), Changed},
	}
	for i, s := range steps {
		if got := v.Apply(t0, s.u); got != s.want {
			t.Fatalf("step %d: Apply(%v) = %v, want %v", i, s.u.Member, got, s.want)
		}
	}
}

// Nodes that hear the same updates in any order end with the same view.
func TestOrderDoesNotMatter(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 4))
	for range 200 {
		var us []msg.Update
		for range 30 {
			id := member.ID(rune('a' + r.IntN(5)))
			us = append(us, up(id, r.Uint64N(3), member.State(r.IntN(3))))
		}
		a, b := New(member.Member{ID: "self"}), New(member.Member{ID: "self"})
		// Alive entries first.
		for _, u := range us {
			u.Member.State = member.Alive
			u.Member.Incarnation = 0
			a.Apply(t0, u)
			b.Apply(t0, u)
		}
		for _, u := range us {
			a.Apply(t0, u)
		}
		r.Shuffle(len(us), func(i, j int) { us[i], us[j] = us[j], us[i] })
		for _, u := range us {
			b.Apply(t0, u)
		}
		if !slices.Equal(a.All(), b.All()) {
			t.Fatalf("views differ:\n%v\n%v", a.All(), b.All())
		}
	}
}

func TestForget(t *testing.T) {
	v := New(member.Member{ID: "self"})
	v.Apply(t0, up("b", 0, member.Alive))
	v.Apply(t0, up("c", 0, member.Alive))
	v.Apply(t0.Add(time.Second), up("b", 0, member.Dead))
	if got := v.NextForget(time.Minute); !got.Equal(t0.Add(time.Second + time.Minute)) {
		t.Fatalf("NextForget = %v", got)
	}
	if got := v.Forget(t0.Add(time.Minute), time.Minute, 0); len(got) != 0 {
		t.Fatalf("forgot %v too early", got)
	}
	if got := v.Forget(t0.Add(time.Minute+time.Second), time.Minute, 0); len(got) != 1 || got[0].ID != "b" {
		t.Fatalf("Forget = %v", got)
	}
	if _, ok := v.Get("b"); ok {
		t.Fatal("b still in the view")
	}
	if !slices.Equal(v.IDs(), []member.ID{"c", "self"}) {
		t.Fatalf("IDs = %v", v.IDs())
	}
	// Once forgotten, news of b's death no longer brings it back.
	if got := v.Apply(t0, up("b", 0, member.Dead)); got != Ignored {
		t.Fatalf("Apply after Forget = %v", got)
	}
}

func TestCommon(t *testing.T) {
	v := New(member.Member{ID: "self", Caps: member.Of(member.CapNack, member.CapNack+1)})
	v.Apply(t0, msg.Update{Member: member.Member{ID: "b", Caps: member.Of(member.CapNack)}})
	if got := v.Common(); got != member.Of(member.CapNack) {
		t.Fatalf("Common = %b", got)
	}
	v.Apply(t0, msg.Update{Member: member.Member{ID: "b", State: member.Dead}})
	if got := v.Common(); got != member.Of(member.CapNack, member.CapNack+1) {
		t.Fatalf("Common after b died = %b", got)
	}
}

// The view's running counts agree with counting its entries, whatever
// order updates, refutations and forgetting come in.
func TestCountsMatchTheEntries(t *testing.T) {
	r := rand.New(rand.NewPCG(9, 9))
	for range 200 {
		v := New(member.Member{ID: "self"})
		now := t0
		for range 200 {
			now = now.Add(time.Duration(r.IntN(10)) * time.Second)
			switch r.IntN(10) {
			case 0:
				v.Forget(now, 30*time.Second, 0)
			case 1:
				v.Drop(now, member.ID(rune('a'+r.IntN(8))), time.Duration(r.IntN(2))*time.Minute)
			case 2:
				v.Lift(member.ID(rune('a' + r.IntN(8))))
			default:
				id := member.ID(rune('a' + r.IntN(8)))
				v.Apply(now, up(id, r.Uint64N(4), member.State(r.IntN(4))))
			}
			if err := v.Check(); err != nil {
				t.Fatal(err)
			}
			live, next := 0, time.Time{}
			for _, m := range v.All() {
				if !m.State.Gone() {
					live++
				} else if e := v.members[m.ID]; next.IsZero() || e.gone.Add(30*time.Second).Before(next) {
					next = e.gone.Add(30 * time.Second)
				}
			}
			if v.Live() != live {
				t.Fatalf("Live %d, counted %d", v.Live(), live)
			}
			if got := v.NextForget(30 * time.Second); !got.Equal(next) {
				t.Fatalf("NextForget %v, computed %v", got, next)
			}
		}
	}
}

func TestRandom(t *testing.T) {
	v := New(member.Member{ID: "self"})
	for _, id := range []member.ID{"a", "b", "c"} {
		v.Apply(t0, up(id, 0, member.Alive))
	}
	r := rand.New(rand.NewPCG(1, 1))
	seen := map[member.ID]bool{}
	for range 100 {
		m, ok := v.Random(r, 4, func(m member.Member) bool { return m.ID != "self" && m.ID != "b" })
		if !ok || m.ID == "self" || m.ID == "b" {
			t.Fatalf("Random = %v, %v", m, ok)
		}
		seen[m.ID] = true
	}
	if len(seen) != 2 {
		t.Fatalf("drew %v", seen)
	}
	if _, ok := v.Random(r, 4, func(member.Member) bool { return false }); ok {
		t.Fatal("drew a member nothing qualifies")
	}
}

// A view's own entry gone, as when the node leaves, is never forgotten and
// never due to be.
func TestSelfGoneIsNotForgotten(t *testing.T) {
	v := New(member.Member{ID: "self"})
	v.SetSelf(member.Member{ID: "self", State: member.Left})
	if next := v.NextForget(time.Minute); !next.IsZero() {
		t.Fatalf("NextForget = %v", next)
	}
	if v.Live() != 0 || len(v.Forget(t0.Add(time.Hour), time.Minute, 0)) != 0 {
		t.Fatalf("Live %d", v.Live())
	}
}

func TestAt(t *testing.T) {
	v := New(member.Member{ID: "self", Addr: "s"})
	v.Apply(t0, msg.Update{Member: member.Member{ID: "a", Addr: "x"}})
	if m, ok := v.At("x"); !ok || m.ID != "a" {
		t.Fatalf("At(x) = %v, %v", m, ok)
	}
	v.Apply(t0, msg.Update{Member: member.Member{ID: "a", Addr: "y", Incarnation: 1}})
	if _, ok := v.At("x"); ok {
		t.Fatal("the old address still finds a")
	}
	if m, ok := v.At("y"); !ok || m.ID != "a" {
		t.Fatalf("At(y) = %v, %v", m, ok)
	}
	v.Apply(t0, msg.Update{Member: member.Member{ID: "a", Addr: "y", Incarnation: 1, State: member.Dead}})
	v.Forget(t0.Add(time.Hour), time.Minute, 0)
	if _, ok := v.At("y"); ok {
		t.Fatal("a forgotten member is still found by address")
	}
}

// A forgotten member leaves a grave: its entries no newer than its last
// are ignored, so a node that slept through its death can't bring it back,
// but a newer one, from a restart, is taken.
func TestAGraveKeepsTheForgottenOut(t *testing.T) {
	v := New(member.Member{ID: "self"})
	v.Apply(t0, up("x", 4, member.Alive))
	v.Apply(t0, up("x", 4, member.Dead))
	if got := v.Forget(t0.Add(time.Hour), time.Minute, 24*time.Hour); len(got) != 1 {
		t.Fatalf("Forget = %v", got)
	}
	if got := v.Apply(t0.Add(time.Hour), up("x", 4, member.Alive)); got != Ignored {
		t.Fatalf("a stale alive entry of a forgotten member: %v", got)
	}
	if g, ok := v.Buried("x"); !ok || g.State != member.Dead || g.Incarnation != 4 {
		t.Fatalf("Buried = %v, %v", g, ok)
	}
	if got := v.Apply(t0.Add(time.Hour), up("x", 5, member.Alive)); got != Changed {
		t.Fatalf("a newer entry of a forgotten member: %v", got)
	}
	if _, ok := v.Buried("x"); ok {
		t.Fatal("the grave outlived the member's return")
	}
}

// A newer tombstone of a forgotten member keeps its grave, raised to the
// newer entry, and says so, to be passed on: entries older than either are
// still ignored.
func TestANewerTombstoneKeepsTheGrave(t *testing.T) {
	v := New(member.Member{ID: "self"})
	v.Apply(t0, up("x", 4, member.Alive))
	v.Apply(t0, up("x", 4, member.Dead))
	v.Forget(t0.Add(time.Hour), time.Minute, 24*time.Hour)
	if got := v.Apply(t0.Add(time.Hour), up("x", 6, member.Left)); got != Reburied {
		t.Fatalf("a newer tombstone of a forgotten member: %v", got)
	}
	if g, ok := v.Buried("x"); !ok || g.State != member.Left || g.Incarnation != 6 {
		t.Fatalf("Buried = %v, %v", g, ok)
	}
	if got := v.Apply(t0.Add(time.Hour), up("x", 5, member.Alive)); got != Ignored {
		t.Fatalf("an alive entry older than the newer tombstone: %v", got)
	}
	if got := v.Apply(t0.Add(time.Hour), up("x", 7, member.Alive)); got != Changed {
		t.Fatalf("an alive entry newer than the tombstone: %v", got)
	}
	// The grave kept its age: a tombstone doesn't make it outlive its time.
	v.Apply(t0.Add(time.Hour), up("x", 7, member.Dead))
	v.Forget(t0.Add(2*time.Hour), time.Minute, 24*time.Hour)
	v.Apply(t0.Add(2*time.Hour), up("x", 8, member.Dead))
	if v.Forget(t0.Add(26*time.Hour), time.Minute, 24*time.Hour); len(v.graves) != 0 {
		t.Fatalf("graves %v outlived their time", v.graves)
	}
}

// Graves dug to make room in a full view count against maxGraves too, and
// so does churn that forgets one member over and over: the graves and the
// order they are kept in stay bounded.
func TestGravesFromMakingRoomAreCapped(t *testing.T) {
	v := New(member.Member{ID: "self"})
	for i := range 3 * maxGraves {
		id := member.ID(fmt.Sprintf("m%05d", i))
		at := t0.Add(time.Duration(i) * time.Millisecond)
		v.Apply(at, up(id, 1, member.Alive))
		v.Apply(at, up(id, 1, member.Dead))
		if _, ok := v.ForgetOldest(at); !ok {
			t.Fatal("forgot nothing")
		}
		// The same member forgotten again and again.
		v.Apply(at, up("churn", uint64(2*i+1), member.Alive))
		v.Apply(at, up("churn", uint64(2*i+1), member.Dead))
		v.ForgetOldest(at)
	}
	if len(v.graves) != maxGraves {
		t.Fatalf("%d graves kept, want %d", len(v.graves), maxGraves)
	}
	if len(v.order) > 2*maxGraves+64 {
		t.Fatalf("%d graves in order for %d kept", len(v.order), len(v.graves))
	}
	if _, ok := v.Buried(member.ID(fmt.Sprintf("m%05d", 3*maxGraves-1))); !ok {
		t.Fatal("dropped the newest grave")
	}
	if _, ok := v.Buried("m00000"); ok {
		t.Fatal("kept the oldest grave")
	}
}

// Unburied reports each grave that went, expired or over the cap, once.
func TestUnburiedReportsGravesThatWent(t *testing.T) {
	v := New(member.Member{ID: "self"})
	v.Apply(t0, up("x", 1, member.Alive))
	v.Apply(t0, up("x", 1, member.Dead))
	v.Forget(t0.Add(time.Hour), time.Minute, time.Hour)
	if got := v.Unburied(); len(got) != 0 {
		t.Fatalf("Unburied = %v with the grave kept", got)
	}
	v.Forget(t0.Add(2*time.Hour), time.Minute, time.Hour)
	if got := v.Unburied(); !slices.Equal(got, []member.ID{"x"}) {
		t.Fatalf("Unburied = %v, want x", got)
	}
	if got := v.Unburied(); len(got) != 0 {
		t.Fatalf("Unburied = %v a second time", got)
	}
	for i := range maxGraves + 1 {
		id := member.ID(fmt.Sprintf("m%05d", i))
		v.Apply(t0, up(id, 1, member.Alive))
		v.Apply(t0, up(id, 1, member.Dead))
		v.ForgetOldest(t0)
	}
	if got := v.Unburied(); !slices.Equal(got, []member.ID{"m00000"}) {
		t.Fatalf("Unburied = %v, want the grave over the cap", got)
	}
}

func TestGravesExpire(t *testing.T) {
	v := New(member.Member{ID: "self"})
	v.Apply(t0, up("x", 1, member.Alive))
	v.Apply(t0, up("x", 1, member.Dead))
	v.Forget(t0.Add(time.Minute), time.Minute, time.Hour)
	if _, ok := v.Grave(rand.New(rand.NewPCG(1, 1))); !ok {
		t.Fatal("no grave to draw")
	}
	v.Forget(t0.Add(2*time.Hour), time.Minute, time.Hour)
	if _, ok := v.Buried("x"); ok {
		t.Fatal("a grave outlived GraveTimeout")
	}
	if got := v.Apply(t0.Add(2*time.Hour), up("x", 1, member.Alive)); got != Changed {
		t.Fatalf("after its grave expired: %v", got)
	}
}

// Two members at one address, one that died there and one that started
// there since: the address finds the live one, and forgetting the dead one
// keeps the live one found.
func TestAtPrefersTheLiveMemberAtAnAddress(t *testing.T) {
	v := New(member.Member{ID: "self"})
	v.Apply(t0, msg.Update{Member: member.Member{ID: "x", Addr: "h:1"}})
	v.Apply(t0, msg.Update{Member: member.Member{ID: "y", Addr: "h:1"}})
	v.Apply(t0, msg.Update{Member: member.Member{ID: "x", Addr: "h:1", State: member.Suspect}})
	if m, _ := v.At("h:1"); m.ID != "y" {
		t.Fatalf("At = %v, want y", m)
	}
	v.Apply(t0, msg.Update{Member: member.Member{ID: "x", Addr: "h:1", State: member.Dead}})
	v.Forget(t0.Add(time.Hour), time.Minute, 0)
	if m, ok := v.At("h:1"); !ok || m.ID != "y" {
		t.Fatalf("At after x was forgotten = %v, %v", m, ok)
	}
	if len(v.byAddr) != 1 {
		t.Fatalf("index %v", v.byAddr)
	}
}

func TestCommonWithNoLiveMember(t *testing.T) {
	v := New(member.Member{ID: "self", Caps: member.Of(member.CapNack)})
	v.SetSelf(member.Member{ID: "self", State: member.Left, Caps: member.Of(member.CapNack)})
	if got := v.Common(); got != 0 {
		t.Fatalf("Common with nobody live = %b", got)
	}
	v.SetSelf(member.Member{ID: "other"})
	if _, ok := v.Get("other"); ok {
		t.Fatal("SetSelf took another member's entry")
	}
}

func TestForgetOldest(t *testing.T) {
	v := New(member.Member{ID: "self"})
	if _, ok := v.ForgetOldest(t0); ok {
		t.Fatal("forgot with nobody gone")
	}
	v.Apply(t0, up("a", 0, member.Alive))
	v.Apply(t0, up("b", 0, member.Alive))
	v.Apply(t0.Add(time.Second), up("b", 0, member.Dead))
	v.Apply(t0.Add(2*time.Second), up("a", 0, member.Dead))
	if m, ok := v.ForgetOldest(t0.Add(3 * time.Second)); !ok || m.ID != "b" || m.State != member.Dead {
		t.Fatalf("ForgetOldest = %v, %v; want b's entry", m, ok)
	}
	if _, ok := v.Get("b"); ok {
		t.Fatal("kept b, gone longest")
	}
	if _, ok := v.Buried("b"); !ok {
		t.Fatal("b left no grave")
	}
	if got := v.NextForget(time.Minute); !got.Equal(t0.Add(2*time.Second + time.Minute)) {
		t.Fatalf("NextForget = %v", got)
	}
}

// Forgetting with no grave time leaves no grave behind: the member is gone
// from the view and from its memory alike.
func TestForgetWithoutGravesLeavesNone(t *testing.T) {
	v := New(member.Member{ID: "self"})
	v.Apply(t0, up("x", 1, member.Alive))
	v.Apply(t0, up("x", 1, member.Dead))
	if got := v.Forget(t0.Add(time.Hour), time.Minute, 0); len(got) != 1 {
		t.Fatalf("Forget = %v", got)
	}
	if g, ok := v.Buried("x"); ok {
		t.Fatalf("a grave %v with no grave time", g)
	}
	if _, ok := v.Grave(rand.New(rand.NewPCG(1, 1))); ok {
		t.Fatal("a grave to draw with no grave time")
	}
}

// The graves are capped at maxGraves, and the oldest go first: among
// graves dug at the same time, the one with the lowest ID.
func TestGravesAreCapped(t *testing.T) {
	v := New(member.Member{ID: "self"})
	id := func(i int) member.ID { return member.ID(fmt.Sprintf("m%05d", i)) }
	// One member forgotten first, then maxGraves+1 at once: two graves too
	// many, the older one and the lowest ID of the rest.
	v.Apply(t0, up("old", 1, member.Alive))
	v.Apply(t0, up("old", 1, member.Dead))
	v.Forget(t0.Add(time.Hour), time.Minute, 24*time.Hour)
	for i := range maxGraves + 1 {
		v.Apply(t0, up(id(i), 1, member.Alive))
		v.Apply(t0.Add(time.Hour), up(id(i), 1, member.Dead))
	}
	v.Forget(t0.Add(2*time.Hour), time.Minute, 24*time.Hour)
	if len(v.graves) != maxGraves {
		t.Fatalf("%d graves kept, want %d", len(v.graves), maxGraves)
	}
	if _, ok := v.Buried("old"); ok {
		t.Fatal("kept the oldest grave")
	}
	if _, ok := v.Buried(id(0)); ok {
		t.Fatal("of graves dug at once, kept the lowest ID")
	}
	if _, ok := v.Buried(id(maxGraves)); !ok {
		t.Fatal("dropped the highest ID of graves dug at once")
	}
}

// Forgetting the member gone longest to make room leaves nobody due to be
// forgotten once it was the only one gone.
func TestForgetOldestOfOneLeavesNoneDue(t *testing.T) {
	v := New(member.Member{ID: "self"})
	v.Apply(t0, up("a", 0, member.Alive))
	v.Apply(t0, up("a", 0, member.Dead))
	if _, ok := v.ForgetOldest(t0.Add(time.Second)); !ok {
		t.Fatal("forgot nothing")
	}
	if next := v.NextForget(time.Minute); !next.IsZero() {
		t.Fatalf("NextForget = %v with nobody gone", next)
	}
	if _, ok := v.ForgetOldest(t0.Add(2 * time.Second)); ok {
		t.Fatal("forgot a second time with nobody gone")
	}
}

// A dropped member, alive or not, is out of the view at once, and its held
// grave ignores every entry of it, newer ones and tombstones included,
// until it goes; its entry is drawn for no healing probe.
func TestADroppedMemberStaysOutUntilItsGraveGoes(t *testing.T) {
	for _, st := range []member.State{member.Alive, member.Suspect, member.Dead} {
		v := New(member.Member{ID: "self"})
		v.Apply(t0, up("x", 4, member.Alive))
		v.Apply(t0, up("x", 4, st))
		m, ok := v.Drop(t0, "x", time.Hour)
		if !ok || m.ID != "x" || m.Incarnation != 4 || m.State != st {
			t.Fatalf("%v: Drop = %v, %v", st, m, ok)
		}
		if _, ok := v.Get("x"); ok || v.Live() != 1 {
			t.Fatalf("%v: still in the view, %d live", st, v.Live())
		}
		if _, ok := v.At("x"); ok {
			t.Fatalf("%v: still found at its address", st)
		}
		for _, u := range []msg.Update{up("x", 4, member.Alive), up("x", 5, member.Alive), up("x", member.MaxIncarnation, member.Alive), up("x", 9, member.Dead)} {
			if v.Admits(u.Member) {
				t.Fatalf("%v: admits %v", st, u.Member)
			}
			if got := v.Apply(t0.Add(59*time.Minute), u); got != Ignored {
				t.Fatalf("%v: Apply(%v) = %v, want Ignored", st, u.Member, got)
			}
		}
		if _, ok := v.Grave(rand.New(rand.NewPCG(1, 1))); ok {
			t.Fatalf("%v: a held grave drawn for healing", st)
		}
		if err := v.Check(); err != nil {
			t.Fatalf("%v: %v", st, err)
		}
		v.Forget(t0.Add(time.Hour), time.Minute, time.Hour)
		if _, ok := v.Buried("x"); ok {
			t.Fatalf("%v: the grave outlived its time", st)
		}
		if got := v.Apply(t0.Add(time.Hour), up("x", 4, member.Alive)); got != Changed {
			t.Fatalf("%v: after the grave went: %v", st, got)
		}
	}
}

// Dropping a buried member holds its grave from then on, and dropping a
// member the view keeps nothing of holds a grave of it; dropping self or
// the empty ID does nothing.
func TestDropHoldsAGraveAndLeavesSelf(t *testing.T) {
	v := New(member.Member{ID: "self"})
	v.Apply(t0, up("x", 1, member.Alive))
	v.Apply(t0, up("x", 1, member.Dead))
	v.Forget(t0.Add(time.Minute), time.Minute, time.Hour)
	if _, ok := v.Drop(t0.Add(30*time.Minute), "x", time.Hour); ok {
		t.Fatal("dropped a buried member again")
	}
	if got := v.Apply(t0.Add(80*time.Minute), up("x", 2, member.Alive)); got != Ignored {
		t.Fatalf("a newer entry of a held grave: %v", got)
	}
	v.Forget(t0.Add(89*time.Minute), time.Minute, time.Hour)
	if _, ok := v.Buried("x"); !ok {
		t.Fatal("the grave didn't last an hour from the drop")
	}
	if _, ok := v.Drop(t0, "self", time.Hour); ok || v.Self().ID != "self" {
		t.Fatal("dropped self")
	}
	if _, ok := v.Drop(t0, "nobody", time.Hour); ok {
		t.Fatal("dropped a stranger out of the view")
	}
	if got := v.Apply(t0, up("nobody", 1, member.Alive)); got != Ignored {
		t.Fatalf("a dropped stranger's entry: %v", got)
	}
	if _, ok := v.Drop(t0, "", time.Hour); ok || v.graves[""].held {
		t.Fatal("dropped the empty ID")
	}
	if err := v.Check(); err != nil {
		t.Fatal(err)
	}
}

// Held graves stay past maxGraves: graves dug past the cap, however many,
// push out the oldest graves that aren't held, never a held one.
func TestHeldGravesOutlastTheCap(t *testing.T) {
	v := New(member.Member{ID: "self"})
	v.Apply(t0, up("x", 1, member.Alive))
	v.Drop(t0, "x", time.Hour)
	for i := range maxGraves + 10 {
		id := member.ID(fmt.Sprintf("f%05d", i))
		v.Apply(t0, up(id, 1, member.Alive))
		v.Apply(t0, up(id, 1, member.Dead))
		v.ForgetOldest(t0)
	}
	if _, ok := v.Buried("x"); !ok {
		t.Fatal("the held grave went over the cap")
	}
	if got := v.Apply(t0.Add(time.Minute), up("x", 9, member.Alive)); got != Ignored {
		t.Fatalf("x after the flood: %v", got)
	}
	if _, ok := v.Buried("f00009"); ok {
		t.Fatal("a grave not held stayed past the cap")
	}
	if _, ok := v.Buried("f00010"); !ok {
		t.Fatal("the held grave took the room of a grave not held")
	}
	if err := v.Check(); err != nil {
		t.Fatal(err)
	}
}

// Lift ends a held grave: the member's entries are taken again. A grave
// that isn't held, and a member with no grave, are left as they are.
func TestLiftEndsAHold(t *testing.T) {
	v := New(member.Member{ID: "self"})
	v.Apply(t0, up("x", 1, member.Alive))
	v.Drop(t0, "x", time.Hour)
	v.Lift("x")
	if v.graves["x"].held || len(v.Unburied()) != 1 {
		t.Fatal("still held, or not reported unburied")
	}
	if got := v.Apply(t0, up("x", 1, member.Alive)); got != Changed {
		t.Fatalf("x after the lift: %v", got)
	}
	v.Apply(t0, up("y", 1, member.Alive))
	v.Apply(t0, up("y", 1, member.Dead))
	v.Forget(t0.Add(time.Minute), time.Minute, time.Hour)
	v.Lift("y")
	v.Lift("nobody")
	if _, ok := v.Buried("y"); !ok {
		t.Fatal("lifted a grave that wasn't held")
	}
	if err := v.Check(); err != nil {
		t.Fatal(err)
	}
}
