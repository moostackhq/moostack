package member

import (
	"math/rand/v2"
	"testing"
)

func randomEntry(r *rand.Rand) Member {
	return Member{
		ID: "a", Incarnation: r.Uint64N(4), State: State(r.IntN(4)),
		Addr: []string{"x", "y"}[r.IntN(2)], Caps: Caps(r.IntN(2)), Version: uint8(r.IntN(2)), MinVersion: uint8(r.IntN(2)),
	}
}

// Overrides orders whole entries: merging entries in any order and any
// number of times gives the same entry.
func TestNewerIsASemilattice(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	for range 10000 {
		a, b, c := randomEntry(r), randomEntry(r), randomEntry(r)
		if Newer(a, a) != a {
			t.Fatalf("not idempotent: %v", a)
		}
		if Newer(a, b) != Newer(b, a) {
			t.Fatalf("not commutative: %v, %v", a, b)
		}
		if Newer(Newer(a, b), c) != Newer(a, Newer(b, c)) {
			t.Fatalf("not associative: %v, %v, %v", a, b, c)
		}
		if n := Newer(a, b); Overrides(a, n) || Overrides(b, n) {
			t.Fatalf("%v is not the upper bound of %v and %v", n, a, b)
		}
	}
}

func TestOverrides(t *testing.T) {
	e := func(inc uint64, s State) Member { return Member{ID: "a", Incarnation: inc, State: s} }
	for _, tc := range []struct {
		a, b Member
		want bool
	}{
		// SWIM 4.2's preference rules.
		{e(2, Alive), e(1, Suspect), true},
		{e(1, Alive), e(1, Suspect), false},
		{e(2, Alive), e(1, Alive), true},
		{e(1, Suspect), e(1, Alive), true},
		{e(1, Suspect), e(0, Suspect), true},
		{e(1, Suspect), e(1, Suspect), false},
		{e(0, Dead), e(0, Suspect), true},
		{e(0, Dead), e(0, Alive), true},
		// A refutation at a higher incarnation brings a dead member back.
		{e(1, Alive), e(0, Dead), true},
		{e(0, Alive), e(0, Dead), false},
		// A member's own leave outranks others finding it dead.
		{e(3, Left), e(3, Dead), true},
		{e(3, Dead), e(3, Left), false},
	} {
		if got := Overrides(tc.a, tc.b); got != tc.want {
			t.Errorf("Overrides(%v, %v) = %v, want %v", tc.a, tc.b, got, tc.want)
		}
	}
}

func TestCaps(t *testing.T) {
	cs := Of(CapNack)
	if !cs.Has(CapNack) || cs.Has(CapNack+1) {
		t.Fatalf("Of(CapNack) = %b", cs)
	}
	if !cs.With(CapNack + 1).Has(CapNack + 1) {
		t.Fatal("With didn't add")
	}
}

func TestStringQuotesWhatIsntPrintable(t *testing.T) {
	m := Member{ID: "a\nforged line", Addr: "x\x1b[31m"}
	if got := m.String(); got != `"a\nforged line"@"x\x1b[31m"#0:alive` {
		t.Fatalf("String = %s", got)
	}
	if got := (Member{ID: "n01", Addr: "10.0.0.1:7946"}).String(); got != "n01@10.0.0.1:7946#0:alive" {
		t.Fatalf("String = %s", got)
	}
}

// Dead and left members are out of the live view; alive and suspect ones
// are in it.
func TestGone(t *testing.T) {
	for s, want := range map[State]bool{Alive: false, Suspect: false, Dead: true, Left: true} {
		if got := s.Gone(); got != want {
			t.Errorf("%v.Gone() = %v, want %v", s, got, want)
		}
	}
}
