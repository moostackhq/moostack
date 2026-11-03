package gossip

import (
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/moostackhq/moostack/pkg/hivemind/membership/member"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/msg"
)

var t0 = time.Unix(1000, 0)

// sizer counts 10 bytes per update plus the length of its ID.
type sizer struct{}

func (sizer) PacketSize(msg.Packet, uint8) int     { return 0 }
func (sizer) UpdateSize(u msg.Update, _ uint8) int { return 10 + len(u.Member.ID) }

func up(id string, inc uint64, s member.State, by string) msg.Update {
	return msg.Update{Member: member.Member{ID: member.ID(id), Incarnation: inc, State: s}, By: member.ID(by)}
}

func TestLimit(t *testing.T) {
	for _, tc := range []struct{ mult, n, want int }{
		{4, 1, 4}, {4, 9, 4}, {4, 10, 8}, {4, 100, 12}, {3, 1000, 12}, {0, 5, 1},
	} {
		if got := Limit(tc.mult, tc.n); got != tc.want {
			t.Errorf("Limit(%d, %d) = %d, want %d", tc.mult, tc.n, got, tc.want)
		}
	}
}

// Take never goes over the budget, sends each update at most limit times,
// and always sends the least-sent updates first.
func TestTakeProperties(t *testing.T) {
	r := rand.New(rand.NewPCG(7, 8))
	for range 300 {
		q := New()
		limit := r.IntN(4) + 1
		sent := map[member.ID]int{}
		for i := range r.IntN(40) {
			q.Add(up(fmt.Sprint(i), 0, member.Alive, ""))
		}
		for range 20 {
			budget := r.IntN(200)
			got := q.Take(budget, msg.MaxUpdates, limit, sizer{}, 1)
			total := 0
			for _, u := range got {
				total += sizer{}.UpdateSize(u, 1)
				sent[u.Member.ID]++
				if sent[u.Member.ID] > limit {
					t.Fatalf("%s sent %d times, limit %d", u.Member.ID, sent[u.Member.ID], limit)
				}
			}
			if total > budget {
				t.Fatalf("took %d bytes of a %d budget", total, budget)
			}
			if len(got) > 1 && sent[got[0].Member.ID] > sent[got[len(got)-1].Member.ID] {
				t.Fatalf("a more-sent update went before a less-sent one")
			}
		}
	}
}

func TestAddReplaces(t *testing.T) {
	q := New()
	q.Add(up("x", 1, member.Alive, ""))
	q.Add(up("x", 1, member.Suspect, "a"))
	q.Add(up("x", 1, member.Suspect, "b"))
	if q.Len() != 2 {
		t.Fatalf("two suspicions of the same incarnation by different members: Len %d, want 2", q.Len())
	}
	q.Add(up("x", 2, member.Suspect, "c"))
	if q.Len() != 1 {
		t.Fatalf("a newer suspicion: Len %d, want 1", q.Len())
	}
	q.Add(up("x", 3, member.Alive, ""))
	got := q.Take(1000, msg.MaxUpdates, 1, sizer{}, 1)
	if len(got) != 1 || got[0].Member.Incarnation != 3 {
		t.Fatalf("Take = %v", got)
	}
	if q.Len() != 0 {
		t.Fatal("an update sent limit times stayed queued")
	}
}

func TestTakeSkips(t *testing.T) {
	q := New()
	q.Add(up("x", 0, member.Alive, ""))
	q.Add(up("y", 0, member.Alive, ""))
	got := q.Take(1000, msg.MaxUpdates, 5, sizer{}, 1, "x")
	if len(got) != 1 || got[0].Member.ID != "y" {
		t.Fatalf("Take skipping x = %v", got)
	}
}

// Replaced updates don't pile up in the queue's buckets.
func TestReplacedUpdatesAreSwept(t *testing.T) {
	q := New()
	for i := range 10000 {
		q.Add(up("x", uint64(i), member.Alive, ""))
		q.Take(1000, msg.MaxUpdates, 100, sizer{}, 1)
	}
	total := 0
	for _, b := range q.buckets {
		total += len(b)
	}
	if q.Len() != 1 || total > 200 {
		t.Fatalf("%d queued, %d in buckets", q.Len(), total)
	}
}

// A run of updates too big for what is left of the budget ends the whole
// Take: nothing more-sent goes out ahead of a less-sent update that didn't
// fit.
// Take takes at most most updates, and leaves the rest unsent.
func TestTakeTakesAtMostMost(t *testing.T) {
	q := New()
	for i := range 10 {
		q.Add(up(fmt.Sprint("m", i), 1, member.Alive, ""))
	}
	if got := q.Take(1000, 3, 5, sizer{}, 1); len(got) != 3 {
		t.Fatalf("took %d, want 3", len(got))
	}
	if got := q.Take(1000, msg.MaxUpdates, 1, sizer{}, 1); len(got) != 10 {
		t.Fatalf("took %d after, want all 10", len(got))
	}
}

// Forget drops every update about a member, suspicions by each raiser
// included, and leaves the rest.
func TestForgetDropsAMembersUpdates(t *testing.T) {
	q := New()
	q.Add(up("x", 1, member.Suspect, "a"))
	q.Add(up("x", 1, member.Suspect, "b"))
	q.Add(up("y", 1, member.Alive, ""))
	q.Forget("x")
	if q.Len() != 1 {
		t.Fatalf("%d queued, want 1", q.Len())
	}
	if got := q.Take(1000, msg.MaxUpdates, 5, sizer{}, 1); len(got) != 1 || got[0].Member.ID != "y" {
		t.Fatalf("took %v, want only y", got)
	}
}

func TestARunOfMissesEndsTheTake(t *testing.T) {
	q := New()
	q.Add(up("z", 0, member.Alive, ""))
	q.Take(1000, msg.MaxUpdates, 10, sizer{}, 1) // z is sent once
	q.Add(up("a", 0, member.Alive, ""))
	for i := range maxMisses {
		q.Add(up(fmt.Sprintf("big%02d-%s", i, strings.Repeat("x", 100)), 0, member.Alive, ""))
	}
	got := q.Take(30, msg.MaxUpdates, 10, sizer{}, 1)
	for _, u := range got {
		if u.Member.ID == "z" {
			t.Fatalf("took z, sent once, past never-sent updates: %v", got)
		}
	}
}

// Replaced updates in a bucket a Take never reaches are swept too.
func TestUnreachedBucketsAreSwept(t *testing.T) {
	q := New()
	q.Add(up("x", 0, member.Alive, ""))
	q.Take(1000, msg.MaxUpdates, 10, sizer{}, 1) // x moves to bucket 1
	for i := range 2000 {
		q.Add(up(fmt.Sprintf("y%d", i%100), uint64(i), member.Alive, ""))
	}
	total := 0
	for _, b := range q.buckets {
		total += len(b)
	}
	if total > 2*q.Len()+64 {
		t.Fatalf("%d in buckets for %d queued", total, q.Len())
	}
}

func TestPacer(t *testing.T) {
	p := NewPacer(200*time.Millisecond, 3, t0)
	if p.Tick(t0.Add(199*time.Millisecond)) || !p.Tick(t0.Add(200*time.Millisecond)) || p.Tick(t0.Add(300*time.Millisecond)) {
		t.Fatal("rounds not every 200ms")
	}
	p.Shift(time.Second)
	if !p.Due().Equal(t0.Add(1400 * time.Millisecond)) {
		t.Fatalf("Due after Shift = %v", p.Due())
	}
	if off := NewPacer(0, 3, t0); off.Tick(t0.Add(time.Hour)) || !off.Due().IsZero() {
		t.Fatal("an interval of 0 isn't off")
	}
}

// A pacer with no members to send to is off too: no rounds, and no time
// it is due.
func TestPacerWithNoFanoutIsOff(t *testing.T) {
	p := NewPacer(200*time.Millisecond, 0, t0)
	if p.Tick(t0.Add(time.Hour)) || !p.Due().IsZero() {
		t.Fatal("a fanout of 0 isn't off")
	}
}

// An update exactly the size of what is left of the budget fits.
func TestAnUpdateFillingTheBudgetFits(t *testing.T) {
	q := New()
	q.Add(up("x", 0, member.Alive, ""))
	if got := q.Take(11, msg.MaxUpdates, 5, sizer{}, 1); len(got) != 1 {
		t.Fatalf("Take with a budget of exactly one update = %v", got)
	}
}
