package suspicion

import (
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/moostackhq/moostack/pkg/hivemind/membership/member"
)

var t0 = time.Unix(1000, 0)

// The timeout starts at max, falls with every confirmation, reaches min at
// k confirmations and never goes below it (Lifeguard 4.2).
func TestTimeoutBoundsAndMonotone(t *testing.T) {
	r := rand.New(rand.NewPCG(5, 6))
	for range 5000 {
		lo := time.Duration(r.IntN(10000)+1) * time.Millisecond
		hi := lo * time.Duration(r.IntN(8)+1)
		k := r.IntN(6)
		prev := hi + 1
		for c := 0; c <= k+2; c++ {
			got := Timeout(lo, hi, k, c)
			if got < lo || got > hi {
				t.Fatalf("Timeout(%v, %v, %d, %d) = %v, outside its bounds", lo, hi, k, c, got)
			}
			if got > prev {
				t.Fatalf("Timeout rose from %v to %v at c=%d", prev, got, c)
			}
			if c >= k && got != lo {
				t.Fatalf("Timeout(%v, %v, %d, %d) = %v, want min", lo, hi, k, c, got)
			}
			prev = got
		}
		if k > 0 && hi > lo && Timeout(lo, hi, k, 0) != hi {
			t.Fatalf("no confirmations: want max")
		}
	}
}

func TestTimeoutFormula(t *testing.T) {
	// max − (max − min)·log(c+1)/log(k+1) with min 2s, max 12s, k 3, c 1:
	// 12 − 10·log 2/log 4 = 7s.
	if got := Timeout(2*time.Second, 12*time.Second, 3, 1); got != 7*time.Second {
		t.Fatalf("Timeout = %v, want 7s", got)
	}
}

func TestConfirmationsShortenTheTimer(t *testing.T) {
	tr := New()
	tr.Start(t0, "x", 1, "a", 2*time.Second, 12*time.Second, 3)
	if got := tr.Due(); !got.Equal(t0.Add(12 * time.Second)) {
		t.Fatalf("Due = %v", got)
	}
	if ok, _ := tr.Confirm("x", 1, "a"); ok {
		t.Fatal("the raiser counted as its own confirmation")
	}
	if ok, _ := tr.Confirm("x", 0, "b"); ok {
		t.Fatal("a suspicion of an older incarnation counted")
	}
	if ok, c := tr.Confirm("x", 1, "b"); !ok || c != 1 {
		t.Fatalf("Confirm = %v, %d", ok, c)
	}
	if got := tr.Due(); !got.Equal(t0.Add(7 * time.Second)) {
		t.Fatalf("Due after one confirmation = %v", got)
	}
	if got := tr.Expired(t0.Add(7*time.Second - 1)); len(got) != 0 {
		t.Fatalf("expired early: %v", got)
	}
	if got := tr.Expired(t0.Add(7 * time.Second)); len(got) != 1 || got[0].ID != "x" || got[0].Incarnation != 1 {
		t.Fatalf("Expired = %v", got)
	}
	if tr.Suspected("x") {
		t.Fatal("still suspected after expiring")
	}
}

func TestStartKeepsTheTimerOfTheSameIncarnation(t *testing.T) {
	tr := New()
	tr.Start(t0, "x", 1, "a", time.Second, time.Second, 0)
	tr.Start(t0.Add(500*time.Millisecond), "x", 1, "b", time.Second, time.Second, 0)
	if got := tr.Due(); !got.Equal(t0.Add(time.Second)) {
		t.Fatalf("a second suspicion restarted the timer: Due = %v", got)
	}
	tr.Start(t0.Add(500*time.Millisecond), "x", 2, "b", time.Second, time.Second, 0)
	if got := tr.Due(); !got.Equal(t0.Add(1500 * time.Millisecond)) {
		t.Fatalf("a newer incarnation didn't restart it: Due = %v", got)
	}
	tr.Shift(time.Second)
	if got := tr.Due(); !got.Equal(t0.Add(2500 * time.Millisecond)) {
		t.Fatalf("Shift: Due = %v", got)
	}
}

// Confirmations past k aren't kept: the timeout is at its shortest.
func TestConfirmersStopAtK(t *testing.T) {
	tr := New()
	tr.Start(t0, "x", 1, "a", time.Second, 6*time.Second, 3)
	for i := range 100 {
		tr.Confirm("x", 1, member.ID(fmt.Sprint("c", i)))
	}
	if n := len(tr.timers["x"].confirmers); n != 4 {
		t.Fatalf("%d confirmers kept, want k+1 = 4", n)
	}
	if got := tr.Due(); !got.Equal(t0.Add(time.Second)) {
		t.Fatalf("Due = %v, want the shortest timeout", got)
	}
}

// Confirm reports the confirmations counted so far when it counts none
// more: for a repeated confirmer, and past k.
func TestConfirmReportsTheCountItKeeps(t *testing.T) {
	tr := New()
	tr.Start(t0, "x", 1, "a", time.Second, 6*time.Second, 2)
	tr.Confirm("x", 1, "b")
	if ok, c := tr.Confirm("x", 1, "b"); ok || c != 1 {
		t.Fatalf("a repeated confirmer: Confirm = %v, %d, want false, 1", ok, c)
	}
	tr.Confirm("x", 1, "c")
	if ok, c := tr.Confirm("x", 1, "d"); ok || c != 2 {
		t.Fatalf("past k: Confirm = %v, %d, want false, 2", ok, c)
	}
	if ok, c := tr.Confirm("y", 1, "b"); ok || c != 0 {
		t.Fatalf("no timer: Confirm = %v, %d, want false, 0", ok, c)
	}
}
