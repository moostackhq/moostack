package pause

import (
	"testing"
	"time"
)

func TestCheck(t *testing.T) {
	t0 := time.Unix(1000, 0)
	d := New(250 * time.Millisecond)
	if _, ok := d.Check(t0); ok {
		t.Fatal("a pause before any wake-up was asked for")
	}
	d.Expect(t0)
	if _, ok := d.Check(t0.Add(250 * time.Millisecond)); ok {
		t.Fatal("a call at the limit counted as a pause")
	}
	late, ok := d.Check(t0.Add(time.Second))
	if !ok || late != time.Second {
		t.Fatalf("Check = %v, %v", late, ok)
	}
	if _, ok := d.Check(t0.Add(time.Second + 100*time.Millisecond)); ok {
		t.Fatal("the same pause counted twice")
	}
	off := New(0)
	off.Expect(t0)
	if _, ok := off.Check(t0.Add(time.Hour)); ok {
		t.Fatal("a limit of 0 noticed a pause")
	}
}
