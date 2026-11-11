package buildx

import (
	"bytes"
	"math/rand/v2"
	"testing"
)

// However the output is cut into writes, the watch passes it through
// unchanged, and sees the needle exactly when the whole output holds it.
func TestTheWatchSeesTheNeedleHoweverTheOutputIsCut(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	needle := []byte(execFormatError)
	for range 2000 {
		var whole []byte
		for range r.IntN(4) {
			whole = append(whole, bytes.Repeat([]byte("x"), r.IntN(40))...)
			if r.IntN(2) == 0 {
				whole = append(whole, needle[:r.IntN(len(needle)+1)]...)
			}
		}
		if r.IntN(3) == 0 {
			at := r.IntN(len(whole) + 1)
			whole = append(whole[:at:at], append(append([]byte(nil), needle...), whole[at:]...)...)
		}
		var out bytes.Buffer
		w := &watch{w: &out, needle: needle}
		for rest := whole; len(rest) > 0; {
			n := 1 + r.IntN(len(rest))
			if _, err := w.Write(rest[:n]); err != nil {
				t.Fatal(err)
			}
			rest = rest[n:]
		}
		if !bytes.Equal(out.Bytes(), whole) {
			t.Fatalf("the watch passed %q through as %q", whole, out.Bytes())
		}
		if w.seen != bytes.Contains(whole, needle) {
			t.Fatalf("the watch of %q saw the needle: %v", whole, w.seen)
		}
	}
}
