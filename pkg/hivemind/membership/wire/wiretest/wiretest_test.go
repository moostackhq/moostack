package wiretest_test

import (
	"testing"

	"github.com/moostackhq/moostack/pkg/hivemind/membership/member"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/msg"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/wire/wiretest"
)

// Breaking's sizes are exact, for a version of one byte and of two.
func TestBreakingSizesAreExact(t *testing.T) {
	p := msg.Packet{From: member.Member{ID: "a", Addr: "a:1"}, Body: msg.Ping{Seq: 1, Target: "b"},
		Gossip: []msg.Update{{Member: member.Member{ID: "c"}}}}
	for _, v := range []uint8{2, 127, 128, 200} {
		f := wiretest.Breaking(v)
		b, err := f.EncodePacket(p)
		if err != nil {
			t.Fatal(err)
		}
		if f.PacketSize(p) != len(b) {
			t.Errorf("version %d: PacketSize %d, encoded %d", v, f.PacketSize(p), len(b))
		}
		if got, err := f.DecodePacket(b); err != nil || got.From.ID != "a" {
			t.Errorf("version %d: %v, %v", v, got, err)
		}
	}
}

// Breaking refuses what isn't in its version, a bare header included,
// packets and syncs alike.
func TestBreakingRefusesOtherBytes(t *testing.T) {
	f := wiretest.Breaking(2)
	s := msg.Sync{From: member.Member{ID: "a"}}
	b, err := f.EncodeSync(s)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := f.DecodeSync(b); err != nil || got.From.ID != "a" {
		t.Fatalf("DecodeSync = %v, %v", got, err)
	}
	for _, b := range [][]byte{{2}, {3, 5}, {}} {
		if _, err := f.DecodePacket(b); err == nil {
			t.Errorf("DecodePacket(%v) read", b)
		}
		if _, err := f.DecodeSync(b); err == nil {
			t.Errorf("DecodeSync(%v) read", b)
		}
	}
}
