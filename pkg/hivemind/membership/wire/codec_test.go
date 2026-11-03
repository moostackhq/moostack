package wire_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/moostackhq/moostack/pkg/hivemind/membership/member"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/msg"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/wire"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/wire/wiretest"
)

var pkt = msg.Packet{
	From:   member.Member{ID: "a", Addr: "a:1", MinVersion: 1, Version: 2},
	Body:   msg.Ping{Seq: 3, Target: "b"},
	Gossip: []msg.Update{{Member: member.Member{ID: "c", Incarnation: 2, State: member.Suspect}, By: "a"}},
}

func TestNewCodecNeedsAFormatForEveryVersion(t *testing.T) {
	if _, err := wire.NewCodec(1, 2); err == nil {
		t.Fatal("a codec up to version 2 without its format")
	}
	if _, err := wire.NewCodec(2, 1); err == nil {
		t.Fatal("a codec from 2 to 1")
	}
	if _, err := wire.NewCodec(1, 2, wiretest.Breaking(2)); err != nil {
		t.Fatal(err)
	}
}

// A codec reads only its versions: one for 2 alone refuses version 1, one
// for 1 alone refuses version 2.
func TestVersionsReadAndRefused(t *testing.T) {
	v1, _ := wire.NewCodec(1, 1)
	both, _ := wire.NewCodec(1, 2, wiretest.Breaking(2))
	v2, _ := wire.NewCodec(2, 2, wiretest.Breaking(2))

	in1, err := both.EncodePacket(pkt, 1)
	if err != nil {
		t.Fatal(err)
	}
	in2, err := both.EncodePacket(pkt, 2)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name  string
		c     *wire.Codec
		b     []byte
		reads bool
	}{
		{"v1 reads v1", v1, in1, true},
		{"v1 refuses v2", v1, in2, false},
		{"both read v1", both, in1, true},
		{"both read v2", both, in2, true},
		{"v2 refuses v1", v2, in1, false},
		{"v2 reads v2", v2, in2, true},
	} {
		got, err := tc.c.DecodePacket(tc.b)
		if tc.reads && (err != nil || !reflect.DeepEqual(got, pkt)) {
			t.Errorf("%s: %+v, %v", tc.name, got, err)
		}
		if !tc.reads && !errors.Is(err, wire.ErrVersion) {
			t.Errorf("%s: err %v, want ErrVersion", tc.name, err)
		}
	}
	// The breaking format really breaks format 1, even ignoring versions.
	if _, err := wire.DecodePacket(in2); err == nil {
		t.Error("format 1 decoded the breaking format")
	}
	if _, err := v1.EncodePacket(pkt, 2); !errors.Is(err, wire.ErrVersion) {
		t.Errorf("v1 wrote version 2: %v", err)
	}
}

func TestSyncVersions(t *testing.T) {
	both, _ := wire.NewCodec(1, 2, wiretest.Breaking(2))
	v1, _ := wire.NewCodec(1, 1)
	s := msg.Sync{From: pkt.From, View: []member.Member{pkt.From}, Reply: true}
	b, _ := both.EncodeSync(s, 2)
	if got, err := both.DecodeSync(b); err != nil || !reflect.DeepEqual(got, s) {
		t.Fatalf("%+v, %v", got, err)
	}
	if _, err := v1.DecodeSync(b); !errors.Is(err, wire.ErrVersion) {
		t.Fatalf("v1 read a version 2 sync: %v", err)
	}
}

func TestEntryCarriesItsVersions(t *testing.T) {
	b, _ := wire.EncodePacket(pkt)
	got, _ := wire.DecodePacket(b)
	if got.From.MinVersion != 1 || got.From.Version != 2 {
		t.Fatalf("entry versions %d to %d", got.From.MinVersion, got.From.Version)
	}
}

// A codec reads the highest version a header can name.
func TestACodecReadsVersion255(t *testing.T) {
	c, err := wire.NewCodec(255, 255, wiretest.Breaking(255))
	if err != nil {
		t.Fatal(err)
	}
	b, err := c.EncodePacket(pkt, 255)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := c.DecodePacket(b); err != nil || !reflect.DeepEqual(got, pkt) {
		t.Fatalf("DecodePacket = %+v, %v", got, err)
	}
}

// Version 1 is built in: a whole format for it doesn't replace it.
func TestNewCodecKeepsFormat1(t *testing.T) {
	if _, err := wire.NewCodec(1, 1, wiretest.Breaking(1)); err == nil {
		t.Fatal("a format replaced version 1")
	}
}
