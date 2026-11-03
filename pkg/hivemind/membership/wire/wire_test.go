package wire

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/moostackhq/moostack/pkg/hivemind/membership/member"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/msg"
)

var (
	update  = flag.Bool("update", false, "rewrite the unreleased golden files")
	release = flag.String("release", "", "freeze the unreleased golden files as the release named")
)

// incarnation is a random incarnation, now and then near the most.
func incarnation(r *rand.Rand) uint64 {
	if r.IntN(4) == 0 {
		return member.MaxIncarnation - r.Uint64N(1000)
	}
	return r.Uint64N(1 << uint(r.IntN(62)+1))
}

func randomMember(r *rand.Rand) member.Member {
	return member.Member{
		ID: member.ID(fmt.Sprintf("node-%d", r.IntN(1000))), Addr: fmt.Sprintf("10.0.%d.%d:7946", r.IntN(256), r.IntN(256)),
		Incarnation: incarnation(r),
		State:       member.State(r.IntN(4)), Caps: member.Caps(r.Uint64N(16)),
		MinVersion: uint8(r.IntN(3)), Version: uint8(r.IntN(3)),
	}
}

func randomPacket(r *rand.Rand) msg.Packet {
	p := msg.Packet{From: randomMember(r)}
	seq := r.Uint32()
	switch r.IntN(5) {
	case 4:
		p.Body = msg.Gossip{}
	case 0:
		p.Body = msg.Ping{Seq: seq, Target: "t"}
	case 1:
		p.Body = msg.Ack{Seq: seq}
	case 2:
		p.Body = msg.PingReq{Seq: seq, Target: "t", TargetAddr: "10.0.0.1:7946"}
	case 3:
		p.Body = msg.Nack{Seq: seq}
	}
	for range r.IntN(8) {
		u := msg.Update{Member: randomMember(r)}
		if r.IntN(2) == 0 {
			u.By = "suspecter"
		}
		p.Gossip = append(p.Gossip, u)
	}
	return p
}

func TestPacketRoundTrip(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 1))
	for range 2000 {
		p := randomPacket(r)
		b, err := EncodePacket(p)
		if err != nil {
			t.Fatal(err)
		}
		got, err := DecodePacket(b)
		if err != nil {
			t.Fatalf("decode %+v: %v", p, err)
		}
		if !reflect.DeepEqual(got, p) {
			t.Fatalf("round trip:\n got %+v\nwant %+v", got, p)
		}
	}
}

func TestSyncRoundTrip(t *testing.T) {
	r := rand.New(rand.NewPCG(2, 2))
	for range 500 {
		s := msg.Sync{From: randomMember(r), Reply: r.IntN(2) == 0}
		for range r.IntN(50) {
			s.View = append(s.View, randomMember(r))
		}
		b, err := EncodeSync(s)
		if err != nil {
			t.Fatal(err)
		}
		if len(b) != cap(b) {
			t.Fatalf("a sync of %d bytes sized as %d", len(b), cap(b))
		}
		got, err := DecodeSync(b)
		if err != nil || !reflect.DeepEqual(got, s) {
			t.Fatalf("round trip: %+v, %v; want %+v", got, err, s)
		}
	}
}

// The sizer is exact: a packet's size is its size without gossip plus each update's size.
func TestSizerIsExact(t *testing.T) {
	var sz Sizer
	r := rand.New(rand.NewPCG(3, 3))
	for range 2000 {
		p := randomPacket(r)
		bare := p
		bare.Gossip = nil
		want := sz.PacketSize(bare, 1)
		for _, u := range p.Gossip {
			want += sz.UpdateSize(u, 1)
		}
		b, _ := EncodePacket(p)
		if len(b) != want || sz.PacketSize(p, 1) != want {
			t.Fatalf("encoded %d bytes, sizer says %d", len(b), want)
		}
	}
}

// A field of an unknown tag is skipped, wherever it is.
func TestUnknownFieldsAreSkipped(t *testing.T) {
	p := msg.Packet{
		From:   member.Member{ID: "a", Addr: "a:1", Incarnation: 3},
		Body:   msg.PingReq{Seq: 9, Target: "b", TargetAddr: "b:1"},
		Gossip: []msg.Update{{Member: member.Member{ID: "c", State: member.Suspect}, By: "a"}},
	}
	b, _ := EncodePacket(p)
	extra := binary.AppendUvarint(nil, 99)
	extra = binary.AppendUvarint(extra, 3)
	extra = append(extra, 1, 2, 3)
	got, err := DecodePacket(append(b, extra...))
	if err != nil || !reflect.DeepEqual(got, p) {
		t.Fatalf("with an unknown field: %+v, %v", got, err)
	}

	// Inside an entry too.
	m := append(encodeMember(member.Member{ID: "x", Incarnation: 1}), extra...)
	got2, err := decodeMember(m)
	if err != nil || got2 != (member.Member{ID: "x", Incarnation: 1}) {
		t.Fatalf("entry with an unknown field: %+v, %v", got2, err)
	}
}

func TestUnknownKindAndNewerVersion(t *testing.T) {
	b := binary.AppendUvarint(nil, Version)
	b = append(b, 42)
	if _, err := DecodePacket(b); !errors.Is(err, ErrUnknownKind) {
		t.Fatalf("unknown kind: %v", err)
	}
	// A packet of a known kind in a higher version decodes.
	p := msg.Packet{From: member.Member{ID: "a"}, Body: msg.Ack{Seq: 1}}
	enc, _ := EncodePacket(p)
	newer := append(binary.AppendUvarint(nil, Version+1), enc[1:]...)
	if got, err := DecodePacket(newer); err != nil || !reflect.DeepEqual(got, p) {
		t.Fatalf("newer version: %+v, %v", got, err)
	}
	if v, k, err := PeekKind(newer); err != nil || v != Version+1 || k != KindAck {
		t.Fatalf("PeekKind = %d, %d, %v", v, k, err)
	}
}

func TestMalformed(t *testing.T) {
	// Gossip comes after the sender, so cuts fall inside every kind of
	// field, not only before the sender.
	good, _ := EncodePacket(msg.Packet{From: member.Member{ID: "a"}, Body: msg.Ping{Seq: 1, Target: "b"},
		Gossip: []msg.Update{{Member: member.Member{ID: "c", Incarnation: 3}, By: "a"}}})
	// A cut inside a field fails; one between fields after the sender is
	// a shorter message, complete in itself.
	for i := range len(good) {
		p, err := DecodePacket(good[:i])
		if err != nil {
			continue
		}
		if again, _ := EncodePacket(p); string(again) != string(good[:i]) {
			t.Errorf("decoded a packet cut to %d of %d bytes", i, len(good))
		}
	}
	noSender := append(binary.AppendUvarint(nil, Version), byte(KindAck))
	if _, err := DecodePacket(noSender); err == nil {
		t.Error("decoded a packet without a sender")
	}
}

// golden are the messages TestGolden encodes and checks every frozen
// release against.
var golden = map[string]any{
	"ping": msg.Packet{
		From: member.Member{ID: "node-a", Addr: "10.0.0.1:7946", Incarnation: 2, Caps: member.Of(member.CapNack), Version: 1},
		Body: msg.Ping{Seq: 41, Target: "node-b"},
		Gossip: []msg.Update{
			{Member: member.Member{ID: "node-c", Addr: "10.0.0.3:7946", Incarnation: 5, State: member.Suspect, Version: 1}, By: "node-a"},
			{Member: member.Member{ID: "node-d", Addr: "10.0.0.4:7946", Incarnation: 1, State: member.Dead}},
		},
	},
	"ack":     msg.Packet{From: member.Member{ID: "node-b", Addr: "10.0.0.2:7946", Version: 1}, Body: msg.Ack{Seq: 41}},
	"pingreq": msg.Packet{From: member.Member{ID: "node-a", Addr: "10.0.0.1:7946", Version: 1}, Body: msg.PingReq{Seq: 7, Target: "node-b", TargetAddr: "10.0.0.2:7946"}},
	"nack":    msg.Packet{From: member.Member{ID: "node-e", Addr: "10.0.0.5:7946", Version: 1}, Body: msg.Nack{Seq: 7}},
	"gossip": msg.Packet{
		From:   member.Member{ID: "node-g", Addr: "10.0.0.7:7946", Caps: member.Of(member.CapNack, member.CapGossip), Version: 1},
		Body:   msg.Gossip{},
		Gossip: []msg.Update{{Member: member.Member{ID: "node-h", Addr: "10.0.0.8:7946", Incarnation: 3, State: member.Left, Version: 1}}},
	},
	"sync": msg.Sync{
		From:  member.Member{ID: "node-a", Addr: "10.0.0.1:7946", Version: 1},
		View:  []member.Member{{ID: "node-a", Addr: "10.0.0.1:7946", Version: 1}, {ID: "node-f", Addr: "10.0.0.6:7946", Incarnation: 9, State: member.Left}},
		Reply: true,
	},
}

func encodeAny(v any) []byte {
	switch v := v.(type) {
	case msg.Packet:
		b, _ := EncodePacket(v)
		return b
	case msg.Sync:
		b, _ := EncodeSync(v)
		return b
	}
	panic(v)
}

// TestGolden checks the messages this code writes against
// testdata/unreleased, and that it still reads what every release wrote,
// in testdata/released/<release>.
// `go test -run Golden -update` rewrites the unreleased files;
// `-release=NAME` freezes them as release NAME.
func TestGolden(t *testing.T) {
	dir := filepath.Join("testdata", "unreleased")
	if *update {
		writeGolden(t, dir, false)
	}
	for name, want := range golden {
		b, err := os.ReadFile(filepath.Join(dir, name+".bin"))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if string(encodeAny(want)) != string(b) {
			t.Errorf("%s: the encoding changed: run with -update if that is meant", name)
		}
	}
	if *release != "" {
		frozen := filepath.Join("testdata", "released", *release)
		if _, err := os.Stat(frozen); err == nil {
			t.Fatalf("release %s is frozen already", *release)
		}
		writeGolden(t, frozen, true)
	}
	releases, _ := filepath.Glob(filepath.Join("testdata", "released", "*", "expect.json"))
	for _, e := range releases {
		checkRelease(t, filepath.Dir(e))
	}
}

// writeGolden writes the golden messages to dir, with what they decode to
// when expect is set.
func writeGolden(t *testing.T, dir string, expect bool) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	want := map[string]goldenMsg{}
	for name, v := range golden {
		if err := os.WriteFile(filepath.Join(dir, name+".bin"), encodeAny(v), 0o644); err != nil {
			t.Fatal(err)
		}
		want[name] = toGolden(v)
	}
	if expect {
		b, _ := json.MarshalIndent(want, "", "  ")
		if err := os.WriteFile(filepath.Join(dir, "expect.json"), b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// checkRelease decodes a frozen release's files with this code, as a node
// of this version reads a node of that release, and compares them with
// what the release froze. Fields the frozen files lack decode to their zero
// value.
func checkRelease(t reporter, dir string) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, "expect.json"))
	if err != nil {
		t.Fatal(err)
	}
	var want map[string]goldenMsg
	if err := json.Unmarshal(b, &want); err != nil {
		t.Fatalf("%s: %v", dir, err)
	}
	codec, err := NewCodec(1, Version)
	if err != nil {
		t.Fatal(err)
	}
	for name, w := range want {
		data, err := os.ReadFile(filepath.Join(dir, name+".bin"))
		if err != nil {
			t.Fatal(err)
		}
		var got goldenMsg
		if w.Kind == "sync" {
			s, err := codec.DecodeSync(data)
			if err != nil {
				t.Errorf("%s %s: %v", dir, name, err)
				continue
			}
			got = toGolden(s)
		} else {
			p, err := codec.DecodePacket(data)
			if err != nil {
				t.Errorf("%s %s: %v", dir, name, err)
				continue
			}
			got = toGolden(p)
		}
		if !reflect.DeepEqual(got, w) {
			t.Errorf("%s %s: decoded %+v, want %+v", dir, name, got, w)
		}
	}
}

// reporter is what checkRelease reports to: a test, or a recorder.
type reporter interface {
	Helper()
	Errorf(format string, args ...any)
	Fatal(args ...any)
	Fatalf(format string, args ...any)
}

// recorder notes that a check failed, without failing the test.
type recorder struct{ failed bool }

func (r *recorder) Helper()               {}
func (r *recorder) Errorf(string, ...any) { r.failed = true }
func (r *recorder) Fatal(...any)          { r.failed = true }
func (r *recorder) Fatalf(string, ...any) { r.failed = true }

// goldenMsg is a message as JSON can hold it: the probe message's fields
// flattened beside the rest.
type goldenMsg struct {
	Kind       string
	From       member.Member
	Seq        uint32          `json:",omitempty"`
	Target     member.ID       `json:",omitempty"`
	TargetAddr string          `json:",omitempty"`
	Gossip     []msg.Update    `json:",omitempty"`
	View       []member.Member `json:",omitempty"`
	Reply      bool            `json:",omitempty"`
}

func toGolden(v any) goldenMsg {
	switch v := v.(type) {
	case msg.Sync:
		return goldenMsg{Kind: "sync", From: v.From, View: v.View, Reply: v.Reply}
	case msg.Packet:
		g := goldenMsg{From: v.From, Gossip: v.Gossip}
		switch b := v.Body.(type) {
		case msg.Ping:
			g.Kind, g.Seq, g.Target = "ping", b.Seq, b.Target
		case msg.Ack:
			g.Kind, g.Seq = "ack", b.Seq
		case msg.PingReq:
			g.Kind, g.Seq, g.Target, g.TargetAddr = "ping-req", b.Seq, b.Target, b.TargetAddr
		case msg.Nack:
			g.Kind, g.Seq = "nack", b.Seq
		case msg.Gossip:
			g.Kind = "gossip"
		}
		return g
	}
	panic(v)
}

// A release frozen from the golden messages decodes back to them through
// the codec.
func TestAFrozenReleaseIsRead(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "r1")
	writeGolden(t, dir, true)
	checkRelease(t, dir)
	// A frozen file that decodes to something other than what was frozen
	// fails.
	b, _ := os.ReadFile(filepath.Join(dir, "expect.json"))
	var want map[string]goldenMsg
	json.Unmarshal(b, &want)
	g := want["ack"]
	g.Seq++
	want["ack"] = g
	b, _ = json.Marshal(want)
	os.WriteFile(filepath.Join(dir, "expect.json"), b, 0o644)
	got := &recorder{}
	checkRelease(got, dir)
	if !got.failed {
		t.Fatal("a release that decodes to something else passed")
	}
}

// FuzzDecode feeds the decoders arbitrary bytes, higher versions' messages
// among them: they must not panic, and what they decode must re-encode to
// a message that decodes the same.
func FuzzDecode(f *testing.F) {
	for _, v := range golden {
		f.Add(encodeAny(v))
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		if p, err := DecodePacket(b); err == nil {
			again, err := EncodePacket(p)
			if err != nil {
				t.Fatal(err)
			}
			p2, err := DecodePacket(again)
			if err != nil || !reflect.DeepEqual(p, p2) {
				t.Fatalf("re-encoded packet decodes as %+v, %v; want %+v", p2, err, p)
			}
		}
		if s, err := DecodeSync(b); err == nil {
			b2, err := EncodeSync(s)
			if err != nil {
				t.Fatalf("a decoded sync doesn't encode: %v", err)
			}
			s2, err := DecodeSync(b2)
			if err != nil || !reflect.DeepEqual(s, s2) {
				t.Fatalf("re-encoded sync decodes as %+v, %v; want %+v", s2, err, s)
			}
		}
	})
}

// What a hostile sender can make a decoder accept is bounded.
func TestLimits(t *testing.T) {
	long := strings.Repeat("x", MaxIDBytes+1)
	for name, p := range map[string]msg.Packet{
		"a long sender ID": {From: member.Member{ID: member.ID(long)}, Body: msg.Ack{}},
		"a long address":   {From: member.Member{ID: "a", Addr: long}, Body: msg.Ack{}},
		"a long target":    {From: member.Member{ID: "a"}, Body: msg.Ping{Target: member.ID(long)}},
		"a long raiser":    {From: member.Member{ID: "a"}, Body: msg.Ack{}, Gossip: []msg.Update{{Member: member.Member{ID: "b"}, By: member.ID(long)}}},
	} {
		b, _ := EncodePacket(p)
		if _, err := DecodePacket(b); err == nil {
			t.Errorf("decoded %s", name)
		}
	}
	many := msg.Packet{From: member.Member{ID: "a"}, Body: msg.Ack{}}
	for range MaxUpdates + 1 {
		many.Gossip = append(many.Gossip, msg.Update{Member: member.Member{ID: "b"}})
	}
	b, _ := EncodePacket(many)
	if _, err := DecodePacket(b); err == nil {
		t.Error("decoded a packet of more than MaxUpdates updates")
	}
	if _, err := DecodePacket(make([]byte, MaxPacketBytes+1)); err == nil {
		t.Error("decoded an oversized packet")
	}
	if _, err := DecodeSync(make([]byte, MaxSyncBytes+1)); err == nil {
		t.Error("decoded an oversized sync")
	}
	at := msg.Packet{From: member.Member{ID: member.ID(strings.Repeat("x", MaxIDBytes))}, Body: msg.Ack{}}
	b, _ = EncodePacket(at)
	if _, err := DecodePacket(b); err != nil {
		t.Errorf("an ID of MaxIDBytes: %v", err)
	}
}

// FuzzCodec feeds the codec of a node that reads versions 1 and 2 arbitrary
// bytes: what it decodes it re-encodes in the version it came in.
func FuzzCodec(f *testing.F) {
	c, err := NewCodec(1, 2, breaking2)
	if err != nil {
		f.Fatal(err)
	}
	for _, v := range golden {
		b := encodeAny(v)
		f.Add(b)
		if p, ok := v.(msg.Packet); ok {
			b2, _ := c.EncodePacket(p, 2)
			f.Add(b2)
		}
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		v, _, err := PeekKind(b)
		if err != nil {
			return
		}
		if p, err := c.DecodePacket(b); err == nil {
			again, err := c.EncodePacket(p, uint8(v))
			if err != nil {
				t.Fatal(err)
			}
			if p2, err := c.DecodePacket(again); err != nil || !reflect.DeepEqual(p, p2) {
				t.Fatalf("re-encoded packet decodes as %+v, %v; want %+v", p2, err, p)
			}
		}
		_, _ = c.DecodeSync(b)
	})
}

// padTo appends a field of a tag format 1 doesn't know, making b exactly n
// bytes long.
func padTo(t *testing.T, b []byte, n int) []byte {
	t.Helper()
	rest := n - len(b) - 1 // the tag, 99, is one byte
	for lenLen := 1; lenLen <= 4; lenLen++ {
		if l := rest - lenLen; l >= 0 && uvarintLen(uint64(l)) == lenLen {
			b = binary.AppendUvarint(b, 99)
			b = binary.AppendUvarint(b, uint64(l))
			return append(b, make([]byte, l)...)
		}
	}
	t.Fatalf("can't pad %d bytes to %d", len(b), n)
	return nil
}

// Every limit is inclusive: a message at it, and every field at its own,
// is read.
func TestLimitsAreInclusive(t *testing.T) {
	id := member.ID(strings.Repeat("i", MaxIDBytes))
	addr := strings.Repeat("a", MaxAddrBytes)
	at := msg.Packet{
		From:   member.Member{ID: id, Addr: addr, MinVersion: 255, Version: 255},
		Body:   msg.PingReq{Seq: 1<<32 - 1, Target: id, TargetAddr: addr},
		Gossip: []msg.Update{{Member: member.Member{ID: "b"}, By: id}},
	}
	b, err := EncodePacket(at)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := DecodePacket(b); err != nil || !reflect.DeepEqual(got, at) {
		t.Fatalf("every field at its limit: %+v, %v", got, err)
	}

	many := msg.Packet{From: member.Member{ID: "a"}, Body: msg.Ack{}}
	for range MaxUpdates {
		many.Gossip = append(many.Gossip, msg.Update{Member: member.Member{ID: "b"}})
	}
	b, err = EncodePacket(many)
	if err != nil {
		t.Fatalf("encoding MaxUpdates updates: %v", err)
	}
	if got, err := DecodePacket(b); err != nil || len(got.Gossip) != MaxUpdates {
		t.Fatalf("decoding MaxUpdates updates: %d, %v", len(got.Gossip), err)
	}

	b, _ = EncodePacket(msg.Packet{From: member.Member{ID: "a"}, Body: msg.Ack{}})
	if _, err := DecodePacket(padTo(t, b, MaxPacketBytes)); err != nil {
		t.Fatalf("a packet of MaxPacketBytes: %v", err)
	}
	s, _ := EncodeSync(msg.Sync{From: member.Member{ID: "a"}})
	if _, err := DecodeSync(padTo(t, s, MaxSyncBytes)); err != nil {
		t.Fatalf("a sync of MaxSyncBytes: %v", err)
	}
}
