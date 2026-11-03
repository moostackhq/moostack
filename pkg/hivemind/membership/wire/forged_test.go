package wire

import (
	"encoding/binary"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/moostackhq/moostack/pkg/hivemind/membership/member"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/msg"
)

// raw builds a message from its header and the given fields.
func raw(version uint64, kind Kind, fields ...enc) []byte {
	b := binary.AppendUvarint(nil, version)
	b = append(b, byte(kind))
	for _, f := range fields {
		b = append(b, f...)
	}
	return b
}

func field(tag uint64, v []byte) enc { return enc(nil).nested(tag, v) }

func num(v uint64) []byte { return binary.AppendUvarint(nil, v) }

// entry is an entry's fields with one changed by edit.
func entry(edit func(enc) enc) []byte {
	e := enc(nil).str(tagID, "a").str(tagAddr, "a:1")
	if edit != nil {
		e = edit(e)
	}
	return e
}

var sender = field(tagFrom, entry(nil))

// Each check of the decoder refuses its forged field, and says what it
// refused.
func TestEachForgedFieldIsRefused(t *testing.T) {
	long := []byte(strings.Repeat("x", MaxAddrBytes+1))
	overlong := []byte{0x81, 0x00} // 1, in two bytes
	for _, tc := range []struct {
		name string
		b    []byte
		want string
	}{
		{"version 0", raw(0, KindAck, sender), "version 0"},
		{"a state past Left in the sender", raw(1, KindAck, field(tagFrom, entry(func(e enc) enc { return e.uvarint(tagState, 9) }))), "unknown state"},
		{"wire version past 255", raw(1, KindAck, field(tagFrom, entry(func(e enc) enc { return e.uvarint(tagVersion, 300) }))), "version out of range"},
		{"lowest wire version past 255", raw(1, KindAck, field(tagFrom, entry(func(e enc) enc { return e.uvarint(tagMinVersion, 300) }))), "version out of range"},
		{"incarnation past the most", raw(1, KindAck, field(tagFrom, entry(func(e enc) enc { return e.uvarint(tagIncarnation, member.MaxIncarnation+1) }))), "incarnation"},
		{"sequence number past 32 bits", raw(1, KindAck, field(tagSeq, num(1<<32)), sender), "sequence number"},
		{"a long target address", raw(1, KindPingReq, field(tagTargetAddr, long), sender), "address"},
		{"a long target", raw(1, KindPing, field(tagTarget, long), sender), "ID"},
		{"an entry without an ID", raw(1, KindAck, field(tagFrom, enc(nil).str(tagAddr, "a:1"))), "without an ID"},
		{"an update without an entry", raw(1, KindAck, sender, field(tagUpdate, enc(nil).str(tagUpdateBy, "b"))), "without an entry"},
		{"a number in two bytes", raw(1, KindAck, field(tagSeq, overlong), sender), "malformed number"},
		{"a tag in two bytes", append(raw(1, KindAck, sender), 0x82, 0x00, 1, 7), "shortest form"},
		{"a length in two bytes", append(raw(1, KindAck, sender), byte(tagSeq), 0x81, 0x00, 7), "shortest form"},
		{"a version in two bytes", append([]byte{0x81, 0x00, byte(KindAck)}, sender...), "shortest form"},
		{"no sender", raw(1, KindAck), "without a sender"},
		{"a packet over MaxPacketBytes", raw(1, KindAck, sender, field(99, make([]byte, MaxPacketBytes))), "over a limit"},
	} {
		_, err := DecodePacket(tc.b)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err %v, want one saying %q", tc.name, err, tc.want)
		}
	}
	for _, tc := range []struct {
		name string
		b    []byte
		want string
	}{
		{"a sync without a sender", raw(1, KindSync), "without a sender"},
		{"a sync over MaxSyncBytes", raw(1, KindSync, field(tagSyncFrom, entry(nil)), field(99, make([]byte, MaxSyncBytes))), "over a limit"},
		{"a sync of too many members", raw(1, KindSync, append([]enc{field(tagSyncFrom, entry(nil))}, repeat(field(tagSyncMember, entry(nil)), MaxSyncMembers+1)...)...), "members"},
	} {
		_, err := DecodeSync(tc.b)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err %v, want one saying %q", tc.name, err, tc.want)
		}
	}
}

func repeat(f enc, n int) []enc {
	out := make([]enc, n)
	for i := range out {
		out[i] = f
	}
	return out
}

// An entry in a state this version doesn't know is skipped alone: the rest of
// the packet, its ack among it, still counts.
func TestAnUnknownStateSkipsOnlyItsEntry(t *testing.T) {
	newer := field(tagUpdate, enc(nil).nested(tagUpdateMember, entry(func(e enc) enc { return e.uvarint(tagState, 7) })))
	known := field(tagUpdate, enc(nil).nested(tagUpdateMember, enc(nil).str(tagID, "c")))
	p, err := DecodePacket(raw(1, KindAck, field(tagSeq, num(7)), sender, newer, known))
	if err != nil {
		t.Fatal(err)
	}
	if p.Body != (msg.Ack{Seq: 7}) || len(p.Gossip) != 1 || p.Gossip[0].Member.ID != "c" {
		t.Fatalf("decoded %+v", p)
	}
	s, err := DecodeSync(raw(1, KindSync, field(tagSyncFrom, entry(nil)), field(tagSyncMember, entry(func(e enc) enc { return e.uvarint(tagState, 7) })), field(tagSyncMember, enc(nil).str(tagID, "c"))))
	if err != nil || len(s.View) != 1 || s.View[0].ID != "c" {
		t.Fatalf("sync %+v, %v", s, err)
	}
}

func TestTheEncoderRefusesWhatDecodersWould(t *testing.T) {
	long := member.ID(strings.Repeat("x", MaxIDBytes+1))
	for name, p := range map[string]msg.Packet{
		"a long ID":            {From: member.Member{ID: long}, Body: msg.Ack{}},
		"a long address":       {From: member.Member{ID: "a", Addr: strings.Repeat("x", MaxAddrBytes+1)}, Body: msg.Ack{}},
		"no ID":                {From: member.Member{}, Body: msg.Ack{}},
		"an update's ID":       {From: member.Member{ID: "a"}, Body: msg.Ack{}, Gossip: []msg.Update{{Member: member.Member{ID: long}}}},
		"an incarnation":       {From: member.Member{ID: "a", Incarnation: member.MaxIncarnation + 1}, Body: msg.Ack{}},
		"too many updates":     {From: member.Member{ID: "a"}, Body: msg.Ack{}, Gossip: make([]msg.Update, MaxUpdates+1)},
		"a state":              {From: member.Member{ID: "a", State: member.Left + 1}, Body: msg.Ack{}},
		"an update's state":    {From: member.Member{ID: "a"}, Body: msg.Ack{}, Gossip: []msg.Update{{Member: member.Member{ID: "b", State: member.Left + 1}}}},
		"a raiser's ID":        {From: member.Member{ID: "a"}, Body: msg.Ack{}, Gossip: []msg.Update{{Member: member.Member{ID: "b"}, By: long}}},
		"a ping's target":      {From: member.Member{ID: "a"}, Body: msg.Ping{Target: long}},
		"a ping-req's target":  {From: member.Member{ID: "a"}, Body: msg.PingReq{Target: long}},
		"a ping-req's address": {From: member.Member{ID: "a"}, Body: msg.PingReq{Target: "b", TargetAddr: strings.Repeat("x", MaxAddrBytes+1)}},
		"too many bytes":       {From: member.Member{ID: "a"}, Body: msg.Ack{}, Gossip: bigUpdates(MaxUpdates)},
	} {
		if _, err := EncodePacket(p); err == nil {
			t.Errorf("encoded %s", name)
		}
	}
	for name, s := range map[string]msg.Sync{
		"a long ID":        {From: member.Member{ID: long}},
		"a member's ID":    {From: member.Member{ID: "a"}, View: []member.Member{{ID: long}}},
		"a member's state": {From: member.Member{ID: "a"}, View: []member.Member{{ID: "b", State: member.Left + 1}}},
		"too many members": {From: member.Member{ID: "a"}, View: slices.Repeat([]member.Member{{ID: "b"}}, MaxSyncMembers+1)},
		"too many bytes":   {From: member.Member{ID: "a"}, View: gossipMembers(bigUpdates(MaxSyncBytes / 500))},
	} {
		if _, err := EncodeSync(s); err == nil {
			t.Errorf("encoded a sync with %s", name)
		}
	}
	// What is just within every limit encodes and decodes.
	b, err := EncodePacket(msg.Packet{From: member.Member{ID: "a"}, Body: msg.PingReq{Target: long[1:], TargetAddr: strings.Repeat("x", MaxAddrBytes)}, Gossip: bigUpdates(80)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodePacket(b); err != nil {
		t.Fatalf("a packet the encoder wrote doesn't decode: %v", err)
	}
}

// bigUpdates returns n updates of the longest ID, address and raiser.
func bigUpdates(n int) []msg.Update {
	us := make([]msg.Update, n)
	for i := range us {
		id := fmt.Sprintf("%0*d", MaxIDBytes, i)
		us[i] = msg.Update{Member: member.Member{ID: member.ID(id), Addr: strings.Repeat("x", MaxAddrBytes)}, By: member.ID(id)}
	}
	return us
}

func gossipMembers(us []msg.Update) []member.Member {
	out := make([]member.Member, len(us))
	for i, u := range us {
		out[i] = u.Member
	}
	return out
}

// A codec asked the size of a version it doesn't write answers more than
// any budget.
func TestACodecSizesWhatItCantWriteAsNotFitting(t *testing.T) {
	c, _ := NewCodec(1, 1)
	if c.UpdateSize(msg.Update{Member: member.Member{ID: "a"}}, 2) < 1<<20 || c.PacketSize(msg.Packet{}, 2) < 1<<20 {
		t.Fatal("a version the codec doesn't write fits a packet")
	}
}

func TestNewCodecRefusesBrokenFormats(t *testing.T) {
	if _, err := NewCodec(1, 2, Format{Version: 2}); err == nil {
		t.Error("a format without its functions")
	}
	if _, err := NewCodec(1, 1, Format{Version: 1}); err == nil {
		t.Error("a format replacing version 1")
	}
	if _, err := NewCodec(0, 1); err == nil {
		t.Error("versions from 0")
	}
}
