// Package wire turns the membership protocol's messages into bytes and
// back.
//
// A message is its wire version (uvarint), its kind (one byte), then
// fields. Each field is a tag (uvarint), a length (uvarint) and that many
// bytes: an unsigned number as a uvarint, a string as its bytes, an entry
// or an update as fields of its own. A decoder skips fields with tags it
// doesn't know, and returns ErrUnknownKind for a kind it doesn't know,
// which the receiver drops.
package wire

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/moostackhq/moostack/pkg/hivemind/membership/member"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/msg"
)

// Version is the wire version this package's functions write: format 1.
// A Codec reads and writes the versions it is given formats for.
const Version = 1

type Kind byte

const (
	KindPing    Kind = 1
	KindAck     Kind = 2
	KindPingReq Kind = 3
	KindNack    Kind = 4
	KindSync    Kind = 5
	KindGossip  Kind = 6
)

// Field tags of a packet.
const (
	tagFrom       = 1 // the sender's entry
	tagSeq        = 2
	tagTarget     = 3
	tagTargetAddr = 4
	tagUpdate     = 5 // repeated
)

// Field tags of a sync.
const (
	tagSyncFrom   = 1
	tagSyncMember = 2 // repeated
	tagSyncReply  = 3
)

// Field tags of an entry.
const (
	tagID          = 1
	tagAddr        = 2
	tagIncarnation = 3
	tagState       = 4
	tagCaps        = 5
	tagVersion     = 6
	tagMinVersion  = 7
)

// Field tags of an update.
const (
	tagUpdateMember = 1
	tagUpdateBy     = 2
)

// Limits on what a decoder accepts, whoever sent it.
const (
	MaxPacketBytes = msg.MaxPacketBytes
	MaxSyncBytes   = 8 << 20 // a whole view of some 100,000 members
	MaxIDBytes     = msg.MaxIDBytes
	MaxAddrBytes   = msg.MaxAddrBytes
	MaxUpdates     = msg.MaxUpdates
	MaxSyncMembers = 1 << 17
)

var (
	errTooBig = errors.New("wire: over a limit")
	// errUnknownState is an entry in a state this version doesn't know. The
	// entry is skipped, the rest of the message read.
	errUnknownState = errors.New("wire: unknown state")
	// ErrUnknownKind is a message of a kind this version doesn't know.
	ErrUnknownKind = errors.New("wire: unknown message kind")
	errShort       = errors.New("wire: message cut short")
)

type enc []byte

func (e enc) uvarint(tag, v uint64) enc {
	if v == 0 {
		return e // absent means zero
	}
	e = binary.AppendUvarint(e, tag)
	e = binary.AppendUvarint(e, uint64(uvarintLen(v)))
	return binary.AppendUvarint(e, v)
}

func (e enc) bytes(tag uint64, b []byte) enc {
	if len(b) == 0 {
		return e
	}
	e = binary.AppendUvarint(e, tag)
	e = binary.AppendUvarint(e, uint64(len(b)))
	return append(e, b...)
}

func (e enc) str(tag uint64, s string) enc { return e.bytes(tag, []byte(s)) }

// nested writes a field holding fields, even when it is empty.
func (e enc) nested(tag uint64, b []byte) enc {
	e = binary.AppendUvarint(e, tag)
	e = binary.AppendUvarint(e, uint64(len(b)))
	return append(e, b...)
}

func uvarintLen(v uint64) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n++
	}
	return n
}

func encodeMember(m member.Member) []byte {
	var e enc
	e = e.str(tagID, string(m.ID))
	e = e.str(tagAddr, m.Addr)
	e = e.uvarint(tagIncarnation, m.Incarnation)
	e = e.uvarint(tagState, uint64(m.State))
	e = e.uvarint(tagCaps, uint64(m.Caps))
	e = e.uvarint(tagVersion, uint64(m.Version))
	e = e.uvarint(tagMinVersion, uint64(m.MinVersion))
	return e
}

func encodeUpdate(u msg.Update) []byte {
	var e enc
	e = e.nested(tagUpdateMember, encodeMember(u.Member))
	return e.str(tagUpdateBy, string(u.By))
}

// checkMember refuses an entry decoders refuse.
func checkMember(m member.Member) error {
	switch {
	case m.ID == "" || len(m.ID) > MaxIDBytes:
		return fmt.Errorf("wire: an ID of %d bytes, want 1 to %d", len(m.ID), MaxIDBytes)
	case len(m.Addr) > MaxAddrBytes:
		return fmt.Errorf("wire: an address of %d bytes, want at most %d", len(m.Addr), MaxAddrBytes)
	case m.Incarnation > member.MaxIncarnation:
		return fmt.Errorf("wire: incarnation %d over the most, %d", m.Incarnation, uint64(member.MaxIncarnation))
	case m.State > member.Left:
		return fmt.Errorf("wire: state %d, which format 1 doesn't write", m.State)
	}
	return nil
}

// checkLen refuses a string field longer than decoders take.
func checkLen(what, s string, most int) error {
	if len(s) > most {
		return fmt.Errorf("wire: %s of %d bytes, want at most %d", what, len(s), most)
	}
	return nil
}

// EncodePacket returns p's bytes, or an error for a packet decoders refuse.
func EncodePacket(p msg.Packet) ([]byte, error) {
	if len(p.Gossip) > MaxUpdates {
		return nil, fmt.Errorf("wire: %d updates, want at most %d", len(p.Gossip), MaxUpdates)
	}
	if err := checkMember(p.From); err != nil {
		return nil, err
	}
	for _, u := range p.Gossip {
		if err := checkMember(u.Member); err != nil {
			return nil, err
		}
		if err := checkLen("a raiser ID", string(u.By), MaxIDBytes); err != nil {
			return nil, err
		}
	}
	e := enc(binary.AppendUvarint(nil, Version))
	switch b := p.Body.(type) {
	case msg.Ping:
		if err := checkLen("a target ID", string(b.Target), MaxIDBytes); err != nil {
			return nil, err
		}
		e = append(e, byte(KindPing))
		e = e.uvarint(tagSeq, uint64(b.Seq)).str(tagTarget, string(b.Target))
	case msg.Ack:
		e = append(e, byte(KindAck))
		e = e.uvarint(tagSeq, uint64(b.Seq))
	case msg.PingReq:
		if err := errors.Join(checkLen("a target ID", string(b.Target), MaxIDBytes), checkLen("a target address", b.TargetAddr, MaxAddrBytes)); err != nil {
			return nil, err
		}
		e = append(e, byte(KindPingReq))
		e = e.uvarint(tagSeq, uint64(b.Seq)).str(tagTarget, string(b.Target)).str(tagTargetAddr, b.TargetAddr)
	case msg.Nack:
		e = append(e, byte(KindNack))
		e = e.uvarint(tagSeq, uint64(b.Seq))
	case msg.Gossip:
		e = append(e, byte(KindGossip))
	default:
		return nil, fmt.Errorf("wire: no encoding for %T", p.Body)
	}
	e = e.nested(tagFrom, encodeMember(p.From))
	for _, u := range p.Gossip {
		e = e.nested(tagUpdate, encodeUpdate(u))
	}
	if len(e) > MaxPacketBytes {
		return nil, fmt.Errorf("wire: a %d-byte packet, want at most %d", len(e), MaxPacketBytes)
	}
	return e, nil
}

// EncodeSync returns s's bytes, or an error for a sync decoders refuse.
func EncodeSync(s msg.Sync) ([]byte, error) {
	if len(s.View) > MaxSyncMembers {
		return nil, fmt.Errorf("wire: %d members, want at most %d", len(s.View), MaxSyncMembers)
	}
	if err := checkMember(s.From); err != nil {
		return nil, err
	}
	for _, m := range s.View {
		if err := checkMember(m); err != nil {
			return nil, err
		}
	}
	// Sized before it is written: a view over MaxSyncBytes is refused
	// without encoding it.
	size := uvarintLen(Version) + 1 + nestedSize(tagSyncFrom, memberSize(s.From))
	for _, m := range s.View {
		size += nestedSize(tagSyncMember, memberSize(m))
	}
	if s.Reply {
		size += numberSize(tagSyncReply, 1)
	}
	if size > MaxSyncBytes {
		return nil, fmt.Errorf("wire: a %d-byte sync, want at most %d", size, MaxSyncBytes)
	}
	e := make(enc, 0, size)
	e = binary.AppendUvarint(e, Version)
	e = append(e, byte(KindSync))
	e = e.nested(tagSyncFrom, encodeMember(s.From))
	for _, m := range s.View {
		e = e.nested(tagSyncMember, encodeMember(m))
	}
	if s.Reply {
		e = e.uvarint(tagSyncReply, 1)
	}
	return e, nil
}

// uvarint reads a uvarint, refusing one not in its shortest form.
func uvarint(b []byte) (uint64, int, error) {
	x, n := binary.Uvarint(b)
	if n <= 0 {
		return 0, 0, errShort
	}
	if n != uvarintLen(x) {
		return 0, 0, errors.New("wire: a number not in its shortest form")
	}
	return x, n, nil
}

func fields(b []byte, fn func(tag uint64, v []byte) error) error {
	for len(b) > 0 {
		tag, n, err := uvarint(b)
		if err != nil {
			return err
		}
		b = b[n:]
		l, n, err := uvarint(b)
		if err != nil {
			return err
		}
		if l > uint64(len(b)-n) {
			return errShort
		}
		b = b[n:]
		if err := fn(tag, b[:l]); err != nil {
			return err
		}
		b = b[l:]
	}
	return nil
}

// number reads a field holding one uvarint, in its shortest form.
func number(v []byte) (uint64, error) {
	x, n := binary.Uvarint(v)
	if n <= 0 || n != len(v) || n != uvarintLen(x) {
		return 0, errors.New("wire: malformed number")
	}
	return x, nil
}

func decodeMember(b []byte) (member.Member, error) {
	var m member.Member
	err := fields(b, func(tag uint64, v []byte) error {
		var err error
		var x uint64
		switch tag {
		case tagID:
			if len(v) > MaxIDBytes {
				return fmt.Errorf("%w: a %d-byte ID", errTooBig, len(v))
			}
			m.ID = member.ID(v)
		case tagAddr:
			if len(v) > MaxAddrBytes {
				return fmt.Errorf("%w: a %d-byte address", errTooBig, len(v))
			}
			m.Addr = string(v)
		case tagIncarnation:
			m.Incarnation, err = number(v)
			if m.Incarnation > member.MaxIncarnation {
				err = fmt.Errorf("%w: incarnation %d", errTooBig, m.Incarnation)
			}
		case tagState:
			x, err = number(v)
			if err == nil && x > uint64(member.Left) {
				err = fmt.Errorf("%w %d", errUnknownState, x)
			}
			m.State = member.State(x)
		case tagCaps:
			x, err = number(v)
			m.Caps = member.Caps(x)
		case tagVersion:
			x, err = number(v)
			if x > 255 {
				err = errors.New("wire: version out of range")
			}
			m.Version = uint8(x)
		case tagMinVersion:
			x, err = number(v)
			if x > 255 {
				err = errors.New("wire: version out of range")
			}
			m.MinVersion = uint8(x)
		}
		return err
	})
	if err == nil && m.ID == "" {
		err = errors.New("wire: entry without an ID")
	}
	return m, err
}

func decodeUpdate(b []byte) (msg.Update, error) {
	var u msg.Update
	seen := false
	err := fields(b, func(tag uint64, v []byte) error {
		var err error
		switch tag {
		case tagUpdateMember:
			u.Member, err = decodeMember(v)
			seen = true
		case tagUpdateBy:
			if len(v) > MaxIDBytes {
				return fmt.Errorf("%w: a %d-byte ID", errTooBig, len(v))
			}
			u.By = member.ID(v)
		}
		return err
	})
	if err == nil && !seen {
		err = errors.New("wire: update without an entry")
	}
	return u, err
}

// header reads the version and the kind.
func header(b []byte) (uint64, Kind, []byte, error) {
	v, n, err := uvarint(b)
	if err != nil {
		return 0, 0, nil, err
	}
	if len(b) <= n {
		return 0, 0, nil, errShort
	}
	if v == 0 {
		return 0, 0, nil, errors.New("wire: version 0")
	}
	return v, Kind(b[n]), b[n+1:], nil
}

// PeekKind returns a message's version and kind without decoding it.
func PeekKind(b []byte) (uint64, Kind, error) {
	v, k, _, err := header(b)
	return v, k, err
}

// A sync or an unknown kind is an error.
func DecodePacket(b []byte) (msg.Packet, error) {
	if len(b) > MaxPacketBytes {
		return msg.Packet{}, fmt.Errorf("%w: a %d-byte packet", errTooBig, len(b))
	}
	_, kind, rest, err := header(b)
	if err != nil {
		return msg.Packet{}, err
	}
	var (
		p                  msg.Packet
		seq                uint64
		target, targetAddr string
		from               bool
	)
	switch kind {
	case KindPing, KindAck, KindPingReq, KindNack, KindGossip:
	default:
		return msg.Packet{}, ErrUnknownKind
	}
	err = fields(rest, func(tag uint64, v []byte) error {
		var err error
		switch tag {
		case tagFrom:
			p.From, err = decodeMember(v)
			from = true
		case tagSeq:
			seq, err = number(v)
			if seq > 1<<32-1 {
				err = errors.New("wire: sequence number out of range")
			}
		case tagTarget:
			if len(v) > MaxIDBytes {
				return fmt.Errorf("%w: a %d-byte ID", errTooBig, len(v))
			}
			target = string(v)
		case tagTargetAddr:
			if len(v) > MaxAddrBytes {
				return fmt.Errorf("%w: a %d-byte address", errTooBig, len(v))
			}
			targetAddr = string(v)
		case tagUpdate:
			if len(p.Gossip) == MaxUpdates {
				return fmt.Errorf("%w: more than %d updates", errTooBig, MaxUpdates)
			}
			var u msg.Update
			u, err = decodeUpdate(v)
			if errors.Is(err, errUnknownState) {
				return nil
			}
			p.Gossip = append(p.Gossip, u)
		}
		return err
	})
	if err != nil {
		return msg.Packet{}, err
	}
	if !from {
		return msg.Packet{}, errors.New("wire: packet without a sender")
	}
	switch kind {
	case KindPing:
		p.Body = msg.Ping{Seq: uint32(seq), Target: member.ID(target)}
	case KindAck:
		p.Body = msg.Ack{Seq: uint32(seq)}
	case KindPingReq:
		p.Body = msg.PingReq{Seq: uint32(seq), Target: member.ID(target), TargetAddr: targetAddr}
	case KindNack:
		p.Body = msg.Nack{Seq: uint32(seq)}
	case KindGossip:
		p.Body = msg.Gossip{}
	}
	return p, nil
}

func DecodeSync(b []byte) (msg.Sync, error) {
	if len(b) > MaxSyncBytes {
		return msg.Sync{}, fmt.Errorf("%w: a %d-byte sync", errTooBig, len(b))
	}
	_, kind, rest, err := header(b)
	if err != nil {
		return msg.Sync{}, err
	}
	if kind != KindSync {
		return msg.Sync{}, ErrUnknownKind
	}
	var (
		s    msg.Sync
		from bool
	)
	err = fields(rest, func(tag uint64, v []byte) error {
		var err error
		switch tag {
		case tagSyncFrom:
			s.From, err = decodeMember(v)
			from = true
		case tagSyncMember:
			if len(s.View) == MaxSyncMembers {
				return fmt.Errorf("%w: more than %d members", errTooBig, MaxSyncMembers)
			}
			var m member.Member
			m, err = decodeMember(v)
			if errors.Is(err, errUnknownState) {
				return nil
			}
			s.View = append(s.View, m)
		case tagSyncReply:
			var x uint64
			x, err = number(v)
			s.Reply = x != 0
		}
		return err
	})
	if err != nil {
		return msg.Sync{}, err
	}
	if !from {
		return msg.Sync{}, errors.New("wire: sync without a sender")
	}
	return s, nil
}

// Sizer counts sizes as format 1 writes them, for the gossip queue, in
// whatever version it is asked: for nodes that only speak version 1.
type Sizer struct{}

// PacketSize is p's encoded size in format 1.
func (Sizer) PacketSize(p msg.Packet, _ uint8) int { return packetSize(p) }

// UpdateSize is what u adds to a packet in format 1.
func (Sizer) UpdateSize(u msg.Update, _ uint8) int { return updateSize(u) }

func packetSize(p msg.Packet) int {
	b, err := EncodePacket(p)
	if err != nil {
		return 0
	}
	return len(b)
}

// updateSize is what u adds to a packet: its tag, length and fields,
// counted without encoding them.
func updateSize(u msg.Update) int {
	n := nestedSize(tagUpdateMember, memberSize(u.Member)) + bytesSize(tagUpdateBy, len(u.By))
	return nestedSize(tagUpdate, n)
}

func memberSize(m member.Member) int {
	return bytesSize(tagID, len(m.ID)) + bytesSize(tagAddr, len(m.Addr)) +
		numberSize(tagIncarnation, m.Incarnation) + numberSize(tagState, uint64(m.State)) +
		numberSize(tagCaps, uint64(m.Caps)) + numberSize(tagVersion, uint64(m.Version)) +
		numberSize(tagMinVersion, uint64(m.MinVersion))
}

// The sizes of fields as enc writes them: absent when empty or zero, but
// a nested field always.
func bytesSize(tag uint64, n int) int {
	if n == 0 {
		return 0
	}
	return nestedSize(tag, n)
}

func numberSize(tag, v uint64) int {
	if v == 0 {
		return 0
	}
	return nestedSize(tag, uvarintLen(v))
}

func nestedSize(tag uint64, n int) int { return uvarintLen(tag) + uvarintLen(uint64(n)) + n }
