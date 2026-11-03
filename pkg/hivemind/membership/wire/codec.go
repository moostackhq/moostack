package wire

import (
	"errors"
	"fmt"
	"math"

	"github.com/moostackhq/moostack/pkg/hivemind/membership/msg"
)

// A message's header is the wire version as a uvarint, then the kind as one
// byte. A node reads the version from it, and drops a message in a version
// it doesn't read.

// Format is how one wire version encodes messages.
type Format struct {
	Version      uint8
	EncodePacket func(msg.Packet) ([]byte, error)
	DecodePacket func([]byte) (msg.Packet, error)
	EncodeSync   func(msg.Sync) ([]byte, error)
	DecodeSync   func([]byte) (msg.Sync, error)
	PacketSize   func(msg.Packet) int
	UpdateSize   func(msg.Update) int
}

// Format1 is wire version 1, this package's own functions.
var Format1 = Format{
	Version: 1, EncodePacket: EncodePacket, DecodePacket: DecodePacket,
	EncodeSync: EncodeSync, DecodeSync: DecodeSync, PacketSize: packetSize, UpdateSize: updateSize,
}

// ErrVersion is a message in a wire version the codec doesn't read.
var ErrVersion = errors.New("wire: a version this node doesn't read")

// Codec reads and writes the wire versions from Min to Max. A node writes
// to each member the highest version both read; the entries carry each
// member's range.
type Codec struct {
	min, max uint8
	formats  map[uint8]Format
}

// NewCodec returns a codec for the versions from min to max, with format 1
// and the formats given for the others. Every version in the range needs
// one.
func NewCodec(min, max uint8, formats ...Format) (*Codec, error) {
	c := &Codec{min: min, max: max, formats: map[uint8]Format{1: Format1}}
	for _, f := range formats {
		switch {
		case f.Version <= 1:
			return nil, fmt.Errorf("wire: a format for version %d: version 1 is built in", f.Version)
		case f.EncodePacket == nil || f.DecodePacket == nil || f.EncodeSync == nil || f.DecodeSync == nil || f.PacketSize == nil || f.UpdateSize == nil:
			return nil, fmt.Errorf("wire: the format for version %d lacks a function", f.Version)
		}
		c.formats[f.Version] = f
	}
	if min == 0 || max < min {
		return nil, fmt.Errorf("wire: versions %d to %d", min, max)
	}
	for v := min; ; v++ {
		if _, ok := c.formats[v]; !ok {
			return nil, fmt.Errorf("wire: no format for version %d", v)
		}
		if v == max {
			break
		}
	}
	return c, nil
}

// Range returns the lowest and highest version the codec reads and writes.
func (c *Codec) Range() (min, max uint8) { return c.min, c.max }

func (c *Codec) format(v uint8) (Format, error) {
	if v < c.min || v > c.max {
		return Format{}, fmt.Errorf("%w: %d, not %d to %d", ErrVersion, v, c.min, c.max)
	}
	return c.formats[v], nil
}

func (c *Codec) peek(b []byte) (Format, error) {
	v, _, err := PeekKind(b)
	if err != nil {
		return Format{}, err
	}
	if v > 255 {
		return Format{}, fmt.Errorf("%w: %d", ErrVersion, v)
	}
	return c.format(uint8(v))
}

func (c *Codec) EncodePacket(p msg.Packet, v uint8) ([]byte, error) {
	f, err := c.format(v)
	if err != nil {
		return nil, err
	}
	return f.EncodePacket(p)
}

// DecodePacket reads a packet in any version the codec reads.
func (c *Codec) DecodePacket(b []byte) (msg.Packet, error) {
	f, err := c.peek(b)
	if err != nil {
		return msg.Packet{}, err
	}
	return f.DecodePacket(b)
}

func (c *Codec) EncodeSync(s msg.Sync, v uint8) ([]byte, error) {
	f, err := c.format(v)
	if err != nil {
		return nil, err
	}
	return f.EncodeSync(s)
}

// DecodeSync reads a sync in any version the codec reads.
func (c *Codec) DecodeSync(b []byte) (msg.Sync, error) {
	f, err := c.peek(b)
	if err != nil {
		return msg.Sync{}, err
	}
	return f.DecodeSync(b)
}

// noFit is the size of what a codec can't write: more than any budget.
const noFit = math.MaxInt32

// PacketSize is p's size in version v; for a version the codec doesn't
// write, more than any budget.
func (c *Codec) PacketSize(p msg.Packet, v uint8) int {
	f, err := c.format(v)
	if err != nil {
		return noFit
	}
	return f.PacketSize(p)
}

// UpdateSize is what u adds to a packet in version v; for a version the
// codec doesn't write, more than any budget.
func (c *Codec) UpdateSize(u msg.Update, v uint8) int {
	f, err := c.format(v)
	if err != nil {
		return noFit
	}
	return f.UpdateSize(u)
}
