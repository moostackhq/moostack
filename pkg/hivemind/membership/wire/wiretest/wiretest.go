// Package wiretest has wire formats for testing protocol upgrades: formats
// that break format 1.
package wiretest

import (
	"encoding/binary"
	"errors"

	"github.com/moostackhq/moostack/pkg/hivemind/membership/msg"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/wire"
)

// Breaking returns a format for version v that no older version can read:
// format 1's fields with every byte after the header inverted: format 1's
// decoder fails on it even ignoring the version. Its sizes are format
// 1's, with the header's version as long as v makes it.
func Breaking(v uint8) wire.Format {
	encode := func(b []byte) []byte {
		_, n := binary.Uvarint(b)
		out := binary.AppendUvarint(nil, uint64(v))
		out = append(out, b[n]) // the kind, unchanged
		for _, c := range b[n+1:] {
			out = append(out, ^c)
		}
		return out
	}
	decode := func(b []byte) ([]byte, error) {
		got, n := binary.Uvarint(b)
		if n <= 0 || got != uint64(v) || len(b) <= n {
			return nil, errors.New("wiretest: not this format")
		}
		out := binary.AppendUvarint(nil, 1)
		out = append(out, b[n])
		for _, c := range b[n+1:] {
			out = append(out, ^c)
		}
		return out, nil
	}
	f1 := wire.Format1
	return wire.Format{
		Version: v,
		EncodePacket: func(p msg.Packet) ([]byte, error) {
			b, err := f1.EncodePacket(p)
			if err != nil {
				return nil, err
			}
			return encode(b), nil
		},
		DecodePacket: func(b []byte) (msg.Packet, error) {
			d, err := decode(b)
			if err != nil {
				return msg.Packet{}, err
			}
			return f1.DecodePacket(d)
		},
		EncodeSync: func(s msg.Sync) ([]byte, error) {
			b, err := f1.EncodeSync(s)
			if err != nil {
				return nil, err
			}
			return encode(b), nil
		},
		DecodeSync: func(b []byte) (msg.Sync, error) {
			d, err := decode(b)
			if err != nil {
				return msg.Sync{}, err
			}
			return f1.DecodeSync(d)
		},
		PacketSize: func(p msg.Packet) int { return f1.PacketSize(p) - 1 + len(binary.AppendUvarint(nil, uint64(v))) },
		UpdateSize: f1.UpdateSize,
	}
}
