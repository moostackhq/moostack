package wire

import (
	"encoding/binary"
	"errors"

	"github.com/moostackhq/moostack/pkg/hivemind/membership/msg"
)

// breaking2 is a wire version 2 for the fuzzer: format 1 with every byte
// after the header inverted, as wiretest.Breaking makes it.
var breaking2 = func() Format {
	flip := func(b []byte, v uint64) []byte {
		_, n := binary.Uvarint(b)
		out := binary.AppendUvarint(nil, v)
		out = append(out, b[n])
		for _, c := range b[n+1:] {
			out = append(out, ^c)
		}
		return out
	}
	unflip := func(b []byte) ([]byte, error) {
		v, n := binary.Uvarint(b)
		if n <= 0 || v != 2 || len(b) <= n {
			return nil, errors.New("not version 2")
		}
		return flip(b, 1), nil
	}
	return Format{
		Version: 2,
		EncodePacket: func(p msg.Packet) ([]byte, error) {
			b, err := EncodePacket(p)
			if err != nil {
				return nil, err
			}
			return flip(b, 2), nil
		},
		DecodePacket: func(b []byte) (msg.Packet, error) {
			d, err := unflip(b)
			if err != nil {
				return msg.Packet{}, err
			}
			return DecodePacket(d)
		},
		EncodeSync: func(s msg.Sync) ([]byte, error) {
			b, err := EncodeSync(s)
			if err != nil {
				return nil, err
			}
			return flip(b, 2), nil
		},
		DecodeSync: func(b []byte) (msg.Sync, error) {
			d, err := unflip(b)
			if err != nil {
				return msg.Sync{}, err
			}
			return DecodeSync(d)
		},
		PacketSize: packetSize, UpdateSize: updateSize,
	}
}()
