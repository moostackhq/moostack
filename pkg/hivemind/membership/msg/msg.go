// Package msg is the membership protocol's messages, as Go values; package
// wire turns them into bytes.
package msg

import "github.com/moostackhq/moostack/pkg/hivemind/membership/member"

// Limits of one packet: a node builds no packet past them, whatever version
// it writes.
const (
	// MaxPacketBytes is the largest packet, on every transport.
	MaxPacketBytes = 65507
	MaxUpdates     = 1024
	MaxIDBytes     = 255 // of a member's ID, in an entry or a message
	MaxAddrBytes   = 255
)

// Body is what a packet carries besides updates: Ping, Ack, PingReq, Nack
// or Gossip.
type Body interface{ body() }

// Ping asks Target to ack. A helper of an indirect probe sends it with a
// sequence number of its own.
type Ping struct {
	Seq    uint32
	Target member.ID // the member asked to ack
}

// Ack answers a Ping with its sequence number.
type Ack struct {
	Seq uint32
}

// PingReq asks a helper to ping Target for the sender, and to forward the
// ack.
type PingReq struct {
	Seq        uint32
	Target     member.ID
	TargetAddr string
}

// Nack tells the sender of a PingReq that the helper got no ack from the
// target in time: the helper itself is reachable (Lifeguard).
type Nack struct {
	Seq uint32
}

// Gossip carries only updates: a node sends it on a timer, apart from its
// probes.
type Gossip struct{}

func (Gossip) body()  {}
func (Ping) body()    {}
func (Ack) body()     {}
func (PingReq) body() {}
func (Nack) body()    {}

// Update is an entry on its way through the cluster. By is the member that
// raised a suspicion; suspicions of the same entry by different members
// count as confirmations.
type Update struct {
	Member member.Member
	By     member.ID
}

// Packet is what goes in one packet of the transport: the sender's own entry, a probe
// message, and as many updates as fit.
type Packet struct {
	From   member.Member
	Body   Body
	Gossip []Update
}

// Sync carries a member's whole view, on a stream, when a node joins and
// every so often after. Reply asks the receiver to answer
// with its own view.
type Sync struct {
	From  member.Member
	View  []member.Member
	Reply bool
}
