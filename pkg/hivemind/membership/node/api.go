package node

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/moostackhq/moostack/pkg/hivemind/membership/gossip"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/health"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/member"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/msg"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/probe"
)

// Config is a node's identity and the protocol's settings.
type Config struct {
	ID   member.ID
	Addr string
	// MinVersion and Version are the lowest and highest wire versions the
	// node reads and writes, and Caps the protocol behaviours it has. All go
	// in its entry. To each member the node writes the highest version both
	// read.
	MinVersion uint8
	Version    uint8
	Caps       member.Caps
	// Incarnation is the node's first incarnation. A process that restarts
	// must begin above any incarnation its earlier runs reached; its entry
	// then supersedes theirs at once, new address, capabilities and wire
	// versions included. The runtime sets it from the clock, in
	// milliseconds.
	Incarnation uint64

	Probe probe.Config

	// HealthMax caps the local health score at HealthMax-1; 1 turns local
	// health awareness off (Lifeguard 4.1).
	HealthMax  int
	HealthRule health.Rule
	// SuspicionMult sets the shortest suspicion timeout, SuspicionMult ×
	// max(1, log10 n) protocol periods (Lifeguard 5.3). Dogpile lets independent confirmations
	// shorten it from SuspicionMaxMult times that, falling to it after
	// Confirmations of them (Lifeguard 4.2). Without Dogpile, every
	// suspicion lasts the shortest timeout.
	SuspicionMult    int
	SuspicionMaxMult int
	Confirmations    int
	Dogpile          bool
	// Buddy puts a member's suspicion of the member it pings first in the
	// ping (Lifeguard 4.3).
	Buddy bool

	// RetransmitMult: each update is gossiped RetransmitMult × ⌈log10(n+1)⌉
	// times. MaxPacket is the most bytes a packet the node sends has, at
	// least MinPacket.
	// GossipInterval: besides piggybacking on probes, every GossipInterval
	// a node with updates queued sends them to GossipNodes random members
	// that have CapGossip. 0 turns it off.
	RetransmitMult int
	MaxPacket      int
	GossipInterval time.Duration
	GossipNodes    int

	// SyncEvery: every SyncEvery protocol periods, swap whole views with a
	// random live member, and every SeedSyncEvery-th such swap with a seed.
	SyncEvery     int
	SeedSyncEvery int
	// RetryJoinEvery: while it knows no other live member, a joining node
	// tries the next few of its seeds this often, in protocol periods.
	RetryJoinEvery int
	// DeadTimeout is how long a gone member stays in the view, and
	// GraveTimeout how long the view then remembers it was forgotten: stale
	// entries of it are ignored, and healing probes still reach it.
	DeadTimeout  time.Duration
	GraveTimeout time.Duration
	// MaxMembers caps the members the view holds: entries of new members
	// beyond it, their own included, are ignored until a gone member makes
	// room. 0 is no cap.
	MaxMembers int
	// LateLimit: a Step this much later than the wake it asked for means the
	// node itself was paused. It then moves its deadlines on by the delay,
	// so the answers that queued up still count. 0 turns this off.
	LateLimit time.Duration

	// Seed seeds the node's randomness, with its ID: nodes of one seed but
	// different IDs draw differently, and one seed replays one run. The
	// runtime picks one at random when it is 0.
	Seed uint64
}

// Defaults is a node with every Lifeguard extension on, timed for a LAN.
func Defaults(id member.ID, addr string) Config {
	return Config{
		ID: id, Addr: addr, MinVersion: 1, Version: 1, Caps: member.Of(member.CapNack, member.CapGossip),
		Probe: probe.Config{
			Interval: time.Second, Timeout: 300 * time.Millisecond,
			Helpers: 3, RTTMult: 3, HealEvery: 5, Nacks: true,
		},
		HealthMax: 8, HealthRule: health.RuleNoNack,
		SuspicionMult: 4, SuspicionMaxMult: 6, Confirmations: 3, Dogpile: true, Buddy: true,
		RetransmitMult: 4, MaxPacket: 1150,
		GossipInterval: 200 * time.Millisecond, GossipNodes: 3,
		SyncEvery: 30, SeedSyncEvery: 4, RetryJoinEvery: 2,
		DeadTimeout: time.Hour, GraveTimeout: 24 * time.Hour, LateLimit: 250 * time.Millisecond, MaxMembers: 10000,
	}
}

// Validate reports a configuration a node can't run on with sz, the codec's
// sizes: the runtime calls it before it starts one.
func (c Config) Validate(sz gossip.Sizer) error {
	switch {
	case c.ID == "" || len(c.ID) > msg.MaxIDBytes:
		return fmt.Errorf("node: an ID of %d bytes, want 1 to %d", len(c.ID), msg.MaxIDBytes)
	case len(c.Addr) > msg.MaxAddrBytes:
		return fmt.Errorf("node: an address of %d bytes, want at most %d", len(c.Addr), msg.MaxAddrBytes)
	case c.MinVersion == 0 || c.Version < c.MinVersion:
		return fmt.Errorf("node: wire versions %d to %d, want 1 or more, lowest first", c.MinVersion, c.Version)
	case c.Probe.Interval <= 0 || c.Probe.Timeout <= 0:
		return fmt.Errorf("node: a probe interval of %v and timeout of %v, want both above 0", c.Probe.Interval, c.Probe.Timeout)
	case sz == nil:
		return errors.New("node: no Sizer")
	case c.MaxPacket < c.MinPacket(sz) || c.MaxPacket > msg.MaxPacketBytes:
		return fmt.Errorf("node: a MaxPacket of %d, want %d to %d", c.MaxPacket, c.MinPacket(sz), msg.MaxPacketBytes)
	case c.HealthRule > health.RuleLifeguard:
		return fmt.Errorf("node: health rule %d, which isn't one", c.HealthRule)
	case c.SuspicionMult < 1:
		return fmt.Errorf("node: a SuspicionMult of %d, want 1 or more", c.SuspicionMult)
	case c.Incarnation > member.MaxIncarnation:
		return fmt.Errorf("node: incarnation %d over the most", c.Incarnation)
	}
	for _, f := range []struct {
		name string
		v    int64
	}{
		{"Probe.Helpers", int64(c.Probe.Helpers)}, {"Probe.RTTMult", int64(c.Probe.RTTMult)}, {"Probe.HealEvery", int64(c.Probe.HealEvery)},
		{"HealthMax", int64(c.HealthMax)}, {"SuspicionMaxMult", int64(c.SuspicionMaxMult)}, {"Confirmations", int64(c.Confirmations)},
		{"RetransmitMult", int64(c.RetransmitMult)}, {"GossipInterval", int64(c.GossipInterval)}, {"GossipNodes", int64(c.GossipNodes)},
		{"SyncEvery", int64(c.SyncEvery)}, {"SeedSyncEvery", int64(c.SeedSyncEvery)}, {"RetryJoinEvery", int64(c.RetryJoinEvery)},
		{"DeadTimeout", int64(c.DeadTimeout)}, {"GraveTimeout", int64(c.GraveTimeout)}, {"MaxMembers", int64(c.MaxMembers)}, {"LateLimit", int64(c.LateLimit)},
	} {
		// Each turns its part off or down at 0, and none may be negative.
		if f.v < 0 {
			return fmt.Errorf("node: a negative %s, %d", f.name, f.v)
		}
	}
	return nil
}

// MinPacket is the smallest MaxPacket c can have: the largest packet the
// node sends with no gossip in it, a ping-req for a member of the longest
// ID and address, from the node's own entry at the highest incarnation, in
// any version it writes, as sz sizes it. Gossip, buddy entries included,
// goes only as far as it fits, so no packet is larger than MaxPacket.
func (c Config) MinPacket(sz gossip.Sizer) int {
	self := member.Member{ID: c.ID, Addr: c.Addr, Incarnation: member.MaxIncarnation, State: member.Left, Caps: c.Caps, MinVersion: c.MinVersion, Version: c.Version}
	req := msg.PingReq{Seq: math.MaxUint32, Target: member.ID(strings.Repeat("x", msg.MaxIDBytes)), TargetAddr: strings.Repeat("x", msg.MaxAddrBytes)}
	most := 0
	for v := int(c.MinVersion); v <= int(c.Version); v++ {
		most = max(most, sz.PacketSize(msg.Packet{From: self, Body: req}, uint8(v)))
	}
	return most
}

// Input is what Step takes: Tick, Receive, ReceiveSync, Join, Leave,
// Forget or Unreadable.
type Input interface{ input() }

// Tick is a wake-up the node asked for.
type Tick struct{}

// Receive is a packet that arrived from Addr, from Peer if the transport
// proved who sent it (transport.Message.Peer).
type Receive struct {
	Addr   string
	Peer   member.ID
	Packet msg.Packet
}

// ReceiveSync is a whole view that arrived on a stream from Addr, from
// Peer if the transport proved who sent it.
type ReceiveSync struct {
	Addr string
	Peer member.ID
	Sync msg.Sync
}

// Join keeps the seeds for the periodic seed syncs, lifts the hold Forget
// put on each seed's grave, and contacts a few of the seeds if they are the
// node's first, it has no other live member, or it had left. A seed with no
// ID or no address, or with the node's own ID or address, is skipped.
type Join struct{ Seeds []Seed }

// Seed is a member to join through: its ID and its address.
type Seed struct {
	ID   member.ID
	Addr string
}

// Forget removes the member ID from the view at once, reports it in
// Result.Forgotten and holds its grave: until a Join names it as a seed,
// past GraveTimeout too, the node ignores every entry of it, at
// any incarnation, probes it no more, and it is no longer a seed. A probe
// of it under way runs out its period with no verdict. A member already
// buried, or one the node keeps nothing of, gets a held grave too, and
// isn't reported. Forgetting the node itself, or the empty ID, does
// nothing.
type Forget struct{ ID member.ID }

// Leave tells the cluster the node is leaving. It stops probing.
type Leave struct{}

// Unreadable is a message from Addr in a wire version the node doesn't
// read. The node answers with its own entry, which names the versions it
// reads, in the lowest version it writes.
type Unreadable struct {
	Addr    string
	Peer    member.ID // proven by the transport, if it proves who sends
	Version uint64    // the version the message said it was in
}

func (Tick) input()        {}
func (Receive) input()     {}
func (ReceiveSync) input() {}
func (Join) input()        {}
func (Leave) input()       {}
func (Forget) input()      {}
func (Unreadable) input()  {}

// Packet is a packet to send, in wire version Version.
type Packet struct {
	To      member.ID
	Addr    string
	Version uint8
	Packet  msg.Packet
}

// Sync is a whole view to send on a stream, in wire version Version.
//
// To is the member the node means it for, whose identity an authenticating
// transport checks at Addr (transport.Dest).
type Sync struct {
	To      member.ID
	Addr    string
	Version uint8
	Sync    msg.Sync
}

// EventKind is what happened, for observers.
type EventKind uint8

const (
	ProbeFailed   EventKind = iota // Member failed a probe; Value is the nacks
	Suspected                      // Member became suspect, raised By
	Confirmed                      // By confirmed the suspicion of Member; Value counts them
	Refuted                        // the node refuted a suspicion of itself; Value is the new incarnation
	Declared                       // Member became dead or left
	Stalled                        // the node was paused for Late
	HealthChanged                  // the local health score is now Value
	Incompatible                   // Member reads no wire version the node writes
	WrongVersion                   // a message came in a wire version the node doesn't read, Value
	Refused                        // the view is full: Member's entry, from By, was left out
	Superseded                     // the node superseded an entry of its own another run left: a restart or a twin
	Impersonated                   // a message from peer By said it was from Member: dropped
)

// Event is one thing that happened in a Step.
type Event struct {
	Kind   EventKind
	Member member.ID
	By     member.ID
	Value  int
	Late   time.Duration
	// Incarnation is the entry's, for Suspected, Confirmed and Declared, and
	// the node's new one for Refuted and Superseded.
	Incarnation uint64
}

// Result is what one Step asks for.
type Result struct {
	Packets []Packet
	Syncs   []Sync
	Changes []member.Member // entries of other members that changed
	// Forgotten are the members the step forgot that are out of the view at
	// its end, each once, with the entry it was last forgotten at; one of
	// them changed in the step changed before it was last forgotten.
	Forgotten []member.Member
	Events    []Event
	// Wake is when to call Step with a Tick next; the zero time when the
	// node needs no wake-up, as after it left with nothing more to time.
	Wake time.Time
}
