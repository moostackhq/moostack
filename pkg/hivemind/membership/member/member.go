// Package member is a member's entry in the membership, and the rule that
// decides which of two entries about the same member is newer.
package member

import (
	"cmp"
	"fmt"
	"strconv"
	"unicode"
)

// ID names a node for its whole life. Addresses change; IDs don't.
type ID string

// State is what an entry says about its member.
type State uint8

const (
	// Alive: the member answers probes, or none of its probes has failed.
	Alive State = iota
	// Suspect: a probe of the member failed, and it has a timeout to refute.
	Suspect
	// Dead: the member didn't refute in time.
	Dead
	// Left: the member said it was leaving.
	Left
)

func (s State) String() string {
	switch s {
	case Alive:
		return "alive"
	case Suspect:
		return "suspect"
	case Dead:
		return "dead"
	case Left:
		return "left"
	}
	return fmt.Sprintf("state(%d)", uint8(s))
}

// rank orders the states at one incarnation: an entry with a higher rank
// overrides one with a lower. Dead and Left are final for an incarnation,
// and a member's own word that it left outranks others finding it dead.
func (s State) rank() int { return int(s) }

// Gone reports whether the state takes the member out of the live view.
func (s State) Gone() bool { return s == Dead || s == Left }

// Capability is a protocol behaviour a member's software has. A node uses
// it towards a member only when the member's entry has it.
type Capability uint8

const (
	// CapNack: the member understands nacks from helpers of an indirect
	// probe (Lifeguard).
	CapNack Capability = iota
	// CapGossip: the member reads gossip packets, which carry updates
	// without a probe message, sent on a timer apart from probes.
	CapGossip
)

// Caps is a bit set of Capability values.
type Caps uint64

func (cs Caps) Has(c Capability) bool { return cs&(1<<c) != 0 }

func (cs Caps) With(c Capability) Caps { return cs | 1<<c }

func Of(c ...Capability) Caps {
	var cs Caps
	for _, x := range c {
		cs = cs.With(x)
	}
	return cs
}

// Member is one entry: what is known of a member at an incarnation.
// Only the member raises its own incarnation, to refute a suspicion or
// to rejoin.
type Member struct {
	ID          ID
	Addr        string
	Incarnation uint64
	State       State
	// Caps are the capabilities of the member's software, and MinVersion
	// and Version the lowest and highest wire versions it reads. All come
	// from the member itself.
	Caps       Caps
	MinVersion uint8
	Version    uint8
}

// MaxIncarnation is the highest incarnation an entry may carry. Decoders
// refuse higher ones.
const MaxIncarnation = 1 << 62

func (m Member) String() string {
	return fmt.Sprintf("%s@%s#%d:%s", printable(string(m.ID)), printable(m.Addr), m.Incarnation, m.State)
}

// printable quotes a string from the network that holds anything but
// printable characters.
func printable(s string) string {
	for _, r := range s {
		if !unicode.IsPrint(r) {
			return strconv.Quote(s)
		}
	}
	return s
}

// Overrides reports whether entry a replaces entry b about the same member:
// a higher incarnation always wins, and at the same incarnation, the state
// further along Alive, Suspect, Dead, Left wins. Entries alike in both but
// different in other fields, as two runs of a member can leave, are ordered
// by those fields: the order is total over whole entries, and applying
// entries in any order, any number of times, ends at the same one. The
// member supersedes the other run's entry as soon as it hears of it.
//
// An alive entry of a higher incarnation overrides a dead one, and the
// member rejoins.
func Overrides(a, b Member) bool {
	if a.Incarnation != b.Incarnation {
		return a.Incarnation > b.Incarnation
	}
	if a.State != b.State {
		return a.State.rank() > b.State.rank()
	}
	return cmp.Or(
		cmp.Compare(a.Addr, b.Addr),
		cmp.Compare(a.Version, b.Version),
		cmp.Compare(a.MinVersion, b.MinVersion),
		cmp.Compare(a.Caps, b.Caps),
	) > 0
}

// Newer returns whichever of a and b overrides the other, a when neither
// does.
func Newer(a, b Member) Member {
	if Overrides(b, a) {
		return b
	}
	return a
}
