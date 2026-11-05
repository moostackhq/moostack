// Package hivemindadapter runs the node's membership on hivemind, over
// QUIC, authenticated by the node identity, and gives the agent the
// cluster's members.
package hivemindadapter

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/moostackhq/moostack/internal/identity"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/member"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/node"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/runtime"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/transport"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/transport/quic"
)

// resetKeyLabel is the label the transport's reset key is derived from
// the node identity under.
const resetKeyLabel = "moostack hivemindadapter membership reset key"

// State is what a node believes of another.
type State string

const (
	Alive   State = "alive"
	Suspect State = "suspect"
	Dead    State = "dead"
	Left    State = "left"
)

// Member is a node as membership knows it.
type Member struct {
	NodeID string
	// Address is where the node is reached for membership.
	Address string
	State   State
}

// Seed is a node to join the cluster through: its name and its address,
// the private address and the peer port.
type Seed struct {
	NodeID  string
	Address string
}

// Status is the node's view of the cluster and how its membership runs.
type Status struct {
	// Members are the nodes it knows, itself included.
	Members []Member
	// HealthScore is 0 while the node hears what it expects from the
	// others, and grows the more answers it misses.
	HealthScore int
	// Dropped counts the messages dropped since Start, by reason, each
	// above 0.
	Dropped map[string]uint64
}

// Config is what membership runs with.
type Config struct {
	// Identity is the node's identity: its name is its member ID, and it
	// proves it to the other nodes and checks theirs.
	Identity *identity.Identity
	// Address is the UDP address to listen and dial from: the node's
	// private address and the peer port.
	Address netip.AddrPort
	// Removed says which identities are removed: membership refuses
	// them both ways.
	Removed identity.IsRemoved
}

// ErrStopped is a call on a membership that stopped.
var ErrStopped = errors.New("membership: stopped")

// Membership is membership as Start runs it, until Stop.
type Membership struct {
	rt  *runtime.Runtime
	ctx context.Context
	// transportDrops are the transport's drop counts, by reason.
	transportDrops func() map[string]uint64
	cutOff         func(match func(*x509.Certificate) bool)
	impersonated   atomic.Uint64
	// mu guards stopped, and is held through a SetSeeds, a Forget or a
	// CutOff.
	mu      sync.Mutex
	stopped bool
}

// Start runs membership as cfg says, until Stop or ctx ends, which stops
// it without leaving. It fails if the UDP socket can't open.
func Start(ctx context.Context, cfg Config) (*Membership, error) {
	tr, err := quic.Listen(transportConfig(cfg))
	if err != nil {
		return nil, fmt.Errorf("membership: listening on UDP %s: %w", cfg.Address, err)
	}
	m, err := start(ctx, cfg.Identity.Node, tr, func() map[string]uint64 { return quicDrops(tr.Stats().Dropped) })
	if err != nil {
		return nil, err
	}
	m.cutOff = tr.CutOff
	return m, nil
}

// cutNothing is the cutOff of a transport with no connections to close.
func cutNothing(func(*x509.Certificate) bool) {}

// transportConfig is the QUIC transport's configuration for cfg: its
// TLS from the node identity, refusing removed identities, and its reset
// key derived from it, the same at every start.
func transportConfig(cfg Config) quic.Config {
	id := cfg.Identity
	return quic.Config{
		Addr:     cfg.Address.String(),
		Server:   id.ServerTLS(cfg.Removed),
		Client:   func(peer string) *tls.Config { return id.ClientTLS(peer, cfg.Removed) },
		PeerID:   peerID,
		ResetKey: id.DeriveKey(resetKeyLabel),
	}
}

// peerID is the node a verified certificate is: its common name.
func peerID(cert *x509.Certificate) (string, error) {
	if cert.Subject.CommonName == "" {
		return "", errors.New("the certificate names no node")
	}
	return cert.Subject.CommonName, nil
}

// start runs membership for nodeID over tr, whose drop counts
// transportDrops returns, nil for none.
func start(ctx context.Context, nodeID string, tr transport.Transport, transportDrops func() map[string]uint64) (*Membership, error) {
	m := &Membership{ctx: ctx, transportDrops: transportDrops, cutOff: cutNothing}
	rt, err := runtime.Start(ctx, node.Defaults(member.ID(nodeID), ""), tr, runtime.Options{
		OnEvent: func(e node.Event) {
			if e.Kind == node.Impersonated {
				m.impersonated.Add(1)
			}
		},
	})
	if err != nil {
		tr.Close()
		return nil, fmt.Errorf("membership: %w", err)
	}
	m.rt = rt
	return m, nil
}

// Members returns every node membership knows, this one included, by
// name.
func (m *Membership) Members() []Member {
	var out []Member
	for _, e := range m.rt.Members() {
		out = append(out, convert(e))
	}
	slices.SortFunc(out, func(a, b Member) int { return strings.Compare(a.NodeID, b.NodeID) })
	return out
}

// Status returns the members, the node's health and what it dropped.
func (m *Membership) Status() Status {
	dropped := runtimeDrops(m.rt.Counts())
	dropped["impersonated"] = m.impersonated.Load()
	if m.transportDrops != nil {
		for reason, n := range m.transportDrops() {
			dropped[reason] += n
		}
	}
	for reason, n := range dropped {
		if n == 0 {
			delete(dropped, reason)
		}
	}
	return Status{Members: m.Members(), HealthScore: m.rt.Health(), Dropped: dropped}
}

// SetSeeds joins the cluster through seeds, and keeps them to sync with
// now and then. A seed that was forgotten is taken back.
func (m *Membership) SetSeeds(seeds []Seed) error {
	s := make([]node.Seed, len(seeds))
	for i, seed := range seeds {
		s[i] = node.Seed{ID: member.ID(seed.NodeID), Addr: seed.Address}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped {
		return ErrStopped
	}
	return stoppedIfClosed(m.rt.Join(s...))
}

// Forget removes the node called nodeID from the view at once, and
// ignores what the others say of it until a SetSeeds names it or
// membership starts again.
func (m *Membership) Forget(nodeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped {
		return ErrStopped
	}
	return stoppedIfClosed(m.rt.Forget(member.ID(nodeID)))
}

// CutOff closes every connection to the peer that proves the identity
// of fingerprint, and refuses that identity from then on. A connection
// of another identity under the same name stays.
func (m *Membership) CutOff(fingerprint string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped || m.ctx.Err() != nil {
		return ErrStopped
	}
	m.cutOff(func(cert *x509.Certificate) bool { return identity.CertFingerprint(cert) == fingerprint })
	return nil
}

// Stop leaves the cluster, and the others hold the node as left, and
// then stops. SetSeeds, Forget and CutOff are refused from its start. A Stop
// after the first returns at once.
func (m *Membership) Stop() {
	m.mu.Lock()
	stopped := m.stopped
	m.stopped = true
	m.mu.Unlock()
	if stopped {
		return
	}
	_ = m.rt.Leave()
	m.rt.Close()
}

// stoppedIfClosed is err, ErrStopped if it says the runtime closed, as
// when Start's context ended.
func stoppedIfClosed(err error) error {
	if errors.Is(err, runtime.ErrClosed) {
		return ErrStopped
	}
	return err
}

// convert is e as a Member. Its name and address are valid UTF-8,
// with a replacement character for what isn't, whatever a member gossiped.
func convert(e member.Member) Member {
	s := map[member.State]State{member.Alive: Alive, member.Suspect: Suspect, member.Dead: Dead, member.Left: Left}[e.State]
	return Member{NodeID: strings.ToValidUTF8(string(e.ID), "\uFFFD"), Address: strings.ToValidUTF8(e.Addr, "\uFFFD"), State: s}
}

// runtimeDrops are the runtime's drop counts, by reason.
func runtimeDrops(c runtime.Counts) map[string]uint64 {
	return map[string]uint64{
		"malformed":       uint64(c.Malformed),
		"unknown_kind":    uint64(c.Unknown),
		"wrong_version":   uint64(c.WrongVersion),
		"unencodable":     uint64(c.Unsent),
		"unauthenticated": uint64(c.Unauthenticated),
	}
}

// quicDrops are the QUIC transport's drop counts, by reason.
func quicDrops(d quic.Drops) map[string]uint64 {
	return map[string]uint64{
		"unpinned":     d.Unpinned,
		"oversize":     d.Oversize,
		"queue_full":   d.QueueFull,
		"unsent":       d.Unsent,
		"closed":       d.Closed,
		"receive_full": d.ReceiveFull,
		"read_failed":  d.ReadFails,
	}
}
