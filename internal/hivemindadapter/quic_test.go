//go:build linux

package hivemindadapter

import (
	"maps"
	"net/netip"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/moostackhq/moostack/internal/identity"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/transport/quic"
)

// These tests run membership over QUIC on the loopback, with node
// identities.

// newIdentity is the identity ca issues the node called name, kept in
// dir.
func newIdentity(t *testing.T, ca *identity.CA, name, dir string) identity.Files {
	t.Helper()
	files := identity.Files{Key: filepath.Join(dir, "node.key"), Cert: filepath.Join(dir, "node.crt"), CA: filepath.Join(dir, "ca.crt")}
	request, err := identity.NewKey(files.Key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := ca.Issue(request, name)
	if err != nil {
		t.Fatal(err)
	}
	for path, data := range map[string][]byte{files.Cert: cert, files.CA: ca.CertPEM()} {
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return files
}

func load(t *testing.T, files identity.Files) *identity.Identity {
	t.Helper()
	id, err := identity.Load(files)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func newCA(t *testing.T) *identity.CA {
	t.Helper()
	ca, err := identity.NewCA("test")
	if err != nil {
		t.Fatal(err)
	}
	return ca
}

// The reset key is the same at every start of one identity, as when the
// agent restarts, and another identity's is another.
func TestTheResetKeyIsTheSameAtEveryStartOfAnIdentity(t *testing.T) {
	ca := newCA(t)
	a := newIdentity(t, ca, "a", t.TempDir())
	b := newIdentity(t, ca, "b", t.TempDir())
	key := func(f identity.Files) [32]byte { return transportConfig(Config{Identity: load(t, f)}).ResetKey }
	if key(a) != key(a) {
		t.Error("two loads of one identity give two reset keys")
	}
	if key(a) == key(b) {
		t.Error("two identities give one reset key")
	}
	if key(a) == ([32]byte{}) {
		t.Error("the reset key is zero")
	}
}

// Two nodes started with their identities join over QUIC, each knowing
// the other by its name; a third, whose identity the first holds as
// removed, isn't taken.
func TestNodesStartedWithTheirIdentitiesJoin(t *testing.T) {
	ca := newCA(t)
	a, b, c := load(t, newIdentity(t, ca, "a", t.TempDir())), load(t, newIdentity(t, ca, "b", t.TempDir())), load(t, newIdentity(t, ca, "c", t.TempDir()))
	addr := func(i byte) netip.AddrPort { return netip.AddrPortFrom(netip.AddrFrom4([4]byte{127, 0, 0, i}), 17443) }
	removed := func(fp string) bool { return fp == c.Fingerprint() }
	ma, err := Start(t.Context(), Config{Identity: a, Address: addr(1), Removed: removed})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ma.Stop)
	seed := []Seed{{NodeID: "a", Address: addr(1).String()}}
	for _, n := range []struct {
		id   *identity.Identity
		addr netip.AddrPort
	}{{b, addr(2)}, {c, addr(3)}} {
		m, err := Start(t.Context(), Config{Identity: n.id, Address: n.addr})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(m.Stop)
		if err := m.SetSeeds(seed); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, "b alive", func() bool { return states(ma)["b"] == Alive })
	for end := time.Now().Add(2 * time.Second); time.Now().Before(end); time.Sleep(50 * time.Millisecond) {
		if s, ok := states(ma)["c"]; ok {
			t.Fatalf("a holds c, whose identity it holds as removed, %s", s)
		}
	}
	if _, err := Start(t.Context(), Config{Identity: b, Address: addr(2)}); err == nil {
		t.Error("a second membership started on b's UDP address")
	}
}

// What the transport drops shows in the status: a seed nothing listens
// at is a send with no connection.
func TestStatusHasTheTransportsDrops(t *testing.T) {
	a := load(t, newIdentity(t, newCA(t), "a", t.TempDir()))
	m, err := Start(t.Context(), Config{Identity: a, Address: netip.AddrPortFrom(netip.AddrFrom4([4]byte{127, 0, 0, 4}), 17443)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Stop)
	if err := m.SetSeeds([]Seed{{NodeID: "b", Address: "127.0.0.5:17443"}}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the unsent message counted", func() bool { return m.Status().Dropped["unsent"] > 0 })
}

// What the transport does shows in the status of each end.
func TestStatusHasTheTransportsCounts(t *testing.T) {
	ca := newCA(t)
	start := func(name string, last byte) *Membership {
		m, err := Start(t.Context(), Config{Identity: load(t, newIdentity(t, ca, name, t.TempDir())), Address: netip.AddrPortFrom(netip.AddrFrom4([4]byte{127, 0, 0, last}), 17445)})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(m.Stop)
		return m
	}
	a, b := start("a", 6), start("b", 7)
	if err := b.SetSeeds([]Seed{{NodeID: "a", Address: "127.0.0.6:17445"}}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the sync counted", func() bool {
		in, out := a.Status().Transport, b.Status().Transport
		return in["accepts"] > 0 && in["syncs_in"] > 0 && in["conns"] > 0 && out["dials"] > 0 && out["syncs_out"] > 0 && out["conns"] > 0
	})
}

// Each of the transport's counts has its own name.
func TestQuicCountsNameEachCount(t *testing.T) {
	got := quicCounts(quic.Stats{Conns: 1, Dials: 2, Accepts: 3, StreamsIn: 4, StreamsOut: 5})
	want := map[string]uint64{"conns": 1, "dials": 2, "accepts": 3, "syncs_in": 4, "syncs_out": 5}
	if !maps.Equal(got, want) {
		t.Fatalf("%v, want %v", got, want)
	}
}

// A node that dials a member whose identity it holds as removed doesn't
// take it.
func TestANodeDoesntTakeARemovedNodeItDials(t *testing.T) {
	ca := newCA(t)
	a, c := load(t, newIdentity(t, ca, "a", t.TempDir())), load(t, newIdentity(t, ca, "c", t.TempDir()))
	addr := func(i byte) netip.AddrPort { return netip.AddrPortFrom(netip.AddrFrom4([4]byte{127, 0, 0, i}), 17444) }
	ma, err := Start(t.Context(), Config{Identity: a, Address: addr(1), Removed: func(fp string) bool { return fp == c.Fingerprint() }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ma.Stop)
	mc, err := Start(t.Context(), Config{Identity: c, Address: addr(3)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mc.Stop)
	if err := ma.SetSeeds([]Seed{{NodeID: "c", Address: addr(3).String()}}); err != nil {
		t.Fatal(err)
	}
	for end := time.Now().Add(3 * time.Second); time.Now().Before(end); time.Sleep(50 * time.Millisecond) {
		if s, ok := states(ma)["c"]; ok {
			t.Fatalf("a holds c, which it dialled and holds as removed, %s", s)
		}
	}
}

// CutOff ends the connections of one identity and no other.
func TestCutOffClosesTheConnectionsOfAnIdentity(t *testing.T) {
	ca := newCA(t)
	a, b, c := load(t, newIdentity(t, ca, "a", t.TempDir())), load(t, newIdentity(t, ca, "b", t.TempDir())), load(t, newIdentity(t, ca, "c", t.TempDir()))
	addr := func(i byte) netip.AddrPort { return netip.AddrPortFrom(netip.AddrFrom4([4]byte{127, 0, 0, i}), 17445) }
	ma, err := Start(t.Context(), Config{Identity: a, Address: addr(1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ma.Stop)
	var others []*Membership
	for i, id := range []*identity.Identity{b, c} {
		m, err := Start(t.Context(), Config{Identity: id, Address: addr(byte(i + 2))})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(m.Stop)
		others = append(others, m)
		if err := m.SetSeeds([]Seed{{NodeID: "a", Address: addr(1).String()}}); err != nil {
			t.Fatal(err)
		}
	}
	mb, mc := others[0], others[1]
	waitFor(t, "a holding b and c alive", func() bool {
		s := states(ma)
		return s["b"] == Alive && s["c"] == Alive
	})
	waitFor(t, "b and c holding each other alive", func() bool { return states(mb)["c"] == Alive && states(mc)["b"] == Alive })
	// Both cut c off, as one alone answers c's indirect probes.
	for _, m := range []*Membership{ma, mb} {
		if err := m.CutOff(c.Fingerprint()); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, "c no longer holding a and b alive", func() bool {
		s := states(mc)
		return s["a"] != Alive && s["b"] != Alive
	})
	if s := states(mb)["a"]; s != Alive {
		t.Errorf("b holds a %s, after each cut off another identity", s)
	}
	if s := states(ma)["b"]; s != Alive {
		t.Errorf("a holds b %s, after each cut off another identity", s)
	}
}
