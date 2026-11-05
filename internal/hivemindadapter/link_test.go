package hivemindadapter

import (
	"net/netip"
	"testing"
)

func TestLinkProblemSaysQUICCantOpenConnectionsBelowItsSmallestPacket(t *testing.T) {
	if got, want := linkProblem(1227), "QUIC can't open connections: the link's MTU, 1227, is below the 1228 it needs"; got != want {
		t.Errorf("linkProblem(1227) = %q, want %q", got, want)
	}
	if got := linkProblem(1228); got != "" {
		t.Errorf("linkProblem(1228) = %q, want none", got)
	}
}

func TestLinkMTUIsTheMTUOfTheInterfaceWithTheAddress(t *testing.T) {
	mtu, ok := linkMTU(netip.MustParseAddr("127.0.0.1"))
	if !ok || mtu == 0 {
		t.Errorf("linkMTU of the loopback address: %d, %v, want its MTU", mtu, ok)
	}
	if none, ok := linkMTU(netip.MustParseAddr("192.0.2.1")); ok {
		t.Errorf("linkMTU of an address no interface has: %d, want none", none)
	}
	if mapped, ok := linkMTU(netip.MustParseAddr("::ffff:127.0.0.1")); !ok || mapped != mtu {
		t.Errorf("linkMTU of the mapped loopback address: %d, %v", mapped, ok)
	}
}

func TestAMembershipWithNoLinkItKnowsHasNoProblem(t *testing.T) {
	for _, m := range []*Membership{{}, {address: netip.MustParseAddr("192.0.2.1")}, {address: netip.MustParseAddr("127.0.0.1")}} {
		if got := m.problem(); got != "" {
			t.Errorf("the problem of a membership at %v: %q", m.address, got)
		}
	}
}

func BenchmarkProblem(b *testing.B) {
	m := &Membership{address: netip.MustParseAddr("127.0.0.1")}
	for b.Loop() {
		m.problem()
	}
}
