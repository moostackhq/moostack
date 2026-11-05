package hivemindadapter

import (
	"fmt"
	"net"
	"net/netip"
)

// minMTU is the smallest link QUIC's handshake goes over on IPv4: its
// 1,200-byte payload and 28 bytes of IP and UDP headers. An IPv6 address
// needs a link of at least 1,280, which is more.
const minMTU = 1228

// linkProblem says why a link of mtu keeps QUIC from opening connections,
// empty if nothing does.
func linkProblem(mtu int) string {
	if mtu >= minMTU {
		return ""
	}
	return fmt.Sprintf("QUIC can't open connections: the link's MTU, %d, is below the %d it needs", mtu, minMTU)
}

// linkMTU is the MTU of the interface that has addr, if one does.
func linkMTU(addr netip.Addr) (int, bool) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return 0, false
	}
	for _, i := range interfaces {
		addrs, err := i.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			if n, ok := a.(*net.IPNet); ok {
				if ip, ok := netip.AddrFromSlice(n.IP); ok && ip.Unmap() == addr.Unmap() {
					return i.MTU, true
				}
			}
		}
	}
	return 0, false
}

// problem says why the node's link keeps membership from opening
// connections, empty if nothing does, or if the membership has no address
// of its own.
func (m *Membership) problem() string {
	if !m.address.IsValid() {
		return ""
	}
	mtu, ok := linkMTU(m.address)
	if !ok {
		return ""
	}
	return linkProblem(mtu)
}
