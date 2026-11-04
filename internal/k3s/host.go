// Package k3s says what mooctl runs on a host to make it a node: what it
// checks first, the files it writes, the scripts that install and remove
// k3s, and what runs on the control plane beside it.
package k3s

import (
	"bufio"
	"errors"
	"fmt"
	"net/netip"
	"slices"
	"strconv"
	"strings"
)

// FactsScript prints what Check, PrivateAddress and PublicAddress need to know of a
// host, a key=value pair per line.
const FactsScript = `echo "arch=$(uname -m)"
echo "mem_kib=$(awk '/^MemTotal:/ {print $2}' /proc/meminfo)"
[ -d /run/systemd/system ] && echo systemd=yes
[ -e /usr/local/bin/k3s ] || [ -e /etc/rancher/k3s ] && echo k3s=yes
for c in curl flock ip; do command -v $c >/dev/null && echo "has=$c"; done
ip -o -4 addr show 2>/dev/null | awk '{print "addr=" $2 " " $4}'
ip -4 route show default 2>/dev/null | awk '{for (i = 1; i < NF; i++) if ($i == "dev") print "route=" $(i+1)}'
`

// Facts are what FactsScript found on a host.
type Facts struct {
	// Architecture is amd64 or arm64, or what uname said if neither.
	Architecture string
	MemoryMiB    int
	Systemd      bool
	// K3s says k3s is installed, or was.
	K3s bool
	// Commands are the commands the host has of those mooctl needs.
	Commands []string
	// Addresses are the host's IPv4 addresses.
	Addresses []Address
	// RouteInterfaces are the interfaces of the host's default routes.
	RouteInterfaces []string
}

// Address is an address on a network interface.
type Address struct {
	Interface string
	Addr      netip.Addr
}

// ParseFacts parses FactsScript's output.
func ParseFacts(out string) (Facts, error) {
	var f Facts
	s := bufio.NewScanner(strings.NewReader(out))
	for s.Scan() {
		key, value, ok := strings.Cut(strings.TrimSpace(s.Text()), "=")
		if !ok {
			continue
		}
		switch key {
		case "arch":
			f.Architecture = goArch(value)
		case "mem_kib":
			kib, err := strconv.Atoi(value)
			if err != nil {
				return Facts{}, fmt.Errorf("the host's memory: %q", value)
			}
			f.MemoryMiB = kib / 1024
		case "systemd":
			f.Systemd = true
		case "k3s":
			f.K3s = true
		case "has":
			f.Commands = append(f.Commands, value)
		case "addr":
			iface, cidr, ok := strings.Cut(value, " ")
			if !ok {
				continue
			}
			p, err := netip.ParsePrefix(cidr)
			if err != nil {
				continue
			}
			// ip shows an alias as eth0:1, on eth0, and a linked
			// interface as name@link.
			iface, _, _ = strings.Cut(iface, ":")
			iface, _, _ = strings.Cut(iface, "@")
			f.Addresses = append(f.Addresses, Address{Interface: iface, Addr: p.Addr()})
		case "route":
			f.RouteInterfaces = append(f.RouteInterfaces, value)
		}
	}
	return f, s.Err()
}

func goArch(uname string) string {
	switch uname {
	case "x86_64":
		return "amd64"
	case "aarch64", "arm64":
		return "arm64"
	}
	return uname
}

// podRoomMiB is the least memory a node keeps for pods beyond what the
// kubelet reserves.
const podRoomMiB = 256

// Check refuses a host that can't be made a node of role.
func (f Facts) Check(role Role) error {
	var problems []string
	if f.Architecture != "amd64" && f.Architecture != "arm64" {
		problems = append(problems, fmt.Sprintf("its architecture is %s, not amd64 or arm64", f.Architecture))
	}
	if !f.Systemd {
		problems = append(problems, "it doesn't run systemd")
	}
	if f.K3s {
		problems = append(problems, "it has k3s, or k3s's files, already: run k3s-uninstall.sh or k3s-agent-uninstall.sh on it, and remove /etc/rancher/k3s")
	}
	for _, c := range []string{"curl", "flock", "ip"} {
		if !slices.Contains(f.Commands, c) {
			problems = append(problems, "it has no "+c+" command")
		}
	}
	if least := MinMemoryMiB(role); f.MemoryMiB < least {
		problems = append(problems, fmt.Sprintf("it has %d MiB of memory, and a %s needs at least %d MiB", f.MemoryMiB, role, least))
	}
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "; "))
	}
	return nil
}

// MinMemoryMiB is the least memory a node of role can have: what the
// kubelet reserves, its eviction threshold, and podRoomMiB.
func MinMemoryMiB(role Role) int {
	return systemReservedMiB + role.reservedMiB + 100 + podRoomMiB
}

// skipped are the prefixes of interfaces whose addresses are never the
// private address: the loopback, k3s's, Docker's and containers'.
var skipped = []string{"lo", "cni", "flannel", "docker", "br-", "veth"}

// PrivateAddress is the host's address on its private network: its one
// private IPv4 address, leaving out those of container
// interfaces, or, of several, the one not on a default route's
// interface. override, if valid, must be one of the host's private IPv4
// addresses.
func (f Facts) PrivateAddress(override netip.Addr) (Address, error) {
	if override.IsValid() {
		if !override.Is4() || !override.IsPrivate() {
			return Address{}, fmt.Errorf("%s isn't a private IPv4 address (10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16)", override)
		}
		for _, a := range f.Addresses {
			if a.Addr == override {
				return a, nil
			}
		}
		return Address{}, fmt.Errorf("the host has no address %s", override)
	}
	var candidates []Address
	for _, a := range f.Addresses {
		if a.Addr.Is4() && a.Addr.IsPrivate() && !slices.ContainsFunc(skipped, func(p string) bool { return strings.HasPrefix(a.Interface, p) }) {
			candidates = append(candidates, a)
		}
	}
	if len(candidates) > 1 {
		offRoute := slices.DeleteFunc(slices.Clone(candidates), func(a Address) bool { return slices.Contains(f.RouteInterfaces, a.Interface) })
		if len(offRoute) > 0 {
			candidates = offRoute
		}
	}
	switch len(candidates) {
	case 1:
		return candidates[0], nil
	case 0:
		return Address{}, errors.New("the host has no private IPv4 address (10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16); attach it to a private network, or name its address with --private-address")
	}
	var list []string
	for _, a := range candidates {
		list = append(list, a.Addr.String()+" on "+a.Interface)
	}
	return Address{}, fmt.Errorf("the host has several private addresses, %s; name the one its nodes share with --private-address", strings.Join(list, ", "))
}

// shared is the address range of carrier-grade NAT, which overlay
// networks such as Tailscale's also use.
var shared = netip.MustParsePrefix("100.64.0.0/10")

// PublicAddress is the host's public IPv4 address: the first on a
// default route's interface, or else the first, of its addresses that
// are neither private nor shared nor on a container interface. It is
// the zero Addr on a host without one, as behind NAT.
func (f Facts) PublicAddress() netip.Addr {
	var first netip.Addr
	for _, a := range f.Addresses {
		if !a.Addr.Is4() || !a.Addr.IsGlobalUnicast() || a.Addr.IsPrivate() || shared.Contains(a.Addr) ||
			slices.ContainsFunc(skipped, func(p string) bool { return strings.HasPrefix(a.Interface, p) }) {
			continue
		}
		if slices.Contains(f.RouteInterfaces, a.Interface) {
			return a.Addr
		}
		if !first.IsValid() {
			first = a.Addr
		}
	}
	return first
}

// lowMemoryMiB is the most memory a node may have for k3s to run with
// lowGOGC.
const lowMemoryMiB = 4096

// lowGOGC is k3s's GOGC on a node of lowMemoryMiB or less.
const lowGOGC = "25"

// Memory is k3s's GOGC, empty for Go's default, and its GOMEMLIMIT on a
// node of memMiB and role: a quarter of its memory on the control plane,
// a tenth on other nodes.
func Memory(memMiB int, role Role) (gogc, gomemlimit string) {
	if memMiB <= lowMemoryMiB {
		gogc = lowGOGC
	}
	return gogc, strconv.Itoa(memMiB*role.memLimitPercent/100) + "MiB"
}
