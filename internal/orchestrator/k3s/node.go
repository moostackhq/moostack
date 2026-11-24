package k3s

import (
	"bufio"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net/netip"
	stdpath "path"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"

	"gopkg.in/yaml.v3"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"

	"github.com/moostackhq/moostack/internal/model"
	"github.com/moostackhq/moostack/internal/orchestrator"
)

// role is what a node is in its cluster: the control plane, or a node
// that joins it.
type role struct {
	controlPlane bool
	// name is how the operator sees the role.
	name string
	// service is k3s's systemd unit.
	service string
	// exec is what k3s's install script installs.
	exec string
	// uninstall is k3s's uninstall script.
	uninstall string
	// reservedMiB is the memory the kubelet keeps from pods for k3s.
	reservedMiB int
	// memLimitPercent is k3s's GOMEMLIMIT, as a share of the node's
	// memory.
	memLimitPercent int
}

var (
	controlPlaneRole = role{controlPlane: true, name: "control plane", service: "k3s", exec: "server", uninstall: "k3s-uninstall.sh", reservedMiB: 650, memLimitPercent: 25}
	agentRole        = role{name: "node", service: "k3s-agent", exec: "agent", uninstall: "k3s-agent-uninstall.sh", reservedMiB: 250, memLimitPercent: 10}
)

func roleOf(controlPlane bool) role {
	if controlPlane {
		return controlPlaneRole
	}
	return agentRole
}

func (r role) String() string { return r.name }

// factsScript prints what a host is, a key=value pair per line.
const factsScript = `echo "arch=$(uname -m)"
echo "mem_kib=$(awk '/^MemTotal:/ {print $2}' /proc/meminfo)"
[ -d /run/systemd/system ] && echo systemd=yes
[ -e /usr/local/bin/k3s ] || [ -e /etc/rancher/k3s ] && echo k3s=yes
for c in curl flock ip; do command -v $c >/dev/null && echo "has=$c"; done
ip -o -4 addr show 2>/dev/null | awk '{print "addr=" $2 " " $4}'
ip -4 route show default 2>/dev/null | awk '{for (i = 1; i < NF; i++) if ($i == "dev") print "route=" $(i+1)}'
`

// facts are what factsScript found on a host.
type facts struct {
	// architecture is amd64 or arm64, or what uname said if neither.
	architecture string
	memoryMiB    int
	systemd      bool
	// k3s says k3s is installed, or was.
	k3s bool
	// commands are the commands the host has of those a node needs.
	commands []string
	// addresses are the host's IPv4 addresses.
	addresses []address
	// routeInterfaces are the interfaces of the host's default routes.
	routeInterfaces []string
}

// address is an address on a network interface.
type address struct {
	iface string
	addr  netip.Addr
}

// parseFacts parses factsScript's output.
func parseFacts(out string) (facts, error) {
	var f facts
	s := bufio.NewScanner(strings.NewReader(out))
	for s.Scan() {
		key, value, ok := strings.Cut(strings.TrimSpace(s.Text()), "=")
		if !ok {
			continue
		}
		switch key {
		case "arch":
			f.architecture = goArch(value)
		case "mem_kib":
			kib, err := strconv.Atoi(value)
			if err != nil {
				return facts{}, fmt.Errorf("the host's memory: %q", value)
			}
			f.memoryMiB = kib / 1024
		case "systemd":
			f.systemd = true
		case "k3s":
			f.k3s = true
		case "has":
			f.commands = append(f.commands, value)
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
			f.addresses = append(f.addresses, address{iface: iface, addr: p.Addr()})
		case "route":
			f.routeInterfaces = append(f.routeInterfaces, value)
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

// check refuses a host that can't be made a node of r.
func (f facts) check(r role) error {
	var problems []string
	if f.architecture != "amd64" && f.architecture != "arm64" {
		problems = append(problems, fmt.Sprintf("its architecture is %s, not amd64 or arm64", f.architecture))
	}
	if !f.systemd {
		problems = append(problems, "it doesn't run systemd")
	}
	if f.k3s {
		problems = append(problems, "it has k3s, or k3s's files, already: run k3s-uninstall.sh or k3s-agent-uninstall.sh on it, and remove /etc/rancher/k3s")
	}
	for _, c := range []string{"curl", "flock", "ip"} {
		if !slices.Contains(f.commands, c) {
			problems = append(problems, "it has no "+c+" command")
		}
	}
	if least := minMemoryMiB(r); f.memoryMiB < least {
		problems = append(problems, fmt.Sprintf("it has %d MiB of memory, and a %s needs at least %d MiB", f.memoryMiB, r, least))
	}
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "; "))
	}
	return nil
}

// minMemoryMiB is the least memory a node of r can have: what the
// kubelet reserves, its eviction threshold, and podRoomMiB.
func minMemoryMiB(r role) int {
	return systemReservedMiB + r.reservedMiB + 100 + podRoomMiB
}

// skipped are the prefixes of interfaces whose addresses are never the
// private address: the loopback, k3s's, Docker's and containers'.
var skipped = []string{"lo", "cni", "flannel", "docker", "br-", "veth"}

// privateAddress is the host's address on its private network: its one
// private IPv4 address, leaving out those of container interfaces, or,
// of several, the one not on a default route's interface. override, if
// valid, must be one of the host's private IPv4 addresses.
func (f facts) privateAddress(override netip.Addr) (address, error) {
	if override.IsValid() {
		if !override.Is4() || !override.IsPrivate() {
			return address{}, fmt.Errorf("%s isn't a private IPv4 address (10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16)", override)
		}
		for _, a := range f.addresses {
			if a.addr == override {
				return a, nil
			}
		}
		return address{}, fmt.Errorf("the host has no address %s", override)
	}
	var candidates []address
	for _, a := range f.addresses {
		if a.addr.Is4() && a.addr.IsPrivate() && !slices.ContainsFunc(skipped, func(p string) bool { return strings.HasPrefix(a.iface, p) }) {
			candidates = append(candidates, a)
		}
	}
	if len(candidates) > 1 {
		offRoute := slices.DeleteFunc(slices.Clone(candidates), func(a address) bool { return slices.Contains(f.routeInterfaces, a.iface) })
		if len(offRoute) > 0 {
			candidates = offRoute
		}
	}
	switch len(candidates) {
	case 1:
		return candidates[0], nil
	case 0:
		return address{}, errors.New("the host has no private IPv4 address (10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16); attach it to a private network, or name its address with --private-address")
	}
	var list []string
	for _, a := range candidates {
		list = append(list, a.addr.String()+" on "+a.iface)
	}
	return address{}, fmt.Errorf("the host has several private addresses, %s; name the one its nodes share with --private-address", strings.Join(list, ", "))
}

// shared is the address range of carrier-grade NAT, which overlay
// networks such as Tailscale's also use.
var shared = netip.MustParsePrefix("100.64.0.0/10")

// publicAddress is the host's public IPv4 address: the first on a
// default route's interface, or else the first, of its addresses that
// are neither private nor shared nor on a container interface. It is
// the zero Addr on a host without one, as behind NAT.
func (f facts) publicAddress() netip.Addr {
	var first netip.Addr
	for _, a := range f.addresses {
		if !a.addr.Is4() || !a.addr.IsGlobalUnicast() || a.addr.IsPrivate() || shared.Contains(a.addr) ||
			slices.ContainsFunc(skipped, func(p string) bool { return strings.HasPrefix(a.iface, p) }) {
			continue
		}
		if slices.Contains(f.routeInterfaces, a.iface) {
			return a.addr
		}
		if !first.IsValid() {
			first = a.addr
		}
	}
	return first
}

// lowMemoryMiB is the most memory a node may have for k3s to run with
// lowGOGC.
const lowMemoryMiB = 4096

// lowGOGC is k3s's GOGC on a node of lowMemoryMiB or less.
const lowGOGC = "25"

// memorySettings is k3s's GOGC, empty for Go's default, and its GOMEMLIMIT on a
// node of memMiB and r: a quarter of its memory on the control plane,
// a tenth on other nodes.
func memorySettings(memMiB int, r role) (gogc, gomemlimit string) {
	if memMiB <= lowMemoryMiB {
		gogc = lowGOGC
	}
	return gogc, strconv.Itoa(memMiB*r.memLimitPercent/100) + "MiB"
}

// Paths on a node.
const (
	// configFile is k3s's config file.
	configFile = "/etc/rancher/k3s/config.yaml"
	// registriesFile is k3s's registry config.
	registriesFile = "/etc/rancher/k3s/registries.yaml"
	// agentTokenFile holds, on the control plane, the token that nodes
	// join it with, which can't join a server.
	agentTokenFile = "/var/lib/rancher/k3s/server/agent-token"
	// kubeletDropIn is the kubelet config drop-in that reserves memory
	// for the system and k3s, beside k3s's own 00-k3s-defaults.conf.
	kubeletDropIn = "/var/lib/rancher/k3s/agent/etc/kubelet.conf.d/10-moostack.conf"
)

// installScriptSHA256 are the checksums of k3s's install script, by k3s
// version.
var installScriptSHA256 = map[string]string{
	"v1.37.2+k3s1": "ed01f89fd977bf20ac1516bbebf8370bf3ddbaa55dac8aba610956a4c78cc00b",
}

// downloadLimits bound a download: it fails after 10 minutes, or below
// 10 KB/s for a minute.
const downloadLimits = "--max-time 600 --speed-limit 10240 --speed-time 60"

// dropIn is the path of the unit drop-in that sets the memory settings
// of k3s on a node of r.
func dropIn(r role) string {
	return "/etc/systemd/system/" + r.service + ".service.d/memory.conf"
}

// memoryDropIn is the drop-in's content.
func memoryDropIn(gogc, gomemlimit string) string {
	var b strings.Builder
	b.WriteString("[Service]\n")
	if gogc != "" {
		b.WriteString("Environment=GOGC=" + gogc + "\n")
	}
	b.WriteString("Environment=GOMEMLIMIT=" + gomemlimit + "\n")
	return b.String()
}

// systemReservedMiB is the memory the kubelet keeps from pods for the
// system.
const systemReservedMiB = 200

// kubeletConfig is the kubelet drop-in of a node of memMiB and r:
// memory reserved for the system and k3s, and pods evicted below
// max(100 MiB, 5% of memMiB) available.
func kubeletConfig(memMiB int, r role) string {
	return marshal(map[string]any{
		"apiVersion":     "kubelet.config.k8s.io/v1beta1",
		"kind":           "KubeletConfiguration",
		"systemReserved": map[string]string{"memory": fmt.Sprintf("%dMi", systemReservedMiB)},
		"kubeReserved":   map[string]string{"memory": fmt.Sprintf("%dMi", r.reservedMiB)},
		"evictionHard":   map[string]string{"memory.available": fmt.Sprintf("%dMi", max(100, memMiB*5/100))},
	})
}

// withExternalIP adds the node's public address to config as k3s's
// node-external-ip, which makes it the address that Traefik's Service
// is reached at.
func withExternalIP(config map[string]any, n model.Node) map[string]any {
	if n.PublicAddress.IsValid() {
		config["node-external-ip"] = n.PublicAddress.String()
	}
	return config
}

// serverConfig is the control plane's config file, with agentToken as
// the token nodes join with.
func serverConfig(n model.Node, agentToken string) string {
	return marshal(withExternalIP(map[string]any{
		"node-name":             n.Name,
		"node-ip":               n.PrivateAddress.String(),
		"advertise-address":     n.PrivateAddress.String(),
		"bind-address":          n.PrivateAddress.String(),
		"flannel-iface":         n.Interface,
		"agent-token":           agentToken,
		"write-kubeconfig-mode": "0600",
	}, n))
}

// agentConfig is the config file of a node that joins the control plane
// at server with token.
func agentConfig(n model.Node, server netip.Addr, token string) string {
	return marshal(withExternalIP(map[string]any{
		"server":        "https://" + netip.AddrPortFrom(server, apiPort).String(),
		"token":         token,
		"node-name":     n.Name,
		"node-ip":       n.PrivateAddress.String(),
		"flannel-iface": n.Interface,
	}, n))
}

// apiPort is the port of the cluster's API on the control plane.
const apiPort = 6443

// registries is the registry config that mirrors the registry called
// name to the one at addr.
func registries(name string, addr netip.AddrPort) string {
	return marshal(map[string]any{
		"mirrors": map[string]any{
			name: map[string]any{
				"endpoint": []string{"http://" + addr.String()},
			},
		},
	})
}

func marshal(v any) string {
	data, err := yaml.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(data)
}

// shellQuote quotes s as one word of a shell command.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// installScript installs k3s version on a node of r, from the install
// script of that version, checked against its checksum, and starts it
// on the control plane; on another node startScript does.
func installScript(version string, r role) (string, error) {
	sum, ok := installScriptSHA256[version]
	if !ok {
		return "", fmt.Errorf("mooctl has no checksum of k3s %s's install script", version)
	}
	return fmt.Sprintf(`set -e
script=$(mktemp)
trap 'rm -f "$script"' EXIT
trap 'exit 143' INT TERM
curl -sfL --retry 3 %s -o "$script" %s
echo "%s  $script" | sha256sum -c --quiet
INSTALL_K3S_VERSION=%s INSTALL_K3S_EXEC=%s INSTALL_K3S_SKIP_START=%t sh "$script"`,
		downloadLimits, shellQuote("https://raw.githubusercontent.com/k3s-io/k3s/"+version+"/install.sh"), sum, shellQuote(version), r.exec, !r.controlPlane), nil
}

// downloadedScript waits until k3s's install has downloaded k3s, or no
// install runs, or a minute has passed, and fails unless k3s is there.
const downloadedScript = `i=0
until [ -e /usr/local/bin/k3s ] || [ "$i" -ge 300 ]; do
	[ "$i" -ge 10 ] && ! pgrep -f 'INSTALL_K3[S]_EXEC' >/dev/null && break
	i=$((i + 1))
	sleep 0.2
done
[ -e /usr/local/bin/k3s ]`

// startScript starts k3s on a node that joins the control plane, waits
// until it has started, and prints its last log lines if it fails to.
const startScript = `systemctl start k3s-agent || {
	journalctl -u k3s-agent -n 20 --no-pager -o cat >&2
	exit 1
}`

// reachScript fails unless the node reaches the control plane's API at
// addr.
func reachScript(addr netip.Addr) string {
	api := netip.AddrPortFrom(addr, apiPort)
	return fmt.Sprintf(`curl -sk --max-time 10 -o /dev/null https://%[1]s/ping || {
	echo "it doesn't reach the control plane at %[1]s" >&2
	exit 1
}`, api)
}

// uninstallScript removes k3s from a node of r, the files of nodeFiles
// and their directories that are empty then, after waiting up to a minute for an install still running
// to end, and fails if k3s's own uninstall script fails.
func uninstallScript(r role) string {
	files := []string{configFile, registriesFile, dropIn(r), kubeletDropIn}
	dirs := []string{"/etc/rancher/k3s", "/etc/rancher", "/etc/systemd/system/" + r.service + ".service.d"}
	if r.controlPlane {
		files = append(files, autoDeployFile("traefik-config"), autoDeployFile("cert-manager"), autoDeployFile("issuer"))
		dirs = append(dirs, stdpath.Dir(autoDeployFile("issuer")), "/var/lib/rancher/k3s/server")
	}
	for dir := stdpath.Dir(kubeletDropIn); dir != "/var/lib"; dir = stdpath.Dir(dir) {
		dirs = append(dirs, dir)
	}
	return fmt.Sprintf(`for i in $(seq 60); do
	pgrep -f 'INSTALL_K3[S]_EXEC' >/dev/null || break
	sleep 1
done
s=0
if [ -x /usr/local/bin/%[1]s ]; then /usr/local/bin/%[1]s || s=$?; fi
rm -f %[2]s
for d in %[3]s; do rmdir "$d" 2>/dev/null; done
systemctl daemon-reload
exit $s`, r.uninstall, strings.Join(files, " "), strings.Join(dirs, " "))
}

// file is a file written on a node.
type file struct {
	path, content string
}

// nodeFiles are the files written on the host that becomes the node n
// before k3s is installed there, for the cluster c, whose control plane
// is at controlPlane and whose nodes join with token.
func nodeFiles(n model.Node, c Cluster, controlPlane netip.Addr, token string) []file {
	r := roleOf(n.ControlPlane)
	config := agentConfig(n, controlPlane, token)
	if n.ControlPlane {
		config = serverConfig(n, token)
	}
	files := []file{
		{configFile, config},
		{registriesFile, registries(c.Registry, netip.AddrPortFrom(controlPlane, c.RegistryPort))},
		{dropIn(r), memoryDropIn(n.GOGC, n.GOMEMLIMIT)},
		{kubeletDropIn, kubeletConfig(n.MemoryMiB, r)},
	}
	if n.ControlPlane {
		files = append(files,
			file{autoDeployFile("traefik-config"), traefikConfig},
			file{autoDeployFile("cert-manager"), certManager()},
			file{autoDeployFile("issuer"), issuerManifest(c.Email)},
		)
	}
	return files
}

// write writes content to path on host, creating its directory.
func write(ctx context.Context, host orchestrator.Host, path, content string) error {
	if _, err := host.Run(ctx, "mkdir -p "+shellQuote(stdpath.Dir(path)), nil); err != nil {
		return err
	}
	return host.Upload(ctx, path, strings.NewReader(content), 0o600)
}

// leftError is an error that a call cut off afterwards still reports:
// that of an undo that failed, or of what failed before the cut.
type leftError struct{ error }

func (e leftError) Unwrap() error { return e.error }

// left are the leftErrors in err.
func left(err error) []error {
	var out []error
	switch e := err.(type) {
	case nil:
	case leftError:
		out = append(out, e)
	case interface{ Unwrap() []error }:
		for _, inner := range e.Unwrap() {
			out = append(out, left(inner)...)
		}
	case interface{ Unwrap() error }:
		out = left(e.Unwrap())
	}
	return out
}

// causeAndLeft is causeIfEnded, with the leftErrors of err kept beside
// the cause.
func causeAndLeft(ctx context.Context, err error) error {
	if err == nil || ctx.Err() == nil || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return err
	}
	return errors.Join(append([]error{context.Cause(ctx)}, left(err)...)...)
}

// checkNodeNames refuses a node's name that k3s can't have, and the name of
// a network interface that holds a space or a character that isn't
// printed.
func checkNodeNames(name, iface string) error {
	if problems := validation.IsDNS1123Subdomain(name); len(problems) > 0 {
		return fmt.Errorf("a node can't be called %q: %s", name, strings.Join(problems, "; "))
	}
	if strings.ContainsFunc(iface, func(r rune) bool { return unicode.IsSpace(r) || !unicode.IsPrint(r) }) {
		return fmt.Errorf("%q is no name of a network interface", iface)
	}
	return nil
}

// errNoControlPlane is the error of what needs the cluster's control
// plane in a cluster without one.
var errNoControlPlane = errors.New("the cluster has no control plane to reach it through")

// controlPlane is the host of the cluster's control plane, nil if it
// has none, and its private address.
func (b *Backend) controlPlane() (orchestrator.Host, netip.Addr) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.host, b.cluster.ControlPlane
}

// Preflight refuses a name a node can't have. It reads host's
// architecture, memory and addresses. The node's
// private address is the one want names, or the host's one private IPv4
// address, or of several the one off its default route; its public
// address is the host's public IPv4 address, if it has one. It refuses
// a k3s version without a checksum of its install script, a host that
// isn't amd64 or arm64, has no systemd, curl, flock or ip, has k3s or
// its files, or has too little memory for the role, and a host that is
// to join and doesn't reach the control plane's API, or whose cluster
// runs another k3s version than the cluster's file says. It changes
// nothing on host.
func (b *Backend) Preflight(ctx context.Context, host orchestrator.Host, want orchestrator.NodeRequest) (model.Node, error) {
	if ctx.Err() != nil {
		return model.Node{}, context.Cause(ctx)
	}
	n, err := b.preflight(ctx, host, want)
	if err != nil {
		return model.Node{}, causeIfEnded(ctx, err)
	}
	return n, nil
}

func (b *Backend) preflight(ctx context.Context, host orchestrator.Host, want orchestrator.NodeRequest) (model.Node, error) {
	r := roleOf(want.ControlPlane)
	if err := checkNodeNames(want.Name, ""); err != nil {
		return model.Node{}, err
	}
	if _, err := installScript(b.cluster.Version, r); err != nil {
		return model.Node{}, err
	}
	out, err := host.Run(ctx, factsScript, nil)
	if err != nil {
		return model.Node{}, err
	}
	f, err := parseFacts(out)
	if err != nil {
		return model.Node{}, err
	}
	if err := f.check(r); err != nil {
		return model.Node{}, fmt.Errorf("it can't be a %s: %w", r, err)
	}
	private, err := f.privateAddress(want.PrivateAddress)
	if err != nil {
		return model.Node{}, err
	}
	if err := checkNodeNames(want.Name, private.iface); err != nil {
		return model.Node{}, err
	}
	if !want.ControlPlane {
		cp, at := b.controlPlane()
		if cp == nil {
			return model.Node{}, errNoControlPlane
		}
		if !at.Is4() {
			return model.Node{}, fmt.Errorf("the control plane's private address %q isn't an IPv4 address", at.String())
		}
		if _, err := host.Run(ctx, reachScript(at), nil); err != nil {
			return model.Node{}, err
		}
		a, err := b.connect(ctx)
		if err != nil {
			return model.Node{}, err
		}
		v, err := a.client.Discovery().ServerVersionWithContext(ctx)
		if err != nil {
			return model.Node{}, fmt.Errorf("reading k3s's version on the control plane: %w", err)
		}
		if v.GitVersion != b.cluster.Version {
			return model.Node{}, fmt.Errorf("the control plane runs k3s %s, and the cluster is to run %s", v.GitVersion, b.cluster.Version)
		}
	}
	gogc, gomemlimit := memorySettings(f.memoryMiB, r)
	return model.Node{
		Name:           want.Name,
		ControlPlane:   want.ControlPlane,
		PrivateAddress: private.addr,
		Interface:      private.iface,
		PublicAddress:  f.publicAddress(),
		Architecture:   f.architecture,
		MemoryMiB:      f.memoryMiB,
		GOGC:           gogc,
		GOMEMLIMIT:     gomemlimit,
	}, nil
}

const (
	// undoTimeout bounds undoing a failed AddNode.
	undoTimeout = 5 * time.Minute
	// readyTimeout bounds waiting for a node to be Ready.
	readyTimeout = 10 * time.Minute
	// drainTimeout bounds evicting a node's pods.
	drainTimeout = 3 * time.Minute
	// uncordonTimeout bounds putting a node back after a failed drain.
	uncordonTimeout = 30 * time.Second
	// goneTimeout bounds waiting for a deleted node to be gone.
	goneTimeout = time.Minute
)

// AddNode refuses a name a node can't have. It writes k3s's config, its registry config, its memory settings
// and the kubelet's on host, and on a control plane Traefik's config,
// cert-manager and the issuer, then installs k3s. A control plane has a
// new token for nodes to join with, starts with its install, and is
// from then on the host the backend reaches the cluster through. Another
// node joins with the control plane's token and is started after its
// install. It refuses a node that joins under the name of a node the
// cluster has. It tells Starting once a control plane has downloaded
// k3s, or at the latest once its install has ended well, and for
// another node before it starts it. It returns once the node is
// Ready at its private address, for which it waits readyTimeout. If it
// fails after it wrote to host, it tells Uninstalling, uninstalls from
// host, and deletes from the cluster a node that joined, though ctx has
// ended; what of that fails is in its error.
func (b *Backend) AddNode(ctx context.Context, host orchestrator.Host, n model.Node, step func(orchestrator.Step)) error {
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	return causeAndLeft(ctx, b.addNode(ctx, host, n, step))
}

func (b *Backend) addNode(ctx context.Context, host orchestrator.Host, n model.Node, step func(orchestrator.Step)) error {
	r := roleOf(n.ControlPlane)
	if err := checkNodeNames(n.Name, n.Interface); err != nil {
		return err
	}
	script, err := installScript(b.cluster.Version, r)
	if err != nil {
		return err
	}
	cp, at := b.controlPlane()
	var token string
	var a *api
	if n.ControlPlane {
		if cp != nil {
			return errors.New("the cluster has a control plane already")
		}
		token, at = rand.Text(), n.PrivateAddress
	} else {
		if cp == nil {
			return errNoControlPlane
		}
		out, err := cp.Run(ctx, "cat "+agentTokenFile, nil)
		if err != nil {
			return fmt.Errorf("reading the join token on the control plane: %w", err)
		}
		token = strings.TrimSpace(out)
		if a, err = b.connect(ctx); err != nil {
			return err
		}
		if _, err := a.client.CoreV1().Nodes().Get(ctx, n.Name, metav1.GetOptions{}); err == nil {
			return fmt.Errorf("the cluster has a node %s already", n.Name)
		} else if !apierrors.IsNotFound(err) {
			return fmt.Errorf("reading node %s: %w", n.Name, err)
		}
	}

	a, err = b.install(ctx, host, n, a, script, nodeFiles(n, b.cluster, at, token), step)
	if err != nil {
		if ctx.Err() == nil {
			err = leftError{err}
		}
		step(orchestrator.Uninstalling)
		undo, cancel := context.WithTimeout(context.WithoutCancel(ctx), undoTimeout)
		defer cancel()
		if _, uerr := host.Run(undo, uninstallScript(r), nil); uerr != nil {
			err = errors.Join(err, leftError{fmt.Errorf("uninstalling from %s: %w", n.Name, uerr)})
		}
		if !n.ControlPlane {
			if uerr := a.deleteNode(undo, n.Name); uerr != nil {
				err = errors.Join(err, leftError{fmt.Errorf("deleting node %s: %w", n.Name, uerr)})
			}
		}
		return err
	}
	if n.ControlPlane {
		b.mu.Lock()
		b.host, b.api, b.cluster.ControlPlane = host, a, n.PrivateAddress
		b.mu.Unlock()
	}
	return nil
}

// install writes files on host, installs k3s there with script, and
// waits until the node n is Ready. a is the cluster's API for a node
// that joins; for a control plane it is connected to through host, and
// returned.
func (b *Backend) install(ctx context.Context, host orchestrator.Host, n model.Node, a *api, script string, files []file, step func(orchestrator.Step)) (*api, error) {
	for _, f := range files {
		if err := write(ctx, host, f.path, f.content); err != nil {
			return a, fmt.Errorf("writing %s: %w", f.path, err)
		}
	}
	step(orchestrator.Installing)
	if n.ControlPlane {
		starting := false
		downloaded := make(chan struct{})
		go func() {
			defer close(downloaded)
			if _, err := host.Run(ctx, downloadedScript, nil); err == nil {
				starting = true
				step(orchestrator.Starting)
			}
		}()
		_, err := host.RunStoppable(ctx, script)
		<-downloaded
		if err != nil {
			return a, fmt.Errorf("installing k3s: %w", err)
		}
		if !starting {
			step(orchestrator.Starting)
		}
		if a, err = connectThrough(ctx, host); err != nil {
			return a, err
		}
		step(orchestrator.WaitingForNode)
		return a, a.waitReady(ctx, n)
	}
	if _, err := host.RunStoppable(ctx, script); err != nil {
		return a, fmt.Errorf("installing k3s: %w", err)
	}
	step(orchestrator.Starting)
	step(orchestrator.WaitingForNode)
	// The wait for Ready ends the wait for the start, whose failure ends
	// both.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	started, ready := make(chan error, 1), make(chan error, 1)
	go func() {
		_, err := host.RunStoppable(ctx, startScript)
		started <- err
	}()
	go func() { ready <- a.waitReady(ctx, n) }()
	select {
	case err := <-ready:
		return a, err
	case err := <-started:
		if err != nil {
			cancel()
			<-ready
			return a, fmt.Errorf("starting k3s: %w", err)
		}
		return a, <-ready
	}
}

// waitReady waits until the cluster has the node n, Ready, with its
// private address. When the time is up, its error lists the cluster's
// nodes and pods and its last events.
func (a *api) waitReady(ctx context.Context, n model.Node) error {
	var last error
	err := poll(ctx, readyTimeout, 2*time.Second, func(ctx context.Context) (bool, error) {
		node, err := a.client.CoreV1().Nodes().Get(ctx, n.Name, metav1.GetOptions{})
		if err != nil {
			last = err
			return false, nil
		}
		last = nil
		return nodeReady(node) && internalIP(node) == n.PrivateAddress.String(), nil
	})
	if !errors.Is(err, errNotInTime) {
		return err
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%s isn't Ready at %s after %s", n.Name, n.PrivateAddress, readyTimeout)
	if last != nil {
		fmt.Fprintf(&b, ": %v", last)
	}
	detail, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	if nodes, err := a.client.CoreV1().Nodes().List(detail, metav1.ListOptions{}); err == nil {
		for _, node := range nodes.Items {
			ready := "not ready"
			if nodeReady(&node) {
				ready = "ready"
			}
			fmt.Fprintf(&b, "\nnode %s at %s: %s", node.Name, internalIP(&node), ready)
		}
	}
	return fmt.Errorf("%s\n%s", b.String(), a.diagnostics(ctx, metav1.NamespaceAll))
}

func nodeReady(n *corev1.Node) bool {
	for _, c := range n.Status.Conditions {
		if c.Type == corev1.NodeReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

func internalIP(n *corev1.Node) string {
	for _, a := range n.Status.Addresses {
		if a.Type == corev1.NodeInternalIP {
			return a.Address
		}
	}
	return ""
}

// RemoveNode does to the node called name what the cluster and host
// allow. If the cluster has the node and o doesn't say its host is
// unreachable, it refuses one that holds volumes, unless o says to lose
// them, and drains it: it keeps new pods off it and evicts its pods, as
// evictAll does. If host isn't nil, it uninstalls k3s from it as
// from a node that joined. If the cluster has the node, it deletes it
// and returns once it is gone. It tells each of the three steps as it
// starts it. For a node the cluster doesn't have and no host, it does
// nothing.
func (b *Backend) RemoveNode(ctx context.Context, name string, host orchestrator.Host, o orchestrator.RemoveOptions, step func(orchestrator.Step)) error {
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	return causeAndLeft(ctx, b.removeNode(ctx, name, host, o, step))
}

func (b *Backend) removeNode(ctx context.Context, name string, host orchestrator.Host, o orchestrator.RemoveOptions, step func(orchestrator.Step)) error {
	a, err := b.connect(ctx)
	if err != nil {
		return err
	}
	_, err = a.client.CoreV1().Nodes().Get(ctx, name, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("reading node %s: %w", name, err)
	}
	there := err == nil
	if there && !o.Unreachable {
		if !o.LoseVolumes {
			held, err := a.volumes(ctx, name)
			if err != nil {
				return err
			}
			if len(held) > 0 {
				return &orchestrator.VolumesError{Node: name, Volumes: held}
			}
		}
		step(orchestrator.Draining)
		if err := a.drain(ctx, name); err != nil {
			return err
		}
	}
	if host != nil {
		step(orchestrator.Uninstalling)
		if _, err := host.Run(ctx, uninstallScript(agentRole), nil); err != nil {
			return fmt.Errorf("uninstalling from %s: %w", name, err)
		}
	}
	if there {
		step(orchestrator.Deleting)
		if err := a.deleteNode(ctx, name); err != nil {
			return fmt.Errorf("deleting node %s: %w", name, err)
		}
	}
	return nil
}

// Uninstall removes k3s from host, as from a control plane or from a
// node that joined, as n says, with the files AddNode wrote there. It
// fails if k3s's own uninstall script fails. A host without k3s is no
// error.
func (b *Backend) Uninstall(ctx context.Context, host orchestrator.Host, n model.Node) error {
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	_, err := host.Run(ctx, uninstallScript(roleOf(n.ControlPlane)), nil)
	return causeIfEnded(ctx, err)
}

// volumes are the names of the volumes that are on the node called name
// and go with it.
func (a *api) volumes(ctx context.Context, name string) ([]string, error) {
	list, err := a.client.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("listing volumes: %w", err)
	}
	var held []string
	for _, pv := range list.Items {
		if pv.Spec.NodeAffinity == nil || pv.Spec.NodeAffinity.Required == nil {
			continue
		}
		for _, term := range pv.Spec.NodeAffinity.Required.NodeSelectorTerms {
			for _, e := range slices.Concat(term.MatchExpressions, term.MatchFields) {
				if slices.Contains(e.Values, name) && !slices.Contains(held, pv.Name) {
					held = append(held, pv.Name)
				}
			}
		}
	}
	return held, nil
}

// deleteNode deletes the node called name and returns once it is gone,
// or another node has its name. One that isn't there is no error.
func (a *api) deleteNode(ctx context.Context, name string) error {
	nodes := a.client.CoreV1().Nodes()
	node, err := nodes.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := nodes.Delete(ctx, name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &node.UID}}); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	err = poll(ctx, goneTimeout, time.Second, func(ctx context.Context) (bool, error) {
		now, err := nodes.Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return err == nil && now.UID != node.UID, err
	})
	if errors.Is(err, errNotInTime) {
		return fmt.Errorf("it is still there after %s", goneTimeout)
	}
	return err
}

// drain keeps new pods off the node called name and evicts the pods on
// it until none is left, for drainTimeout or until ctx's deadline. If
// it fails, the node takes pods again, though ctx has ended.
func (a *api) drain(ctx context.Context, name string) error {
	if err := a.schedulable(ctx, name, false); err != nil {
		return fmt.Errorf("draining %s: %w", name, err)
	}
	err := a.evictAll(ctx, name)
	if err != nil {
		err = fmt.Errorf("draining %s: %w", name, err)
		back, cancel := context.WithTimeout(context.WithoutCancel(ctx), uncordonTimeout)
		defer cancel()
		if uerr := a.schedulable(back, name, true); uerr != nil {
			err = errors.Join(err, leftError{fmt.Errorf("uncordoning %s: %w", name, uerr)})
		}
	}
	return err
}

// schedulable sets whether the node called name takes new pods.
func (a *api) schedulable(ctx context.Context, name string, yes bool) error {
	patch := fmt.Sprintf(`{"spec":{"unschedulable":%t}}`, !yes)
	_, err := a.client.CoreV1().Nodes().Patch(ctx, name, types.MergePatchType, []byte(patch), metav1.PatchOptions{})
	return err
}

// evictAll evicts the pods it first finds on the node called name, once
// a second, until none of them is left. It leaves a DaemonSet's pods and the node's
// static pods. It refuses, with none evicted, while a pod is there that
// no controller would start again and that hasn't ended. An eviction
// that a disruption budget holds back is tried again. When the time is
// up, its error names the pods held back, or else those still there.
func (a *api) evictAll(ctx context.Context, name string) error {
	var remaining, blocked []string
	var found map[types.UID]bool
	err := poll(ctx, drainTimeout, time.Second, func(ctx context.Context) (bool, error) {
		pods, err := a.client.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{FieldSelector: "spec.nodeName=" + name})
		if err != nil {
			return false, fmt.Errorf("listing its pods: %w", err)
		}
		var evict []corev1.Pod
		var alone []string
		for _, p := range pods.Items {
			if _, static := p.Annotations[corev1.MirrorPodAnnotationKey]; static {
				continue
			}
			owner := metav1.GetControllerOf(&p)
			if owner != nil && owner.Kind == "DaemonSet" {
				continue
			}
			if found != nil && !found[p.UID] {
				continue
			}
			if owner == nil && p.Status.Phase != corev1.PodSucceeded && p.Status.Phase != corev1.PodFailed {
				alone = append(alone, p.Namespace+"/"+p.Name)
			}
			evict = append(evict, p)
		}
		if len(alone) > 0 {
			return false, fmt.Errorf("no controller would start these pods again: %s; delete them, or give them one", strings.Join(alone, ", "))
		}
		if found == nil {
			found = map[types.UID]bool{}
			for _, p := range evict {
				found[p.UID] = true
			}
		}
		remaining, blocked = nil, nil
		for _, p := range evict {
			remaining = append(remaining, p.Namespace+"/"+p.Name)
			if p.DeletionTimestamp != nil {
				continue
			}
			err := a.client.PolicyV1().Evictions(p.Namespace).Evict(ctx, &policyv1.Eviction{ObjectMeta: metav1.ObjectMeta{Name: p.Name, Namespace: p.Namespace}})
			switch {
			case err == nil, apierrors.IsNotFound(err):
			case apierrors.IsTooManyRequests(err):
				blocked = append(blocked, p.Namespace+"/"+p.Name+": "+err.Error())
			case apierrors.IsForbidden(err) && apierrors.HasStatusCause(err, corev1.NamespaceTerminatingCause):
			default:
				return false, fmt.Errorf("evicting %s/%s: %w", p.Namespace, p.Name, err)
			}
		}
		return len(remaining) == 0, nil
	})
	if !errors.Is(err, errNotInTime) {
		return err
	}
	if len(blocked) > 0 {
		return fmt.Errorf("these pods can't be evicted in time:\n%s", strings.Join(blocked, "\n"))
	}
	return fmt.Errorf("these pods are still there: %s", strings.Join(remaining, ", "))
}
