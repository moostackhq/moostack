package k3s

import (
	"fmt"
	"net/netip"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/moostackhq/moostack/internal/sshclient"
)

// Paths on a node.
const (
	// ConfigFile is k3s's config file.
	ConfigFile = "/etc/rancher/k3s/config.yaml"
	// RegistriesFile is k3s's registry config.
	RegistriesFile = "/etc/rancher/k3s/registries.yaml"
	// AgentTokenFile holds, on the control plane, the token that nodes
	// join it with, which can't join a server.
	AgentTokenFile = "/var/lib/rancher/k3s/server/agent-token"
	// AppliedCommitFile holds, on the control plane, the commit of the
	// cluster directory the cluster last applied.
	AppliedCommitFile = "/var/lib/moostack/applied-commit"
	// KubeletDropIn is the kubelet config drop-in that reserves memory
	// for the system and k3s, beside k3s's own 00-k3s-defaults.conf.
	KubeletDropIn = "/var/lib/rancher/k3s/agent/etc/kubelet.conf.d/10-moostack.conf"
)

// Registry is the registry name that nodes pull the cluster's own app
// images from, mirrored to zot.
const Registry = "moostack.local"

// RegistryPort is zot's port on the control plane's private address.
const RegistryPort = 5000

// installScriptSHA256 are the checksums of k3s's install script, by k3s
// version.
var installScriptSHA256 = map[string]string{
	"v1.37.2+k3s1": "ed01f89fd977bf20ac1516bbebf8370bf3ddbaa55dac8aba610956a4c78cc00b",
}

// downloadLimits bound a download: it fails after 10 minutes, or below
// 10 KB/s for a minute.
const downloadLimits = "--max-time 600 --speed-limit 10240 --speed-time 60"

// DropIn is the path of the unit drop-in that sets the memory settings
// of k3s on a node of role.
func DropIn(role Role) string {
	return "/etc/systemd/system/" + role.service + ".service.d/memory.conf"
}

// MemoryDropIn is the drop-in's content.
func MemoryDropIn(gogc, gomemlimit string) string {
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

// KubeletConfig is the kubelet drop-in of a node of memMiB and role:
// memory reserved for the system and k3s, and pods evicted below
// max(100 MiB, 5% of memMiB) available.
func KubeletConfig(memMiB int, role Role) string {
	return marshal(map[string]any{
		"apiVersion":     "kubelet.config.k8s.io/v1beta1",
		"kind":           "KubeletConfiguration",
		"systemReserved": map[string]string{"memory": fmt.Sprintf("%dMi", systemReservedMiB)},
		"kubeReserved":   map[string]string{"memory": fmt.Sprintf("%dMi", role.reservedMiB)},
		"evictionHard":   map[string]string{"memory.available": fmt.Sprintf("%dMi", max(100, memMiB*5/100))},
	})
}

// Node is what k3s's config of a node says.
type Node struct {
	Name           string
	PrivateAddress netip.Addr
	Interface      string
	// PublicAddress is the node's public address, the zero Addr if it
	// has none.
	PublicAddress netip.Addr
}

// withExternalIP adds the node's public address to config as k3s's
// node-external-ip, which makes it the address that Traefik's Service
// is reached at.
func withExternalIP(config map[string]any, n Node) map[string]any {
	if n.PublicAddress.IsValid() {
		config["node-external-ip"] = n.PublicAddress.String()
	}
	return config
}

// ServerConfig is the control plane's config file, with agentToken as the
// token nodes join with.
func ServerConfig(n Node, agentToken string) string {
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

// AgentConfig is the config file of a node that joins the control plane
// at server with token.
func AgentConfig(n Node, server netip.Addr, token string) string {
	return marshal(withExternalIP(map[string]any{
		"server":        "https://" + netip.AddrPortFrom(server, 6443).String(),
		"token":         token,
		"node-name":     n.Name,
		"node-ip":       n.PrivateAddress.String(),
		"flannel-iface": n.Interface,
	}, n))
}

// Registries is the registry config that mirrors Registry to zot on the
// control plane at controlPlane.
func Registries(controlPlane netip.Addr) string {
	return marshal(map[string]any{
		"mirrors": map[string]any{
			Registry: map[string]any{
				"endpoint": []string{"http://" + netip.AddrPortFrom(controlPlane, RegistryPort).String()},
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

// InstallScript installs k3s version on a node of role, from the install
// script of that version, checked against its checksum, and starts it
// on the control plane; on another node StartScript does.
func InstallScript(version string, role Role) (string, error) {
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
		downloadLimits, sshclient.ShellQuote("https://raw.githubusercontent.com/k3s-io/k3s/"+version+"/install.sh"), sum, sshclient.ShellQuote(version), role.exec, !role.ControlPlane), nil
}

// StartScript starts k3s on a node that joins the control plane, waits
// until it has started, and prints its last log lines if it fails to.
const StartScript = `systemctl start k3s-agent || {
	journalctl -u k3s-agent -n 20 --no-pager -o cat >&2
	exit 1
}`

// WaitReadyScript waits, on the control plane, until the node called name
// is Ready with the private address addr, and says what is wrong if it
// isn't within 10 minutes.
func WaitReadyScript(name string, addr netip.Addr) string {
	return fmt.Sprintf(`k3s kubectl wait --for=create node/%[1]s --timeout=600s >/dev/null 2>&1 &&
	k3s kubectl wait --for=condition=Ready node/%[1]s --timeout=600s >/dev/null 2>&1
state=$(k3s kubectl get node %[1]s --request-timeout=10s -o jsonpath='{range .status.addresses[?(@.type=="InternalIP")]}{.address}{end} {.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null)
[ "$state" = "%[2]s True" ] && exit 0
echo "%[1]s isn't Ready at %[2]s after 10 minutes" >&2
k3s kubectl get nodes -o wide >&2
k3s kubectl get events -A --sort-by=.lastTimestamp 2>/dev/null | tail -20 >&2
exit 1`, sshclient.ShellQuote(name), addr)
}

// NodeNamesScript prints the names of the cluster's nodes, as node/<name>
// lines, run on the control plane.
const NodeNamesScript = `k3s kubectl get nodes --request-timeout=30s -o name`

// VersionScript prints the k3s version of a node.
const VersionScript = `k3s --version | sed -n 's/^k3s version \([^ ]*\).*/\1/p'`

// ReachScript fails unless the node reaches the control plane's API at
// addr.
func ReachScript(addr netip.Addr) string {
	api := netip.AddrPortFrom(addr, 6443)
	return fmt.Sprintf(`curl -sk --max-time 10 -o /dev/null https://%[1]s/ping || {
	echo "it doesn't reach the control plane at %[1]s" >&2
	exit 1
}`, api)
}

// UninstallScript removes k3s from a node of role, and the files mooctl
// wrote there, after waiting up to a minute for an install still running
// to end, and fails if k3s's own uninstall script fails.
func UninstallScript(role Role) string {
	// The quotes keep the script's own command line from matching the
	// pgrep pattern below.
	files := []string{ConfigFile, RegistriesFile, DropIn(role), KubeletDropIn, `/usr/local/bin/zot.downloa"d"`}
	dirs := []string{"/etc/rancher/k3s", "/etc/systemd/system/" + role.service + ".service.d"}
	extra := ""
	if role.ControlPlane {
		files = append(files, "/usr/local/bin/zot", zotConfig, "/etc/systemd/system/"+ZotUnit, AppliedCommitFile,
			AutoDeployFile("traefik-config"), AutoDeployFile("cert-manager"), AutoDeployFile("issuer"))
		dirs = append(dirs, "/etc/moostack", "/var/lib/moostack")
		extra = fmt.Sprintf("systemctl disable --now %s 2>/dev/null\nrm -rf /var/lib/zot /var/lib/private/zot\n", ZotUnit)
	}
	return fmt.Sprintf(`for i in $(seq 60); do
	pgrep -f 'INSTALL_K3[S]_EXEC|zot[.]downloa[d]' >/dev/null || break
	sleep 1
done
s=0
if [ -x /usr/local/bin/%[1]s ]; then /usr/local/bin/%[1]s || s=$?; fi
%[2]srm -f %[3]s
for d in %[4]s; do rmdir "$d" 2>/dev/null; done
systemctl daemon-reload
exit $s`, role.uninstall, extra, strings.Join(files, " "), strings.Join(dirs, " "))
}

// LockScript holds the operation lock, printing LockReady once it has it,
// until its stdin closes or no line has come on it for 45 seconds.
const LockScript = `mkdir -p /run/moostack
exec 9>>/run/moostack/operation.lock
flock -n 9 || {
	echo "another mooctl operation is running on this cluster; wait for it to finish" >&2
	exit 1
}
echo ` + LockReady + `
while timeout 45 sh -c 'IFS= read -r _' 9>&-; do :; done`

// LockReady is what LockScript prints once it holds the lock.
const LockReady = "operation-lock-held"

// AppliedCommitScript prints the commit the cluster last applied, nothing
// if it has applied none, or "empty" if its file is empty, and fails if
// the file can't be read.
const AppliedCommitScript = `if [ -e ` + AppliedCommitFile + ` ]; then c=$(cat ` + AppliedCommitFile + `) || exit 1; echo "${c:-empty}"; fi`

// File is a file mooctl writes on a node.
type File struct {
	Path, Content string
}

// Written says what the files mooctl writes on a node hold.
type Written struct {
	Node Node
	Role Role
	// MemoryMiB is the node's memory, and GOGC and GOMEMLIMIT k3s's
	// memory settings on it.
	MemoryMiB        int
	GOGC, GOMEMLIMIT string
	// ControlPlane is the private address of the cluster's control plane.
	ControlPlane netip.Addr
	// Token is the token nodes join with.
	Token string
	// Email is the ACME account's email, empty for a self-signed issuer.
	Email string
}

// Files are the files mooctl writes on the node before it installs k3s.
func (w Written) Files() []File {
	config := AgentConfig(w.Node, w.ControlPlane, w.Token)
	if w.Role.ControlPlane {
		config = ServerConfig(w.Node, w.Token)
	}
	files := []File{
		{ConfigFile, config},
		{RegistriesFile, Registries(w.ControlPlane)},
		{DropIn(w.Role), MemoryDropIn(w.GOGC, w.GOMEMLIMIT)},
		{KubeletDropIn, KubeletConfig(w.MemoryMiB, w.Role)},
	}
	if w.Role.ControlPlane {
		files = append(files,
			File{AutoDeployFile("traefik-config"), TraefikConfig},
			File{AutoDeployFile("cert-manager"), CertManager()},
			File{AutoDeployFile("issuer"), IssuerManifest(w.Email)},
		)
	}
	return files
}
