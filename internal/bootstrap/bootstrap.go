// Package bootstrap turns a host into a node over SSH: it checks the
// host, installs containerd, the updater and the agent from a release as
// systemd units, issues the node its identity, and removes them again if
// anything fails.
package bootstrap

import (
	"bytes"
	"context"
	"embed"
	"errors"
	"fmt"
	"net/netip"
	"slices"
	"strings"
	"text/template"
	"time"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/hostlayout"
	"github.com/moostackhq/moostack/internal/identity"
	"github.com/moostackhq/moostack/internal/nodeclient"
	"github.com/moostackhq/moostack/internal/release"
	"github.com/moostackhq/moostack/internal/sshclient"
)

// The systemd units that run the node components.
const (
	containerdUnit = "moostack-containerd.service"
	updaterUnit    = "moostack-updater.service"
	agentUnit      = "moostack-agent.service"
)

// units are the systemd units. systemd starts them together, the agent
// after containerd. Uninstall stops them one by one in the reverse order,
// the agent first.
var units = []string{containerdUnit, updaterUnit, agentUnit}

// hostFile is a file to write on the host.
type hostFile struct{ path, content string }

// unitTemplates are the unit files, each a text/template named for its
// unit and executed with unitData. Every one of them is installed.
//
//go:embed units/*.service
var unitTemplates embed.FS

// unitData is what the unit templates are executed with.
type unitData struct {
	BinDir, AgentBinary, UpdaterBinary, ContainerdConfigFile, ContainerdUnit string
}

// unitFiles are the unit files to install, rendered from unitTemplates,
// by path.
var unitFiles = renderUnits()

// renderUnits renders every template in unitTemplates.
func renderUnits() []hostFile {
	tmpl := template.Must(template.New("").Option("missingkey=error").ParseFS(unitTemplates, "units/*.service"))
	data := unitData{
		BinDir:               hostlayout.BinDir,
		AgentBinary:          hostlayout.Agent,
		UpdaterBinary:        hostlayout.Updater,
		ContainerdConfigFile: hostlayout.ContainerdConfigFile,
		ContainerdUnit:       containerdUnit,
	}
	var files []hostFile
	for _, t := range tmpl.Templates() {
		if t.Name() == "" {
			continue
		}
		var b strings.Builder
		if err := t.Execute(&b, data); err != nil {
			panic(err)
		}
		files = append(files, hostFile{"/etc/systemd/system/" + t.Name(), b.String()})
	}
	slices.SortFunc(files, func(a, b hostFile) int { return strings.Compare(a.path, b.path) })
	return files
}

// installed are the paths an install creates, and so those whose
// presence means the host is or was a node.
func installed() []string {
	return append(append([]string{}, hostlayout.Dirs...), unitPaths()...)
}

// unitPaths are the paths of unitFiles.
func unitPaths() []string {
	var paths []string
	for _, u := range unitFiles {
		paths = append(paths, u.path)
	}
	return paths
}

// Host is what Check found out about a host.
type Host struct {
	// Architecture is amd64 or arm64.
	Architecture string
}

// Check checks that the host c connects to can become a node: that it
// is systemd-based Linux on amd64 or arm64, and has nothing of
// Moostack's installed.
func Check(ctx context.Context, c *sshclient.Client) (Host, error) {
	system, err := output(ctx, c, "uname -sm")
	if err != nil {
		return Host{}, err
	}
	var h Host
	switch kernel, machine, _ := strings.Cut(system, " "); {
	case kernel != "Linux":
		return Host{}, fmt.Errorf("the host runs %s, not Linux", kernel)
	case machine == "x86_64":
		h.Architecture = "amd64"
	case machine == "aarch64" || machine == "arm64":
		h.Architecture = "arm64"
	default:
		return Host{}, fmt.Errorf("the host is %s, not amd64 or arm64", machine)
	}
	if res, err := c.Run(ctx, "test -d /run/systemd/system", nil); err != nil {
		return Host{}, err
	} else if res.ExitCode != 0 {
		return Host{}, errors.New("the host doesn't run systemd")
	}
	if err := checkNotNode(ctx, c); err != nil {
		return Host{}, err
	}
	return h, nil
}

// Addresses are the host's addresses of global scope, so neither
// loopback, link-local nor site-local ones, as iproute2's ip lists them,
// one interface address per line: "2: eth0 inet 10.0.1.2/24 ...". The
// host needs ip.
func Addresses(ctx context.Context, c *sshclient.Client) ([]netip.Addr, error) {
	out, err := output(ctx, c, "ip -o addr show scope global")
	if err != nil {
		return nil, fmt.Errorf("listing the host's addresses: %w", err)
	}
	var addrs []netip.Addr
	for _, line := range strings.Split(out, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 4 || fields[2] != "inet" && fields[2] != "inet6" {
			continue
		}
		prefix, err := netip.ParsePrefix(fields[3])
		if err != nil {
			return nil, fmt.Errorf("listing the host's addresses: %q: %w", line, err)
		}
		addrs = append(addrs, prefix.Addr())
	}
	return addrs, nil
}

// PrivateAddress is the address other nodes reach a host at, when the
// operator doesn't say, from the host's Addresses: the first of hosts
// that is one of them, or else the only one. hosts are how mooctl names
// the host: the address the operator typed, then the one the SSH config
// resolved it to, which a port forward or jump host makes differ. With no
// such address, it fails, listing the host's.
func PrivateAddress(addrs []netip.Addr, hosts ...string) (netip.Addr, error) {
	for _, h := range hosts {
		if a, err := netip.ParseAddr(h); err == nil && slices.Contains(addrs, a.Unmap()) {
			return a.Unmap(), nil
		}
	}
	switch len(addrs) {
	case 0:
		return netip.Addr{}, errors.New("the host has no address other nodes could reach it at")
	case 1:
		return addrs[0], nil
	}
	list := make([]string, len(addrs))
	for i, a := range addrs {
		list[i] = a.String()
	}
	return netip.Addr{}, fmt.Errorf("the host has the addresses %s, so choose the one other nodes reach it at with --private-address", strings.Join(list, ", "))
}

// checkNotNode fails if the host has any of the paths an install creates.
func checkNotNode(ctx context.Context, c *sshclient.Client) error {
	var script strings.Builder
	for _, p := range installed() {
		fmt.Fprintf(&script, "if [ -e %[1]s ] || [ -L %[1]s ]; then echo %[1]s; fi\n", p)
	}
	found, err := output(ctx, c, script.String())
	if err != nil || found == "" {
		return err
	}
	res, err := c.Run(ctx, "cat "+hostlayout.NodeFile, nil)
	if err == nil && res.ExitCode == 0 {
		if n, err := hostlayout.ParseNodeConfig(res.Stdout); err == nil {
			return fmt.Errorf("the host already belongs to cluster %s, as node %s", n.Cluster, n.Node)
		}
	}
	paths := strings.Fields(found)
	return fmt.Errorf("the host already has Moostack's %s, so it may belong to a cluster", strings.Join(paths, ", "))
}

// healthTimeout bounds waiting for the node components to become healthy.
// A dev build can set another bound (healthWait).
const healthTimeout = time.Minute

// Install installs the components of rel for the host's architecture,
// configured as node, with the node identity that ca issues it, starts
// them, and waits until the agent and the updater report them healthy at
// rel's versions. The node's key is made on the host, and never leaves
// it. The host must have passed Check. If Install fails, it rolls back.
func Install(ctx context.Context, c *sshclient.Client, h Host, rel *release.Release, node hostlayout.NodeConfig, ca *identity.CA) error {
	wait, err := healthWait()
	if err != nil {
		return err
	}
	if err := install(ctx, c, h, rel, node, ca, wait); err != nil {
		return Rollback(ctx, c, err)
	}
	return nil
}

// Rollback uninstalls what an install put on the host, after the
// failure err, and returns err. If uninstalling fails too, the error
// says so, and says what to stop and remove by hand.
func Rollback(ctx context.Context, c *sshclient.Client, err error) error {
	if rbErr := Uninstall(ctx, c); rbErr != nil {
		return fmt.Errorf("%w; removing what was installed failed too, so stop %s and remove %s by hand: %v",
			err, strings.Join(units, ", "), strings.Join(installed(), ", "), rbErr)
	}
	return err
}

func install(ctx context.Context, c *sshclient.Client, h Host, rel *release.Release, node hostlayout.NodeConfig, ca *identity.CA, wait time.Duration) error {
	if _, err := output(ctx, c, "mkdir -p "+hostlayout.BinDir+" "+hostlayout.ConfigDir); err != nil {
		return err
	}
	for _, comp := range rel.Components {
		for _, f := range comp.Files {
			data, err := rel.ReadFile(h.Architecture, f)
			if err != nil {
				return err
			}
			if err := c.Upload(ctx, hostlayout.BinDir+"/"+f, bytes.NewReader(data), 0o755, "root:root"); err != nil {
				return err
			}
		}
	}
	request, err := output(ctx, c, hostlayout.Agent+" request-identity")
	if err != nil {
		return fmt.Errorf("making the node's key: %w", err)
	}
	cert, err := ca.Issue([]byte(request), node.Node)
	if err != nil {
		return fmt.Errorf("issuing the node's identity: %w", err)
	}
	nodeFile, err := node.Marshal()
	if err != nil {
		return err
	}
	files := append([]hostFile{
		{hostlayout.NodeFile, string(nodeFile)},
		{hostlayout.ContainerdConfigFile, hostlayout.ContainerdConfig},
		{hostlayout.Identity.Cert, string(cert)},
		{hostlayout.Identity.CA, string(ca.CertPEM())},
	}, unitFiles...)
	for _, f := range files {
		if err := c.Upload(ctx, f.path, strings.NewReader(f.content), 0o644, "root:root"); err != nil {
			return err
		}
	}
	if _, err := output(ctx, c, "systemctl daemon-reload && systemctl enable --now "+strings.Join(units, " ")); err != nil {
		return err
	}
	return waitHealthy(ctx, c, rel, wait)
}

// waitHealthy waits until the agent and the updater report every
// component healthy at the version rel pins, for up to wait, or until ctx
// ends.
func waitHealthy(parent context.Context, c *sshclient.Client, rel *release.Release, wait time.Duration) error {
	node, err := nodeclient.Dial(c)
	if err != nil {
		return err
	}
	defer node.Close()
	ctx, cancel := context.WithTimeout(parent, wait)
	defer cancel()
	for {
		err := healthy(ctx, node, rel)
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			if parent.Err() != nil {
				return fmt.Errorf("waiting for the node to become healthy: %w", parent.Err())
			}
			return fmt.Errorf("the node didn't become healthy within %s: %w", wait, err)
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// healthy fails unless the agent and the updater report every component
// of rel healthy at its pinned version.
func healthy(ctx context.Context, node *nodeclient.Node, rel *release.Release) error {
	callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	status := node.Status(callCtx)
	running := map[string]*nodev1.Component{}
	for _, comp := range status.Components {
		running[comp.Name] = comp
	}
	for _, want := range rel.Components {
		got := running[want.Name]
		switch {
		case got == nil:
			return fmt.Errorf("the node doesn't report %s", want.Name)
		case got.Health != nodev1.Health_HEALTH_HEALTHY:
			return fmt.Errorf("%s is %s: %s", want.Name, nodeclient.HealthName(got.Health), got.Problem)
		case got.Version != want.Version:
			return fmt.Errorf("%s runs version %s, not %s", want.Name, got.Version, want.Version)
		}
	}
	return nil
}

// uninstallTimeout bounds removing an install.
const uninstallTimeout = 2 * time.Minute

// Uninstall stops and removes the node components and everything an
// install created. A unit that systemd doesn't know is fine, but a unit
// that doesn't stop fails it before anything is removed, since its
// program would keep running from the files. It goes ahead after ctx
// ends, within its own timeout.
func Uninstall(ctx context.Context, c *sshclient.Client) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), uninstallTimeout)
	defer cancel()
	stopOrder := slices.Clone(units)
	slices.Reverse(stopOrder)
	stopping := strings.Join(stopOrder, " ")
	res, err := c.Run(ctx, `set -e
for unit in `+stopping+`; do
	if [ "$(systemctl show -p LoadState --value "$unit")" != not-found ]; then
		systemctl disable --now "$unit"
	fi
	state=$(systemctl show -p ActiveState --value "$unit")
	case $state in
	inactive|failed) ;;
	*) echo "$unit is still $state" >&2; exit 1 ;;
	esac
done
rm -f `+strings.Join(unitPaths(), " ")+`
systemctl daemon-reload
systemctl reset-failed `+stopping+` 2>/dev/null || true
rm -rf `+strings.Join(hostlayout.Dirs, " "), nil)
	if err != nil {
		return err
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("stopping and removing the node components: %s", bytes.TrimSpace(res.Stderr))
	}
	return nil
}

// output runs cmd as root, and returns its output trimmed. A command
// that exits non-zero fails with what it wrote to stderr.
func output(ctx context.Context, c *sshclient.Client, cmd string) (string, error) {
	res, err := c.Run(ctx, cmd, nil)
	if err != nil {
		return "", err
	}
	if res.ExitCode != 0 {
		return "", fmt.Errorf("%s: exit %d: %s", firstLine(cmd), res.ExitCode, bytes.TrimSpace(res.Stderr))
	}
	return strings.TrimSpace(string(res.Stdout)), nil
}

func firstLine(s string) string {
	line, _, _ := strings.Cut(s, "\n")
	return line
}
