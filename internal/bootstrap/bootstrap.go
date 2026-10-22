// Package bootstrap turns a host into a node over SSH: it checks the
// host, installs containerd, the updater and the agent from a release as
// systemd units, issues the node its identity, seeds the release's
// cluster services into containerd through the agent and starts them,
// and removes it all again if anything fails. Or it joins the host to a
// control plane, which issues the node its identity and starts the
// cluster services itself.
package bootstrap

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"path"
	"slices"
	"strings"
	"text/template"
	"time"

	controlplanev1 "github.com/moostackhq/moostack/api/controlplane/v1"
	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/controlplane"
	"github.com/moostackhq/moostack/internal/hostlayout"
	"github.com/moostackhq/moostack/internal/identity"
	"github.com/moostackhq/moostack/internal/nodeclient"
	"github.com/moostackhq/moostack/internal/release"
	"github.com/moostackhq/moostack/internal/sshclient"
)

// units are the systemd units. systemd starts them together, the agent
// after containerd. Uninstall stops them one by one in the reverse order,
// the agent first, and containerd once the cluster services are gone.
var units = []string{hostlayout.ContainerdUnit, hostlayout.UpdaterUnit, hostlayout.AgentUnit}

// hostFile is a file to write on the host.
type hostFile struct{ path, content string }

// unitTemplates are the unit files, each a text/template named for its
// unit and executed with unitData. Every one of them is installed.
//
//go:embed units/*.service
var unitTemplates embed.FS

// unitData is what the unit templates are executed with.
type unitData struct {
	BinDir, AgentBinary, UpdaterBinary, ContainerdConfigFile, ContainerdUnit, ContainerdPath string
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
		ContainerdUnit:       hostlayout.ContainerdUnit,
		ContainerdPath:       hostlayout.ContainerdPath,
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
// rel's versions. Then it has the agent import rel's cluster services'
// images and run them, and waits until the agent reports them healthy at
// rel's versions too. The node's key is made on the host, and never
// leaves it. The host must have passed Check. Install refuses a release
// without the cluster services before it changes the host. If Install
// fails after that, it rolls back. The node holds the identities removed
// lists as removed from before its agent starts. Install returns the
// fingerprint of the node's identity.
func Install(ctx context.Context, c *sshclient.Client, h Host, rel *release.Release, node hostlayout.NodeConfig, ca *identity.CA, removed []string) (string, error) {
	if err := rel.CheckClusterServices(); err != nil {
		return "", err
	}
	wait, err := healthWait()
	if err != nil {
		return "", err
	}
	fingerprint, err := install(ctx, c, h, rel, wait, installPlan{
		identify: func() (identified, error) {
			request, err := output(ctx, c, hostlayout.Agent+" request-identity")
			if err != nil {
				return identified{}, fmt.Errorf("making the node's key: %w", err)
			}
			cert, err := ca.Issue([]byte(request), node.Node)
			if err != nil {
				return identified{}, fmt.Errorf("issuing the node's identity: %w", err)
			}
			return identified{node: node, cert: cert, ca: ca.CertPEM()}, nil
		},
		removed:  removed,
		services: true,
	})
	if err != nil {
		return "", Rollback(ctx, c, err)
	}
	return fingerprint, nil
}

// Rollback uninstalls what an install put on the host, after the
// failure err, and returns err. If uninstalling fails too, the error
// says so, and says what to stop and remove by hand.
func Rollback(ctx context.Context, c *sshclient.Client, err error) error {
	if rbErr := Uninstall(ctx, c); rbErr != nil {
		return fmt.Errorf("%w; removing what was installed failed too, so %s: %v", err, CleanByHand, rbErr)
	}
	return err
}

// CleanByHand tells an operator how to clean a host of Moostack by hand,
// when Uninstall can't: what to stop and what to remove.
var CleanByHand = fmt.Sprintf("stop %s and remove %s by hand", strings.Join(units, ", "), strings.Join(installed(), ", "))

// Join installs the components of rel for the host's architecture, has
// the agent join the control plane of tok, and starts the components in
// control plane mode, as a node of the control plane's cluster with
// private as its private address. It waits until the agent and the
// updater report the components healthy at rel's versions, and leaves
// the cluster services to the control plane. The node's key is made on
// the host, and never leaves it, and so does tok's secret, which goes to
// the agent on its stdin. The host must have passed Check. If Join
// fails, it rolls back. Join returns what the node got from the control
// plane.
func Join(ctx context.Context, c *sshclient.Client, h Host, rel *release.Release, private netip.Addr, tok *controlplanev1.JoinToken) (*controlplane.Joined, error) {
	wait, err := healthWait()
	if err != nil {
		return nil, err
	}
	var joined *controlplane.Joined
	_, err = install(ctx, c, h, rel, wait, installPlan{identify: func() (identified, error) {
		token, err := controlplanev1.EncodeToken(tok)
		if err != nil {
			return identified{}, err
		}
		res, err := c.Run(ctx, hostlayout.Agent+" join", strings.NewReader(token+"\n"))
		switch {
		case err != nil:
			// The session ended, as by an interrupt or a cut connection,
			// but agent join may run on.
			return identified{}, fmt.Errorf("joining the control plane at %s: %w; agent join may still finish on the host, spending the join token, so the control plane may hold a node that no host is", tok.Address, err)
		case res.ExitCode != 0:
			return identified{}, fmt.Errorf("agent join failed: %s", strings.TrimPrefix(string(bytes.TrimSpace(res.Stderr)), "agent: "))
		}
		var j controlplane.Joined
		if err := json.Unmarshal(res.Stdout, &j); err != nil {
			return identified{}, fmt.Errorf("agent join printed %q: %w; it may have joined the control plane, spending the join token", res.Stdout, err)
		}
		joined = &j
		node := hostlayout.NodeConfig{Cluster: joined.Cluster, Node: joined.Node, Mode: hostlayout.ModeControlPlane, ControlPlane: tok.Address, PrivateAddress: private}
		return identified{node: node, cert: joined.Certificate, ca: joined.CA}, nil
	}})
	if err != nil {
		if joined != nil {
			err = fmt.Errorf("%w\nThe control plane at %s issued the host its identity as node %s, so the join token is spent: ask the control plane for a new one, and to forget node %s", err, tok.Address, joined.Node, joined.Node)
		}
		return nil, Rollback(ctx, c, err)
	}
	return joined, nil
}

// identified is how a node is identified: its node file, its
// certificate and the CA's, both PEM.
type identified struct {
	node     hostlayout.NodeConfig
	cert, ca []byte
}

// installPlan is what tells one install from another: how the node gets
// its identity, once its key is the only one there can be, the
// identities it holds as removed, and whether it starts the release's
// cluster services.
type installPlan struct {
	identify func() (identified, error)
	removed  []string
	services bool
}

// install installs the components of rel, identified as plan says, and
// starts them, and the cluster services if plan says. It returns the
// fingerprint of the node's identity.
func install(ctx context.Context, c *sshclient.Client, h Host, rel *release.Release, wait time.Duration, plan installPlan) (string, error) {
	if _, err := output(ctx, c, "mkdir -p "+hostlayout.BinDir+" "+hostlayout.ConfigDir); err != nil {
		return "", err
	}
	for _, comp := range rel.Components {
		for _, f := range comp.Files {
			data, err := rel.ReadFile(h.Architecture, f)
			if err != nil {
				return "", err
			}
			to := hostlayout.BinDir + "/" + f
			if slots, ok := hostlayout.Slots(to); ok {
				to = slots[0]
			}
			if err := c.Upload(ctx, to, bytes.NewReader(data), 0o755, "root:root"); err != nil {
				return "", err
			}
		}
	}
	// The agent and the updater run from their first A/B slots, until
	// one swaps the other.
	for _, link := range []string{hostlayout.Agent, hostlayout.Updater} {
		slots, _ := hostlayout.Slots(link)
		if _, err := output(ctx, c, "ln -sfn "+path.Base(slots[0])+" "+link); err != nil {
			return "", err
		}
	}
	id, err := plan.identify()
	if err != nil {
		return "", err
	}
	fingerprint, err := identity.Fingerprint(id.cert)
	if err != nil {
		return "", err
	}
	nodeFile, err := id.node.Marshal()
	if err != nil {
		return "", err
	}
	files := append([]hostFile{
		{hostlayout.NodeFile, string(nodeFile)},
		{hostlayout.ContainerdConfigFile, hostlayout.ContainerdConfig},
		{hostlayout.Identity.Cert, string(id.cert)},
		{hostlayout.Identity.CA, string(id.ca)},
	}, unitFiles...)
	for _, f := range files {
		if err := c.Upload(ctx, f.path, strings.NewReader(f.content), 0o644, "root:root"); err != nil {
			return "", err
		}
	}
	// The state directory exists once the node's key is made.
	if err := c.Upload(ctx, hostlayout.RemovedIdentitiesFile, bytes.NewReader(hostlayout.MarshalRemovedIdentities(plan.removed)), 0o600, "root:root"); err != nil {
		return "", err
	}
	if _, err := output(ctx, c, "systemctl daemon-reload && systemctl enable --now "+strings.Join(units, " ")); err != nil {
		return "", err
	}
	api, err := nodeclient.Dial(c)
	if err != nil {
		return "", err
	}
	defer api.Close()
	if err := waitHealthy(ctx, api, wait, func(s nodeclient.Status) error { return componentsHealthy(s, rel) }); err != nil {
		return "", err
	}
	if !plan.services {
		return fingerprint, nil
	}
	if err := startServices(ctx, api, h, rel); err != nil {
		return "", err
	}
	if err := waitHealthy(ctx, api, wait, func(s nodeclient.Status) error { return servicesHealthy(s, rel) }); err != nil {
		return "", err
	}
	return fingerprint, nil
}

// startServices has the agent import the image of each of rel's cluster
// services for the host's architecture, checked against rel's checksums,
// and run the service.
func startServices(ctx context.Context, node *nodeclient.Node, h Host, rel *release.Release) error {
	for _, s := range rel.Services {
		data, err := rel.ReadFile(h.Architecture, s.Image)
		if err != nil {
			return err
		}
		ref := release.ImageRef(s.Name, s.Version)
		images, err := nodeclient.ImportImage(ctx, node.Agent, data)
		if err != nil {
			return fmt.Errorf("seeding %s's image: %w", s.Name, err)
		}
		if !slices.Contains(images, ref) {
			return fmt.Errorf("seeding %s's image imported %s, not %s", s.Name, strings.Join(images, ", "), ref)
		}
		_, err = node.Agent.RunService(ctx, &nodev1.RunServiceRequest{Service: &nodev1.Service{Name: s.Name, Version: s.Version, Api: s.API, Image: ref}})
		if err != nil {
			return fmt.Errorf("starting %s: %w", s.Name, err)
		}
	}
	return nil
}

// waitHealthy waits until the status the agent and the updater report
// passes check, for up to wait, or until ctx ends.
func waitHealthy(parent context.Context, node *nodeclient.Node, wait time.Duration, check func(nodeclient.Status) error) error {
	ctx, cancel := context.WithTimeout(parent, wait)
	defer cancel()
	for {
		callCtx, cancelCall := context.WithTimeout(ctx, 5*time.Second)
		err := check(node.Status(callCtx))
		cancelCall()
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

// componentsHealthy fails unless status reports every component of rel
// healthy at its pinned version.
func componentsHealthy(status nodeclient.Status, rel *release.Release) error {
	return reportedHealthy(status.Components, rel.PinnedComponents())
}

// servicesHealthy fails unless status reports every cluster service of
// rel healthy at its pinned version.
func servicesHealthy(status nodeclient.Status, rel *release.Release) error {
	if status.AgentErr != nil {
		return fmt.Errorf("the agent doesn't answer: %w", status.AgentErr)
	}
	return reportedHealthy(status.Services, rel.PinnedServices())
}

// reportedHealthy fails unless reported has each of pinned, healthy at
// its version.
func reportedHealthy(reported []*nodev1.Component, pinned []release.Pin) error {
	running := map[string]*nodev1.Component{}
	for _, comp := range reported {
		running[comp.Name] = comp
	}
	for _, want := range pinned {
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

// Uninstall stops and removes the node components, the cluster
// services' containers, and everything an install created. A unit that
// systemd doesn't know is fine, but a unit that doesn't stop fails it
// before anything is removed, since its program would keep running from
// the files, and so do containers that don't end. It goes ahead after ctx
// ends, within its own timeout.
func Uninstall(ctx context.Context, c *sshclient.Client) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), uninstallTimeout)
	defer cancel()
	stopOrder := slices.Clone(units)
	slices.Reverse(stopOrder)
	stopping := strings.Join(stopOrder, " ")
	cgroup := hostlayout.CgroupDir
	res, err := c.Run(ctx, `set -e
loaded() { [ "$(systemctl show -p LoadState --value "$1")" != not-found ]; }
stop() {
	if loaded "$1"; then
		systemctl disable --now "$1"
	fi
	state=$(systemctl show -p ActiveState --value "$1")
	case $state in
	inactive|failed) ;;
	*) echo "$1 is still $state" >&2; exit 1 ;;
	esac
}
stop `+hostlayout.AgentUnit+`
stop `+hostlayout.UpdaterUnit+`
# The cluster services' containers, and containerd's shims, outlive
# containerd's unit stopping, by its KillMode, so they are killed first.
if [ -d `+cgroup+` ]; then
	find `+cgroup+` -name cgroup.kill | while read -r kill; do echo 1 > "$kill"; done
	for i in $(seq 50); do
		find `+cgroup+` -depth -type d -exec rmdir {} + 2>/dev/null || true
		[ -d `+cgroup+` ] || break
		sleep 0.1
	done
	if [ -d `+cgroup+` ]; then
		echo "the cluster services' containers in `+cgroup+` didn't end" >&2; exit 1
	fi
fi
if loaded `+hostlayout.ContainerdUnit+`; then
	systemctl kill --kill-whom=all --signal=SIGKILL `+hostlayout.ContainerdUnit+` 2>/dev/null || true
fi
stop `+hostlayout.ContainerdUnit+`
# The containers' root filesystems stay mounted once they are killed.
awk '{print $2}' /proc/self/mounts | grep -E '^(`+strings.Join(hostlayout.Dirs, "|")+`)(/|$)' | sort -r | while read -r m; do umount -l "$m"; done
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
