//go:build linux

// Package nodetest runs a node's components in a privileged container, as
// a node runs them, for tests that call the node API through the
// components' relays. It needs docker; tests that use it are skipped
// without it.
package nodetest

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/hostlayout"
	"github.com/moostackhq/moostack/internal/identity"
	"github.com/moostackhq/moostack/internal/nodeclient"
	"github.com/moostackhq/moostack/internal/ociimage"
	"github.com/moostackhq/moostack/internal/release"
)

// Version is the version the agent and the updater under test are built
// with.
const Version = "9.8.7-test"

// NextVersion is the version of NextAgent and NextUpdater, an agent and
// an updater to update to.
const NextVersion = "9.8.8-test"

// Node is a container running containerd, the updater and the agent.
type Node struct {
	// Name is the container's.
	Name string
	// NodeName is the node's name, which its identity names.
	NodeName string
	// CA is the CA of the node's cluster, which issued its identity.
	CA *identity.CA
	// PeerAddress is where the agent listens for peers: the container's
	// address, which is the node's private address.
	PeerAddress netip.AddrPort
	// Agent and Updater call the node API through each one's own relay.
	Agent   nodev1.AgentClient
	Updater nodev1.UpdaterClient
	// config is the directory mounted as hostlayout.ConfigDir.
	config string
	// request is the certificate request for the node's key.
	request []byte
}

// programs are the node's programs, each started in the background with
// its pid in /run/<name>.pid and its output in /var/log/<name>.log, in
// place of its systemd unit. containerd has the PATH its unit gives it.
var programs = []program{
	{"containerd", hostlayout.ContainerdUnit, "env PATH=" + hostlayout.ContainerdPath + " " + hostlayout.BinDir + "/containerd --config " + hostlayout.ContainerdConfigFile},
	{"updater", hostlayout.UpdaterUnit, hostlayout.Updater + " serve"},
	{"agent", hostlayout.AgentUnit, hostlayout.Agent + " serve"},
}

// program is one of the node's programs, by name, with its unit and the
// command that starts it.
type program struct{ name, unit, command string }

// builtDir is where the container mounts the directory that binaries
// returns, read-only. Start copies the programs from there into
// hostlayout.BinDir, laid out as node add lays them out.
const builtDir = "/mnt/nodetest-bin"

// systemctl is a stand-in for systemctl, since the container doesn't run
// systemd: "systemctl restart UNIT" stops the program of the unit, waits
// for it to exit, and starts it again, as programs start it, and
// "systemctl reset-failed UNIT" does nothing. BreakRestarts makes its
// restarts fail or hang.
func systemctl() string {
	var cases strings.Builder
	for _, p := range programs {
		fmt.Fprintf(&cases, "%s) name=%s ;;\n", p.unit, p.name)
	}
	var starts strings.Builder
	for _, p := range programs {
		fmt.Fprintf(&starts, "%s) %s", p.name, startScript(p.name, p.command))
		starts.WriteString(";;\n")
	}
	return "#!/bin/sh\n" +
		"[ \"$1\" = reset-failed ] && exit 0\n" +
		"[ -e " + restartsFail + " ] && { echo 'the restart failed' >&2; exit 1; }\n" +
		"[ -e " + restartsHang + " ] && exec sleep infinity\n" +
		"[ \"$1\" = restart ] || { echo \"nodetest's systemctl only restarts\" >&2; exit 1; }\n" +
		"case $2 in\n" + cases.String() + "*) echo \"no unit $2\" >&2; exit 1 ;;\nesac\n" +
		stopScript("$name", "TERM") +
		"case $name in\n" + starts.String() + "esac\n"
}

// The files that make nodetest's systemctl fail or hang a restart.
const (
	restartsFail = "/run/nodetest-restarts-fail"
	restartsHang = "/run/nodetest-restarts-hang"
)

// Restarts is how a node's systemctl restarts a unit.
type Restarts int

const (
	// RestartsWork restarts the unit.
	RestartsWork Restarts = iota
	// RestartsFail fails at once, as when systemd refuses.
	RestartsFail
	// RestartsHang never returns, as when a program won't stop.
	RestartsHang
)

// BreakRestarts makes systemctl restart units as r says, from now on.
func (n *Node) BreakRestarts(t *testing.T, r Restarts) {
	t.Helper()
	script := "rm -f " + restartsFail + " " + restartsHang
	switch r {
	case RestartsFail:
		script += " && touch " + restartsFail
	case RestartsHang:
		script += " && touch " + restartsHang
	}
	n.Exec(t, "root", "sh", "-c", script)
}

// stopScript is the shell lines that send the program name signal and
// wait for it to exit. Nothing reaps a program once it exits, so it is
// gone when it is a zombie.
func stopScript(name, signal string) string {
	return "pid=$(cat /run/" + name + ".pid)\n" +
		"kill -" + signal + " $pid 2>/dev/null\n" +
		"while [ -e /proc/$pid ] && [ \"$(cut -d' ' -f3 /proc/$pid/stat)\" != Z ]; do sleep 0.1; done\n"
}

// Start starts a node, and removes it when the test ends. It runs the
// test in parallel with others. The node's identity is issued as node
// add issues it: the agent makes the key on the node, and the CA signs
// its request.
func Start(t *testing.T) *Node {
	t.Helper()
	if exec.Command("docker", "info").Run() != nil {
		t.Skip("docker doesn't work")
	}
	t.Parallel()
	bin := binaries(t)
	n := &Node{NodeName: "node-1", config: t.TempDir()}
	// The state directory is a volume of its own, since containerd's
	// snapshots can't be overlays on the container's overlay.
	out, err := exec.Command("docker", "run", "-d", "--rm", "--privileged",
		"-v", bin+":"+builtDir+":ro",
		"-v", n.config+":"+hostlayout.ConfigDir+":ro",
		"-v", hostlayout.StateDir,
		"debian:13-slim", "sleep", "infinity").Output()
	if err != nil {
		t.Fatalf("starting the node: %v\n%s", err, stderr(err))
	}
	n.Name = strings.TrimSpace(string(out))
	t.Cleanup(func() {
		if t.Failed() {
			for _, p := range programs {
				log, _ := exec.Command("docker", "exec", n.Name, "cat", "/var/log/"+p.name+".log").CombinedOutput()
				t.Logf("%s's log:\n%s", p.name, log)
			}
			for _, s := range release.ClusterServices {
				log, _ := exec.Command("docker", "exec", n.Name, "cat", hostlayout.ServiceLogDir+"/"+s.Name+".log").CombinedOutput()
				t.Logf("%s's log:\n%s", s.Name, log)
			}
		}
		exec.Command("docker", "rm", "-f", "-v", n.Name).Run()
	})
	out, err = exec.Command("docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", n.Name).Output()
	if err != nil {
		t.Fatalf("finding the node's address: %v\n%s", err, stderr(err))
	}
	private, err := netip.ParseAddr(strings.TrimSpace(string(out)))
	if err != nil {
		t.Fatalf("the node's address: %v", err)
	}
	n.PeerAddress = netip.AddrPortFrom(private, hostlayout.PeerPort)

	nodeFile, err := hostlayout.NodeConfig{Cluster: "test", Node: n.NodeName, Mode: hostlayout.ModeDirect, PrivateAddress: private}.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	n.WriteConfig(t, hostlayout.NodeFile, nodeFile)
	n.WriteConfig(t, hostlayout.ContainerdConfigFile, []byte(hostlayout.ContainerdConfig))
	if n.CA, err = identity.NewCA("test"); err != nil {
		t.Fatal(err)
	}
	n.Exec(t, "root", "sh", "-c", "set -e\n"+
		"mkdir -p "+hostlayout.BinDir+"\n"+
		"find "+builtDir+" -maxdepth 1 -type f -exec cp {} "+hostlayout.BinDir+" \\;\n"+
		"mv "+hostlayout.Agent+" "+hostlayout.AgentSlots[0]+"\n"+
		"ln -s "+filepath.Base(hostlayout.AgentSlots[0])+" "+hostlayout.Agent+"\n"+
		"mv "+hostlayout.Updater+" "+hostlayout.UpdaterSlots[0]+"\n"+
		"ln -s "+filepath.Base(hostlayout.UpdaterSlots[0])+" "+hostlayout.Updater+"\n"+
		"cat >/usr/local/bin/systemctl <<'EOF'\n"+systemctl()+"EOF\n"+
		"chmod 755 /usr/local/bin/systemctl\n")
	request, err := exec.Command("docker", "exec", n.Name, hostlayout.Agent, "request-identity").Output()
	if err != nil {
		t.Fatalf("making the node's key: %v\n%s", err, stderr(err))
	}
	n.request = request
	n.WriteConfig(t, hostlayout.Identity.Cert, n.Issue(t, n.CA, request))
	n.WriteConfig(t, hostlayout.Identity.CA, n.CA.CertPEM())

	var script strings.Builder
	for _, p := range programs {
		script.WriteString(startScript(p.name, p.command))
	}
	if out, err := exec.Command("docker", "exec", n.Name, "sh", "-c", script.String()).CombinedOutput(); err != nil {
		t.Fatalf("starting the node's programs: %v\n%s", err, out)
	}
	agent := n.dial(t, hostlayout.AgentSocket)
	updater := n.dial(t, hostlayout.UpdaterSocket)
	n.Agent, n.Updater = nodev1.NewAgentClient(agent), nodev1.NewUpdaterClient(updater)
	return n
}

// WriteConfig writes data to path in hostlayout.ConfigDir, where the
// node's programs read it.
func (n *Node) WriteConfig(t *testing.T, path string, data []byte) {
	t.Helper()
	rel, ok := strings.CutPrefix(path, hostlayout.ConfigDir+"/")
	if !ok {
		t.Fatalf("%s isn't in %s", path, hostlayout.ConfigDir)
	}
	writeFile(t, filepath.Join(n.config, rel), data)
}

// Issue is the node's identity that ca issues for request: its
// certificate, PEM.
func (n *Node) Issue(t *testing.T, ca *identity.CA, request []byte) []byte {
	t.Helper()
	cert, err := ca.Issue(request, n.NodeName)
	if err != nil {
		t.Fatal(err)
	}
	return cert
}

// PeerIdentity is the identity that ca issues a peer called name, made
// on the test's machine.
func PeerIdentity(t *testing.T, ca *identity.CA, name string) *identity.Identity {
	t.Helper()
	dir := t.TempDir()
	files := identity.Files{Key: filepath.Join(dir, "node.key"), Cert: filepath.Join(dir, "node.crt"), CA: filepath.Join(dir, "ca.crt")}
	request, err := identity.NewKey(files.Key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := ca.Issue(request, name)
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, files.Cert, cert)
	writeFile(t, files.CA, ca.CertPEM())
	id, err := identity.Load(files)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// CheckRefusesInvalidIdentity checks that program, the agent's or the
// updater's binary, won't serve without a valid node identity: with no
// key, no certificate, a certificate for another key, or one from
// another CA.
func CheckRefusesInvalidIdentity(t *testing.T, program string) {
	n := Start(t)
	n.EventuallyHealthy(t)
	other, err := identity.NewCA("other")
	if err != nil {
		t.Fatal(err)
	}
	otherKey, err := identity.NewKey(filepath.Join(t.TempDir(), "other.key"))
	if err != nil {
		t.Fatal(err)
	}
	cert := n.Issue(t, n.CA, n.request)
	for _, c := range []struct {
		name string
		// removeKey moves the node's key away while program runs.
		removeKey bool
		cert      []byte
	}{
		{name: "no key", removeKey: true, cert: cert},
		{name: "no certificate"},
		{name: "a certificate for another key", cert: n.Issue(t, n.CA, otherKey)},
		{name: "a certificate from another CA", cert: n.Issue(t, other, n.request)},
	} {
		if c.removeKey {
			n.Exec(t, "root", "mv", hostlayout.Identity.Key, "/tmp/node.key")
		}
		if c.cert == nil {
			if err := os.Remove(filepath.Join(n.config, filepath.Base(hostlayout.Identity.Cert))); err != nil {
				t.Fatal(err)
			}
		} else {
			n.WriteConfig(t, hostlayout.Identity.Cert, c.cert)
		}
		out, err := n.Output("root", "timeout", "20", program, "serve")
		exit, ok := err.(*exec.ExitError)
		switch {
		case !ok:
			t.Errorf("with %s, %s serve: %v, want it to refuse\n%s", c.name, filepath.Base(program), err, out)
		case exit.ExitCode() == 124:
			t.Errorf("with %s, %s serve kept serving", c.name, filepath.Base(program))
		case !strings.Contains(out, "identity"):
			t.Errorf("with %s, %s serve failed with %q, want an error about the node's identity", c.name, filepath.Base(program), out)
		}
		if c.removeKey {
			n.Exec(t, "root", "mv", "/tmp/node.key", hostlayout.Identity.Key)
		}
		n.WriteConfig(t, hostlayout.Identity.Cert, cert)
	}
}

// dial is a connection to socket through the relay of the program that
// serves it, run as root in the container.
func (n *Node) dial(t *testing.T, socket string) *grpc.ClientConn {
	t.Helper()
	program, ok := hostlayout.Server(socket)
	if !ok {
		t.Fatalf("no program serves %s", socket)
	}
	return n.dialThrough(t, program, socket)
}

// dialThrough is a connection to socket through program's relay, run as
// root in the container.
func (n *Node) dialThrough(t *testing.T, program, socket string) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient("passthrough:///"+filepath.Base(program),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return n.relay(program, socket) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

// relay is a connection to socket through program's relay.
func (n *Node) relay(program, socket string) (net.Conn, error) {
	cmd := exec.Command("docker", "exec", "-i", n.Name, program, "relay", socket)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return &pipeConn{cmd: cmd, stdin: stdin, stdout: stdout, socket: socket}, nil
}

// Status is the agent's status, within a few seconds.
func (n *Node) Status(t *testing.T) (*nodev1.StatusResponse, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	return n.Agent.Status(ctx, &nodev1.StatusRequest{})
}

// UpdaterStatus is the updater's status, within a few seconds.
func (n *Node) UpdaterStatus(t *testing.T) (*nodev1.UpdaterStatusResponse, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	return n.Updater.Status(ctx, &nodev1.UpdaterStatusRequest{})
}

// EventuallyHealthy waits for the agent to report every component
// healthy, and returns that status.
func (n *Node) EventuallyHealthy(t *testing.T) *nodev1.StatusResponse {
	t.Helper()
	var status *nodev1.StatusResponse
	Eventually(t, 60*time.Second, func() error {
		var err error
		if status, err = n.Status(t); err != nil {
			return err
		}
		for _, c := range append(slices.Clone(status.Components), status.Services...) {
			if c.Health != nodev1.Health_HEALTH_HEALTHY {
				return fmt.Errorf("%s is %s: %s", c.Name, c.Health, c.Problem)
			}
		}
		return nil
	})
	return status
}

// StartServices has the agent import the cluster services' images and
// run them, as node add does, and waits until the agent reports every
// component and service healthy.
func (n *Node) StartServices(t *testing.T) *nodev1.StatusResponse {
	t.Helper()
	for _, s := range release.ClusterServices {
		n.ImportImage(t, s.Name)
		n.RunService(t, s.Name)
	}
	return n.EventuallyHealthy(t)
}

// ImportImage has the agent import the image of the cluster service
// called name, within a minute.
func (n *Node) ImportImage(t *testing.T, name string) {
	t.Helper()
	n.importImage(t, name+".tar", release.ImageRef(name, Version))
}

// DeleteImage deletes the image called name from the node's containerd,
// as an operator who cleans up by hand would, leaving the containers made
// from it.
func (n *Node) DeleteImage(t *testing.T, name string) {
	t.Helper()
	client, err := containerd.NewWithConn(n.dialThrough(t, hostlayout.Agent, hostlayout.ContainerdSocket))
	if err != nil {
		t.Fatal(err)
	}
	ctx := namespaces.WithNamespace(t.Context(), hostlayout.ContainerdNamespace)
	if err := client.ImageService().Delete(ctx, name); err != nil {
		t.Fatalf("deleting the image %s: %v", name, err)
	}
}

// ImageArchive is the image archive of the cluster service called name,
// as ImportImage imports it.
func ImageArchive(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(binaries(t), imagesDir, name+".tar"))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// ImportFixtures has the agent import the images of Fixture and Broken.
func (n *Node) ImportFixtures(t *testing.T) {
	t.Helper()
	n.importImage(t, fixtureImage, Fixture().Image)
	n.importImage(t, brokenImage, Broken().Image)
}

// ImportFixtureAs has the agent import an image of fixtureservice that
// serves svc's API, named as svc's image.
func (n *Node) ImportFixtureAs(t *testing.T, svc *nodev1.Service) {
	t.Helper()
	program, err := os.ReadFile(filepath.Join(binaries(t), imagesDir, fixtureProgram))
	if err != nil {
		t.Fatal(err)
	}
	var archive bytes.Buffer
	if err := ociimage.Write(&archive, fixtureImageOf(program, svc)); err != nil {
		t.Fatal(err)
	}
	n.importArchive(t, svc.Image, archive.Bytes(), svc.Image)
}

// ImportFixtureRebuiltAs is ImportFixtureAs, but of another build of
// that image, under the same name: one with a file more.
func (n *Node) ImportFixtureRebuiltAs(t *testing.T, svc *nodev1.Service) {
	t.Helper()
	program, err := os.ReadFile(filepath.Join(binaries(t), imagesDir, fixtureProgram))
	if err != nil {
		t.Fatal(err)
	}
	img := fixtureImageOf(program, svc)
	img.Files = append(img.Files, ociimage.File{Name: "rebuilt", Mode: 0o644, Data: []byte("another build\n")})
	var archive bytes.Buffer
	if err := ociimage.Write(&archive, img); err != nil {
		t.Fatal(err)
	}
	n.importArchive(t, svc.Image, archive.Bytes(), svc.Image)
}

// importImage has the agent import the image archive file in
// imagesDir, which holds the image want, within a minute.
func (n *Node) importImage(t *testing.T, file, want string) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(binaries(t), imagesDir, file))
	if err != nil {
		t.Fatal(err)
	}
	n.importArchive(t, file, data, want)
}

// importArchive has the agent import the image archive data, which
// holds the image want, within a minute. what names the archive in a
// failure.
func (n *Node) importArchive(t *testing.T, what string, data []byte, want string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	images, err := nodeclient.ImportImage(ctx, n.Agent, data)
	if err != nil {
		t.Fatalf("importing %s: %v", what, err)
	}
	if !slices.Contains(images, want) {
		t.Fatalf("importing %s imported %v, want %s", what, images, want)
	}
}

// Fixture is the dns service as the test program fixtureservice serves
// it: the same API, but every caller kind may call each method, Ping
// too.
func Fixture() *nodev1.Service {
	s := Service("dns")
	s.Version = "fixture"
	s.Image = release.ImageRef(s.Name, s.Version)
	return s
}

// Broken is a service whose image has no program, so that it never
// starts.
func Broken() *nodev1.Service {
	return &nodev1.Service{Name: "broken", Version: Version, Api: "moostack.broken.v1.Broken", Image: release.ImageRef("broken", Version)}
}

// RunService has the agent run the cluster service called name, as the
// release pins it, within a minute.
func (n *Node) RunService(t *testing.T, name string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	if _, err := n.Agent.RunService(ctx, &nodev1.RunServiceRequest{Service: Service(name)}); err != nil {
		t.Fatalf("running %s: %v", name, err)
	}
}

// Service is the cluster service called name, as the release under test
// pins it.
func Service(name string) *nodev1.Service {
	for _, s := range release.ClusterServices {
		if s.Name == name {
			return &nodev1.Service{Name: s.Name, Version: Version, Api: s.API, Image: release.ImageRef(s.Name, Version)}
		}
	}
	panic("nodetest: no cluster service " + name)
}

// AgentConn is a connection to the agent's socket, through its relay,
// for calls to the cluster services.
func (n *Node) AgentConn(t *testing.T) *grpc.ClientConn {
	t.Helper()
	return n.dial(t, hostlayout.AgentSocket)
}

// Kill kills one of the node's programs: containerd, updater or agent.
func (n *Node) Kill(t *testing.T, program string) {
	t.Helper()
	n.Exec(t, "root", "sh", "-c", "kill $(cat /run/"+program+".pid)")
}

// Restart stops one of the node's programs, waits for it to exit, and
// starts it again, as restarting its unit would.
func (n *Node) Restart(t *testing.T, program string) {
	t.Helper()
	n.Exec(t, "root", "systemctl", "restart", programNamed(t, program).unit)
}

// programNamed is the node's program called name, failing t if there is
// none.
func programNamed(t *testing.T, name string) program {
	t.Helper()
	for _, p := range programs {
		if p.name == name {
			return p
		}
	}
	t.Fatalf("the node has no program %s", name)
	return program{}
}

// Crash kills one of the node's programs with SIGKILL, as a crash would,
// waits for it to exit, and starts it again, as systemd restarts its
// unit.
func (n *Node) Crash(t *testing.T, program string) {
	t.Helper()
	p := programNamed(t, program)
	n.Exec(t, "root", "sh", "-c", stopScript(p.name, "KILL")+startScript(p.name, p.command))
}

// Agent is the agent's binary, built at Version.
func Agent(t *testing.T) []byte { return readBuilt(t, "agent") }

// NextAgent is the agent's binary built at NextVersion, to update to.
func NextAgent(t *testing.T) []byte { return readBuilt(t, nextAgent) }

// readBuilt reads the file called name in the directory that binaries
// returns.
func readBuilt(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(binaries(t), name))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// Containerd are the release's containerd binaries, which the node
// runs, by name.
func Containerd(t *testing.T) map[string][]byte {
	t.Helper()
	files := map[string][]byte{}
	for _, f := range release.ContainerdFiles {
		files[f] = readBuilt(t, f)
	}
	return files
}

// Updater is the updater's binary, built at Version.
func Updater(t *testing.T) []byte { return readBuilt(t, "updater") }

// NextUpdater is the updater's binary built at NextVersion, to update
// to.
func NextUpdater(t *testing.T) []byte { return readBuilt(t, nextUpdater) }

// nextAgent and nextUpdater are NextAgent's and NextUpdater's files in
// binaries', in a directory of their own, so that Start doesn't install
// them.
const (
	nextAgent   = "next/agent"
	nextUpdater = "next/updater"
)

// NextAgentPath and NextUpdaterPath are where the container has
// NextAgent and NextUpdater.
const (
	NextAgentPath   = builtDir + "/" + nextAgent
	NextUpdaterPath = builtDir + "/" + nextUpdater
)

// startScript is the shell line that starts the program name with
// command in the background, with its pid in /run/<name>.pid and its
// output appended to /var/log/<name>.log.
func startScript(name, command string) string {
	return fmt.Sprintf("%s >>/var/log/%s.log 2>&1 & echo $! >/run/%[2]s.pid\n", command, name)
}

// Exec runs args in the node as user, failing the test if it fails.
func (n *Node) Exec(t *testing.T, user string, args ...string) {
	t.Helper()
	if out, err := n.Output(user, args...); err != nil {
		t.Fatalf("%s: %v\n%s", strings.Join(args, " "), err, out)
	}
}

// Output runs args in the node as user, and returns what they print and
// how they failed, if they did.
func (n *Node) Output(user string, args ...string) (string, error) {
	out, err := exec.Command("docker", append([]string{"exec", "-u", user, n.Name}, args...)...).CombinedOutput()
	return string(out), err
}

var (
	binOnce sync.Once
	binDir  string
	binErr  error
)

// imagesDir is the directory, in binaries', of the cluster services'
// images, and of fixtureImage and brokenImage.
const imagesDir = "images"

// The image archives of Fixture and Broken, in imagesDir.
const (
	fixtureImage = "fixture.tar"
	brokenImage  = "broken.tar"
)

// binaries returns a directory holding the agent and the updater, built
// from this checkout with Version, the agent and the updater built with
// NextVersion in nextAgent and nextUpdater, and the release's containerd
// binaries and runc for this machine's architecture, with the cluster
// services' images in imagesDir, also built with Version, and Fixture's
// and Broken's.
// Cleanup removes it.
func binaries(t *testing.T) string {
	t.Helper()
	binOnce.Do(func() {
		binDir, binErr = os.MkdirTemp("", "nodetest-bin-")
		if binErr != nil {
			return
		}
		cache, err := os.UserCacheDir()
		if err != nil {
			binErr = err
			return
		}
		if binErr = release.FetchContainerd(context.Background(), runtime.GOARCH, filepath.Join(cache, "moostack-release"), binDir); binErr != nil {
			return
		}
		if binErr = release.FetchRunc(context.Background(), runtime.GOARCH, filepath.Join(cache, "moostack-release"), binDir); binErr != nil {
			return
		}
		if _, binErr = release.BuildServices(context.Background(), checkout(), runtime.GOARCH, Version, filepath.Join(binDir, imagesDir)); binErr != nil {
			return
		}
		binErr = writeFixtureImages(filepath.Join(binDir, imagesDir))
		if binErr != nil {
			return
		}
		for _, b := range []struct{ program, version, out string }{
			{"agent", Version, "agent"},
			{"updater", Version, "updater"},
			{"agent", NextVersion, nextAgent},
			{"updater", NextVersion, nextUpdater},
		} {
			cmd := exec.Command("go", "build", "-ldflags", "-X github.com/moostackhq/moostack/internal/buildinfo.Version="+b.version,
				"-o", filepath.Join(binDir, b.out), "github.com/moostackhq/moostack/cmd/"+b.program)
			cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
			if out, err := cmd.CombinedOutput(); err != nil {
				binErr = fmt.Errorf("building the %s at %s: %v\n%s", b.program, b.version, err, out)
				return
			}
		}
	})
	if binErr != nil {
		t.Fatal(binErr)
	}
	return binDir
}

// fixtureProgram is fixtureservice's binary, in imagesDir.
const fixtureProgram = "fixtureservice"

// fixtureImageOf is an image of fixtureservice, program, that serves
// svc's API, named as svc's image.
func fixtureImageOf(program []byte, svc *nodev1.Service) ociimage.Image {
	return ociimage.Image{Name: svc.Image, Architecture: runtime.GOARCH, Entrypoint: []string{"/fixtureservice", svc.Api},
		Files: []ociimage.File{{Name: "fixtureservice", Mode: 0o755, Data: program}}}
}

// writeFixtureImages builds fixtureservice into dir, as fixtureProgram,
// and writes Fixture's image of it there, and Broken's, which has no
// program.
func writeFixtureImages(dir string) error {
	program := filepath.Join(dir, fixtureProgram)
	cmd := exec.Command("go", "build", "-o", program, "github.com/moostackhq/moostack/internal/nodetest/fixtureservice")
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("building fixtureservice: %v\n%s", err, out)
	}
	data, err := os.ReadFile(program)
	if err != nil {
		return err
	}
	images := []struct {
		file  string
		image ociimage.Image
	}{
		{fixtureImage, fixtureImageOf(data, Fixture())},
		{brokenImage, ociimage.Image{Name: Broken().Image, Architecture: runtime.GOARCH, Entrypoint: []string{"/missing"}}},
	}
	for _, img := range images {
		if err := ociimage.WriteFile(filepath.Join(dir, img.file), img.image); err != nil {
			return err
		}
	}
	return nil
}

// checkout is the root of this checkout.
func checkout() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "..", "..")
}

// Cleanup removes what the package's tests left behind. Call it from
// TestMain after the tests have run.
func Cleanup() {
	if binDir != "" {
		os.RemoveAll(binDir)
	}
}

// pipeConn is a connection over a command's stdin and stdout.
type pipeConn struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout io.ReadCloser
	socket string
}

func (c *pipeConn) Read(p []byte) (int, error)  { return c.stdout.Read(p) }
func (c *pipeConn) Write(p []byte) (int, error) { return c.stdin.Write(p) }
func (c *pipeConn) Close() error {
	c.stdin.Close()
	c.cmd.Process.Kill()
	return c.cmd.Wait()
}
func (c *pipeConn) LocalAddr() net.Addr              { return &net.UnixAddr{Net: "unix"} }
func (c *pipeConn) RemoteAddr() net.Addr             { return &net.UnixAddr{Net: "unix", Name: c.socket} }
func (c *pipeConn) SetDeadline(time.Time) error      { return nil }
func (c *pipeConn) SetReadDeadline(time.Time) error  { return nil }
func (c *pipeConn) SetWriteDeadline(time.Time) error { return nil }

// stderr is what the command that failed with err wrote to stderr.
func stderr(err error) []byte {
	if exit, ok := err.(*exec.ExitError); ok {
		return exit.Stderr
	}
	return nil
}

// Eventually calls f until it succeeds, failing the test with its last
// error after timeout.
func Eventually(t *testing.T, timeout time.Duration, f func() error) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		err := f()
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// writeFile writes data to path, replacing it through a rename, so that
// a program never reads it half written.
func writeFile(t *testing.T, path string, data []byte) {
	t.Helper()
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatal(err)
	}
}
