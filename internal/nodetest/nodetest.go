//go:build linux

// Package nodetest runs a node's components in a privileged container, as
// a node runs them, for tests that call the node API through the
// components' relays. It needs docker; tests that use it are skipped
// without it.
package nodetest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/leases"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/durationpb"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/controlplane"
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
// gone when it is a zombie, and no thread of it runs on, holding what it
// had open, such as the port the program listens on.
func stopScript(name, signal string) string {
	return "pid=$(cat /run/" + name + ".pid)\n" +
		"kill -" + signal + " $pid 2>/dev/null\n" +
		"while [ -e /proc/$pid ] && { [ \"$(cut -d' ' -f3 /proc/$pid/stat)\" != Z ] || [ \"$(ls /proc/$pid/task 2>/dev/null | wc -l)\" -gt 1 ]; }; do sleep 0.1; done\n"
}

// Start starts a node, and removes it when the test ends. It runs the
// test in parallel with others. The node's identity is issued as node
// add issues it: the agent makes the key on the node, and the CA signs
// its request.
func Start(t *testing.T) *Node {
	t.Helper()
	return start(t, nil)
}

// StartJoined starts a node joined to peer, as node add --join-token
// joins it: agent join gets the node's identity from peer, and the node
// starts in control plane mode. Agent and Updater call the node's local
// sockets, and peer calls the node API over its tunnels.
func StartJoined(t *testing.T, peer *TunnelPeer) *Node {
	t.Helper()
	return start(t, peer)
}

// start starts a node, joined to peer unless it is nil.
func start(t *testing.T, peer *TunnelPeer) *Node {
	t.Helper()
	if exec.Command("docker", "info").Run() != nil {
		t.Skip("docker doesn't work")
	}
	t.Parallel()
	bin := binaries(t)
	n := &Node{NodeName: "node-1", config: t.TempDir()}
	// The state directory is a volume of its own, since containerd's
	// snapshots can't be overlays on the container's overlay. The agent
	// of a joined node rewrites the node file when it detaches, and
	// reaches the peer by TunnelHost.
	config := n.config + ":" + hostlayout.ConfigDir + ":ro"
	if peer != nil {
		config = n.config + ":" + hostlayout.ConfigDir
	}
	args := []string{"run", "-d", "--rm", "--privileged",
		"-v", bin + ":" + builtDir + ":ro",
		"-v", config,
		"-v", hostlayout.StateDir}
	if peer != nil {
		args = append(args, "--add-host", TunnelHost+":host-gateway")
	}
	out, err := exec.Command("docker", append(args, "debian:13-slim", "sleep", "infinity")...).Output()
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
				log, _ := exec.Command("docker", "exec", n.Name, "cat", hostlayout.ContainerLogDir+"/"+release.ContainerName(s.Name)+".log").CombinedOutput()
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

	n.WriteConfig(t, hostlayout.ContainerdConfigFile, []byte(hostlayout.ContainerdConfig))
	n.Exec(t, "root", "sh", "-c", "set -e\n"+
		"mkdir -p "+hostlayout.BinDir+"\n"+
		"find "+builtDir+" -maxdepth 1 -type f -exec cp {} "+hostlayout.BinDir+" \\;\n"+
		"mv "+hostlayout.Agent+" "+hostlayout.AgentSlots[0]+"\n"+
		"ln -s "+filepath.Base(hostlayout.AgentSlots[0])+" "+hostlayout.Agent+"\n"+
		"mv "+hostlayout.Updater+" "+hostlayout.UpdaterSlots[0]+"\n"+
		"ln -s "+filepath.Base(hostlayout.UpdaterSlots[0])+" "+hostlayout.Updater+"\n"+
		"cat >/usr/local/bin/systemctl <<'EOF'\n"+systemctl()+"EOF\n"+
		"chmod 755 /usr/local/bin/systemctl\n")
	node := hostlayout.NodeConfig{Cluster: "test", Node: n.NodeName, Mode: hostlayout.ModeDirect, PrivateAddress: private}
	if peer == nil {
		if n.CA, err = identity.NewCA("test"); err != nil {
			t.Fatal(err)
		}
		request, err := exec.Command("docker", "exec", n.Name, hostlayout.Agent, "request-identity").Output()
		if err != nil {
			t.Fatalf("making the node's key: %v\n%s", err, stderr(err))
		}
		n.request = request
		n.WriteConfig(t, hostlayout.Identity.Cert, n.Issue(t, n.CA, request))
		n.WriteConfig(t, hostlayout.Identity.CA, n.CA.CertPEM())
	} else {
		cmd := exec.Command("docker", "exec", "-i", n.Name, hostlayout.Agent, "join")
		cmd.Stdin = strings.NewReader(peer.Token(t) + "\n")
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("joining the tunnel peer: %v\n%s", err, stderr(err))
		}
		var joined controlplane.Joined
		if err := json.Unmarshal(out, &joined); err != nil {
			t.Fatalf("agent join printed %q: %v", out, err)
		}
		n.CA, n.NodeName = peer.CA, joined.Node
		node.Node, node.Cluster = joined.Node, joined.Cluster
		node.Mode, node.ControlPlane = hostlayout.ModeControlPlane, peer.Address
		n.WriteConfig(t, hostlayout.Identity.Cert, joined.Certificate)
		n.WriteConfig(t, hostlayout.Identity.CA, joined.CA)
	}
	nodeFile, err := node.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	n.WriteConfig(t, hostlayout.NodeFile, nodeFile)

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
// root in the container, with opts.
func (n *Node) dialThrough(t *testing.T, program, socket string, opts ...grpc.DialOption) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient("passthrough:///"+filepath.Base(program), append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return n.relay(program, socket) }),
	}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

// ContainerdConn is a connection to the node's containerd, with opts.
func (n *Node) ContainerdConn(t *testing.T, opts ...grpc.DialOption) *grpc.ClientConn {
	t.Helper()
	return n.dialThrough(t, hostlayout.Agent, hostlayout.ContainerdSocket, opts...)
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
// healthy, and no container's API unhealthy, and returns that status.
func (n *Node) EventuallyHealthy(t *testing.T) *nodev1.StatusResponse {
	t.Helper()
	var status *nodev1.StatusResponse
	Eventually(t, 60*time.Second, func() error {
		var err error
		if status, err = n.Status(t); err != nil {
			return err
		}
		for _, c := range status.Components {
			if c.Health != nodev1.Health_HEALTH_HEALTHY {
				return fmt.Errorf("%s is %s: %s", c.Name, c.Health, c.Problem)
			}
		}
		for _, c := range status.Containers {
			if c.Health == nodev1.Health_HEALTH_UNHEALTHY {
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
	ctx := namespaces.WithNamespace(t.Context(), hostlayout.ContainerdNamespace)
	if err := n.Containerd(t).ImageService().Delete(ctx, name); err != nil {
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

// Image is an image of fixtureservice, the test program, as a cluster
// service that serves an API, or as an app, which serves none.
type Image struct {
	// Name is the image's, such as docker.io/library/app:1.
	Name string
	// API is the API it serves, as the dns service: every caller kind may
	// call each method, Ping too. An app's is empty.
	API string
	// User is the user the image runs as, root if empty.
	User string
	// Group is the image's /etc/group, none if empty.
	Group string
	// Exit has it exit at once with that code, unless it is nil.
	Exit *int
	// Rebuild tells builds of the image under the same name apart: each,
	// from 1, has a file more.
	Rebuild int
	// Reconfigure tells builds of the image under the same name apart
	// that differ in their config alone: each, from 1, runs the same
	// program by another path, from the layer of the image without it.
	Reconfigure int
	// HealthDelay is how long its API takes to answer Health calls.
	HealthDelay time.Duration
	// Slow adds a large file, so that unpacking the image takes seconds.
	Slow bool
}

// ImageOf is the image of fixtureservice that serves c's API, named as
// c's image.
func ImageOf(c *nodev1.Container) Image {
	return Image{Name: c.Image, API: c.Api}
}

// Import has the agent import img.
func (n *Node) Import(t *testing.T, img Image) {
	t.Helper()
	n.importArchive(t, img.Name, img.Archive(t), img.Name)
}

// Archive is img's OCI image layout archive, as Import imports it.
func (img Image) Archive(t *testing.T) []byte {
	t.Helper()
	program, err := os.ReadFile(filepath.Join(binaries(t), imagesDir, fixtureProgram))
	if err != nil {
		t.Fatal(err)
	}
	var archive bytes.Buffer
	if err := ociimage.Write(&archive, img.of(program)); err != nil {
		t.Fatal(err)
	}
	return archive.Bytes()
}

// of is img, of fixtureservice's binary program.
func (img Image) of(program []byte) ociimage.Image {
	command := []string{"/" + strings.Repeat("./", img.Reconfigure) + fixtureProgram}
	if img.Exit != nil {
		command = append(command, "-exit", strconv.Itoa(*img.Exit))
	}
	if img.API != "" {
		command = append(command, "-api", img.API)
	}
	if img.HealthDelay != 0 {
		command = append(command, "-health-delay", img.HealthDelay.String())
	}
	files := []ociimage.File{{Name: fixtureProgram, Mode: 0o755, Data: program}}
	if img.Rebuild != 0 {
		files = append(files, ociimage.File{Name: "build", Mode: 0o644, Data: []byte(strconv.Itoa(img.Rebuild))})
	}
	if img.Slow {
		files = append(files, ociimage.File{Name: "large", Mode: 0o644, Data: make([]byte, 1<<30)})
	}
	if img.Group != "" {
		files = append(files, ociimage.File{Name: "etc/group", Mode: 0o644, Data: []byte(img.Group)})
	}
	return ociimage.Image{Name: img.Name, Architecture: runtime.GOARCH, Entrypoint: command, User: img.User, Files: files}
}

// Containerd is a client of the node's containerd.
func (n *Node) Containerd(t *testing.T) *containerd.Client {
	t.Helper()
	client, err := containerd.NewWithConn(n.ContainerdConn(t))
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// CollectGarbageOnce has the node's containerd collect garbage, and
// returns once it has.
func (n *Node) CollectGarbageOnce(t *testing.T) {
	t.Helper()
	if err := collectGarbage(namespaces.WithNamespace(t.Context(), hostlayout.ContainerdNamespace), n.Containerd(t)); err != nil {
		t.Fatal(err)
	}
}

// CollectGarbage has the node's containerd collect garbage, over and
// over, until the returned stop is first called, which fails the test if
// a collection failed.
func (n *Node) CollectGarbage(t *testing.T) (stop func()) {
	t.Helper()
	client := n.Containerd(t)
	ctx, cancel := context.WithCancel(namespaces.WithNamespace(t.Context(), hostlayout.ContainerdNamespace))
	done := make(chan error, 1)
	go func() {
		for ctx.Err() == nil {
			if err := collectGarbage(ctx, client); err != nil && ctx.Err() == nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	var once sync.Once
	return func() {
		t.Helper()
		once.Do(func() {
			cancel()
			if err := <-done; err != nil {
				t.Error(err)
			}
		})
	}
}

// collectGarbage has client's containerd collect garbage, and returns
// once it has: deleting a lease synchronously collects before it returns.
func collectGarbage(ctx context.Context, client *containerd.Client) error {
	l, err := client.LeasesService().Create(ctx, leases.WithRandomID())
	if err == nil {
		err = client.LeasesService().Delete(ctx, l, leases.SynchronousDelete)
	}
	if err != nil {
		return fmt.Errorf("collecting garbage: %v", err)
	}
	return nil
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
	// The import holds the lock for Lease, as a run does, and lets go of
	// it after, unless Lease held it already.
	if !n.holdsLock(t) {
		n.HoldLock(t)
		defer n.releaseLock(t)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	images, err := nodeclient.ImportImage(ctx, n.Agent, data, Lease)
	if err != nil {
		t.Fatalf("importing %s: %v", what, err)
	}
	if !slices.Contains(images, want) {
		t.Fatalf("importing %s imported %v, want %s", what, images, want)
	}
}

// Fixture is the dns service as the test program fixtureservice serves
// it, as Image says.
func Fixture() *nodev1.Container {
	dns := clusterService("dns")
	return release.Service{Name: dns.Name, Version: "fixture", API: dns.API, Properties: dns.Properties}.Container()
}

// Broken is a service whose image has no program, so that it never
// starts.
func Broken() *nodev1.Container {
	return release.Service{Name: "broken", Version: Version, API: "moostack.broken.v1.Broken", Properties: clusterService("dns").Properties}.Container()
}

// Lease is the lease that RunService runs a cluster service for, and
// that the imports hold the operation lock for while they import.
const Lease = "nodetest"

// HoldLock has the node's operation lock held for Lease, for an hour.
func (n *Node) HoldLock(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	res, err := n.Agent.AcquireLock(ctx, &nodev1.AcquireLockRequest{Lock: &nodev1.Lock{Lease: Lease, Holder: "nodetest"}, Ttl: durationpb.New(time.Hour)})
	if err != nil {
		t.Fatalf("taking the operation lock: %v", err)
	}
	if res.Lock.GetLease() != Lease {
		t.Fatalf("the operation lock is held for %s", res.Lock.GetLease())
	}
}

// holdsLock reports whether Lease holds the node's operation lock.
func (n *Node) holdsLock(t *testing.T) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	res, err := n.Agent.Status(ctx, &nodev1.StatusRequest{})
	if err != nil {
		t.Fatalf("asking for the operation lock: %v", err)
	}
	return res.Lock.GetLease() == Lease
}

// releaseLock has the node's operation lock released, if Lease holds it.
func (n *Node) releaseLock(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 5*time.Second)
	defer cancel()
	if _, err := n.Agent.ReleaseLock(ctx, &nodev1.ReleaseLockRequest{Lease: Lease}); err != nil {
		t.Fatalf("releasing the operation lock: %v", err)
	}
}

// RunService has the agent run the cluster service called name, as the
// release pins it, within a minute, holding the lock for Lease.
func (n *Node) RunService(t *testing.T, name string) {
	t.Helper()
	n.HoldLock(t)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	if _, err := n.Agent.Run(ctx, &nodev1.RunRequest{Container: Service(name), Lease: Lease}); err != nil {
		t.Fatalf("running %s: %v", name, err)
	}
}

// Services are the cluster services among the containers that status
// reports, as nodeclient says them.
func Services(status *nodev1.StatusResponse) []*nodev1.Component {
	return nodeclient.Status{Containers: status.Containers}.Services()
}

// Service is the container of the cluster service called name, as the
// release under test pins it.
func Service(name string) *nodev1.Container {
	s := clusterService(name)
	return release.Service{Name: s.Name, Version: Version, API: s.API, Properties: s.Properties}.Container()
}

// clusterService is the cluster service called name, of
// release.ClusterServices.
func clusterService(name string) release.ClusterService {
	for _, s := range release.ClusterServices {
		if s.Name == name {
			return s
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

// UpdaterConn is a connection to the updater's socket, through its
// relay.
func (n *Node) UpdaterConn(t *testing.T) *grpc.ClientConn {
	t.Helper()
	return n.dial(t, hostlayout.UpdaterSocket)
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
		if _, binErr = release.BuildServices(context.Background(), checkout(), runtime.GOARCH, Version, filepath.Join(binDir, imagesDir), release.ClusterServices); binErr != nil {
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
		{fixtureImage, ImageOf(Fixture()).of(data)},
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
