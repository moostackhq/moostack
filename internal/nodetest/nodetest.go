//go:build linux

// Package nodetest runs a node's components in a privileged container, as
// a node runs them, for tests that call the node API through the
// components' relays. It needs docker; tests that use it are skipped
// without it.
package nodetest

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/hostlayout"
	"github.com/moostackhq/moostack/internal/identity"
	"github.com/moostackhq/moostack/internal/release"
)

// Version is the version the agent and the updater under test are built
// with.
const Version = "9.8.7-test"

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
// its pid in /run/<name>.pid and its output in /var/log/<name>.log.
var programs = []struct{ name, command string }{
	{"containerd", hostlayout.BinDir + "/containerd --config " + hostlayout.ContainerdConfigFile},
	{"updater", hostlayout.Updater + " serve"},
	{"agent", hostlayout.Agent + " serve"},
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
	out, err := exec.Command("docker", "run", "-d", "--rm", "--privileged",
		"-v", bin+":"+hostlayout.BinDir+":ro",
		"-v", n.config+":"+hostlayout.ConfigDir+":ro",
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
		}
		exec.Command("docker", "rm", "-f", n.Name).Run()
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
		for _, c := range status.Components {
			if c.Health != nodev1.Health_HEALTH_HEALTHY {
				return fmt.Errorf("%s is %s: %s", c.Name, c.Health, c.Problem)
			}
		}
		return nil
	})
	return status
}

// Kill kills one of the node's programs: containerd, updater or agent.
func (n *Node) Kill(t *testing.T, program string) {
	t.Helper()
	n.Exec(t, "root", "sh", "-c", "kill $(cat /run/"+program+".pid)")
}

// Restart stops one of the node's programs, waits for it to exit, and
// starts it again.
func (n *Node) Restart(t *testing.T, program string) {
	t.Helper()
	for _, p := range programs {
		if p.name == program {
			// Nothing reaps the program once it exits, so it is gone
			// when it is a zombie.
			n.Exec(t, "root", "sh", "-c", "pid=$(cat /run/"+program+".pid); kill $pid\n"+
				"while [ -e /proc/$pid ] && [ \"$(cut -d' ' -f3 /proc/$pid/stat)\" != Z ]; do sleep 0.1; done\n"+
				startScript(p.name, p.command))
			return
		}
	}
	t.Fatalf("the node has no program %s", program)
}

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

// binaries returns a directory holding the agent and the updater, built
// from this checkout with Version, and the release's containerd binaries
// for this machine's architecture. Cleanup removes it.
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
		for _, program := range []string{"agent", "updater"} {
			cmd := exec.Command("go", "build", "-ldflags", "-X github.com/moostackhq/moostack/internal/buildinfo.Version="+Version,
				"-o", filepath.Join(binDir, program), "github.com/moostackhq/moostack/cmd/"+program)
			cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
			if out, err := cmd.CombinedOutput(); err != nil {
				binErr = fmt.Errorf("building the %s: %v\n%s", program, err, out)
				return
			}
		}
	})
	if binErr != nil {
		t.Fatal(binErr)
	}
	return binDir
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
