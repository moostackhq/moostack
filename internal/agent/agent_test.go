//go:build linux

package agent_test

import (
	"context"
	"fmt"
	"io"
	"net"
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
	"github.com/moostackhq/moostack/internal/release"
)

// These tests run the agent and containerd in a privileged container, as
// a node runs them, and call the node API through the agent's relay.

// testVersion is the version the agent under test is built with.
const testVersion = "9.8.7-test"

func TestStatusReportsTheNodesModeAndComponents(t *testing.T) {
	n := startNode(t)
	status := n.eventuallyHealthy(t)
	if status.Mode != nodev1.Mode_MODE_DIRECT {
		t.Errorf("mode is %s, want direct", status.Mode)
	}
	got := map[string]string{}
	for _, c := range status.Components {
		got[c.Name] = c.Version + " " + c.Health.String()
	}
	want := map[string]string{
		"agent":      testVersion + " HEALTH_HEALTHY",
		"containerd": release.ContainerdVersion + " HEALTH_HEALTHY",
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("components are %v, want %v", got, want)
	}
}

func TestStatusReportsAStoppedContainerdUnhealthy(t *testing.T) {
	n := startNode(t)
	n.eventuallyHealthy(t)
	n.exec(t, "root", "sh", "-c", "kill $(cat /run/containerd.pid)")
	var containerd *nodev1.Component
	eventually(t, 30*time.Second, func() error {
		status, err := n.status(t)
		if err != nil {
			return err
		}
		for _, c := range status.Components {
			if c.Name == "containerd" {
				containerd = c
			}
			if c.Name == "agent" && c.Health != nodev1.Health_HEALTH_HEALTHY {
				t.Errorf("the agent is %s with containerd stopped", c.Health)
			}
		}
		if containerd == nil || containerd.Health != nodev1.Health_HEALTH_UNHEALTHY {
			return fmt.Errorf("containerd is %v", containerd)
		}
		return nil
	})
	if containerd.Problem == "" {
		t.Error("an unhealthy containerd has no problem given")
	}
}

func TestContainerdKeepsToMoostacksDirectories(t *testing.T) {
	n := startNode(t)
	n.eventuallyHealthy(t)
	for _, dir := range []string{"/run/containerd", "/var/lib/containerd", "/opt/containerd", "/etc/containerd", "/var/run/nri"} {
		if out, err := exec.Command("docker", "exec", n.name, "test", "-e", dir).CombinedOutput(); err == nil {
			t.Errorf("containerd made %s", dir)
		} else if _, ok := err.(*exec.ExitError); !ok {
			t.Fatalf("checking for %s: %v\n%s", dir, err, out)
		}
	}
}

func TestOnlyRootCanConnectToTheSocket(t *testing.T) {
	n := startNode(t)
	n.eventuallyHealthy(t)
	cmd := exec.Command("docker", "exec", "-u", "65534", n.name, hostlayout.Agent, "relay", hostlayout.AgentSocket)
	out, err := cmd.CombinedOutput()
	if err == nil || !strings.Contains(string(out), "permission denied") {
		t.Errorf("a relay as nobody: %v, %s; want permission denied", err, out)
	}
	// The socket is root's alone, not only its directory.
	mode, err := exec.Command("docker", "exec", n.name, "stat", "-c", "%a %U", hostlayout.AgentSocket).Output()
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(string(mode)); got != "600 root" {
		t.Errorf("the socket is %s, want 600 root", got)
	}
}

// node is a container running the agent and containerd.
type node struct {
	name string
	conn *grpc.ClientConn
}

// startNode starts a node, and removes it when the test ends.
func startNode(t *testing.T) *node {
	t.Helper()
	if exec.Command("docker", "info").Run() != nil {
		t.Skip("docker doesn't work")
	}
	t.Parallel()
	bin := nodeBinaries(t)
	config := t.TempDir()
	nodeFile, err := hostlayout.NodeConfig{Cluster: "test", Node: "node-1", Mode: hostlayout.ModeDirect}.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(config, filepath.Base(hostlayout.NodeFile)), nodeFile)
	writeFile(t, filepath.Join(config, filepath.Base(hostlayout.ContainerdConfigFile)), []byte(hostlayout.ContainerdConfig))
	script := hostlayout.BinDir + "/containerd --config " + hostlayout.ContainerdConfigFile + " >/var/log/containerd.log 2>&1 & " +
		"echo $! >/run/containerd.pid; exec " + hostlayout.Agent + " serve"
	out, err := exec.Command("docker", "run", "-d", "--rm", "--privileged",
		"-v", bin+":"+hostlayout.BinDir+":ro",
		"-v", config+":"+hostlayout.ConfigDir+":ro",
		"debian:13-slim", "sh", "-c", script).Output()
	if err != nil {
		t.Fatalf("starting the node: %v\n%s", err, stderr(err))
	}
	n := &node{name: strings.TrimSpace(string(out))}
	t.Cleanup(func() {
		if t.Failed() {
			logs, _ := exec.Command("docker", "logs", n.name).CombinedOutput()
			containerd, _ := exec.Command("docker", "exec", n.name, "cat", "/var/log/containerd.log").CombinedOutput()
			t.Logf("agent's log:\n%s\ncontainerd's log:\n%s", logs, containerd)
		}
		exec.Command("docker", "rm", "-f", n.name).Run()
	})
	n.conn, err = grpc.NewClient("passthrough:///agent",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return n.relay() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { n.conn.Close() })
	return n
}

// relay connects to the agent's socket through the agent's relay, run as
// root in the container.
func (n *node) relay() (net.Conn, error) {
	cmd := exec.Command("docker", "exec", "-i", n.name, hostlayout.Agent, "relay", hostlayout.AgentSocket)
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
	return &pipeConn{cmd: cmd, stdin: stdin, stdout: stdout}, nil
}

func (n *node) status(t *testing.T) (*nodev1.StatusResponse, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	return nodev1.NewAgentClient(n.conn).Status(ctx, &nodev1.StatusRequest{})
}

// eventuallyHealthy waits for the node to report every component healthy,
// and returns that status.
func (n *node) eventuallyHealthy(t *testing.T) *nodev1.StatusResponse {
	t.Helper()
	var status *nodev1.StatusResponse
	eventually(t, 60*time.Second, func() error {
		var err error
		if status, err = n.status(t); err != nil {
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

// exec runs args in the node as user, failing the test if it fails.
func (n *node) exec(t *testing.T, user string, args ...string) {
	t.Helper()
	out, err := exec.Command("docker", append([]string{"exec", "-u", user, n.name}, args...)...).CombinedOutput()
	if err != nil {
		t.Fatalf("%s: %v\n%s", strings.Join(args, " "), err, out)
	}
}

var (
	binOnce sync.Once
	binDir  string
	binErr  error
)

// nodeBinaries returns a directory holding the agent, built from this
// checkout with testVersion, and the release's containerd binaries for
// this machine's architecture.
func nodeBinaries(t *testing.T) string {
	t.Helper()
	binOnce.Do(func() {
		binDir, binErr = os.MkdirTemp("", "agent-test-bin-")
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
		cmd := exec.Command("go", "build", "-ldflags", "-X github.com/moostackhq/moostack/internal/buildinfo.Version="+testVersion,
			"-o", filepath.Join(binDir, "agent"), "github.com/moostackhq/moostack/cmd/agent")
		cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
		if out, err := cmd.CombinedOutput(); err != nil {
			binErr = fmt.Errorf("building the agent: %v\n%s", err, out)
		}
	})
	if binErr != nil {
		t.Fatal(binErr)
	}
	return binDir
}

func TestMain(m *testing.M) {
	code := m.Run()
	if binDir != "" {
		os.RemoveAll(binDir)
	}
	os.Exit(code)
}

// pipeConn is a connection over a command's stdin and stdout.
type pipeConn struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout io.ReadCloser
}

func (c *pipeConn) Read(p []byte) (int, error)  { return c.stdout.Read(p) }
func (c *pipeConn) Write(p []byte) (int, error) { return c.stdin.Write(p) }
func (c *pipeConn) Close() error {
	c.stdin.Close()
	c.cmd.Process.Kill()
	return c.cmd.Wait()
}
func (c *pipeConn) LocalAddr() net.Addr { return &net.UnixAddr{Net: "unix"} }
func (c *pipeConn) RemoteAddr() net.Addr {
	return &net.UnixAddr{Net: "unix", Name: hostlayout.AgentSocket}
}
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

// eventually calls f until it succeeds, failing the test with its last
// error after timeout.
func eventually(t *testing.T, timeout time.Duration, f func() error) {
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

func writeFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}
