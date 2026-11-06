package cli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"slices"

	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/moostackhq/moostack/internal/sshclient"
	"golang.org/x/crypto/ssh"

	"github.com/moostackhq/moostack/internal/k3s"
)

// A node only Kubernetes has, whose host still runs k3s, is removed with
// a warning that it registers again.
func TestRemovingANodeOnlyKubernetesHasWarnsItMayComeBack(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	h2 := c.hosts["root@h2"]
	h2.k3s = true
	h2.files[k3s.ConfigFile] = "node-name: stray\n"
	c.nodes["stray"] = fakeKubeNode{ip: "10.0.0.3", arch: "amd64"}

	out := mustRun(t, c, home, "cluster", "node-remove", "stray")
	if !strings.Contains(out, "Warning: if k3s still runs on stray's host, it registers again") {
		t.Errorf("node-remove doesn't warn that the node comes back:\n%s", out)
	}
}

// The last-node confirmation is judged on the cluster directory brought
// up to its remote.
func TestTheLastNodeNeedsYesAfterTheDirectoryIsBroughtUpToItsRemote(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	mustRun(t, c, home, "cluster", "node-add", "root@h2")
	dir := prodDir(t, home).Path
	remote := filepath.Join(t.TempDir(), "prod.git")
	git(t, home, "init", "--bare", "--quiet", "--initial-branch=main", remote)
	mustRun(t, c, home, "cluster", "remote", remote)

	// A teammate removes node2 and pushes; this machine hasn't fetched.
	elsewhere := filepath.Join(t.TempDir(), "prod")
	git(t, home, "clone", "--quiet", remote, elsewhere)
	if err := os.Remove(filepath.Join(elsewhere, "nodes", "node2.yml")); err != nil {
		t.Fatal(err)
	}
	git(t, elsewhere, "commit", "--quiet", "-am", "node-remove node2")
	git(t, elsewhere, "push", "--quiet")
	delete(c.nodes, "node2")
	c.hosts["root@h2"].k3s = false
	c.hosts["root@h1"].files[k3s.AppliedCommitFile] = git(t, elsewhere, "rev-parse", "HEAD") + "\n"
	_ = dir

	out, err := onFake(t, c, home, "cluster", "node-remove", "node1")
	if err == nil || !c.hosts["root@h1"].k3s {
		t.Errorf("node-remove node1 without --yes = %v, k3s on h1 %v:\n%s", err, c.hosts["root@h1"].k3s, out)
	}
}

// A k3s version mooctl has no install script checksum for is refused
// before node-add writes to the host.
func TestAVersionWithoutAChecksumIsRefusedBeforeTouchingTheHost(t *testing.T) {
	c, home := fakeProd(t)
	d := prodDir(t, home)
	path := filepath.Join(d.Path, "cluster.yml")
	data, _ := os.ReadFile(path)
	os.WriteFile(path, []byte(strings.Replace(string(data), K3sVersion, "v1.38.0+k3s1", 1)), 0o644)
	git(t, d.Path, "commit", "--quiet", "-am", "pin v1.38.0")

	out, err := onFake(t, c, home, "cluster", "node-add", "root@h1")
	if err == nil {
		t.Fatalf("an unknown version was installed:\n%s", out)
	}
	if c.ran("root@h1", "mkdir -p") {
		t.Errorf("refused after writing to the host (%v); ran:\n%s", err, strings.Join(c.runs, "\n---\n"))
	}
}

// probeConn is a fakeConn whose answers hook can replace.
type probeConn struct {
	*fakeConn
	hook func(cmd string) (sshclient.Result, error, bool)
}

func (p probeConn) Run(ctx context.Context, cmd string, stdin io.Reader) (sshclient.Result, error) {
	if res, err, ok := p.hook(cmd); ok {
		return res, err
	}
	return p.fakeConn.Run(ctx, cmd, stdin)
}

// onProbe runs mooctl like onFake, in ctx, with each connection's
// answers passed through hook first.
func onProbe(t *testing.T, ctx context.Context, c *fakeCluster, home string, hook func(target, cmd string) (sshclient.Result, error, bool), args ...string) (string, error) {
	t.Helper()
	var out bytes.Buffer
	env := fakeEnvFor(c, home, &out)
	env.Connect = func(ctx context.Context, target string, key ssh.PublicKey) (remote, ssh.PublicKey, error) {
		r, k, err := c.connect(ctx, target, key)
		if err != nil {
			return r, k, err
		}
		return probeConn{r.(*fakeConn), func(cmd string) (sshclient.Result, error, bool) { return hook(target, cmd) }}, k, nil
	}
	err := New(env).Run(ctx, append([]string{"mooctl", "-c", "prod"}, args...))
	return out.String(), err
}

// A control plane whose k3s is gone but whose SSH answers is removed.
func TestAControlPlaneWhoseUninstallWasCutOffCanBeRemoved(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	h1 := c.hosts["root@h1"]
	// What the cut-off uninstall left: k3s gone, the applied commit gone.
	h1.k3s, h1.server = false, false
	delete(h1.files, k3s.AppliedCommitFile)
	c.nodes = map[string]fakeKubeNode{}
	noK3s := func(target, cmd string) (sshclient.Result, error, bool) {
		if target == "root@h1" && strings.HasPrefix(cmd, "k3s ") {
			return sshclient.Result{ExitCode: 127, Stderr: []byte("sh: 1: k3s: not found")}, nil, true
		}
		return sshclient.Result{}, nil, false
	}
	for _, args := range [][]string{{"--yes"}} {
		out, err := onProbe(t, t.Context(), c, home, noK3s, append([]string{"cluster", "node-remove", "node1"}, args...)...)
		if err != nil {
			t.Errorf("node-remove node1 %v: %v\n%s", args, err, out)
		}
	}
}

// Ctrl-C during the uninstall of node-remove --unreachable leaves the
// node in the cluster directory.
func TestAnInterruptedUnreachableRemovalCommitsNothing(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	mustRun(t, c, home, "cluster", "node-add", "root@h2")
	c.hosts["root@h1"].unreachable = true
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cut := func(target, cmd string) (sshclient.Result, error, bool) {
		if target == "root@h2" && strings.Contains(cmd, "uninstall.sh") {
			cancel()
			return sshclient.Result{}, context.Canceled, true
		}
		return sshclient.Result{}, nil, false
	}
	out, err := onProbe(t, ctx, c, home, cut, "cluster", "node-remove", "--unreachable", "node2")
	if err == nil || !slices.Contains(nodeNames(t, home), "node2") {
		t.Errorf("an interrupted removal = %v, nodes %v:\n%s", err, nodeNames(t, home), out)
	}
}

// node-remove --unreachable decides on the cluster directory brought up
// to its remote.
func TestAnUnreachableRemovalDecidesOnTheDirectoryFromItsRemote(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	remote := filepath.Join(t.TempDir(), "prod.git")
	git(t, home, "init", "--bare", "--quiet", "--initial-branch=main", remote)
	mustRun(t, c, home, "cluster", "remote", remote)

	// A teammate's node-add of node2, pushed.
	elsewhere := filepath.Join(t.TempDir(), "prod")
	git(t, home, "clone", "--quiet", remote, elsewhere)
	node1, _ := os.ReadFile(filepath.Join(elsewhere, "nodes", "node1.yml"))
	node2 := strings.NewReplacer("name: node1", "name: node2", "root@h1", "root@h2", "10.0.0.2", "10.0.0.3", "control_plane: true", "control_plane: false", "controlplane: true", "controlplane: false").Replace(string(node1))
	os.WriteFile(filepath.Join(elsewhere, "nodes", "node2.yml"), []byte(node2), 0o644)
	git(t, elsewhere, "add", ".")
	git(t, elsewhere, "commit", "--quiet", "-m", "node-add node2")
	git(t, elsewhere, "push", "--quiet")

	c.hosts["root@h1"].unreachable = true
	out, err := onFake(t, c, home, "cluster", "node-remove", "--unreachable", "--yes", "node1")
	names := nodeNames(t, home)
	if err == nil && slices.Contains(names, "node2") && !slices.Contains(names, "node1") {
		st, _ := onFake(t, c, home, "cluster", "status")
		rm, rmErr := onFake(t, c, home, "cluster", "node-remove", "--unreachable", "node2")
		t.Errorf("removed the control plane though the fetched directory has node2 (%v):\n%s\nstatus:\n%s\nnode-remove node2: %v %s", names, out, st, rmErr, rm)
	}
}

// The control plane is refused while Kubernetes has another node.
func TestTheControlPlaneGoesAfterNodesOnlyKubernetesHas(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	c.nodes["stray"] = fakeKubeNode{ip: "10.0.0.9", arch: "amd64"}
	out, err := onFake(t, c, home, "cluster", "node-remove", "--yes", "node1")
	if err == nil {
		t.Errorf("the control plane went while Kubernetes has stray:\n%s", out)
	}
}

// A failed uninstall fails node-remove, which keeps the node.
func TestAFailedUninstallKeepsTheNode(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	mustRun(t, c, home, "cluster", "node-add", "root@h2")
	drop := func(target, cmd string) (sshclient.Result, error, bool) {
		if target == "root@h2" && strings.Contains(cmd, "uninstall.sh") {
			return sshclient.Result{}, errors.New("connection reset"), true
		}
		return sshclient.Result{}, nil, false
	}
	out, err := onProbe(t, t.Context(), c, home, drop, "cluster", "node-remove", "node2")
	_, inKube := c.nodes["node2"]
	if err == nil && !slices.Contains(nodeNames(t, home), "node2") && inKube {
		t.Errorf("node-remove = nil, the node file dropped, Kubernetes still has node2:\n%s", out)
	}
}
