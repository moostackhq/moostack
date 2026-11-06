package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/moostackhq/moostack/internal/clusterdir"
	"github.com/moostackhq/moostack/internal/k3s"
)

// onFake runs mooctl with args on the cluster prod of home, whose hosts
// are c's.
func onFake(t *testing.T, c *fakeCluster, home string, args ...string) (string, error) {
	t.Helper()
	var out bytes.Buffer
	env := fakeEnvFor(c, home, &out)
	err := New(env).Run(t.Context(), append([]string{"mooctl", "-c", "prod"}, args...))
	return out.String(), err
}

// fakeProd is an initialised cluster prod with the hosts h1, h2 and h3
// on 10.0.0.0/24.
func fakeProd(t *testing.T) (*fakeCluster, string) {
	t.Helper()
	home := newHome(t)
	key := sshKey(t, home)
	if out, err := mooctl(t, home, home, "--ssh-key", key, "cluster", "init", "prod"); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	c := newFakeCluster(t)
	c.host("root@h1", "10.0.0.2")
	c.host("root@h2", "10.0.0.3")
	c.host("root@h3", "10.0.0.4")
	return c, home
}

func prodDir(t *testing.T, home string) *clusterdir.Dir {
	t.Helper()
	d, err := clusterdir.Open(filepath.Join(home, "clusters", "default", "prod"))
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func nodeNames(t *testing.T, home string) []string {
	t.Helper()
	nodes, err := prodDir(t, home).Nodes()
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, n := range nodes {
		names = append(names, n.Name)
	}
	return names
}

func mustRun(t *testing.T, c *fakeCluster, home string, args ...string) string {
	t.Helper()
	out, err := onFake(t, c, home, args...)
	if err != nil {
		t.Fatalf("mooctl %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return out
}

func TestNodeAddMakesTheFirstTheControlPlaneAndJoinsTheOthers(t *testing.T) {
	c, home := fakeProd(t)
	out := mustRun(t, c, home, "cluster", "node-add", "root@h1")
	if !strings.Contains(out, "Added node1 to prod") {
		t.Errorf("output:\n%s", out)
	}
	mustRun(t, c, home, "cluster", "node-add", "root@h2")

	d := prodDir(t, home)
	cp, ok, err := d.ControlPlane()
	if err != nil || !ok || cp.Name != "node1" || cp.Target != "root@h1" {
		t.Errorf("control plane = %+v, %v, %v", cp, ok, err)
	}
	if got := strings.Join(nodeNames(t, home), " "); got != "node1 node2" {
		t.Errorf("nodes = %s", got)
	}
	h1, h2 := c.hosts["root@h1"], c.hosts["root@h2"]
	if !h1.server || h2.server || !h2.k3s {
		t.Errorf("h1 server %v, h2 server %v k3s %v", h1.server, h2.server, h2.k3s)
	}
	if !strings.Contains(h1.files[k3s.ConfigFile], "agent-token:") {
		t.Errorf("the control plane has no agent token:\n%s", h1.files[k3s.ConfigFile])
	}
	if cfg := h2.files[k3s.ConfigFile]; !strings.Contains(cfg, "K10agent::node:token") || !strings.Contains(cfg, "https://10.0.0.2:6443") {
		t.Errorf("the agent joins with:\n%s", cfg)
	}
	if c.locked {
		t.Error("the lock is still held")
	}
	if applied := strings.TrimSpace(h1.files[k3s.AppliedCommitFile]); applied != git(t, d.Path, "rev-parse", "HEAD") {
		t.Errorf("applied %q", applied)
	}
	if msg := git(t, d.Path, "log", "-1", "--format=%s"); msg != "node-add node2" {
		t.Errorf("commit %q", msg)
	}
}

func TestNodeAddRefusesBeforeChangingAnything(t *testing.T) {
	for name, tc := range map[string]struct {
		setup func(c *fakeCluster)
		args  []string
		want  string
	}{
		"a taken name": {
			args: []string{"--name", "node1", "root@h2"},
			want: "already",
		},
		"a name Kubernetes has": {
			setup: func(c *fakeCluster) { c.nodes["node7"] = fakeKubeNode{ip: "10.0.0.9", arch: "amd64"} },
			args:  []string{"--name", "node7", "root@h2"},
			want:  "node7",
		},
		"a host with k3s": {
			setup: func(c *fakeCluster) { c.hosts["root@h2"].k3s = true },
			args:  []string{"root@h2"},
			want:  "k3s",
		},
		"a host too small": {
			setup: func(c *fakeCluster) { c.hosts["root@h2"].memMiB = 512 },
			args:  []string{"root@h2"},
			want:  "MiB",
		},
		"a host that doesn't reach the control plane": {
			setup: func(c *fakeCluster) { c.hosts["root@h2"].noReach = true },
			args:  []string{"root@h2"},
			want:  "control plane",
		},
		"a control plane of another k3s": {
			setup: func(c *fakeCluster) { c.hosts["root@h1"].version = "v1.36.0+k3s1" },
			args:  []string{"root@h2"},
			want:  "v1.36.0+k3s1",
		},
		"a public private address": {
			args: []string{"--private-address", "203.0.113.5", "root@h2"},
			want: "private",
		},
	} {
		t.Run(name, func(t *testing.T) {
			c, home := fakeProd(t)
			mustRun(t, c, home, "cluster", "node-add", "root@h1")
			if tc.setup != nil {
				tc.setup(c)
			}
			head := git(t, prodDir(t, home).Path, "rev-parse", "HEAD")
			out, err := onFake(t, c, home, append([]string{"cluster", "node-add"}, tc.args...)...)
			if err == nil || !strings.Contains(err.Error()+out, tc.want) {
				t.Fatalf("err = %v, want %q\n%s", err, tc.want, out)
			}
			if c.ran("root@h2", "INSTALL_K3S_EXEC") {
				t.Error("k3s was installed")
			}
			if got := git(t, prodDir(t, home).Path, "rev-parse", "HEAD"); got != head {
				t.Error("the directory was committed")
			}
			if len(nodeNames(t, home)) != 1 || c.locked {
				t.Errorf("nodes %v, locked %v", nodeNames(t, home), c.locked)
			}
		})
	}
}

func TestAFailedJoinIsUndone(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	c.hosts["root@h2"].failInstall = true
	if out, err := onFake(t, c, home, "cluster", "node-add", "root@h2"); err == nil {
		t.Fatalf("a failed install passed:\n%s", out)
	}
	if !c.ran("root@h2", "uninstall.sh") {
		t.Error("the host wasn't cleaned up")
	}
	if names := nodeNames(t, home); len(names) != 1 {
		t.Errorf("nodes = %v", names)
	}
	if status := git(t, prodDir(t, home).Path, "status", "--porcelain"); status != "" {
		t.Errorf("left in the directory: %s", status)
	}
}

func TestAJoinWhoseK3sFailsToStartIsUndone(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	c.hosts["root@h2"].failStart = true
	out, err := onFake(t, c, home, "cluster", "node-add", "root@h2")
	if err == nil || !strings.Contains(err.Error(), "starting k3s: exit 1: k3s-agent: token is invalid") {
		t.Fatalf("err %v:\n%s", err, out)
	}
	if !c.ran("root@h2", "uninstall.sh") {
		t.Error("the host wasn't cleaned up")
	}
	if names := nodeNames(t, home); len(names) != 1 {
		t.Errorf("nodes = %v", names)
	}
}

func TestNodeRemove(t *testing.T) {
	c, home := fakeProd(t)
	for _, h := range []string{"root@h1", "root@h2", "root@h3"} {
		mustRun(t, c, home, "cluster", "node-add", h)
	}

	if out, err := onFake(t, c, home, "cluster", "node-remove", "node1"); err == nil {
		t.Errorf("the control plane went before the agents:\n%s", out)
	}
	out := mustRun(t, c, home, "cluster", "node-remove", "node2")
	if !strings.Contains(out, "Removed node2 from prod") || !c.ran("root@h1", "drain 'node2'") || !c.ran("root@h2", "uninstall.sh") {
		t.Errorf("output:\n%s", out)
	}
	if _, found := c.nodes["node2"]; found {
		t.Error("Kubernetes still has node2")
	}

	// A node whose host is gone goes with --unreachable only.
	c.hosts["root@h3"].unreachable = true
	if out, err := onFake(t, c, home, "cluster", "node-remove", "node3"); err == nil {
		t.Errorf("an unreachable node went without --unreachable:\n%s", out)
	}
	mustRun(t, c, home, "cluster", "node-remove", "--unreachable", "node3")
	if _, found := c.nodes["node3"]; found {
		t.Error("Kubernetes still has node3")
	}

	// The last node goes with --yes only.
	if out, err := onFake(t, c, home, "cluster", "node-remove", "node1"); err == nil {
		t.Errorf("the last node went without --yes:\n%s", out)
	}
	mustRun(t, c, home, "cluster", "node-remove", "--yes", "node1")
	if names := nodeNames(t, home); len(names) != 0 {
		t.Errorf("nodes = %v", names)
	}
	if c.hosts["root@h1"].k3s {
		t.Error("k3s is still on h1")
	}
}

func TestNodeRemoveOfANodeOnlyKubernetesHas(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	c.nodes["stray"] = fakeKubeNode{ip: "10.0.0.9", arch: "amd64"}
	mustRun(t, c, home, "cluster", "node-remove", "stray")
	if _, found := c.nodes["stray"]; found {
		t.Error("Kubernetes still has the node")
	}
}

func TestNodeRemoveWithTheControlPlaneGone(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	mustRun(t, c, home, "cluster", "node-add", "root@h2")
	c.hosts["root@h1"].unreachable = true

	if out, err := onFake(t, c, home, "cluster", "node-remove", "node2"); err == nil {
		t.Errorf("node2 went without the control plane or --unreachable:\n%s", out)
	}
	if out, err := onFake(t, c, home, "cluster", "node-remove", "--unreachable", "node1"); err == nil {
		t.Errorf("the control plane went before node2:\n%s", out)
	}
	mustRun(t, c, home, "cluster", "node-remove", "--unreachable", "node2")
	if c.hosts["root@h2"].k3s {
		t.Error("k3s is still on h2")
	}
	mustRun(t, c, home, "cluster", "node-remove", "--unreachable", "--yes", "node1")
	if names := nodeNames(t, home); len(names) != 0 {
		t.Errorf("nodes = %v", names)
	}
}

func TestAnOperationRefusesWhileAnotherHoldsTheLock(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	c.locked = true
	if out, err := onFake(t, c, home, "cluster", "node-add", "root@h2"); err == nil || !strings.Contains(err.Error(), "another mooctl operation") {
		t.Errorf("node-add = %v\n%s", err, out)
	}
	if c.ran("root@h2", "INSTALL_K3S_EXEC") {
		t.Error("node-add went on")
	}
	if out, err := onFake(t, c, home, "cluster", "node-remove", "node1", "--yes"); err == nil {
		t.Errorf("node-remove went on:\n%s", out)
	}
	if !c.hosts["root@h1"].k3s {
		t.Error("node1 was removed")
	}
}

func TestNodeRemoveFinishesAPartialRemoval(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	mustRun(t, c, home, "cluster", "node-add", "root@h2")
	// A run cut off after Kubernetes deleted the node.
	delete(c.nodes, "node2")
	mustRun(t, c, home, "cluster", "node-remove", "node2")
	if c.hosts["root@h2"].k3s {
		t.Error("k3s is still on h2")
	}
	if names := nodeNames(t, home); len(names) != 1 {
		t.Errorf("nodes = %v", names)
	}
}

func TestNodeAddOfARebuiltNodeNamesTheNodeAndItsCluster(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	c.host("root@h1", "10.0.0.1")
	_, err := onFake(t, c, home, "cluster", "node-add", "root@h1")
	if err == nil || !strings.Contains(err.Error(), "root@h1 is node node1 of prod already") {
		t.Errorf("node-add = %v", err)
	}
}

func TestNodeAddPointsAtUnreachableWhenTheControlPlaneIsGone(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	c.hosts["root@h1"].unreachable = true
	if _, err := onFake(t, c, home, "cluster", "node-add", "root@h2"); err == nil || !strings.Contains(err.Error(), "node-remove --unreachable node1") {
		t.Errorf("node-add = %v", err)
	}
}

func TestNodeAddMakesThePublicAddressTheNodesExternalOne(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	natted := c.host("root@h2", "10.0.0.3")
	natted.addrs, natted.routes = []string{"ens10 10.0.0.3/24"}, []string{"ens10"}
	mustRun(t, c, home, "cluster", "node-add", "root@h2")

	d := prodDir(t, home)
	n1, err := d.Node("node1")
	if err != nil {
		t.Fatal(err)
	}
	if n1.PublicAddress.String() != "192.0.2.10" || !strings.Contains(c.hosts["root@h1"].files[k3s.ConfigFile], "node-external-ip: 192.0.2.10\n") {
		t.Errorf("node1 has the public address %s and the config:\n%s", n1.PublicAddress, c.hosts["root@h1"].files[k3s.ConfigFile])
	}
	n2, err := d.Node("node2")
	if err != nil {
		t.Fatal(err)
	}
	if n2.PublicAddress.IsValid() || strings.Contains(c.hosts["root@h2"].files[k3s.ConfigFile], "node-external-ip") {
		t.Errorf("node2, behind NAT, has the public address %s and the config:\n%s", n2.PublicAddress, c.hosts["root@h2"].files[k3s.ConfigFile])
	}
	data, err := os.ReadFile(filepath.Join(d.Path, "nodes", "node2.yml"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "public_address: \"\"\n") {
		t.Errorf("node2's file:\n%s", data)
	}
}
