package cli

import (
	"context"
	"maps"
	"net"
	"slices"

	"github.com/google/go-containerregistry/pkg/name"
	ggcrremote "github.com/google/go-containerregistry/pkg/v1/remote"

	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/moostackhq/moostack/internal/clusterdir"
	"github.com/moostackhq/moostack/internal/deployfile"
	"github.com/moostackhq/moostack/internal/k3s"
	"github.com/moostackhq/moostack/internal/registry"
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

func TestStatusWithTheControlPlaneDown(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	c.hosts["root@h1"].unreachable = true
	out, err := onFake(t, c, home, "cluster", "status")
	if err == nil || !strings.Contains(err.Error(), "can't be reached") || !strings.Contains(out, "node1  control plane") {
		t.Errorf("status = %v:\n%s", err, out)
	}
}

func TestStatusShowsDrift(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	mustRun(t, c, home, "cluster", "node-add", "root@h2")
	if out := mustRun(t, c, home, "cluster", "status"); strings.Contains(out, "Drift") || !strings.Contains(out, "node2") {
		t.Errorf("a cluster as its directory says:\n%s", out)
	}
	c.nodes["stray"] = fakeKubeNode{ip: "10.0.0.9", arch: "amd64"}
	c.nodes["node2"] = fakeKubeNode{ip: "10.0.0.8", arch: "arm64"}
	c.hosts["root@h1"].files[k3s.AppliedCommitFile] = strings.Repeat("ab", 20) + "\n"
	out := mustRun(t, c, home, "cluster", "status")
	for _, want := range []string{
		"stray is on the cluster but not in the cluster directory",
		"node2 is at 10.0.0.8 on the cluster, and at 10.0.0.3",
		"node2 is arm64 on the cluster, and amd64",
		"the cluster applied commit abababababab",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("no %q in:\n%s", want, out)
		}
	}
	delete(c.nodes, "node2")
	if out := mustRun(t, c, home, "cluster", "status"); !strings.Contains(out, "node2 is in the cluster directory but not on the cluster") {
		t.Errorf("a node gone from the cluster:\n%s", out)
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

func TestStatusOfADirectoryWithNodesButNoControlPlane(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	d := prodDir(t, home)
	path := filepath.Join(d.Path, "nodes", "node1.yml")
	data, _ := os.ReadFile(path)
	os.WriteFile(path, []byte(strings.Replace(string(data), "control_plane: true\n", "", 1)), 0o644)
	out, err := onFake(t, c, home, "cluster", "status")
	if err == nil || !strings.Contains(err.Error(), "nodes but no control plane") || !strings.Contains(out, "node1") {
		t.Errorf("status = %v\n%s", err, out)
	}
}

func TestStatusShowsAnAppliedCommitFileThatHoldsNoCommit(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	c.hosts["root@h1"].files[k3s.AppliedCommitFile] = "garbage\n"
	if out := mustRun(t, c, home, "cluster", "status"); !strings.Contains(out, `doesn't hold a commit: "garbage"`) {
		t.Errorf("status:\n%s", out)
	}
}

func TestStatusShowsACordonedNode(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	c.cordoned = map[string]bool{"node1": true}
	if out := mustRun(t, c, home, "cluster", "status"); !strings.Contains(out, "cordoned") {
		t.Errorf("status:\n%s", out)
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

// consoleImage is the tags of the console's images in the cluster's
// registry, and the digest of the image for linux and arch among those
// of the first tag.
func consoleImage(t *testing.T, c *fakeCluster, arch string) (tags []string, digest string) {
	t.Helper()
	repo, err := name.NewRepository(c.registry+"/"+k3s.ConsoleRepository, name.Insecure)
	if err != nil {
		t.Fatal(err)
	}
	if tags, err = ggcrremote.List(repo); err != nil || len(tags) == 0 {
		return nil, ""
	}
	slices.Sort(tags)
	idx, err := ggcrremote.Index(repo.Tag(tags[0]))
	if err != nil {
		t.Fatal(err)
	}
	m, err := idx.IndexManifest()
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range m.Manifests {
		if d.Platform != nil && d.Platform.OS == "linux" && d.Platform.Architecture == arch {
			digest = d.Digest.String()
		}
	}
	return tags, digest
}

func TestAddingTheControlPlaneInstallsTheConsoleAndAddingAnotherNodeDoesnt(t *testing.T) {
	c, home := fakeProd(t)
	t.Setenv("TMPDIR", t.TempDir())
	out := mustRun(t, c, home, "cluster", "node-add", "root@h1")
	for _, want := range []string{"Downloading the console...", "Installing the console...", "Added node1 to prod"} {
		if !strings.Contains(out, want) {
			t.Errorf("the output lacks %q:\n%s", want, out)
		}
	}
	tags, digest := consoleImage(t, c, "amd64")
	if len(tags) != 2 || tags[1] != k3s.LiveTag {
		t.Fatalf("the console's images are tagged %q, want its digest and %s", tags, k3s.LiveTag)
	}
	if digest != c.consoles["amd64"] {
		t.Errorf("the registry's image for amd64 is %s, the release's is %s", digest, c.consoles["amd64"])
	}
	objects := c.hosts["root@h1"].files[k3s.AutoDeployFile("console")]
	for _, want := range []string{
		"image: moostack.local/" + k3s.ConsoleRepository + "@sha256:" + tags[0],
		"match: (Host(`192.0.2.10`) || Host(`10.0.0.2`)) && (Path(`/console`) || PathPrefix(`/console/`))",
	} {
		if !strings.Contains(objects, want) {
			t.Errorf("the console's objects lack %q:\n%s", want, objects)
		}
	}
	if !c.ran("root@h1", k3s.ConsoleReadyScript) {
		t.Error("node-add didn't wait for the console to be ready")
	}
	if left, _ := filepath.Glob(filepath.Join(os.TempDir(), "mooctl-console-*")); len(left) != 0 {
		t.Errorf("the downloaded archive was left in %v", left)
	}

	downloads := c.downloads.Load()
	mustRun(t, c, home, "cluster", "node-add", "root@h2")
	if _, ok := c.hosts["root@h2"].files[k3s.AutoDeployFile("console")]; ok {
		t.Error("a node that isn't the control plane got the console's objects")
	}
	if got := c.downloads.Load(); got != downloads {
		t.Errorf("adding a node that isn't the control plane downloaded %d files", got-downloads)
	}
	if got, _ := consoleImage(t, c, "amd64"); !slices.Equal(got, tags) {
		t.Errorf("the console's images are tagged %q after another node, were %q", got, tags)
	}
}

func TestTheConsoleOfTheControlPlanesArchitectureIsInstalled(t *testing.T) {
	c, home := fakeProd(t)
	c.hosts["root@h1"].arch = "aarch64"
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	if _, digest := consoleImage(t, c, "arm64"); digest != c.consoles["arm64"] {
		t.Errorf("the registry's image for arm64 is %q, the release's is %s", digest, c.consoles["arm64"])
	}
}

func TestAConsoleThatCantBeDownloadedRefusesTheNodeBeforeChangingIt(t *testing.T) {
	c, home := fakeProd(t)
	c.release += "/nothing"
	out, err := onFake(t, c, home, "cluster", "node-add", "root@h1")
	if err == nil || !strings.Contains(err.Error(), "sha256sums.txt: 404 Not Found") {
		t.Errorf("node-add without a console to download: %v\n%s", err, out)
	}
	h1 := c.hosts["root@h1"]
	if h1.k3s || len(h1.files) != 0 || c.ran("root@h1", "INSTALL_K3S") {
		t.Errorf("the host was changed: k3s %v, files %v", h1.k3s, slices.Collect(maps.Keys(h1.files)))
	}
	if nodes := nodeNames(t, home); len(nodes) != 0 {
		t.Errorf("the cluster directory has the nodes %v", nodes)
	}
}

func TestAConsoleThatDoesntGetReadyUndoesTheControlPlane(t *testing.T) {
	c, home := fakeProd(t)
	c.consoleFails = true
	out, err := onFake(t, c, home, "cluster", "node-add", "root@h1")
	if err == nil || !strings.Contains(err.Error(), "the console isn't ready") {
		t.Errorf("node-add with a console that doesn't get ready: %v\n%s", err, out)
	}
	if !strings.Contains(out, "Uninstalling...") || c.hosts["root@h1"].k3s {
		t.Errorf("the control plane wasn't uninstalled:\n%s", out)
	}
	if tags, _ := consoleImage(t, c, "amd64"); len(tags) != 1 || tags[0] == k3s.LiveTag {
		t.Errorf("the console's images are tagged %q, want the digest alone: the image never ran", tags)
	}
	if nodes := nodeNames(t, home); len(nodes) != 0 {
		t.Errorf("the cluster directory has the nodes %v", nodes)
	}
}

func TestNoAppCanHaveTheConsolesNamespaceOrItsRepositorysPrefix(t *testing.T) {
	prefix, _, _ := strings.Cut(k3s.ConsoleRepository, "/")
	for _, name := range []string{k3s.ConsoleNamespace, prefix} {
		if err := deployfile.CheckName(name); err == nil {
			t.Errorf("an app may be named %s, and removing it would remove the console's objects or images", name)
		}
	}
}

func TestRemovingAnAppNamedMoostackLeavesTheConsolesImage(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	tags, _ := consoleImage(t, c, "amd64")
	if len(tags) != 2 {
		t.Fatalf("the console's images are tagged %q", tags)
	}
	dial := func(ctx context.Context, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", c.registry)
	}
	for _, app := range []string{"moostack", "console", "moostack-console"} {
		if err := registry.Remove(t.Context(), c.registry, app, dial); err != nil {
			t.Fatal(err)
		}
	}
	if got, _ := consoleImage(t, c, "amd64"); !slices.Equal(got, tags) {
		t.Errorf("the console's images are tagged %q after apps were removed, were %q", got, tags)
	}
}
