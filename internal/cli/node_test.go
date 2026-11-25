package cli

import (
	"context"
	"maps"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"path"
	"slices"

	"github.com/google/go-containerregistry/pkg/name"
	ggcrremote "github.com/google/go-containerregistry/pkg/v1/remote"

	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/moostackhq/moostack/internal/clusterdir"
	"github.com/moostackhq/moostack/internal/model"
	"github.com/moostackhq/moostack/internal/orchestrator"
	"github.com/moostackhq/moostack/internal/registry"
)

// onFake runs mooctl with args on the cluster prod of home, whose hosts
// are c's.
func onFake(t testing.TB, c *fakeCluster, home string, args ...string) (string, error) {
	t.Helper()
	var out bytes.Buffer
	env := fakeEnvFor(c, home, &out)
	err := New(env).Run(t.Context(), append([]string{"mooctl", "-c", "prod"}, args...))
	return out.String(), err
}

// fakeProd is an initialised cluster prod with the hosts h1, h2 and h3
// on 10.0.0.0/24.
func fakeProd(t testing.TB) (*fakeCluster, string) {
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

func prodDir(t testing.TB, home string) *clusterdir.Dir {
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

func mustRun(t testing.TB, c *fakeCluster, home string, args ...string) string {
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
	if !h1.zot || h2.zot {
		t.Errorf("zot on h1 %v, on h2 %v", h1.zot, h2.zot)
	}
	if !c.ran("root@h1", "set -e\ncurl ") {
		t.Error("zot's install didn't run on the control plane")
	}
	asked := []orchestrator.NodeRequest{{Name: "node1", ControlPlane: true}, {Name: "node2"}}
	if !slices.Equal(c.orch.asked, asked) {
		t.Errorf("the hosts were checked as %+v, want %+v", c.orch.asked, asked)
	}
	for name, want := range map[string]model.Node{
		"node1": {Name: "node1", ControlPlane: true, PrivateAddress: netip.MustParseAddr("10.0.0.2"), Interface: "ens10", PublicAddress: netip.MustParseAddr("192.0.2.10"), Architecture: "amd64", MemoryMiB: 1974, GOGC: "25", GOMEMLIMIT: "197MiB"},
		"node2": {Name: "node2", PrivateAddress: netip.MustParseAddr("10.0.0.3"), Interface: "ens10", PublicAddress: netip.MustParseAddr("192.0.2.10"), Architecture: "amd64", MemoryMiB: 1974, GOGC: "25", GOMEMLIMIT: "197MiB"},
	} {
		if got := c.orch.added[name]; got != want {
			t.Errorf("the orchestrator was asked to add\n%+v, want what it found the host to be,\n%+v", got, want)
		}
		written, err := d.Node(name)
		if err != nil {
			t.Fatal(err)
		}
		if got := modelNode(written); got != want {
			t.Errorf("the cluster directory holds\n%+v, want\n%+v", got, want)
		}
	}
	if c.locked {
		t.Error("the lock is still held")
	}
	if applied := strings.TrimSpace(h1.files[AppliedCommitFile]); applied != git(t, d.Path, "rev-parse", "HEAD") {
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
		"a name the cluster has": {
			setup: func(c *fakeCluster) { c.nodes["node7"] = fakeClusterNode{ip: "10.0.0.9", arch: "amd64"} },
			args:  []string{"--name", "node7", "root@h2"},
			want:  "node7",
		},
		"a host that is a node": {
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
		"a control plane of another version": {
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
			if added := c.orch.made("AddNode"); len(added) != 1 {
				t.Errorf("the orchestrator was asked to add %q, want node1 alone", added)
			}
			if c.ran("root@h2", "") {
				t.Errorf("the refusal ran on the host:\n%s", strings.Join(c.runs, "\n---\n"))
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
	out, err := onFake(t, c, home, "cluster", "node-add", "root@h2")
	if err == nil || err.Error() != "installing k3s: exit 1: install failed" {
		t.Fatalf("a failed install: %v\n%s", err, out)
	}
	if want := "Adding node2 to prod (node, amd64, 1974 MiB, 10.0.0.3)\nInstalling k3s...\nUninstalling...\n"; out != want {
		t.Errorf("the output of a failed install:\n%s\nwant:\n%s", out, want)
	}
	if l := left(c.hosts["root@h2"]); l != "" {
		t.Errorf("h2 still has %s", l)
	}
	if names := nodeNames(t, home); len(names) != 1 {
		t.Errorf("nodes = %v", names)
	}
	if status := git(t, prodDir(t, home).Path, "status", "--porcelain"); status != "" {
		t.Errorf("left in the directory: %s", status)
	}
}

func TestAJoinThatFailsToStartIsUndone(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	c.hosts["root@h2"].failStart = true
	out, err := onFake(t, c, home, "cluster", "node-add", "root@h2")
	if err == nil || !strings.Contains(err.Error(), "starting k3s: exit 1: k3s-agent: token is invalid") {
		t.Fatalf("err %v:\n%s", err, out)
	}
	if l := left(c.hosts["root@h2"]); l != "" || strings.Count(out, "Uninstalling...\n") != 1 {
		t.Errorf("h2 still has %q after:\n%s", l, out)
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
	if want := "Removing node2 from prod\nDraining node2...\nUninstalling...\nRemoved node2 from prod\n"; out != want || c.hosts["root@h2"].k3s {
		t.Errorf("output:\n%s\nwant:\n%s\nk3s on h2: %t", out, want, c.hosts["root@h2"].k3s)
	}
	if got, want := c.orch.removals["node2"], (orchestrator.RemoveOptions{}); got != want || !slices.Equal(c.orch.made("RemoveNode"), []string{"RemoveNode node2"}) {
		t.Errorf("the orchestrator was asked for the removals %q, node2's with %+v", c.orch.made("RemoveNode"), got)
	}
	if _, found := c.nodes["node2"]; found {
		t.Error("the cluster still has node2")
	}

	// A node whose host is gone goes with --unreachable only.
	c.hosts["root@h3"].unreachable = true
	if out, err := onFake(t, c, home, "cluster", "node-remove", "node3"); err == nil {
		t.Errorf("an unreachable node went without --unreachable:\n%s", out)
	}
	out = mustRun(t, c, home, "cluster", "node-remove", "--unreachable", "node3")
	if _, found := c.nodes["node3"]; found {
		t.Error("the cluster still has node3")
	}
	if want := "Removing node3 from prod\nWarning: node3 can't be reached; if k3s still runs there, it registers again: run k3s-agent-uninstall.sh on it.\nRemoved node3 from prod\n"; out != want || !c.orch.removals["node3"].Unreachable {
		t.Errorf("output:\n%s\nwant:\n%s\nremoved with %+v", out, want, c.orch.removals["node3"])
	}

	// The last node goes with --yes only.
	if out, err := onFake(t, c, home, "cluster", "node-remove", "node1"); err == nil {
		t.Errorf("the last node went without --yes:\n%s", out)
	}
	mustRun(t, c, home, "cluster", "node-remove", "--yes", "node1")
	if names := nodeNames(t, home); len(names) != 0 {
		t.Errorf("nodes = %v", names)
	}
	if l := left(c.hosts["root@h1"]); l != "" {
		t.Errorf("h1 still has %s", l)
	}
}

func TestNodeRemoveOfANodeOnlyTheClusterHas(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	c.nodes["stray"] = fakeClusterNode{ip: "10.0.0.9", arch: "amd64"}
	mustRun(t, c, home, "cluster", "node-remove", "stray")
	if _, found := c.nodes["stray"]; found {
		t.Error("the cluster still has the node")
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
	c.nodes["stray"] = fakeClusterNode{ip: "10.0.0.9", arch: "amd64"}
	c.nodes["node2"] = fakeClusterNode{ip: "10.0.0.8", arch: "arm64"}
	c.hosts["root@h1"].files[AppliedCommitFile] = strings.Repeat("ab", 20) + "\n"
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
	if added := c.orch.made("AddNode"); len(added) != 1 {
		t.Errorf("node-add went on: the orchestrator was asked to add %q", added)
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
	// A run cut off after the cluster deleted the node.
	delete(c.nodes, "node2")
	mustRun(t, c, home, "cluster", "node-remove", "node2")
	if c.hosts["root@h2"].k3s {
		t.Error("k3s is still on h2")
	}
	if names := nodeNames(t, home); len(names) != 1 {
		t.Errorf("nodes = %v", names)
	}
}

func TestStatusShowsAnAppliedCommitFileThatHoldsNoCommit(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	c.hosts["root@h1"].files[AppliedCommitFile] = "garbage\n"
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
	natted.public = netip.Addr{}
	mustRun(t, c, home, "cluster", "node-add", "root@h2")

	d := prodDir(t, home)
	n1, err := d.Node("node1")
	if err != nil {
		t.Fatal(err)
	}
	if n1.PublicAddress.String() != "192.0.2.10" || c.orch.added["node1"].PublicAddress != n1.PublicAddress {
		t.Errorf("node1 has the public address %s, and the orchestrator was given %s", n1.PublicAddress, c.orch.added["node1"].PublicAddress)
	}
	n2, err := d.Node("node2")
	if err != nil {
		t.Fatal(err)
	}
	if n2.PublicAddress.IsValid() || c.orch.added["node2"].PublicAddress.IsValid() {
		t.Errorf("node2, behind NAT, has the public address %s, and the orchestrator was given %s", n2.PublicAddress, c.orch.added["node2"].PublicAddress)
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
	repo, err := name.NewRepository(c.registry+"/"+consoleRepository, name.Insecure)
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
	if len(tags) != 2 || tags[1] != registry.LiveTag {
		t.Fatalf("the console's images are tagged %q, want its digest and %s", tags, registry.LiveTag)
	}
	if digest != c.consoles["amd64"] {
		t.Errorf("the registry's image for amd64 is %s, the release's is %s", digest, c.consoles["amd64"])
	}
	if want := "moostack.local/" + consoleRepository + "@sha256:" + tags[0]; c.consoleRuns != want || !slices.Equal(c.consoleAt, []string{"192.0.2.10", "10.0.0.2"}) {
		t.Errorf("the console runs from %s on %q, want %s on the node's public and private addresses", c.consoleRuns, c.consoleAt, want)
	}
	if left, _ := filepath.Glob(filepath.Join(os.TempDir(), "mooctl-console-*")); len(left) != 0 {
		t.Errorf("the downloaded archive was left in %v", left)
	}

	downloads := c.downloads.Load()
	mustRun(t, c, home, "cluster", "node-add", "root@h2")
	if runs := c.orch.made("RunConsole"); len(runs) != 1 {
		t.Errorf("the console was run %d times, want once, with the control plane", len(runs))
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

type roundTrip func(*http.Request) (*http.Response, error)

func (f roundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestNodeAddDownloadsTheConsoleOfTheReleaseBuiltFromMooctlsCommit(t *testing.T) {
	const github = "https://github.com/moostackhq/moostack/releases/download/"
	const built = "861df463416c35c9042a9df97e5ba73b942469bb"
	files := func(release string) []string {
		return []string{github + release + "/sha256sums.txt", github + release + "/console-linux-amd64.tar"}
	}
	for name, c := range map[string]struct {
		commit string
		set    bool
		want   []string
	}{
		"built from a commit":                    {commit: built, want: files("edge-" + built)},
		"built from none":                        {want: files("edge")},
		"built from a commit, a release URL set": {commit: built, set: true},
		"built from none, a release URL set":     {set: true},
	} {
		t.Run(name, func(t *testing.T) {
			fake, home := fakeProd(t)
			served, err := url.Parse(fake.release)
			if err != nil {
				t.Fatal(err)
			}
			if !c.set {
				fake.release = ""
			}
			// Requests to GitHub are answered by the fake release.
			var asked []string
			transport := http.DefaultTransport
			http.DefaultTransport = roundTrip(func(r *http.Request) (*http.Response, error) {
				if r.URL.Host != "github.com" {
					return transport.RoundTrip(r)
				}
				asked = append(asked, r.URL.String())
				r = r.Clone(r.Context())
				r.URL.Scheme, r.URL.Host, r.URL.Path = served.Scheme, served.Host, served.Path+"/"+path.Base(r.URL.Path)
				return transport.RoundTrip(r)
			})
			commit = c.commit
			t.Cleanup(func() { http.DefaultTransport, commit = transport, "" })

			mustRun(t, fake, home, "cluster", "node-add", "root@h1")
			if !slices.Equal(asked, c.want) {
				t.Errorf("node-add downloaded %v from GitHub, want %v", asked, c.want)
			}
			if got := fake.downloads.Load(); got != 2 {
				t.Errorf("node-add downloaded %d files of the release, want 2", got)
			}
			if _, digest := consoleImage(t, fake, "amd64"); digest != fake.consoles["amd64"] {
				t.Errorf("the registry's image is %q, the release's is %s", digest, fake.consoles["amd64"])
			}
		})
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
	if h1.k3s || len(h1.files) != 0 || len(c.orch.made("AddNode")) != 0 {
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
	if l := left(c.hosts["root@h1"]); !strings.Contains(out, "Uninstalling...") || l != "" {
		t.Errorf("the control plane wasn't uninstalled, it still has %q:\n%s", l, out)
	}
	if tags, _ := consoleImage(t, c, "amd64"); len(tags) != 1 || tags[0] == registry.LiveTag {
		t.Errorf("the console's images are tagged %q, want the digest alone: the image never ran", tags)
	}
	if nodes := nodeNames(t, home); len(nodes) != 0 {
		t.Errorf("the cluster directory has the nodes %v", nodes)
	}
}

func TestRemovingAnAppNamedAsTheConsolesRepositorysPrefixRemovesNothing(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	before, _ := consoleImage(t, c, "amd64")
	prefix, _, _ := strings.Cut(consoleRepository, "/")
	for _, name := range []string{"kube-system", prefix} {
		if out, err := onFake(t, c, home, "deploy", "--remove", name); err == nil || !strings.Contains(err.Error(), "there is no app "+name) {
			t.Errorf("deploy --remove %s: %v\n%s", name, err, out)
		}
	}
	if after, _ := consoleImage(t, c, "amd64"); !slices.Equal(after, before) {
		t.Errorf("the console's images are tagged %q after the removals, and %q before", after, before)
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

func TestNodeAddAsksTheOrchestratorForTheNameAndTheAddressGiven(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "--name", "cp", "--private-address", "10.0.0.2", "root@h1")
	mustRun(t, c, home, "cluster", "node-add", "root@h2")
	want := []orchestrator.NodeRequest{{Name: "cp", ControlPlane: true, PrivateAddress: netip.MustParseAddr("10.0.0.2")}, {Name: "node1"}}
	if !slices.Equal(c.orch.asked, want) {
		t.Errorf("the hosts were checked as %+v, want %+v", c.orch.asked, want)
	}
}

func TestTheFirstNodeOfAnotherArchitectureWarnsOnlyIfTheClusterHasApps(t *testing.T) {
	for _, apps := range []bool{false, true} {
		c, home := fakeProd(t)
		c.hosts["root@h2"].arch = "aarch64"
		c.hosts["root@h3"].arch = "aarch64"
		mustRun(t, c, home, "cluster", "node-add", "root@h1")
		if apps {
			hasApp(t, c)
		}
		out := mustRun(t, c, home, "cluster", "node-add", "root@h2")
		warning := "Warning: node2 is the cluster's first arm64 node; deploy its built apps again to run them there.\n"
		if strings.Contains(out, warning) != apps || len(c.orch.made("Apps")) != 1 {
			t.Errorf("with apps %t, after %d reads of the apps, node-add printed:\n%s", apps, len(c.orch.made("Apps")), out)
		}
		// The second arm64 node is no news.
		if out := mustRun(t, c, home, "cluster", "node-add", "root@h3"); strings.Contains(out, "Warning") || len(c.orch.made("Apps")) != 1 {
			t.Errorf("with apps %t, after %d reads of the apps, the second arm64 node printed:\n%s", apps, len(c.orch.made("Apps")), out)
		}
	}
	// Apps that can't be read are no warning and no failure.
	c, home := fakeProd(t)
	c.hosts["root@h2"].arch = "aarch64"
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	hasApp(t, c)
	c.orch.Fail("Apps", errors.New("the API is down"))
	if out := mustRun(t, c, home, "cluster", "node-add", "root@h2"); strings.Contains(out, "Warning") {
		t.Errorf("node-add with apps that can't be read printed:\n%s", out)
	}
}

func TestAControlPlaneWhoseAddFailsAtAnyStepLeavesNothingOnTheHost(t *testing.T) {
	for step, tc := range map[string]struct {
		fail func(c *fakeCluster)
		want string
		// uninstalls says the command has the orchestrator uninstall.
		uninstalls bool
	}{
		"the orchestrator's install":         {func(c *fakeCluster) { c.hosts["root@h1"].failInstall = true }, "installing k3s: exit 1: install failed", false},
		"the registry's install":             {func(c *fakeCluster) { c.zotFails = true }, "installing zot: exit 1: zot doesn't answer at 10.0.0.2:5000", true},
		"the push of the console":            {func(c *fakeCluster) { c.registry = "" }, "the console's image: ", true},
		"the console":                        {func(c *fakeCluster) { c.consoleFails = true }, "the console isn't ready: ", true},
		"an orchestrator that tells no step": {func(c *fakeCluster) { c.orch.Fail("AddNode", errors.New("writing /etc/file: no space left")) }, "writing /etc/file: no space left", false},
	} {
		t.Run(step, func(t *testing.T) {
			c, home := fakeProd(t)
			tc.fail(c)
			out, err := onFake(t, c, home, "cluster", "node-add", "root@h1")
			if err == nil || !strings.HasPrefix(err.Error(), tc.want) {
				t.Fatalf("node-add: %v, want an error that starts with %q\n%s", err, tc.want, out)
			}
			h1 := c.hosts["root@h1"]
			if l := left(h1); l != "" || !c.ran("root@h1", ownUninstallScript) {
				t.Errorf("h1 still has %q; its registry's uninstall ran: %t", l, c.ran("root@h1", ownUninstallScript))
			}
			if got := len(c.orch.made("Uninstall")) == 1; got != tc.uninstalls {
				t.Errorf("the orchestrator was asked to uninstall: %t, want %t", got, tc.uninstalls)
			}
			if said := strings.Count(out, "Uninstalling...\n"); said != 1 {
				t.Errorf("the output says Uninstalling... %d times, want once:\n%s", said, out)
			}
			if len(nodeNames(t, home)) != 0 || len(c.nodes) != 0 || c.locked {
				t.Errorf("the cluster directory has %v, the cluster %d nodes, and the lock is held: %t", nodeNames(t, home), len(c.nodes), c.locked)
			}
			if status := git(t, prodDir(t, home).Path, "status", "--porcelain"); status != "" {
				t.Errorf("left in the directory: %s", status)
			}
		})
	}
}

func TestANodeWhoseAddFailsIsNotGivenTheControlPlanesOwnUninstall(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	c.hosts["root@h2"].failStart = true
	if _, err := onFake(t, c, home, "cluster", "node-add", "root@h2"); err == nil {
		t.Fatal("a join whose start fails passed")
	}
	if c.ran("root@h2", "") || len(c.orch.made("Uninstall")) != 0 || !c.hosts["root@h1"].zot {
		t.Errorf("the failed join ran on its host:\n%s\nthe orchestrator was asked to uninstall %d times, and zot is on h1: %t", strings.Join(c.runs, "\n---\n"), len(c.orch.made("Uninstall")), c.hosts["root@h1"].zot)
	}
}

func TestAJoinThatFailsBeforeItChangesTheHostSaysNothingOfAnUninstall(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	c.orch.Fail("AddNode", errors.New("reading the join token on the control plane: exit 1"))
	out, err := onFake(t, c, home, "cluster", "node-add", "root@h2")
	if err == nil || err.Error() != "reading the join token on the control plane: exit 1" {
		t.Fatalf("node-add: %v\n%s", err, out)
	}
	if want := "Adding node2 to prod (node, amd64, 1974 MiB, 10.0.0.3)\n"; out != want {
		t.Errorf("output:\n%s\nwant:\n%s", out, want)
	}
	if len(nodeNames(t, home)) != 1 || c.locked {
		t.Errorf("the cluster directory has %v, and the lock is held: %t", nodeNames(t, home), c.locked)
	}
}

func TestTheRegistrysInstallStartsOnceTheOrchestratorStartsTheControlPlane(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	if c.orch.zotAtInstalling || !c.hosts["root@h1"].zot {
		t.Errorf("zot's install ran while the orchestrator installed: %t; zot is on h1: %t", c.orch.zotAtInstalling, c.hosts["root@h1"].zot)
	}
}

// streams runs mooctl with args on prod and returns each stream.
func streams(t *testing.T, c *fakeCluster, home string, ctx context.Context, args ...string) (stdout, stderr string, err error) {
	t.Helper()
	var out, errOut bytes.Buffer
	env := fakeEnvFor(c, home, &out)
	env.Stderr = &errOut
	err = New(env).Run(ctx, append([]string{"mooctl", "-c", "prod"}, args...))
	return out.String(), errOut.String(), err
}

func TestTheNodeCommandsPrintTheirResultOnStandardOutputAndTheRestOnStandardError(t *testing.T) {
	c, home := fakeProd(t)
	c.hosts["root@h2"].arch = "aarch64"
	for _, tc := range []struct {
		args           []string
		stdout, stderr string
	}{
		{[]string{"cluster", "node-add", "root@h1"}, "Added node1 to prod\n", "Adding node1 to prod (control plane, amd64, 1974 MiB, 10.0.0.2)\nDownloading the console...\nInstalling k3s and zot...\nWaiting for node1 to be Ready...\nInstalling the console...\n"},
		{[]string{"deploy", "--remove", "none"}, "", ""},
		{[]string{"cluster", "node-add", "root@h2"}, "Added node2 to prod\n", "Adding node2 to prod (node, arm64, 1974 MiB, 10.0.0.3)\nInstalling k3s...\nWaiting for node2 to be Ready...\nWarning: node2 is the cluster's first arm64 node; deploy its built apps again to run them there.\n"},
		{[]string{"cluster", "node-add", "root@h3"}, "Added node3 to prod\n", "Adding node3 to prod (node, amd64, 1974 MiB, 10.0.0.4)\nInstalling k3s...\nWaiting for node3 to be Ready...\n"},
		{[]string{"cluster", "node-remove", "node2"}, "Removed node2 from prod\n", "Removing node2 from prod\nDraining node2...\nUninstalling...\n"},
		{[]string{"cluster", "node-remove", "--unreachable", "node3"}, "Removed node3 from prod\n", "Removing node3 from prod\nWarning: node3 can't be reached; if k3s still runs there, it registers again: run k3s-agent-uninstall.sh on it.\n"},
		{[]string{"cluster", "node-remove", "--yes", "node1"}, "Removed node1 from prod\n", "Removing node1 from prod\nUninstalling...\n"},
	} {
		switch tc.args[len(tc.args)-1] {
		case "none":
			hasApp(t, c)
			continue
		case "node3":
			if tc.args[1] == "node-remove" {
				c.hosts["root@h3"].unreachable = true
			}
		}
		stdout, stderr, err := streams(t, c, home, t.Context(), tc.args...)
		if err != nil || stdout != tc.stdout || stderr != tc.stderr {
			t.Errorf("mooctl %s: %v\nstandard output:\n%s\nwant:\n%s\nstandard error:\n%s\nwant:\n%s", strings.Join(tc.args, " "), err, stdout, tc.stdout, stderr, tc.stderr)
		}
	}
	c.host("root@h4", "10.0.0.5").failInstall = true
	if _, _, err := streams(t, c, home, t.Context(), "cluster", "node-add", "root@h4"); err == nil {
		t.Fatal("node-add on a cluster without nodes, of a host whose install fails, passed")
	}
	stdout, stderr, err := streams(t, c, home, t.Context(), "cluster", "node-remove", "node9")
	if err == nil || stdout != "" || stderr != "" {
		t.Errorf("a refused node-remove: %v, printed %q and said %q, want nothing but its error", err, stdout, stderr)
	}
}

func TestAnInterruptedAddOfAControlPlaneStillLeavesNothingOnTheHost(t *testing.T) {
	c, home := fakeProd(t)
	ctx, cancel := context.WithCancelCause(t.Context())
	c.orch.onRunConsole = func() { cancel(errors.New("interrupted")) }
	_, stderr, err := streams(t, c, home, ctx, "cluster", "node-add", "root@h1")
	if err == nil || !strings.Contains(err.Error(), "interrupted") {
		t.Fatalf("node-add interrupted while the console is run: %v", err)
	}
	if l := left(c.hosts["root@h1"]); l != "" || len(c.orch.made("Uninstall")) != 1 || !strings.HasSuffix(stderr, "Uninstalling...\n") {
		t.Errorf("h1 still has %q after %d uninstalls by the orchestrator; it said:\n%s", l, len(c.orch.made("Uninstall")), stderr)
	}
	if len(nodeNames(t, home)) != 0 || c.locked {
		t.Errorf("the cluster directory has %v, and the lock is held: %t", nodeNames(t, home), c.locked)
	}
}

func TestACommandThatActsOnNodesReachesTheClusterThroughItsControlPlane(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	if c.orch.hosts != 0 {
		t.Errorf("adding the first node built %d orchestrators through a control plane, of a cluster without one", c.orch.hosts)
	}
	for _, args := range [][]string{
		{"cluster", "node-add", "root@h2"},
		{"cluster", "node-remove", "node2"},
		{"console"},
	} {
		before := c.orch.hosts
		mustRun(t, c, home, args...)
		if got := c.orch.hosts - before; got != 1 {
			t.Errorf("mooctl %s built %d orchestrators through the control plane, want 1", strings.Join(args, " "), got)
		}
	}
}

func TestARegistryInstallThatFailsEndsTheOrchestratorsAddAtOnce(t *testing.T) {
	c, home := fakeProd(t)
	c.zotFails, c.orch.lingers = true, true
	start := time.Now()
	out, err := onFake(t, c, home, "cluster", "node-add", "root@h1")
	if err == nil || err.Error() != "installing zot: exit 1: zot doesn't answer at 10.0.0.2:5000" {
		t.Errorf("node-add: %v\n%s", err, out)
	}
	if c.orch.addEnded == nil || time.Since(start) > 4*time.Second {
		t.Errorf("the orchestrator's add ended with %v after %s, want the registry's failure, at once", c.orch.addEnded, time.Since(start))
	}
	if l := left(c.hosts["root@h1"]); l != "" {
		t.Errorf("h1 still has %s", l)
	}
}

// transcript runs each of the commands on c and returns them with what
// each printed and how it failed. A command that starts with a + is run
// with an operator who types the cluster's name.
func transcript(t *testing.T, c *fakeCluster, home string, commands ...string) string {
	t.Helper()
	var b strings.Builder
	for _, command := range commands {
		args := strings.Fields(strings.TrimPrefix(command, "+"))
		var out string
		var err error
		if strings.HasPrefix(command, "+") {
			out, err = typing(t, c, home, []string{"prod"}, append([]string{"-c", "prod"}, args...)...)
		} else {
			out, err = onFake(t, c, home, args...)
		}
		b.WriteString("$ mooctl " + strings.Join(args, " ") + "\n" + out)
		if err != nil {
			b.WriteString("error: " + err.Error() + "\n")
		}
	}
	return strings.ReplaceAll(b.String(), home, "<home>")
}

func TestTheNodeCommandsPrintWhatTheyPrinted(t *testing.T) {
	c, home := fakeProd(t)
	c.hosts["root@h3"].arch = "aarch64"
	c.host("root@h4", "10.0.0.5").failInstall = true
	got := transcript(t, c, home,
		"cluster node-add root@h1",
		"cluster node-add root@h2",
	)
	hasApp(t, c)
	got += transcript(t, c, home,
		"cluster node-add --name db root@h3",
		"cluster node-add root@h4",
		"cluster node-add --name db root@h4",
		"cluster node-remove node1",
		"cluster node-remove node2",
	)
	c.nodes["stray"] = fakeClusterNode{ip: "10.0.0.9", arch: "amd64"}
	c.hosts["root@h3"].unreachable = true
	got += transcript(t, c, home,
		"cluster node-remove stray",
		"cluster node-remove db",
		"cluster node-remove --unreachable db",
		"cluster node-remove absent",
		"cluster node-remove node1",
		"cluster node-remove --yes node1",
	)
	golden(t, "node.golden", got)
}

func TestTheNodeCommandsWithoutTheControlPlanePrintWhatTheyPrinted(t *testing.T) {
	c, home := fakeProd(t)
	got := transcript(t, c, home,
		"cluster node-add root@h1",
		"cluster node-add root@h2",
		"cluster node-add root@h3",
	)
	c.hosts["root@h1"].unreachable = true
	c.hosts["root@h3"].unreachable = true
	got += transcript(t, c, home,
		"cluster node-add root@h2",
		"cluster node-remove node2",
		"cluster node-remove --unreachable node1",
		"cluster node-remove --unreachable node2",
		"cluster node-remove --unreachable node3",
		"cluster node-remove --unreachable node1",
		"cluster node-remove --unreachable --yes node1",
	)
	golden(t, "node-unreachable.golden", got)
}

func TestClusterRemovePrintsWhatItPrinted(t *testing.T) {
	c, home := fakeProd(t)
	got := transcript(t, c, home,
		"cluster node-add root@h1",
		"cluster node-add root@h2",
		"cluster node-add root@h3",
	)
	c.hosts["root@h3"].unreachable = true
	got += transcript(t, c, home,
		"+cluster remove prod",
		"+cluster remove --unreachable prod",
	)
	golden(t, "cluster-remove.golden", got)
}

// Sequences of node-add and node-remove, some failing at a step of the
// orchestrator's, run against a model of the cluster: the names of its
// nodes, in the order they joined, with the host of each. After every
// command the cluster directory, the cluster and the hosts hold the
// model's nodes and nothing else, the control plane is the first that
// joined, and the lock is free.
func TestSequencesOfNodeCommandsMatchAModel(t *testing.T) {
	for seed := range uint64(25) {
		rng := rand.New(rand.NewPCG(seed, 7))
		c, home := fakeProd(t)
		c.host("root@h4", "10.0.0.5")
		targets := slices.Sorted(maps.Keys(c.hosts))
		type node struct{ name, target string }
		var model []node
		var history []string
		check := func() {
			t.Helper()
			var want, onHosts []string
			for _, n := range model {
				want = append(want, n.name)
			}
			slices.Sort(want)
			for _, target := range targets {
				if h := c.hosts[target]; h.k3s {
					onHosts = append(onHosts, h.nodeName)
				} else if l := left(h); l != "" {
					t.Fatalf("seed %d: %s, no node, still has %s after\n%s", seed, target, l, strings.Join(history, "\n"))
				}
			}
			slices.Sort(onHosts)
			inCluster := slices.Sorted(maps.Keys(c.nodes))
			inDir := nodeNames(t, home)
			slices.Sort(inDir)
			if !slices.Equal(inDir, want) || !slices.Equal(inCluster, want) || !slices.Equal(onHosts, want) || c.locked {
				t.Fatalf("seed %d: the cluster directory has %v, the cluster %v, the hosts %v, want %v; the lock is held: %t; after\n%s", seed, inDir, inCluster, onHosts, want, c.locked, strings.Join(history, "\n"))
			}
			if len(model) > 0 {
				cp, ok, err := prodDir(t, home).ControlPlane()
				if err != nil || !ok || cp.Name != model[0].name || !c.hosts[model[0].target].zot {
					t.Fatalf("seed %d: the control plane is %q (%v, %v), with zot on the first node's host: %t, want %s; after\n%s", seed, cp.Name, ok, err, c.hosts[model[0].target].zot, model[0].name, strings.Join(history, "\n"))
				}
			}
		}
		for range 14 {
			c.orch.failRemovalAt, c.orch.onUninstall = nil, nil
			for _, h := range c.hosts {
				h.failInstall, h.failStart = false, false
			}
			c.consoleFails, c.zotFails = false, false
			if add := rng.IntN(2) == 0; add {
				target := targets[rng.IntN(len(targets))]
				taken := slices.ContainsFunc(model, func(n node) bool { return n.target == target })
				fault := []string{"", "", "install", "start", "console", "zot"}[rng.IntN(6)]
				switch fault {
				case "install":
					c.hosts[target].failInstall = true
				case "start":
					c.hosts[target].failStart = len(model) > 0
				case "console":
					c.consoleFails = true
				case "zot":
					c.zotFails = true
				}
				fails := taken || fault == "install" || (fault == "start" && len(model) > 0) || ((fault == "console" || fault == "zot") && len(model) == 0)
				name := fmt.Sprintf("n%d", len(history))
				out, err := onFake(t, c, home, "cluster", "node-add", "--name", name, target)
				history = append(history, fmt.Sprintf("node-add --name %s %s, fault %q: %v", name, target, fault, err))
				if (err != nil) != fails {
					t.Fatalf("seed %d: failed %t, want %t:\n%s\n%s", seed, err != nil, fails, strings.Join(history, "\n"), out)
				}
				if err == nil {
					model = append(model, node{name, target})
				}
			} else {
				name := "absent"
				at := -1
				if len(model) > 0 && rng.IntN(5) > 0 {
					at = rng.IntN(len(model))
					name = model[at].name
				}
				yes := rng.IntN(2) == 0
				fault := []orchestrator.Step{0, 0, orchestrator.Draining, orchestrator.Uninstalling, orchestrator.Deleting}[rng.IntN(5)]
				if fault != 0 {
					c.orch.failRemovalAt = map[orchestrator.Step]error{fault: errors.New("a fault")}
				}
				args := []string{"cluster", "node-remove", name}
				if yes {
					args = append(args, "--yes")
				}
				fails := at < 0 || (at == 0 && (len(model) > 1 || !yes)) || (at > 0 && fault != 0)
				out, err := onFake(t, c, home, args...)
				history = append(history, fmt.Sprintf("%s, fault at step %d: %v", strings.Join(args[1:], " "), fault, err))
				if (err != nil) != fails {
					t.Fatalf("seed %d: failed %t, want %t:\n%s\n%s", seed, err != nil, fails, strings.Join(history, "\n"), out)
				}
				if err == nil {
					model = slices.Delete(model, at, at+1)
				} else if at > 0 && fault == orchestrator.Deleting {
					// The host is uninstalled from and the node is kept: the
					// removal run again finishes it.
					c.orch.failRemovalAt = nil
					if out, err := onFake(t, c, home, "cluster", "node-remove", name); err != nil {
						t.Fatalf("seed %d: the removal run again: %v\n%s\n%s", seed, err, strings.Join(history, "\n"), out)
					}
					history = append(history, "node-remove "+name+" again")
					model = slices.Delete(model, at, at+1)
				}
			}
			check()
		}
	}
}
