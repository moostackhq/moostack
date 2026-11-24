package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/moostackhq/moostack/internal/model"
)

// status is what mooctl cluster status prints on standard output, and
// its error. It fails the test if the command writes to standard error.
func status(t *testing.T, c *fakeCluster, home string) (string, error) {
	t.Helper()
	var out, errOut bytes.Buffer
	env := fakeEnvFor(c, home, &out)
	env.Stderr = &errOut
	err := New(env).Run(t.Context(), []string{"mooctl", "-c", "prod", "cluster", "status"})
	if errOut.Len() != 0 {
		t.Errorf("status wrote to standard error:\n%s", errOut.String())
	}
	return out.String(), err
}

func TestStatusListsTheNodesAndTheApps(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	mustRun(t, c, home, "cluster", "node-add", "root@h2")
	c.cordoned = map[string]bool{"node2": true}
	digest := "0661d8f5d56599a9d355618bc035ff3ff6e45800674aa9e8063287a57637e17c"
	c.orch.states = []model.AppState{
		{Name: "blog", Ready: 1, Want: 1, Images: []string{"ghcr.io/example/blog:1"}, Hosts: []string{"blog.example.com", "www.blog.example.com"}},
		{Name: "idle"},
		{Name: "shop", Ready: 3, Want: 4, Images: []string{"moostack.local/shop/jobs@sha256:" + digest, "valkey/valkey:8"}, Hosts: []string{"shop.example.com"}},
	}
	out, err := status(t, c, home)
	if err != nil {
		t.Fatal(err)
	}
	golden(t, "status.golden", out)
}

func TestStatusOfAClusterWithoutApps(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	out, err := status(t, c, home)
	if err != nil {
		t.Fatal(err)
	}
	golden(t, "status-no-apps.golden", out)
	c.orch.states = []model.AppState{}
	if out, err = status(t, c, home); err != nil {
		t.Fatal(err)
	}
	golden(t, "status-no-apps.golden", out)
}

func TestStatusOfAClusterWithoutNodes(t *testing.T) {
	c, home := fakeProd(t)
	out, err := status(t, c, home)
	if err != nil {
		t.Fatal(err)
	}
	golden(t, "status-no-nodes.golden", out)
	if c.orch.hosts != 0 {
		t.Error("status acted through a host of a cluster without nodes")
	}
}

func TestStatusListsEachKindOfDrift(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	mustRun(t, c, home, "cluster", "node-add", "root@h2")
	mustRun(t, c, home, "cluster", "node-add", "root@h3")
	c.nodes["stray"] = fakeClusterNode{ip: "10.0.0.9", arch: "amd64"}
	c.nodes["node2"] = fakeClusterNode{ip: "10.0.0.8", arch: "arm64", controlPlane: true}
	delete(c.nodes, "node3")
	c.hosts["root@h1"].files[AppliedCommitFile] = strings.Repeat("ab", 20) + "\n"
	cluster := filepath.Join(prodDir(t, home).Path, "cluster.yml")
	data, err := os.ReadFile(cluster)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), K3sVersion) {
		t.Fatalf("the cluster file doesn't pin %s:\n%s", K3sVersion, data)
	}
	if err := os.WriteFile(cluster, []byte(strings.Replace(string(data), K3sVersion, "v1.30.0+k3s1", 1)), 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := status(t, c, home)
	if err != nil {
		t.Fatal(err)
	}
	golden(t, "status-drift.golden", out)
}

func TestStatusOfAControlPlaneThatCantBeReachedIsTheClusterDirectoryAlone(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	mustRun(t, c, home, "cluster", "node-add", "root@h2")
	c.hosts["root@h1"].unreachable = true
	out, err := status(t, c, home)
	if err == nil || err.Error() != "the control plane can't be reached, so this is the cluster directory alone: node1: connecting to root@h1: no route to host" {
		t.Errorf("status: %v", err)
	}
	golden(t, "status-unreachable.golden", out)
}

func TestStatusShowsTwelveCharactersOfAnImagesDigestAndEachImageOnce(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	short := "0661d8f5d565"
	c.orch.states = []model.AppState{{Name: "shop", Ready: 3, Want: 3, Images: []string{
		"moostack.local/shop/web@sha256:" + short + strings.Repeat("a", 52),
		"moostack.local/shop/web@sha256:" + short + strings.Repeat("b", 52),
		"moostack.local/shop/web@sha256:" + short,
		"valkey/valkey:8",
	}}}
	out := mustRun(t, c, home, "cluster", "status")
	want := "shop  3/3    moostack.local/shop/web@sha256:0661d8f5d565, valkey/valkey:8  \n"
	if !strings.Contains(out, want) {
		t.Errorf("no %q in:\n%s", want, out)
	}
}

func TestStatusWhoseReadOfTheNodesFailsIsTheClusterDirectoryAlone(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	mustRun(t, c, home, "cluster", "node-add", "root@h2")
	c.orch.Fail("Nodes", errors.New("listing nodes: the API is down"))
	out, err := status(t, c, home)
	if err == nil || err.Error() != "listing nodes: the API is down" {
		t.Errorf("status: %v", err)
	}
	golden(t, "status-unreachable.golden", out)
}

func TestStatusWhoseReadOfTheAppsFailsPrintsNoTable(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	c.orch.Fail("Apps", errors.New("listing the apps: the API is down"))
	out, err := status(t, c, home)
	if err == nil || err.Error() != "listing the apps: the API is down" {
		t.Errorf("status: %v", err)
	}
	if want := "Cluster prod, k3s " + K3sVersion + "\n"; out != want {
		t.Errorf("status printed:\n%s\nwant:\n%s", out, want)
	}
}

func TestStatusShowsANodeAsReadyAsTheClusterSaysAndNoMemoryItDoesntKnow(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	mustRun(t, c, home, "cluster", "node-add", "root@h2")
	c.nodes["node2"] = fakeClusterNode{ip: "10.0.0.3", arch: "amd64", ready: "no", noMemory: true}
	out := mustRun(t, c, home, "cluster", "status")
	want := "node2  node           no     " + K3sVersion + "  amd64            10.0.0.3\n"
	if !strings.Contains(out, want) || strings.Contains(out, "Drift") {
		t.Errorf("no %q, or drift, in:\n%s", want, out)
	}
	c.nodes["stray"] = fakeClusterNode{ip: "10.0.0.9", arch: "arm64", ready: "?", noMemory: true}
	out = mustRun(t, c, home, "cluster", "status")
	want = "stray                 ?      " + K3sVersion + "  arm64            10.0.0.9\n"
	if !strings.Contains(out, want) {
		t.Errorf("no %q in:\n%s", want, out)
	}
}

func TestStatusReadsTheClusterThroughTheControlPlaneAlone(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	mustRun(t, c, home, "cluster", "node-add", "root@h2")
	before, hosts := len(c.runs), c.orch.hosts
	mustRun(t, c, home, "cluster", "status")
	if got := c.runs[before:]; len(got) != 1 || got[0] != "root@h1: "+AppliedCommitScript {
		t.Errorf("status ran on the hosts:\n%s\nwant the read of the applied commit on root@h1 alone", strings.Join(got, "\n"))
	}
	if c.orch.hosts != hosts+1 || len(c.orch.made("Nodes")) == 0 || len(c.orch.made("Apps")) != 1 {
		t.Errorf("status built %d orchestrators and called %q, want one, asked for the nodes and the apps", c.orch.hosts-hosts, c.orch.calls)
	}
}

func TestStatusOfADirectoryWithNodesButNoControlPlaneIsTheClusterDirectoryAlone(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	path := filepath.Join(prodDir(t, home).Path, "nodes", "node1.yml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(strings.Replace(string(data), "control_plane: true\n", "", 1)), 0o644); err != nil {
		t.Fatal(err)
	}
	hosts := c.orch.hosts
	out, err := status(t, c, home)
	if err == nil || err.Error() != "the cluster directory has nodes but no control plane" {
		t.Errorf("status: %v", err)
	}
	golden(t, "status-no-control-plane.golden", out)
	if c.orch.hosts != hosts {
		t.Error("status acted through a host of a cluster whose directory has no control plane")
	}
}

func TestStatusListsAnAppliedCommitThatCantBeReadAsDrift(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	c.commitUnread = true
	out, err := status(t, c, home)
	if err != nil {
		t.Fatal(err)
	}
	want := "\nNo apps.\n\nDrift:\n- reading the commit the cluster applied: exit 1: cat: " + AppliedCommitFile + ": Input/output error\n"
	if !strings.HasSuffix(out, want) {
		t.Errorf("the status doesn't end with %q:\n%s", want, out)
	}
}

func TestTheFakeOrchestratorListsTheFakeClustersNodesAndTheAppsItIsGiven(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	mustRun(t, c, home, "cluster", "node-add", "root@h2")
	c.nodes["a"] = fakeClusterNode{ip: "10.0.0.9", arch: "arm64", ready: "no", noMemory: true}
	c.cordoned = map[string]bool{"node2": true}
	ctx := context.Background()
	nodes, err := c.orch.Nodes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := []model.NodeState{
		{Name: "a", Address: "10.0.0.9", Architecture: "arm64", Version: K3sVersion, Ready: "no"},
		{Name: "node1", Address: "10.0.0.2", Architecture: "amd64", Version: K3sVersion, MemoryMiB: 1974, ControlPlane: true, Ready: "yes"},
		{Name: "node2", Address: "10.0.0.3", Architecture: "amd64", Version: K3sVersion, MemoryMiB: 1974, Ready: "yes, cordoned"},
	}
	if !slices.Equal(nodes, want) {
		t.Errorf("Nodes:\n got %+v\nwant %+v", nodes, want)
	}
	states := []model.AppState{{Name: "shop", Ready: 1, Want: 2}}
	c.orch.states = states
	apps, err := c.orch.Apps(ctx)
	if err != nil || len(apps) != 1 || apps[0].Name != "shop" || apps[0].Want != 2 {
		t.Errorf("Apps: %+v, %v, want the states given", apps, err)
	}
	apps[0].Name = "changed"
	if states[0].Name != "shop" {
		t.Error("Apps handed out the states it was given, not a copy")
	}
	down := errors.New("down")
	c.orch.Fail("Nodes", down)
	c.orch.Fail("Apps", down)
	if _, err := c.orch.Nodes(ctx); !errors.Is(err, down) {
		t.Errorf("Nodes told to fail: %v", err)
	}
	if _, err := c.orch.Apps(ctx); !errors.Is(err, down) {
		t.Errorf("Apps told to fail, with states given: %v", err)
	}
}

// BenchmarkStatus is the status of a cluster of 100 nodes and 100 apps,
// against the fake cluster.
func BenchmarkStatus(b *testing.B) {
	c, home := fakeProd(b)
	for i := range 100 {
		target := fmt.Sprintf("root@n%d", i)
		c.host(target, fmt.Sprintf("10.0.1.%d", i+1))
		mustRun(b, c, home, "cluster", "node-add", target)
	}
	for i := range 100 {
		c.orch.states = append(c.orch.states, model.AppState{
			Name: fmt.Sprintf("app%03d", i), Ready: 2, Want: 2,
			Images: []string{fmt.Sprintf("moostack.local/app%03d/web@sha256:%064d", i, i)},
			Hosts:  []string{fmt.Sprintf("app%03d.example.com", i)},
		})
	}
	for b.Loop() {
		mustRun(b, c, home, "cluster", "status")
	}
}
