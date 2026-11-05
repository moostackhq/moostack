package cluster_test

import (
	"errors"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/moostackhq/moostack/internal/cluster"
)

// clusterOf is a new cluster directory with a node of each of names, at
// generation 2.
func clusterOf(t *testing.T, names ...string) *cluster.Dir {
	t.Helper()
	d := newCluster(t)
	for _, name := range names {
		if err := d.AddNode(node(t, name), 2); err != nil {
			t.Fatal(err)
		}
	}
	return d
}

func TestRemoveNodeListsItsIdentityAsRemoved(t *testing.T) {
	d := clusterOf(t, "web-1", "web-2", "web-3")
	if err := d.RemoveNodes([]string{"web-2"}, 3); err != nil {
		t.Fatal(err)
	}
	if err := d.RemoveNodes([]string{"web-1"}, 4); err != nil {
		t.Fatal(err)
	}
	reopened, err := cluster.Open(d.Path())
	if err != nil {
		t.Fatal(err)
	}
	if reopened.Cluster.Generation != 4 || d.Cluster.Generation != 4 {
		t.Errorf("the cluster file is at generation %d, the open directory at %d, want 4", reopened.Cluster.Generation, d.Cluster.Generation)
	}
	nodes, err := reopened.Nodes()
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 || nodes[0].Name != "web-3" {
		t.Errorf("the nodes are %v, want web-3 alone", nodes)
	}
	removed, err := reopened.RemovedIdentities()
	if err != nil {
		t.Fatal(err)
	}
	want := []cluster.RemovedIdentity{
		{Node: "web-2", Identity: node(t, "web-2").Identity},
		{Node: "web-1", Identity: node(t, "web-1").Identity},
	}
	if !slices.Equal(removed, want) {
		t.Errorf("the removed identities are %v, want %v", removed, want)
	}
	if got, want := readFile(t, filepath.Join(d.Path(), "removed-identities.yml")),
		"- node: web-2\n  identity: "+want[0].Identity+"\n- node: web-1\n  identity: "+want[1].Identity+"\n"; got != want {
		t.Errorf("removed-identities.yml is\n%s\nwant\n%s", got, want)
	}
}

func TestANewClusterHasNoRemovedIdentities(t *testing.T) {
	removed, err := newCluster(t).RemovedIdentities()
	if err != nil || len(removed) != 0 {
		t.Errorf("RemovedIdentities: %v, %v, want none", removed, err)
	}
}

func TestRemoveNodeOfAnUnknownNodeIsNotExist(t *testing.T) {
	d := clusterOf(t, "web-1")
	before := snapshot(t, d.Path())
	if err := d.RemoveNodes([]string{"web-2"}, 3); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("RemoveNode of an unknown node: %v, want an error that is fs.ErrNotExist", err)
	}
	if after := snapshot(t, d.Path()); !maps.Equal(before, after) {
		t.Errorf("RemoveNode of an unknown node changed the directory from %v to %v", before, after)
	}
}

func TestRemoveNodeThatCantWriteTheGenerationLeavesTheDirectoryAsItWas(t *testing.T) {
	d := clusterOf(t, "web-1", "web-2")
	if err := d.RemoveNodes([]string{"web-1"}, 3); err != nil {
		t.Fatal(err)
	}
	// A directory in the cluster file's place can't be replaced by it.
	clusterFile := filepath.Join(d.Path(), "cluster.yml")
	content := readFile(t, clusterFile)
	if err := os.Remove(clusterFile); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(clusterFile, "keep"), content)
	before := snapshot(t, d.Path())
	if err := d.RemoveNodes([]string{"web-2"}, 4); err == nil {
		t.Fatal("RemoveNode succeeded without writing the cluster file")
	}
	if after := snapshot(t, d.Path()); !maps.Equal(before, after) {
		t.Errorf("the failed RemoveNode changed the directory from\n%v\nto\n%v", before, after)
	}
	if d.Cluster.Generation != 3 {
		t.Errorf("after the failed RemoveNode, the open directory is at generation %d, want 3", d.Cluster.Generation)
	}
}

func TestRemoveNodeThatCantRemoveTheNodeFileLeavesTheDirectoryAsItWas(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root writes to read-only directories")
	}
	d := clusterOf(t, "web-1", "web-2")
	if err := d.RemoveNodes([]string{"web-1"}, 3); err != nil {
		t.Fatal(err)
	}
	nodes := filepath.Join(d.Path(), "nodes")
	if err := os.Chmod(nodes, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(nodes, 0o755) })
	before := snapshot(t, d.Path())
	if err := d.RemoveNodes([]string{"web-2"}, 4); err == nil {
		t.Fatal("RemoveNode succeeded without removing the node file")
	}
	if after := snapshot(t, d.Path()); !maps.Equal(before, after) {
		t.Errorf("the failed RemoveNode changed the directory from\n%v\nto\n%v", before, after)
	}
}

// snapshot is every regular file under dir, by its path relative to
// dir, with its content.
func snapshot(t *testing.T, dir string) map[string]string {
	t.Helper()
	files := map[string]string{}
	err := filepath.WalkDir(dir, func(path string, e fs.DirEntry, err error) error {
		if err != nil || !e.Type().IsRegular() {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		files[rel] = readFile(t, path)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}

func TestARemovedIdentitiesFileItCantUseIsRefused(t *testing.T) {
	fp := "sha256:" + strings.Repeat("a", 64)
	for name, c := range map[string]struct{ content, says string }{
		"no node":        {"- identity: " + fp + "\n", "names no node"},
		"a bad name":     {"- node: Web_1\n  identity: " + fp + "\n", "Web_1"},
		"no fingerprint": {"- node: web-1\n", "isn't sha256:"},
		"a bad one":      {"- node: web-1\n  identity: sha256:aa\n", "isn't sha256:"},
		"two names":      {"- node: web-1\n  identity: " + fp + "\n- node: web-2\n  identity: " + fp + "\n", "edit the file"},
		"an odd field":   {"- node: web-1\n  identity: " + fp + "\n  address: x\n", "address"},
		"garbage":        {"{", "removed-identities.yml"},
	} {
		t.Run(name, func(t *testing.T) {
			d := newCluster(t)
			if err := os.WriteFile(filepath.Join(d.Path(), cluster.RemovedFileName), []byte(c.content), 0o600); err != nil {
				t.Fatal(err)
			}
			if removed, err := d.RemovedIdentities(); err == nil || !strings.Contains(err.Error(), c.says) {
				t.Errorf("RemovedIdentities: %v, %v, want an error saying %q", removed, err, c.says)
			}
		})
	}
}

func TestRemoveNodesTakesSeveralAtOneGeneration(t *testing.T) {
	d := clusterOf(t, "web-1", "web-2", "web-3")
	if err := d.RemoveNodes([]string{"web-1", "web-3"}, 3); err != nil {
		t.Fatal(err)
	}
	reopened, err := cluster.Open(d.Path())
	if err != nil {
		t.Fatal(err)
	}
	if reopened.Cluster.Generation != 3 {
		t.Errorf("the directory is at generation %d, want 3", reopened.Cluster.Generation)
	}
	nodes, err := reopened.Nodes()
	if err != nil || len(nodes) != 1 || nodes[0].Name != "web-2" {
		t.Errorf("the nodes are %v, %v, want web-2 alone", nodes, err)
	}
	removed, err := reopened.RemovedIdentities()
	if err != nil || len(removed) != 2 || removed[0].Node != "web-1" || removed[1].Node != "web-3" {
		t.Errorf("the removed identities are %v, %v, want web-1's and web-3's", removed, err)
	}
}

func TestRemoveNodesOfANameItLacksChangesNothing(t *testing.T) {
	d := clusterOf(t, "web-1", "web-2")
	before := snapshot(t, d.Path())
	if err := d.RemoveNodes([]string{"web-1", "web-9"}, 3); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("RemoveNodes: %v, want fs.ErrNotExist", err)
	}
	if after := snapshot(t, d.Path()); !maps.Equal(before, after) {
		t.Errorf("the failed RemoveNodes changed the directory from\n%v\nto\n%v", before, after)
	}
}

func TestRemoveNodesThatCantWriteTheGenerationPutsEveryNodeBack(t *testing.T) {
	d := clusterOf(t, "web-1", "web-2", "web-3")
	clusterFile := filepath.Join(d.Path(), "cluster.yml")
	content := readFile(t, clusterFile)
	if err := os.Remove(clusterFile); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(clusterFile, "keep"), content)
	before := snapshot(t, d.Path())
	if err := d.RemoveNodes([]string{"web-1", "web-2"}, 4); err == nil {
		t.Fatal("RemoveNodes succeeded without writing the cluster file")
	}
	if after := snapshot(t, d.Path()); !maps.Equal(before, after) {
		t.Errorf("the failed RemoveNodes changed the directory from\n%v\nto\n%v", before, after)
	}
}

// The second removal of a name fails, with the first gone.
func TestRemoveNodesThatFailsPartwayPutsBackWhatItRemoved(t *testing.T) {
	d := clusterOf(t, "web-1", "web-2")
	before := snapshot(t, d.Path())
	if err := d.RemoveNodes([]string{"web-1", "web-1"}, 3); err == nil {
		t.Fatal("RemoveNodes of a name twice succeeded")
	}
	if after := snapshot(t, d.Path()); !maps.Equal(before, after) {
		t.Errorf("the failed RemoveNodes changed the directory from\n%v\nto\n%v", before, after)
	}
}

// A crash between the list's write and the node file's removal leaves
// the identity listed with the node still there.
func TestARerunOfARemovalCutShortListsTheIdentityOnce(t *testing.T) {
	d := clusterOf(t, "web-1", "web-2")
	n, err := d.Node("web-2")
	if err != nil {
		t.Fatal(err)
	}
	entry := "- node: web-2\n  identity: " + n.Identity + "\n"
	if err := os.WriteFile(filepath.Join(d.Path(), cluster.RemovedFileName), []byte(entry), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := d.RemoveNodes([]string{"web-2"}, 5); err != nil {
		t.Fatalf("the rerun: %v", err)
	}
	removed, err := d.RemovedIdentities()
	if err != nil || !slices.Equal(removed, []cluster.RemovedIdentity{{Node: "web-2", Identity: n.Identity}}) {
		t.Errorf("the removed identities are %v, %v", removed, err)
	}
}

// A file a crash or a merge left with one entry twice is read as it is.
func TestAnIdentityListedTwiceUnderItsNameIsRead(t *testing.T) {
	d := newCluster(t)
	entry := "- node: web-1\n  identity: sha256:" + strings.Repeat("a", 64) + "\n"
	if err := os.WriteFile(filepath.Join(d.Path(), cluster.RemovedFileName), []byte(entry+entry), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := d.RemovedIdentities(); err != nil {
		t.Errorf("RemovedIdentities: %v", err)
	}
}

func FuzzRemovedIdentities(f *testing.F) {
	f.Add("- node: web-1\n  identity: sha256:" + strings.Repeat("a", 64) + "\n")
	f.Add("- {}\n")
	f.Add("- node: !!binary /w==\n")
	f.Fuzz(func(t *testing.T, content string) {
		d := newCluster(t)
		if err := os.WriteFile(filepath.Join(d.Path(), cluster.RemovedFileName), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		removed, err := d.RemovedIdentities()
		if err != nil {
			return
		}
		for _, r := range removed {
			if err := cluster.CheckName(r.Node); err != nil {
				t.Fatalf("read the node %q: %v", r.Node, err)
			}
		}
	})
}
