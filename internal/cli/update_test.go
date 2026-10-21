package cli_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"maps"
	"net/netip"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"golang.org/x/crypto/ssh"

	"github.com/moostackhq/moostack/internal/cluster"
	"github.com/moostackhq/moostack/internal/release"
)

// updateCluster makes the operator's cluster directory, on release
// 1.0.0, with node1 and node3 on amd64 and node2 on arm64, all healthy at
// generation 1, and a release source with 1.0.0 and 1.1.0, each of whose
// agents says its architecture and version. It pins 1.1.0, as an
// operator does before cluster update, and returns the release source.
func updateCluster(t *testing.T, o *operator) string {
	t.Helper()
	o.version = "1.0.0"
	o.mustRun("cluster", "init", "--ssh-key", o.key, "--name", "prod")
	d, err := cluster.Open(o.dir)
	if err != nil {
		t.Fatal(err)
	}
	o.nodes = map[string]*fakeNode{}
	for i, n := range []struct{ name, arch string }{{"node1", "amd64"}, {"node2", "arm64"}, {"node3", "amd64"}} {
		pub, _, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		key, err := ssh.NewPublicKey(pub)
		if err != nil {
			t.Fatal(err)
		}
		err = d.AddNode(cluster.Node{
			Name: n.name, Address: n.name + ".invalid", User: "root", HostKey: key,
			PrivateAddress: netip.AddrFrom4([4]byte{10, 0, 0, byte(i + 1)}), Architecture: n.arch,
			Identity: identityOf(n.name),
		}, 1)
		if err != nil {
			t.Fatal(err)
		}
		o.nodes[n.name] = healthyNode(1)
	}
	source := t.TempDir()
	for _, version := range []string{"1.0.0", "1.1.0"} {
		dir := filepath.Join(source, version)
		for _, arch := range release.Architectures {
			for _, f := range []string{"agent", "updater"} {
				writeFile(t, filepath.Join(dir, "linux-"+arch, f), []byte(f+" for "+arch+" at "+version))
			}
		}
		err = release.Write(dir, release.Manifest{Version: version, Components: []release.Component{
			{Name: "agent", Version: version, Files: []string{"agent"}},
			{Name: "updater", Version: version, Files: []string{"updater"}},
		}})
		if err != nil {
			t.Fatal(err)
		}
	}
	pin(t, o, "1.1.0")
	return source
}

// pin pins version in the operator's cluster file, as an operator edits
// it.
func pin(t *testing.T, o *operator, version string) {
	t.Helper()
	path := filepath.Join(o.dir, cluster.FileName)
	data := readFile(t, path)
	d, err := cluster.Open(o.dir)
	if err != nil {
		t.Fatal(err)
	}
	pinned := strings.Replace(data, "release: "+d.Cluster.Release, "release: "+version, 1)
	if pinned == data {
		t.Fatalf("%s doesn't pin %s:\n%s", path, d.Cluster.Release, data)
	}
	writeFile(t, path, []byte(pinned))
}

func TestClusterUpdateSendsEachNodeThePinnedAgentForItsArchitecture(t *testing.T) {
	o := newOperator(t, "op")
	source := updateCluster(t, o)
	before := snapshot(t, o.dir)
	r := o.mustRun("--release-source", source, "cluster", "update")
	for name, want := range map[string]string{"node1": "amd64", "node2": "arm64", "node3": "amd64"} {
		if got := o.nodes[name].agentUpdates(); !slices.Equal(got, []string{"1.1.0 agent for " + want + " at 1.1.0"}) {
			t.Errorf("%s's agent was updated to %q, want once to 1.1.0's agent for %s", name, got, want)
		}
		if l := o.nodes[name].heldLock(); l != nil {
			t.Errorf("after cluster update, %s holds the lock %+v", name, l)
		}
		if g := o.nodes[name].generations(); len(g) > 0 {
			t.Errorf("cluster update recorded the generations %v on %s, which it doesn't change", g, name)
		}
		if n := len(o.nodes[name].granted()); n != 1 {
			t.Errorf("%s granted the lock %d times, want once, in the preflight", name, n)
		}
	}
	want := "Updating the agent on node1 to 1.1.0\nUpdating the agent on node2 to 1.1.0\nUpdating the agent on node3 to 1.1.0\nEvery node runs the agent of release 1.1.0.\n"
	if r.stdout != want {
		t.Errorf("cluster update printed\n%s\nwant\n%s", r.stdout, want)
	}
	if after := snapshot(t, o.dir); !maps.Equal(before, after) {
		t.Error("cluster update changed the cluster directory")
	}
}

func TestClusterUpdateStopsAtTheFirstNodeThatFailsNamingIt(t *testing.T) {
	o := newOperator(t, "op")
	source := updateCluster(t, o)
	o.nodes["node2"].updateErr = errors.New("the new agent didn't become healthy within 30s; rolled back to the agent it replaced")
	r := o.run("--release-source", source, "cluster", "update")
	if r.err == nil {
		t.Fatal("cluster update succeeded")
	}
	wantContains(t, "cluster update's error", r.err.Error(), "node2: updating the agent to 1.1.0: the new agent didn't become healthy within 30s; rolled back")
	if n := len(o.nodes["node1"].agentUpdates()); n != 1 {
		t.Errorf("node1 was updated %d times, want once", n)
	}
	if n := len(o.nodes["node3"].agentUpdates()); n != 0 {
		t.Errorf("node3 was updated %d times after node2 failed, want never", n)
	}
	for name, n := range o.nodes {
		if l := n.heldLock(); l != nil {
			t.Errorf("after the failed update, %s holds the lock %+v", name, l)
		}
	}
}

func TestClusterUpdateUpdatesNothingWhenThePreflightFails(t *testing.T) {
	o := newOperator(t, "op")
	source := updateCluster(t, o)
	o.nodes["node3"].locked(bobsLock)
	r := o.run("--release-source", source, "cluster", "update")
	if r.err == nil {
		t.Fatal("cluster update succeeded on a locked cluster")
	}
	wantContains(t, "cluster update's error", r.err.Error(), "node3", "bob on desktop")
	for name, n := range o.nodes {
		if u := n.agentUpdates(); len(u) > 0 {
			t.Errorf("%s was updated to %v after the preflight failed", name, u)
		}
	}
}

func TestClusterUpdateRefusesAReleaseWithoutAnAgent(t *testing.T) {
	o := newOperator(t, "op")
	source := updateCluster(t, o)
	dir := filepath.Join(source, "1.2.0")
	for _, arch := range release.Architectures {
		writeFile(t, filepath.Join(dir, "linux-"+arch, "updater"), []byte("updater"))
	}
	if err := release.Write(dir, release.Manifest{Version: "1.2.0", Components: []release.Component{{Name: "updater", Version: "1.2.0", Files: []string{"updater"}}}}); err != nil {
		t.Fatal(err)
	}
	pin(t, o, "1.2.0")
	r := o.run("--release-source", source, "cluster", "update")
	if r.err == nil || !strings.Contains(r.err.Error(), "release 1.2.0 pins no agent") {
		t.Errorf("cluster update of a release without an agent: %v", r.err)
	}
	for name, n := range o.nodes {
		if n.dialed() > 0 {
			t.Errorf("cluster update reached %s for a release without an agent", name)
		}
	}
}

func TestClusterUpdateRefusesAnAgentThatDoesntMatchItsChecksum(t *testing.T) {
	o := newOperator(t, "op")
	source := updateCluster(t, o)
	writeFile(t, filepath.Join(source, "1.1.0", "linux-arm64", "agent"), []byte("tampered"))
	r := o.run("--release-source", source, "cluster", "update")
	if r.err == nil || !strings.Contains(r.err.Error(), "node2") || !strings.Contains(r.err.Error(), "doesn't match its checksum") {
		t.Errorf("cluster update with a tampered agent: %v", r.err)
	}
	if n := len(o.nodes["node2"].agentUpdates()); n != 0 {
		t.Errorf("node2 was sent the tampered agent")
	}
}
