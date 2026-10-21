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

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/cluster"
	"github.com/moostackhq/moostack/internal/release"
)

// updateCluster makes the operator's cluster directory, on release
// 1.0.0, with node1 and node3 on amd64 and node2 on arm64, all healthy at
// generation 1, and a release source with 1.0.0 and 1.1.0, each of whose
// agents, updaters and containerd binaries says its architecture and
// version, and whose containerd is at 2.0.0 and 2.1.0. It pins 1.1.0, as an
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
	for version, containerd := range map[string]string{"1.0.0": "2.0.0", "1.1.0": "2.1.0"} {
		dir := filepath.Join(source, version)
		for _, arch := range release.Architectures {
			for _, f := range append([]string{"agent", "updater"}, release.ContainerdFiles...) {
				writeFile(t, filepath.Join(dir, "linux-"+arch, f), []byte(f+" for "+arch+" at "+version))
			}
		}
		err = release.Write(dir, release.Manifest{Version: version, Components: []release.Component{
			{Name: "agent", Version: version, Files: []string{"agent"}},
			{Name: "updater", Version: version, Files: []string{"updater"}},
			{Name: "containerd", Version: containerd, Files: release.ContainerdFiles},
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

func TestClusterUpdateSendsEachNodeThePinnedComponentsForItsArchitecture(t *testing.T) {
	o := newOperator(t, "op")
	source := updateCluster(t, o)
	before := snapshot(t, o.dir)
	r := o.mustRun("--release-source", source, "cluster", "update")
	for name, arch := range map[string]string{"node1": "amd64", "node2": "arm64", "node3": "amd64"} {
		want := []string{
			"agent 1.1.0 agent for " + arch + " at 1.1.0",
			"updater 1.1.0 updater for " + arch + " at 1.1.0",
			"containerd 2.1.0 containerd: containerd for " + arch + " at 1.1.0 containerd-shim-runc-v2: containerd-shim-runc-v2 for " + arch + " at 1.1.0",
		}
		if got := o.nodes[name].nodeUpdates(); !slices.Equal(got, want) {
			t.Errorf("%s was updated to\n%s\nwant\n%s", name, strings.Join(got, "\n"), strings.Join(want, "\n"))
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
	var want string
	for _, name := range []string{"node1", "node2", "node3"} {
		want += "Updating the agent on " + name + " to 1.1.0\n" +
			"Updating the updater on " + name + " to 1.1.0\n" +
			"Updating containerd on " + name + " to 2.1.0\n"
	}
	want += "Every node runs the agent, the updater and containerd of release 1.1.0.\n"
	if r.stdout != want {
		t.Errorf("cluster update printed\n%s\nwant\n%s", r.stdout, want)
	}
	if after := snapshot(t, o.dir); !maps.Equal(before, after) {
		t.Error("cluster update changed the cluster directory")
	}
}

func TestClusterUpdateSkipsTheAgentAndTheUpdaterANodeRunsAlready(t *testing.T) {
	o := newOperator(t, "op")
	source := updateCluster(t, o)
	healthy := nodev1.Health_HEALTH_HEALTHY
	o.nodes["node1"].status.Components = []*nodev1.Component{
		{Name: "agent", Version: "1.1.0", Health: healthy},
		{Name: "containerd", Version: "2.1.0", Health: nodev1.Health_HEALTH_UNHEALTHY},
		{Name: "updater", Version: "1.1.0", Health: healthy},
	}
	r := o.mustRun("--release-source", source, "cluster", "update")
	if got := o.nodes["node1"].nodeUpdates(); len(got) != 1 || !strings.HasPrefix(got[0], "containerd 2.1.0") {
		t.Errorf("node1 was updated to %q, want containerd only", got)
	}
	wantContains(t, "cluster update's output", r.stdout,
		"node1 runs the agent at 1.1.0 already\nnode1 runs the updater at 1.1.0 already\nUpdating containerd on node1 to 2.1.0\nUpdating the agent on node2 to 1.1.0\n")
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

func TestClusterUpdateRefusesAReleaseWithoutANodeComponent(t *testing.T) {
	for _, missing := range []string{"agent", "updater", "containerd"} {
		t.Run(missing, func(t *testing.T) {
			o := newOperator(t, "op")
			source := updateCluster(t, o)
			dir := filepath.Join(source, "1.2.0")
			var components []release.Component
			for _, c := range []release.Component{
				{Name: "agent", Version: "1.2.0", Files: []string{"agent"}},
				{Name: "updater", Version: "1.2.0", Files: []string{"updater"}},
				{Name: "containerd", Version: "2.2.0", Files: release.ContainerdFiles},
			} {
				if c.Name == missing {
					continue
				}
				components = append(components, c)
				for _, arch := range release.Architectures {
					for _, f := range c.Files {
						writeFile(t, filepath.Join(dir, "linux-"+arch, f), []byte(f))
					}
				}
			}
			if err := release.Write(dir, release.Manifest{Version: "1.2.0", Components: components}); err != nil {
				t.Fatal(err)
			}
			pin(t, o, "1.2.0")
			r := o.run("--release-source", source, "cluster", "update")
			if r.err == nil || !strings.Contains(r.err.Error(), "release 1.2.0 pins no "+missing) {
				t.Errorf("cluster update of a release without %s: %v", missing, r.err)
			}
			for name, n := range o.nodes {
				if n.dialed() > 0 {
					t.Errorf("cluster update reached %s for a release without %s", name, missing)
				}
			}
		})
	}
}

func TestClusterUpdateRefusesANodeComponentWithOtherFiles(t *testing.T) {
	for _, c := range []struct {
		name  string
		files map[string][]string
		want  string
	}{
		{"an updater named otherwise", map[string][]string{"updater": {"updater-x"}}, "pins the updater as updater-x"},
		{"an agent with a second file", map[string][]string{"agent": {"agent", "extra"}}, "pins the agent as agent, extra"},
		{"containerd without its shim", map[string][]string{"containerd": {"containerd"}}, "pins containerd as containerd"},
	} {
		t.Run(c.name, func(t *testing.T) {
			o := newOperator(t, "op")
			source := updateCluster(t, o)
			dir := filepath.Join(source, "1.2.0")
			files := map[string][]string{"agent": {"agent"}, "updater": {"updater"}, "containerd": release.ContainerdFiles}
			for name, f := range c.files {
				files[name] = f
			}
			var components []release.Component
			for _, name := range []string{"agent", "updater", "containerd"} {
				components = append(components, release.Component{Name: name, Version: "1.2.0", Files: files[name]})
				for _, arch := range release.Architectures {
					for _, f := range files[name] {
						writeFile(t, filepath.Join(dir, "linux-"+arch, f), []byte(f))
					}
				}
			}
			if err := release.Write(dir, release.Manifest{Version: "1.2.0", Components: components}); err != nil {
				t.Fatal(err)
			}
			pin(t, o, "1.2.0")
			r := o.run("--release-source", source, "cluster", "update")
			if r.err == nil || !strings.Contains(r.err.Error(), c.want) {
				t.Errorf("cluster update of a release with %s: %v, want it refused saying %q", c.name, r.err, c.want)
			}
			for name, n := range o.nodes {
				if n.dialed() > 0 {
					t.Errorf("cluster update reached %s for a release with %s", name, c.name)
				}
			}
		})
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
