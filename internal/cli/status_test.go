package cli_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"net/netip"
	"path/filepath"
	"testing"

	"golang.org/x/crypto/ssh"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/cluster"
	"github.com/moostackhq/moostack/internal/nodeclient"
	"github.com/moostackhq/moostack/internal/release"
)

// statusCluster makes the operator's cluster directory a cluster on
// release 1.0.0, which pins the agent and the updater at 1.0.0 and
// containerd at 2.0.0, with the nodes names, and returns the release
// source to pass to mooctl.
func statusCluster(t *testing.T, o *operator, names ...string) string {
	t.Helper()
	o.version = "1.0.0"
	o.mustRun("cluster", "init", "--ssh-key", o.key, "--name", "prod")
	d, err := cluster.Open(o.dir)
	if err != nil {
		t.Fatal(err)
	}
	for i, name := range names {
		pub, _, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		key, err := ssh.NewPublicKey(pub)
		if err != nil {
			t.Fatal(err)
		}
		err = d.AddNode(cluster.Node{
			Name: name, Address: name, User: "root", HostKey: key,
			PrivateAddress: netip.AddrFrom4([4]byte{10, 0, 0, byte(i + 1)}), Architecture: "amd64",
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	source := t.TempDir()
	dir := filepath.Join(source, "1.0.0")
	for _, arch := range release.Architectures {
		for _, f := range []string{"agent", "updater", "containerd"} {
			writeFile(t, filepath.Join(dir, "linux-"+arch, f), []byte(f))
		}
	}
	err = release.Write(dir, release.Manifest{Version: "1.0.0", Components: []release.Component{
		{Name: "agent", Version: "1.0.0", Files: []string{"agent"}},
		{Name: "updater", Version: "1.0.0", Files: []string{"updater"}},
		{Name: "containerd", Version: "2.0.0", Files: []string{"containerd"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	return source
}

func component(name, version string, health nodev1.Health, problem string) *nodev1.Component {
	return &nodev1.Component{Name: name, Version: version, Health: health, Problem: problem}
}

const (
	healthy   = nodev1.Health_HEALTH_HEALTHY
	unhealthy = nodev1.Health_HEALTH_UNHEALTHY
)

func TestClusterStatusShowsEachPinnedComponentOfEachNode(t *testing.T) {
	o := newOperator(t, "op")
	source := statusCluster(t, o, "node1", "node2", "node3")
	o.nodeStatus = func(_ context.Context, n cluster.Node) (nodeclient.Status, error) {
		switch n.Name {
		case "node1":
			// Up to date, apart from containerd.
			return nodeclient.Status{Mode: nodev1.Mode_MODE_DIRECT, Components: []*nodev1.Component{
				component("agent", "1.0.0", healthy, ""),
				component("containerd", "1.9.0", healthy, ""),
				component("updater", "1.0.0", healthy, ""),
			}}, nil
		case "node2":
			// The agent is stopped, so only the updater answers.
			return nodeclient.Status{Components: []*nodev1.Component{
				component("agent", "", unhealthy, "the agent doesn't answer: connection refused"),
				component("updater", "1.0.0", healthy, ""),
			}}, nil
		}
		return nodeclient.Status{}, errors.New("dial tcp: no route to host")
	}
	r := o.mustRun("--release-source", source, "cluster", "status")
	want := `Cluster prod on release 1.0.0

NODE   REACHABLE  MODE    COMPONENT   RUNNING  PINNED  HEALTH
node1  yes        direct  agent       1.0.0    1.0.0   healthy
node1  yes        direct  updater     1.0.0    1.0.0   healthy
node1  yes        direct  containerd  1.9.0    2.0.0   healthy
node2  yes        -       agent       -        1.0.0   unhealthy
node2  yes        -       updater     1.0.0    1.0.0   healthy
node2  yes        -       containerd  -        2.0.0   unknown
node3  no         -       -           -        -       -

node2: agent: the agent doesn't answer: connection refused
node3: dial tcp: no route to host
`
	if r.stdout != want {
		t.Errorf("cluster status printed\n%s\nwant\n%s", r.stdout, want)
	}
}

func TestClusterStatusShowsAComponentTheReleaseDoesntPin(t *testing.T) {
	o := newOperator(t, "op")
	source := statusCluster(t, o, "node1")
	o.nodeStatus = func(context.Context, cluster.Node) (nodeclient.Status, error) {
		return nodeclient.Status{Mode: nodev1.Mode_MODE_DIRECT, Components: []*nodev1.Component{
			component("agent", "1.0.0", healthy, ""),
			component("updater", "1.0.0", healthy, ""),
			component("containerd", "2.0.0", healthy, ""),
			component("extra", "0.1.0", unhealthy, "it is broken"),
		}}, nil
	}
	r := o.mustRun("--release-source", source, "cluster", "status")
	want := `Cluster prod on release 1.0.0

NODE   REACHABLE  MODE    COMPONENT   RUNNING  PINNED  HEALTH
node1  yes        direct  agent       1.0.0    1.0.0   healthy
node1  yes        direct  updater     1.0.0    1.0.0   healthy
node1  yes        direct  containerd  2.0.0    2.0.0   healthy
node1  yes        direct  extra       0.1.0    -       unhealthy

node1: extra: it is broken
`
	if r.stdout != want {
		t.Errorf("cluster status printed\n%s\nwant\n%s", r.stdout, want)
	}
}
