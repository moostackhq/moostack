package cli_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/netip"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/crypto/ssh"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/buildid"
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
			Name: name, Address: name + ".invalid", User: "root", HostKey: key,
			PrivateAddress: netip.AddrFrom4([4]byte{10, 0, 0, byte(i + 1)}), Architecture: "amd64",
			Identity: identityOf(name),
		}, 1)
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

// asContainers are the cluster services as a node reports them: each a
// container that runs the release's image of the service at its version.
func asContainers(services []*nodev1.Component) []*nodev1.ContainerStatus {
	var containers []*nodev1.ContainerStatus
	for _, s := range services {
		containers = append(containers, &nodev1.ContainerStatus{Name: release.ContainerName(s.Name), Image: release.ImageRef(s.Name, s.Version), Digest: s.Build,
			State: nodev1.TaskState_TASK_STATE_RUNNING, Health: s.Health, Problem: s.Problem})
	}
	return containers
}

const (
	healthy   = nodev1.Health_HEALTH_HEALTHY
	unhealthy = nodev1.Health_HEALTH_UNHEALTHY
)

func TestClusterStatusShowsEachPinnedComponentOfEachNode(t *testing.T) {
	o := newOperator(t, "op")
	source := statusCluster(t, o, "node1", "node2", "node3")
	o.nodes = map[string]*fakeNode{
		// Up to date, apart from containerd.
		"node1": {status: nodeclient.Status{Mode: nodev1.Mode_MODE_DIRECT, Generation: 1, Components: []*nodev1.Component{
			component("agent", "1.0.0", healthy, ""),
			component("containerd", "1.9.0", healthy, ""),
			component("updater", "1.0.0", healthy, ""),
		}}},
		// The agent is stopped, so only the updater answers.
		"node2": {status: nodeclient.Status{AgentErr: errors.New("connection refused"), Components: []*nodev1.Component{
			component("agent", "", unhealthy, "the agent doesn't answer: connection refused"),
			component("updater", "1.0.0", healthy, ""),
		}}},
		"node3": {err: errors.New("dial tcp: no route to host")},
	}
	r := o.mustRun("--release-source", source, "cluster", "status")
	want := `Cluster prod on release 1.0.0
Operation lock: free on the nodes whose agent answered
Update: stopped partway; run mooctl cluster update again to finish it

NODE          RUNNING  PINNED  HEALTH
node1  direct
  agent       1.0.0    1.0.0   healthy
  updater     1.0.0    1.0.0   healthy
  containerd  1.9.0    2.0.0   healthy
node2  mode unknown
  agent       -        1.0.0   unhealthy
  updater     1.0.0    1.0.0   healthy
  containerd  -        2.0.0   unknown
node3  unreachable

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
	o.nodes = map[string]*fakeNode{"node1": {status: nodeclient.Status{Mode: nodev1.Mode_MODE_DIRECT, Generation: 1, Components: []*nodev1.Component{
		component("agent", "1.0.0", healthy, ""),
		component("updater", "1.0.0", healthy, ""),
		component("containerd", "2.0.0", healthy, ""),
		component("extra", "0.1.0", unhealthy, "it is broken"),
	}}}}
	r := o.mustRun("--release-source", source, "cluster", "status")
	want := `Cluster prod on release 1.0.0
Operation lock: free
Update: none needed

NODE          RUNNING  PINNED  HEALTH
node1  direct
  agent       1.0.0    1.0.0   healthy
  updater     1.0.0    1.0.0   healthy
  containerd  2.0.0    2.0.0   healthy
  extra       0.1.0    -       unhealthy

node1: extra: it is broken
`
	if r.stdout != want {
		t.Errorf("cluster status printed\n%s\nwant\n%s", r.stdout, want)
	}
}

func TestClusterStatusPrintsJSON(t *testing.T) {
	o := newOperator(t, "op")
	source := statusCluster(t, o, "node1", "node2", "node3")
	o.nodes = map[string]*fakeNode{
		"node1": {status: nodeclient.Status{Mode: nodev1.Mode_MODE_DIRECT, Generation: 1, Components: []*nodev1.Component{
			component("agent", "1.0.0", healthy, ""),
			component("updater", "1.0.0", unhealthy, "it is stuck"),
		}}},
		"node2": {err: errors.New("dial tcp: no route to host")},
		// Only the updater answers, so the mode and generation aren't
		// known.
		"node3": {status: nodeclient.Status{AgentErr: errors.New("connection refused"), Components: []*nodev1.Component{
			component("agent", "", unhealthy, "the agent doesn't answer: connection refused"),
			component("updater", "1.0.0", healthy, ""),
		}}},
	}
	r := o.mustRun("--release-source", source, "cluster", "status", "--json")
	want := `{
  "cluster": "prod",
  "release": "1.0.0",
  "generation": 1,
  "update": "stopped_partway",
  "nodes": [
    {
      "name": "node1",
      "reachable": true,
      "mode": "direct",
      "generation": 1,
      "components": [
        {
          "name": "agent",
          "running": "1.0.0",
          "pinned": "1.0.0",
          "pinned_build": "{agent}",
          "health": "healthy"
        },
        {
          "name": "updater",
          "running": "1.0.0",
          "pinned": "1.0.0",
          "pinned_build": "{updater}",
          "health": "unhealthy",
          "problem": "it is stuck"
        },
        {
          "name": "containerd",
          "pinned": "2.0.0",
          "pinned_build": "{containerd}",
          "health": "unknown"
        }
      ]
    },
    {
      "name": "node2",
      "reachable": false,
      "error": "dial tcp: no route to host"
    },
    {
      "name": "node3",
      "reachable": true,
      "components": [
        {
          "name": "agent",
          "pinned": "1.0.0",
          "pinned_build": "{agent}",
          "health": "unhealthy",
          "problem": "the agent doesn't answer: connection refused"
        },
        {
          "name": "updater",
          "running": "1.0.0",
          "pinned": "1.0.0",
          "pinned_build": "{updater}",
          "health": "healthy"
        },
        {
          "name": "containerd",
          "pinned": "2.0.0",
          "pinned_build": "{containerd}",
          "health": "unknown"
        }
      ]
    }
  ]
}
`
	want = strings.NewReplacer("{agent}", fileBuild("agent"), "{updater}", fileBuild("updater"), "{containerd}", fileBuild("containerd")).Replace(want)
	if r.stdout != want {
		t.Errorf("cluster status --json printed\n%s\nwant\n%s", r.stdout, want)
	}
}

func TestClusterStatusReachesEveryNodeAtOnce(t *testing.T) {
	o := newOperator(t, "op")
	source := statusCluster(t, o, "node1", "node2", "node3")
	o.nodes = map[string]*fakeNode{"node1": healthyNode(1), "node2": healthyNode(1), "node3": healthyNode(1)}
	barrier(t, o.nodes)
	o.mustRun("--release-source", source, "cluster", "status")
}

func TestClusterStatusSaysWhenANodeAppliedALaterGeneration(t *testing.T) {
	o := newOperator(t, "op")
	source := statusCluster(t, o, "node1", "node2")
	o.nodes = map[string]*fakeNode{"node1": healthyNode(1), "node2": healthyNode(4)}
	r := o.mustRun("--release-source", source, "cluster", "status")
	wantContains(t, "cluster status", r.stdout, "\nnode2: it has applied generation 4, later than this cluster directory's 1\n")
	if strings.Contains(r.stdout, "node1: ") {
		t.Errorf("cluster status gives a problem for node1, which is at the directory's generation:\n%s", r.stdout)
	}
}

func TestClusterStatusSaysWhenANodeIsBehindTheClusterDirectory(t *testing.T) {
	o := newOperator(t, "op")
	source := statusCluster(t, o, "node1", "node2")
	o.nodes = map[string]*fakeNode{"node1": healthyNode(1), "node2": healthyNode(0)}
	r := o.mustRun("--release-source", source, "cluster", "status")
	wantContains(t, "cluster status", r.stdout, "\nnode2: it has applied generation 0, earlier than this cluster directory's 1, until an operation that changes the cluster directory records its generation\n")
	if strings.Contains(r.stdout, "node1: ") {
		t.Errorf("cluster status gives a problem for node1, which is at the directory's generation:\n%s", r.stdout)
	}
}

func TestClusterStatusSaysWhenANodeCantTellItsGeneration(t *testing.T) {
	o := newOperator(t, "op")
	source := statusCluster(t, o, "node1", "node2")
	o.nodes = map[string]*fakeNode{
		"node1": healthyNode(1),
		"node2": {status: nodeclient.Status{Mode: nodev1.Mode_MODE_DIRECT, GenerationUnknown: "reading /var/lib/moostack/generation: invalid character"}},
	}
	r := o.mustRun("--release-source", source, "cluster", "status")
	wantContains(t, "cluster status", r.stdout, "\nnode2: it can't tell the generation it has applied: reading /var/lib/moostack/generation: invalid character\n")
	r = o.mustRun("--release-source", source, "cluster", "status", "--json")
	wantContains(t, "cluster status --json", r.stdout, `"generation_problem": "it can't tell the generation it has applied: reading /var/lib/moostack/generation: invalid character"`)
}

func TestClusterStatusJSONSaysHowEachNodesGenerationDiffers(t *testing.T) {
	o := newOperator(t, "op")
	source := statusCluster(t, o, "node1", "node2", "node3")
	o.nodes = map[string]*fakeNode{"node1": healthyNode(1), "node2": healthyNode(0), "node3": healthyNode(4)}
	r := o.mustRun("--release-source", source, "cluster", "status", "--json")
	wantContains(t, "cluster status --json", r.stdout,
		`"generation_problem": "it has applied generation 0, earlier than this cluster directory's 1`,
		`"generation_problem": "it has applied generation 4, later than this cluster directory's 1"`)
	if n := strings.Count(r.stdout, "generation_problem"); n != 2 {
		t.Errorf("cluster status --json gives %d generation problems, want 2, for node2 and node3:\n%s", n, r.stdout)
	}
}

// fileBuild is the build of the component of the one file name whose
// content is its name, as statusCluster writes it.
func fileBuild(name string) string {
	sum := sha256.Sum256([]byte(name))
	return buildid.FromSums(map[string]string{name: hex.EncodeToString(sum[:])})
}
