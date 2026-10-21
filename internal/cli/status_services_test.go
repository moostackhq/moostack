package cli_test

import (
	"errors"
	"path/filepath"
	"testing"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/nodeclient"
	"github.com/moostackhq/moostack/internal/release"
)

// pinServices makes release 1.0.0 in source pin the registry and DNS at
// 1.0.0, besides its node components.
func pinServices(t *testing.T, source string) {
	t.Helper()
	dir := filepath.Join(source, "1.0.0")
	for _, arch := range release.Architectures {
		for _, f := range []string{"registry.tar", "dns.tar"} {
			writeFile(t, filepath.Join(dir, "linux-"+arch, f), []byte(f))
		}
	}
	err := release.Write(dir, release.Manifest{
		Version: "1.0.0",
		Components: []release.Component{
			{Name: "agent", Version: "1.0.0", Files: []string{"agent"}},
			{Name: "updater", Version: "1.0.0", Files: []string{"updater"}},
			{Name: "containerd", Version: "2.0.0", Files: []string{"containerd"}},
		},
		Services: []release.Service{
			{Name: "registry", Version: "1.0.0", API: "moostack.registry.v1.Registry", Image: "registry.tar"},
			{Name: "dns", Version: "1.0.0", API: "moostack.dns.v1.DNS", Image: "dns.tar"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
}

// upToDate are the node components of release 1.0.0, healthy.
func upToDate() []*nodev1.Component {
	return []*nodev1.Component{
		component("agent", "1.0.0", healthy, ""),
		component("updater", "1.0.0", healthy, ""),
		component("containerd", "2.0.0", healthy, ""),
	}
}

func TestClusterStatusShowsEachPinnedServiceOfEachNode(t *testing.T) {
	o := newOperator(t, "op")
	source := statusCluster(t, o, "node1", "node2", "node3")
	pinServices(t, source)
	o.nodes = map[string]*fakeNode{
		// DNS is behind, on a release that 1.0.0 doesn't update, and
		// stopped, and a service the release doesn't pin runs too.
		"node1": {status: nodeclient.Status{Mode: nodev1.Mode_MODE_DIRECT, Generation: 1, Components: upToDate(), Services: []*nodev1.Component{
			component("dns", "0.9.0", unhealthy, "the service doesn't answer: connection refused"),
			component("registry", "1.0.0", healthy, ""),
			component("extra", "0.1.0", healthy, ""),
		}}},
		// Only the updater answers, so the services aren't known.
		"node2": {status: nodeclient.Status{AgentErr: errors.New("connection refused"), Components: []*nodev1.Component{
			component("agent", "", unhealthy, "the agent doesn't answer: connection refused"),
			component("updater", "1.0.0", healthy, ""),
		}}},
		"node3": {err: errors.New("dial tcp: no route to host")},
	}
	r := o.mustRun("--release-source", source, "cluster", "status")
	want := `Cluster prod on release 1.0.0
Operation lock: free on the nodes whose agent answered
Update: refused, so cluster update won't run:
node1: its dns service runs release 0.9.0, and release 1.0.0 updates nodes on 1.0.0 only
Pin the release the nodes run, or the one after it, and run the mooctl of that release.

NODE   REACHABLE  MODE    COMPONENT   RUNNING  PINNED  HEALTH
node1  yes        direct  agent       1.0.0    1.0.0   healthy
node1  yes        direct  updater     1.0.0    1.0.0   healthy
node1  yes        direct  containerd  2.0.0    2.0.0   healthy
node1  yes        direct  registry    1.0.0    1.0.0   healthy
node1  yes        direct  dns         0.9.0    1.0.0   unhealthy
node1  yes        direct  extra       0.1.0    -       healthy
node2  yes        -       agent       -        1.0.0   unhealthy
node2  yes        -       updater     1.0.0    1.0.0   healthy
node2  yes        -       containerd  -        2.0.0   unknown
node2  yes        -       registry    -        1.0.0   unknown
node2  yes        -       dns         -        1.0.0   unknown
node3  no         -       -           -        -       -

node1: dns: the service doesn't answer: connection refused
node2: agent: the agent doesn't answer: connection refused
node3: dial tcp: no route to host
`
	if r.stdout != want {
		t.Errorf("cluster status printed\n%s\nwant\n%s", r.stdout, want)
	}
}

func TestClusterStatusJSONListsTheServicesApart(t *testing.T) {
	o := newOperator(t, "op")
	source := statusCluster(t, o, "node1")
	pinServices(t, source)
	o.nodes = map[string]*fakeNode{"node1": {status: nodeclient.Status{Mode: nodev1.Mode_MODE_DIRECT, Generation: 1, Components: upToDate(), Services: []*nodev1.Component{
		component("registry", "1.0.0", healthy, ""),
	}}}}
	r := o.mustRun("--release-source", source, "cluster", "status", "--json")
	wantContains(t, "cluster status --json", r.stdout, `      "services": [
        {
          "name": "registry",
          "running": "1.0.0",
          "pinned": "1.0.0",
          "health": "healthy"
        },
        {
          "name": "dns",
          "pinned": "1.0.0",
          "health": "unknown"
        }
      ]`)
}
