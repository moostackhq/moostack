package cli_test

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/nodeclient"
	"github.com/moostackhq/moostack/internal/release"
)

// running, exited and failed are containers as a node reports them.
func running(name, image string, health nodev1.Health, problem string) *nodev1.ContainerStatus {
	return &nodev1.ContainerStatus{Name: name, Image: image, Digest: "sha256:" + name, State: nodev1.TaskState_TASK_STATE_RUNNING, Health: health, Problem: problem}
}

func exited(name, image string, code uint32) *nodev1.ContainerStatus {
	return &nodev1.ContainerStatus{Name: name, Image: image, Digest: "sha256:" + name, State: nodev1.TaskState_TASK_STATE_EXITED, ExitCode: code}
}

func failed(name, image, problem string) *nodev1.ContainerStatus {
	return &nodev1.ContainerStatus{Name: name, Image: image, State: nodev1.TaskState_TASK_STATE_FAILED, Problem: problem}
}

// containersStatus is the status of a node that runs the release 1.0.0
// of pinServices, with the cluster services as containers, and apps
// beside them: one running, one exited and one that failed.
func containersStatus() nodeclient.Status {
	return nodeclient.Status{Mode: nodev1.Mode_MODE_DIRECT, Generation: 1, Components: upToDate(),
		Containers: []*nodev1.ContainerStatus{
			running("app-api", "docker.io/library/httpd:2.4", nodev1.Health_HEALTH_UNSPECIFIED, ""),
			exited("app-crash", "moostack.local/crash:20261024T153000Z", 3),
			failed("app-web", "docker.io/library/nginx:1.27", "starting it: the node has no image docker.io/library/nginx:1.27"),
			running("dns", release.ImageRef("dns", "1.0.0"), healthy, ""),
			running("registry", release.ImageRef("registry", "1.0.0"), healthy, ""),
		}}
}

func TestClusterStatusListsEachNodesContainersFlatUnderTheAgent(t *testing.T) {
	o := newOperator(t, "op")
	source := statusCluster(t, o, "node1", "node2")
	pinServices(t, source)
	o.nodes = map[string]*fakeNode{
		"node1": {status: containersStatus()},
		// A node that runs only the cluster services, one of another release.
		"node2": {status: nodeclient.Status{Mode: nodev1.Mode_MODE_DIRECT, Generation: 1, Components: upToDate(), Containers: []*nodev1.ContainerStatus{
			running("registry", release.ImageRef("registry", "1.0.0"), healthy, ""),
			running("dns", release.ImageRef("dns", "0.9.0"), unhealthy, "the service doesn't answer"),
		}}},
	}
	want := `Cluster prod on release 1.0.0
Operation lock: free
Update: refused, so cluster update won't run:
node2: its dns service runs release 0.9.0, and release 1.0.0 updates nodes on 1.0.0 only
Pin the release the nodes run, or the one after it, and run the mooctl of that release.

NODE           RUNNING                                PINNED  HEALTH
node1  direct
  agent        1.0.0                                  1.0.0   healthy
    registry   1.0.0                                  1.0.0   healthy
    dns        1.0.0                                  1.0.0   healthy
    app-api    docker.io/library/httpd:2.4            -       running
    app-crash  moostack.local/crash:20261024T153000Z  -       exited 3
    app-web    docker.io/library/nginx:1.27           -       failed
  updater      1.0.0                                  1.0.0   healthy
  containerd   2.0.0                                  2.0.0   healthy
node2  direct
  agent        1.0.0                                  1.0.0   healthy
    registry   1.0.0                                  1.0.0   healthy
    dns        0.9.0                                  1.0.0   unhealthy
  updater      1.0.0                                  1.0.0   healthy
  containerd   2.0.0                                  2.0.0   healthy

node1: app-web: starting it: the node has no image docker.io/library/nginx:1.27
node2: dns: the service doesn't answer
`
	piped := o.mustRun("--release-source", source, "cluster", "status")
	if piped.stdout != want {
		t.Errorf("cluster status printed\n%s\nwant\n%s", piped.stdout, want)
	}
	o.terminal = &scriptedTerminal{}
	if onTerminal := o.mustRun("--release-source", source, "cluster", "status"); onTerminal.stdout != want {
		t.Errorf("cluster status on a terminal printed\n%s\nwant\n%s", onTerminal.stdout, want)
	}
}

// A pinned cluster service's container that runs another version, or
// none at all, shows as the service's would, and one the node lacks is
// of unknown health.
func TestClusterStatusShowsAPinnedContainerByItsVersion(t *testing.T) {
	o := newOperator(t, "op")
	source := statusCluster(t, o, "node1")
	pinServices(t, source)
	s := containersStatus()
	s.Containers = []*nodev1.ContainerStatus{
		running("dns", release.ImageRef("dns", "0.9.0"), unhealthy, "the service doesn't answer"),
		running("app-web", "docker.io/library/nginx:1.27", nodev1.Health_HEALTH_UNSPECIFIED, ""),
	}
	o.nodes = map[string]*fakeNode{"node1": {status: s}}
	r := o.mustRun("--release-source", source, "cluster", "status")
	wantContains(t, "cluster status", r.stdout, `
  agent       1.0.0                         1.0.0   healthy
    registry  -                             1.0.0   unknown
    dns       0.9.0                         1.0.0   unhealthy
    app-web   docker.io/library/nginx:1.27  -       running
`, "\nnode1: dns: the service doesn't answer\n")
}

// Apps take no part in whether the nodes run the pinned release.
func TestClusterStatusNeedsNoUpdateWhateverTheAppsDo(t *testing.T) {
	o := newOperator(t, "op")
	source := statusCluster(t, o, "node1")
	pinServices(t, source)
	o.nodes = map[string]*fakeNode{"node1": {status: containersStatus()}}
	r := o.mustRun("--release-source", source, "cluster", "status")
	wantContains(t, "cluster status", r.stdout, "Update: none needed\n")
}

func TestClusterStatusJSONListsTheContainersOfEachNode(t *testing.T) {
	o := newOperator(t, "op")
	source := statusCluster(t, o, "node1")
	pinServices(t, source)
	o.nodes = map[string]*fakeNode{"node1": {status: containersStatus()}}
	r := o.mustRun("--release-source", source, "cluster", "status", "--json")
	var view struct {
		Nodes []struct {
			Name       string
			Services   []any
			Containers []map[string]any
		}
	}
	if err := json.Unmarshal([]byte(r.stdout), &view); err != nil {
		t.Fatal(err)
	}
	if len(view.Nodes) != 1 || view.Nodes[0].Services != nil {
		t.Fatalf("cluster status --json shows the nodes %v, want node1 with containers in place of services", view.Nodes)
	}
	got, err := json.MarshalIndent(view.Nodes[0].Containers, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	want := `[
  {
    "build": "sha256:registry",
    "health": "healthy",
    "image": "moostack.local/registry:1.0.0",
    "name": "registry",
    "pinned": "1.0.0",
    "running": "1.0.0"
  },
  {
    "build": "sha256:dns",
    "health": "healthy",
    "image": "moostack.local/dns:1.0.0",
    "name": "dns",
    "pinned": "1.0.0",
    "running": "1.0.0"
  },
  {
    "build": "sha256:app-api",
    "health": "running",
    "image": "docker.io/library/httpd:2.4",
    "name": "app-api",
    "running": "docker.io/library/httpd:2.4"
  },
  {
    "build": "sha256:app-crash",
    "health": "exited 3",
    "image": "moostack.local/crash:20261024T153000Z",
    "name": "app-crash",
    "running": "moostack.local/crash:20261024T153000Z"
  },
  {
    "health": "failed",
    "image": "docker.io/library/nginx:1.27",
    "name": "app-web",
    "problem": "starting it: the node has no image docker.io/library/nginx:1.27",
    "running": "docker.io/library/nginx:1.27"
  }
]`
	if string(got) != want {
		t.Errorf("cluster status --json shows the containers\n%s\nwant\n%s", got, want)
	}
}

// What a node reports of its containers, as it reports it of anything
// else, can't add lines to status or move the cursor.
func TestClusterStatusQuotesWhatAContainerReportsThatIsntPrintable(t *testing.T) {
	o := newOperator(t, "op")
	source := statusCluster(t, o, "node1")
	s := containersStatus()
	s.Containers = []*nodev1.ContainerStatus{
		failed("app-web", "nginx\nnode9  unreachable", "it\x1b[2Jfailed"),
	}
	o.nodes = map[string]*fakeNode{"node1": {status: s}}
	r := o.mustRun("--release-source", source, "cluster", "status")
	wantContains(t, "cluster status", r.stdout, `    app-web   "nginx\nnode9  unreachable"  -       failed`, `node1: app-web: "it\x1b[2Jfailed"`)
}

// A container a node can't read, as while its containerd is down, is of
// unknown health, not failed.
func TestClusterStatusShowsAContainerTheNodeCantReadAsUnknown(t *testing.T) {
	o := newOperator(t, "op")
	source := statusCluster(t, o, "node1")
	s := containersStatus()
	s.Containers = []*nodev1.ContainerStatus{{Name: "app-web", Problem: "reading its container: connection refused"}}
	o.nodes = map[string]*fakeNode{"node1": {status: s}}
	r := o.mustRun("--release-source", source, "cluster", "status")
	wantContains(t, "cluster status", r.stdout, "    app-web   -        -       unknown\n", "node1: app-web: reading its container: connection refused\n")
}

// A container under a pinned service's name that runs another image, as
// another service's or one named as the pinned version, runs no version
// of the service, to status as to update.
func TestClusterStatusShowsAPinnedNameRunningAnotherImageAsNoVersion(t *testing.T) {
	for _, image := range []string{"1.0.0", release.ImageRef("registry", "1.0.0")} {
		o := newOperator(t, "op")
		source := statusCluster(t, o, "node1")
		pinServices(t, source)
		s := containersStatus()
		s.Containers = []*nodev1.ContainerStatus{
			running("dns", image, healthy, ""),
			running("registry", release.ImageRef("registry", "1.0.0"), healthy, ""),
		}
		if slices.ContainsFunc(s.Services(), func(c *nodev1.Component) bool { return c.Name == "dns" }) {
			t.Errorf("%s: update takes dns for the service", image)
		}
		o.nodes = map[string]*fakeNode{"node1": {status: s}}
		r := o.mustRun("--release-source", source, "cluster", "status")
		wantContains(t, "cluster status of dns running "+image, r.stdout, "\n    dns       -        1.0.0   healthy\n")
		if strings.Contains(r.stdout, "Update: none needed") {
			t.Errorf("%s: cluster status says no update is needed:\n%s", image, r.stdout)
		}
	}
}
