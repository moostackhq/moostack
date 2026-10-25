package cli_test

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/release"
)

// onOldNames has each of the operator's nodes run release 1.0.0 with its
// services under their names alone, as releases before the service-
// names ran them.
func onOldNames(o *operator) {
	for _, n := range o.nodes {
		n.runs("1.0.0", "agent", "updater").runs("2.0.0", "containerd").runsUnderOldNames("1.0.0", "dns", "proxy")
	}
}

// containerNames are the names of the containers that n reports.
func containerNames(t *testing.T, n *fakeNode) []string {
	var names []string
	for _, c := range n.Status(t.Context()).Containers {
		names = append(names, c.Name)
	}
	return names
}

// One cluster update renames the services: on each node, each service's
// old container goes before it runs under its service- name, since no
// two containers serve one API, and the steps print as any service's
// update does. A rerun changes nothing.
func TestClusterUpdateRenamesTheServicesOfEachNode(t *testing.T) {
	o := newOperator(t, "op")
	source := updateCluster(t, o)
	onOldNames(o)
	r := o.mustRun("--release-source", source, "cluster", "update")
	for _, name := range []string{"node1", "node2", "node3"} {
		n := o.nodes[name]
		want := []string{"agent 1.1.0", "updater 1.1.0", "containerd 2.1.0", "remove dns", "run service-dns 1.1.0", "remove proxy", "run service-proxy 1.1.0"}
		if got := updatedTo(n); !slices.Equal(got, want) {
			t.Errorf("%s was updated to %q, want %q", name, got, want)
		}
		if got, want := containerNames(t, n), []string{"service-dns", "service-proxy"}; !slices.Equal(got, want) {
			t.Errorf("%s runs %q, want %q", name, got, want)
		}
	}
	wantContains(t, "cluster update's output", r.stdout,
		"Updating the dns service on node1 to 1.1.0\nUpdating the proxy service on node1 to 1.1.0\nUpdating the dns service on node2 to 1.1.0\n",
		"Every node runs release 1.1.0.\n")
	status := o.mustRun("--release-source", source, "cluster", "status")
	wantContains(t, "cluster status", status.stdout, "Update: none needed\n",
		"\n    service-dns    1.1.0    1.1.0   healthy\n    service-proxy  1.1.0    1.1.0   healthy\n")
	r = o.mustRun("--release-source", source, "cluster", "update")
	wantContains(t, "the rerun's output", r.stdout, "node3 runs the proxy service at 1.1.0 already\n", "Every node runs release 1.1.0 already; nothing changed.\n")
	for name, n := range o.nodes {
		if got := updatedTo(n); len(got) != 7 {
			t.Errorf("the rerun changed %s: %q", name, got[7:])
		}
	}
}

// cluster status of a node that runs the services under their old
// names shows each pinned service as not running, and each old
// container by its name and image, and that an update is needed.
func TestClusterStatusShowsServicesUnderTheirOldNamesAsNotThePinnedOnes(t *testing.T) {
	o := newOperator(t, "op")
	source := updateCluster(t, o)
	onOldNames(o)
	for _, n := range o.nodes {
		n.runs("1.1.0", "agent", "updater").runs("2.1.0", "containerd")
	}
	r := o.mustRun("--release-source", source, "cluster", "status")
	want := `Cluster prod on release 1.1.0
Operation lock: free
Update: stopped partway; run mooctl cluster update again to finish it

NODE               RUNNING                     PINNED  HEALTH
node1  direct
  agent            1.1.0                       1.1.0   healthy
    service-dns    -                           1.1.0   unknown
    service-proxy  -                           1.1.0   unknown
    dns            moostack.local/dns:1.0.0    -       healthy
    proxy          moostack.local/proxy:1.0.0  -       healthy
  updater          1.1.0                       1.1.0   healthy
  containerd       2.1.0                       2.1.0   healthy
node2  direct
  agent            1.1.0                       1.1.0   healthy
    service-dns    -                           1.1.0   unknown
    service-proxy  -                           1.1.0   unknown
    dns            moostack.local/dns:1.0.0    -       healthy
    proxy          moostack.local/proxy:1.0.0  -       healthy
  updater          1.1.0                       1.1.0   healthy
  containerd       2.1.0                       2.1.0   healthy
node3  direct
  agent            1.1.0                       1.1.0   healthy
    service-dns    -                           1.1.0   unknown
    service-proxy  -                           1.1.0   unknown
    dns            moostack.local/dns:1.0.0    -       healthy
    proxy          moostack.local/proxy:1.0.0  -       healthy
  updater          1.1.0                       1.1.0   healthy
  containerd       2.1.0                       2.1.0   healthy
`
	if r.stdout != want {
		t.Errorf("cluster status printed\n%s\nwant\n%s", r.stdout, want)
	}
	o.terminal = &scriptedTerminal{}
	if onTerminal := o.mustRun("--release-source", source, "cluster", "status"); onTerminal.stdout != want {
		t.Errorf("cluster status on a terminal printed\n%s\nwant\n%s", onTerminal.stdout, want)
	}
}

// A node that crashes, or fails, at any step of the rename stops the
// update; a rerun finishes it, and a third run changes nothing.
func TestARerunOfClusterUpdateFinishesARenameStoppedAtAnyStep(t *testing.T) {
	for _, c := range []struct {
		name string
		set  func(n *fakeNode, err error)
	}{
		{"removing", func(n *fakeNode, err error) { n.removeErr = err }},
		{"running", func(n *fakeNode, err error) { n.serviceErr = err }},
	} {
		t.Run(c.name, func(t *testing.T) {
			o := newOperator(t, "op")
			source := updateCluster(t, o)
			onOldNames(o)
			n := o.nodes["node2"]
			c.set(n, errors.New("the node crashed"))
			r := o.run("--release-source", source, "cluster", "update")
			if r.err == nil || !strings.Contains(r.err.Error(), "node2: updating the dns service to 1.1.0") {
				t.Fatalf("cluster update with node2 crashing: %v", r.err)
			}
			c.set(n, nil)
			o.mustRun("--release-source", source, "cluster", "update")
			for name, n := range o.nodes {
				if got, want := containerNames(t, n), []string{"service-dns", "service-proxy"}; !slices.Equal(got, want) {
					t.Errorf("%s runs %q, want %q", name, got, want)
				}
			}
			r = o.mustRun("--release-source", source, "cluster", "update")
			wantContains(t, "the third run's output", r.stdout, "nothing changed")
		})
	}
}

// mooctl runs each cluster service as its release states it: named for
// it, with the user, root and capabilities the release gives it, whatever
// they are, and its API.
func TestClusterUpdateRunsEachServiceWithTheReleasesProperties(t *testing.T) {
	o := newOperator(t, "op")
	source := updateCluster(t, o)
	writeUpdateReleaseWith(t, source, "1.1.0", "1.0.0", "2.1.0", release.Properties{User: "1000:1000", Capabilities: []string{"CAP_NET_BIND_SERVICE"}})
	// The fake node refuses a service that isn't locked down, so the
	// update stops at the first.
	o.run("--release-source", source, "cluster", "update")
	user := "1000:1000"
	want := &nodev1.Container{Name: "service-dns", Image: "moostack.local/dns:1.1.0", User: &user, Capabilities: &nodev1.Capabilities{Names: []string{"CAP_NET_BIND_SERVICE"}}, Api: "moostack.dns.v1.DNS"}
	n := o.nodes["node1"]
	n.mu.Lock()
	asked := slices.Clone(n.asked)
	n.mu.Unlock()
	if len(asked) == 0 || !proto.Equal(asked[0], want) {
		t.Errorf("node1 was asked to run %v, want %v first", asked, want)
	}
}

// A rename at the version the nodes run already, as of a cluster whose
// services ran under their names alone at the pinned release, runs each
// service under its service- name, though each removal deletes the
// image the update imported for it.
func TestClusterUpdateRenamesTheServicesAtTheVersionTheyRun(t *testing.T) {
	o := newOperator(t, "op")
	source := updateCluster(t, o)
	for _, n := range o.nodes {
		n.runs("1.1.0", "agent", "updater").runs("2.1.0", "containerd").runsUnderOldNames("1.1.0", "dns", "proxy")
	}
	o.mustRun("--release-source", source, "cluster", "update")
	for name, n := range o.nodes {
		if got, want := containerNames(t, n), []string{"service-dns", "service-proxy"}; !slices.Equal(got, want) {
			t.Errorf("%s runs %q, want %q", name, got, want)
		}
	}
}

// A container under a service's name alone that runs another image isn't
// the service's: cluster update leaves it running.
func TestClusterUpdateLeavesAContainerOfAServicesNameThatIsntTheServices(t *testing.T) {
	o := newOperator(t, "op")
	source := updateCluster(t, o)
	for _, n := range o.nodes {
		n.runs("1.1.0", "agent", "updater").runs("2.1.0", "containerd").runsServices("1.0.0", "dns", "proxy")
	}
	n := o.nodes["node1"]
	n.mu.Lock()
	n.status.Containers = append(n.status.Containers, &nodev1.ContainerStatus{Name: "dns", Image: "docker.io/library/dns:1", State: nodev1.TaskState_TASK_STATE_RUNNING})
	n.mu.Unlock()
	o.mustRun("--release-source", source, "cluster", "update")
	if got := updatedTo(n); slices.Contains(got, "remove dns") || !slices.Contains(got, "run service-dns 1.1.0") {
		t.Errorf("node1 was updated to %q, want dns run again at 1.1.0 and the dns container that isn't the service's kept", got)
	}
	if !slices.Contains(containerNames(t, n), "dns") {
		t.Errorf("node1 runs %q, want its own dns container kept", containerNames(t, n))
	}
}
