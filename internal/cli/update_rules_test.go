package cli_test

import (
	"context"
	"errors"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/release"
)

// updatedTo110 has every node of the operator's update cluster report that
// it runs release 1.1.0, healthy.
func updatedTo110(o *operator) {
	for _, n := range o.nodes {
		n.runs("1.1.0", "agent", "updater").runs("2.1.0", "containerd").runsServices("1.1.0", "dns", "proxy")
	}
}

func TestClusterUpdateOnAnUpToDateClusterChangesNothing(t *testing.T) {
	o := newOperator(t, "op")
	source := updateCluster(t, o)
	updatedTo110(o)
	r := o.mustRun("--release-source", source, "cluster", "update")
	for name, n := range o.nodes {
		if u := n.nodeUpdates(); len(u) > 0 {
			t.Errorf("%s, up to date, was updated to %v", name, u)
		}
	}
	var want string
	for _, name := range []string{"node1", "node2", "node3"} {
		want += name + " runs the agent at 1.1.0 already\n" +
			name + " runs the updater at 1.1.0 already\n" +
			name + " runs containerd at 2.1.0 already\n"
	}
	for _, name := range []string{"node1", "node2", "node3"} {
		want += name + " runs the dns service at 1.1.0 already\n" +
			name + " runs the proxy service at 1.1.0 already\n"
	}
	want += "Every node runs release 1.1.0 already; nothing changed.\n"
	if r.stdout != want {
		t.Errorf("cluster update printed\n%s\nwant\n%s", r.stdout, want)
	}
}

func TestARerunOfClusterUpdateGoesOnFromTheStepThatFailed(t *testing.T) {
	o := newOperator(t, "op")
	source := updateCluster(t, o)
	o.nodes["node2"].serviceErr = errors.New("the disk is full")
	r := o.run("--release-source", source, "cluster", "update")
	if r.err == nil {
		t.Fatal("cluster update succeeded")
	}
	wantContains(t, "cluster update's error", r.err.Error(), "node2: updating the dns service to 1.1.0: the disk is full",
		"Once that is fixed, run mooctl -C "+o.dir+" --release-source "+source+" cluster update again: it goes on from there.")
	// Every node's components were done before node2 failed; node3's
	// services weren't started.
	for name, want := range map[string]int{"node1": 5, "node2": 4, "node3": 3} {
		if n := len(o.nodes[name].nodeUpdates()); n != want {
			t.Errorf("%s had %d updates, want %d", name, n, want)
		}
	}
	o.nodes["node2"].mu.Lock()
	o.nodes["node2"].serviceErr = nil
	o.nodes["node2"].mu.Unlock()
	before := map[string]int{}
	for name, n := range o.nodes {
		before[name] = len(n.nodeUpdates())
	}
	r = o.mustRun("--release-source", source, "cluster", "update")
	for name, want := range map[string][]string{
		"node1": nil,
		"node2": {"run service-dns 1.1.0 moostack.local/dns:1.1.0", "run service-proxy 1.1.0 moostack.local/proxy:1.1.0"},
		"node3": {"run service-dns 1.1.0 moostack.local/dns:1.1.0", "run service-proxy 1.1.0 moostack.local/proxy:1.1.0"},
	} {
		if got := o.nodes[name].nodeUpdates()[before[name]:]; !slices.Equal(got, want) {
			t.Errorf("the rerun updated %s to %v, want %v", name, got, want)
		}
	}
	wantContains(t, "the rerun's output", r.stdout, "node1 runs the agent at 1.1.0 already\n", "node1 runs the dns service at 1.1.0 already\n", "Updating the dns service on node2 to 1.1.0\n", "Every node runs release 1.1.0.\n")
}

// refused checks that the run r failed saying each of want, before any
// node granted the lock or was updated.
func refused(t *testing.T, o *operator, r result, want ...string) {
	t.Helper()
	if r.err == nil {
		t.Fatalf("mooctl succeeded:\n%s", r.stdout)
	}
	wantContains(t, "the error", r.err.Error(), want...)
	for name, n := range o.nodes {
		if g := n.granted(); len(g) > 0 {
			t.Errorf("%s granted the lock %+v", name, g)
		}
		if u := n.nodeUpdates(); len(u) > 0 {
			t.Errorf("%s was updated to %v", name, u)
		}
	}
}

// mooctl installs only its own release and the one before it, so a pin
// that skips a release finds nodes on a release older than both, which
// mooctl refuses to operate.
func TestClusterUpdateRefusesAPinThatSkipsARelease(t *testing.T) {
	o := newOperator(t, "op")
	source := updateCluster(t, o)
	writeUpdateRelease(t, source, "1.2.0", "1.1.0", "2.2.0")
	o.version, o.previous = "1.2.0", "1.1.0"
	o.nodes["node1"].runs("1.1.0", "agent")
	pin(t, o, "1.2.0")
	r := o.run("--release-source", source, "cluster", "update")
	refused(t, o, r, "node2: it runs release 1.0.0, which this mooctl, of release 1.2.0, doesn't operate: it operates clusters on 1.2.0 and 1.1.0 only",
		"node3: it runs release 1.0.0", "Operate the cluster with the mooctl of the release it runs, and update it one release at a time.")
	if strings.Contains(r.err.Error(), "node1:") {
		t.Errorf("node1, on 1.1.0, was refused: %v", r.err)
	}
	// node2 and node3 are refused for the same reason, which says once
	// what to do.
	if n := strings.Count(r.err.Error(), "Operate the cluster with the mooctl"); n != 1 {
		t.Errorf("the error gives its hint %d times, want once:\n%v", n, r.err)
	}
}

func TestARefusedPinSendsAnHTTPSReleaseSourceNoRequest(t *testing.T) {
	o := newOperator(t, "op")
	source := updateCluster(t, o)
	writeUpdateRelease(t, source, "1.2.0", "1.1.0", "2.2.0")
	s := serveSource(t, source)
	o.client = s.Client()
	pin(t, o, "1.2.0")
	refused(t, o, o.run("--release-source", s.URL, "cluster", "update"), "doesn't operate release 1.2.0")
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.requests) > 0 {
		t.Errorf("the release source got the requests %v for a pin mooctl refuses", s.requests)
	}
}

func TestAMooctlRefusesToInstallAReleaseItDoesntOperate(t *testing.T) {
	for _, c := range []struct {
		name string
		args []string
	}{
		{"cluster update", []string{"cluster", "update"}},
		{"node add", []string{"node", "add", "root@new.invalid"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			o := newOperator(t, "op")
			source := updateCluster(t, o)
			writeUpdateRelease(t, source, "1.2.0", "1.1.0", "2.2.0")
			pin(t, o, "1.2.0")
			refused(t, o, o.run(append([]string{"--release-source", source}, c.args...)...),
				"this mooctl, of release 1.1.0, doesn't operate release 1.2.0, which the cluster directory pins: it operates clusters on 1.1.0 and 1.0.0 only",
				"Use the mooctl of release 1.2.0.")
			for name, n := range o.nodes {
				if n.dialed() > 0 {
					t.Errorf("mooctl reached %s for a release it doesn't operate", name)
				}
			}
		})
	}
}

// A node stopped partway runs its agent at one release and its updater or
// a cluster service at the one before: a pin that skips a release for
// any of them is refused, naming it, though the agent is on a release
// the pin updates.
func TestClusterUpdateRefusesAPinThatSkipsAReleaseForAnyComponent(t *testing.T) {
	o := newOperator(t, "op")
	source := updateCluster(t, o)
	writeUpdateRelease(t, source, "1.2.0", "1.1.0", "2.2.0")
	o.version, o.previous = "1.2.0", "1.1.0"
	updatedTo110(o)
	o.nodes["node2"].runs("1.0.0", "updater")
	o.nodes["node3"].runsServices("1.0.0", "dns")
	pin(t, o, "1.2.0")
	r := o.run("--release-source", source, "cluster", "update")
	refused(t, o, r,
		"node2: its updater runs release 1.0.0, and release 1.2.0 updates nodes on 1.2.0 or 1.1.0",
		"node3: its dns service runs release 1.0.0, and release 1.2.0 updates nodes on 1.2.0 or 1.1.0",
		"Pin the release the nodes run, or the one after it, and run the mooctl of that release.")
	if strings.Contains(r.err.Error(), "node1:") {
		t.Errorf("node1, on 1.1.0 throughout, was refused: %v", r.err)
	}
}

// unknownService has node report that it can't read the release of its
// service called name, as when its containerd is down.
func unknownService(n *fakeNode, name string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, c := range n.status.Containers {
		if c.Name == release.ContainerName(name) {
			c.Image, c.Digest, c.State = "", "", nodev1.TaskState_TASK_STATE_UNSPECIFIED
			c.Health, c.Problem = nodev1.Health_HEALTH_UNHEALTHY, "reading its container: connection refused"
		}
	}
}

// A node that can't say which release a service runs may be skipping a
// release for it: a pin past the release its agent runs is refused, and
// a rerun at that release, which repairs it, isn't.
func TestAServiceOfUnknownReleaseIsUpdatedOnlyAtTheAgentsRelease(t *testing.T) {
	for _, c := range []struct {
		pin     string
		refused bool
	}{{"1.2.0", true}, {"1.1.0", false}} {
		t.Run(c.pin, func(t *testing.T) {
			o := newOperator(t, "op")
			source := updateCluster(t, o)
			writeUpdateRelease(t, source, "1.2.0", "1.1.0", "2.2.0")
			o.version, o.previous = "1.2.0", "1.1.0"
			updatedTo110(o)
			unknownService(o.nodes["node3"], "dns")
			// updateCluster pins 1.1.0.
			if c.pin != "1.1.0" {
				pin(t, o, c.pin)
			}
			r := o.run("--release-source", source, "cluster", "update")
			if c.refused {
				refused(t, o, r, "node3: its dns service doesn't say which release it runs, and release 1.2.0 updates nodes on 1.2.0 or 1.1.0")
				return
			}
			if r.err != nil {
				t.Fatalf("a rerun at the agent's release was refused: %v", r.err)
			}
			wantContains(t, "the rerun's output", r.stdout, "Updating the dns service on node3 to 1.1.0\n")
		})
	}
}

// A node whose agent is a development build is on no release, so a part
// of it that doesn't say its release may be on any: the pinned release
// replaces it with the agent.
func TestAServiceOfUnknownReleaseBesideADevelopmentAgentIsUpdated(t *testing.T) {
	o := newOperator(t, "op")
	source := updateCluster(t, o)
	updatedTo110(o)
	o.nodes["node3"].runs(devVersion, "agent")
	unknownService(o.nodes["node3"], "dns")
	r := o.run("--release-source", source, "cluster", "update")
	if r.err != nil {
		t.Fatalf("cluster update was refused: %v", r.err)
	}
	wantContains(t, "cluster update's output", r.stdout, "Updating the agent on node3 to 1.1.0\n", "Updating the dns service on node3 to 1.1.0\n")
}

// A service that the pinned release doesn't have, as one an earlier
// release dropped, stays on the node at its release, and doesn't count.
func TestAServiceThePinnedReleaseDoesntHaveDoesntCount(t *testing.T) {
	o := newOperator(t, "op")
	source := updateCluster(t, o)
	writeUpdateRelease(t, source, "1.2.0", "1.1.0", "2.2.0")
	o.version, o.previous = "1.2.0", "1.1.0"
	updatedTo110(o)
	for _, n := range o.nodes {
		n.runsServices("1.0.0", "legacy")
	}
	pin(t, o, "1.2.0")
	if r := o.run("--release-source", source, "cluster", "update"); r.err != nil {
		t.Errorf("a cluster with a service 1.2.0 doesn't have was refused: %v", r.err)
	}
}

// Going back moves the parts of a node on this mooctl's release back,
// and none on a release after it.
func TestGoingBackRefusesAPartOnAReleaseAfterThisMooctls(t *testing.T) {
	o := newOperator(t, "op")
	source := updateCluster(t, o)
	updatedTo110(o)
	o.nodes["node1"].runs("1.0.0", "agent")
	o.nodes["node2"].runsServices("1.2.0", "dns")
	pin(t, o, "1.0.0")
	refused(t, o, o.run("--release-source", source, "cluster", "update"), "node2: its dns service runs release 1.2.0")
}

// A node that runs two releases, as when an older mooctl meets a going
// back that a newer one stopped partway, is refused with what to do: the
// mooctl of the release pinned.
func TestARefusalOfANodeOnTwoReleasesSaysWhichMooctlToRun(t *testing.T) {
	o := newOperator(t, "op")
	source := updateCluster(t, o)
	o.version, o.previous = "1.0.0", ""
	pin(t, o, "1.0.0")
	o.nodes["node1"].runs("1.1.0", "updater")
	refused(t, o, o.run("--release-source", source, "cluster", "update"),
		"node1: its updater runs release 1.1.0", "Pin the release the nodes run, or the one after it, and run the mooctl of that release.")
}

func TestClusterUpdateRefusesAPinThatGoesBack(t *testing.T) {
	o := newOperator(t, "op")
	source := updateCluster(t, o)
	for _, n := range o.nodes {
		n.runs("1.1.0", "agent")
	}
	pin(t, o, "1.0.0")
	refused(t, o, o.run("--release-source", source, "cluster", "update"),
		"node1: it runs release 1.1.0, and release 1.0.0 updates nodes on 1.0.0 only, and moves nodes on 1.1.0 back to it only while a node still runs it\n",
		"node2: it runs release 1.1.0", "node3: it runs release 1.1.0",
		"Pin the release the nodes run, or the one after it, and run the mooctl of that release.")
}

func TestEveryOperationRefusesAClusterOnAReleaseThisMooctlDoesntOperate(t *testing.T) {
	for _, c := range []struct {
		name string
		args []string
	}{
		{"cluster update", []string{"cluster", "update"}},
		{"cluster update --source", nil},
		{"node remove", []string{"node", "remove", "node1"}},
		{"node add", []string{"node", "add", "root@new.invalid"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			o := newOperator(t, "op")
			source := updateCluster(t, o)
			if c.args == nil {
				c.args = []string{"cluster", "update", "--source", devCheckout(t), "agent"}
			}
			o.nodes["node3"].runs("0.9.0", "agent")
			refused(t, o, o.run(append([]string{"--release-source", source}, c.args...)...),
				"node3: it runs release 0.9.0, which this mooctl, of release 1.1.0, doesn't operate: it operates clusters on 1.1.0 and 1.0.0 only",
				"Operate the cluster with the mooctl of the release it runs, and update it one release at a time.")
		})
	}
}

func TestEveryOperationRefusesANodeThatDoesntSayItsRelease(t *testing.T) {
	for _, c := range []struct {
		name string
		args []string
	}{
		{"cluster update", []string{"cluster", "update"}},
		{"cluster update --source", nil},
		{"node remove", []string{"node", "remove", "node1"}},
		{"node add", []string{"node", "add", "root@new.invalid"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			o := newOperator(t, "op")
			source := updateCluster(t, o)
			if c.args == nil {
				c.args = []string{"cluster", "update", "--source", devCheckout(t), "agent"}
			}
			o.nodes["node2"].status.Components = nil
			refused(t, o, o.run(append([]string{"--release-source", source}, c.args...)...), "node2: its agent doesn't say which release it runs",
				"Check the node with mooctl cluster status: its agent may be broken.")
		})
	}
}

// A development build sits outside the releases mooctl operates.
func TestEveryOperationOperatesANodeWhoseAgentIsADevelopmentBuild(t *testing.T) {
	for _, c := range []struct {
		name string
		args []string
	}{
		{"cluster update", []string{"cluster", "update"}},
		{"cluster update --source", nil},
		{"node remove", []string{"node", "remove", "node1"}},
		{"node add", []string{"node", "add", "root@new.invalid"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			o := newOperator(t, "op")
			source := updateCluster(t, o)
			if c.args == nil {
				c.args = []string{"cluster", "update", "--source", devCheckout(t), "agent"}
			}
			o.nodes["node3"].runs(devVersion, "agent")
			r := o.run(append([]string{"--release-source", source}, c.args...)...)
			if r.err != nil && strings.Contains(r.err.Error(), "node3") {
				t.Errorf("%s refused node3, whose agent is a development build: %v", c.name, r.err)
			}
			if g := o.nodes["node3"].granted(); len(g) != 1 || g[0].Operation != strings.TrimSuffix(c.name, " --source") {
				t.Errorf("node3 granted the locks %+v, want one, in the preflight of %s", g, c.name)
			}
		})
	}
}

func TestAMooctlOfTheFirstReleaseOperatesOnlyClustersOnIt(t *testing.T) {
	o := newOperator(t, "op")
	source := updateCluster(t, o)
	o.version, o.previous = "1.0.0", ""
	pin(t, o, "1.0.0")
	o.nodes["node1"].runs("1.1.0", "agent")
	refused(t, o, o.run("--release-source", source, "cluster", "update"),
		"node1: it runs release 1.1.0, which this mooctl, of release 1.0.0, doesn't operate: it operates clusters on 1.0.0 only")
}

// sourceServer serves the release source at source over HTTPS, and counts
// the requests for each path.
type sourceServer struct {
	*httptest.Server
	mu       sync.Mutex
	requests map[string]int
}

func serveSource(t *testing.T, source string) *sourceServer {
	t.Helper()
	s := &sourceServer{requests: map[string]int{}}
	files := http.FileServer(http.Dir(source))
	s.Server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		s.requests[r.URL.Path]++
		s.mu.Unlock()
		files.ServeHTTP(w, r)
	}))
	t.Cleanup(s.Close)
	return s
}

func TestClusterUpdateDownloadsTheReleaseOnceFromAnHTTPSSource(t *testing.T) {
	o := newOperator(t, "op")
	s := serveSource(t, updateCluster(t, o))
	o.client = s.Client()
	r := o.mustRun("--release-source", s.URL, "cluster", "status")
	wantContains(t, "the first command's warnings", r.stderr, "Downloading release 1.1.0 from "+s.URL+"\n")
	r = o.mustRun("--release-source", s.URL, "cluster", "update")
	if strings.Contains(r.stderr, "Downloading") {
		t.Errorf("cluster update downloaded the release again:\n%s", r.stderr)
	}
	r = o.mustRun("--release-source", s.URL, "cluster", "update")
	wantContains(t, "the rerun's output", r.stdout, "Every node runs release 1.1.0 already; nothing changed.")
	for _, p := range []string{"/1.1.0/manifest.yml", "/1.1.0/SHA256SUMS", "/1.1.0/linux-arm64/agent", "/1.1.0/linux-amd64/dns.tar"} {
		s.mu.Lock()
		n := s.requests[p]
		s.mu.Unlock()
		if n != 1 {
			t.Errorf("%s was downloaded %d times, want once", p, n)
		}
	}
	if got := o.nodes["node2"].nodeUpdates()[0]; got != "agent 1.1.0 agent for arm64 at 1.1.0" {
		t.Errorf("node2 was updated to %q first, want the downloaded agent", got)
	}
}

func TestClusterUpdateRefusesADownloadThatDoesntMatchItsChecksums(t *testing.T) {
	o := newOperator(t, "op")
	source := updateCluster(t, o)
	writeFile(t, filepath.Join(source, "1.1.0", "linux-arm64", "updater"), []byte("tampered"))
	s := serveSource(t, source)
	o.client = s.Client()
	refused(t, o, o.run("--release-source", s.URL, "cluster", "update"), "release 1.1.0: linux-arm64/updater doesn't match its checksum")
	for name, n := range o.nodes {
		if n.dialed() > 0 {
			t.Errorf("cluster update reached %s with a tampered download", name)
		}
	}
	filepath.WalkDir(o.cache(), func(path string, d fs.DirEntry, err error) error {
		if err == nil && d.Name() == "manifest.yml" {
			t.Errorf("the cache kept %s from a tampered download", filepath.Dir(path))
		}
		return nil
	})
}

func TestClusterUpdateTakesNoPlainHTTPSource(t *testing.T) {
	o := newOperator(t, "op")
	source := updateCluster(t, o)
	s := httptest.NewServer(http.FileServer(http.Dir(source)))
	defer s.Close()
	refused(t, o, o.run("--release-source", s.URL, "cluster", "update"), "neither a directory nor an https URL")
}

func TestCtrlCStopsClusterUpdateAndGivesBackTheLock(t *testing.T) {
	o := newOperator(t, "op")
	source := updateCluster(t, o)
	o.nodes["node1"].updating = func(ctx context.Context) {
		syscall.Kill(os.Getpid(), syscall.SIGINT)
		<-ctx.Done()
	}
	r := o.run("--release-source", source, "cluster", "update")
	if r.err == nil {
		t.Fatal("cluster update succeeded")
	}
	wantContains(t, "cluster update's error", r.err.Error(), "cluster update interrupted", "node1", "Run mooctl -C "+o.dir+" --release-source "+source+" cluster update again: it goes on from there.")
	if got := o.exitCodes(); len(got) > 0 {
		t.Errorf("mooctl exited with %v, want it to return", got)
	}
	for name, n := range o.nodes {
		if l := n.heldLock(); l != nil {
			t.Errorf("after the interrupted update, %s holds the lock %+v", name, l)
		}
	}
	if u := o.nodes["node2"].nodeUpdates(); len(u) > 0 {
		t.Errorf("node2 was updated to %v after the interrupt", u)
	}
}

func TestCtrlCWhileClusterUpdateDownloadsOrRunsThePreflightStopsIt(t *testing.T) {
	for name, interrupt := range map[string]func(o *operator, source string) string{
		"the download": func(o *operator, source string) string {
			files := http.FileServer(http.Dir(source))
			s := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/linux-amd64/agent") {
					syscall.Kill(os.Getpid(), syscall.SIGINT)
					<-r.Context().Done()
					return
				}
				files.ServeHTTP(w, r)
			}))
			o.t.Cleanup(s.Close)
			o.client = s.Client()
			return s.URL
		},
		"the preflight": func(o *operator, source string) string {
			o.nodes["node2"].dialing = func(ctx context.Context) {
				syscall.Kill(os.Getpid(), syscall.SIGINT)
				<-ctx.Done()
			}
			return source
		},
	} {
		t.Run(name, func(t *testing.T) {
			o := newOperator(t, "op")
			source := interrupt(o, updateCluster(t, o))
			r := o.run("--release-source", source, "cluster", "update")
			if r.err == nil {
				t.Fatal("cluster update succeeded")
			}
			wantContains(t, "cluster update's error", r.err.Error(), "cluster update interrupted", "Run mooctl -C "+o.dir+" --release-source "+source+" cluster update again: it goes on from there.")
			if got := o.exitCodes(); len(got) > 0 {
				t.Errorf("mooctl exited with %v, want it to return", got)
			}
			for name, n := range o.nodes {
				if l := n.heldLock(); l != nil {
					t.Errorf("after the interrupted update, %s holds the lock %+v", name, l)
				}
				if u := n.nodeUpdates(); len(u) > 0 {
					t.Errorf("%s was updated to %v", name, u)
				}
			}
			// An interrupted download keeps nothing in the cache.
			filepath.WalkDir(o.cache(), func(path string, d fs.DirEntry, err error) error {
				if err == nil && (d.Name() == "manifest.yml" || strings.HasPrefix(d.Name(), ".download-")) {
					t.Errorf("the interrupted update left %s in the cache", path)
				}
				return nil
			})
		})
	}
}

// A node that crashed mid-update and is back holds the stopped update's
// lease until it runs out: status says whose update holds the lock where,
// and a rerun is refused saying until when, and how to break it.
func TestAStoppedUpdatesLeaseOnANodeThatCameBackSaysWhatHoldsIt(t *testing.T) {
	o := newOperator(t, "op")
	source := updateCluster(t, o)
	stale := bobsLock
	stale.Operation = "cluster update"
	o.nodes["node2"].locked(stale)
	r := o.mustRun("--release-source", source, "cluster", "status")
	wantContains(t, "cluster status", r.stdout, "Operation lock: held by bob on desktop since ", ", for cluster update on node2\n", "Update: in progress\n")
	r = o.run("--release-source", source, "cluster", "update")
	if r.err == nil {
		t.Fatal("cluster update succeeded on a node locked by a stopped update")
	}
	wantContains(t, "the rerun's error", r.err.Error(), "node2: it is locked by bob on desktop since ", ", for cluster update, and expires at ", "mooctl cluster unlock")
	for name, n := range o.nodes {
		if u := n.nodeUpdates(); len(u) > 0 {
			t.Errorf("%s was updated to %v", name, u)
		}
		if l := n.heldLock(); name != "node2" && l != nil {
			t.Errorf("%s holds the lock %+v after the refusal, want it handed back", name, l)
		}
	}
}

// An update stopped partway can go back: pinning the release before moves
// the nodes already on the new one back, while a node still runs it.
func TestAnUpdateStoppedPartwayGoesBackToTheReleaseBefore(t *testing.T) {
	o := newOperator(t, "op")
	source := updateCluster(t, o)
	o.nodes["node1"].runs("1.1.0", "agent", "updater").runs("2.1.0", "containerd")
	pin(t, o, "1.0.0")
	r := o.mustRun("--release-source", source, "cluster", "update")
	wantContains(t, "cluster update's output", r.stdout,
		"Updating the agent on node1 to 1.0.0\n", "Updating the updater on node1 to 1.0.0\n", "Updating containerd on node1 to 2.0.0\n",
		"Every node runs release 1.0.0.\n")
	if got := o.nodes["node1"].agentUpdates(); len(got) != 1 || got[0] != "1.0.0 agent for amd64 at 1.0.0" {
		t.Errorf("node1's agent was updated to %q, want 1.0.0's", got)
	}
}

// An update that stopped in its services has every agent on the new
// release, and some services still on the one before: it hasn't
// finished, so it can still go back.
func TestAnUpdateStoppedInItsServicesGoesBackToTheReleaseBefore(t *testing.T) {
	o := newOperator(t, "op")
	source := updateCluster(t, o)
	updatedTo110(o)
	o.nodes["node3"].runsServices("1.0.0", "dns", "proxy")
	pin(t, o, "1.0.0")
	r := o.mustRun("--release-source", source, "cluster", "update")
	wantContains(t, "cluster update's output", r.stdout,
		"Updating the agent on node1 to 1.0.0\n", "Updating the dns service on node1 to 1.0.0\n",
		"node3 runs the dns service at 1.0.0 already\n", "Every node runs release 1.0.0.\n")
}

// pinnedBuild is the build of 1.1.0's component called name for amd64,
// as node status says it.
func pinnedBuild(t *testing.T, source, name string) string {
	t.Helper()
	r, err := release.Open(source, "1.1.0")
	if err != nil {
		t.Fatal(err)
	}
	c, _ := r.Component(name)
	b, err := r.Build("amd64", c.Files)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// A node may run another build of the pinned version, as a bad build of a
// release built again: cluster update replaces it, and status says so.
func TestAnotherBuildOfThePinnedReleaseIsReplacedAndStatusSaysSo(t *testing.T) {
	o := newOperator(t, "op")
	source := updateCluster(t, o)
	updatedTo110(o)
	n := o.nodes["node3"]
	n.mu.Lock()
	for _, c := range n.status.Components {
		if c.Name == "agent" {
			c.Build = "sha256:another"
		}
	}
	n.mu.Unlock()
	o.nodes["node1"].mu.Lock()
	o.nodes["node1"].status.Components[0].Build = pinnedBuild(t, source, o.nodes["node1"].status.Components[0].Name)
	o.nodes["node1"].mu.Unlock()

	r := o.mustRun("--release-source", source, "cluster", "status")
	wantContains(t, "cluster status", r.stdout, "Update: stopped partway", "node3: agent: it runs another build of 1.1.0 than the release pins")

	r = o.mustRun("--release-source", source, "cluster", "update")
	wantContains(t, "cluster update's output", r.stdout, "Updating the agent on node3 to 1.1.0\n", "node1 runs the agent at 1.1.0 already\n", "node2 runs the agent at 1.1.0 already\n")
	if u := o.nodes["node3"].agentUpdates(); len(u) != 1 {
		t.Errorf("node3's agent was updated to %v, want once, to 1.1.0's build", u)
	}
}

// A node may run another build of a pinned cluster service, as of a
// release built again: status says so, and cluster update replaces it.
func TestAnotherBuildOfAPinnedServiceIsReplacedAndStatusSaysSo(t *testing.T) {
	o := newOperator(t, "op")
	source := updateCluster(t, o)
	updatedTo110(o)
	n := o.nodes["node3"]
	n.mu.Lock()
	n.setService(&nodev1.Component{Name: "dns", Version: "1.1.0", Build: "sha256:another", Health: nodev1.Health_HEALTH_HEALTHY})
	n.mu.Unlock()
	r := o.mustRun("--release-source", source, "cluster", "status")
	wantContains(t, "cluster status", r.stdout, "Update: stopped partway", "node3: service-dns: it runs another build of 1.1.0 than the release pins")
	o.mustRun("--release-source", source, "cluster", "update")
	if got := updatedTo(n); !slices.Contains(got, "run service-dns 1.1.0") {
		t.Errorf("node3 was updated to %q, want dns run again at 1.1.0's build", got)
	}
}
