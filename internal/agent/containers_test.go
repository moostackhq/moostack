//go:build linux

package agent_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/defaults"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/errdefs"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	servicev1 "github.com/moostackhq/moostack/api/service/v1"
	"github.com/moostackhq/moostack/internal/hostlayout"
	"github.com/moostackhq/moostack/internal/nodetest"
	"github.com/moostackhq/moostack/internal/release"
)

// These tests run containers through Run and Remove, as an operation
// that holds the operation lock does, and look at what runs on the node.

// The images of the apps the tests run.
const (
	appImage   = "example.test/app:1"
	otherImage = "example.test/app:2"
	thirdImage = "example.test/other:1"
)

// app is a container with only a name and an image, as an app is run.
func app(name, image string) *nodev1.Container {
	return &nodev1.Container{Name: name, Image: image}
}

// hold has the operation lock held for nodetest.Lease, for the rest of
// the test, and returns the lease.
func hold(t *testing.T, n *nodetest.Node) string {
	t.Helper()
	n.HoldLock(t)
	return nodetest.Lease
}

// run has n's agent run c for lease, within a minute.
func run(t *testing.T, n *nodetest.Node, lease string, c *nodev1.Container) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	_, err := n.Agent.Run(ctx, &nodev1.RunRequest{Container: c, Lease: lease})
	return err
}

// mustRun is run, failing the test if the run fails.
func mustRun(t *testing.T, n *nodetest.Node, lease string, c *nodev1.Container) {
	t.Helper()
	if err := run(t, n, lease, c); err != nil {
		t.Fatalf("running %s from %s: %v", c.Name, c.Image, err)
	}
}

// removeContainer has n's agent remove the container called name for
// lease, within a minute.
func removeContainer(t *testing.T, n *nodetest.Node, lease, name string) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	_, err := n.Agent.Remove(ctx, &nodev1.RemoveRequest{Name: name, Lease: lease})
	return err
}

// containerStatus is the container called name as n's agent reports it,
// nil if it reports none.
func containerStatus(t *testing.T, n *nodetest.Node, name string) *nodev1.ContainerStatus {
	t.Helper()
	s, err := n.Status(t)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range s.Containers {
		if c.Name == name {
			return c
		}
	}
	return nil
}

// eventuallyRunning waits until n's agent reports the container called
// name running, and returns it as reported.
func eventuallyRunning(t *testing.T, n *nodetest.Node, name string) *nodev1.ContainerStatus {
	t.Helper()
	var c *nodev1.ContainerStatus
	nodetest.Eventually(t, 30*time.Second, func() error {
		c = containerStatus(t, n, name)
		if c.GetState() != nodev1.TaskState_TASK_STATE_RUNNING {
			return fmt.Errorf("%s isn't running: %v", name, c)
		}
		return nil
	})
	return c
}

// containerPID is the pid of the one process in the cgroup of the
// container called name, which only that container's process is in.
func containerPID(t *testing.T, n *nodetest.Node, name string) string {
	t.Helper()
	var pids []string
	nodetest.Eventually(t, 10*time.Second, func() error {
		out, err := n.Output("root", "sh", "-c", `for p in /proc/[0-9]*; do grep -qx "0::.*$1" $p/cgroup 2>/dev/null && echo ${p#/proc/}; done; true`, "-", hostlayout.CgroupParent+"/"+name)
		if err != nil {
			return fmt.Errorf("finding %s's processes: %v %s", name, err, out)
		}
		if pids = strings.Fields(out); len(pids) != 1 {
			return fmt.Errorf("%s's cgroup holds the processes %v", name, pids)
		}
		return nil
	})
	return pids[0]
}

// procFile is the file of the process pid in /proc.
func procFile(t *testing.T, n *nodetest.Node, pid, file string) string {
	t.Helper()
	out, err := n.Output("root", "cat", "/proc/"+pid+"/"+file)
	if err != nil {
		t.Fatalf("reading /proc/%s/%s: %v", pid, file, err)
	}
	return out
}

// mountedAt are the options of the mount of process pid at path, empty
// if nothing is mounted there.
func mountedAt(t *testing.T, n *nodetest.Node, pid, path string) string {
	t.Helper()
	for _, line := range strings.Split(procFile(t, n, pid, "mountinfo"), "\n") {
		if f := strings.Fields(line); len(f) > 5 && f[4] == path {
			return f[5]
		}
	}
	return ""
}

// interfaces are the network interfaces process pid sees.
func interfaces(t *testing.T, n *nodetest.Node, pid string) []string {
	t.Helper()
	var names []string
	for _, line := range strings.Split(procFile(t, n, pid, "net/dev"), "\n")[2:] {
		if name, _, ok := strings.Cut(strings.TrimSpace(line), ":"); ok {
			names = append(names, name)
		}
	}
	return names
}

// wantStatusLines fails unless the /proc status of process pid has each
// of want as a line.
func wantStatusLines(t *testing.T, n *nodetest.Node, pid string, want ...string) {
	t.Helper()
	st := procFile(t, n, pid, "status")
	for _, w := range want {
		if !strings.Contains(st, w+"\n") {
			t.Errorf("the container's status has no %q:\n%s", w, st)
		}
	}
}

// hasImage is whether n's containerd holds the image called name.
func hasImage(t *testing.T, n *nodetest.Node, name string) bool {
	t.Helper()
	ctx := namespaces.WithNamespace(t.Context(), hostlayout.ContainerdNamespace)
	_, err := n.Containerd(t).ImageService().Get(ctx, name)
	if err != nil && !errdefs.IsNotFound(err) {
		t.Fatal(err)
	}
	return err == nil
}

// containerdDefaultCapabilities is containerd's default set of
// capabilities, as /proc shows it.
const containerdDefaultCapabilities = "00000000a80425fb"

func TestRunRunsAContainerAsItsImageSaysWithCommonDefaults(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.Import(t, nodetest.Image{Name: appImage})
	mustRun(t, n, hold(t, n), app("app-web", appImage))
	got := eventuallyRunning(t, n, "app-web")
	digest, _ := imageOf(t, n, appImage)
	want := &nodev1.ContainerStatus{Name: "app-web", Image: appImage, Digest: digest, State: nodev1.TaskState_TASK_STATE_RUNNING}
	if !proto.Equal(got, want) {
		t.Errorf("the agent reports %v, want %v", got, want)
	}
	pid := containerPID(t, n, "app-web")
	// The image names no user, so the container runs as root, with
	// containerd's default capabilities, and can't gain more.
	wantStatusLines(t, n, pid, "Uid:\t0\t0\t0\t0", "CapEff:\t"+containerdDefaultCapabilities, "CapBnd:\t"+containerdDefaultCapabilities, "NoNewPrivs:\t1")
	if opts := mountedAt(t, n, pid, "/"); !strings.HasPrefix(opts, "rw") {
		t.Errorf("the container's root is mounted %q, want writable", opts)
	}
	if got := interfaces(t, n, pid); !slices.Equal(got, []string{"lo"}) {
		t.Errorf("the container has the network interfaces %v, want only lo", got)
	}
	for _, path := range []string{"/etc/resolv.conf", "/etc/hosts", servicev1.SocketDir} {
		if opts := mountedAt(t, n, pid, path); opts != "" {
			t.Errorf("the container has %s mounted %s, want nothing there", path, opts)
		}
		if _, err := n.Output("root", "test", "-e", "/proc/"+pid+"/root"+path); err == nil {
			t.Errorf("the container has %s", path)
		}
	}
	if cgroup := procFile(t, n, pid, "cgroup"); !strings.HasSuffix(strings.TrimSpace(cgroup), hostlayout.CgroupParent+"/app-web") {
		t.Errorf("the container's cgroup is %q, want one of its own under %s", cgroup, hostlayout.CgroupParent)
	}
}

func TestRunRunsAContainerAsItsImagesUser(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.Import(t, nodetest.Image{Name: appImage, User: "1000:1001"})
	mustRun(t, n, hold(t, n), app("app-web", appImage))
	wantStatusLines(t, n, containerPID(t, n, "app-web"), "Uid:\t1000\t1000\t1000\t1000", "Gid:\t1001\t1001\t1001\t1001", "NoNewPrivs:\t1")
}

func TestRunRunsAContainerWithTheGivenProperties(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.Import(t, nodetest.Image{Name: appImage, User: "1000:1001"})
	lease := hold(t, n)
	user := "0:3000"
	c := app("app-web", appImage)
	c.User, c.ReadOnlyRoot, c.Capabilities = &user, true, &nodev1.Capabilities{Names: []string{"CAP_NET_BIND_SERVICE"}}
	mustRun(t, n, lease, c)
	pid := containerPID(t, n, "app-web")
	wantStatusLines(t, n, pid, "Uid:\t0\t0\t0\t0", "Gid:\t3000\t3000\t3000\t3000", "CapEff:\t0000000000000400", "CapBnd:\t0000000000000400", "NoNewPrivs:\t1")
	if opts := mountedAt(t, n, pid, "/"); !strings.HasPrefix(opts, "ro") {
		t.Errorf("the container's root is mounted %q, want read-only", opts)
	}
	// No capabilities at all is a set, unlike none given.
	c.Capabilities = &nodev1.Capabilities{}
	mustRun(t, n, lease, c)
	pid = containerPID(t, n, "app-web")
	wantStatusLines(t, n, pid, "CapEff:\t0000000000000000", "CapBnd:\t0000000000000000")
	// The agent keeps the properties as they were given across its
	// restart, rather than making the container anew.
	n.Restart(t, "agent")
	n.EventuallyHealthy(t)
	time.Sleep(3 * time.Second)
	if again := containerPID(t, n, "app-web"); again != pid {
		t.Errorf("app-web ran as pid %s, then as %s once the agent restarted", pid, again)
	}
	wantStatusLines(t, n, containerPID(t, n, "app-web"), "CapEff:\t0000000000000000", "CapBnd:\t0000000000000000")
}

// A container run as a user other than root has the capabilities it is
// given, which the exec of its process would otherwise lose.
func TestRunGivesTheCapabilitiesToAUserOtherThanRoot(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.Import(t, nodetest.Image{Name: appImage})
	user := "1000:1000"
	c := app("app-web", appImage)
	c.User, c.Capabilities = &user, &nodev1.Capabilities{Names: []string{"CAP_NET_BIND_SERVICE"}}
	lease := hold(t, n)
	mustRun(t, n, lease, c)
	wantStatusLines(t, n, containerPID(t, n, "app-web"), "Uid:\t1000\t1000\t1000\t1000",
		"CapInh:\t0000000000000400", "CapPrm:\t0000000000000400", "CapEff:\t0000000000000400", "CapBnd:\t0000000000000400", "CapAmb:\t0000000000000400")
	// No capabilities leave none in any set.
	c.Capabilities = &nodev1.Capabilities{}
	mustRun(t, n, lease, c)
	wantStatusLines(t, n, containerPID(t, n, "app-web"),
		"CapInh:\t0000000000000000", "CapPrm:\t0000000000000000", "CapEff:\t0000000000000000", "CapBnd:\t0000000000000000", "CapAmb:\t0000000000000000")
}

func TestAContainerWithAnAPIIsRoutedAndReportsItsHealth(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.ImportFixtures(t)
	fixture := nodetest.Fixture()
	nobody := "65534:65534"
	c := &nodev1.Container{Name: "service-dns", Image: fixture.Image, User: &nobody, ReadOnlyRoot: true, Capabilities: &nodev1.Capabilities{}, Api: fixture.Api}
	mustRun(t, n, hold(t, n), c)
	nodetest.Eventually(t, 30*time.Second, func() error {
		_, err := call(t.Context(), n.AgentConn(t), fixture.Api, "Echo", nil)
		return err
	})
	if got := containerStatus(t, n, "service-dns"); got.Health != nodev1.Health_HEALTH_HEALTHY || got.State != nodev1.TaskState_TASK_STATE_RUNNING || got.Problem != "" {
		t.Errorf("the agent reports %v, want it running and healthy", got)
	}
	if opts := mountedAt(t, n, containerPID(t, n, "service-dns"), servicev1.SocketDir); opts == "" {
		t.Errorf("the container has nothing mounted at %s", servicev1.SocketDir)
	}
	killService(t, n, "fixtureservice")
	nodetest.Eventually(t, 30*time.Second, func() error {
		got := containerStatus(t, n, "service-dns")
		if got.Health != nodev1.Health_HEALTH_UNHEALTHY || got.State != nodev1.TaskState_TASK_STATE_EXITED || got.Problem == "" {
			return fmt.Errorf("the agent reports %v, want it exited and unhealthy, saying why", got)
		}
		return nil
	})
	// Running it again repairs it, keeping the log that says why it
	// failed.
	mustRun(t, n, hold(t, n), c)
	want := "serving " + fixture.Api + "\n"
	nodetest.Eventually(t, 30*time.Second, func() error {
		log, err := n.Output("root", "cat", hostlayout.ContainerLogDir+"/service-dns.log")
		if err != nil || strings.Count(log, want) != 2 {
			return fmt.Errorf("service-dns's log is %q, %v, want what both runs wrote", log, err)
		}
		return nil
	})
	if got := eventuallyRunning(t, n, "service-dns"); got.Health != nodev1.Health_HEALTH_HEALTHY {
		t.Errorf("the agent reports %v once it was run again", got)
	}
	// Removing it routes its API no more, and its socket directory goes.
	if err := removeContainer(t, n, hold(t, n), "service-dns"); err != nil {
		t.Fatal(err)
	}
	if _, err := call(t.Context(), n.AgentConn(t), fixture.Api, "Echo", nil); !noRoute(err) {
		t.Errorf("a call of the removed container's API: %v, want no route", err)
	}
	if _, err := n.Output("root", "test", "-e", hostlayout.ContainersDir+"/service-dns"); err == nil {
		t.Errorf("the removed container's socket directory is kept")
	}
}

func TestStatusReportsAContainerThatExitedWithItsCode(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	code := 3
	n.Import(t, nodetest.Image{Name: appImage, Exit: &code})
	mustRun(t, n, hold(t, n), app("app-web", appImage))
	nodetest.Eventually(t, 30*time.Second, func() error {
		got := containerStatus(t, n, "app-web")
		if got.State != nodev1.TaskState_TASK_STATE_EXITED || got.ExitCode != 3 || got.Health != nodev1.Health_HEALTH_UNSPECIFIED {
			return fmt.Errorf("the agent reports %v, want it exited with 3, of no health", got)
		}
		return nil
	})
}

func TestRunningAContainerAgainUnchangedChangesNothing(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.Import(t, nodetest.Image{Name: appImage})
	lease := hold(t, n)
	c := app("app-web", appImage)
	c.Capabilities = &nodev1.Capabilities{Names: []string{"CAP_NET_BIND_SERVICE", "CAP_CHOWN"}}
	mustRun(t, n, lease, c)
	pid := containerPID(t, n, "app-web")
	log := hostlayout.ContainerLogDir + "/app-web.log"
	nodetest.Eventually(t, 10*time.Second, func() error {
		if got, err := n.Output("root", "cat", log); err != nil || got != "running as 0:0\n" {
			return fmt.Errorf("app-web's log is %q, %v", got, err)
		}
		return nil
	})
	// The same capabilities in another order are the same container.
	c.Capabilities.Names = []string{"CAP_CHOWN", "CAP_NET_BIND_SERVICE"}
	mustRun(t, n, lease, c)
	if again := containerPID(t, n, "app-web"); again != pid {
		t.Errorf("app-web ran as pid %s, then as %s once run again unchanged", pid, again)
	}
	if got, err := n.Output("root", "cat", log); err != nil || got != "running as 0:0\n" {
		t.Errorf("app-web's log is %q, %v once run again unchanged, want what it wrote kept", got, err)
	}
}

// Each change of what a container is replaces it: its image, the build
// of its image, and each of its properties.
func TestRunningAContainerChangedReplacesIt(t *testing.T) {
	user, caps := "1000", &nodev1.Capabilities{}
	for name, change := range map[string]func(t *testing.T, n *nodetest.Node, c *nodev1.Container){
		"another image": func(t *testing.T, n *nodetest.Node, c *nodev1.Container) {
			n.Import(t, nodetest.Image{Name: otherImage})
			c.Image = otherImage
		},
		"another build of its image": func(t *testing.T, n *nodetest.Node, c *nodev1.Container) {
			n.Import(t, nodetest.Image{Name: appImage, Rebuild: 1})
		},
		"another user":       func(_ *testing.T, _ *nodetest.Node, c *nodev1.Container) { c.User = &user },
		"a read-only root":   func(_ *testing.T, _ *nodetest.Node, c *nodev1.Container) { c.ReadOnlyRoot = true },
		"other capabilities": func(_ *testing.T, _ *nodetest.Node, c *nodev1.Container) { c.Capabilities = caps },
		"an API":             func(_ *testing.T, _ *nodetest.Node, c *nodev1.Container) { c.Api = "moostack.app.v1.App" },
	} {
		t.Run(name, func(t *testing.T) {
			n := nodetest.Start(t)
			n.EventuallyHealthy(t)
			n.Import(t, nodetest.Image{Name: appImage})
			lease := hold(t, n)
			c := app("app-web", appImage)
			mustRun(t, n, lease, c)
			pid := containerPID(t, n, "app-web")
			change(t, n, c)
			mustRun(t, n, lease, c)
			if again := containerPID(t, n, "app-web"); again == pid {
				t.Errorf("app-web runs on as pid %s, rather than replaced", pid)
			}
			digest, _ := imageOf(t, n, c.Image)
			if got := eventuallyRunning(t, n, "app-web"); got.Image != c.Image || got.Digest != digest {
				t.Errorf("the agent reports %v, want it from %s at %s", got, c.Image, digest)
			}
		})
	}
}

// A replaced container's image goes, unless another container runs
// from it.
func TestReplacingAContainerDeletesItsOldImageUnlessAnotherRunsFromIt(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.Import(t, nodetest.Image{Name: appImage})
	n.Import(t, nodetest.Image{Name: thirdImage})
	lease := hold(t, n)
	mustRun(t, n, lease, app("app-web", appImage))
	mustRun(t, n, lease, app("app-api", appImage))
	mustRun(t, n, lease, app("app-web", thirdImage))
	if !hasImage(t, n, appImage) {
		t.Errorf("%s is gone, although app-api runs from it", appImage)
	}
	mustRun(t, n, lease, app("app-api", thirdImage))
	if hasImage(t, n, appImage) {
		t.Errorf("%s is kept, although no container runs from it", appImage)
	}
}

func TestARunFromAnImageTheNodeLacksLeavesTheContainerRunning(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.Import(t, nodetest.Image{Name: appImage})
	lease := hold(t, n)
	mustRun(t, n, lease, app("app-web", appImage))
	pid := containerPID(t, n, "app-web")
	if err := run(t, n, lease, app("app-web", otherImage)); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("running app-web from an image the node lacks: %v, want it refused", err)
	}
	if again := containerPID(t, n, "app-web"); again != pid {
		t.Errorf("app-web ran as pid %s, then as %s", pid, again)
	}
	if got := containerStatus(t, n, "app-web"); got.Image != appImage || got.State != nodev1.TaskState_TASK_STATE_RUNNING {
		t.Errorf("the agent reports %v, want it running from %s", got, appImage)
	}
}

func TestARunGoesOnWhenItsCallEnds(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.Import(t, nodetest.Image{Name: appImage, Slow: true})
	lease := hold(t, n)
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if _, err := n.Agent.Run(ctx, &nodev1.RunRequest{Container: app("app-web", appImage), Lease: lease}); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("running app-web from the slow image within 50ms: %v, want the call to end first", err)
	}
	nodetest.Eventually(t, time.Minute, func() error {
		if got := containerStatus(t, n, "app-web"); got.GetState() != nodev1.TaskState_TASK_STATE_RUNNING {
			return fmt.Errorf("app-web isn't running: %v", got)
		}
		return nil
	})
}

// The image a run that goes on after its call ended replaced, imported
// again by the next operation meanwhile, is kept, and runs: the import
// waits for the run, and the image, updated since it became unused, is
// no longer to delete.
func TestAnImportDuringARunsCleanupKeepsTheImage(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.Import(t, nodetest.Image{Name: appImage})
	n.Import(t, nodetest.Image{Name: thirdImage, Slow: true})
	lease := hold(t, n)
	mustRun(t, n, lease, app("app-web", appImage))
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if _, err := n.Agent.Run(ctx, &nodev1.RunRequest{Container: app("app-web", thirdImage), Lease: lease}); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("running app-web from the slow image within 50ms: %v, want the call to end first", err)
	}
	n.Import(t, nodetest.Image{Name: appImage})
	if err := run(t, n, lease, app("app-api", appImage)); err != nil {
		t.Errorf("running app-api from %s, imported while the run that replaced it went on: %v", appImage, err)
	}
	// The run whose call ended went on, so the import met its cleanup.
	if got := eventuallyRunning(t, n, "app-web"); got.Image != thirdImage {
		t.Errorf("the agent reports %v, want app-web from %s, as the run whose call ended left it", got, thirdImage)
	}
}

func TestAContainersOutputGoesToItsLogWhichStartsAgainWhenItIsReplacedOrRemoved(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.Import(t, nodetest.Image{Name: appImage})
	lease := hold(t, n)
	log := hostlayout.ContainerLogDir + "/app-web.log"
	wantLog := func(want string) {
		t.Helper()
		nodetest.Eventually(t, 10*time.Second, func() error {
			got, err := n.Output("root", "cat", log)
			if err != nil {
				return err
			}
			if got != want {
				return fmt.Errorf("app-web's log is %q, want %q", got, want)
			}
			return nil
		})
	}
	mustRun(t, n, lease, app("app-web", appImage))
	wantLog("running as 0:0\n")
	n.Import(t, nodetest.Image{Name: otherImage, User: "1000:1000"})
	mustRun(t, n, lease, app("app-web", otherImage))
	wantLog("running as 1000:1000\n")
	if err := removeContainer(t, n, lease, "app-web"); err != nil {
		t.Fatal(err)
	}
	wantLog("")
}

func TestRemoveDeletesAContainerWithItsSnapshotAndImage(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.Import(t, nodetest.Image{Name: appImage})
	lease := hold(t, n)
	mustRun(t, n, lease, app("app-web", appImage))
	pid := containerPID(t, n, "app-web")
	if err := removeContainer(t, n, lease, "app-web"); err != nil {
		t.Fatal(err)
	}
	if got := containerStatus(t, n, "app-web"); got != nil {
		t.Errorf("the agent reports the removed container: %v", got)
	}
	if _, err := n.Output("root", "test", "-e", "/proc/"+pid); err == nil {
		t.Errorf("app-web runs on as pid %s", pid)
	}
	client := n.Containerd(t)
	ctx := namespaces.WithNamespace(t.Context(), hostlayout.ContainerdNamespace)
	if _, err := client.LoadContainer(ctx, "app-web"); !errdefs.IsNotFound(err) {
		t.Errorf("loading the removed container: %v, want it gone", err)
	}
	if _, err := client.SnapshotService(defaults.DefaultSnapshotter).Stat(ctx, "app-web"); !errdefs.IsNotFound(err) {
		t.Errorf("the removed container's snapshot: %v, want it gone", err)
	}
	if hasImage(t, n, appImage) {
		t.Errorf("%s is kept, although no container runs from it", appImage)
	}
	// It stays removed across a restart of the agent.
	n.Restart(t, "agent")
	n.EventuallyHealthy(t)
	time.Sleep(3 * time.Second)
	if got := containerStatus(t, n, "app-web"); got != nil {
		t.Errorf("the agent reports the removed container after a restart: %v", got)
	}
}

func TestRemoveKeepsAnImageAnotherContainerRunsFrom(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.Import(t, nodetest.Image{Name: appImage})
	lease := hold(t, n)
	mustRun(t, n, lease, app("app-web", appImage))
	mustRun(t, n, lease, app("app-api", appImage))
	if err := removeContainer(t, n, lease, "app-web"); err != nil {
		t.Fatal(err)
	}
	if !hasImage(t, n, appImage) {
		t.Errorf("%s is gone, although app-api runs from it", appImage)
	}
	if got := eventuallyRunning(t, n, "app-api"); got.Image != appImage {
		t.Errorf("the agent reports %v", got)
	}
}

func TestRemovingAContainerTheNodeDoesntKeepChangesNothing(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.StartServices(t)
	before, err := n.Status(t)
	if err != nil {
		t.Fatal(err)
	}
	if err := removeContainer(t, n, hold(t, n), "app-web"); err != nil {
		t.Errorf("removing a container the node doesn't keep: %v", err)
	}
	after, err := n.Status(t)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(&nodev1.StatusResponse{Containers: before.Containers}, &nodev1.StatusResponse{Containers: after.Containers}) {
		t.Errorf("the containers were %v, and are %v once a container the node doesn't keep was removed", before.Containers, after.Containers)
	}
}

// Run and Remove are refused without the lease that holds the operation
// lock, whatever else holds it, changing nothing.
func TestRunAndRemoveAreRefusedWithoutTheLeaseThatHoldsTheLock(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.Import(t, nodetest.Image{Name: appImage})
	n.Import(t, nodetest.Image{Name: thirdImage})
	mustRun(t, n, hold(t, n), app("app-web", appImage))
	pid := containerPID(t, n, "app-web")
	breakLock(t, n)
	acquire(t, n, "expired", 100*time.Millisecond)
	time.Sleep(200 * time.Millisecond)
	acquire(t, n, "holder", time.Hour)
	for _, lease := range []string{"", "foreign", "expired"} {
		if err := run(t, n, lease, app("app-web", thirdImage)); status.Code(err) != codes.FailedPrecondition {
			t.Errorf("replacing app-web with the lease %q: %v, want it refused", lease, err)
		}
		if err := run(t, n, lease, app("app-api", thirdImage)); status.Code(err) != codes.FailedPrecondition {
			t.Errorf("running app-api with the lease %q: %v, want it refused", lease, err)
		}
		if err := removeContainer(t, n, lease, "app-web"); status.Code(err) != codes.FailedPrecondition {
			t.Errorf("removing app-web with the lease %q: %v, want it refused", lease, err)
		}
	}
	if again := containerPID(t, n, "app-web"); again != pid {
		t.Errorf("app-web ran as pid %s, then as %s", pid, again)
	}
	s, err := n.Status(t)
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Containers) != 1 || s.Containers[0].Image != appImage {
		t.Errorf("the agent reports %v, want app-web alone, from %s", s.Containers, appImage)
	}
	if !hasImage(t, n, thirdImage) || !hasImage(t, n, appImage) {
		t.Errorf("an image went")
	}
}

func TestAContainerRunsOnAcrossRestartsOfTheAgentAndContainerd(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.Import(t, nodetest.Image{Name: appImage})
	mustRun(t, n, hold(t, n), app("app-web", appImage))
	pid := containerPID(t, n, "app-web")
	for _, program := range []string{"agent", "containerd"} {
		n.Restart(t, program)
		n.EventuallyHealthy(t)
		if got := eventuallyRunning(t, n, "app-web"); got.Image != appImage {
			t.Errorf("after %s restarted, the agent reports %v", program, got)
		}
		if again := containerPID(t, n, "app-web"); again != pid {
			t.Errorf("app-web ran as pid %s, then as %s after %s restarted", pid, again, program)
		}
	}
}

func TestTheAgentStartsTheContainersAgainAfterTheNodeReboots(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.StartServices(t)
	n.Import(t, nodetest.Image{Name: appImage})
	mustRun(t, n, hold(t, n), app("app-web", appImage))
	// As after the node rebooted: the containers' processes are gone, and
	// the agent starts again.
	n.Kill(t, "agent")
	n.Exec(t, "root", "sh", "-c", "kill -KILL "+containerPID(t, n, "app-web"))
	for _, s := range release.ClusterServices {
		killService(t, n, s.Name)
	}
	n.Restart(t, "agent")
	n.EventuallyHealthy(t)
	eventuallyRunning(t, n, "app-web")
	for _, s := range release.ClusterServices {
		if got := eventuallyRunning(t, n, s.Name); got.Health != nodev1.Health_HEALTH_HEALTHY {
			t.Errorf("the agent reports %v after it started again", got)
		}
	}
}

// A container's API is another's while that one serves it.
func TestRunningAContainerOfAnAPIAnotherServesIsRefused(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.StartServices(t)
	n.Import(t, nodetest.Image{Name: appImage})
	c := app("app-web", appImage)
	c.Api = nodetest.Service("dns").Api
	if err := run(t, n, hold(t, n), c); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("running a container of dns's API: %v, want it refused", err)
	}
	if got := containerStatus(t, n, "app-web"); got != nil {
		t.Errorf("the agent reports %v", got)
	}
}

// The largest container Run accepts, with every capability, the longest
// API and the highest uid, is one containerd makes and runs.
func TestRunMakesTheLargestContainerItAccepts(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.Import(t, nodetest.Image{Name: appImage})
	user := "2147483647:2147483647"
	c := app("app-web", appImage)
	c.User, c.Api = &user, "a"+strings.Repeat(".b", 127)
	c.Capabilities = &nodev1.Capabilities{Names: allCapabilities}
	mustRun(t, n, hold(t, n), c)
	if got := eventuallyRunning(t, n, "app-web"); got.Image != appImage {
		t.Errorf("the agent reports %v", got)
	}
	wantStatusLines(t, n, containerPID(t, n, "app-web"), "Uid:\t2147483647\t2147483647\t2147483647\t2147483647")
}

// allCapabilities are the capabilities Linux has, as runc names them.
var allCapabilities = []string{
	"CAP_AUDIT_CONTROL", "CAP_AUDIT_READ", "CAP_AUDIT_WRITE", "CAP_BLOCK_SUSPEND", "CAP_BPF",
	"CAP_CHECKPOINT_RESTORE", "CAP_CHOWN", "CAP_DAC_OVERRIDE", "CAP_DAC_READ_SEARCH", "CAP_FOWNER",
	"CAP_FSETID", "CAP_IPC_LOCK", "CAP_IPC_OWNER", "CAP_KILL", "CAP_LEASE",
	"CAP_LINUX_IMMUTABLE", "CAP_MAC_ADMIN", "CAP_MAC_OVERRIDE", "CAP_MKNOD", "CAP_NET_ADMIN",
	"CAP_NET_BIND_SERVICE", "CAP_NET_BROADCAST", "CAP_NET_RAW", "CAP_PERFMON", "CAP_SETFCAP",
	"CAP_SETGID", "CAP_SETPCAP", "CAP_SETUID", "CAP_SYSLOG", "CAP_SYS_ADMIN",
	"CAP_SYS_BOOT", "CAP_SYS_CHROOT", "CAP_SYS_MODULE", "CAP_SYS_NICE", "CAP_SYS_PACCT",
	"CAP_SYS_PTRACE", "CAP_SYS_RAWIO", "CAP_SYS_RESOURCE", "CAP_SYS_TIME", "CAP_SYS_TTY_CONFIG",
	"CAP_WAKE_ALARM",
}

// A run waiting for another to end is refused if its lease ran out
// meanwhile.
func TestARunWhoseLeaseRunsOutWhileItWaitsIsRefused(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.Import(t, nodetest.Image{Name: appImage, Slow: true})
	n.Import(t, nodetest.Image{Name: thirdImage})
	acquire(t, n, "short", 2*time.Second)
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if _, err := n.Agent.Run(ctx, &nodev1.RunRequest{Container: app("app-web", appImage), Lease: "short"}); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("running app-web from the slow image within 50ms: %v, want the call to end first", err)
	}
	start := time.Now()
	err := run(t, n, "short", app("app-api", thirdImage))
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("running app-api after waiting %s for the slow run: %v, want it refused, its lease run out", time.Since(start), err)
	}
	if time.Since(start) < 2*time.Second {
		t.Fatalf("the run waited %s, less than the lease had left: the slow run didn't hold it", time.Since(start))
	}
	eventuallyRunning(t, n, "app-web")
	if got := containerStatus(t, n, "app-api"); got != nil {
		t.Errorf("the agent reports %v", got)
	}
}

func TestARemovalGoesOnWhenItsCallEnds(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.Import(t, nodetest.Image{Name: appImage})
	lease := hold(t, n)
	mustRun(t, n, lease, app("app-web", appImage))
	ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond)
	defer cancel()
	n.Agent.Remove(ctx, &nodev1.RemoveRequest{Name: "app-web", Lease: lease})
	nodetest.Eventually(t, 30*time.Second, func() error {
		if got := containerStatus(t, n, "app-web"); got != nil {
			return fmt.Errorf("the agent reports %v", got)
		}
		return nil
	})
	if hasImage(t, n, appImage) {
		t.Errorf("%s is kept", appImage)
	}
}

// While containerd doesn't answer, status lists every container it keeps,
// of a state and image it can't tell, saying why.
func TestStatusListsTheContainersWhileContainerdIsDown(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.Import(t, nodetest.Image{Name: appImage})
	mustRun(t, n, hold(t, n), app("app-web", appImage))
	n.Kill(t, "containerd")
	nodetest.Eventually(t, 30*time.Second, func() error {
		got := containerStatus(t, n, "app-web")
		if got.GetState() != nodev1.TaskState_TASK_STATE_UNSPECIFIED || got.Image != "" || got.Problem == "" {
			return fmt.Errorf("the agent reports %v, want app-web of a state and image it can't tell, saying why", got)
		}
		return nil
	})
}

// A container whose process can't start is reported failed, saying why.
func TestStatusReportsAContainerThatFailedToStart(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.ImportFixtures(t)
	if err := run(t, n, hold(t, n), app("app-web", nodetest.Broken().Image)); err == nil {
		t.Fatal("running app-web from an image without its program succeeded")
	}
	got := containerStatus(t, n, "app-web")
	if got.GetState() != nodev1.TaskState_TASK_STATE_FAILED || !strings.Contains(got.Problem, "starting it") || got.Health != nodev1.Health_HEALTH_UNSPECIFIED {
		t.Errorf("the agent reports %v, want it failed, saying why", got)
	}
}

// Runs, removals, imports and status at once, on containers of other
// names, each end as alone.
func TestRunsRemovalsImportsAndStatusAtOnce(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.Import(t, nodetest.Image{Name: appImage})
	n.Import(t, nodetest.Image{Name: thirdImage})
	lease := hold(t, n)
	for _, name := range []string{"app-a", "app-b"} {
		mustRun(t, n, lease, app(name, thirdImage))
	}
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for _, name := range []string{"app-c", "app-d", "app-e"} {
		wg.Go(func() { errs <- run(t, n, lease, app(name, appImage)) })
	}
	for _, name := range []string{"app-a", "app-b"} {
		wg.Go(func() { errs <- removeContainer(t, n, lease, name) })
	}
	wg.Go(func() { n.Import(t, nodetest.Image{Name: otherImage}) })
	for range 3 {
		wg.Go(func() {
			_, err := n.Status(t)
			errs <- err
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Error(err)
		}
	}
	s, err := n.Status(t)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, c := range s.Containers {
		got = append(got, c.Name+" "+c.Image)
	}
	want := []string{"app-c " + appImage, "app-d " + appImage, "app-e " + appImage}
	if !slices.Equal(got, want) {
		t.Errorf("the agent reports %v, want %v", got, want)
	}
	if hasImage(t, n, thirdImage) || !hasImage(t, n, appImage) {
		t.Errorf("the node holds %s: %v and %s: %v, want only the second", thirdImage, hasImage(t, n, thirdImage), appImage, hasImage(t, n, appImage))
	}
}

// Status asks each container's API for its health once, all at once, so
// that APIs slow to answer keep it within the updater's bound on an
// agent's status, 2 s.
func TestStatusAsksEachAPIOnceAllAtOnce(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	lease := hold(t, n)
	nobody := "65534:65534"
	for _, name := range []string{"dns", "proxy", "registry"} {
		svc := nodetest.Service(name)
		svc.Image = "example.test/slow-" + name + ":1"
		n.Import(t, nodetest.Image{Name: svc.Image, API: svc.Api, HealthDelay: 1200 * time.Millisecond})
		mustRun(t, n, lease, &nodev1.Container{Name: "service-" + name, Image: svc.Image, User: &nobody, ReadOnlyRoot: true, Capabilities: &nodev1.Capabilities{}, Api: svc.Api})
	}
	nodetest.Eventually(t, 30*time.Second, func() error {
		start := time.Now()
		s, err := n.Status(t)
		took := time.Since(start)
		if err != nil {
			return err
		}
		var healthy int
		for _, c := range s.Containers {
			if c.Health == nodev1.Health_HEALTH_HEALTHY {
				healthy++
			}
		}
		if healthy != 3 {
			return fmt.Errorf("%d of the 3 services are healthy: %v", healthy, s.Containers)
		}
		if took >= 2*time.Second {
			t.Fatalf("status took %s with three APIs answering Health in 1.2 s, want it within 2 s", took)
		}
		// Quicker than an API answers is a status that didn't wait for
		// them, which proves nothing of how it waits.
		if took < 1200*time.Millisecond {
			t.Fatalf("status took %s, less than the 1.2 s the APIs take to answer Health", took)
		}
		return nil
	})
}

// A container replaced by one that serves no API loses the socket
// directory the one before served its API from.
func TestAContainerThatNoLongerServesAnAPILosesItsSocketDirectory(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	const api = "moostack.probe.v1.Probe"
	n.Import(t, nodetest.Image{Name: appImage, API: api})
	lease := hold(t, n)
	c := app("app-web", appImage)
	c.Api = api
	mustRun(t, n, lease, c)
	eventuallyRunning(t, n, "app-web")
	n.Import(t, nodetest.Image{Name: thirdImage})
	mustRun(t, n, lease, app("app-web", thirdImage))
	eventuallyRunning(t, n, "app-web")
	if out, err := n.Output("root", "ls", "-la", hostlayout.ContainersDir+"/app-web"); err == nil {
		t.Errorf("app-web serves no API, but its socket directory is kept:\n%s", out)
	}
}

// A run of a container that can't be made is refused as an invalid
// argument before anything changes: the container of its name runs on.
func TestRunRefusesAnInvalidContainerBeforeAnythingChanges(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.Import(t, nodetest.Image{Name: appImage})
	lease := hold(t, n)
	mustRun(t, n, lease, app("app-web", appImage))
	pid := containerPID(t, n, "app-web")
	invalid := app("app-web", appImage)
	invalid.Capabilities = &nodev1.Capabilities{Names: []string{"CAP_MADE_UP"}}
	if err := run(t, n, lease, invalid); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("running app-web with a capability Linux lacks: %v, want it refused as an invalid argument", err)
	}
	if again := containerPID(t, n, "app-web"); again != pid {
		t.Errorf("app-web ran as pid %s, then as %s after the refused run", pid, again)
	}
}

// A container given a user runs as it, though its image names a user the
// image lacks, which it never runs as, and in its own group alone.
func TestRunGivesAContainerItsUserThoughItsImageNamesOneItLacks(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	// Root, which the image config is read as once its user is left out,
	// is in groups of the image's that the given user isn't.
	n.Import(t, nodetest.Image{Name: appImage, User: "nosuchuser", Group: "root:x:0:\ndisk:x:6:root\nwheel:x:10:root\n"})
	user := "1000:1001"
	c := app("app-web", appImage)
	c.User = &user
	mustRun(t, n, hold(t, n), c)
	wantStatusLines(t, n, containerPID(t, n, "app-web"), "Uid:\t1000\t1000\t1000\t1000", "Gid:\t1001\t1001\t1001\t1001", "Groups:\t1001 ")
}

// A replacement whose image can't be unpacked leaves the old container,
// which runs on, and starts it again when the node reboots, rather than
// leaving the node without it until a run succeeds.
func TestAReplacementThatCantUnpackKeepsTheOldContainerAcrossAReboot(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.Import(t, nodetest.Image{Name: appImage})
	lease := hold(t, n)
	mustRun(t, n, lease, app("app-web", appImage))
	n.Import(t, nodetest.Image{Name: otherImage, Rebuild: 2})
	// Its layer goes, so that unpacking it fails.
	client := n.Containerd(t)
	ctx := namespaces.WithNamespace(t.Context(), hostlayout.ContainerdNamespace)
	img, err := client.GetImage(ctx, otherImage)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := images.Manifest(ctx, client.ContentStore(), img.Target(), nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, layer := range manifest.Layers {
		if err := client.ContentStore().Delete(ctx, layer.Digest); err != nil {
			t.Fatal(err)
		}
	}
	if err := run(t, n, lease, app("app-web", otherImage)); err == nil {
		t.Fatal("app-web ran from an image whose layer is gone")
	}
	if got := eventuallyRunning(t, n, "app-web"); got.Image != appImage {
		t.Fatalf("after the failed replacement, the agent reports %v, want app-web from %s", got, appImage)
	}
	// As after the node rebooted.
	n.Kill(t, "agent")
	n.Exec(t, "root", "sh", "-c", "kill -KILL "+containerPID(t, n, "app-web"))
	n.Restart(t, "agent")
	n.EventuallyHealthy(t)
	if got := eventuallyRunning(t, n, "app-web"); got.Image != appImage {
		t.Errorf("after the reboot, the agent reports %v, want app-web from %s", got, appImage)
	}
}
