//go:build linux

package agent_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/agent"
	"github.com/moostackhq/moostack/internal/hostlayout"
	"github.com/moostackhq/moostack/internal/nodeclient"
	"github.com/moostackhq/moostack/internal/nodetest"
	"github.com/moostackhq/moostack/internal/release"
)

// Broken updaters, as shell scripts.
var (
	// exitsAtOnce never serves.
	exitsAtOnce = []byte("#!/bin/sh\nexit 1\n")
	// neverServes runs, but never listens.
	neverServes = []byte("#!/bin/sh\nexec sleep infinity\n")
)

// startsSlowly is NextUpdater, which starts only after longer than the
// agent gives its calls to finish once told to stop.
var startsSlowly = []byte(fmt.Sprintf("#!/bin/sh\nsleep %d\nexec %s \"$@\"\n", int(agent.StopTimeout/time.Second)+3, nodetest.NextUpdaterPath))

func TestUpdateUpdaterSwapsToTheNewUpdaterAndBack(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	if err := updateUpdater(t, n, nodetest.NextUpdater(t), nodetest.NextVersion, 30*time.Second); err != nil {
		t.Fatalf("updating the updater to %s: %v", nodetest.NextVersion, err)
	}
	wantUpdater(t, n, nodetest.NextVersion, hostlayout.UpdaterSlots[1])
	// The A/B slot it swapped from keeps the updater it replaced.
	if got, want := sum(t, n, hostlayout.UpdaterSlots[0]), sha(nodetest.Updater(t)); got != want {
		t.Errorf("%s's SHA-256 is %s, want the replaced updater's, %s", hostlayout.UpdaterSlots[0], got, want)
	}
	// A second update swaps to the other A/B slot again.
	if err := updateUpdater(t, n, nodetest.Updater(t), nodetest.Version, 30*time.Second); err != nil {
		t.Fatalf("updating the updater back to %s: %v", nodetest.Version, err)
	}
	wantUpdater(t, n, nodetest.Version, hostlayout.UpdaterSlots[0])
	wantNoUpdaterSwapRecord(t, n)
}

// A node that a release before the updater's A/B slots installed has
// the updater as a plain file, which the first update moves into its
// first A/B slot.
func TestUpdateUpdaterMovesAPlainUpdaterIntoItsFirstABSlot(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.Exec(t, "root", "mv", hostlayout.UpdaterSlots[0], hostlayout.Updater)
	if err := updateUpdater(t, n, nodetest.NextUpdater(t), nodetest.NextVersion, 30*time.Second); err != nil {
		t.Fatalf("updating a plain updater: %v", err)
	}
	wantUpdater(t, n, nodetest.NextVersion, hostlayout.UpdaterSlots[1])
	if got, want := sum(t, n, hostlayout.UpdaterSlots[0]), sha(nodetest.Updater(t)); got != want {
		t.Errorf("%s's SHA-256 is %s, want the plain updater's, %s", hostlayout.UpdaterSlots[0], got, want)
	}
	// A rollback switches back to the plain updater, now in its A/B slot.
	if err := updateUpdater(t, n, exitsAtOnce, "9.9.9-test", 5*time.Second); status.Code(err) != codes.Aborted {
		t.Errorf("updating to a broken updater: %v, want ABORTED", err)
	}
	wantUpdater(t, n, nodetest.NextVersion, hostlayout.UpdaterSlots[1])
}

func TestABrokenUpdaterIsRolledBack(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	for _, c := range []struct {
		name    string
		binary  []byte
		version string
		// want is in the error, which says why the new updater wasn't
		// healthy.
		want string
	}{
		{"an updater that exits at once", exitsAtOnce, nodetest.NextVersion, "the updater doesn't answer"},
		{"an updater that never serves", neverServes, nodetest.NextVersion, "the updater doesn't answer"},
		{"an updater that reports another version", nodetest.Updater(t), "9.9.9-test", "runs version " + nodetest.Version + ", not 9.9.9-test"},
	} {
		t.Run(c.name, func(t *testing.T) {
			err := updateUpdater(t, n, c.binary, c.version, 5*time.Second)
			if status.Code(err) != codes.Aborted || !strings.Contains(err.Error(), "rolled back to the updater it replaced") || !strings.Contains(err.Error(), c.want) {
				t.Errorf("updating to %s: %v, want ABORTED saying it rolled back, and %q", c.name, err, c.want)
			}
			wantUpdater(t, n, nodetest.Version, hostlayout.UpdaterSlots[0])
			wantNoUpdaterSwapRecord(t, n)
		})
	}
}

func TestAnUpdateOfTheUpdaterThatDoesntMatchItsChecksumChangesNothing(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	pid := pidOf(t, n, "updater")
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	stream, err := n.Agent.UpdateUpdater(ctx)
	if err != nil {
		t.Fatal(err)
	}
	err = stream.Send(&nodev1.UpdateUpdaterRequest{
		Update: &nodev1.UpdaterUpdate{Version: nodetest.NextVersion, Sha256: sha(nodetest.Updater(t)), HealthTimeout: durationpb.New(30 * time.Second)},
		Chunk:  nodetest.NextUpdater(t)[:1<<20],
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.CloseAndRecv(); status.Code(err) != codes.InvalidArgument {
		t.Errorf("an update whose binary doesn't match its checksum: %v, want INVALID_ARGUMENT", err)
	}
	if got := pidOf(t, n, "updater"); got != pid {
		t.Errorf("the updater's pid is %s, want %s: it was restarted", got, pid)
	}
	wantUpdater(t, n, nodetest.Version, hostlayout.UpdaterSlots[0])
	if out, err := n.Output("root", "ls", hostlayout.BinDir); err != nil || strings.Contains(out, filepath.Base(hostlayout.UpdaterSlots[1])) {
		t.Errorf("the bin directory holds %s, %v; want nothing in the updater's inactive A/B slot", out, err)
	}
}

func TestASecondUpdateOfTheUpdaterWhileOneRunsIsRefused(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	first := make(chan error, 1)
	go func() { first <- updateUpdater(t, n, neverServes, nodetest.NextVersion, 10*time.Second) }()
	waitForUpdaterLink(t, n, hostlayout.UpdaterSlots[1])
	if err := updateUpdater(t, n, nodetest.NextUpdater(t), nodetest.NextVersion, 10*time.Second); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("a second update while one runs: %v, want FAILED_PRECONDITION", err)
	}
	if err := <-first; status.Code(err) != codes.Aborted {
		t.Errorf("the first update: %v, want ABORTED", err)
	}
	wantUpdater(t, n, nodetest.Version, hostlayout.UpdaterSlots[0])
}

// An agent that crashes mid-swap switches the updater back once it
// starts again, while it serves, and refuses another update of the
// updater until then.
func TestAnAgentThatCrashesMidSwapSwitchesTheUpdaterBackWhenItStarts(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	done := make(chan error, 1)
	go func() { done <- updateUpdater(t, n, neverServes, nodetest.NextVersion, 20*time.Second) }()
	waitForUpdaterLink(t, n, hostlayout.UpdaterSlots[1])
	// Restarts hang, so that the agent that starts again can't switch
	// back yet.
	n.BreakRestarts(t, nodetest.RestartsHang)
	n.Crash(t, "agent")
	if err := <-done; err == nil {
		t.Error("the update succeeded, though the agent crashed before the new updater was healthy")
	}
	n.EventuallyHealthy(t)
	if err := updateUpdater(t, n, nodetest.NextUpdater(t), nodetest.NextVersion, 20*time.Second); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("an update while the agent switches the updater back: %v, want FAILED_PRECONDITION", err)
	}
	n.BreakRestarts(t, nodetest.RestartsWork)
	waitForNoUpdaterSwapRecord(t, n)
	wantUpdater(t, n, nodetest.Version, hostlayout.UpdaterSlots[0])
}

func TestAnAgentStoppedMidSwapOfTheUpdaterFinishesItFirst(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	done := make(chan error, 1)
	go func() {
		done <- nodeclient.UpdateUpdater(t.Context(), n.Agent, nodetest.NextVersion, startsSlowly, 30*time.Second)
	}()
	waitForUpdaterLink(t, n, hostlayout.UpdaterSlots[1])
	// Restart stops the agent as systemd does, with SIGTERM, and waits
	// for it to exit.
	n.Restart(t, "agent")
	<-done
	waitForNoUpdaterSwapRecord(t, n)
	wantUpdater(t, n, nodetest.NextVersion, hostlayout.UpdaterSlots[1])
}

func TestUpdateContainerdSwapsItsBinariesWhileContainersKeepRunning(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.StartServices(t)
	services := map[string]string{}
	for _, s := range release.ClusterServices {
		services[s.Name] = servicePID(t, n, s.Name)
	}
	containerd := pidOf(t, n, "containerd")
	inodes := containerdInodes(t, n)
	if err := updateContainerd(t, n, nodetest.Containerd(t), release.ContainerdVersion, 30*time.Second); err != nil {
		t.Fatalf("updating containerd: %v", err)
	}
	if got := pidOf(t, n, "containerd"); got == containerd {
		t.Errorf("containerd runs on as pid %s, want it restarted", got)
	}
	for f, inode := range containerdInodes(t, n) {
		if inode == inodes[f] {
			t.Errorf("%s is the file it was, want it replaced", f)
		}
	}
	for f, data := range nodetest.Containerd(t) {
		if got := sum(t, n, hostlayout.BinDir+"/"+f); got != sha(data) {
			t.Errorf("%s's SHA-256 is %s, want the update's, %s", f, got, sha(data))
		}
	}
	// The cluster services' containers ran on through their shims.
	for name, pid := range services {
		if got := servicePID(t, n, name); got != pid {
			t.Errorf("%s ran as pid %s, and as %s after containerd's update, want it to run on", name, pid, got)
		}
	}
	s := n.EventuallyHealthy(t)
	for _, c := range s.Components {
		if c.Name == "containerd" && c.Version != release.ContainerdVersion {
			t.Errorf("containerd runs %s, want %s", c.Version, release.ContainerdVersion)
		}
	}
	// The new containerd knows the containers: replacing one stops it.
	before := servicePID(t, n, "dns")
	moved := nodetest.Service("dns")
	moved.Api = otherAPI
	if _, err := n.Agent.RunService(t.Context(), &nodev1.RunServiceRequest{Service: moved}); err != nil {
		t.Fatalf("replacing dns after containerd's update: %v", err)
	}
	if after := servicePID(t, n, "dns"); after == before {
		t.Errorf("dns runs on as pid %s, rather than being replaced", after)
	}
}

func TestAContainerdUpdateThatIsRefusedChangesNothing(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	containerd := pidOf(t, n, "containerd")
	inodes := containerdInodes(t, n)
	files := nodetest.Containerd(t)
	// update sends only the first MiB of each file, which is enough for
	// each refusal: the sums are checked once the files have come.
	update := func(version string, files map[string][]byte, sums map[string]string) error {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		stream, err := n.Agent.UpdateContainerd(ctx)
		if err != nil {
			t.Fatal(err)
		}
		u := &nodev1.ContainerdUpdate{Version: version, HealthTimeout: durationpb.New(10 * time.Second)}
		for name, sum := range sums {
			u.Files = append(u.Files, &nodev1.ContainerdFile{Name: name, Sha256: sum})
		}
		for name, data := range files {
			if stream.Send(&nodev1.UpdateContainerdRequest{Update: u, File: name, Chunk: data[:min(len(data), 1<<20)]}) != nil {
				break
			}
			u = nil
		}
		_, err = stream.CloseAndRecv()
		return err
	}
	shim := hostlayout.ContainerdShim
	sums := map[string]string{"containerd": sha(files["containerd"]), shim: sha(files[shim])}
	for _, c := range []struct {
		name  string
		files map[string][]byte
		sums  map[string]string
		want  string
	}{
		{"binaries that don't match their checksums", files, map[string]string{"containerd": sums[shim], shim: sums["containerd"]}, "SHA-256"},
		{"a file outside containerd's", map[string][]byte{"containerd": files["containerd"], shim: files[shim], "../../etc/x": []byte("x")},
			map[string]string{"containerd": sums["containerd"], shim: sums[shim], "../../etc/x": sha([]byte("x"))}, "isn't one of containerd's binaries"},
		{"runc", map[string][]byte{"containerd": files["containerd"], shim: files[shim], "runc": []byte("x")},
			map[string]string{"containerd": sums["containerd"], shim: sums[shim], "runc": sha([]byte("x"))}, "isn't one of containerd's binaries"},
		{"no shim", map[string][]byte{"containerd": files["containerd"]}, map[string]string{"containerd": sums["containerd"]}, "the update has no " + shim},
		{"a chunk of a file it doesn't list", map[string][]byte{"containerd": files["containerd"], "runc": []byte("x")}, sums, "isn't one of the update's files"},
	} {
		t.Run(c.name, func(t *testing.T) {
			err := update(release.ContainerdVersion, c.files, c.sums)
			if status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), c.want) {
				t.Errorf("an update with %s: %v, want INVALID_ARGUMENT saying %q", c.name, err, c.want)
			}
		})
	}
	if got := pidOf(t, n, "containerd"); got != containerd {
		t.Errorf("containerd's pid is %s, want %s: it was restarted", got, containerd)
	}
	if got := containerdInodes(t, n); !maps.Equal(got, inodes) {
		t.Errorf("containerd's binaries were replaced by an update that was refused")
	}
	out, err := n.Output("root", "sh", "-c", "for f in "+filepath.Join(hostlayout.BinDir, "../../etc/x")+"* "+hostlayout.BinDir+"/*.new; do [ -e \"$f\" ] && echo \"$f\"; done; true")
	if err != nil || strings.TrimSpace(out) != "" {
		t.Errorf("a refused update left %q, %v", out, err)
	}
}

// Binaries that are part old and part new, as when the host restarted
// in the middle of putting them in place, show containerd unhealthy, and
// an update puts them in place again.
func TestStatusShowsContainerdUnhealthyWhenItsShimIsAnotherVersion(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	shim := filepath.Join(hostlayout.BinDir, hostlayout.ContainerdShim)
	n.Exec(t, "root", "sh", "-c", "printf '#!/bin/sh\\necho containerd-shim-runc-v2:\\necho \"  Version:  v2.3.0\"\\n' >"+shim+".x && chmod 755 "+shim+".x && mv "+shim+".x "+shim)
	s, err := n.Status(t)
	if err != nil {
		t.Fatal(err)
	}
	if c := component(t, s, "containerd"); c.Health != nodev1.Health_HEALTH_UNHEALTHY || !strings.Contains(c.Problem, "2.3.0") {
		t.Errorf("with its shim at 2.3.0, containerd is %s: %q, want unhealthy saying the shim's version", c.Health, c.Problem)
	}
	if err := updateContainerd(t, n, nodetest.Containerd(t), release.ContainerdVersion, 30*time.Second); err != nil {
		t.Fatalf("updating containerd: %v", err)
	}
	n.EventuallyHealthy(t)
}

// A node whose shim is gone, as when someone removed it, shows containerd
// unhealthy, and an update puts it in place again.
func TestUpdateContainerdPutsAMissingShimInPlace(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.Exec(t, "root", "rm", filepath.Join(hostlayout.BinDir, hostlayout.ContainerdShim))
	if s, err := n.Status(t); err != nil {
		t.Fatal(err)
	} else if c := component(t, s, "containerd"); c.Health == nodev1.Health_HEALTH_HEALTHY {
		t.Errorf("without its shim, containerd is healthy, want it unhealthy")
	}
	if err := updateContainerd(t, n, nodetest.Containerd(t), release.ContainerdVersion, 30*time.Second); err != nil {
		t.Fatalf("updating containerd without its shim: %v", err)
	}
	n.EventuallyHealthy(t)
}

func TestABrokenContainerdIsRolledBack(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.StartServices(t)
	services := map[string]string{}
	for _, s := range release.ClusterServices {
		services[s.Name] = servicePID(t, n, s.Name)
	}
	before := map[string]string{}
	for f := range nodetest.Containerd(t) {
		before[f] = sum(t, n, hostlayout.BinDir+"/"+f)
	}
	exits := nodetest.Containerd(t)
	exits["containerd"] = exitsAtOnce
	for _, c := range []struct {
		name    string
		files   map[string][]byte
		version string
		// want is in the error, which says why the new containerd wasn't
		// healthy.
		want string
	}{
		{"a containerd that exits at once", exits, release.ContainerdVersion, "containerd is unhealthy"},
		{"a containerd that reports another version", nodetest.Containerd(t), "9.9.9", "containerd runs version " + release.ContainerdVersion + ", not 9.9.9"},
	} {
		t.Run(c.name, func(t *testing.T) {
			err := updateContainerd(t, n, c.files, c.version, 5*time.Second)
			if status.Code(err) != codes.Aborted || !strings.Contains(err.Error(), "rolled back to the containerd it replaced") || !strings.Contains(err.Error(), c.want) {
				t.Errorf("updating to %s: %v, want ABORTED saying it rolled back, and %q", c.name, err, c.want)
			}
			for f, want := range before {
				if got := sum(t, n, hostlayout.BinDir+"/"+f); got != want {
					t.Errorf("%s's SHA-256 is %s after the rollback, want the one it replaced, %s", f, got, want)
				}
			}
			s := n.EventuallyHealthy(t)
			for _, c := range s.Components {
				if c.Name == "containerd" && c.Version != release.ContainerdVersion {
					t.Errorf("containerd runs %s after the rollback, want %s", c.Version, release.ContainerdVersion)
				}
			}
		})
	}
	for name, pid := range services {
		if got := servicePID(t, n, name); got != pid {
			t.Errorf("%s ran as pid %s, and as %s after the rollbacks, want it to run on", name, pid, got)
		}
	}
}

func TestASecondUpdateOfContainerdWhileOneRunsIsRefused(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	// The first update's restart hangs, until its health timeout.
	n.BreakRestarts(t, nodetest.RestartsHang)
	inodes := containerdInodes(t, n)
	first := make(chan error, 1)
	go func() {
		first <- updateContainerd(t, n, nodetest.Containerd(t), release.ContainerdVersion, 10*time.Second)
	}()
	nodetest.Eventually(t, 30*time.Second, func() error {
		if maps.Equal(containerdInodes(t, n), inodes) {
			return errors.New("the first update hasn't put containerd's binaries in place")
		}
		return nil
	})
	if err := updateContainerd(t, n, nodetest.Containerd(t), release.ContainerdVersion, 10*time.Second); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("a second update while one runs: %v, want FAILED_PRECONDITION", err)
	}
	// Its rollback's restart hangs too.
	if err := <-first; status.Code(err) != codes.Internal || !strings.Contains(err.Error(), "restarting the new containerd") {
		t.Errorf("the update whose restarts hang: %v, want INTERNAL saying the restarts failed", err)
	}
}

// The swap renames containerd's new binaries over the old ones before it
// restarts containerd, so an agent that crashes in between leaves the
// old containerd running, and a rerun swaps again.
func TestAnAgentThatCrashesMidSwapOfContainerdLeavesItRunningAndARerunSwaps(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.StartServices(t)
	services := map[string]string{}
	for _, s := range release.ClusterServices {
		services[s.Name] = servicePID(t, n, s.Name)
	}
	containerd := pidOf(t, n, "containerd")
	// containerd's restart hangs before it stops containerd.
	n.BreakRestarts(t, nodetest.RestartsHang)
	inodes := containerdInodes(t, n)
	done := make(chan error, 1)
	go func() {
		done <- updateContainerd(t, n, nodetest.Containerd(t), release.ContainerdVersion, 30*time.Second)
	}()
	nodetest.Eventually(t, 30*time.Second, func() error {
		if maps.Equal(containerdInodes(t, n), inodes) {
			return errors.New("the update hasn't put containerd's binaries in place")
		}
		return nil
	})
	n.BreakRestarts(t, nodetest.RestartsWork)
	n.Crash(t, "agent")
	if err := <-done; err == nil {
		t.Error("the update succeeded, though the agent crashed before restarting containerd")
	}
	if got := pidOf(t, n, "containerd"); got != containerd {
		t.Errorf("containerd's pid is %s, want the one that ran, %s", got, containerd)
	}
	n.EventuallyHealthy(t)
	if err := updateContainerd(t, n, nodetest.Containerd(t), release.ContainerdVersion, 30*time.Second); err != nil {
		t.Fatalf("updating containerd again: %v", err)
	}
	if got := pidOf(t, n, "containerd"); got == containerd {
		t.Errorf("containerd runs on as pid %s after the rerun, want it restarted", got)
	}
	for name, pid := range services {
		if got := servicePID(t, n, name); got != pid {
			t.Errorf("%s ran as pid %s, and as %s after the crash and the rerun, want it to run on", name, pid, got)
		}
	}
	n.EventuallyHealthy(t)
}

// updateContainerd has the node's agent replace containerd's binaries
// with files, containerd at version.
func updateContainerd(t *testing.T, n *nodetest.Node, files map[string][]byte, version string, healthTimeout time.Duration) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	return nodeclient.UpdateContainerd(ctx, n.Agent, version, files, healthTimeout)
}

// containerdInodes are the inodes of containerd's binaries in the node,
// by name.
func containerdInodes(t *testing.T, n *nodetest.Node) map[string]string {
	t.Helper()
	inodes := map[string]string{}
	for _, f := range release.ContainerdFiles {
		out, err := n.Output("root", "stat", "-c", "%i", hostlayout.BinDir+"/"+f)
		if err != nil {
			t.Fatalf("stat %s: %v\n%s", f, err, out)
		}
		inodes[f] = strings.TrimSpace(out)
	}
	return inodes
}

// updateUpdater has the node's agent replace its updater with binary,
// the updater at version.
func updateUpdater(t *testing.T, n *nodetest.Node, binary []byte, version string, healthTimeout time.Duration) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	return nodeclient.UpdateUpdater(ctx, n.Agent, version, binary, healthTimeout)
}

// wantUpdater fails unless the node's updater reports version, healthy,
// and runs from slot.
func wantUpdater(t *testing.T, n *nodetest.Node, version, slot string) {
	t.Helper()
	nodetest.Eventually(t, 30*time.Second, func() error {
		s, err := n.UpdaterStatus(t)
		if err != nil {
			return err
		}
		if u := s.Updater; u.Version != version || u.Health.String() != "HEALTH_HEALTHY" {
			return errors.New("the updater reports " + u.String() + ", want " + version + ", healthy")
		}
		return nil
	})
	if got := updaterLink(t, n); got != filepath.Base(slot) {
		t.Errorf("%s links to %s, want %s", hostlayout.Updater, got, filepath.Base(slot))
	}
}

// wantNoUpdaterSwapRecord fails if the agent still records a swap of the
// updater.
func wantNoUpdaterSwapRecord(t *testing.T, n *nodetest.Node) {
	t.Helper()
	if out, err := n.Output("root", "test", "-e", hostlayout.UpdaterSwapFile); err == nil {
		t.Errorf("%s is still there once the update ended %s", hostlayout.UpdaterSwapFile, out)
	}
}

// waitForNoUpdaterSwapRecord waits until the agent no longer records a
// swap of the updater.
func waitForNoUpdaterSwapRecord(t *testing.T, n *nodetest.Node) {
	t.Helper()
	nodetest.Eventually(t, 30*time.Second, func() error {
		if _, err := n.Output("root", "test", "-e", hostlayout.UpdaterSwapFile); err == nil {
			return errors.New(hostlayout.UpdaterSwapFile + " is still there")
		}
		return nil
	})
}

// waitForUpdaterLink waits until the updater's link points to slot.
func waitForUpdaterLink(t *testing.T, n *nodetest.Node, slot string) {
	t.Helper()
	nodetest.Eventually(t, 30*time.Second, func() error {
		if got := updaterLink(t, n); got != filepath.Base(slot) {
			return errors.New(hostlayout.Updater + " links to " + got)
		}
		return nil
	})
}

// updaterLink is what the updater's link points to.
func updaterLink(t *testing.T, n *nodetest.Node) string {
	t.Helper()
	out, err := n.Output("root", "readlink", hostlayout.Updater)
	if err != nil {
		t.Fatalf("readlink %s: %v\n%s", hostlayout.Updater, err, out)
	}
	return strings.TrimSpace(out)
}

// sum is the SHA-256 of the file at path in the node, hex.
func sum(t *testing.T, n *nodetest.Node, path string) string {
	t.Helper()
	out, err := n.Output("root", "sha256sum", path)
	if err != nil {
		t.Fatalf("sha256sum %s: %v\n%s", path, err, out)
	}
	return strings.Fields(out)[0]
}

// sha is the SHA-256 of data, hex.
func sha(data []byte) string {
	s := sha256.Sum256(data)
	return hex.EncodeToString(s[:])
}

// pidOf is the pid of the node's program called name, as nodetest
// started it.
func pidOf(t *testing.T, n *nodetest.Node, name string) string {
	t.Helper()
	out, err := n.Output("root", "cat", "/run/"+name+".pid")
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(out)
}

// component is the node component called name that s reports, failing t
// if it reports none.
func component(t *testing.T, s *nodev1.StatusResponse, name string) *nodev1.Component {
	t.Helper()
	for _, c := range s.Components {
		if c.Name == name {
			return c
		}
	}
	t.Fatalf("the node reports no %s: %v", name, s.Components)
	return nil
}
