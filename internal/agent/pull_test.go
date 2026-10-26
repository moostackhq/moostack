//go:build linux

package agent_test

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	godigest "github.com/opencontainers/go-digest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/hostlayout"
	"github.com/moostackhq/moostack/internal/nodetest"
)

// These tests pull images from a registry in a container of its own,
// which the node trusts through its system trust store, as it would a
// public registry: the pull is production's, with no seam for the test.

// pull has n's agent pull ref for lease, within a minute.
func pull(t *testing.T, n *nodetest.Node, lease, ref string) (*nodev1.PullImageResponse, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	return n.Agent.PullImage(ctx, &nodev1.PullImageRequest{Reference: ref, Lease: lease})
}

// requestsFor are the requests the registry answered for repo so far.
func requestsFor(t *testing.T, r *nodetest.Registry, repo string) []string {
	t.Helper()
	var got []string
	for _, req := range r.Requests(t) {
		if strings.Contains(req, "/v2/"+repo+"/") {
			got = append(got, req)
		}
	}
	return got
}

// otherArch is an architecture other than this machine's.
func otherArch() string {
	if runtime.GOARCH == "arm64" {
		return "amd64"
	}
	return "arm64"
}

func TestPullImagePullsAPublicImageWhichRunsAsAnApp(t *testing.T) {
	n := nodetest.Start(t)
	r := nodetest.StartRegistry(t, n)
	n.EventuallyHealthy(t)
	ref, digest := r.Push(t, "team/web", "1", nodetest.Image{Name: "registry.test/team/web:1"})
	lease := hold(t, n)
	res, err := pull(t, n, lease, ref)
	if err != nil {
		t.Fatal(err)
	}
	if res.Image != ref || res.Digest != digest {
		t.Errorf("PullImage answered %s at %s, want %s at %s", res.Image, res.Digest, ref, digest)
	}
	mustRun(t, n, lease, app("app-web", ref))
	c := eventuallyRunning(t, n, "app-web")
	if c.Image != ref || c.Digest != digest {
		t.Errorf("the node reports app-web from %s at %s, want %s at %s", c.Image, c.Digest, ref, digest)
	}
}

// A pull of an image the node has fetches its manifest again, which says
// whether it changed, and none of its blobs.
func TestAPullAgainFetchesOnlyWhatIsMissing(t *testing.T) {
	n := nodetest.Start(t)
	r := nodetest.StartRegistry(t, n)
	n.EventuallyHealthy(t)
	ref, _ := r.Push(t, "web", "1", nodetest.Image{Name: "registry.test/web:1"})
	lease := hold(t, n)
	if _, err := pull(t, n, lease, ref); err != nil {
		t.Fatal(err)
	}
	before := len(requestsFor(t, r, "web"))
	if _, err := pull(t, n, lease, ref); err != nil {
		t.Fatal(err)
	}
	for _, req := range requestsFor(t, r, "web")[before:] {
		if strings.Contains(req, "/blobs/") {
			t.Errorf("the second pull fetched a blob: %s", req)
		}
	}
}

func TestPullImageRefusesAReferenceThatIsntNormalized(t *testing.T) {
	n := nodetest.Start(t)
	r := nodetest.StartRegistry(t, n)
	n.EventuallyHealthy(t)
	r.Push(t, "library/web", "latest", nodetest.Image{Name: "registry.test/library/web:latest"})
	lease := hold(t, n)
	for _, ref := range []string{"registry.test/library/web", "web", "web:latest", "docker.io/web:latest", ""} {
		if _, err := pull(t, n, lease, ref); status.Code(err) != codes.InvalidArgument {
			t.Errorf("PullImage of %q: %v, want INVALID_ARGUMENT", ref, err)
		}
	}
	if got := requestsFor(t, r, "library/web"); len(got) != 0 {
		t.Errorf("the registry was asked %q for references that aren't normalized", got)
	}
}

// A pull is refused without the operation's lease, pulling nothing.
func TestPullImageIsFencedByTheLease(t *testing.T) {
	n := nodetest.Start(t)
	r := nodetest.StartRegistry(t, n)
	n.EventuallyHealthy(t)
	ref, _ := r.Push(t, "web", "1", nodetest.Image{Name: "registry.test/web:1"})
	for name, lease := range map[string]string{"no lease": "", "while no lease holds the lock": nodetest.Lease} {
		if _, err := pull(t, n, lease, ref); status.Code(err) != codes.FailedPrecondition {
			t.Errorf("PullImage with %s: %v, want FAILED_PRECONDITION", name, err)
		}
	}
	acquire(t, n, "expired", 100*time.Millisecond)
	time.Sleep(200 * time.Millisecond)
	if _, err := pull(t, n, "expired", ref); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("PullImage with a lease that ran out: %v, want FAILED_PRECONDITION", err)
	}
	lease := hold(t, n)
	if _, err := pull(t, n, "another-operations", ref); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("PullImage with another lease: %v, want FAILED_PRECONDITION", err)
	}
	if got := requestsFor(t, r, "web"); len(got) != 0 || hasImage(t, n, ref) {
		t.Errorf("refused pulls asked the registry %q, and left the image: %v", got, hasImage(t, n, ref))
	}
	if _, err := pull(t, n, lease, ref); err != nil {
		t.Errorf("PullImage with the lease: %v", err)
	}
}

func TestPullImageTellsTheRegistrysFailuresApart(t *testing.T) {
	n := nodetest.Start(t)
	r := nodetest.StartRegistry(t, n)
	n.EventuallyHealthy(t)
	lease := hold(t, n)
	for fault, want := range map[string]codes.Code{"404": codes.NotFound, "401": codes.PermissionDenied, "429": codes.ResourceExhausted} {
		ref := r.Fault(t, "fault"+fault, fault)
		if _, err := pull(t, n, lease, ref); status.Code(err) != want {
			t.Errorf("PullImage from a registry answering %s: %v, want %s", fault, err, want)
		}
		if hasImage(t, n, ref) {
			t.Errorf("a pull from a registry answering %s left an image", fault)
		}
	}
	// A 429 is asked once, and not again at once, which would spend what
	// the registry allows the node.
	if got := requestsFor(t, r, "fault429"); len(got) != 1 {
		t.Errorf("a rate limited pull asked the registry %q, want once", got)
	}
	if _, err := pull(t, n, lease, "nowhere.invalid/web:1"); status.Code(err) != codes.Unavailable {
		t.Errorf("PullImage from a registry that doesn't resolve: %v, want UNAVAILABLE", err)
	}
}

// An image for another architecture fails at the pull, rather than once
// its process starts, whether the registry serves its manifest alone or
// an index without the node's platform.
func TestPullImageRefusesAnImageForAnotherPlatform(t *testing.T) {
	n := nodetest.Start(t)
	r := nodetest.StartRegistry(t, n)
	n.EventuallyHealthy(t)
	other := nodetest.Image{Name: "registry.test/other:1", Architecture: otherArch()}
	single, _ := r.Push(t, "other", "1", other)
	index := r.PushIndex(t, "other", "index", other)
	both := r.PushIndex(t, "both", "1", other, nodetest.Image{Name: "registry.test/both:1"})
	lease := hold(t, n)
	for ref, says := range map[string]string{
		single: "the image is for linux/" + otherArch() + ", not for this node's linux/" + runtime.GOARCH,
		index:  "it has no image for this node's linux/" + runtime.GOARCH,
	} {
		_, err := pull(t, n, lease, ref)
		if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), says) {
			t.Errorf("PullImage of %s, for %s alone: %v, want FAILED_PRECONDITION saying %q", ref, otherArch(), err, says)
		}
		if hasImage(t, n, ref) {
			t.Errorf("a refused pull of %s left its image", ref)
		}
	}
	if _, err := pull(t, n, lease, both); err != nil {
		t.Errorf("PullImage of an index with this machine's platform among others: %v", err)
	}
	mustRun(t, n, lease, app("app-both", both))
	eventuallyRunning(t, n, "app-both")
}

// A pull ends when its call ends, and keeps what it fetched until its
// lease expires, through garbage collection, so that a pull again fetches
// none of it, and finishes.
func TestAPullCutOffIsFinishedByAPullAgain(t *testing.T) {
	n := nodetest.Start(t)
	r := nodetest.StartRegistry(t, n)
	n.EventuallyHealthy(t)
	// The slow image's layer is large once unpacked and small to fetch,
	// so the call is cut off while the pull unpacks what it fetched.
	img := nodetest.Image{Name: "registry.test/slow:1", Slow: true}
	ref, manifest := r.Push(t, "slow", "1", img)
	lease := hold(t, n)
	// The call is cut off once containerd holds each of the image's blobs,
	// committed: one the registry served may not be stored whole yet. The
	// blobs and the client of containerd are ready before the pull starts,
	// so that the cut comes as soon as they are committed. The manifest is
	// left out: the pull stores it before it fetches the blobs it lists,
	// and fetches it by the manifests route, not the blobs route the pull
	// again is checked on.
	var blobs []godigest.Digest
	for _, d := range blobsOf(t, img) {
		if d.String() != manifest {
			blobs = append(blobs, d)
		}
	}
	store := n.Containerd(t).ContentStore()
	nsCtx := namespaces.WithNamespace(t.Context(), hostlayout.ContainerdNamespace)
	if _, err := store.Info(nsCtx, blobs[0]); err == nil {
		t.Fatalf("containerd holds %s before the pull", blobs[0])
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := n.Agent.PullImage(ctx, &nodev1.PullImageRequest{Reference: ref, Lease: lease})
		done <- err
	}()
	nodetest.Eventually(t, time.Minute, func() error {
		for _, d := range blobs {
			if _, err := store.Info(nsCtx, d); err != nil {
				return fmt.Errorf("containerd doesn't hold %s yet: %v", d, err)
			}
		}
		return nil
	})
	cancel()
	if err := <-done; err == nil {
		t.Fatal("the pull finished before its call ended")
	}
	if hasImage(t, n, ref) {
		t.Error("a pull cut off left an image")
	}
	before := len(requestsFor(t, r, "slow"))
	n.CollectGarbageOnce(t)
	if _, err := pull(t, n, lease, ref); err != nil {
		t.Fatalf("a pull again: %v", err)
	}
	for _, req := range requestsFor(t, r, "slow")[before:] {
		for _, d := range blobs {
			if strings.HasPrefix(req, "GET ") && strings.Contains(req, "/blobs/"+d.String()) {
				t.Errorf("the pull again fetched %s, which the pull cut off had stored: %s", d, req)
			}
		}
	}
	mustRun(t, n, lease, app("app-slow", ref))
	eventuallyRunning(t, n, "app-slow")
}

// blobsOf are the digests of the blobs of img's archive: its manifest,
// config and layer.
func blobsOf(t *testing.T, img nodetest.Image) []godigest.Digest {
	t.Helper()
	tr := tar.NewReader(bytes.NewReader(img.Archive(t)))
	var digests []godigest.Digest
	for {
		h, err := tr.Next()
		if err == io.EOF {
			return digests
		}
		if err != nil {
			t.Fatal(err)
		}
		if hex, ok := strings.CutPrefix(h.Name, "blobs/sha256/"); ok {
			digests = append(digests, godigest.NewDigestFromEncoded(godigest.SHA256, hex))
		}
	}
}

// Garbage collection while a pull runs takes nothing the pull fetched.
func TestCollectionDuringAPullLeavesTheImageWhole(t *testing.T) {
	n := nodetest.Start(t)
	r := nodetest.StartRegistry(t, n)
	n.EventuallyHealthy(t)
	ref, _ := r.Push(t, "slow", "1", nodetest.Image{Name: "registry.test/slow:1", Slow: true})
	lease := hold(t, n)
	stop := n.CollectGarbage(t)
	_, err := pull(t, n, lease, ref)
	stop()
	if err != nil {
		t.Fatal(err)
	}
	n.CollectGarbageOnce(t)
	mustRun(t, n, lease, app("app-slow", ref))
	eventuallyRunning(t, n, "app-slow")
}

// The cases of deploy's check, three seconds after the app's container
// started: one that exits at once has exited with its code, one that
// exits after five seconds still runs, as does one that keeps running.
func TestThreeSecondsAfterARunTheNodeSaysWhetherTheAppRuns(t *testing.T) {
	n := nodetest.Start(t)
	r := nodetest.StartRegistry(t, n)
	n.EventuallyHealthy(t)
	three := 3
	cases := []struct {
		name string
		img  nodetest.Image
		want nodev1.TaskState
	}{
		{"app-crash", nodetest.Image{Exit: &three}, nodev1.TaskState_TASK_STATE_EXITED},
		{"app-late", nodetest.Image{Exit: &three, ExitAfter: 5 * time.Second}, nodev1.TaskState_TASK_STATE_RUNNING},
		{"app-web", nodetest.Image{}, nodev1.TaskState_TASK_STATE_RUNNING},
	}
	lease := hold(t, n)
	for _, c := range cases {
		c.img.Name = "registry.test/" + c.name + ":1"
		ref, _ := r.Push(t, c.name, "1", c.img)
		if _, err := pull(t, n, lease, ref); err != nil {
			t.Fatal(err)
		}
		mustRun(t, n, lease, app(c.name, ref))
		time.Sleep(3 * time.Second)
		got := containerStatus(t, n, c.name)
		if got.GetState() != c.want || c.want == nodev1.TaskState_TASK_STATE_EXITED && got.ExitCode != 3 {
			t.Errorf("%s three seconds after its run: %v, want %s", c.name, got, c.want)
		}
	}
	nodetest.Eventually(t, 10*time.Second, func() error {
		if got := containerStatus(t, n, "app-late"); got.GetState() != nodev1.TaskState_TASK_STATE_EXITED || got.ExitCode != 3 {
			return fmt.Errorf("app-late after five seconds: %v", got)
		}
		return nil
	})
	if out, err := n.Output("root", "cat", hostlayout.ContainerLogDir+"/app-crash.log"); err != nil || !strings.Contains(out, "exiting with 3") {
		t.Errorf("app-crash's log: %q, %v, want it to say it exits", out, err)
	}
}

// A pull of an image an app runs from, of another build under the same
// name, leaves the app running as it runs until a run replaces it.
func TestAPullOfAnotherBuildLeavesTheAppRunning(t *testing.T) {
	n := nodetest.Start(t)
	r := nodetest.StartRegistry(t, n)
	n.EventuallyHealthy(t)
	ref, first := r.Push(t, "web", "1", nodetest.Image{Name: "registry.test/web:1"})
	lease := hold(t, n)
	if _, err := pull(t, n, lease, ref); err != nil {
		t.Fatal(err)
	}
	mustRun(t, n, lease, app("app-web", ref))
	before := eventuallyRunning(t, n, "app-web")
	_, second := r.Push(t, "web", "1", nodetest.Image{Name: "registry.test/web:1", Rebuild: 1})
	res, err := pull(t, n, lease, ref)
	if err != nil {
		t.Fatal(err)
	}
	if res.Digest != second || second == first {
		t.Fatalf("the pull of the rebuild answered %s, want %s, not %s", res.Digest, second, first)
	}
	if c := containerStatus(t, n, "app-web"); c.GetState() != nodev1.TaskState_TASK_STATE_RUNNING || c.Digest != before.Digest {
		t.Errorf("after the pull, app-web is %v, want it running at %s", c, before.Digest)
	}
	mustRun(t, n, lease, app("app-web", ref))
	if c := eventuallyRunning(t, n, "app-web"); c.Digest != second {
		t.Errorf("after the run, app-web runs %s, want %s", c.Digest, second)
	}
	if got := requestsFor(t, r, "web"); !slices.ContainsFunc(got, func(s string) bool { return strings.Contains(s, "/blobs/") }) {
		t.Errorf("the registry served no blob of web: %q", got)
	}
}

// A pull of another build under a tag apps run from, with no run after
// it, as a deploy stopped between its pull and its run leaves it, changes
// no app across a restart of the agent or a reboot: each runs the build
// its last run asked for, until a run asks for another.
func TestAPullWithoutARunChangesNoAppAcrossARestart(t *testing.T) {
	n := nodetest.Start(t)
	r := nodetest.StartRegistry(t, n)
	n.EventuallyHealthy(t)
	ref, first := r.Push(t, "web", "1", nodetest.Image{Name: "registry.test/web:1"})
	lease := hold(t, n)
	if _, err := pull(t, n, lease, ref); err != nil {
		t.Fatal(err)
	}
	mustRun(t, n, lease, app("app-web", ref))
	mustRun(t, n, lease, app("app-api", ref))
	eventuallyRunning(t, n, "app-web")
	eventuallyRunning(t, n, "app-api")
	_, second := r.Push(t, "web", "1", nodetest.Image{Name: "registry.test/web:1", Rebuild: 1})
	if _, err := pull(t, n, lease, ref); err != nil {
		t.Fatal(err)
	}
	want := func(when string, digests map[string]string) {
		t.Helper()
		for name, digest := range digests {
			keepsRunning(t, n, name, digest, when)
		}
	}
	pid := containerPID(t, n, "app-web")
	n.Restart(t, "agent")
	n.EventuallyHealthy(t)
	want("after the agent restarted", map[string]string{"app-web": first, "app-api": first})
	if again := containerPID(t, n, "app-web"); again != pid {
		t.Errorf("the agent's restart restarted app-web: it ran as %s, and runs as %s", pid, again)
	}
	// As after the node rebooted.
	n.Kill(t, "agent")
	n.Exec(t, "root", "sh", "-c", "kill -KILL "+containerPID(t, n, "app-web")+" "+containerPID(t, n, "app-api"))
	n.Restart(t, "agent")
	n.EventuallyHealthy(t)
	want("after a reboot", map[string]string{"app-web": first, "app-api": first})
	// A run asks for the build the tag names now, for that app alone.
	mustRun(t, n, lease, app("app-web", ref))
	n.Restart(t, "agent")
	n.EventuallyHealthy(t)
	want("after a run of the new build and a restart", map[string]string{"app-web": second, "app-api": first})
}

// A pull waiting for a run to end is refused if its lease ran out
// meanwhile, pulling nothing.
func TestAPullWhoseLeaseRunsOutWhileItWaitsIsRefused(t *testing.T) {
	n := nodetest.Start(t)
	r := nodetest.StartRegistry(t, n)
	n.EventuallyHealthy(t)
	ref, _ := r.Push(t, "web", "1", nodetest.Image{Name: "registry.test/web:1"})
	n.Import(t, nodetest.Image{Name: appImage, Slow: true})
	acquire(t, n, "short", shortLease)
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if _, err := n.Agent.Run(ctx, &nodev1.RunRequest{Container: app("app-web", appImage), Lease: "short"}); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("running app-web from the slow image within 50ms: %v, want the call to end first", err)
	}
	start := time.Now()
	_, err := pull(t, n, "short", ref)
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("pulling after waiting %s for the slow run: %v, want it refused, its lease run out", time.Since(start), err)
	}
	if time.Since(start) < shortLease {
		t.Fatalf("the pull waited %s, less than the lease had left: the slow run didn't hold it", time.Since(start))
	}
	if got := requestsFor(t, r, "web"); len(got) != 0 || hasImage(t, n, ref) {
		t.Errorf("the pull refused asked the registry %q, or left its image", got)
	}
}

// A container made anew, as one lost while the agent was down, from the
// build its image's name is at by then, runs that build as asked from then
// on: a later pull of another build, with no run, changes it no more than
// any other.
func TestAContainerMadeAnewKeepsItsBuildThroughALaterPull(t *testing.T) {
	for name, storeFails := range map[string]bool{"stored at once": false, "stored once the list can be written": true} {
		t.Run(name, func(t *testing.T) { containerMadeAnew(t, storeFails) })
	}
}

// containerMadeAnew is TestAContainerMadeAnewKeepsItsBuildThroughALaterPull,
// with the list unwritable while the container is made anew if
// storeFails.
func containerMadeAnew(t *testing.T, storeFails bool) {
	n := nodetest.Start(t)
	r := nodetest.StartRegistry(t, n)
	n.EventuallyHealthy(t)
	ref, _ := r.Push(t, "web", "1", nodetest.Image{Name: "registry.test/web:1"})
	lease := hold(t, n)
	if _, err := pull(t, n, lease, ref); err != nil {
		t.Fatal(err)
	}
	mustRun(t, n, lease, app("app-api", ref))
	eventuallyRunning(t, n, "app-api")
	_, second := r.Push(t, "web", "1", nodetest.Image{Name: "registry.test/web:1", Rebuild: 1})
	if _, err := pull(t, n, lease, ref); err != nil {
		t.Fatal(err)
	}
	// The container is lost while the agent is down.
	n.Kill(t, "agent")
	ctx := namespaces.WithNamespace(t.Context(), hostlayout.ContainerdNamespace)
	c, err := n.Containerd(t).LoadContainer(ctx, "app-api")
	if err != nil {
		t.Fatal(err)
	}
	if task, err := c.Task(ctx, nil); err == nil {
		if _, err := task.Delete(ctx, containerd.WithProcessKill); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Delete(ctx, containerd.WithSnapshotCleanup); err != nil {
		t.Fatal(err)
	}
	// The list can't be written while the agent makes it anew, as on a
	// full disk: the rename onto it fails while a file is mounted over it.
	if storeFails {
		n.Exec(t, "root", "sh", "-c", "cp "+hostlayout.ContainersFile+" /run/list && mount --bind /run/list "+hostlayout.ContainersFile)
	}
	n.Restart(t, "agent")
	n.EventuallyHealthy(t)
	if got := eventuallyRunning(t, n, "app-api"); got.Digest != second {
		t.Fatalf("app-api made anew runs %s, want %s, the build its image's name is at", got.Digest, second)
	}
	if storeFails {
		// The start tries again, and stores the build once it can.
		time.Sleep(3 * time.Second)
		n.Exec(t, "root", "umount", hostlayout.ContainersFile)
		time.Sleep(5 * time.Second)
	}
	r.Push(t, "web", "1", nodetest.Image{Name: "registry.test/web:1", Rebuild: 2})
	if _, err := pull(t, n, lease, ref); err != nil {
		t.Fatal(err)
	}
	n.Restart(t, "agent")
	n.EventuallyHealthy(t)
	keepsRunning(t, n, "app-api", second, "after a pull of a third build and a restart, with no run")
}

// Containers the agent keeps in a list written before it kept each run's
// build run on across a restart, untouched.
func TestContainersOfAListWithoutBuildsRunOnAcrossARestart(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.Import(t, nodetest.Image{Name: appImage})
	mustRun(t, n, hold(t, n), app("app-web", appImage))
	eventuallyRunning(t, n, "app-web")
	pid := containerPID(t, n, "app-web")
	n.Kill(t, "agent")
	n.Exec(t, "root", "sed", "-i", `s/,"digest":"[^"]*"//g`, hostlayout.ContainersFile)
	if out, err := n.Output("root", "cat", hostlayout.ContainersFile); err != nil || strings.Contains(out, `"digest"`) {
		t.Fatalf("the list still keeps builds: %s, %v", out, err)
	}
	n.Restart(t, "agent")
	n.EventuallyHealthy(t)
	eventuallyRunning(t, n, "app-web")
	if again := containerPID(t, n, "app-web"); again != pid {
		t.Errorf("app-web ran as %s, and runs as %s after the restart", pid, again)
	}
}

// keepsRunning fails t unless n's agent reports the container called name
// running, of the build digest, throughout the next few seconds, as long
// as the agent's start takes to replace a container it would.
func keepsRunning(t *testing.T, n *nodetest.Node, name, digest, when string) {
	t.Helper()
	eventuallyRunning(t, n, name)
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(250 * time.Millisecond) {
		if c := containerStatus(t, n, name); c.GetDigest() != digest || c.GetState() != nodev1.TaskState_TASK_STATE_RUNNING {
			t.Errorf("%s, %s is %v, want it running %s", when, name, c, digest)
			return
		}
	}
}

// A run of the build a container runs, under another name of its image,
// as a deploy of a new tag of the same build, keeps the container running
// as it runs, under the new name, across restarts too, and the old name
// goes, as an image no container runs.
func TestARunOfTheSameBuildUnderAnotherNameKeepsTheContainer(t *testing.T) {
	n := nodetest.Start(t)
	r := nodetest.StartRegistry(t, n)
	n.EventuallyHealthy(t)
	img := nodetest.Image{Name: "registry.test/web:1"}
	first, digest := r.Push(t, "web", "1", img)
	second, again := r.Push(t, "web", "2", img)
	if again != digest {
		t.Fatalf("the tags name builds %s and %s, want one", digest, again)
	}
	lease := hold(t, n)
	if _, err := pull(t, n, lease, first); err != nil {
		t.Fatal(err)
	}
	if res := runFor(t, n, lease, app("app-web", first)); !res.Started {
		t.Errorf("the first run of app-web says it started no process")
	}
	eventuallyRunning(t, n, "app-web")
	pid := containerPID(t, n, "app-web")
	// As a deploy pulls right before it runs.
	if _, err := pull(t, n, lease, second); err != nil {
		t.Fatal(err)
	}
	if res := runFor(t, n, lease, app("app-web", second)); res.Started {
		t.Errorf("the run under another name says it started app-web's process")
	}
	if c := eventuallyRunning(t, n, "app-web"); c.Image != second || c.Digest != digest {
		t.Errorf("after the run under %s, app-web runs %s at %s", second, c.Image, c.Digest)
	}
	if again := containerPID(t, n, "app-web"); again != pid {
		t.Errorf("the run under another name restarted app-web: it ran as %s, and runs as %s", pid, again)
	}
	if hasImage(t, n, first) {
		t.Errorf("%s, which no container runs, is kept", first)
	}
	n.Restart(t, "agent")
	n.EventuallyHealthy(t)
	keepsRunning(t, n, "app-web", digest, "after the agent restarted")
	if c := containerStatus(t, n, "app-web"); c.GetImage() != second || containerPID(t, n, "app-web") != pid {
		t.Errorf("after the agent restarted, app-web is %v, as %s, want it from %s as %s", c, containerPID(t, n, "app-web"), second, pid)
	}
	// A run that starts the process again, as of a container whose process
	// stopped, says so, under another name too.
	n.Exec(t, "root", "sh", "-c", "kill -KILL "+pid)
	nodetest.Eventually(t, 10*time.Second, func() error {
		if c := containerStatus(t, n, "app-web"); c.GetState() == nodev1.TaskState_TASK_STATE_RUNNING {
			return fmt.Errorf("app-web still runs")
		}
		return nil
	})
	if _, err := pull(t, n, lease, first); err != nil {
		t.Fatal(err)
	}
	if res := runFor(t, n, lease, app("app-web", first)); !res.Started {
		t.Errorf("a run of app-web, whose process had stopped, says it started none")
	}
}

// runFor has n's agent run c for lease, within a minute, and returns its
// answer, failing t if the run fails.
func runFor(t *testing.T, n *nodetest.Node, lease string, c *nodev1.Container) *nodev1.RunResponse {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	res, err := n.Agent.Run(ctx, &nodev1.RunRequest{Container: c, Lease: lease})
	if err != nil {
		t.Fatalf("running %s from %s: %v", c.Name, c.Image, err)
	}
	return res
}
