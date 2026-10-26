//go:build linux

package agent_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/errdefs"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/hostlayout"
	"github.com/moostackhq/moostack/internal/nodetest"
	"github.com/moostackhq/moostack/internal/ociimage"
)

// A container replaced again and again, from rebuilds that change the
// image's config alone and from others, while containerd collects
// garbage over and over, runs from each.
func TestRunsSurviveForcedCollection(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	lease := hold(t, n)
	stop := n.CollectGarbage(t)
	defer stop()
	for i, img := range []nodetest.Image{
		{Name: appImage},
		{Name: appImage, User: "1:1"},
		{Name: appImage, User: "2:2"},
		{Name: appImage, Rebuild: 1},
		{Name: thirdImage},
		{Name: appImage, Rebuild: 2, User: "3:3"},
	} {
		n.Import(t, img)
		mustRun(t, n, lease, app("app-web", img.Name))
		digest, _ := imageOf(t, n, img.Name)
		if got := eventuallyRunning(t, n, "app-web"); got.Digest != digest {
			t.Fatalf("run %d: the agent reports %v, want it from %s", i, got, digest)
		}
		want := "0"
		if img.User != "" {
			want, _, _ = strings.Cut(img.User, ":")
		}
		wantStatusLines(t, n, containerPID(t, n, "app-web"), "Uid:\t"+want+"\t"+want+"\t"+want+"\t"+want)
	}
}

// crashDelays are the delays after a call starts at which the crash
// tests kill the agent: from at once to past how long the call took
// without a crash, so that the crashes land on each step in between.
func crashDelays(took time.Duration) []time.Duration {
	var delays []time.Duration
	for k := range 13 {
		delays = append(delays, took*time.Duration(k)/10)
	}
	return delays
}

// An agent killed at any step of a replacement, and started again,
// finishes it when it is run again: the container runs from the new
// image, and the image it replaced is gone.
func TestAReplacementCrashedAtAnyStepFinishesWhenRunAgain(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	lease := hold(t, n)
	images := []string{appImage, thirdImage}
	n.Import(t, nodetest.Image{Name: images[0]})
	mustRun(t, n, lease, app("app-web", images[0]))
	n.Import(t, nodetest.Image{Name: images[1]})
	start := time.Now()
	mustRun(t, n, lease, app("app-web", images[1]))
	took := time.Since(start)
	for i, delay := range crashDelays(took) {
		from, to := images[(i+1)%2], images[i%2]
		// A build of its own, so that the run unpacks a layer the node
		// lacks, and a crash can land in the unpacking too.
		n.Import(t, nodetest.Image{Name: to, Rebuild: i + 1})
		done := make(chan struct{})
		go func() {
			defer close(done)
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			n.Agent.Run(ctx, &nodev1.RunRequest{Container: app("app-web", to), Lease: lease})
		}()
		time.Sleep(delay)
		n.Crash(t, "agent")
		<-done
		n.EventuallyHealthy(t)
		// A crash while containerd deleted the old task can leave its shim
		// shutting down, and the new task made in it then goes with it:
		// the container is reported without a process, and a run again
		// starts it, as deploy's rerun does.
		for tries := 1; ; tries++ {
			if err := run(t, n, lease, app("app-web", to)); err != nil {
				t.Fatalf("crashed after %s: running app-web from %s again: %v", delay, to, err)
			}
			time.Sleep(time.Second)
			if containerStatus(t, n, "app-web").GetState() == nodev1.TaskState_TASK_STATE_RUNNING || tries == 3 {
				break
			}
			t.Logf("crashed after %s: app-web isn't running after run %d again", delay, tries)
		}
		if got := eventuallyRunning(t, n, "app-web"); got.Image != to {
			t.Errorf("crashed after %s: the agent reports %v, want it from %s", delay, got, to)
		}
		if hasImage(t, n, from) {
			t.Errorf("crashed after %s: %s, which app-web ran from before, is kept", delay, from)
		}
		if s, err := n.Status(t); err != nil || len(s.Containers) != 1 {
			t.Errorf("crashed after %s: the agent reports %v, %v, want app-web alone", delay, s.GetContainers(), err)
		}
	}
}

// An agent killed at any step of a removal, and started again, reports
// the container while it is there, and once it is removed again keeps
// nothing of it.
func TestARemovalCrashedAtAnyStepFinishesWhenRemovedAgain(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	lease := hold(t, n)
	n.Import(t, nodetest.Image{Name: appImage})
	mustRun(t, n, lease, app("app-web", appImage))
	start := time.Now()
	if err := removeContainer(t, n, lease, "app-web"); err != nil {
		t.Fatal(err)
	}
	took := time.Since(start)
	client := n.Containerd(t)
	ctx := namespaces.WithNamespace(t.Context(), hostlayout.ContainerdNamespace)
	for _, delay := range crashDelays(took) {
		n.Import(t, nodetest.Image{Name: appImage})
		mustRun(t, n, lease, app("app-web", appImage))
		done := make(chan struct{})
		go func() {
			defer close(done)
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			n.Agent.Remove(ctx, &nodev1.RemoveRequest{Name: "app-web", Lease: lease})
		}()
		time.Sleep(delay)
		n.Crash(t, "agent")
		<-done
		n.EventuallyHealthy(t)
		_, err := client.LoadContainer(ctx, "app-web")
		if err != nil && !errdefs.IsNotFound(err) {
			t.Fatal(err)
		}
		reported := containerStatus(t, n, "app-web")
		// A container that is still there is still reported, so that
		// the removal is known to be unfinished.
		if err == nil && reported == nil {
			t.Fatalf("crashed after %s: app-web's container is there, but the agent doesn't report it", delay)
		}
		// Once the node keeps it no more, as when the crash came after
		// the removal stored the list without it, the agent's start
		// deletes its image, which no removal again would see.
		if reported == nil {
			nodetest.Eventually(t, 30*time.Second, func() error {
				if hasImage(t, n, appImage) {
					return fmt.Errorf("crashed after %s: the agent keeps app-web no more, but %s is kept after its start", delay, appImage)
				}
				return nil
			})
		}
		// Removing it again finishes what the crash left, even once the
		// node no longer keeps it, its image still to delete.
		if err := removeContainer(t, n, lease, "app-web"); err != nil {
			t.Fatalf("crashed after %s: removing app-web again: %v", delay, err)
		}
		if got := containerStatus(t, n, "app-web"); got != nil {
			t.Errorf("crashed after %s: the agent reports %v once it was removed again", delay, got)
		}
		if _, err := client.LoadContainer(ctx, "app-web"); !errdefs.IsNotFound(err) {
			t.Errorf("crashed after %s: app-web's container: %v, want it gone", delay, err)
		}
		if hasImage(t, n, appImage) {
			t.Errorf("crashed after %s: %s is kept", delay, appImage)
		}
	}
}

// stateMachineSeed is the seed of TestRunAndRemoveSequencesMatchTheModel,
// from MOOSTACK_STATE_SEED if set, so that a failing sequence runs again.
func stateMachineSeed(t *testing.T) uint64 {
	if s := os.Getenv("MOOSTACK_STATE_SEED"); s != "" {
		seed, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		return seed
	}
	return uint64(time.Now().UnixNano())
}

// containerModel is what the node keeps, as the node API says it
// should: the image each container runs from, by name, and the images
// the node holds.
type containerModel struct {
	runs   map[string]string
	images map[string]bool
	// tagged is the build each image's name is at, as an import of a build
	// last moved it, and ran the build each container's run asked for, by
	// the container's name.
	tagged, ran map[string]int
}

// used is whether a container runs from image.
func (m *containerModel) used(image string) bool {
	for _, img := range m.runs {
		if img == image {
			return true
		}
	}
	return false
}

// run is the model of running name from image: the image it replaced
// goes, and so do the other tags of image's repository, unless a
// container runs from them.
func (m *containerModel) run(name, image string) {
	replaced, ok := m.runs[name]
	m.runs[name] = image
	repository := image[:strings.LastIndex(image, ":")+1]
	for img := range m.images {
		if ((ok && img == replaced) || (img != image && strings.HasPrefix(img, repository))) && !m.used(img) {
			delete(m.images, img)
		}
	}
}

// remove is the model of removing name: its image goes, unless another
// container runs from it.
func (m *containerModel) remove(name string) {
	image, ok := m.runs[name]
	delete(m.runs, name)
	if ok && !m.used(image) {
		delete(m.images, image)
	}
}

// Random sequences of runs, replacements and removals of a few
// containers from a few images leave the node as the model says after
// each step: each container running from its image, and each image held
// only while a container runs from it or no run has taken it since its
// import.
func TestRunAndRemoveSequencesMatchTheModel(t *testing.T) {
	seed := stateMachineSeed(t)
	t.Logf("MOOSTACK_STATE_SEED=%d", seed)
	r := rand.New(rand.NewPCG(seed, 0))
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	lease := hold(t, n)
	names := []string{"app-a", "app-b", "app-c"}
	images := []string{appImage, otherImage, thirdImage}
	m := &containerModel{runs: map[string]string{}, images: map[string]bool{}, tagged: map[string]int{}, ran: map[string]int{}}
	// digests are the digests of each image's builds, by image and build.
	digests := map[string]string{}
	digest := func(image string, build int) string {
		key := image + " " + strconv.Itoa(build)
		if _, ok := digests[key]; !ok {
			d, err := ociimage.Digest(nodetest.Image{Name: image, Rebuild: build}.Archive(t))
			if err != nil {
				t.Fatal(err)
			}
			digests[key] = d
		}
		return digests[key]
	}
	for step := range 50 {
		name := names[r.IntN(len(names))]
		var did string
		switch r.IntN(7) {
		case 0:
			// An image imported and not run, as one an update stopped after
			// importing, is held until a run deletes it; a build of it other
			// than one a container runs, as a pull of a moved tag with no run
			// after it, changes no container, then or once the agent
			// restarts. Each import moves the name to its other build.
			image := images[r.IntN(len(images))]
			build := 1 - m.tagged[image]
			did = "import " + image + " of build " + strconv.Itoa(build)
			n.Import(t, nodetest.Image{Name: image, Rebuild: build})
			m.images[image], m.tagged[image] = true, build
			if r.IntN(2) == 0 {
				did += ", then restart the agent"
				n.Restart(t, "agent")
				n.EventuallyHealthy(t)
			}
		case 1:
			did = "remove " + name
			if err := removeContainer(t, n, lease, name); err != nil {
				t.Fatalf("step %d, %s: %v", step, did, err)
			}
			m.remove(name)
		case 5:
			did = "restart the agent"
			n.Restart(t, "agent")
			n.EventuallyHealthy(t)
		default:
			image := images[r.IntN(len(images))]
			did = "run " + name + " from " + image
			if !m.images[image] {
				n.Import(t, nodetest.Image{Name: image, Rebuild: m.tagged[image]})
				m.images[image] = true
			}
			if err := run(t, n, lease, app(name, image)); err != nil {
				t.Fatalf("step %d, %s: %v", step, did, err)
			}
			m.run(name, image)
			m.ran[name] = m.tagged[image]
		}
		s, err := n.Status(t)
		if err != nil {
			t.Fatal(err)
		}
		var got, want []string
		for _, c := range s.Containers {
			got = append(got, c.Name+" "+c.Image+" "+c.Digest)
		}
		for name, image := range m.runs {
			want = append(want, name+" "+image+" "+digest(image, m.ran[name]))
		}
		slices.Sort(got)
		slices.Sort(want)
		if !slices.Equal(got, want) {
			t.Fatalf("step %d, %s: the agent reports %v, want %v", step, did, got, want)
		}
		for _, image := range images {
			if has := hasImage(t, n, image); has != m.images[image] {
				t.Fatalf("step %d, %s: the node holds %s: %v, want %v", step, did, image, has, m.images[image])
			}
		}
	}
	for name := range m.runs {
		if got := eventuallyRunning(t, n, name); got.Image != m.runs[name] {
			t.Errorf("%s: the agent reports %v, want it from %s", name, got, m.runs[name])
		}
	}
	if t.Failed() {
		t.Logf("run again with MOOSTACK_STATE_SEED=%d", seed)
	}
}
