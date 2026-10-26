//go:build linux

package agent

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	imagesapi "github.com/containerd/containerd/api/services/images/v1"
	snapshotsapi "github.com/containerd/containerd/api/services/snapshots/v1"
	containerd "github.com/containerd/containerd/v2/client"
	containerdcontainers "github.com/containerd/containerd/v2/core/containers"
	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/leases"
	"github.com/containerd/containerd/v2/defaults"
	"github.com/containerd/errdefs"
	"github.com/opencontainers/image-spec/identity"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/gateway"
	"github.com/moostackhq/moostack/internal/hostlayout"
	"github.com/moostackhq/moostack/internal/nodetest"
)

// failing is a client of n's containerd whose calls of method, or of
// method as a stream, fail as unavailable while fail says so.
func failing(t *testing.T, n *nodetest.Node, method string, fail *bool) *containerd.Client {
	t.Helper()
	failed := func(m string) bool { return *fail && (m == method || m == method+"Stream") }
	client, err := containerd.NewWithConn(n.ContainerdConn(t,
		grpc.WithUnaryInterceptor(func(ctx context.Context, m string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			if failed(m) {
				return status.Error(codes.Unavailable, "failed for the test")
			}
			return invoker(ctx, m, req, reply, cc, opts...)
		}),
		grpc.WithStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, m string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			if failed(m) {
				return nil, status.Error(codes.Unavailable, "failed for the test")
			}
			return streamer(ctx, desc, cc, m, opts...)
		})))
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// hasImageIn reports whether n's containerd holds the image called name.
func hasImageIn(t *testing.T, n *nodetest.Node, name string) bool {
	t.Helper()
	_, err := n.Containerd(t).ImageService().Get(namespaced(t.Context()), name)
	if err != nil && !errdefs.IsNotFound(err) {
		t.Fatal(err)
	}
	return err == nil
}

// takeOver has n's agent run the container called name from image,
// then stops it, and returns the containers of an agent in this test
// that keeps that container, as n's did, whose calls to n's containerd
// go through client, nil for a plain one. A run can't make a container
// from here, which mounts its image, but the changes before it can be
// made.
func takeOver(t *testing.T, n *nodetest.Node, name, image string, client *containerd.Client) *containers {
	t.Helper()
	n.HoldLock(t)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	if _, err := n.Agent.Run(ctx, &nodev1.RunRequest{Container: &nodev1.Container{Name: name, Image: image}, Lease: nodetest.Lease}); err != nil {
		t.Fatal(err)
	}
	n.Kill(t, "agent")
	s := containersThrough(t, n, nil)
	if err := s.store([]*container{{Name: name, Image: image}}, nil); err != nil {
		t.Fatal(err)
	}
	if client == nil {
		client = s.client
	}
	return loadAgain(t, s, client)
}

// loadAgain is the containers that s keeps, loaded again, as by an agent
// that starts again, whose calls go through client.
func loadAgain(t *testing.T, s *containers, client *containerd.Client) *containers {
	t.Helper()
	again, err := loadContainers(t.Context(), client, gateway.New(), Config{ContainersFile: s.file, ContainersDir: s.dir, ContainerLogDir: s.logDir, Runc: s.runc})
	if err != nil {
		t.Fatal(err)
	}
	return again
}

// An image whose deletion failed, as while containerd didn't answer, or
// couldn't be checked, as when the containers or the image can't be
// read, is kept to delete across a restart of the agent, and the next
// change deletes it, even a removal of a container the node doesn't
// keep.
func TestAnImageWhoseDeletionFailedIsDeletedByTheNextChange(t *testing.T) {
	for what, method := range map[string]string{
		"deleting it":            "/containerd.services.images.v1.Images/Delete",
		"listing the containers": "/containerd.services.containers.v1.Containers/List",
		"reading the image":      "/containerd.services.images.v1.Images/Get",
	} {
		t.Run(what, func(t *testing.T) {
			n := nodetest.Start(t)
			n.EventuallyHealthy(t)
			n.Import(t, nodetest.Image{Name: "example.test/app:1"})
			deleting := true
			s := takeOver(t, n, "app-web", "example.test/app:1", failing(t, n, "/containerd.services.images.v1.Images/Delete", &deleting))
			if _, err := s.Remove(t.Context(), "app-web", unfenced); err != nil {
				t.Fatalf("removing app-web while images can't be deleted: %v, want it removed all the same", err)
			}
			deleting = false
			fail := true
			again := loadAgain(t, s, failing(t, n, method, &fail))
			if _, err := again.Remove(t.Context(), "app-other", unfenced); err != nil {
				t.Fatalf("a change while %s fails: %v", what, err)
			}
			if !hasImageIn(t, n, "example.test/app:1") {
				t.Fatalf("app-web's image went while %s fails", what)
			}
			fail = false
			if _, err := loadAgain(t, s, s.client).Remove(t.Context(), "app-other", unfenced); err != nil {
				t.Fatal(err)
			}
			if hasImageIn(t, n, "example.test/app:1") {
				t.Errorf("app-web's image is kept after the next change, once %s works", what)
			}
		})
	}
}

// An image kept to delete, as a removal whose deletion failed keeps it,
// or one a crash cut off once it stored the list without its container,
// goes when the agent starts, without another change: no removal again
// would see the container, which the node keeps no more.
func TestAnImageKeptToDeleteIsDeletedWhenTheAgentStarts(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.Import(t, nodetest.Image{Name: "example.test/app:1"})
	deleting := true
	s := takeOver(t, n, "app-web", "example.test/app:1", failing(t, n, "/containerd.services.images.v1.Images/Delete", &deleting))
	if _, err := s.Remove(t.Context(), "app-web", unfenced); err != nil {
		t.Fatalf("removing app-web while images can't be deleted: %v, want it removed all the same", err)
	}
	if !hasImageIn(t, n, "example.test/app:1") {
		t.Fatal("app-web's image went, though images can't be deleted")
	}
	deleting = false
	loadAgain(t, s, s.client).StartAll(t.Context())
	if hasImageIn(t, n, "example.test/app:1") {
		t.Error("app-web's image, kept to delete, is kept after the agent started")
	}
}

// A replacement cut off once it stored the list with the new image, the
// old container still there, is finished when the agent starts, and the
// image it replaced goes then too: a deploy run again sees the new build
// run and sends no run that would delete it.
func TestAReplacementFinishedByTheAgentsStartDeletesTheImageItReplaced(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.Import(t, nodetest.Image{Name: "example.test/app:1"})
	n.Import(t, nodetest.Image{Name: "example.test/other:1", Rebuild: 1})
	s := takeOver(t, n, "app-web", "example.test/app:1", nil)
	unused, err := s.markUnused(namespaced(t.Context()), nil, "example.test/app:1")
	if err != nil {
		t.Fatal(err)
	}
	// The agent's list says what a replacement cut off once it stored the
	// list leaves: app-web from the new image, and the old one unused.
	data, err := json.Marshal(listFile{Containers: []*container{{Name: "app-web", Image: "example.test/other:1"}}, Unused: unused})
	if err != nil {
		t.Fatal(err)
	}
	n.Exec(t, "root", "sh", "-c", `printf '%s\n' "$1" > `+hostlayout.ContainersFile, "-", string(data))
	// The real agent starts from the state the crash left.
	n.Restart(t, "agent")
	n.EventuallyHealthy(t)
	runs := func() string {
		s, err := n.Status(t)
		if err != nil {
			return ""
		}
		for _, c := range s.Containers {
			if c.Name == "app-web" && c.State == nodev1.TaskState_TASK_STATE_RUNNING {
				return c.Image
			}
		}
		return ""
	}
	nodetest.Eventually(t, time.Minute, func() error {
		if got := runs(); got != "example.test/other:1" {
			return fmt.Errorf("app-web runs from %q after the agent's start, want example.test/other:1", got)
		}
		if hasImageIn(t, n, "example.test/app:1") {
			return errors.New("example.test/app:1, which the replacement replaced, is kept after the agent's start")
		}
		return nil
	})
}

// An image kept to delete goes at the agent's first try of its starts,
// though another container fails every try, as one whose image the node
// lacks.
func TestAnImageKeptToDeleteGoesWhileAnotherContainerFailsToStart(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.Import(t, nodetest.Image{Name: "example.test/app:1"})
	deleting := true
	s := takeOver(t, n, "app-web", "example.test/app:1", failing(t, n, "/containerd.services.images.v1.Images/Delete", &deleting))
	if _, err := s.Remove(t.Context(), "app-web", unfenced); err != nil {
		t.Fatal(err)
	}
	deleting = false
	again := loadAgain(t, s, s.client)
	if err := again.store([]*container{{Name: "app-bad", Image: "example.test/missing:1"}}, again.unused); err != nil {
		t.Fatal(err)
	}
	// Less than a second try's wait, so that the first try decides.
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	loadAgain(t, again, s.client).StartAll(ctx)
	if hasImageIn(t, n, "example.test/app:1") {
		t.Error("app-web's image, kept to delete, is kept while app-bad fails to start")
	}
}

// An image whose deletion failed goes on a later try, with no change or
// start of the agent, while one that still can't be deleted stays kept.
func TestAnImageWhoseDeletionFailedGoesOnALaterTry(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.Import(t, nodetest.Image{Name: "example.test/app:1"})
	// Deletions fail while deleting says so, and each that fails is told
	// on failed, so that the test sees the tries go on failing.
	var mu sync.Mutex
	deleting := true
	failed := make(chan struct{}, 16)
	client := clientThrough(t, n, func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		mu.Lock()
		fail := deleting && method == "/containerd.services.images.v1.Images/Delete"
		mu.Unlock()
		if fail {
			select {
			case failed <- struct{}{}:
			default:
			}
			return status.Error(codes.Unavailable, "failed for the test")
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	})
	s := takeOver(t, n, "app-web", "example.test/app:1", client)
	if _, err := s.Remove(t.Context(), "app-web", unfenced); err != nil {
		t.Fatal(err)
	}
	<-failed
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.KeepDeletingUnused(ctx, 50*time.Millisecond, 50*time.Millisecond)
	}()
	defer func() { cancel(); <-done }()
	// Two tries fail before deletions work again.
	for range 2 {
		select {
		case <-failed:
		case <-time.After(10 * time.Second):
			t.Fatal("no try deleted the image left to delete")
		}
	}
	if !hasImageIn(t, n, "example.test/app:1") {
		t.Fatal("app-web's image went while it can't be deleted")
	}
	mu.Lock()
	deleting = false
	mu.Unlock()
	nodetest.Eventually(t, 10*time.Second, func() error {
		if hasImageIn(t, n, "example.test/app:1") {
			return errors.New("app-web's image is kept once it can be deleted, with no change to delete it")
		}
		return nil
	})
}

// A try to delete the images left to delete, while containerd doesn't
// answer, gives up within its bound, so that a removal waiting for the
// runner meanwhile goes on.
func TestATryToDeleteUnusedImagesGivesUpWhileContainerdDoesntAnswer(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.Import(t, nodetest.Image{Name: "example.test/app:1"})
	var mu sync.Mutex
	deleting, hang := false, false
	hung := make(chan struct{}, 1)
	client := clientThrough(t, n, func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		mu.Lock()
		failDelete, hangGet := deleting && method == "/containerd.services.images.v1.Images/Delete", hang && method == "/containerd.services.images.v1.Images/Get"
		if hangGet {
			hang = false
		}
		mu.Unlock()
		switch {
		case failDelete:
			return status.Error(codes.Unavailable, "failed for the test")
		case hangGet:
			hung <- struct{}{}
			<-ctx.Done()
			return status.FromContextError(ctx.Err()).Err()
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	})
	mu.Lock()
	deleting = true
	mu.Unlock()
	s := takeOver(t, n, "app-web", "example.test/app:1", client)
	if _, err := s.Remove(t.Context(), "app-web", unfenced); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	deleting, hang = false, true
	mu.Unlock()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.KeepDeletingUnused(ctx, 500*time.Millisecond, 500*time.Millisecond)
	}()
	defer func() { cancel(); <-done }()
	select {
	case <-hung:
	case <-time.After(10 * time.Second):
		t.Fatal("no try read the image left to delete")
	}
	removed := make(chan error, 1)
	go func() { _, err := s.Remove(t.Context(), "app-other", unfenced); removed <- err }()
	select {
	case err := <-removed:
		if err != nil {
			t.Errorf("a removal once the try gave up: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a removal still waits for the runner 5s after a try that should give up within 500ms")
	}
}

// The agent tries again, every unusedRetry, to delete an image left to
// delete, with no change or restart: one a removal kept while a container
// the agent doesn't keep ran from it goes once that container goes.
func TestTheAgentDeletesAnImageLeftToDeleteOnALaterTry(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.Import(t, nodetest.Image{Name: "example.test/app:1"})
	n.HoldLock(t)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	if _, err := n.Agent.Run(ctx, &nodev1.RunRequest{Container: &nodev1.Container{Name: "app-web", Image: "example.test/app:1"}, Lease: nodetest.Lease}); err != nil {
		t.Fatal(err)
	}
	client := n.Containerd(t)
	nsCtx := namespaced(t.Context())
	other, err := client.ContainerService().Create(nsCtx, containerdcontainers.Container{ID: "not-kept",
		Labels: map[string]string{imageLabel: "example.test/app:1"}, Runtime: containerdcontainers.RuntimeInfo{Name: "io.containerd.runc.v2"}, Spec: &anypb.Any{}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := n.Agent.Remove(ctx, &nodev1.RemoveRequest{Name: "app-web", Lease: nodetest.Lease}); err != nil {
		t.Fatal(err)
	}
	if !hasImageIn(t, n, "example.test/app:1") {
		t.Fatal("app-web's image went, though a container the agent doesn't keep runs from it")
	}
	if err := client.ContainerService().Delete(nsCtx, other.ID); err != nil {
		t.Fatal(err)
	}
	nodetest.Eventually(t, unusedRetry+30*time.Second, func() error {
		if hasImageIn(t, n, "example.test/app:1") {
			return errors.New("app-web's image is kept after nothing runs from it, with no change to delete it")
		}
		return nil
	})
}

// A removal that waits for the runner, as while a run of the same name is
// under way, judges whether the node keeps the container once it holds
// the runner: it removes what the run stored meanwhile, and says so.
func TestARemovalWaitingForARunRemovesWhatTheRunStored(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	s := containersThrough(t, n, nil)
	s.running.Lock()
	type answer struct {
		removed bool
		err     error
	}
	answered := make(chan answer, 1)
	go func() {
		removed, err := s.Remove(t.Context(), "app-web", unfenced)
		answered <- answer{removed, err}
	}()
	// The run, holding the runner, stores the container it makes once the
	// removal waits for the runner.
	waitingForTheRunner(t, 1)
	if err := s.store([]*container{{Name: "app-web", Image: "example.test/app:1"}}, nil); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	s.list = []*container{{Name: "app-web", Image: "example.test/app:1"}}
	s.mu.Unlock()
	s.running.Unlock()
	a := <-answered
	if a.err != nil || !a.removed {
		t.Errorf("the removal that waited for the run = %v, %v, want it removed app-web", a.removed, a.err)
	}
	if s.stored("app-web") != nil {
		t.Error("the node keeps app-web once the removal that waited for the run ended")
	}
}

// A container that goes once the agent loaded it, as when containerd
// finishes a deletion that an agent which crashed had asked for, is
// deleted all the same.
func TestDeletingAContainerGoneMeanwhileSucceeds(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	client := n.Containerd(t)
	ctx := namespaced(t.Context())
	if _, err := client.ContainerService().Create(ctx, containerdcontainers.Container{ID: "gone",
		Runtime: containerdcontainers.RuntimeInfo{Name: "io.containerd.runc.v2"}, Spec: &anypb.Any{}}); err != nil {
		t.Fatal(err)
	}
	c, err := client.LoadContainer(ctx, "gone")
	if err != nil {
		t.Fatal(err)
	}
	if err := client.ContainerService().Delete(ctx, "gone"); err != nil {
		t.Fatal(err)
	}
	if err := deleteContainer(ctx, c); err != nil {
		t.Errorf("deleting a container gone meanwhile: %v, want it deleted", err)
	}
}

// waitingForTheRunner waits until n removals wait for the runner, as the
// goroutines' stacks show them, failing t after 10 seconds.
func waitingForTheRunner(t *testing.T, n int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		buf := make([]byte, 1<<20)
		buf = buf[:runtime.Stack(buf, true)]
		waiting := 0
		for _, g := range strings.Split(string(buf), "\n\n") {
			if strings.Contains(g, "sync.(*Mutex).Lock") && strings.Contains(g, "(*containers).Remove") {
				waiting++
			}
		}
		if waiting >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d removals wait for the runner, want %d", waiting, n)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Two removals of one container, the second waiting for the first, as the
// runner has it, answer of what the first left: one says it removed the
// container, and the other that the node kept none.
func TestARemovalWaitingForARemovalSaysItRemovedNone(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	s := containersThrough(t, n, nil)
	if err := s.store([]*container{{Name: "app-web", Image: "example.test/app:1"}}, nil); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	s.list = []*container{{Name: "app-web", Image: "example.test/app:1"}}
	s.mu.Unlock()
	s.running.Lock()
	answers := make(chan bool, 2)
	for range 2 {
		go func() {
			removed, err := s.Remove(t.Context(), "app-web", unfenced)
			if err != nil {
				t.Error(err)
			}
			answers <- removed
		}()
	}
	waitingForTheRunner(t, 2)
	s.running.Unlock()
	if first, second := <-answers, <-answers; first == second {
		t.Errorf("two removals of app-web said removed %v and %v, want one of each", first, second)
	}
}

// An image kept to delete while containerd can't list its containers at
// the agent's start stays kept, and the next change deletes it.
func TestAnImageTheAgentsStartCantDeleteIsKeptForTheNextChange(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.Import(t, nodetest.Image{Name: "example.test/app:1"})
	deleting := true
	s := takeOver(t, n, "app-web", "example.test/app:1", failing(t, n, "/containerd.services.images.v1.Images/Delete", &deleting))
	if _, err := s.Remove(t.Context(), "app-web", unfenced); err != nil {
		t.Fatal(err)
	}
	deleting = false
	listing := true
	loadAgain(t, s, failing(t, n, "/containerd.services.containers.v1.Containers/List", &listing)).StartAll(t.Context())
	if !hasImageIn(t, n, "example.test/app:1") {
		t.Fatal("app-web's image went while containerd couldn't list its containers")
	}
	listing = false
	if _, err := loadAgain(t, s, s.client).Remove(t.Context(), "app-other", unfenced); err != nil {
		t.Fatal(err)
	}
	if hasImageIn(t, n, "example.test/app:1") {
		t.Error("app-web's image is kept after the next change")
	}
}

// A removal that can't read the container's image, to record it as
// unused, fails as unavailable, changing nothing: the container is
// still there and stored. Removing it again once the image can be read
// finishes it, its image gone.
func TestARemovalThatCantReadTheImageChangesNothingAndFinishesWhenRemovedAgain(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.Import(t, nodetest.Image{Name: "example.test/app:1"})
	fail := true
	s := takeOver(t, n, "app-web", "example.test/app:1", failing(t, n, "/containerd.services.images.v1.Images/Get", &fail))
	if _, err := s.Remove(t.Context(), "app-web", unfenced); status.Code(err) != codes.Unavailable {
		t.Fatalf("removing app-web while its image can't be read: %v, want it refused as unavailable", err)
	}
	if made, _, err := s.load(namespaced(t.Context()), "app-web"); err != nil || made == nil {
		t.Errorf("app-web's container after the failed removal: %v, %v, want it there", made, err)
	}
	if loadAgain(t, s, s.client).stored("app-web") == nil {
		t.Error("app-web is no longer stored after its removal failed")
	}
	fail = false
	if _, err := loadAgain(t, s, s.client).Remove(t.Context(), "app-web", unfenced); err != nil {
		t.Fatal(err)
	}
	if hasImageIn(t, n, "example.test/app:1") {
		t.Error("app-web's image is kept once its removal finished")
	}
}

// A run that can't read the image of the container it replaces, to
// record it as unused, fails as unavailable, changing nothing: the old
// container runs on, stored as it was.
func TestARunThatCantReadTheReplacedImageChangesNothing(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.Import(t, nodetest.Image{Name: "example.test/app:1"})
	fail := false
	client, err := containerd.NewWithConn(n.ContainerdConn(t, grpc.WithUnaryInterceptor(func(ctx context.Context, m string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if r, ok := req.(*imagesapi.GetImageRequest); ok && fail && r.Name == "example.test/app:1" {
			return status.Error(codes.Unavailable, "failed for the test")
		}
		return invoker(ctx, m, req, reply, cc, opts...)
	})))
	if err != nil {
		t.Fatal(err)
	}
	s := takeOver(t, n, "app-web", "example.test/app:1", client)
	if _, err := s.Import(t.Context(), bytes.NewReader(nodetest.Image{Name: "example.test/app:2", Rebuild: 2}.Archive(t)), unfenced); err != nil {
		t.Fatal(err)
	}
	fail = true
	if _, err := s.Run(t.Context(), &container{Name: "app-web", Image: "example.test/app:2"}, unfenced); status.Code(err) != codes.Unavailable {
		t.Fatalf("running app-web from app:2 while app:1 can't be read: %v, want it refused as unavailable", err)
	}
	again := loadAgain(t, s, s.client)
	if c := again.stored("app-web"); c == nil || c.Image != "example.test/app:1" || len(again.unused) != 0 {
		t.Errorf("after the failed run, the agent keeps %+v with the unused images %v, want app-web from app:1 and none", c, again.unused)
	}
	made, labels, err := s.load(namespaced(t.Context()), "app-web")
	if err != nil || made == nil || labels[imageLabel] != "example.test/app:1" {
		t.Errorf("app-web's container after the failed run: %v, %v, %v, want the one from app:1", made, labels[imageLabel], err)
	}
}

// A removal of a container whose image was deleted by hand finishes,
// with no image to delete.
func TestARemovalOfAContainerWhoseImageIsGoneFinishes(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.Import(t, nodetest.Image{Name: "example.test/app:1"})
	s := takeOver(t, n, "app-web", "example.test/app:1", nil)
	n.DeleteImage(t, "example.test/app:1")
	if _, err := s.Remove(t.Context(), "app-web", unfenced); err != nil {
		t.Fatal(err)
	}
	if again := loadAgain(t, s, s.client); again.stored("app-web") != nil || len(again.unused) != 0 {
		t.Errorf("after the removal, the agent keeps %v with the unused images %v, want neither", again.list, again.unused)
	}
}

// A run whose new image fails to unpack leaves the container it would
// replace running, as it was.
func TestARunWhoseImageFailsToUnpackLeavesTheOldContainerRunning(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.Import(t, nodetest.Image{Name: "example.test/app:1"})
	fail := true
	s := takeOver(t, n, "app-web", "example.test/app:1", failing(t, n, "/containerd.services.diff.v1.Diff/Apply", &fail))
	// Imported once app:1 runs, whose run deletes the other tags.
	if _, err := s.Import(t.Context(), bytes.NewReader(nodetest.Image{Name: "example.test/app:2", Rebuild: 2}.Archive(t)), unfenced); err != nil {
		t.Fatal(err)
	}
	_, err := s.Run(t.Context(), &container{Name: "app-web", Image: "example.test/app:2"}, unfenced)
	if err == nil {
		t.Fatal("app-web ran from an image that can't be unpacked")
	}
	if status.Code(err) == codes.FailedPrecondition {
		t.Fatalf("the run failed before it unpacked: %v", err)
	}
	ctx := namespaced(t.Context())
	made, labels, err := s.load(ctx, "app-web")
	if err != nil || made == nil {
		t.Fatalf("app-web's container after the failed run: %v, %v, want the old one", made, err)
	}
	if labels[imageLabel] != "example.test/app:1" {
		t.Errorf("app-web runs %s after the failed run, want example.test/app:1", labels[imageLabel])
	}
	task, err := made.Task(ctx, nil)
	if err != nil {
		t.Fatalf("app-web's task after the failed run: %v", err)
	}
	if st, err := task.Status(ctx); err != nil || st.Status != containerd.Running {
		t.Errorf("app-web's task after the failed run is %v, %v, want running", st.Status, err)
	}
	// The old container's image, which it runs from, stays while it does,
	// whatever the next change deletes.
	fail = false
	if _, err := s.Remove(t.Context(), "app-other", unfenced); err != nil {
		t.Fatal(err)
	}
	if !hasImageIn(t, n, "example.test/app:1") {
		t.Error("the image that app-web still runs from went with the next change")
	}
}

// An image imported again once no container runs from it, as to go back
// to it, is no longer to delete: the next change keeps it.
func TestAnImageImportedAgainIsKept(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.Import(t, nodetest.Image{Name: "example.test/app:1"})
	fail := true
	s := takeOver(t, n, "app-web", "example.test/app:1", failing(t, n, "/containerd.services.images.v1.Images/Delete", &fail))
	if _, err := s.Remove(t.Context(), "app-web", unfenced); err != nil {
		t.Fatal(err)
	}
	if !hasImageIn(t, n, "example.test/app:1") {
		t.Fatal("app-web's image went while images can't be deleted")
	}
	if _, err := s.Import(t.Context(), bytes.NewReader(nodetest.Image{Name: "example.test/app:1"}.Archive(t)), unfenced); err != nil {
		t.Fatal(err)
	}
	fail = false
	if _, err := loadAgain(t, s, s.client).Remove(t.Context(), "app-other", unfenced); err != nil {
		t.Fatal(err)
	}
	if !hasImageIn(t, n, "example.test/app:1") {
		t.Error("the image imported again went with the next change")
	}
}

// An image whose update in containerd isn't the one it had when it
// became unused was imported again, whichever way the time differs, as
// when a clock was stepped back between, and the next change keeps it.
func TestAnImageUpdatedSinceItBecameUnusedIsKeptWhateverTheClock(t *testing.T) {
	for what, shift := range map[string]time.Duration{"later": time.Hour, "earlier": -time.Hour} {
		t.Run(what, func(t *testing.T) {
			n := nodetest.Start(t)
			n.EventuallyHealthy(t)
			n.Import(t, nodetest.Image{Name: "example.test/app:1"})
			fail := true
			s := takeOver(t, n, "app-web", "example.test/app:1", failing(t, n, "/containerd.services.images.v1.Images/Delete", &fail))
			if _, err := s.Remove(t.Context(), "app-web", unfenced); err != nil {
				t.Fatal(err)
			}
			fail = false
			img, err := s.client.ImageService().Get(namespaced(t.Context()), "example.test/app:1")
			if err != nil {
				t.Fatal(err)
			}
			// As if it was updated at another time than now, once unused.
			if err := s.store(nil, []unusedImage{{Image: "example.test/app:1", Updated: img.UpdatedAt.Add(-shift)}}); err != nil {
				t.Fatal(err)
			}
			if _, err := loadAgain(t, s, s.client).Remove(t.Context(), "app-other", unfenced); err != nil {
				t.Fatal(err)
			}
			if !hasImageIn(t, n, "example.test/app:1") {
				t.Errorf("the image updated %s than it was when it became unused went with the next change", what)
			}
			// Recorded as it is, it goes.
			if err := s.store(nil, []unusedImage{{Image: "example.test/app:1", Updated: img.UpdatedAt}}); err != nil {
				t.Fatal(err)
			}
			if _, err := loadAgain(t, s, s.client).Remove(t.Context(), "app-other", unfenced); err != nil {
				t.Fatal(err)
			}
			if hasImageIn(t, n, "example.test/app:1") {
				t.Error("the unused image, as it was when it became unused, is kept by the next change")
			}
		})
	}
}

// An import waits for a change of the containers under way, so that
// nothing it imports is read, marked or deleted halfway through one.
func TestAnImportWaitsForAChangeUnderWay(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.Import(t, nodetest.Image{Name: "example.test/app:1"})
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	client, err := containerd.NewWithConn(n.ContainerdConn(t, grpc.WithUnaryInterceptor(func(ctx context.Context, m string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if m == "/containerd.services.images.v1.Images/Delete" {
			once.Do(func() { close(entered) })
			<-release
		}
		return invoker(ctx, m, req, reply, cc, opts...)
	})))
	if err != nil {
		t.Fatal(err)
	}
	s := takeOver(t, n, "app-web", "example.test/app:1", client)
	removed := make(chan error, 1)
	go func() { _, err := s.Remove(t.Context(), "app-web", unfenced); removed <- err }()
	select {
	case <-entered:
	case <-time.After(time.Minute):
		t.Fatal("the removal didn't delete the image within a minute")
	}
	imported := make(chan error, 1)
	go func() {
		_, err := s.Import(t.Context(), bytes.NewReader(nodetest.Image{Name: "example.test/app:2"}.Archive(t)), unfenced)
		imported <- err
	}()
	select {
	case err := <-imported:
		close(release)
		<-removed
		t.Fatalf("an import during a removal's deletion of an image ended at once: %v, want it to wait for the removal", err)
	case <-time.After(time.Second):
	}
	close(release)
	if err := <-removed; err != nil {
		t.Fatal(err)
	}
	if err := <-imported; err != nil {
		t.Fatal(err)
	}
}

// Whatever keeps the new container from being made, the old one, stopped
// as by a reboot, is started again, and why the new one wasn't made is
// reported: the new image missing, its config unreadable, or the lease
// that would hold it refused.
func TestTheOldContainerStartsAgainWhateverKeepsTheNewOneFromBeingMade(t *testing.T) {
	user := "1000:1000"
	for what, c := range map[string]struct {
		image, method string
	}{
		"the new image missing": {"example.test/app:9", ""},
		"its config unreadable": {"example.test/app:2", "/containerd.services.content.v1.Content/Read"},
		"the lease refused":     {"example.test/app:2", "/containerd.services.leases.v1.Leases/Create"},
	} {
		t.Run(what, func(t *testing.T) {
			n := nodetest.Start(t)
			n.EventuallyHealthy(t)
			n.Import(t, nodetest.Image{Name: "example.test/app:1"})
			fail := false
			s := takeOver(t, n, "app-web", "example.test/app:1", failing(t, n, c.method, &fail))
			if _, err := s.Import(t.Context(), bytes.NewReader(nodetest.Image{Name: "example.test/app:2", Rebuild: 2}.Archive(t)), unfenced); err != nil {
				t.Fatal(err)
			}
			ctx := namespaced(t.Context())
			made, _, err := s.load(ctx, "app-web")
			if err != nil || made == nil {
				t.Fatalf("app-web's container: %v, %v", made, err)
			}
			task, err := made.Task(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			exited, err := task.Wait(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := task.Kill(ctx, syscall.SIGKILL); err != nil {
				t.Fatal(err)
			}
			<-exited
			fail = true
			next := &container{Name: "app-web", Image: c.image, properties: properties{User: &user}}
			if err := s.start(t.Context(), next, false); err == nil {
				t.Fatalf("app-web started from %s with %s", c.image, what)
			}
			if st := s.status(t.Context(), next, s.problems["app-web"]); st.State != nodev1.TaskState_TASK_STATE_RUNNING || st.Image != "example.test/app:1" || st.Problem == "" {
				t.Errorf("with %s, app-web is %v, want it running from app:1 again, saying why app:2 didn't", what, st)
			}
		})
	}
}

// A service without a container, as one whose replacement failed after
// its old container went, run from a rebuild of its image that changes
// only the config, keeps the layer's snapshot, which nothing else holds,
// when containerd collects garbage just as the run finds it there,
// whichever snapshotter the namespace names as its default.
func TestRunningAServiceWithoutAContainerKeepsItsSnapshotUnderGarbageCollection(t *testing.T) {
	for name, nsDefault := range map[string]string{"the namespace naming no default snapshotter": "", "the namespace naming another": "native"} {
		t.Run(name, func(t *testing.T) {
			n, plain, chain := startWithFixtureUnpacked(t)
			ctx := namespaced(t.Context())
			if nsDefault != "" {
				if err := plain.NamespaceService().SetLabel(ctx, hostlayout.ContainerdNamespace, defaults.DefaultSnapshotterNSLabel, nsDefault); err != nil {
					t.Fatal(err)
				}
			}
			// The snapshot is held by a lease alone once the rebuild's
			// import replaces the image it was unpacked for.
			hold, err := plain.LeasesService().Create(ctx, leases.WithRandomID())
			if err != nil {
				t.Fatal(err)
			}
			if err := plain.LeasesService().AddResource(ctx, hold, leases.Resource{ID: chain, Type: "snapshots/" + snapshotter}); err != nil {
				t.Fatal(err)
			}
			fixture := nodetest.Fixture()
			n.Import(t, nodetest.Image{Name: fixture.Image, API: fixture.Api, Reconfigure: 1})

			// The lease goes, collecting garbage, once the run has found
			// the snapshot there.
			var once sync.Once
			collected, prepareSeen, prepared := false, false, error(nil)
			s := containersThrough(t, n, func(ctx context.Context, req any, err error) {
				switch r := req.(type) {
				case *snapshotsapi.StatSnapshotRequest:
					if r.Key == chain && err == nil {
						once.Do(func() {
							if err := plain.LeasesService().Delete(ctx, hold, leases.SynchronousDelete); err != nil {
								t.Errorf("collecting garbage: %v", err)
							}
							collected = true
						})
					}
				case *snapshotsapi.PrepareSnapshotRequest:
					if r.Key == fixture.Name {
						prepareSeen, prepared = true, err
					}
				}
			})
			// The container can't be made from this machine, since its
			// spec reads the node's files: what counts is the snapshot it
			// is made on.
			_, runErr := s.Run(t.Context(), containerOf(fixture), unfenced)
			if !collected {
				t.Fatalf("the run never found the layer's snapshot there: %v", runErr)
			}
			if !prepareSeen || prepared != nil {
				t.Errorf("making the container's snapshot on the layer's: %v (tried: %v), want it made (the run: %v)", prepared, prepareSeen, runErr)
			}
		})
	}
}

// A run whose context ends while it makes a service's container anew
// lets go of the lease that held what it was made on.
func TestARunThatEndsWhileItMakesAContainerLetsGoOfItsLease(t *testing.T) {
	n, plain, chain := startWithFixtureUnpacked(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	s := containersThrough(t, n, func(_ context.Context, req any, err error) {
		if r, ok := req.(*snapshotsapi.StatSnapshotRequest); ok && r.Key == chain {
			cancel()
		}
	})
	if _, err := s.Run(ctx, containerOf(nodetest.Fixture()), unfenced); err == nil {
		t.Fatal("the run went on once its context ended")
	}
	if ls, err := plain.LeasesService().List(namespaced(t.Context())); err != nil || len(ls) != 0 {
		t.Errorf("the leases after the run: %v, %v, want none", ls, err)
	}
}

// A containerd that doesn't answer when a run lets go of its lease holds
// the run no longer than leaseReleaseTimeout past its context.
func TestARunLetsGoOfItsLeaseWithinABound(t *testing.T) {
	n, _, _ := startWithFixtureUnpacked(t)
	hung := make(chan struct{})
	t.Cleanup(func() { close(hung) })
	s := containersThrough(t, n, nil)
	s.client = clientThrough(t, n, func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if method == "/containerd.services.leases.v1.Leases/Delete" {
			select {
			case <-hung:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	})
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	ran := make(chan error, 1)
	go func() { _, err := s.Run(ctx, containerOf(nodetest.Fixture()), unfenced); ran <- err }()
	select {
	case <-ran:
	case <-time.After(3*time.Second + leaseReleaseTimeout + 2*time.Second):
		t.Errorf("the run goes on with the lease's deletion unanswered, more than %s past its context", leaseReleaseTimeout)
	}
}

// An import whose call names a containerd lease, as containerd's clients
// name theirs in a header, holds what it writes in the agent's own lease,
// so that garbage collected before the images are made doesn't take it.
func TestAnImportHoldsWhatItWritesInItsOwnLeaseWhateverLeaseItsCallNames(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	s := containersThrough(t, n, nil)
	s.client = clientThrough(t, n, func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if method == "/containerd.services.images.v1.Images/Create" {
			n.CollectGarbageOnce(t)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	})
	ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs("containerd-lease", "none"))
	if _, err := s.Import(ctx, bytes.NewReader(nodetest.ImageArchive(t, "dns")), unfenced); err != nil {
		t.Errorf("importing dns with a call that names the lease none: %v", err)
	}
}

// A snapshot left under a service's name, as by an agent killed after it
// made the snapshot but before it made the container, gives way to the
// one the service's container is made on.
func TestASnapshotLeftUnderAServicesNameGivesWayToItsContainers(t *testing.T) {
	n, plain, chain := startWithFixtureUnpacked(t)
	ctx := namespaced(t.Context())
	fixture := nodetest.Fixture()
	// As the killed agent's lease holds it.
	left, err := plain.LeasesService().Create(ctx, leases.WithRandomID())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := plain.SnapshotService(snapshotter).Prepare(leases.WithLease(ctx, left.ID), fixture.Name, chain); err != nil {
		t.Fatal(err)
	}
	prepareSeen, prepared := false, error(nil)
	s := containersThrough(t, n, func(_ context.Context, req any, err error) {
		if r, ok := req.(*snapshotsapi.PrepareSnapshotRequest); ok && r.Key == fixture.Name {
			prepareSeen, prepared = true, err
		}
	})
	_, runErr := s.Run(t.Context(), containerOf(fixture), unfenced)
	if !prepareSeen || prepared != nil {
		t.Errorf("making the container's snapshot with one left under its name: %v (tried: %v), want it made (the run: %v)", prepared, prepareSeen, runErr)
	}
}

// unseekable is a reader that can't seek.
type unseekable struct{ io.Reader }

func (unseekable) Seek(int64, int) (int64, error) { return 0, errors.New("can't seek") }

// An import that fails once it has written content, or whose call ends
// partway through the archive, as when mooctl is cut off, leaves no
// lease, and nothing it wrote once garbage is collected.
func TestAnImportThatFailsLeavesNothing(t *testing.T) {
	for name, importThat := range map[string]func(t *testing.T, n *nodetest.Node, s *containers, archive []byte){
		"fails to make its images": func(t *testing.T, n *nodetest.Node, s *containers, archive []byte) {
			made := false
			s.client = clientThrough(t, n, func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
				if method == "/containerd.services.images.v1.Images/Create" {
					made = true
					return status.Error(codes.Unavailable, "images unavailable")
				}
				return invoker(ctx, method, req, reply, cc, opts...)
			})
			// containerd's failure may pass, so it isn't the archive's.
			if _, err := s.Import(t.Context(), bytes.NewReader(archive), unfenced); status.Code(err) != codes.Unavailable || !made {
				t.Fatalf("the import with its images failing to be made: %v (tried: %v), want it failed as UNAVAILABLE", err, made)
			}
		},
		"is refused for its platform": func(t *testing.T, n *nodetest.Node, s *containers, _ []byte) {
			arch := "arm64"
			if runtime.GOARCH == "arm64" {
				arch = "amd64"
			}
			other := nodetest.Image{Name: "example.test/other:1", Architecture: arch}.Archive(t)
			if _, err := s.Import(t.Context(), bytes.NewReader(other), unfenced); status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("the import of an image for %s: %v, want it refused", arch, err)
			}
		},
		"is cut off": func(t *testing.T, n *nodetest.Node, s *containers, archive []byte) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			// The archive's first half, then the call ends.
			r := unseekable{io.MultiReader(bytes.NewReader(archive[:len(archive)/2]), readerFunc(func([]byte) (int, error) {
				cancel()
				return 0, io.ErrUnexpectedEOF
			}))}
			if _, err := s.Import(ctx, r, unfenced); err == nil {
				t.Fatal("the import cut off succeeded")
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			n := nodetest.Start(t)
			n.EventuallyHealthy(t)
			plain := n.Containerd(t)
			ctx := namespaced(t.Context())
			before := blobs(t, plain)
			importThat(t, n, containersThrough(t, n, nil), nodetest.ImageArchive(t, "dns"))
			if ls, err := plain.LeasesService().List(ctx); err != nil || len(ls) != 0 {
				t.Errorf("the leases after the import: %v, %v, want none", ls, err)
			}
			n.CollectGarbageOnce(t)
			if ingests, err := plain.ContentStore().ListStatuses(ctx); err != nil || len(ingests) != 0 {
				t.Errorf("the writes open after the import and a collection: %v, %v, want none", ingests, err)
			}
			if after := blobs(t, plain); !slices.Equal(after, before) {
				t.Errorf("the content after the import and a collection: %v, want %v", after, before)
			}
		})
	}
}

// readerFunc is a reader that is f.
type readerFunc func([]byte) (int, error)

func (f readerFunc) Read(p []byte) (int, error) { return f(p) }

// blobs is the digests of the content in client's containerd, sorted.
func blobs(t *testing.T, client *containerd.Client) []string {
	t.Helper()
	var digests []string
	if err := client.ContentStore().Walk(namespaced(t.Context()), func(info content.Info) error {
		digests = append(digests, info.Digest.String())
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	slices.Sort(digests)
	return digests
}

// startWithFixtureUnpacked starts a node with the fixture's image
// imported and unpacked, and returns it, a client of its containerd, and
// the chain of the image's layers.
func startWithFixtureUnpacked(t *testing.T) (*nodetest.Node, *containerd.Client, string) {
	t.Helper()
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.ImportFixtures(t)
	ctx := namespaced(t.Context())
	plain := n.Containerd(t)
	img, err := plain.GetImage(ctx, nodetest.Fixture().Image)
	if err != nil {
		t.Fatal(err)
	}
	if err := img.Unpack(ctx, snapshotter); err != nil {
		t.Fatal(err)
	}
	diffIDs, err := img.RootFS(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return n, plain, identity.ChainID(diffIDs).String()
}

// containersThrough is the containers of a node's agent, with none stored,
// whose calls to n's containerd each call after, if not nil, once
// answered.
func containersThrough(t *testing.T, n *nodetest.Node, after func(ctx context.Context, req any, err error)) *containers {
	t.Helper()
	client := clientThrough(t, n, func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		err := invoker(ctx, method, req, reply, cc, opts...)
		if after != nil {
			after(ctx, req, err)
		}
		return err
	})
	dir := t.TempDir()
	s, err := loadContainers(t.Context(), client, gateway.New(), Config{ContainersFile: filepath.Join(dir, "containers"), ContainersDir: dir, ContainerLogDir: dir, Runc: "runc"})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// unfenced lets every change of the containers happen.
func unfenced() error { return nil }

// clientThrough is a client of n's containerd whose calls go through
// interceptor.
func clientThrough(t *testing.T, n *nodetest.Node, interceptor grpc.UnaryClientInterceptor) *containerd.Client {
	t.Helper()
	client, err := containerd.NewWithConn(n.ContainerdConn(t, grpc.WithUnaryInterceptor(interceptor)))
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// An import writes each of the archive's blobs to containerd once: its
// platform check reads the archive itself, not containerd's copy of it.
func TestAnImportWritesEachBlobOnce(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	var mu sync.Mutex
	writes := 0
	count := func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		if method == "/containerd.services.content.v1.Content/Write" {
			mu.Lock()
			writes++
			mu.Unlock()
		}
		return streamer(ctx, desc, cc, method, opts...)
	}
	client, err := containerd.NewWithConn(n.ContainerdConn(t, grpc.WithStreamInterceptor(count)))
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	s, err := loadContainers(t.Context(), client, gateway.New(), Config{ContainersFile: filepath.Join(dir, "containers"), ContainersDir: dir, ContainerLogDir: dir, Runc: "runc"})
	if err != nil {
		t.Fatal(err)
	}
	archive := nodetest.Image{Name: "example.test/once:1", Rebuild: 7}.Archive(t)
	if _, err := s.Import(t.Context(), bytes.NewReader(archive), unfenced); err != nil {
		t.Fatal(err)
	}
	// The archive's layer, config and manifest, and its index, which
	// containerd keeps too.
	if writes != 4 {
		t.Errorf("the import wrote to containerd %d times, want once for each of the archive's 3 blobs and its index", writes)
	}
}

// A failure containerd reports that the archive causes, of the class of
// an invalid argument, is the archive's, which no rerun passes; one of
// another class, as containerd not answering, may pass.
func TestAnImportFailingInContainerdIsTheArchivesOrMayPassByItsClass(t *testing.T) {
	for code, want := range map[codes.Code]codes.Code{
		codes.InvalidArgument:    codes.InvalidArgument,
		codes.NotFound:           codes.InvalidArgument,
		codes.FailedPrecondition: codes.InvalidArgument,
		codes.Unavailable:        codes.Unavailable,
		codes.ResourceExhausted:  codes.Unavailable,
		codes.Internal:           codes.Unavailable,
	} {
		t.Run(code.String(), func(t *testing.T) {
			n := nodetest.Start(t)
			n.EventuallyHealthy(t)
			s := containersThrough(t, n, nil)
			s.client = clientThrough(t, n, func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
				if method == "/containerd.services.images.v1.Images/Create" {
					return status.Error(code, "refused by containerd")
				}
				return invoker(ctx, method, req, reply, cc, opts...)
			})
			_, err := s.Import(t.Context(), bytes.NewReader(nodetest.Image{Name: "example.test/class:1"}.Archive(t)), unfenced)
			if status.Code(err) != want {
				t.Errorf("an import that containerd fails with %s = %v, want %s", code, err, want)
			}
		})
	}
}

// An import cut while containerd wrote the archive's index.json, as a
// crash or a stop leaves it, doesn't spoil the next import, of another
// build whose index.json is of the same size.
func TestAnImportCutShortLeavesNoIngestForTheNext(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	index := func(archive []byte) []byte {
		tr := tar.NewReader(bytes.NewReader(archive))
		for {
			h, err := tr.Next()
			if err != nil {
				t.Fatal(err)
			}
			if h.Name == "index.json" {
				b, _ := io.ReadAll(tr)
				return b
			}
		}
	}
	a := index(nodetest.Image{Name: "example.test/a:1"}.Archive(t))
	b := nodetest.Image{Name: "example.test/b:1"}.Archive(t)
	if len(a) != len(index(b)) {
		t.Fatalf("the two builds' index.json are of %d and %d bytes, not the same size the test needs", len(a), len(index(b)))
	}
	client := n.Containerd(t)
	ctx := namespaced(t.Context())
	lease, err := client.LeasesService().Create(ctx, leases.WithRandomID(), leases.WithExpiration(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	// What containerd's import wrote of a's index.json before it was cut.
	w, err := content.OpenWriter(leases.WithLease(ctx, lease.ID), client.ContentStore(), content.WithRef("tar-index.json"), content.WithDescriptor(ocispec.Descriptor{Size: int64(len(a))}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(a[:len(a)-10]); err != nil {
		t.Fatal(err)
	}
	w.Close()
	if _, err := containersThrough(t, n, nil).Import(t.Context(), bytes.NewReader(b), unfenced); err != nil {
		t.Errorf("an import after one cut short: %v", err)
	}
}

// An import aborts only what an earlier import left: a pull's ingest cut
// short stays, for a pull again to take up.
func TestAnImportKeepsAPullsIngest(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	client := n.Containerd(t)
	ctx := namespaced(t.Context())
	lease, err := client.LeasesService().Create(ctx, leases.WithRandomID(), leases.WithExpiration(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	ref := "layer-sha256:" + strings.Repeat("0", 64)
	w, err := content.OpenWriter(leases.WithLease(ctx, lease.ID), client.ContentStore(), content.WithRef(ref), content.WithDescriptor(ocispec.Descriptor{Size: 100}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(make([]byte, 50)); err != nil {
		t.Fatal(err)
	}
	w.Close()
	if _, err := containersThrough(t, n, nil).Import(t.Context(), bytes.NewReader(nodetest.Image{Name: "example.test/b:1"}.Archive(t)), unfenced); err != nil {
		t.Fatal(err)
	}
	if st, err := client.ContentStore().Status(ctx, ref); err != nil || st.Offset != 50 {
		t.Errorf("the pull's ingest after an import: %+v, %v, want it as it was, at 50 bytes", st, err)
	}
}
