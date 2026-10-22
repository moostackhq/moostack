//go:build linux

package agent

import (
	"bytes"
	"context"
	"io"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	snapshotsapi "github.com/containerd/containerd/api/services/snapshots/v1"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/leases"
	"github.com/containerd/containerd/v2/defaults"
	"github.com/opencontainers/image-spec/identity"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/moostackhq/moostack/internal/gateway"
	"github.com/moostackhq/moostack/internal/hostlayout"
	"github.com/moostackhq/moostack/internal/nodetest"
)

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
			n.ImportFixtureReconfiguredAs(t, fixture, 1)

			// The lease goes, collecting garbage, once the run has found
			// the snapshot there.
			var once sync.Once
			collected, prepareSeen, prepared := false, false, error(nil)
			s := servicesThrough(t, n, func(ctx context.Context, req any, err error) {
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
			runErr := s.Run(t.Context(), fixture)
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
	s := servicesThrough(t, n, func(_ context.Context, req any, err error) {
		if r, ok := req.(*snapshotsapi.StatSnapshotRequest); ok && r.Key == chain {
			cancel()
		}
	})
	if err := s.Run(ctx, nodetest.Fixture()); err == nil {
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
	s := servicesThrough(t, n, nil)
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
	go func() { ran <- s.Run(ctx, nodetest.Fixture()) }()
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
	s := servicesThrough(t, n, nil)
	s.client = clientThrough(t, n, func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if method == "/containerd.services.images.v1.Images/Create" {
			n.CollectGarbageOnce(t)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	})
	ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs("containerd-lease", "none"))
	if _, err := s.Import(ctx, bytes.NewReader(nodetest.ImageArchive(t, "dns"))); err != nil {
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
	s := servicesThrough(t, n, func(_ context.Context, req any, err error) {
		if r, ok := req.(*snapshotsapi.PrepareSnapshotRequest); ok && r.Key == fixture.Name {
			prepareSeen, prepared = true, err
		}
	})
	runErr := s.Run(t.Context(), fixture)
	if !prepareSeen || prepared != nil {
		t.Errorf("making the container's snapshot with one left under its name: %v (tried: %v), want it made (the run: %v)", prepared, prepareSeen, runErr)
	}
}

// An import that fails once it has written content, or whose call ends
// partway through the archive, as when mooctl is cut off, leaves no
// lease, and nothing it wrote once garbage is collected.
func TestAnImportThatFailsLeavesNothing(t *testing.T) {
	for name, importThat := range map[string]func(t *testing.T, n *nodetest.Node, s *services, archive []byte){
		"fails to make its images": func(t *testing.T, n *nodetest.Node, s *services, archive []byte) {
			made := false
			s.client = clientThrough(t, n, func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
				if method == "/containerd.services.images.v1.Images/Create" {
					made = true
					return status.Error(codes.Unavailable, "images unavailable")
				}
				return invoker(ctx, method, req, reply, cc, opts...)
			})
			if _, err := s.Import(t.Context(), bytes.NewReader(archive)); err == nil || !made {
				t.Fatalf("the import with its images failing to be made: %v (tried: %v), want it failed", err, made)
			}
		},
		"is cut off": func(t *testing.T, n *nodetest.Node, s *services, archive []byte) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			// The archive's first half, then the call ends.
			r := io.MultiReader(bytes.NewReader(archive[:len(archive)/2]), readerFunc(func([]byte) (int, error) {
				cancel()
				return 0, io.ErrUnexpectedEOF
			}))
			if _, err := s.Import(ctx, r); err == nil {
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
			importThat(t, n, servicesThrough(t, n, nil), nodetest.ImageArchive(t, "dns"))
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

// servicesThrough is the services of a node's agent, with none stored,
// whose calls to n's containerd each call after, if not nil, once
// answered.
func servicesThrough(t *testing.T, n *nodetest.Node, after func(ctx context.Context, req any, err error)) *services {
	t.Helper()
	client := clientThrough(t, n, func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		err := invoker(ctx, method, req, reply, cc, opts...)
		if after != nil {
			after(ctx, req, err)
		}
		return err
	})
	dir := t.TempDir()
	s, err := loadServices(t.Context(), client, gateway.New(), filepath.Join(dir, "services.json"), dir, dir)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

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
