package orchestrate_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/nodeclient"
	"github.com/moostackhq/moostack/internal/nodetest"
	"github.com/moostackhq/moostack/internal/orchestrate"
)

// fixtureNode is the node API of a node fixture, whose connections the
// fixture closes.
type fixtureNode struct{ *nodeclient.Node }

func (fixtureNode) Close() error { return nil }

// An update renames a service whose old container runs the image the
// update runs it from, of the same build or another of the same version,
// as cluster update --source from the commit the cluster runs does: the
// removal of the old container, which deletes its image, doesn't take the
// image the update imported, and the service runs under its service-
// name alone, on a real agent.
func TestARenameAtTheSameVersionRunsTheService(t *testing.T) {
	for name, rebuild := range map[string]int{"the same build": 0, "another build": 1} {
		t.Run(name, func(t *testing.T) {
			n := nodetest.Start(t)
			n.EventuallyHealthy(t)
			n.ImportFixtures(t)
			// The service under its name alone, from the image the update
			// runs it from.
			old := proto.Clone(nodetest.Fixture()).(*nodev1.Container)
			old.Name = "dns"
			n.HoldLock(t)
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			if _, err := n.Agent.Run(ctx, &nodev1.RunRequest{Container: old, Lease: nodetest.Lease}); err != nil {
				t.Fatal(err)
			}
			if _, err := n.Agent.ReleaseLock(ctx, &nodev1.ReleaseLockRequest{Lease: nodetest.Lease}); err != nil {
				t.Fatal(err)
			}

			dial := func(context.Context, string) (orchestrate.Node, error) {
				return fixtureNode{&nodeclient.Node{Agent: n.Agent, Updater: n.Updater}}, nil
			}
			cl, err := orchestrate.Preflight(ctx, orchestrate.Nodes{Names: []string{"node1"}, Dial: dial}, 0, true, holder)
			if err != nil {
				t.Fatal(err)
			}
			defer cl.Close()
			img := nodetest.ImageOf(nodetest.Fixture())
			img.Rebuild = rebuild
			svc := orchestrate.Service{Name: "dns", Version: "fixture", Container: nodetest.Fixture(), Archive: img.Archive(t), Replaces: "dns", ReplacesImages: "moostack.local/dns:"}
			err = cl.Update(cl.Context(), func(string) (orchestrate.NodeRelease, error) {
				return orchestrate.NodeRelease{Services: []orchestrate.Service{svc}}, nil
			}, func(orchestrate.Step) {})
			if err != nil {
				t.Fatalf("the rename: %v", err)
			}
			s, err := n.Status(t)
			if err != nil {
				t.Fatal(err)
			}
			var names []string
			for _, c := range s.Containers {
				names = append(names, c.Name)
			}
			if !slices.Equal(names, []string{nodetest.Fixture().Name}) {
				t.Errorf("the node runs %q after the rename, want %s alone", names, nodetest.Fixture().Name)
			}
		})
	}
}

// stopDuringRemove is a fixture node whose Remove stops mooctl while the
// call is in flight, as a Ctrl-C would.
type stopDuringRemove struct {
	fixtureNode
	stop func()
}

func (s stopDuringRemove) Remove(ctx context.Context, name, lease string) error {
	go func() { time.Sleep(20 * time.Millisecond); s.stop() }()
	return s.fixtureNode.Remove(ctx, name, lease)
}

// mooctl stopped while a rename's removal of the old container is under
// way, which the agent finishes, still runs the service, on a real agent,
// rather than leaving the node with neither container.
func TestARenameStoppedDuringTheRemovalStillRunsTheService(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.ImportFixtures(t)
	old := proto.Clone(nodetest.Fixture()).(*nodev1.Container)
	old.Name = "dns"
	n.HoldLock(t)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	if _, err := n.Agent.Run(ctx, &nodev1.RunRequest{Container: old, Lease: nodetest.Lease}); err != nil {
		t.Fatal(err)
	}
	if _, err := n.Agent.ReleaseLock(ctx, &nodev1.ReleaseLockRequest{Lease: nodetest.Lease}); err != nil {
		t.Fatal(err)
	}
	opCtx, stop := context.WithCancel(t.Context())
	defer stop()
	dial := func(context.Context, string) (orchestrate.Node, error) {
		return stopDuringRemove{fixtureNode{&nodeclient.Node{Agent: n.Agent, Updater: n.Updater}}, stop}, nil
	}
	cl, err := orchestrate.Preflight(opCtx, orchestrate.Nodes{Names: []string{"node1"}, Dial: dial}, 0, true, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	img := nodetest.ImageOf(nodetest.Fixture())
	img.Rebuild = 1
	svc := orchestrate.Service{Name: "dns", Version: "fixture", Container: nodetest.Fixture(), Archive: img.Archive(t), Replaces: "dns", ReplacesImages: "moostack.local/dns:"}
	err = cl.Update(opCtx, func(string) (orchestrate.NodeRelease, error) {
		return orchestrate.NodeRelease{Services: []orchestrate.Service{svc}}, nil
	}, func(orchestrate.Step) {})
	t.Logf("the update stopped during the removal: %v", err)
	s, err := n.Status(t)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, c := range s.Containers {
		names = append(names, c.Name)
	}
	if !slices.Contains(names, nodetest.Fixture().Name) {
		t.Errorf("the node runs %q after mooctl was stopped during the removal, want %s", names, nodetest.Fixture().Name)
	}
}
