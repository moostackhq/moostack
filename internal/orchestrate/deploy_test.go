package orchestrate_test

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/nodeclient"
	"github.com/moostackhq/moostack/internal/orchestrate"
)

// appFake is how a fake node pulls images and runs apps, as the agent
// does.
type appFake struct {
	// registry are the images the node can pull, by reference, each with
	// the digest of its build. pullErr, if set, is why a pull fails, and
	// pullDelay how long one takes; onPull, if set, is called as one
	// starts.
	registry  map[string]string
	pullErr   error
	pullDelay time.Duration
	onPull    func()
	// afterPull, if set, is called as a pull that succeeds returns.
	afterPull func()
	// pulled are the references the node pulled, in order.
	pulled []string
	// importErr, if set, is why an import of an app's image fails, and
	// importNames, if set, the names an import answers with in place of
	// the image's; imported are the names of the images the node
	// imported, in order. afterImport, if set, is called once an import
	// is done, before it answers.
	importErr   error
	importNames []string
	imported    []string
	afterImport func()
	// digests are the digests of the builds of the images the node
	// pulled, by name.
	digests map[string]string
	// exits has an app's container run from the image it names exit at
	// once with the code it gives, and fails one fail with the problem
	// it gives.
	exits map[string]uint32
	fails map[string]string
	// runErr, if set, is why running an app fails, and stopsOld says the
	// app's old container is gone by then, as when the new one can't
	// start once the old one is deleted.
	runErr   error
	stopsOld bool
	// restartsOnRename has a run that renames a container start its
	// process all the same, as when the process stopped meanwhile.
	restartsOnRename bool
	// leaves, if set, is the app's container a failed run leaves the node
	// running in place of the one before.
	leaves *nodev1.ContainerStatus
	// started says whether the last run started its container's process.
	started bool
	// statusErr, if set, is why the agent doesn't answer status, and
	// afterRun, if set, is called once an app's run is done, with n.mu
	// held.
	statusErr error
	afterRun  func()
	// onRun, if set, is called as an app's run starts, holding the runner,
	// and runDelay is how long the run takes before it makes the
	// container; waiting, if set, is called as a run is about to wait for
	// the runner.
	onRun    func()
	runDelay time.Duration
	waiting  func()
}

// PullImage pulls ref from the node's registry, keeping it as an image
// of the node's, unless its call ended, lease doesn't hold the node's
// operation lock, or pullErr says why it fails; a reference the
// registry doesn't have isn't found.
func (n *node) PullImage(ctx context.Context, ref, lease string) (nodeclient.PulledImage, error) {
	if err := ctx.Err(); err != nil {
		return nodeclient.PulledImage{}, err
	}
	if err := n.fence(lease); err != nil {
		return nodeclient.PulledImage{}, err
	}
	n.mu.Lock()
	onPull, delay := n.app.onPull, n.app.pullDelay
	n.app.pulled = append(n.app.pulled, ref)
	n.mu.Unlock()
	if onPull != nil {
		onPull()
	}
	select {
	case <-time.After(delay):
	case <-ctx.Done():
		return nodeclient.PulledImage{}, ctx.Err()
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.app.pullErr != nil {
		return nodeclient.PulledImage{}, n.app.pullErr
	}
	digest, ok := n.app.registry[ref]
	if !ok {
		return nodeclient.PulledImage{}, status.Errorf(codes.NotFound, "%s: not found", ref)
	}
	if n.images == nil {
		n.images = map[string]bool{}
	}
	if n.app.digests == nil {
		n.app.digests = map[string]string{}
	}
	n.images[ref], n.app.digests[ref] = true, digest
	if n.app.afterPull != nil {
		n.mu.Unlock()
		n.app.afterPull()
		n.mu.Lock()
	}
	return nodeclient.PulledImage{Name: ref, Digest: digest}, nil
}

// importApp imports the app's image in the OCI image layout archive, as
// the name its index gives it, with its digest, unless importErr says
// why it fails.
func (n *node) importApp(archive []byte) ([]string, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.app.importErr != nil {
		return nil, n.app.importErr
	}
	name, digest := archiveImage(archive)
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "importing the image: not an image's archive")
	}
	if n.images == nil {
		n.images = map[string]bool{}
	}
	if n.app.digests == nil {
		n.app.digests = map[string]string{}
	}
	n.images[name], n.app.digests[name] = true, digest
	n.app.imported = append(n.app.imported, name)
	if n.app.afterImport != nil {
		n.mu.Unlock()
		n.app.afterImport()
		n.mu.Lock()
	}
	if n.app.importNames != nil {
		return n.app.importNames, nil
	}
	return []string{name}, nil
}

// archiveImage is the name and the digest of the image in the OCI image
// layout archive, as its index gives them, empty if it isn't one.
func archiveImage(archive []byte) (string, string) {
	r := tar.NewReader(bytes.NewReader(archive))
	for {
		h, err := r.Next()
		if err != nil {
			return "", ""
		}
		if h.Name != "index.json" {
			continue
		}
		var index struct {
			Manifests []struct {
				Digest      string
				Annotations map[string]string
			}
		}
		if err := json.NewDecoder(r).Decode(&index); err != nil || len(index.Manifests) != 1 {
			return "", ""
		}
		return index.Manifests[0].Annotations["io.containerd.image.name"], index.Manifests[0].Digest
	}
}

// imports are the names of the apps' images the node imported so far.
func (n *node) imports() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return slices.Clone(n.app.imported)
}

// runAppCall runs the app's container c, as runApp does, holding the
// runner, as the agent's run does, which checks the lease once it no
// longer waits for it. Like the agent's, it goes on if its call ends,
// its caller told at once that the call ended.
func (n *node) runAppCall(ctx context.Context, c *nodev1.Container, lease string) error {
	ran := make(chan error, 1)
	n.mu.Lock()
	waiting := n.app.waiting
	n.mu.Unlock()
	go func() {
		if waiting != nil {
			waiting()
		}
		n.runner.Lock()
		defer n.runner.Unlock()
		if err := n.fence(lease); err != nil {
			ran <- err
			return
		}
		n.mu.Lock()
		onRun, delay := n.app.onRun, n.app.runDelay
		n.mu.Unlock()
		if onRun != nil {
			onRun()
		}
		time.Sleep(delay)
		ran <- n.runApp(c)
	}()
	select {
	case err := <-ran:
		if err != nil {
			return err
		}
		return ctx.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}

// runApp runs the app's container c, as the agent does, from an image
// the node has, refusing one with properties other than its name and
// image, as no app runs with; it replaces the container of its name,
// running unless exits or fails says otherwise. The caller holds the
// lease, and not n.mu.
func (n *node) runApp(c *nodev1.Container) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.ran = append(n.ran, c)
	if !proto.Equal(c, &nodev1.Container{Name: c.Name, Image: c.Image}) {
		return status.Errorf(codes.InvalidArgument, "%s has properties other than its name and image: %v", c.Name, c)
	}
	if !n.images[c.Image] {
		return status.Errorf(codes.FailedPrecondition, "the node has no image %s", c.Image)
	}
	if n.app.runErr != nil {
		if n.app.stopsOld || n.app.leaves != nil {
			n.others = without(n.others, c.Name)
		}
		if n.app.leaves != nil {
			n.others = append(n.others, n.app.leaves)
		}
		return n.app.runErr
	}
	k := &nodev1.ContainerStatus{Name: c.Name, Image: c.Image, Digest: n.app.digests[c.Image], State: nodev1.TaskState_TASK_STATE_RUNNING}
	// A running container of the same build takes the new name in place.
	if i := slices.IndexFunc(n.others, func(o *nodev1.ContainerStatus) bool { return o.Name == c.Name }); i >= 0 &&
		n.others[i].State == nodev1.TaskState_TASK_STATE_RUNNING && n.others[i].Digest == k.Digest && !n.app.restartsOnRename {
		n.others[i].Image = c.Image
		n.app.started = false
		return nil
	}
	if code, ok := n.app.exits[c.Image]; ok {
		k.State, k.ExitCode = nodev1.TaskState_TASK_STATE_EXITED, code
	}
	if problem, ok := n.app.fails[c.Image]; ok {
		k.State, k.Problem = nodev1.TaskState_TASK_STATE_FAILED, problem
	}
	n.others = append(without(n.others, c.Name), k)
	if n.app.afterRun != nil {
		n.app.afterRun()
	}
	return nil
}

// pulls are the references the node pulled so far.
func (n *node) pulls() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return slices.Clone(n.app.pulled)
}

// appContainer is a container of the app called name, running image of
// the build digest.
func appContainer(name, image, digest string) *nodev1.ContainerStatus {
	return &nodev1.ContainerStatus{Name: "app-" + name, Image: image, Digest: digest, State: nodev1.TaskState_TASK_STATE_RUNNING}
}

// containersOf are containers called names.
func containersOf(names ...string) []*nodev1.ContainerStatus {
	var cs []*nodev1.ContainerStatus
	for _, name := range names {
		cs = append(cs, &nodev1.ContainerStatus{Name: name})
	}
	return cs
}

func TestPlacePicksTheNodeThatRunsTheAppElseTheOneWithTheFewestApps(t *testing.T) {
	for name, c := range map[string]struct {
		containers map[string][]*nodev1.ContainerStatus
		want       orchestrate.Placement
	}{
		"one node": {map[string][]*nodev1.ContainerStatus{"node1": nil}, orchestrate.Placement{Node: "node1"}},
		"the node that runs it, though it runs the most apps": {map[string][]*nodev1.ContainerStatus{
			"node1": containersOf("app-api"), "node2": containersOf("app-web", "app-db"),
		}, orchestrate.Placement{Node: "node2", RunsThere: true}},
		"the one that runs it, exited": {map[string][]*nodev1.ContainerStatus{
			"node1": nil, "node2": {{Name: "app-web", State: nodev1.TaskState_TASK_STATE_EXITED}},
		}, orchestrate.Placement{Node: "node2", RunsThere: true}},
		"the fewest apps": {map[string][]*nodev1.ContainerStatus{
			"node1": containersOf("app-a", "app-b"), "node2": containersOf("app-c"), "node3": containersOf("app-d", "app-e"),
		}, orchestrate.Placement{Node: "node2"}},
		"a tie, by name": {map[string][]*nodev1.ContainerStatus{
			"node3": containersOf("app-a"), "node1": containersOf("app-b"), "node2": containersOf("app-c"),
		}, orchestrate.Placement{Node: "node1"}},
		"a tie of names that sort apart from their numbers": {map[string][]*nodev1.ContainerStatus{
			"node10": nil, "node9": nil, "node2": nil,
		}, orchestrate.Placement{Node: "node10"}},
		"cluster services and other containers count for nothing": {map[string][]*nodev1.ContainerStatus{
			"node1": containersOf("app-a"), "node2": containersOf("service-dns", "service-proxy", "dns", "apps", "app-"),
		}, orchestrate.Placement{Node: "node2"}},
		"an app of a longer name isn't the app": {map[string][]*nodev1.ContainerStatus{
			"node1": containersOf("app-web-2"), "node2": nil,
		}, orchestrate.Placement{Node: "node2"}},
		// A node keeps an app whose container exited, failed or is of a
		// state it can't tell, and starts it again when it reboots.
		"apps that don't run count as the node's": {map[string][]*nodev1.ContainerStatus{
			"node1": {{Name: "app-a", State: nodev1.TaskState_TASK_STATE_EXITED}},
			"node2": {{Name: "app-b", State: nodev1.TaskState_TASK_STATE_FAILED}},
			"node3": {{Name: "app-c", State: nodev1.TaskState_TASK_STATE_UNSPECIFIED}},
			"node4": containersOf("app-d", "app-e"),
			"node5": {appContainer("f", nginx, nginxBuild)},
		}, orchestrate.Placement{Node: "node1"}},
	} {
		got, err := orchestrate.Place("web", c.containers)
		if err != nil || got != c.want {
			t.Errorf("%s: Place = %+v, %v, want %+v", name, got, err, c.want)
		}
	}
}

func TestPlaceRefusesAnAppOnMoreThanOneNodeNamingThem(t *testing.T) {
	_, err := orchestrate.Place("web", map[string][]*nodev1.ContainerStatus{
		"node3": containersOf("app-web"), "node1": containersOf("app-web"), "node2": nil,
	})
	var dup *orchestrate.DuplicateAppError
	if !errors.As(err, &dup) || !slices.Equal(dup.Nodes, []string{"node1", "node3"}) || dup.App != "web" {
		t.Fatalf("Place of an app on two nodes: %v, want a *DuplicateAppError naming node1 and node3", err)
	}
	if want := "web runs on node1 and node3, more than one node, so mooctl can't tell which to replace; nothing changed"; err.Error() != want {
		t.Errorf("the error says %q, want %q", err, want)
	}
	if _, err := orchestrate.Place("web", nil); err == nil {
		t.Error("Place on no nodes picked one")
	}
	// Copies that don't run are copies all the same.
	_, err = orchestrate.Place("web", map[string][]*nodev1.ContainerStatus{
		"node4":  {{Name: "app-web", State: nodev1.TaskState_TASK_STATE_EXITED}},
		"node2":  {{Name: "app-web", State: nodev1.TaskState_TASK_STATE_UNSPECIFIED}},
		"node10": {{Name: "app-web", State: nodev1.TaskState_TASK_STATE_FAILED}},
		"node1":  containersOf("app-web"),
	})
	if !errors.As(err, &dup) || !slices.Equal(dup.Nodes, []string{"node1", "node10", "node2", "node4"}) {
		t.Fatalf("Place of an app on four nodes, three not running: %v, want a *DuplicateAppError naming the four in order", err)
	}
	if want := "web runs on node1, node10, node2 and node4, more than one node, so mooctl can't tell which to replace; nothing changed"; err.Error() != want {
		t.Errorf("the error says %q, want %q", err, want)
	}
}

// Place, over seeded clusters of generated nodes and containers, picks
// as its rule says, whatever order the nodes come in: the app on more
// than one node is refused, naming them in order; on one, that node; or
// else a node with the fewest apps, the first by name of those.
func TestPlaceFollowsItsRule(t *testing.T) {
	seed := stateSeed(t)
	t.Logf("MOOSTACK_STATE_SEED=%d", seed)
	r := rand.New(rand.NewPCG(seed, seed))
	names := []string{"app-web", "app-api", "app-", "app-web-2", "web", "service-dns", "apps"}
	states := []nodev1.TaskState{nodev1.TaskState_TASK_STATE_RUNNING, nodev1.TaskState_TASK_STATE_EXITED, nodev1.TaskState_TASK_STATE_FAILED, nodev1.TaskState_TASK_STATE_UNSPECIFIED}
	for i := range 20000 {
		containers := map[string][]*nodev1.ContainerStatus{}
		var on, nodes []string
		apps := map[string]int{}
		for range 1 + r.IntN(12) {
			// Numbers that sort apart from their names, as node10 before
			// node2.
			node := fmt.Sprint("node", 1+r.IntN(20))
			if _, ok := containers[node]; ok {
				continue
			}
			nodes = append(nodes, node)
			var cs []*nodev1.ContainerStatus
			for range r.IntN(4) {
				cs = append(cs, &nodev1.ContainerStatus{Name: names[r.IntN(len(names))], State: states[r.IntN(len(states))]})
			}
			containers[node] = cs
			for _, c := range cs {
				if c.Name == "app-web" && !slices.Contains(on, node) {
					on = append(on, node)
				}
				if c.Name == "app-web" || c.Name == "app-api" || c.Name == "app-web-2" {
					apps[node]++
				}
			}
		}
		slices.Sort(on)
		slices.Sort(nodes)
		var want orchestrate.Placement
		switch len(on) {
		case 0:
			want.Node = nodes[0]
			for _, node := range nodes {
				if apps[node] < apps[want.Node] {
					want.Node = node
				}
			}
		case 1:
			want = orchestrate.Placement{Node: on[0], RunsThere: true}
		}
		// Each call ranges over the map in an order of its own.
		for range 5 {
			got, err := orchestrate.Place("web", containers)
			var dup *orchestrate.DuplicateAppError
			if len(on) > 1 {
				if !errors.As(err, &dup) || !slices.Equal(dup.Nodes, on) {
					t.Fatalf("cluster %d, %v: Place = %+v, %v, want a *DuplicateAppError naming %v", i, containers, got, err, on)
				}
			} else if err != nil || got != want {
				t.Fatalf("cluster %d, %v: Place = %+v, %v, want %+v", i, containers, got, err, want)
			}
		}
	}
}

func BenchmarkPlace(b *testing.B) {
	containers := map[string][]*nodev1.ContainerStatus{}
	for i := range 1000 {
		var cs []*nodev1.ContainerStatus
		for j := range 50 {
			cs = append(cs, &nodev1.ContainerStatus{Name: fmt.Sprintf("app-%d-%d", i, j)})
		}
		containers[fmt.Sprintf("node%d", i)] = cs
	}
	for b.Loop() {
		if _, err := orchestrate.Place("web", containers); err != nil {
			b.Fatal(err)
		}
	}
}

// The images the deploy tests pull.
const (
	nginx      = "docker.io/library/nginx:1.27"
	nginxBuild = "sha256:1111"
	newBuild   = "sha256:2222"
)

// deploying is a cluster of f, past its preflight, for the test.
func deploying(t *testing.T, f fakes, edit func(*orchestrate.Nodes)) *orchestrate.Cluster {
	t.Helper()
	nodes := f.nodes()
	nodes.StartCheck = time.Millisecond
	if edit != nil {
		edit(&nodes)
	}
	cl, err := orchestrate.Preflight(t.Context(), nodes, 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cl.Close() })
	return cl
}

// withRegistry is n, able to pull nginx at the build digest.
func withRegistry(n *node, digest string) *node {
	n.generation = 1
	n.app.registry = map[string]string{nginx: digest}
	return n
}

// deploy deploys web, running nginx, on cl, and returns what it told.
func deploy(t *testing.T, cl *orchestrate.Cluster) (*orchestrate.Deployed, []orchestrate.DeployStep, error) {
	t.Helper()
	var steps []orchestrate.DeployStep
	d, err := cl.Deploy(t.Context(), orchestrate.App{Name: "web", Image: nginx}, func(s orchestrate.DeployStep) { steps = append(steps, s) })
	return d, steps, err
}

func TestDeployRunsANewAppOnTheNodeWithTheFewestApps(t *testing.T) {
	busy := withRegistry(&node{others: containersOf("app-api")}, nginxBuild)
	free := withRegistry(&node{}, nginxBuild)
	cl := deploying(t, fakes{"node1": busy, "node2": free}, nil)
	d, steps, err := deploy(t, cl)
	if err != nil {
		t.Fatal(err)
	}
	if *d != (orchestrate.Deployed{Node: "node2", Image: nginx}) {
		t.Errorf("Deploy = %+v, want web on node2, changed", d)
	}
	want := []orchestrate.DeployStep{{Placement: orchestrate.Placement{Node: "node2"}}, {Placement: orchestrate.Placement{Node: "node2"}, Fetching: true}}
	if !slices.Equal(steps, want) {
		t.Errorf("Deploy told %+v, want %+v", steps, want)
	}
	if got := free.containersRun(); len(got) != 1 || !proto.Equal(got[0], &nodev1.Container{Name: "app-web", Image: nginx}) {
		t.Errorf("node2 ran %v, want app-web from %s with nothing else", got, nginx)
	}
	if len(busy.pulls()) != 0 || len(busy.containersRun()) != 0 {
		t.Errorf("node1 pulled %v and ran %v, want nothing", busy.pulls(), busy.containersRun())
	}
}

func TestARedeployOfTheSameBuildChangesNothing(t *testing.T) {
	n := withRegistry(&node{others: []*nodev1.ContainerStatus{appContainer("web", nginx, nginxBuild)}}, nginxBuild)
	cl := deploying(t, fakes{"node1": withRegistry(&node{}, nginxBuild), "node2": n}, nil)
	d, steps, err := deploy(t, cl)
	if err != nil {
		t.Fatal(err)
	}
	if *d != (orchestrate.Deployed{Node: "node2", Image: nginx, Unchanged: true}) || !steps[0].RunsThere {
		t.Errorf("Deploy = %+v, told %+v, want web on node2, where it runs, unchanged", d, steps)
	}
	if got := n.containersRun(); len(got) != 0 {
		t.Errorf("the node ran %v, want nothing", got)
	}
	if got := n.pulls(); !slices.Equal(got, []string{nginx}) {
		t.Errorf("the node pulled %v, want %s, which tells the build", got, nginx)
	}
}

func TestARedeployReplacesAnAppThatDoesntRunAsTheImagesBuild(t *testing.T) {
	for name, running := range map[string]*nodev1.ContainerStatus{
		"another build":       appContainer("web", nginx, nginxBuild),
		"exited":              {Name: "app-web", Image: nginx, Digest: newBuild, State: nodev1.TaskState_TASK_STATE_EXITED, ExitCode: 1},
		"whose image unknown": {Name: "app-web", State: nodev1.TaskState_TASK_STATE_UNSPECIFIED},
	} {
		n := withRegistry(&node{others: []*nodev1.ContainerStatus{running}}, newBuild)
		cl := deploying(t, fakes{"node1": withRegistry(&node{}, newBuild), "node2": n}, nil)
		d, _, err := deploy(t, cl)
		if err != nil || *d != (orchestrate.Deployed{Node: "node2", Image: nginx}) {
			t.Errorf("%s: Deploy = %+v, %v, want web on node2, changed", name, d, err)
		}
		if got := n.containersRun(); len(got) != 1 {
			t.Errorf("%s: the node ran %v, want app-web once", name, got)
		}
	}
}

func TestAFailedPullChangesNothing(t *testing.T) {
	limited := status.Error(codes.ResourceExhausted, "rate limited")
	n := withRegistry(&node{others: []*nodev1.ContainerStatus{appContainer("web", nginx, nginxBuild)}}, newBuild)
	n.app.pullErr = limited
	cl := deploying(t, fakes{"node1": n}, nil)
	_, _, err := deploy(t, cl)
	var pe *orchestrate.PullError
	if !errors.As(err, &pe) || pe.Node != "node1" || pe.Image != nginx || status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("Deploy with a pull that fails: %v, want a *PullError of node1 and %s, RESOURCE_EXHAUSTED", err, nginx)
	}
	if got := n.containersRun(); len(got) != 0 {
		t.Errorf("the node ran %v after a failed pull", got)
	}
}

func TestAFailedRunSaysWhatRuns(t *testing.T) {
	old := appContainer("web", nginx, nginxBuild)
	for name, c := range map[string]struct {
		before    []*nodev1.ContainerStatus
		stopsOld  bool
		leaves    *nodev1.ContainerStatus
		want      orchestrate.RunOutcome
		wantImage string
	}{
		"the old one runs on":             {[]*nodev1.ContainerStatus{old}, false, nil, orchestrate.RunsAsBefore, nginx},
		"the old one was stopped already": {[]*nodev1.ContainerStatus{old}, true, nil, orchestrate.NotRunning, ""},
		"none ran":                        {nil, false, nil, orchestrate.NotRunning, ""},
		// The new container started, though the run's answer says it failed.
		"the new one runs": {[]*nodev1.ContainerStatus{old}, false, appContainer("web", nginx, newBuild), orchestrate.RunsAsAsked, nginx},
		"another runs": {[]*nodev1.ContainerStatus{old}, false, appContainer("web", "docker.io/library/nginx:1.26", "sha256:3333"),
			orchestrate.RunsOther, "docker.io/library/nginx:1.26"},
		// containerd doesn't answer, so the node can't tell.
		"the node can't tell": {[]*nodev1.ContainerStatus{old}, false, &nodev1.ContainerStatus{Name: "app-web", State: nodev1.TaskState_TASK_STATE_UNSPECIFIED},
			orchestrate.RunUnknown, ""},
	} {
		n := withRegistry(&node{others: c.before}, newBuild)
		n.app.runErr, n.app.stopsOld, n.app.leaves = status.Error(codes.Internal, "the disk is full"), c.stopsOld, c.leaves
		cl := deploying(t, fakes{"node1": n}, nil)
		_, _, err := deploy(t, cl)
		var re *orchestrate.RunError
		if !errors.As(err, &re) || re.Node != "node1" || re.Runs != c.want || re.Image != c.wantImage || !strings.Contains(err.Error(), "the disk is full") {
			t.Errorf("%s: Deploy = %v (%+v), want a *RunError saying %v runs, %q", name, err, re, c.want, c.wantImage)
		}
	}
}

func TestAnAppThatDoesntRunOnceStartedFailsTheDeploy(t *testing.T) {
	exits := withRegistry(&node{}, nginxBuild)
	exits.app.exits = map[string]uint32{nginx: 3}
	cl := deploying(t, fakes{"node1": exits}, func(n *orchestrate.Nodes) { n.StartCheck = 50 * time.Millisecond })
	_, _, err := deploy(t, cl)
	var nr *orchestrate.NotRunningError
	if !errors.As(err, &nr) || !nr.Exited || nr.ExitCode != 3 || nr.Log != "/var/lib/moostack/logs/app-web.log" || nr.Within != 50*time.Millisecond {
		t.Errorf("Deploy of an app that exits: %v (%+v), want a *NotRunningError with code 3 and its log", err, nr)
	}
	fails := withRegistry(&node{}, nginxBuild)
	fails.app.fails = map[string]string{nginx: "exec format error"}
	cl = deploying(t, fakes{"node1": fails}, nil)
	_, _, err = deploy(t, cl)
	if !errors.As(err, &nr) || nr.Exited || nr.Problem != "exec format error" {
		t.Errorf("Deploy of an app that fails: %v, want a *NotRunningError saying why", err)
	}
}

// The check waits its time out before it reads the node's status.
func TestTheStartCheckWaitsItsTime(t *testing.T) {
	cl := deploying(t, fakes{"node1": withRegistry(&node{}, nginxBuild)}, func(n *orchestrate.Nodes) { n.StartCheck = 300 * time.Millisecond })
	start := time.Now()
	if _, _, err := deploy(t, cl); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took < 300*time.Millisecond {
		t.Errorf("the deploy took %s, less than its start check", took)
	}
}

func TestADeployStoppedBeforeTheCheckSaysItCouldntCheck(t *testing.T) {
	cl := deploying(t, fakes{"node1": withRegistry(&node{}, nginxBuild)}, func(n *orchestrate.Nodes) { n.StartCheck = time.Hour })
	ctx, cancel := context.WithCancel(t.Context())
	time.AfterFunc(100*time.Millisecond, cancel)
	_, err := cl.Deploy(ctx, orchestrate.App{Name: "web", Image: nginx}, func(orchestrate.DeployStep) {})
	var u *orchestrate.UncheckedError
	if !errors.As(err, &u) || u.Node != "node1" || !errors.Is(err, context.Canceled) {
		t.Errorf("Deploy stopped during the check: %v, want an *UncheckedError", err)
	}
}

func TestADeployOfAnAppOnTwoNodesChangesNothing(t *testing.T) {
	a := withRegistry(&node{others: []*nodev1.ContainerStatus{appContainer("web", nginx, nginxBuild)}}, nginxBuild)
	b := withRegistry(&node{others: []*nodev1.ContainerStatus{appContainer("web", nginx, nginxBuild)}}, nginxBuild)
	cl := deploying(t, fakes{"node1": a, "node2": b}, nil)
	if _, _, err := deploy(t, cl); !errors.As(err, new(*orchestrate.DuplicateAppError)) {
		t.Errorf("Deploy of an app on two nodes: %v, want a *DuplicateAppError", err)
	}
	for name, n := range map[string]*node{"node1": a, "node2": b} {
		if len(n.pulls())+len(n.containersRun()) != 0 {
			t.Errorf("%s pulled %v and ran %v for an app on two nodes", name, n.pulls(), n.containersRun())
		}
	}
}

func TestADeployFailsForANodeThatDoesntSayWhatItRuns(t *testing.T) {
	n := withRegistry(&node{}, nginxBuild)
	cl := deploying(t, fakes{"node1": n, "node2": withRegistry(&node{}, nginxBuild)}, nil)
	n.mu.Lock()
	n.app.statusErr = errors.New("connection reset")
	n.mu.Unlock()
	_, _, err := deploy(t, cl)
	var ne *orchestrate.NodeError
	if !errors.As(err, &ne) || ne.Node != "node1" || !strings.Contains(err.Error(), "connection reset") || !strings.Contains(err.Error(), "nothing changed") {
		t.Errorf("Deploy with a node that doesn't answer status: %v, want a *NodeError naming node1", err)
	}
}

// A pull that outlasts the lease, as when the lock was broken and taken
// meanwhile, runs nothing.
func TestADeployThatLostTheLockRunsNothing(t *testing.T) {
	n := withRegistry(&node{}, nginxBuild)
	n.app.pullDelay = 600 * time.Millisecond
	n.app.onPull = func() { n.lockFor("bobs", time.Hour) }
	cl := deploying(t, fakes{"node1": n}, func(nodes *orchestrate.Nodes) { nodes.LeaseTTL = 300 * time.Millisecond })
	_, _, err := deploy(t, cl)
	if !errors.Is(err, orchestrate.ErrLockLost) || !strings.Contains(err.Error(), "nothing changed") {
		t.Errorf("Deploy that lost the lock: %v, want ErrLockLost, saying nothing changed", err)
	}
	if got := n.containersRun(); len(got) != 0 {
		t.Errorf("the node ran %v once the lock was lost", got)
	}
}

// A deploy running in the operation's context, as mooctl runs it, that
// loses the lock during its pull says it lost the lock, not that it was
// stopped, and runs nothing.
func TestADeployThatLostTheLockDuringItsPullSaysSo(t *testing.T) {
	n := withRegistry(&node{}, nginxBuild)
	n.app.pullDelay = time.Minute
	n.app.onPull = func() { n.lockFor("bobs", time.Hour) }
	cl := deploying(t, fakes{"node1": n}, func(nodes *orchestrate.Nodes) { nodes.LeaseTTL = 300 * time.Millisecond })
	_, err := cl.Deploy(cl.Context(), orchestrate.App{Name: "web", Image: nginx}, func(orchestrate.DeployStep) {})
	var pe *orchestrate.PullError
	if !errors.As(err, &pe) || !errors.Is(err, orchestrate.ErrLockLost) {
		t.Errorf("Deploy that lost the lock during its pull: %v, want a *PullError of ErrLockLost", err)
	}
	if got := n.containersRun(); len(got) != 0 {
		t.Errorf("the node ran %v once the lock was lost", got)
	}
}

// A deploy stopped while it asks the nodes what they run says it changed
// nothing, rather than blaming a node.
func TestADeployStoppedBeforeItsPickSaysNothingChanged(t *testing.T) {
	n := withRegistry(&node{}, nginxBuild)
	cl := deploying(t, fakes{"node1": n}, nil)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := cl.Deploy(ctx, orchestrate.App{Name: "web", Image: nginx}, func(orchestrate.DeployStep) {})
	if err == nil || !errors.Is(err, context.Canceled) || errors.As(err, new(*orchestrate.NodeError)) || errors.As(err, new(*orchestrate.PullError)) {
		t.Errorf("Deploy stopped before its pick: %v, want it stopped, naming no node", err)
	}
	if got := n.pulls(); len(got) != 0 {
		t.Errorf("Deploy stopped before its pick pulled %v", got)
	}
}

// A deploy stopped once its pull ended runs nothing, and says nothing
// changed, rather than that a run may go on.
func TestADeployStoppedAfterItsPullRunsNothing(t *testing.T) {
	n := withRegistry(&node{}, nginxBuild)
	cl := deploying(t, fakes{"node1": n}, nil)
	ctx, cancel := context.WithCancel(t.Context())
	n.app.afterPull = cancel
	_, err := cl.Deploy(ctx, orchestrate.App{Name: "web", Image: nginx}, func(orchestrate.DeployStep) {})
	if err == nil || !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "nothing changed") || errors.As(err, new(*orchestrate.RunError)) {
		t.Errorf("Deploy stopped after its pull: %v, want it stopped before the run, nothing changed", err)
	}
	if got := n.containersRun(); len(got) != 0 {
		t.Errorf("the node ran %v after the deploy stopped", got)
	}
}

// An app that exits while the node pulls the build it runs isn't taken
// for unchanged: the deploy runs it again and checks it, as for any app
// that doesn't run as asked.
func TestARedeployOfAnAppThatExitsDuringThePullRunsItAgain(t *testing.T) {
	n := withRegistry(&node{others: []*nodev1.ContainerStatus{appContainer("web", nginx, nginxBuild)}}, nginxBuild)
	n.app.onPull = func() {
		n.mu.Lock()
		defer n.mu.Unlock()
		n.others[0] = &nodev1.ContainerStatus{Name: "app-web", Image: nginx, Digest: nginxBuild, State: nodev1.TaskState_TASK_STATE_EXITED, ExitCode: 1}
	}
	cl := deploying(t, fakes{"node1": n}, nil)
	d, _, err := deploy(t, cl)
	if err != nil || d.Unchanged {
		t.Errorf("Deploy of an app that exited during the pull: %+v, %v, want it run again", d, err)
	}
	if got := n.containersRun(); len(got) != 1 {
		t.Errorf("the node ran %v, want app-web run again", got)
	}
}

// Once the start check's time has passed, a container the node no longer
// keeps fails the deploy, and so does a node that doesn't say, as one the
// deploy couldn't check.
func TestADeployFailsForAnAppItCantSeeRunOnceStarted(t *testing.T) {
	gone := withRegistry(&node{}, nginxBuild)
	gone.app.afterRun = func() { gone.others = nil }
	_, _, err := deploy(t, deploying(t, fakes{"node1": gone}, nil))
	var nr *orchestrate.NotRunningError
	if !errors.As(err, &nr) || nr.Exited || nr.Problem != "the node no longer keeps its container" {
		t.Errorf("Deploy of an app whose container went: %v, want a *NotRunningError saying so", err)
	}
	silent := withRegistry(&node{}, nginxBuild)
	silent.app.afterRun = func() { silent.app.statusErr = errors.New("connection reset") }
	_, _, err = deploy(t, deploying(t, fakes{"node1": silent}, nil))
	var u *orchestrate.UncheckedError
	if !errors.As(err, &u) || !strings.Contains(err.Error(), "connection reset") {
		t.Errorf("Deploy on a node that stops answering once it ran the app: %v, want an *UncheckedError", err)
	}
}

// A redeploy of another name of the build an app runs, as a new tag of
// the same build, changes nothing: the image's digest decides, not its
// tag. The run renames the app's image on the node, which runs on, and
// the deploy doesn't wait to check it.
func TestARedeployOfAnotherTagOfTheSameBuildChangesNothing(t *testing.T) {
	n := withRegistry(&node{others: []*nodev1.ContainerStatus{appContainer("web", "docker.io/library/nginx:1.26", nginxBuild)}}, nginxBuild)
	cl := deploying(t, fakes{"node1": n}, func(nodes *orchestrate.Nodes) { nodes.StartCheck = time.Hour })
	d, _, err := deploy(t, cl)
	if err != nil || *d != (orchestrate.Deployed{Node: "node1", Image: nginx, Unchanged: true}) {
		t.Errorf("Deploy of another tag of the build web runs: %+v, %v, want it unchanged, under %s", d, err, nginx)
	}
	if got := n.containersRun(); len(got) != 1 || got[0].Image != nginx {
		t.Errorf("the node ran %v, want app-web run under %s, which renames its image", got, nginx)
	}
}

// A redeploy of another tag of the build an app runs whose run starts the
// app's process all the same, as when it exited between the deploy's look
// and the run, checks the app as any start does.
func TestARetagThatStartsTheAppChecksIt(t *testing.T) {
	n := withRegistry(&node{others: []*nodev1.ContainerStatus{appContainer("web", "docker.io/library/nginx:1.26", nginxBuild)}}, nginxBuild)
	n.app.restartsOnRename = true
	n.app.exits = map[string]uint32{nginx: 3}
	cl := deploying(t, fakes{"node1": n}, nil)
	_, _, err := deploy(t, cl)
	var nr *orchestrate.NotRunningError
	if !errors.As(err, &nr) || nr.ExitCode != 3 {
		t.Errorf("Deploy of another tag, whose run started an app that exits: %v, want it checked, exited with code 3", err)
	}
}

// A retag runs nothing once the lock was lost during the pull, as any run.
func TestARetagThatLostTheLockRunsNothing(t *testing.T) {
	n := withRegistry(&node{others: []*nodev1.ContainerStatus{appContainer("web", "docker.io/library/nginx:1.26", nginxBuild)}}, nginxBuild)
	n.app.pullDelay = 600 * time.Millisecond
	n.app.onPull = func() { n.lockFor("bobs", time.Hour) }
	cl := deploying(t, fakes{"node1": n}, func(nodes *orchestrate.Nodes) { nodes.LeaseTTL = 300 * time.Millisecond })
	if _, _, err := deploy(t, cl); !errors.Is(err, orchestrate.ErrLockLost) {
		t.Errorf("a retag that lost the lock: %v, want ErrLockLost", err)
	}
	if got := n.containersRun(); len(got) != 0 {
		t.Errorf("the node ran %v once the lock was lost", got)
	}
}

// without is cs without the container called name.
func without(cs []*nodev1.ContainerStatus, name string) []*nodev1.ContainerStatus {
	return slices.DeleteFunc(cs, func(k *nodev1.ContainerStatus) bool { return k.Name == name })
}

// A redeploy that can't read the node once the pull ended runs the app,
// and is unchanged if the run started no process, as of an app that runs
// as asked already: it says so, and doesn't wait to check it.
func TestARedeployThatCantReadTheNodeIsUnchangedIfTheRunStartedNothing(t *testing.T) {
	n := withRegistry(&node{others: []*nodev1.ContainerStatus{appContainer("web", nginx, nginxBuild)}}, nginxBuild)
	n.app.afterPull = func() {
		n.mu.Lock()
		n.app.statusErr = errors.New("connection reset")
		n.mu.Unlock()
	}
	cl := deploying(t, fakes{"node1": n}, func(nodes *orchestrate.Nodes) { nodes.StartCheck = time.Hour })
	d, _, err := deploy(t, cl)
	if err != nil || !d.Unchanged {
		t.Errorf("Deploy that couldn't read the node, of an app that runs as asked: %+v, %v, want it unchanged", d, err)
	}
	if got := n.containersRun(); len(got) != 1 {
		t.Errorf("the node ran %v, want app-web run once", got)
	}
}

// Two operators deploying two new apps at once, on two nodes with none,
// never both pass the preflight at once: a deploy that passed it always
// runs its app, the other is refused as the lock is held, and two that
// both deploy, one after the other, put their apps on different nodes.
// Both may be refused, each holding the lock on one node as the other
// asks.
func TestTwoDeploysOfNewAppsAtOnceNeverShareTheLockOrANode(t *testing.T) {
	const nginx2 = "docker.io/library/nginx:2"
	for round := range 100 {
		n1, n2 := withRegistry(&node{}, nginxBuild), withRegistry(&node{}, nginxBuild)
		n1.app.registry[nginx2], n2.app.registry[nginx2] = newBuild, newBuild
		f := fakes{"node1": n1, "node2": n2}
		apps := []orchestrate.App{{Name: "web", Image: nginx}, {Name: "api", Image: nginx2}}
		placed := make([]*orchestrate.Deployed, 2)
		var wg sync.WaitGroup
		for i, app := range apps {
			wg.Go(func() {
				nodes := f.nodes()
				nodes.StartCheck = time.Millisecond
				cl, err := orchestrate.Preflight(t.Context(), nodes, 1, false, orchestrate.Holder{Who: fmt.Sprint("op", i), Machine: "laptop"})
				if err != nil {
					if !errors.As(err, new(*orchestrate.LockHeldError)) {
						t.Errorf("round %d: the preflight of %s failed for another reason than the lock: %v", round, app.Name, err)
					}
					return
				}
				defer cl.Close()
				d, err := cl.Deploy(cl.Context(), app, func(orchestrate.DeployStep) {})
				if err != nil {
					t.Errorf("round %d: the deploy of %s failed past its preflight: %v", round, app.Name, err)
					return
				}
				placed[i] = d
			})
		}
		wg.Wait()
		if placed[0] != nil && placed[1] != nil && placed[0].Node == placed[1].Node {
			t.Errorf("round %d: web and api both went to %s", round, placed[0].Node)
		}
	}
}
