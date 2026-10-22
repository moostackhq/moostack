package orchestrate_test

import (
	"cmp"
	"context"
	"errors"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/nodeclient"
	"github.com/moostackhq/moostack/internal/orchestrate"
)

// node is a fake node in direct mode.
type node struct {
	generation int64
	// nodes are the nodes of the cluster directory at generation.
	nodes []string
	// problem, if set, says why the node can't tell its generation.
	problem string
	// recordErr, if set, is why recording a generation fails.
	recordErr error
	// hangs says the node never answers once reached: a status call, a
	// recording or a lock call waits until its context ends.
	hangs bool
	// answerLost says the node grants the lock, but its answer never
	// reaches the operation: AcquireLock waits until its context ends.
	answerLost bool
	// answerDelay is how long the node's answer to AcquireLock takes to
	// reach the operation, after the node granted the lock.
	answerDelay time.Duration
	// removeErr, if set, is why removing identities fails.
	removeErr error
	// removeDelay is how long the node's answer to RemoveIdentities takes
	// to reach the operation, after the node took them.
	removeDelay time.Duration
	// stolenOnRemove, if set, is the lease of another operation that takes
	// the node's lock while the node answers RemoveIdentities, as when an
	// operator breaks the lock and another operation takes it.
	stolenOnRemove string
	// laterGeneration, if set, is the generation the node reports from
	// its second status call on, as if another operation recorded it in
	// between.
	laterGeneration int64

	// updateErr, if set, is why updating the component failing fails,
	// the agent if failing is empty, and updateDelay how long an update
	// of any component takes.
	updateErr   error
	failing     string
	updateDelay time.Duration
	// onUpdate, if set, is called as an update of the agent starts, and
	// onUpdateOf as an update of the component it names starts.
	onUpdate   func()
	onUpdateOf map[string]func()
	// restartFor is how long the node's agent doesn't answer once an
	// update ends, as it restarts.
	restartFor time.Duration
	// rollsBack says the update uses its whole health timeout and as
	// long again rolling back, and then fails.
	rollsBack bool
	// components are the node components the node reports, and services
	// the cluster services. An update of a service runs it at its version,
	// healthy unless it is sickService.
	components  []*nodev1.Component
	services    []*nodev1.Component
	sickService string

	mu sync.Mutex
	// updates are the updates of the node's components, in order.
	updates []componentUpdate
	// downUntil is when the agent answers again after a restart.
	downUntil time.Time
	// lock is the lease that holds the node's operation lock, nil if
	// none, until expires.
	lock    *nodeclient.Lock
	expires time.Time
	// acquired counts the calls to AcquireLock.
	acquired int
	// statusCalls counts the calls to Status.
	statusCalls int
	// hanging, once set, makes the node stop answering, as hangs does.
	hanging  bool
	recorded []int64
	// recordedNodes are the nodes given with the last recording.
	recordedNodes []string
	// removed are the identities the node holds as removed, and pushes
	// counts the pushes of them.
	removed []string
	pushes  int
	closed  bool
}

func (n *node) RemoveIdentities(ctx context.Context, identities []string, lease string) error {
	if n.hang(ctx) {
		return ctx.Err()
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.removeErr != nil {
		return n.removeErr
	}
	if l := n.held(); l == nil || l.Lease != lease {
		return errors.New("the operation's lease doesn't hold the node's operation lock")
	}
	n.pushes++
	for _, id := range identities {
		if !slices.Contains(n.removed, id) {
			n.removed = append(n.removed, id)
		}
	}
	if n.stolenOnRemove != "" {
		n.lock = &nodeclient.Lock{Lease: n.stolenOnRemove, Holder: "bob", Machine: "desktop"}
		n.expires = time.Now().Add(time.Hour)
	}
	if n.removeDelay > 0 {
		n.mu.Unlock()
		time.Sleep(n.removeDelay)
		n.mu.Lock()
	}
	return nil
}

// removedIdentities are the identities the node holds as removed.
func (n *node) removedIdentities() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return slices.Clone(n.removed)
}

func (n *node) Status(ctx context.Context) nodeclient.Status {
	if n.hangs {
		<-ctx.Done()
		return nodeclient.Status{AgentErr: ctx.Err(), UpdaterErr: ctx.Err()}
	}
	n.mu.Lock()
	n.statusCalls++
	generation := n.generation
	if n.laterGeneration != 0 && n.statusCalls > 1 {
		generation = n.laterGeneration
	}
	n.mu.Unlock()
	return nodeclient.Status{Mode: nodev1.Mode_MODE_DIRECT, Generation: generation, Nodes: n.nodes, GenerationUnknown: n.problem, Lock: n.heldLock(), Components: n.components, Services: n.reportedServices()}
}

func (n *node) RecordGeneration(ctx context.Context, g int64, nodes []string, lease string) error {
	if n.hang(ctx) {
		return ctx.Err()
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.recordErr != nil {
		return n.recordErr
	}
	if l := n.held(); l == nil || l.Lease != lease {
		return errors.New("the operation's lease doesn't hold the node's operation lock")
	}
	n.recorded = append(n.recorded, g)
	n.recordedNodes = nodes
	return nil
}

// hang waits until ctx ends and returns true if the node doesn't answer,
// or if ctx has ended already, as a call on an ended context fails at
// once.
func (n *node) hang(ctx context.Context) bool {
	if ctx.Err() != nil {
		return true
	}
	n.mu.Lock()
	hangs := n.hangs || n.hanging
	n.mu.Unlock()
	if hangs {
		<-ctx.Done()
	}
	return hangs
}

// held is the lease that holds the lock now, nil if none. n.mu must be
// held.
func (n *node) held() *nodeclient.Lock {
	if n.lock != nil && !time.Now().Before(n.expires) {
		n.lock = nil
	}
	return n.lock
}

// heldLock is the lease that holds the lock now, nil if none.
func (n *node) heldLock() *nodeclient.Lock {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.held()
}

// lockFor has another operation, of lease, hold the lock for ttl.
func (n *node) lockFor(lease string, ttl time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.lock = &nodeclient.Lock{Lease: lease, Holder: "bob", Machine: "desktop", Since: time.Date(2026, 10, 19, 9, 30, 0, 0, time.UTC)}
	n.expires = time.Now().Add(ttl)
}

func (n *node) AcquireLock(ctx context.Context, lock nodeclient.Lock, ttl time.Duration) (nodeclient.Lock, error) {
	if n.hang(ctx) {
		return nodeclient.Lock{}, ctx.Err()
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.acquired++
	if l := n.held(); l != nil && l.Lease != lock.Lease {
		return *l, nil
	}
	if n.lock == nil {
		lock.Since = time.Now()
		n.lock = &lock
	}
	n.expires = time.Now().Add(ttl)
	if n.answerLost {
		n.mu.Unlock()
		<-ctx.Done()
		n.mu.Lock()
		return nodeclient.Lock{}, ctx.Err()
	}
	if n.answerDelay > 0 {
		n.mu.Unlock()
		time.Sleep(n.answerDelay)
		n.mu.Lock()
	}
	return *n.lock, nil
}

func (n *node) RenewLock(ctx context.Context, lease string, ttl time.Duration) (*nodeclient.Lock, error) {
	if n.hang(ctx) {
		return nil, ctx.Err()
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if time.Now().Before(n.downUntil) {
		return nil, errors.New("the agent is restarting")
	}
	l := n.held()
	if l != nil && l.Lease == lease {
		n.expires = time.Now().Add(ttl)
	}
	return l, nil
}

// leaseExpires is when the node's lease runs out unless renewed.
func (n *node) leaseExpires() time.Time {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.expires
}

func (n *node) ReleaseLock(ctx context.Context, lease string) error {
	if n.hang(ctx) {
		return ctx.Err()
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if l := n.held(); l != nil && l.Lease == lease {
		n.lock = nil
	}
	return nil
}

func (n *node) BreakLock(ctx context.Context) (*nodeclient.Lock, error) {
	if n.hang(ctx) {
		return nil, ctx.Err()
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	l := n.held()
	n.lock = nil
	return l, nil
}

// componentUpdate is an update of a fake node's component: agent,
// updater or containerd.
type componentUpdate struct {
	component, version, binary string
	healthTimeout              time.Duration
	started, ended             time.Time
}

func (n *node) UpdateAgent(ctx context.Context, version string, binary []byte, healthTimeout time.Duration) error {
	if n.onUpdate != nil {
		n.onUpdate()
	}
	return n.update(ctx, "agent", version, string(binary), healthTimeout)
}

func (n *node) UpdateUpdater(ctx context.Context, version string, binary []byte, healthTimeout time.Duration) error {
	return n.update(ctx, "updater", version, string(binary), healthTimeout)
}

func (n *node) UpdateContainerd(ctx context.Context, version string, files map[string][]byte, healthTimeout time.Duration) error {
	var binaries []string
	for _, name := range slices.Sorted(maps.Keys(files)) {
		binaries = append(binaries, name+": "+string(files[name]))
	}
	return n.update(ctx, "containerd", version, strings.Join(binaries, ", "), healthTimeout)
}

// ImportImage imports the image whose archive is "image " and its name,
// failing it if failing names the image's service.
func (n *node) ImportImage(ctx context.Context, archive []byte) ([]string, error) {
	image := strings.TrimPrefix(string(archive), "image ")
	if service, _, _ := strings.Cut(strings.TrimPrefix(image, "moostack.local/"), ":"); n.failing == "import "+service {
		return nil, n.updateErr
	}
	return []string{image}, nil
}

// RunService records an update of the service, as update records one of
// a component, as "service <name>", with its image as the binary.
func (n *node) RunService(ctx context.Context, svc *nodev1.Service) error {
	err := n.update(ctx, "service "+svc.Name, svc.Version, svc.Image, 0)
	if err != nil {
		return err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	health := nodev1.Health_HEALTH_HEALTHY
	if svc.Name == n.sickService {
		health = nodev1.Health_HEALTH_UNHEALTHY
	}
	n.services = slices.DeleteFunc(n.services, func(c *nodev1.Component) bool { return c.Name == svc.Name })
	n.services = append(n.services, &nodev1.Component{Name: svc.Name, Version: svc.Version, Health: health})
	return nil
}

// reportedServices are the services the node reports.
func (n *node) reportedServices() []*nodev1.Component {
	n.mu.Lock()
	defer n.mu.Unlock()
	return slices.Clone(n.services)
}

// update records an update of component, after updateDelay, and fails
// it with updateErr if it is the one failing. An update of the agent
// restarts it, for restartFor.
func (n *node) update(ctx context.Context, component, version, binary string, healthTimeout time.Duration) error {
	started := time.Now()
	if f := n.onUpdateOf[component]; f != nil {
		f()
	}
	delay, err := n.updateDelay, n.updateErr
	if failing := cmp.Or(n.failing, "agent"); failing != component {
		err = nil
	}
	if n.rollsBack && component == "agent" {
		delay, err = 2*healthTimeout, errors.New("rolled back to the agent it replaced")
	}
	select {
	case <-time.After(delay):
	case <-ctx.Done():
		return ctx.Err()
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.updates = append(n.updates, componentUpdate{component, version, binary, healthTimeout, started, time.Now()})
	if component == "agent" {
		n.downUntil = time.Now().Add(n.restartFor)
	}
	return err
}

// componentUpdates are the updates of the node's components so far, of
// component only unless that is empty.
func (n *node) componentUpdates(component string) []componentUpdate {
	n.mu.Lock()
	defer n.mu.Unlock()
	var updates []componentUpdate
	for _, u := range n.updates {
		if component == "" || u.component == component {
			updates = append(updates, u)
		}
	}
	return updates
}

// agentUpdates are the updates of the node's agent so far.
func (n *node) agentUpdates() []componentUpdate { return n.componentUpdates("agent") }

func (n *node) Close() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.closed = true
	return nil
}

func (n *node) isClosed() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.closed
}

// fakes is a cluster of fake nodes, by name.
type fakes map[string]*node

// names are the names of the cluster's nodes.
func (f fakes) names() []string {
	var names []string
	for name := range f {
		names = append(names, name)
	}
	return names
}

// nodes are the cluster's nodes, with extra names, as orchestrate takes
// them.
func (f fakes) nodes(extra ...string) orchestrate.Nodes {
	return orchestrate.Nodes{Names: append(f.names(), extra...), Dial: f.dial}
}

func (f fakes) dial(_ context.Context, name string) (orchestrate.Node, error) {
	if fake, ok := f[name]; ok {
		return fake, nil
	}
	return nil, errors.New("no route to host")
}

func TestTheNextGenerationIsLaterThanTheDirectorysAndEveryNodes(t *testing.T) {
	for _, c := range []struct {
		name         string
		dir          int64
		applied      []int64
		force        bool
		wantNext     int64
		wantRefusing bool
	}{
		{name: "every node applied the directory", dir: 4, applied: []int64{4, 4}, wantNext: 5},
		{name: "a node applied an older one", dir: 4, applied: []int64{2, 4}, wantNext: 5},
		{name: "a new cluster", dir: 1, wantNext: 2},
		{name: "a node applied a later one", dir: 4, applied: []int64{4, 6}, wantRefusing: true},
		{name: "a later one forced", dir: 4, applied: []int64{4, 6}, force: true, wantNext: 7},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := fakes{}
			for i, g := range c.applied {
				f[string(rune('a'+i))] = &node{generation: g}
			}
			cl, err := orchestrate.Preflight(t.Context(), f.nodes(), c.dir, c.force, holder)
			if c.wantRefusing {
				if err == nil {
					t.Fatal("the preflight passed")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer cl.Close()
			if got := cl.Next(c.dir); got != c.wantNext {
				t.Errorf("Next(%d) is %d, want %d", c.dir, got, c.wantNext)
			}
		})
	}
}

func TestRecordRecordsOnEveryNodeTheAddedOneToo(t *testing.T) {
	f := fakes{"node1": {generation: 1}, "node2": {generation: 1}}
	cl, err := orchestrate.Preflight(t.Context(), f.nodes(), 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	added := &node{}
	cl.Add("node3", added)
	if err := cl.Record(t.Context(), 2); err != nil {
		t.Fatal(err)
	}
	for name, n := range map[string]*node{"node1": f["node1"], "node2": f["node2"], "node3": added} {
		if len(n.recorded) != 1 || n.recorded[0] != 2 {
			t.Errorf("%s recorded %v, want [2]", name, n.recorded)
		}
	}
	cl.Close()
	for name, n := range map[string]*node{"node1": f["node1"], "node2": f["node2"], "node3": added} {
		if !n.isClosed() {
			t.Errorf("Close left the connection to %s open", name)
		}
	}
}

func TestRecordNamesEveryNodeThatDidntRecord(t *testing.T) {
	f := fakes{
		"node1": {generation: 1, recordErr: errors.New("it is full")},
		"node2": {generation: 1},
		"node3": {generation: 1, recordErr: errors.New("it is stuck")},
	}
	cl, err := orchestrate.Preflight(t.Context(), f.nodes(), 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	err = cl.Record(t.Context(), 2)
	if err == nil {
		t.Fatal("Record succeeded")
	}
	for _, want := range []string{"node1 didn't record generation 2: it is full", "node3 didn't record generation 2: it is stuck"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("Record's error is %v, want it to contain %q", err, want)
		}
	}
	if strings.Contains(err.Error(), "node2") {
		t.Errorf("Record's error names node2, which recorded: %v", err)
	}
	if len(f["node2"].recorded) != 1 {
		t.Errorf("node2 recorded %v, want [2]", f["node2"].recorded)
	}
}

func TestAFailedPreflightClosesTheConnectionsItMade(t *testing.T) {
	f := fakes{"node1": {generation: 1}, "node2": {generation: 9}}
	if _, err := orchestrate.Preflight(t.Context(), f.nodes("gone"), 1, false, holder); err == nil {
		t.Fatal("the preflight passed")
	}
	for name, n := range f {
		if !n.isClosed() {
			t.Errorf("the failed preflight left the connection to %s open", name)
		}
	}
}

func TestSurveyClosesTheConnectionsItMade(t *testing.T) {
	f := fakes{"node1": {generation: 1}, "node2": {generation: 9}}
	reached := orchestrate.Survey(t.Context(), f.nodes("gone"))
	if len(reached) != 3 {
		t.Fatalf("Survey reported %d nodes, want 3", len(reached))
	}
	for _, r := range reached {
		if (r.Err == nil) != (r.Name != "gone") {
			t.Errorf("Survey reports %s with the error %v", r.Name, r.Err)
		}
	}
	for name, n := range f {
		if !n.isClosed() {
			t.Errorf("Survey left the connection to %s open", name)
		}
	}
}

func TestRecordGivesUpOnANodeThatDoesntAnswer(t *testing.T) {
	t.Parallel()
	f := fakes{"node1": {generation: 1}, "node2": {generation: 1}}
	nodes := f.nodes()
	nodes.RecordTimeout = 100 * time.Millisecond
	nodes.ReachTimeout = time.Second
	cl, err := orchestrate.Preflight(t.Context(), nodes, 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	// node2 stops answering after the preflight.
	f["node2"].mu.Lock()
	f["node2"].hanging = true
	f["node2"].mu.Unlock()
	done := make(chan error, 1)
	go func() { done <- cl.Record(context.Background(), 2) }()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "node2 didn't record generation 2") || !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Record's error is %v, want node2 timing out", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Record still waits for node2")
	}
	if len(f["node1"].recorded) != 1 {
		t.Errorf("node1 recorded %v, want [2]", f["node1"].recorded)
	}
}

func TestThePreflightGivesUpOnANodeThatDoesntAnswer(t *testing.T) {
	t.Parallel()
	f := fakes{"node1": {generation: 1}, "node2": {generation: 1, hangs: true}}
	nodes := f.nodes()
	nodes.ReachTimeout = 100 * time.Millisecond
	done := make(chan error, 1)
	go func() {
		_, err := orchestrate.Preflight(context.Background(), nodes, 1, false, holder)
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "node2: the agent doesn't answer") || !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("the preflight's error is %v, want node2 timing out", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the preflight still waits for node2")
	}
}

func TestRecordRecordsTheNodesOfTheClusterDirectory(t *testing.T) {
	f := fakes{"node2": {generation: 1}, "node1": {generation: 1}}
	cl, err := orchestrate.Preflight(t.Context(), f.nodes(), 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	added := &node{}
	cl.Add("node3", added)
	if err := cl.Record(t.Context(), 2); err != nil {
		t.Fatal(err)
	}
	want := []string{"node1", "node2", "node3"}
	for name, n := range map[string]*node{"node1": f["node1"], "node2": f["node2"], "node3": added} {
		if !slices.Equal(n.recordedNodes, want) {
			t.Errorf("%s recorded the nodes %v, want %v", name, n.recordedNodes, want)
		}
	}
}

func TestThePreflightFindsTheNodesTheLatestGenerationListsButTheDirectoryDoesnt(t *testing.T) {
	f := fakes{
		// The directory lists node1 and node2. node1 has applied
		// generation 3, of node1, node2, node3 and node4; node2 an older
		// one, of node2 and node5.
		"node1": {generation: 3, nodes: []string{"node1", "node2", "node3", "node4"}},
		"node2": {generation: 2, nodes: []string{"node2", "node5"}},
	}
	cl, err := orchestrate.Preflight(t.Context(), f.nodes(), 2, true, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if want := []string{"node3", "node4"}; !slices.Equal(cl.Unlisted, want) {
		t.Errorf("the unlisted nodes are %v, want %v", cl.Unlisted, want)
	}
	if cl.Latest != 3 {
		t.Errorf("the latest generation is %d, want 3", cl.Latest)
	}
}

func TestThePreflightListsNoNodesWhenTheDirectoryListsThemAll(t *testing.T) {
	f := fakes{"node1": {generation: 2, nodes: []string{"node1", "node2"}}, "node2": {generation: 2, nodes: []string{"node1", "node2"}}}
	cl, err := orchestrate.Preflight(t.Context(), f.nodes(), 2, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if len(cl.Unlisted) != 0 || len(cl.Forced) != 0 {
		t.Errorf("the unlisted nodes are %v and the forced problems %v, want none", cl.Unlisted, cl.Forced)
	}
}

func TestThePreflightRefusesANodeThatCantTellItsGenerationUnlessForced(t *testing.T) {
	f := fakes{"node1": {generation: 1}, "node2": {problem: "reading /var/lib/moostack/generation: invalid character"}}
	_, err := orchestrate.Preflight(t.Context(), f.nodes(), 1, false, holder)
	if err == nil || !strings.Contains(err.Error(), "node2: it can't tell the generation it has applied: reading /var/lib/moostack/generation") {
		t.Errorf("the preflight's error is %v, want node2 refused for not telling its generation", err)
	}
	cl, err := orchestrate.Preflight(t.Context(), f.nodes(), 1, true, holder)
	if err != nil {
		t.Fatalf("the forced preflight failed: %v", err)
	}
	defer cl.Close()
	if len(cl.Forced) != 1 || !strings.Contains(cl.Forced[0].Error(), "node2: it can't tell the generation") {
		t.Errorf("the forced problems are %v, want node2's", cl.Forced)
	}
}

func TestAnUnforcedPreflightLooksForNoUnlistedNodes(t *testing.T) {
	// node1 lists node3, which the directory doesn't, at the directory's
	// generation, as after a removal it didn't hear of.
	f := fakes{"node1": {generation: 2, nodes: []string{"node1", "node2", "node3"}}, "node2": {generation: 2, nodes: []string{"node1", "node2"}}}
	cl, err := orchestrate.Preflight(t.Context(), f.nodes(), 2, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if len(cl.Unlisted) != 0 {
		t.Errorf("an unforced preflight found the unlisted nodes %v", cl.Unlisted)
	}
}

func TestANodeThatWasntReachedHasNoKnownGeneration(t *testing.T) {
	f := fakes{"node1": {generation: 3}}
	for _, r := range orchestrate.Survey(t.Context(), f.nodes("gone")) {
		got, ok := r.KnownGeneration()
		if want := r.Name == "node1"; ok != want || (ok && got != 3) {
			t.Errorf("%s's known generation is %d, %t", r.Name, got, ok)
		}
	}
}

func TestAPreflightErrorSaysWhetherForceWouldPassIt(t *testing.T) {
	for _, c := range []struct {
		name string
		f    fakes
		// unreachable are nodes the directory lists that can't be reached.
		unreachable []string
		forceable   bool
	}{
		{name: "a later generation", f: fakes{"node1": {generation: 5}}, forceable: true},
		{name: "a node that can't tell its generation", f: fakes{"node1": {problem: "unreadable"}}, forceable: true},
		{name: "a later generation and an unreachable node", f: fakes{"node1": {generation: 5}}, unreachable: []string{"gone"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err := orchestrate.Preflight(t.Context(), c.f.nodes(c.unreachable...), 1, false, holder)
			var perr *orchestrate.PreflightError
			if !errors.As(err, &perr) {
				t.Fatalf("the preflight's error is %v, want a PreflightError", err)
			}
			if perr.Forceable() != c.forceable {
				t.Errorf("Forceable is %t, want %t, for %v", perr.Forceable(), c.forceable, err)
			}
		})
	}
}

func TestPreflightRefusesNodesItsCheckFailsBeforeTakingTheLock(t *testing.T) {
	f := fakes{"node1": {generation: 1}, "node2": {generation: 1, nodes: []string{"bad"}}}
	nodes := inOrder(f, "node1", "node2")
	nodes.Check = func(s nodeclient.Status, _ []nodeclient.Status) error {
		if slices.Contains(s.Nodes, "bad") {
			return errors.New("it is bad")
		}
		return nil
	}
	_, err := orchestrate.Preflight(t.Context(), nodes, 1, false, holder)
	var pe *orchestrate.PreflightError
	if !errors.As(err, &pe) || len(pe.Problems) != 1 || !strings.Contains(pe.Problems[0].Error(), "node2: it is bad") {
		t.Fatalf("Preflight: %v, want node2 refused as bad", err)
	}
	for name, n := range f {
		if n.acquired != 0 {
			t.Errorf("%s was asked for its lock %d times, want never", name, n.acquired)
		}
	}
	// --force doesn't pass it.
	if _, err := orchestrate.Preflight(t.Context(), nodes, 1, true, holder); !errors.As(err, &pe) {
		t.Errorf("Preflight with force: %v, want node2 refused", err)
	}
}

// Another operation may have changed what a node runs between the check
// and the lock, so the check runs again on what the nodes say once the
// lock is held.
func TestPreflightChecksTheNodesAgainWhileHoldingTheLock(t *testing.T) {
	f := fakes{"node1": {generation: 1}, "node2": {generation: 1}}
	nodes := inOrder(f, "node1", "node2")
	var mu sync.Mutex
	checks := 0
	nodes.Check = func(nodeclient.Status, []nodeclient.Status) error {
		mu.Lock()
		defer mu.Unlock()
		checks++
		// The two nodes pass before the lock, and fail after.
		if checks > 2 {
			return errors.New("it runs another release now")
		}
		return nil
	}
	_, err := orchestrate.Preflight(t.Context(), nodes, 1, false, holder)
	var pe *orchestrate.PreflightError
	if !errors.As(err, &pe) || !strings.Contains(err.Error(), "it runs another release now") {
		t.Fatalf("Preflight: %v, want the nodes refused on the second check", err)
	}
	for name, n := range f {
		if n.acquired == 0 {
			t.Errorf("%s was never asked for its lock, want the second check to come after it", name)
		}
		if l := n.heldLock(); l != nil {
			t.Errorf("%s holds the lock %+v after the refusal", name, l)
		}
	}
}

// A node's check may depend on the others, as going back to a release
// some node still runs does.
func TestPreflightChecksEachNodeWithWhatEveryNodeSays(t *testing.T) {
	f := fakes{"node1": {generation: 1, nodes: []string{"a"}}, "node2": {generation: 1, nodes: []string{"b"}}}
	nodes := inOrder(f, "node1", "node2")
	var mu sync.Mutex
	var seen [][]string
	nodes.Check = func(_ nodeclient.Status, all []nodeclient.Status) error {
		mu.Lock()
		defer mu.Unlock()
		var said []string
		for _, s := range all {
			said = append(said, s.Nodes...)
		}
		slices.Sort(said)
		seen = append(seen, said)
		return nil
	}
	cl, err := orchestrate.Preflight(t.Context(), nodes, 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if len(seen) == 0 {
		t.Fatal("the check never ran")
	}
	for _, said := range seen {
		if !slices.Equal(said, []string{"a", "b"}) {
			t.Errorf("a node's check saw the statuses %v, want every node's", said)
		}
	}
}
