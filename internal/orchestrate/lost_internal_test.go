package orchestrate

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/nodeclient"
)

// A machine that sleeps stops the monotonic clock but not the wall
// clock, while the nodes' leases run out. Only an internal test can make
// the two disagree.
func TestALeaseIsLostOnceTheWallClockSaysSoEvenIfTheMonotonicClockDoesnt(t *testing.T) {
	var l leased
	l.granted(time.Now().Add(-2*time.Second), time.Minute)
	// Asleep for the lease time and more: the monotonic clock has moved
	// two seconds, the wall clock a minute and two seconds.
	l.expiresWall = l.expiresWall.Add(-time.Minute)
	c := &Cluster{conns: []*namedConn{{name: "node1", lease: l}}}
	err := c.lost(time.Now())
	if err == nil || !strings.Contains(err.Error(), "node1") {
		t.Errorf("lost says %v, want node1's lease run out", err)
	}
}

// Woken from sleep, the operation can't wait for keepLease's next check,
// whose timer runs on the monotonic clock: CheckLock looks at once.
func TestCheckLockCancelsAnOperationWhoseLeaseRanOutWhileTheMachineSlept(t *testing.T) {
	var l leased
	l.granted(time.Now().Add(-2*time.Second), time.Minute)
	l.expiresWall = l.expiresWall.Add(-time.Minute)
	c := &Cluster{conns: []*namedConn{{name: "node1", lease: l}}}
	c.ctx, c.cancel = context.WithCancelCause(context.Background())
	c.held, c.endHeld = context.WithCancelCause(context.Background())
	err := c.CheckLock()
	if !errors.Is(err, ErrLockLost) {
		t.Errorf("CheckLock says %v, want ErrLockLost", err)
	}
	if cause := context.Cause(c.ctx); !errors.Is(cause, ErrLockLost) {
		t.Errorf("the operation's context ended with %v, want ErrLockLost", cause)
	}
	if cause := context.Cause(c.Held()); !errors.Is(cause, ErrLockLost) {
		t.Errorf("the operation's held context ended with %v, want ErrLockLost", cause)
	}
}

// updateRecorder is a node that only counts the updates it is sent.
type updateRecorder struct {
	Node
	updates int
}

func (r *updateRecorder) UpdateAgent(context.Context, string, []byte, time.Duration) error {
	r.updates++
	return nil
}

func (r *updateRecorder) Status(context.Context) nodeclient.Status { return nodeclient.Status{} }

func (r *updateRecorder) UpdateUpdater(context.Context, string, []byte, time.Duration) error {
	r.updates++
	return nil
}

func (r *updateRecorder) UpdateContainerd(context.Context, string, map[string][]byte, time.Duration) error {
	r.updates++
	return nil
}

func (r *updateRecorder) RenewLock(context.Context, string, time.Duration) (*nodeclient.Lock, error) {
	return nil, errors.New("the lease ran out")
}

// Woken from sleep before keepLease noticed the leases ran out,
// Update must not send an update once the lease may have run out.
func TestUpdateSendsNoUpdateOnceTheLeaseRanOutWhileTheMachineSlept(t *testing.T) {
	var l leased
	l.granted(time.Now().Add(-2*time.Second), time.Minute)
	l.expiresWall = l.expiresWall.Add(-time.Minute)
	node := &updateRecorder{}
	c := &Cluster{conns: []*namedConn{{name: "node1", conn: node, lease: l}}, ttl: time.Minute}
	c.ctx, c.cancel = context.WithCancelCause(context.Background())
	c.held, c.endHeld = context.WithCancelCause(context.Background())
	releaseOf := func(string) (NodeRelease, error) {
		return NodeRelease{Components: map[string]Component{"agent": {Version: "2.0.0", Files: map[string][]byte{"agent": []byte("agent")}}}}, nil
	}
	err := c.Update(context.Background(), releaseOf, func(Step) {})
	if !errors.Is(err, ErrLockLost) {
		t.Errorf("Update says %v, want ErrLockLost", err)
	}
	if node.updates != 0 {
		t.Errorf("node1 was sent %d updates, want none", node.updates)
	}
}

// stepRecorder is a node that keeps the lease, and records the updates
// it is sent, by component. onUpdater, if set, runs as the updater is
// updated.
type stepRecorder struct {
	Node
	lease     string
	steps     []string
	onUpdater func()
}

func (r *stepRecorder) Status(context.Context) nodeclient.Status { return nodeclient.Status{} }

func (r *stepRecorder) UpdateAgent(context.Context, string, []byte, time.Duration) error {
	r.steps = append(r.steps, "agent")
	return nil
}

func (r *stepRecorder) UpdateUpdater(context.Context, string, []byte, time.Duration) error {
	r.steps = append(r.steps, "updater")
	r.onUpdater()
	return nil
}

func (r *stepRecorder) UpdateContainerd(context.Context, string, map[string][]byte, time.Duration) error {
	r.steps = append(r.steps, "containerd")
	return nil
}

func (r *stepRecorder) RenewLock(context.Context, string, time.Duration) (*nodeclient.Lock, error) {
	return &nodeclient.Lock{Lease: r.lease}, nil
}

// Asleep while the updater updates, Update must not go on to
// containerd once the lease may have run out.
func TestUpdateTakesNoNextStepOnceTheLeaseRanOutWhileTheMachineSlept(t *testing.T) {
	var l leased
	l.granted(time.Now(), time.Minute)
	node := &stepRecorder{lease: "ours"}
	n := &namedConn{name: "node1", conn: node, lease: l}
	c := &Cluster{conns: []*namedConn{n}, ttl: time.Minute, lease: nodeclient.Lock{Lease: "ours"}}
	c.ctx, c.cancel = context.WithCancelCause(context.Background())
	c.held, c.endHeld = context.WithCancelCause(context.Background())
	node.onUpdater = func() {
		c.mu.Lock()
		n.lease.expiresWall = time.Now().Add(-time.Second)
		c.mu.Unlock()
	}
	releaseOf := func(string) (NodeRelease, error) {
		return NodeRelease{Components: map[string]Component{
			"agent":   {Version: "2.0.0", Files: map[string][]byte{"agent": []byte("agent")}},
			"updater": {Version: "2.0.0", Files: map[string][]byte{"updater": []byte("updater")}},
		}}, nil
	}
	err := c.Update(context.Background(), releaseOf, func(Step) {})
	if !errors.Is(err, ErrLockLost) {
		t.Errorf("Update says %v, want ErrLockLost", err)
	}
	if want := []string{"agent", "updater"}; !slices.Equal(node.steps, want) {
		t.Errorf("node1 was sent the updates %v, want %v", node.steps, want)
	}
}

// lateAgent is a node whose agent update takes until just before the
// step's time is up, and whose new agent answers renewals only a while
// after it.
type lateAgent struct {
	Node
	took, restarting time.Duration
	updated          time.Time
}

func (r *lateAgent) Status(context.Context) nodeclient.Status { return nodeclient.Status{} }

func (r *lateAgent) UpdateAgent(context.Context, string, []byte, time.Duration) error {
	time.Sleep(r.took)
	r.updated = time.Now()
	return nil
}

func (r *lateAgent) UpdateUpdater(context.Context, string, []byte, time.Duration) error { return nil }

func (r *lateAgent) UpdateContainerd(context.Context, string, map[string][]byte, time.Duration) error {
	return nil
}

func (r *lateAgent) RenewLock(context.Context, string, time.Duration) (*nodeclient.Lock, error) {
	if time.Since(r.updated) < r.restarting {
		return nil, errors.New("the agent is restarting")
	}
	return &nodeclient.Lock{Lease: "ours"}, nil
}

// An agent update that ends late in its step leaves the renewal on the
// restarted agent its own time, not what is left of the step's, so that
// an update that succeeded isn't taken for one whose agent doesn't
// answer.
func TestTheRenewalAfterALateAgentUpdateHasItsOwnTime(t *testing.T) {
	defer SetUpdateTimeout(500 * time.Millisecond)()
	var l leased
	l.granted(time.Now(), time.Minute)
	node := &lateAgent{took: 450 * time.Millisecond, restarting: 300 * time.Millisecond}
	c := &Cluster{conns: []*namedConn{{name: "node1", conn: node, lease: l}}, ttl: time.Minute, lease: nodeclient.Lock{Lease: "ours"}}
	c.ctx, c.cancel = context.WithCancelCause(context.Background())
	c.held, c.endHeld = context.WithCancelCause(context.Background())
	releaseOf := func(string) (NodeRelease, error) {
		return NodeRelease{Components: map[string]Component{
			"agent":      {Version: "2.0.0", Files: map[string][]byte{"agent": []byte("agent")}},
			"updater":    {Version: "2.0.0", Files: map[string][]byte{"updater": []byte("updater")}},
			"containerd": {Version: "2.4.1"},
		}}, nil
	}
	if err := c.Update(context.Background(), releaseOf, func(Step) {}); err != nil {
		t.Errorf("Update says %v, want the late agent update to succeed", err)
	}
}

// serviceRecorder is a node that keeps the lease, runs what it is asked
// to at the version asked, and records the services it runs. onImport
// and onRun, if set, run as an image is imported and a service is run.
type serviceRecorder struct {
	stepRecorder
	containers      []*nodev1.ContainerStatus
	onImport, onRun func()
}

func (r *serviceRecorder) Status(context.Context) nodeclient.Status {
	return nodeclient.Status{Containers: r.containers}
}

func (r *serviceRecorder) ImportImage(_ context.Context, archive []byte, _ string) ([]string, error) {
	if r.onImport != nil {
		r.onImport()
	}
	return []string{string(archive)}, nil
}

func (r *serviceRecorder) Remove(_ context.Context, name, _ string) error {
	r.steps = append(r.steps, "remove "+name)
	r.containers = slices.DeleteFunc(r.containers, func(c *nodev1.ContainerStatus) bool { return c.Name == name })
	return nil
}

func (r *serviceRecorder) Run(_ context.Context, c *nodev1.Container, _ string) error {
	r.steps = append(r.steps, c.Name)
	r.containers = append(r.containers, &nodev1.ContainerStatus{Name: c.Name, Image: c.Image, Health: nodev1.Health_HEALTH_HEALTHY})
	if r.onRun != nil {
		r.onRun()
	}
	return nil
}

// Asleep while a service runs, Update must not go on to the next service
// once the lease may have run out.
func TestUpdateRunsNoNextServiceOnceTheLeaseRanOutWhileTheMachineSlept(t *testing.T) {
	var l leased
	l.granted(time.Now(), time.Minute)
	node := &serviceRecorder{stepRecorder: stepRecorder{lease: "ours"}}
	n := &namedConn{name: "node1", conn: node, lease: l}
	c := &Cluster{conns: []*namedConn{n}, ttl: time.Minute, lease: nodeclient.Lock{Lease: "ours"}, serviceTimeout: time.Second, reachTimeout: time.Second}
	c.ctx, c.cancel = context.WithCancelCause(context.Background())
	c.held, c.endHeld = context.WithCancelCause(context.Background())
	node.onRun = func() {
		c.mu.Lock()
		n.lease.expiresWall = time.Now().Add(-time.Second)
		c.mu.Unlock()
	}
	releaseOf := func(string) (NodeRelease, error) {
		return NodeRelease{Services: []Service{
			{Name: "dns", Version: "2.0.0", Container: &nodev1.Container{Name: "service-dns", Image: "moostack.local/dns:2.0.0"}, Archive: []byte("moostack.local/dns:2.0.0")},
			{Name: "proxy", Version: "2.0.0", Container: &nodev1.Container{Name: "service-proxy", Image: "moostack.local/proxy:2.0.0"}, Archive: []byte("moostack.local/proxy:2.0.0")},
		}}, nil
	}
	err := c.updateServices(context.Background(), n, must(releaseOf("node1")), func(Step) {})
	if !errors.Is(err, ErrLockLost) {
		t.Errorf("updateServices says %v, want ErrLockLost", err)
	}
	if want := []string{"service-dns"}; !slices.Equal(node.steps, want) {
		t.Errorf("node1 ran the services %v, want %v", node.steps, want)
	}
}

// Asleep while a service's image imports, Update must not run the
// service once the lease may have run out, since another operation may
// hold the lock by then.
func TestUpdateRunsNoServiceOnceTheLeaseRanOutWhileItsImageImported(t *testing.T) {
	var l leased
	l.granted(time.Now(), time.Minute)
	node := &serviceRecorder{stepRecorder: stepRecorder{lease: "ours"}}
	n := &namedConn{name: "node1", conn: node, lease: l}
	c := &Cluster{conns: []*namedConn{n}, ttl: time.Minute, lease: nodeclient.Lock{Lease: "ours"}, serviceTimeout: time.Second, reachTimeout: time.Second}
	c.ctx, c.cancel = context.WithCancelCause(context.Background())
	c.held, c.endHeld = context.WithCancelCause(context.Background())
	node.onImport = func() {
		c.mu.Lock()
		n.lease.expiresWall = time.Now().Add(-time.Second)
		c.mu.Unlock()
	}
	releaseOf := func(string) (NodeRelease, error) {
		return NodeRelease{Services: []Service{{Name: "dns", Version: "2.0.0", Container: &nodev1.Container{Name: "service-dns", Image: "moostack.local/dns:2.0.0"}, Archive: []byte("moostack.local/dns:2.0.0")}}}, nil
	}
	err := c.updateServices(context.Background(), n, must(releaseOf("node1")), func(Step) {})
	if !errors.Is(err, ErrLockLost) {
		t.Errorf("updateServices says %v, want ErrLockLost", err)
	}
	if len(node.steps) > 0 {
		t.Errorf("node1 ran the services %v, want none", node.steps)
	}
}

// stoppedAgent is a node whose agent update ends at once, and whose
// restarted agent never answers a renewal, until mooctl stops waiting.
type stoppedAgent struct {
	Node
	updated chan struct{}
}

func (r *stoppedAgent) Status(context.Context) nodeclient.Status { return nodeclient.Status{} }

func (r *stoppedAgent) UpdateAgent(context.Context, string, []byte, time.Duration) error {
	close(r.updated)
	return nil
}

func (r *stoppedAgent) RenewLock(ctx context.Context, _ string, _ time.Duration) (*nodeclient.Lock, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, errors.New("connection refused")
}

// agentOnly is a release of the agent alone, which Update sends first.
func agentOnly(string) (NodeRelease, error) {
	return NodeRelease{Components: map[string]Component{"agent": {Version: "2.0.0", Files: map[string][]byte{"agent": []byte("agent")}}}}, nil
}

// The renewal on the restarted agent ends with the operation: an operator
// who stops mooctl then is told mooctl stopped waiting, not that the new
// agent doesn't answer, and a lost lock is said as the lost lock it is.
func TestTheRenewalAfterAnAgentUpdateEndsWithTheOperation(t *testing.T) {
	for _, c := range []struct {
		name  string
		cause error
		want  func(error) bool
	}{
		{"stopped", context.Canceled, func(err error) bool {
			return strings.Contains(err.Error(), "mooctl stopped waiting") && !strings.Contains(err.Error(), "doesn't answer")
		}},
		{"lock lost", errors.Join(ErrLockLost, errors.New("node2: its lock was broken")), func(err error) bool { return errors.Is(err, ErrLockLost) }},
	} {
		t.Run(c.name, func(t *testing.T) {
			var l leased
			l.granted(time.Now(), time.Hour)
			node := &stoppedAgent{updated: make(chan struct{})}
			cl := &Cluster{conns: []*namedConn{{name: "node1", conn: node, lease: l}}, ttl: time.Minute, lease: nodeclient.Lock{Lease: "ours"}}
			cl.ctx, cl.cancel = context.WithCancelCause(context.Background())
			cl.held, cl.endHeld = context.WithCancelCause(context.Background())
			go func() {
				<-node.updated
				time.Sleep(100 * time.Millisecond)
				cl.cancel(c.cause)
			}()
			start := time.Now()
			err := cl.Update(cl.ctx, agentOnly, func(Step) {})
			if took := time.Since(start); took > 2*time.Second {
				t.Errorf("the renewal went on %s after the operation ended", took)
			}
			if err == nil || !c.want(err) {
				t.Errorf("Update says %v", err)
			}
		})
	}
}

// must is rel, which err must not have failed.
func must(rel NodeRelease, err error) NodeRelease {
	if err != nil {
		panic(err)
	}
	return rel
}

// Asleep while a service's image imports, Update must not remove the
// container the service replaces once the lease may have run out.
func TestUpdateRemovesNoOldContainerOnceTheLeaseRanOutWhileItsImageImported(t *testing.T) {
	var l leased
	l.granted(time.Now(), time.Minute)
	node := &serviceRecorder{stepRecorder: stepRecorder{lease: "ours"}, containers: []*nodev1.ContainerStatus{{Name: "dns", Image: "moostack.local/dns:1.0.0"}}}
	n := &namedConn{name: "node1", conn: node, lease: l}
	c := &Cluster{conns: []*namedConn{n}, ttl: time.Minute, lease: nodeclient.Lock{Lease: "ours"}, serviceTimeout: time.Second, reachTimeout: time.Second}
	c.ctx, c.cancel = context.WithCancelCause(context.Background())
	c.held, c.endHeld = context.WithCancelCause(context.Background())
	node.onImport = func() {
		c.mu.Lock()
		n.lease.expiresWall = time.Now().Add(-time.Second)
		c.mu.Unlock()
	}
	rel := NodeRelease{Services: []Service{{Name: "dns", Version: "2.0.0", Container: &nodev1.Container{Name: "service-dns", Image: "moostack.local/dns:2.0.0"}, Archive: []byte("moostack.local/dns:2.0.0"), Replaces: "dns", ReplacesImages: "moostack.local/dns:"}}}
	err := c.updateServices(context.Background(), n, rel, func(Step) {})
	if !errors.Is(err, ErrLockLost) {
		t.Errorf("updateServices says %v, want ErrLockLost", err)
	}
	if len(node.steps) > 0 {
		t.Errorf("node1 had %v, want nothing", node.steps)
	}
}
