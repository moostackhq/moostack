package orchestrate

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

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
	err := c.CheckLock()
	if !errors.Is(err, ErrLockLost) {
		t.Errorf("CheckLock says %v, want ErrLockLost", err)
	}
	if cause := context.Cause(c.ctx); !errors.Is(cause, ErrLockLost) {
		t.Errorf("the operation's context ended with %v, want ErrLockLost", cause)
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
// UpdateNodes must not send an update once the lease may have run out.
func TestUpdateNodesSendsNoUpdateOnceTheLeaseRanOutWhileTheMachineSlept(t *testing.T) {
	var l leased
	l.granted(time.Now().Add(-2*time.Second), time.Minute)
	l.expiresWall = l.expiresWall.Add(-time.Minute)
	node := &updateRecorder{}
	c := &Cluster{conns: []*namedConn{{name: "node1", conn: node, lease: l}}, ttl: time.Minute}
	c.ctx, c.cancel = context.WithCancelCause(context.Background())
	releaseOf := func(string) (NodeRelease, error) {
		return NodeRelease{"agent": {Version: "2.0.0", Files: map[string][]byte{"agent": []byte("agent")}}}, nil
	}
	err := c.UpdateNodes(context.Background(), releaseOf, func(Step) {})
	if !errors.Is(err, ErrLockLost) {
		t.Errorf("UpdateNodes says %v, want ErrLockLost", err)
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

// Asleep while the updater updates, UpdateNodes must not go on to
// containerd once the lease may have run out.
func TestUpdateNodesTakesNoNextStepOnceTheLeaseRanOutWhileTheMachineSlept(t *testing.T) {
	var l leased
	l.granted(time.Now(), time.Minute)
	node := &stepRecorder{lease: "ours"}
	n := &namedConn{name: "node1", conn: node, lease: l}
	c := &Cluster{conns: []*namedConn{n}, ttl: time.Minute, lease: nodeclient.Lock{Lease: "ours"}}
	c.ctx, c.cancel = context.WithCancelCause(context.Background())
	node.onUpdater = func() {
		c.mu.Lock()
		n.lease.expiresWall = time.Now().Add(-time.Second)
		c.mu.Unlock()
	}
	releaseOf := func(string) (NodeRelease, error) {
		return NodeRelease{
			"agent":   {Version: "2.0.0", Files: map[string][]byte{"agent": []byte("agent")}},
			"updater": {Version: "2.0.0", Files: map[string][]byte{"updater": []byte("updater")}},
		}, nil
	}
	err := c.UpdateNodes(context.Background(), releaseOf, func(Step) {})
	if !errors.Is(err, ErrLockLost) {
		t.Errorf("UpdateNodes says %v, want ErrLockLost", err)
	}
	if want := []string{"agent", "updater"}; !slices.Equal(node.steps, want) {
		t.Errorf("node1 was sent the updates %v, want %v", node.steps, want)
	}
}
