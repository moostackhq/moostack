package orchestrate

import (
	"context"
	"errors"
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

func (r *updateRecorder) RenewLock(context.Context, string, time.Duration) (*nodeclient.Lock, error) {
	return nil, errors.New("the lease ran out")
}

// Woken from sleep before keepLease noticed the leases ran out,
// UpdateAgents must not send an update once the lease may have run out.
func TestUpdateAgentsSendsNoUpdateOnceTheLeaseRanOutWhileTheMachineSlept(t *testing.T) {
	var l leased
	l.granted(time.Now().Add(-2*time.Second), time.Minute)
	l.expiresWall = l.expiresWall.Add(-time.Minute)
	node := &updateRecorder{}
	c := &Cluster{conns: []*namedConn{{name: "node1", conn: node, lease: l}}, ttl: time.Minute}
	c.ctx, c.cancel = context.WithCancelCause(context.Background())
	agentOf := func(string) ([]byte, error) { return []byte("agent"), nil }
	err := c.UpdateAgents(context.Background(), "2.0.0", agentOf, func(string) {})
	if !errors.Is(err, ErrLockLost) {
		t.Errorf("UpdateAgents says %v, want ErrLockLost", err)
	}
	if node.updates != 0 {
		t.Errorf("node1 was sent %d updates, want none", node.updates)
	}
}
