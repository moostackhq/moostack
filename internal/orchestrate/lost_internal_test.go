package orchestrate

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
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
