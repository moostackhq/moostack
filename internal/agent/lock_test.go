//go:build linux

package agent_test

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/nodetest"
)

func TestTheAgentGrantsTheLockToOneLeaseAtATime(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	before := time.Now().Add(-time.Second)
	got := acquire(t, n, "a", time.Minute)
	if got.GetLease() != "a" || got.GetHolder() != "alice" || got.GetMachine() != "laptop-a" {
		t.Fatalf("acquiring a free lock: %v, want it held by lease a of alice on laptop-a", got)
	}
	since := got.GetSince().AsTime()
	if since.Before(before) || since.After(time.Now().Add(time.Second)) {
		t.Errorf("the lock is held since %s, want now", since)
	}
	if until := got.GetUntil().AsTime(); until.Before(before.Add(time.Minute)) || until.After(time.Now().Add(time.Minute+time.Second)) {
		t.Errorf("the lock is held until %s, want a minute from now", until)
	}
	if got := n.EventuallyHealthy(t).GetLock(); got.GetLease() != "a" || !got.GetSince().AsTime().Equal(since) {
		t.Errorf("status reports the lock %v, want lease a since %s", got, since)
	}

	// Another lease is refused, and told who holds it.
	if got := acquire(t, n, "b", time.Minute); got.GetLease() != "a" || got.GetHolder() != "alice" || got.GetMachine() != "laptop-a" {
		t.Errorf("acquiring a lock that a holds: %v, want it still held by a", got)
	}
	// The lease that holds it renews it, since the same time.
	if got := acquire(t, n, "a", time.Minute); got.GetLease() != "a" || !got.GetSince().AsTime().Equal(since) {
		t.Errorf("renewing the lock: %v, want lease a since %s", got, since)
	}
	// Only the lease that holds it releases it.
	releaseLock(t, n, "b")
	if got := n.EventuallyHealthy(t).GetLock(); got.GetLease() != "a" {
		t.Errorf("after lease b released it, status reports the lock %v, want it held by a", got)
	}
	releaseLock(t, n, "a")
	if got := n.EventuallyHealthy(t).GetLock(); got != nil {
		t.Errorf("after lease a released it, status reports the lock %v, want none", got)
	}
	if got := acquire(t, n, "b", time.Minute); got.GetLease() != "b" {
		t.Errorf("acquiring a released lock: %v, want it held by b", got)
	}
}

func TestARenewalHoldsTheLockForItsTTLFromTheCall(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	held := acquire(t, n, "a", time.Minute)
	// Asked again, or renewed, for less than the lock has left, it ends
	// sooner: the new ttl counts from the call, not from the old end,
	// and replaces it.
	for _, c := range []struct {
		what string
		call func() *nodev1.Lock
		ttl  time.Duration
	}{
		{"acquired again", func() *nodev1.Lock { return acquire(t, n, "a", 10*time.Second) }, 10 * time.Second},
		{"renewed", func() *nodev1.Lock { return renew(t, n, "a", 5*time.Second) }, 5 * time.Second},
	} {
		before := time.Now().Add(-time.Second)
		got := c.call()
		after := time.Now().Add(time.Second)
		if until := got.GetUntil().AsTime(); until.Before(before.Add(c.ttl)) || until.After(after.Add(c.ttl)) {
			t.Errorf("%s for %s, the lock is held until %s, want %s from the call, between %s and %s", c.what, c.ttl, until, c.ttl, before.Add(c.ttl), after.Add(c.ttl))
		}
		if !got.GetSince().AsTime().Equal(held.GetSince().AsTime()) {
			t.Errorf("%s, the lock is held since %s, want %s", c.what, got.GetSince().AsTime(), held.GetSince().AsTime())
		}
	}
}

func TestALeaseThatIsntRenewedExpires(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	acquire(t, n, "a", time.Second)
	if got := acquire(t, n, "b", time.Minute); got.GetLease() != "a" {
		t.Fatalf("acquiring a lock that a holds: %v, want it still held by a", got)
	}
	time.Sleep(1500 * time.Millisecond)
	if got := n.EventuallyHealthy(t).GetLock(); got != nil {
		t.Errorf("after lease a expired, status reports the lock %v, want none", got)
	}
	if got := acquire(t, n, "b", time.Minute); got.GetLease() != "b" {
		t.Errorf("acquiring the lock after lease a expired: %v, want it held by b", got)
	}
}

func TestBreakingTheLockReportsWhoHeldIt(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	held := acquire(t, n, "a", time.Minute)
	if got := breakLock(t, n); got.GetLease() != "a" || got.GetHolder() != "alice" || !got.GetSince().AsTime().Equal(held.GetSince().AsTime()) {
		t.Errorf("breaking the lock reports %v, want %v", got, held)
	}
	if got := n.EventuallyHealthy(t).GetLock(); got != nil {
		t.Errorf("after the lock was broken, status reports %v, want none", got)
	}
	if got := breakLock(t, n); got != nil {
		t.Errorf("breaking a free lock reports %v, want none", got)
	}
	// The broken lease can't renew it once another holds it.
	acquire(t, n, "b", time.Minute)
	if got := acquire(t, n, "a", time.Minute); got.GetLease() != "b" {
		t.Errorf("renewing a broken lease: %v, want the lock held by b", got)
	}
}

func TestTheAgentRecordsAGenerationOnlyForTheLeaseThatHoldsTheLock(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	acquire(t, n, "a", time.Minute)
	for _, lease := range []string{"", "b"} {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		_, err := n.Agent.RecordGeneration(ctx, &nodev1.RecordGenerationRequest{Generation: 2, Nodes: []string{"node-1"}, Lease: lease})
		cancel()
		if status.Code(err) != codes.FailedPrecondition {
			t.Errorf("recording a generation with lease %q while a holds the lock: %v, want FailedPrecondition", lease, err)
		}
	}
	if got := n.EventuallyHealthy(t).Generation; got != 0 {
		t.Errorf("after the refused recordings, the node reports generation %d, want 0", got)
	}
	recordAs(t, n, "a", 2, "node-1")
	if got := n.EventuallyHealthy(t).Generation; got != 2 {
		t.Errorf("after lease a recorded generation 2, the node reports %d", got)
	}
}

func TestTheAgentKeepsTheLockAcrossARestart(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	held := acquire(t, n, "a", time.Minute)
	n.Restart(t, "agent")
	if got := n.EventuallyHealthy(t).GetLock(); got.GetLease() != "a" || !got.GetSince().AsTime().Equal(held.GetSince().AsTime()) {
		t.Errorf("after a restart, status reports the lock %v, want %v", got, held)
	}
	if got := acquire(t, n, "b", time.Minute); got.GetLease() != "a" {
		t.Errorf("after a restart, acquiring a lock that a holds: %v, want it still held by a", got)
	}
	// A released lock stays released.
	releaseLock(t, n, "a")
	n.Restart(t, "agent")
	if got := n.EventuallyHealthy(t).GetLock(); got != nil {
		t.Errorf("after a restart, status reports the released lock %v", got)
	}
}

// acquire asks n's agent for the lock for lease, of alice on
// laptop-<lease>, for ttl, and returns the lock as the agent says it now
// is.
func acquire(t *testing.T, n *nodetest.Node, lease string, ttl time.Duration) *nodev1.Lock {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	res, err := n.Agent.AcquireLock(ctx, &nodev1.AcquireLockRequest{
		Lock: &nodev1.Lock{Lease: lease, Holder: "alice", Machine: "laptop-" + lease, Operation: "cluster update"},
		Ttl:  durationpb.New(ttl),
	})
	if err != nil {
		t.Fatalf("acquiring the lock for lease %s: %v", lease, err)
	}
	return res.GetLock()
}

// releaseLock has n's agent release the lock for lease.
func releaseLock(t *testing.T, n *nodetest.Node, lease string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if _, err := n.Agent.ReleaseLock(ctx, &nodev1.ReleaseLockRequest{Lease: lease}); err != nil {
		t.Fatalf("releasing the lock for lease %s: %v", lease, err)
	}
}

// breakLock has n's agent break the lock, and returns what it broke.
func breakLock(t *testing.T, n *nodetest.Node) *nodev1.Lock {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	res, err := n.Agent.BreakLock(ctx, &nodev1.BreakLockRequest{})
	if err != nil {
		t.Fatalf("breaking the lock: %v", err)
	}
	return res.GetBroken()
}

func TestALeaseThatLostTheLockCantRenewItBack(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	// Broken: the renewal finds the lock free, and leaves it so.
	acquire(t, n, "a", time.Minute)
	breakLock(t, n)
	if got := renew(t, n, "a", time.Minute); got != nil {
		t.Errorf("renewing a broken lease: %v, want the lock free", got)
	}
	if got := n.EventuallyHealthy(t).GetLock(); got != nil {
		t.Errorf("after a broken lease tried to renew, status reports the lock %v, want none", got)
	}
	// Expired: the same.
	acquire(t, n, "b", time.Second)
	time.Sleep(1500 * time.Millisecond)
	if got := renew(t, n, "b", time.Minute); got != nil {
		t.Errorf("renewing an expired lease: %v, want the lock free", got)
	}
	// Held by another: the renewal is told who.
	acquire(t, n, "c", time.Minute)
	if got := renew(t, n, "a", time.Minute); got.GetLease() != "c" {
		t.Errorf("renewing a lease while c holds the lock: %v, want c's", got)
	}
	// The lease that holds it renews it.
	if got := renew(t, n, "c", time.Minute); got.GetLease() != "c" {
		t.Errorf("renewing the lease that holds the lock: %v, want c's", got)
	}
}

// renew asks n's agent to renew the lock for lease, for ttl, and returns
// the lock as the agent says it now is, nil if free.
func renew(t *testing.T, n *nodetest.Node, lease string, ttl time.Duration) *nodev1.Lock {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	res, err := n.Agent.RenewLock(ctx, &nodev1.RenewLockRequest{Lease: lease, Ttl: durationpb.New(ttl)})
	if err != nil {
		t.Fatalf("renewing the lock for lease %s: %v", lease, err)
	}
	return res.GetLock()
}

func TestALeaseThatRanOutLongBeforeARestartDoesntComeBack(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	acquire(t, n, "a", time.Second)
	// Nothing asks the agent about the lock until it restarts, well past
	// the lease's end.
	time.Sleep(2500 * time.Millisecond)
	n.Restart(t, "agent")
	if got := n.EventuallyHealthy(t).GetLock(); got != nil {
		t.Errorf("after a restart, status reports the lock %v, which ran out before it", got)
	}
}

func TestALeaseThatLostTheLockCantRecordAGeneration(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	acquire(t, n, "a", time.Minute)
	breakLock(t, n)
	acquire(t, n, "b", time.Second)
	time.Sleep(1500 * time.Millisecond)
	// The lock is free: a was broken, and b ran out.
	for _, lease := range []string{"a", "b"} {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		_, err := n.Agent.RecordGeneration(ctx, &nodev1.RecordGenerationRequest{Generation: 2, Nodes: []string{"node-1"}, Lease: lease})
		cancel()
		if status.Code(err) != codes.FailedPrecondition {
			t.Errorf("recording a generation with lease %s, which no longer holds the free lock: %v, want FailedPrecondition", lease, err)
		}
	}
	if got := n.EventuallyHealthy(t).Generation; got != 0 {
		t.Errorf("after the refused recordings, the node reports generation %d, want 0", got)
	}
}

func TestARenewedLeaseOutlivesARestart(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	acquire(t, n, "a", time.Second)
	renew(t, n, "a", time.Minute)
	// Past twice the first grant's ttl, a restart keeps the lease only if
	// the renewal was kept.
	time.Sleep(2500 * time.Millisecond)
	n.Restart(t, "agent")
	if got := n.EventuallyHealthy(t).GetLock(); got.GetLease() != "a" {
		t.Errorf("after a restart, status reports the lock %v, want the renewed lease a", got)
	}
}

func TestTheLockRecordsTheOperationThatHoldsItAcrossARestart(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	if got := acquire(t, n, "a", time.Minute); got.GetOperation() != "cluster update" {
		t.Fatalf("acquiring the lock: %v, want it held for cluster update", got)
	}
	n.Restart(t, "agent")
	if got := n.EventuallyHealthy(t).GetLock(); got.GetOperation() != "cluster update" {
		t.Errorf("after a restart, status reports the lock %v, want it held for cluster update", got)
	}
}
