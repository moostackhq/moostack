package orchestrate

import (
	"cmp"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/moostackhq/moostack/internal/nodeclient"
)

// The operation lock lets one mutating operation run on a cluster at a
// time. Each node's agent holds it for one operation's lease, and an
// operation holds it only if every node does, so two operations never
// both hold it, however the nodes are partitioned. A lease runs out a
// lease time after the node got the call that granted or renewed it, so
// a killed mooctl's lease expires. The operation renews it while it
// runs, and gives up once it can't be sure every node still holds it:
// before a lease time has passed since it sent its last renewal that was
// answered, which is never later than the node got it.

// Holder is who an operation runs for, on which machine, and which
// command it runs, such as "cluster update", as the operation lock
// records them.
type Holder struct {
	Who, Machine, Operation string
}

// DefaultLeaseTTL is how long a node holds the lock for an operation
// after the operation last renewed it, when Nodes doesn't say.
const DefaultLeaseTTL = time.Minute

// releaseTimeout bounds releasing the lock on a node. A node that
// doesn't answer frees it when the lease runs out, so an operation that
// ends doesn't wait long for it.
const releaseTimeout = 5 * time.Second

// ErrLockLost says an operation stopped because it couldn't be sure it
// still held the operation lock.
var ErrLockLost = errors.New("the operation lost the operation lock")

// LockHeldError says a node's operation lock is held by another
// operation.
type LockHeldError struct {
	Lock nodeclient.Lock
}

func (e *LockHeldError) Error() string {
	return fmt.Sprintf("it is locked by %s, and expires at %s unless renewed", DescribeLock(e.Lock), LocalTime(e.Lock.Until))
}

// DescribeLock says who holds l, where, since when, in local time, and
// for which command, if the node said.
func DescribeLock(l nodeclient.Lock) string {
	s := fmt.Sprintf("%s on %s since %s", l.Holder, l.Machine, LocalTime(l.Since))
	if l.Operation != "" {
		s += ", for " + l.Operation
	}
	return s
}

// LocalTime is t as the lock's messages show it, in local time.
func LocalTime(t time.Time) string {
	return t.Local().Format("2006-01-02 15:04:05 MST")
}

// newLease is a lease's ID, unique to one operation.
func newLease() string {
	return rand.Text()
}

// leased is the operation's hold on one node's lock.
type leased struct {
	// expires is when the operation stops believing the node holds the
	// lock for it: a lease time after it sent the last renewal the node
	// granted. It is zero if the node never granted it. expiresWall is the
	// same on the wall clock, which keeps going while the machine sleeps,
	// when the monotonic clock stops but the nodes' leases run out.
	expires, expiresWall time.Time
	// lost says why the node no longer holds the lock for the operation,
	// as it answered a renewal: another operation holds it, or it was
	// broken or ran out.
	lost string
	// err is why the node can't take part, such as the lock refused when
	// it was added.
	err error
}

// granted keeps that the node granted the lease, or renewed it, for ttl
// on a call sent at sent.
func (l *leased) granted(sent time.Time, ttl time.Duration) {
	l.expires, l.expiresWall = sent.Add(ttl), sent.Round(0).Add(ttl)
}

// takeLease asks every node the cluster has a connection to for the
// lock, all at once, each within timeout. It returns one error per node
// that didn't grant it, which names the node.
func (c *Cluster) takeLease(ctx context.Context, timeout time.Duration) []error {
	c.mu.Lock()
	conns := slices.Clone(c.conns)
	c.mu.Unlock()
	errs := make([]error, len(conns))
	var wg sync.WaitGroup
	for i, n := range conns {
		wg.Go(func() {
			sent, err := c.take(ctx, n.conn, timeout)
			if err != nil {
				errs[i] = &NodeError{Node: n.name, Err: err}
				return
			}
			c.mu.Lock()
			n.lease.granted(sent, c.ttl)
			c.mu.Unlock()
		})
	}
	wg.Wait()
	return slices.DeleteFunc(errs, func(err error) bool { return err == nil })
}

// take asks conn for the lock within timeout. It returns when it sent the
// call, and fails if the node didn't grant the lock.
func (c *Cluster) take(ctx context.Context, conn Node, timeout time.Duration) (time.Time, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	sent := time.Now()
	got, err := conn.AcquireLock(ctx, c.lease, c.ttl)
	switch {
	case err != nil:
		return sent, fmt.Errorf("taking the operation lock: %w", err)
	case got.Lease != c.lease.Lease:
		return sent, &LockHeldError{Lock: got}
	}
	return sent, nil
}

// keepLease renews the lease on every node that granted it, every third
// of a lease time, until stop is closed. It cancels the operation as
// soon as a node answers that it no longer holds it, or once the lease
// may have run out on a node: a tenth of a lease time early, so that a
// node's clock running a little fast doesn't let the node free it first.
func (c *Cluster) keepLease(stop <-chan struct{}) {
	renew := time.NewTicker(c.ttl / 3)
	defer renew.Stop()
	check := time.NewTicker(c.ttl / 20)
	defer check.Stop()
	var renewing sync.WaitGroup
	defer renewing.Wait()
	inFlight := false
	done := make(chan struct{}, 1)
	for {
		select {
		case <-stop:
			return
		case <-done:
			inFlight = false
		case <-renew.C:
			if inFlight {
				continue
			}
			inFlight = true
			renewing.Go(func() {
				c.renewAll()
				done <- struct{}{}
			})
		case <-check.C:
			if c.CheckLock() != nil {
				return
			}
		}
	}
}

// renewAll renews the lease on every node that holds it, all at once,
// each within a third of a lease time. A node that no longer holds it
// for the operation never gives it back on a renewal, so that a lease
// that was broken or ran out stays lost.
func (c *Cluster) renewAll() {
	c.mu.Lock()
	var holding []*namedConn
	for _, n := range c.conns {
		if !n.lease.expires.IsZero() {
			holding = append(holding, n)
		}
	}
	c.mu.Unlock()
	var wg sync.WaitGroup
	for _, n := range holding {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(c.ctx, c.ttl/3)
			defer cancel()
			c.renew(ctx, n)
		})
	}
	wg.Wait()
}

// renew renews the lease on n, and keeps what n answered. It fails if n
// didn't answer, and then the lease holds until it may have run out,
// which lost tells.
func (c *Cluster) renew(ctx context.Context, n *namedConn) error {
	sent := time.Now()
	got, err := n.conn.RenewLock(ctx, c.lease.Lease, c.ttl)
	c.mu.Lock()
	defer c.mu.Unlock()
	switch {
	case err != nil:
		return err
	case got == nil:
		n.lease.lost = "its lock was broken or ran out"
	case got.Lease != c.lease.Lease:
		n.lease.lost = (&LockHeldError{Lock: *got}).Error()
	default:
		n.lease.granted(sent, c.ttl)
	}
	return nil
}

// lost says why the operation can't be sure it holds the lock at by:
// a node answered that it no longer holds it, or the lease may have run
// out on a node by then. It is nil if every node that granted it still
// holds it.
func (c *Cluster) lost(by time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var problems []string
	for _, n := range c.conns {
		switch {
		case n.lease.lost != "":
			problems = append(problems, n.name+": "+n.lease.lost)
		case !n.lease.expires.IsZero() && (!by.Before(n.lease.expires) || !by.Round(0).Before(n.lease.expiresWall)):
			problems = append(problems, n.name+" didn't renew the lease in time")
		}
	}
	if len(problems) == 0 {
		return nil
	}
	return fmt.Errorf("%w: %s", ErrLockLost, strings.Join(problems, "; "))
}

// releaseLease releases the lock on every node, all at once, each within
// timeout: also on those whose grant never reached the operation, since
// a node releases only the operation's own lease. A node that doesn't
// answer frees it when the lease runs out.
func (c *Cluster) releaseLease(timeout time.Duration) {
	c.mu.Lock()
	conns := slices.Clone(c.conns)
	c.mu.Unlock()
	var wg sync.WaitGroup
	for _, n := range conns {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			n.conn.ReleaseLock(ctx, c.lease.Lease)
		})
	}
	wg.Wait()
}

// Unlocked is what breaking the lock did on one node.
type Unlocked struct {
	// Name is the node's name.
	Name string
	// Broken is the lock that was broken, nil if none was held.
	Broken *nodeclient.Lock
	// Err says why the node couldn't be reached or the lock broken.
	Err error
}

// Unlock breaks the operation lock on every node, whichever operation
// holds it, all at once, and says what it broke on each.
func Unlock(ctx context.Context, nodes Nodes) []Unlocked {
	reached := reach(ctx, nodes)
	unlocked := make([]Unlocked, len(reached))
	var wg sync.WaitGroup
	for i, r := range reached {
		unlocked[i].Name = r.Name
		if r.conn == nil {
			unlocked[i].Err = r.Err
			continue
		}
		wg.Go(func() {
			defer r.conn.Close()
			ctx, cancel := context.WithTimeout(ctx, cmp.Or(nodes.ReachTimeout, DefaultTimeout))
			defer cancel()
			unlocked[i].Broken, unlocked[i].Err = r.conn.BreakLock(ctx)
		})
	}
	wg.Wait()
	return unlocked
}
