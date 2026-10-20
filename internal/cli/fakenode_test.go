package cli_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"slices"
	"sync"
	"time"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/cluster"
	"github.com/moostackhq/moostack/internal/nodeclient"
	"github.com/moostackhq/moostack/internal/orchestrate"
)

// fakeNode answers for a node in place of its node API.
type fakeNode struct {
	// err, if set, is why the node can't be reached.
	err error
	// status is what the node reports.
	status nodeclient.Status
	// dialing, if set, is called when mooctl starts to reach the node,
	// and mooctl reaches it once dialing returns.
	dialing func(ctx context.Context)

	mu sync.Mutex
	// recorded are the generations recorded on the node, in order.
	recorded []int64
	// lock is the lease that holds the node's operation lock, nil if
	// none. It never expires.
	lock *nodeclient.Lock
	// taken are the locks the node granted, in order.
	taken []nodeclient.Lock
	// acquired counts the calls to AcquireLock.
	acquired int
	// removed are the identities the node holds as removed.
	removed []string
	// refuseNewRemovals makes the node fail to take identities it doesn't
	// hold as removed yet, as when it can't keep them.
	refuseNewRemovals bool
	// dials counts the times mooctl reached the node.
	dials int
}

// identityOf is the fingerprint of the identity of the node called name
// in the test's cluster directories.
func identityOf(name string) string {
	return fmt.Sprintf("sha256:%064x", sha256.Sum256([]byte(name)))
}

// removedIdentities are the identities the node holds as removed.
func (n *fakeNode) removedIdentities() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return slices.Clone(n.removed)
}

// dialed is how many times mooctl reached the node.
func (n *fakeNode) dialed() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.dials
}

func (n *fakeNode) RemoveIdentities(_ context.Context, identities []string, lease string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.lock == nil || n.lock.Lease != lease {
		return fmt.Errorf("the operation's lease doesn't hold the node's operation lock")
	}
	for _, id := range identities {
		if slices.Contains(n.removed, id) {
			continue
		}
		if n.refuseNewRemovals {
			return fmt.Errorf("keeping the removed identities: the disk is full")
		}
		n.removed = append(n.removed, id)
	}
	return nil
}

// bobsLock is a lock that bob's operation holds, from his desktop.
var bobsLock = nodeclient.Lock{Lease: "bobs", Holder: "bob", Machine: "desktop", Since: time.Date(2026, 10, 19, 9, 30, 0, 0, time.UTC), Until: time.Date(2026, 10, 19, 9, 31, 0, 0, time.UTC)}

// locked is n with its operation lock held by l.
func (n *fakeNode) locked(l nodeclient.Lock) *fakeNode {
	n.lock = &l
	return n
}

// heldLock is the lease that holds the node's lock, nil if none.
func (n *fakeNode) heldLock() *nodeclient.Lock {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.lock
}

// granted are the locks the node granted so far.
func (n *fakeNode) granted() []nodeclient.Lock {
	n.mu.Lock()
	defer n.mu.Unlock()
	return slices.Clone(n.taken)
}

// healthyNode is a node in direct mode that has applied generation.
func healthyNode(generation int64) *fakeNode {
	return &fakeNode{status: nodeclient.Status{Mode: nodev1.Mode_MODE_DIRECT, Generation: generation}}
}

// generations are the generations recorded on the node so far.
func (n *fakeNode) generations() []int64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return slices.Clone(n.recorded)
}

func (n *fakeNode) Status(context.Context) nodeclient.Status {
	s := n.status
	s.Lock = n.heldLock()
	return s
}

func (n *fakeNode) RecordGeneration(_ context.Context, g int64, _ []string, lease string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.lock == nil || n.lock.Lease != lease {
		return fmt.Errorf("the operation's lease doesn't hold the node's operation lock")
	}
	n.recorded = append(n.recorded, g)
	return nil
}

func (n *fakeNode) AcquireLock(_ context.Context, l nodeclient.Lock, _ time.Duration) (nodeclient.Lock, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.acquired++
	if n.lock != nil && n.lock.Lease != l.Lease {
		return *n.lock, nil
	}
	if n.lock == nil {
		l.Since = time.Now()
		n.lock = &l
		n.taken = append(n.taken, l)
	}
	return *n.lock, nil
}

func (n *fakeNode) RenewLock(_ context.Context, lease string, _ time.Duration) (*nodeclient.Lock, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.lock, nil
}

func (n *fakeNode) ReleaseLock(_ context.Context, lease string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.lock != nil && n.lock.Lease == lease {
		n.lock = nil
	}
	return nil
}

func (n *fakeNode) BreakLock(context.Context) (*nodeclient.Lock, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	l := n.lock
	n.lock = nil
	return l, nil
}

func (n *fakeNode) Close() error { return nil }

// dialNode is what reaches the operator's fake nodes, or nil to reach
// nodes over SSH if the operator has none.
func (o *operator) dialNode() func(context.Context, cluster.Node) (orchestrate.Node, error) {
	if o.nodes == nil {
		return nil
	}
	return func(ctx context.Context, n cluster.Node) (orchestrate.Node, error) {
		f, ok := o.nodes[n.Name]
		if !ok {
			return nil, fmt.Errorf("the test has no node %s", n.Name)
		}
		f.mu.Lock()
		f.dials++
		f.mu.Unlock()
		if f.dialing != nil {
			f.dialing(ctx)
		}
		if f.err != nil {
			return nil, f.err
		}
		return f, nil
	}
}
