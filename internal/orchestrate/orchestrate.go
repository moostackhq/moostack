// Package orchestrate is what an operation does across the whole
// cluster through the node API: the preflight, which reaches every node,
// takes the operation lock and pushes the removed identities before
// anything changes, the rolling update, and recording the generation
// every node has applied.
package orchestrate

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/nodeclient"
)

// Node is a connection to a node's node API.
type Node interface {
	// Status is what the node's agent and updater report.
	Status(ctx context.Context) nodeclient.Status
	// RecordGeneration has the node record generation, of a cluster
	// directory of the nodes called nodes, as the one it has applied, for
	// the operation whose lease holds the node's operation lock.
	RecordGeneration(ctx context.Context, generation int64, nodes []string, lease string) error
	// AcquireLock asks the node for its operation lock for lock's lease,
	// or to renew it, for ttl, and returns the lock as it now is.
	AcquireLock(ctx context.Context, lock nodeclient.Lock, ttl time.Duration) (nodeclient.Lock, error)
	// ReleaseLock releases the node's operation lock, if lease holds it.
	ReleaseLock(ctx context.Context, lease string) error
	// RenewLock renews the node's operation lock for lease, for ttl, only
	// if lease still holds it, and returns the lock as it now is, nil if
	// free.
	RenewLock(ctx context.Context, lease string, ttl time.Duration) (*nodeclient.Lock, error)
	// BreakLock releases the node's operation lock, whichever lease holds
	// it, and returns the lock it broke, nil if none.
	BreakLock(ctx context.Context) (*nodeclient.Lock, error)
	// RemoveIdentities adds identities, by their fingerprints, to those
	// the node holds as removed and refuses as peers, for the operation
	// whose lease holds the node's operation lock.
	RemoveIdentities(ctx context.Context, identities []string, lease string) error
	// UpdateAgent has the node's updater replace its agent with binary,
	// the agent at version, which must become healthy within
	// healthTimeout, or the updater rolls it back.
	UpdateAgent(ctx context.Context, version string, binary []byte, healthTimeout time.Duration) error
	// UpdateUpdater has the node's agent replace its updater with binary,
	// the updater at version, which must become healthy within
	// healthTimeout, or the agent rolls it back.
	UpdateUpdater(ctx context.Context, version string, binary []byte, healthTimeout time.Duration) error
	// UpdateContainerd has the node's agent replace containerd's binaries
	// with files, by name, containerd at version, which must answer
	// within healthTimeout, or the agent puts the old ones back.
	UpdateContainerd(ctx context.Context, version string, files map[string][]byte, healthTimeout time.Duration) error
	// ImportImage has the node's agent import the image in archive, an OCI
	// image layout archive, and returns the names of the images imported.
	ImportImage(ctx context.Context, archive []byte) ([]string, error)
	// Run has the node's agent run the container c, from its image, which
	// the node must have imported, replacing the one of its name, for the
	// operation whose lease holds the node's operation lock.
	Run(ctx context.Context, c *nodev1.Container, lease string) error
	// Close closes the connection.
	Close() error
}

// Dial connects to the node API of the node called name.
type Dial func(ctx context.Context, name string) (Node, error)

// DefaultTimeout is how long reaching a node, or recording a generation on
// it, may take when Nodes doesn't say.
const DefaultTimeout = 30 * time.Second

// DefaultServiceTimeout is how long a cluster service that an update ran
// has to answer its health call, when Nodes doesn't say.
const DefaultServiceTimeout = time.Minute

// Nodes are the nodes an operation runs on, and how to reach them.
type Nodes struct {
	// Names are the names of the nodes in the cluster directory.
	Names []string
	// Dial connects to a node by its name.
	Dial Dial
	// ReachTimeout bounds connecting to one node and asking for its
	// status, and RecordTimeout recording a generation on one node. Zero
	// means DefaultTimeout.
	ReachTimeout, RecordTimeout time.Duration
	// ServiceTimeout is how long a cluster service that an update ran has
	// to answer its health call. Zero means DefaultServiceTimeout.
	ServiceTimeout time.Duration
	// LeaseTTL is how long a node holds the operation lock after the
	// operation last renewed it. Zero means DefaultLeaseTTL.
	LeaseTTL time.Duration
	// Removed are the fingerprints of the identities the cluster
	// directory lists as removed, which the preflight pushes to every
	// node.
	Removed []string
	// Check, if set, is what else the preflight checks of each node, such
	// as the release it runs, given the status of each node it reached
	// too: a node it fails can't be operated, even with force.
	Check func(s nodeclient.Status, all []nodeclient.Status) error
}

// Reached is a node as Survey found it.
type Reached struct {
	// Name is the node's name.
	Name string
	// Err says why the node couldn't be reached, nil if it was.
	Err error
	// Status is what the node reported, if it was reached.
	Status nodeclient.Status
	// conn is the connection to the node, nil if it wasn't reached.
	conn Node
}

// KnownGeneration is the generation the reached node r last applied,
// and whether it is known: whether r was reached, and its agent answered
// and could tell.
func (r Reached) KnownGeneration() (int64, bool) {
	if r.Err != nil {
		return 0, false
	}
	return r.Status.KnownGeneration()
}

// GenerationProblem says how the generation the reached node r has
// applied differs from a cluster directory at generation: a
// *LaterGenerationError, an *EarlierGenerationError, an
// *UnknownGenerationError if the node can't tell, or nil if they are the
// same or the node's agent didn't answer.
func (r Reached) GenerationProblem(generation int64) error {
	switch applied, ok := r.KnownGeneration(); {
	case r.Err != nil || r.Status.AgentErr != nil:
		return nil
	case !ok:
		return &UnknownGenerationError{Problem: r.Status.GenerationUnknown}
	case applied > generation:
		return &LaterGenerationError{Applied: applied, Generation: generation}
	case applied < generation:
		return &EarlierGenerationError{Applied: applied, Generation: generation}
	}
	return nil
}

// reach connects to nodes, all at once, each within its reach timeout,
// and asks each for its status. It returns them in the order of their
// names, with the connections to the nodes it reached open.
func reach(ctx context.Context, nodes Nodes) []Reached {
	reached := make([]Reached, len(nodes.Names))
	var wg sync.WaitGroup
	for i, name := range nodes.Names {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(ctx, cmp.Or(nodes.ReachTimeout, DefaultTimeout))
			defer cancel()
			r := Reached{Name: name}
			r.conn, r.Err = nodes.Dial(ctx, name)
			if r.Err == nil {
				r.Status = r.conn.Status(ctx)
			}
			reached[i] = r
		})
	}
	wg.Wait()
	return reached
}

// Survey reaches nodes, all at once, as the preflight does, and reports
// what each one says, or why it couldn't be reached. It never fails.
func Survey(ctx context.Context, nodes Nodes) []Reached {
	reached := reach(ctx, nodes)
	for i := range reached {
		if reached[i].conn != nil {
			reached[i].conn.Close()
			reached[i].conn = nil
		}
	}
	return reached
}

// Cluster is the cluster an operation runs on once its preflight has
// passed, with a connection to every node, holding the operation lock
// until it is closed. Close it when done.
type Cluster struct {
	// Forced are the generation problems that force let the preflight
	// pass, one error per node, which names it.
	Forced []error
	// Unlisted are the names of the nodes that the latest generation any
	// node applied lists, but the cluster directory doesn't, when force
	// let the preflight pass. The operation can't reach them, so they
	// keep the generation they have applied.
	Unlisted []string
	// Latest is the latest generation any node has applied.
	Latest         int64
	recordTimeout  time.Duration
	reachTimeout   time.Duration
	serviceTimeout time.Duration
	// checkNode is Nodes.Check, nil if it isn't set.
	checkNode func(s nodeclient.Status, all []nodeclient.Status) error
	// lease is the operation's hold on the lock, which every node holds
	// for ttl after the operation last renewed it.
	lease nodeclient.Lock
	ttl   time.Duration
	// ctx ends when the operation loses the lock, or the cluster is
	// closed.
	ctx    context.Context
	cancel context.CancelCauseFunc
	// held ends as ctx does, but not with the context the preflight was
	// given: the lease is kept in it, and new nodes locked, so that what
	// ends the caller's work, such as a signal, doesn't end the lease.
	held    context.Context
	endHeld context.CancelCauseFunc
	// stop ends keepLease, which closes kept once it has.
	stop, kept chan struct{}
	mu         sync.Mutex
	conns      []*namedConn
}

// namedConn is the connection to the node called name, and the
// operation's hold on its lock.
type namedConn struct {
	name  string
	conn  Node
	lease leased
}

// LaterGenerationError says a node has applied a later generation than
// the cluster directory's.
type LaterGenerationError struct {
	// Applied is the generation the node has applied, and Generation the
	// cluster directory's.
	Applied, Generation int64
}

func (e *LaterGenerationError) Error() string {
	return fmt.Sprintf("it has applied generation %d, later than this cluster directory's %d", e.Applied, e.Generation)
}

// EarlierGenerationError says a node has applied an earlier generation
// than the cluster directory's, as when it failed to record the latest.
type EarlierGenerationError struct {
	// Applied is the generation the node has applied, and Generation the
	// cluster directory's.
	Applied, Generation int64
}

func (e *EarlierGenerationError) Error() string {
	return fmt.Sprintf("it has applied generation %d, earlier than this cluster directory's %d, until an operation that changes the cluster directory records its generation", e.Applied, e.Generation)
}

// UnknownGenerationError says a node can't tell the generation it has
// applied.
type UnknownGenerationError struct {
	// Problem is why, as the node says.
	Problem string
}

func (e *UnknownGenerationError) Error() string {
	return "it can't tell the generation it has applied: " + e.Problem
}

// Preflight reaches nodes, all at once, before an operation changes
// anything, and checks that each can be operated from a cluster
// directory at generation: that it can be reached with its pinned host
// key, its agent and updater answer, it is in direct mode, and it can
// tell the generation it has applied, which isn't later than the
// directory's, and that it passes nodes.Check. With force, the
// generation checks pass, and Cluster.Forced says what they found. It
// fails naming every node that fails a check, and why. Once every node
// passes, it takes the operation lock for holder on every node, or fails
// naming each node whose lock another operation holds, after handing
// back the lock on the others. Holding the lock, it checks the
// generations again, fails if a lease may already have run out, and
// then, renewing the lease, has every node hold the identities
// nodes.Removed lists as removed, and fails if the operation can't be
// sure it still holds the lock once they have.
func Preflight(ctx context.Context, nodes Nodes, generation int64, force bool, holder Holder) (*Cluster, error) {
	c := &Cluster{
		recordTimeout:  cmp.Or(nodes.RecordTimeout, DefaultTimeout),
		reachTimeout:   cmp.Or(nodes.ReachTimeout, DefaultTimeout),
		serviceTimeout: cmp.Or(nodes.ServiceTimeout, DefaultServiceTimeout),
		checkNode:      nodes.Check,
		lease:          nodeclient.Lock{Lease: newLease(), Holder: holder.Who, Machine: holder.Machine, Operation: holder.Operation},
		ttl:            cmp.Or(nodes.LeaseTTL, DefaultLeaseTTL),
	}
	reached := reach(ctx, nodes)
	for _, r := range reached {
		if r.conn != nil {
			c.conns = append(c.conns, &namedConn{name: r.Name, conn: r.conn})
		}
	}
	// A node that can't be operated fails the preflight before the lock
	// is asked for.
	if problems := c.check(reached, generation, force); len(problems) > 0 {
		c.closeConns()
		return nil, &PreflightError{Problems: problems}
	}
	fail := func(err error) (*Cluster, error) {
		c.releaseLease(min(c.reachTimeout, releaseTimeout))
		c.closeConns()
		return nil, err
	}
	if problems := c.takeLease(ctx, c.reachTimeout); len(problems) > 0 {
		return fail(&PreflightError{Problems: problems})
	}
	// Another operation may have recorded a generation and ended between
	// the check and the lock. Holding the lock, no other can, so the
	// generations are checked again on what the nodes say now.
	refresh(ctx, reached, c.reachTimeout)
	if problems := c.check(reached, generation, force); len(problems) > 0 {
		return fail(&PreflightError{Problems: problems})
	}
	if force {
		c.Unlisted = unlisted(reached, nodes.Names, c.Latest)
	}
	// A grant that took long to arrive may have run out already.
	if err := c.lost(time.Now().Add(c.ttl / 10)); err != nil {
		return fail(err)
	}
	c.ctx, c.cancel = context.WithCancelCause(ctx)
	c.held, c.endHeld = context.WithCancelCause(context.WithoutCancel(ctx))
	c.stop, c.kept = make(chan struct{}), make(chan struct{})
	go func() {
		defer close(c.kept)
		c.keepLease(c.stop)
	}()
	// Every operation pushes the removed identities, so that a node that
	// missed a removal gets them before anything changes. The lease is
	// renewed meanwhile, and checked again once the push is done.
	problems := c.push(c.ctx, c.conns, nodes.Removed)
	if err := c.CheckLock(); err != nil {
		c.Close()
		return nil, err
	}
	if len(problems) > 0 {
		c.Close()
		return nil, &PreflightError{Problems: problems}
	}
	return c, nil
}

// check checks that each reached node can be operated from a cluster
// directory at generation, and sets c.Latest and, with force, c.Forced
// from what they report. It returns one error per node that fails, which
// names it.
func (c *Cluster) check(reached []Reached, generation int64, force bool) []error {
	c.Latest, c.Forced = 0, nil
	var all []nodeclient.Status
	for _, r := range reached {
		if r.Err == nil {
			all = append(all, r.Status)
		}
	}
	var problems []error
	for _, r := range reached {
		if applied, ok := r.KnownGeneration(); ok {
			c.Latest = max(c.Latest, applied)
		}
		err := check(r)
		if err == nil && c.checkNode != nil {
			err = c.checkNode(r.Status, all)
		}
		if err == nil {
			err = r.GenerationProblem(generation)
			if errors.As(err, new(*EarlierGenerationError)) {
				err = nil
			} else if err != nil && force {
				c.Forced = append(c.Forced, fmt.Errorf("%s: %w", r.Name, err))
				err = nil
			}
		}
		if err != nil {
			problems = append(problems, &NodeError{Node: r.Name, Err: err})
		}
	}
	return problems
}

// refresh asks each reached node for its status again, all at once, each
// within timeout.
func refresh(ctx context.Context, reached []Reached, timeout time.Duration) {
	var wg sync.WaitGroup
	for i := range reached {
		if reached[i].conn == nil {
			continue
		}
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			reached[i].Status = reached[i].conn.Status(ctx)
		})
	}
	wg.Wait()
}

// Context is the operation's context, which ends with a cause that wraps
// ErrLockLost when the operation can no longer be sure it holds the
// operation lock, or when the cluster is closed. Everything the operation
// does once the preflight has passed runs in it.
func (c *Cluster) Context() context.Context { return c.ctx }

// Held is the operation's context apart from what ends the context the
// preflight was given: it ends, with a cause that wraps ErrLockLost, when
// the operation can no longer be sure it holds the operation lock, or
// when the cluster is closed, but a signal that ends the caller's work
// doesn't end it. Telling the nodes of a change already made runs in it,
// so that they are told once the change is made.
func (c *Cluster) Held() context.Context { return c.held }

// CheckLock fails, and ends the operation's context, if the operation
// can't be sure it still holds the operation lock. keepLease calls it
// every twentieth of a lease time, but its timer runs on the monotonic
// clock, which stops while the machine sleeps, so on waking it may not
// have noticed yet that the leases ran out. An operation calls it too
// before a step that changes a node outside what the node fences, such
// as an install.
func (c *Cluster) CheckLock() error {
	if err := c.lost(time.Now().Add(c.ttl / 10)); err != nil {
		c.endHeld(err)
		c.cancel(err)
		return err
	}
	return nil
}

// check fails if the reached node r can't be operated, whatever the
// generation.
func check(r Reached) error {
	switch {
	case r.Err != nil:
		// The error says what failed: reaching the node, or its host key.
		return &UnreachableError{Err: r.Err}
	case r.Status.AgentErr != nil:
		return &UnreachableError{Err: fmt.Errorf("the agent doesn't answer: %w", r.Status.AgentErr)}
	case r.Status.UpdaterErr != nil:
		return &UnreachableError{Err: fmt.Errorf("the updater doesn't answer: %w", r.Status.UpdaterErr)}
	case r.Status.Mode == nodev1.Mode_MODE_CONTROL_PLANE:
		return errors.New("it is in control plane mode: a control plane operates it. To operate it from here again, detach it from the control plane with agent detach, run as root on the node")
	case r.Status.Mode != nodev1.Mode_MODE_DIRECT:
		return fmt.Errorf("it is in mode %s, not direct mode", r.Status.Mode)
	case r.Status.UpdaterMode == nodev1.Mode_MODE_CONTROL_PLANE:
		return errors.New("its updater is in control plane mode, though its agent is in direct mode, as for a second after a detach; try again")
	}
	return nil
}

// unlisted are the names, sorted, of the nodes that the nodes reached at
// generation latest list, but names don't.
func unlisted(reached []Reached, names []string, latest int64) []string {
	var found []string
	for _, r := range reached {
		if applied, ok := r.KnownGeneration(); !ok || applied != latest {
			continue
		}
		for _, n := range r.Status.Nodes {
			if !slices.Contains(names, n) && !slices.Contains(found, n) {
				found = append(found, n)
			}
		}
	}
	slices.Sort(found)
	return found
}

// UnreachableError says a node, or its agent or updater, couldn't be
// reached, and why.
type UnreachableError struct{ Err error }

func (e *UnreachableError) Error() string { return e.Err.Error() }

func (e *UnreachableError) Unwrap() error { return e.Err }

// NodeError is why the node called Node failed.
type NodeError struct {
	Node string
	Err  error
}

func (e *NodeError) Error() string { return e.Node + ": " + e.Err.Error() }

func (e *NodeError) Unwrap() error { return e.Err }

// PreflightError says which nodes failed the preflight, and why.
type PreflightError struct {
	// Problems are one error per node, which names it, a *NodeError
	// where the node failed a check or refused the lock.
	Problems []error
}

func (e *PreflightError) Error() string {
	lines := make([]string, len(e.Problems))
	for i, p := range e.Problems {
		lines[i] = "  " + p.Error()
	}
	return "the preflight failed, so nothing changed:\n" + strings.Join(lines, "\n")
}

func (e *PreflightError) Unwrap() []error { return e.Problems }

// Unreachable says whether the node called name, or its agent or
// updater, couldn't be reached.
func (e *PreflightError) Unreachable(name string) bool {
	for _, p := range e.Problems {
		var ne *NodeError
		if errors.As(p, &ne) && ne.Node == name && errors.As(p, new(*UnreachableError)) {
			return true
		}
	}
	return false
}

// Forceable says whether force would have let the preflight pass: every
// problem is a generation that is later than the directory's, or that
// the node can't tell.
func (e *PreflightError) Forceable() bool {
	for _, p := range e.Problems {
		if !errors.As(p, new(*LaterGenerationError)) && !errors.As(p, new(*UnknownGenerationError)) {
			return false
		}
	}
	return true
}

// Next is the generation an operation that writes the cluster directory,
// now at generation, gives it: one later than both that and every
// generation a node has applied, so that it is the latest even when an
// older generation is forced.
func (c *Cluster) Next(generation int64) int64 { return max(generation, c.Latest) + 1 }

// Add adds the connection to a new node called name, which Close closes,
// and takes the operation lock on it. A node that doesn't grant it can't
// record a generation, and Record says why. The new node gets the
// removed identities from its install, not from Add.
func (c *Cluster) Add(name string, conn Node) {
	n := &namedConn{name: name, conn: conn}
	sent, err := c.take(c.held, conn, c.reachTimeout)
	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil {
		n.lease.err = err
	} else {
		n.lease.granted(sent, c.ttl)
	}
	c.conns = append(c.conns, n)
}

// Remove leaves the node called name out of the rest of the operation:
// it releases the lock on it and closes the connection, so that the node
// can go away without the operation losing its lease.
func (c *Cluster) Remove(name string) {
	c.mu.Lock()
	i := slices.IndexFunc(c.conns, func(n *namedConn) bool { return n.name == name })
	if i < 0 {
		c.mu.Unlock()
		return
	}
	n := c.conns[i]
	c.conns = slices.Delete(c.conns, i, i+1)
	c.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), min(c.reachTimeout, releaseTimeout))
	defer cancel()
	n.conn.ReleaseLock(ctx, c.lease.Lease)
	n.conn.Close()
}

// RemoveIdentities has every node the cluster has a connection to hold
// identities, by their fingerprints, as removed, all at once, each within
// the record timeout. It fails naming every node that didn't.
func (c *Cluster) RemoveIdentities(ctx context.Context, identities []string) error {
	c.mu.Lock()
	conns := slices.Clone(c.conns)
	c.mu.Unlock()
	return errors.Join(c.push(ctx, conns, identities)...)
}

// push has each of conns hold identities as removed, all at once, each
// within the record timeout. It returns one error per node that didn't,
// which names it.
func (c *Cluster) push(ctx context.Context, conns []*namedConn, identities []string) []error {
	errs := make([]error, len(conns))
	var wg sync.WaitGroup
	for i, n := range conns {
		wg.Go(func() {
			err := n.lease.err
			if err == nil {
				ctx, cancel := context.WithTimeout(ctx, c.recordTimeout)
				defer cancel()
				err = n.conn.RemoveIdentities(ctx, identities, c.lease.Lease)
			}
			if err != nil {
				errs[i] = fmt.Errorf("%s didn't take the removed identities: %w", n.name, err)
			}
		})
	}
	wg.Wait()
	return slices.DeleteFunc(errs, func(err error) bool { return err == nil })
}

// Record has every node record generation, of a cluster directory of
// every node the cluster has a connection to, as the one it has applied,
// all at once, each within the record timeout. It fails naming every
// node that didn't.
func (c *Cluster) Record(ctx context.Context, generation int64) error {
	c.mu.Lock()
	conns := slices.Clone(c.conns)
	c.mu.Unlock()
	names := make([]string, len(conns))
	for i, n := range conns {
		names[i] = n.name
	}
	slices.Sort(names)
	errs := make([]error, len(conns))
	var wg sync.WaitGroup
	for i, n := range conns {
		wg.Go(func() {
			if n.lease.err != nil {
				errs[i] = fmt.Errorf("%s didn't record generation %d: %w", n.name, generation, n.lease.err)
				return
			}
			ctx, cancel := context.WithTimeout(ctx, c.recordTimeout)
			defer cancel()
			if err := n.conn.RecordGeneration(ctx, generation, names, c.lease.Lease); err != nil {
				errs[i] = fmt.Errorf("%s didn't record generation %d: %w", n.name, generation, err)
			}
		})
	}
	wg.Wait()
	return errors.Join(slices.DeleteFunc(errs, func(err error) bool { return err == nil })...)
}

// Close releases the operation lock, ends the operation's context, and
// closes the connections to the nodes.
func (c *Cluster) Close() error {
	// Cancelling first ends a renewal in flight, which keepLease waits
	// for.
	c.endHeld(context.Canceled)
	c.cancel(context.Canceled)
	close(c.stop)
	<-c.kept
	c.releaseLease(min(c.reachTimeout, releaseTimeout))
	return c.closeConns()
}

// closeConns closes the connections to the nodes.
func (c *Cluster) closeConns() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var errs []error
	for _, n := range c.conns {
		errs = append(errs, n.conn.Close())
	}
	return errors.Join(errs...)
}
