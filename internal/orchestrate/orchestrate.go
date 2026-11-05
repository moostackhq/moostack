// Package orchestrate is what an operation does across the whole
// cluster through the node API: the preflight, which reaches every node,
// takes the operation lock, brings up a node behind the cluster directory
// and pushes the removed identities before anything changes, the rolling
// update, and recording the generation
// every node has applied.
package orchestrate

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"time"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/hostlayout"
	"github.com/moostackhq/moostack/internal/nodeclient"
)

// Node is a connection to a node's node API.
type Node interface {
	// Status is what the node's agent and updater report.
	Status(ctx context.Context) nodeclient.Status
	// RecordGeneration has the node record generation, of a cluster
	// directory of nodes, as the one it has applied, for the operation
	// whose lease holds the node's operation lock.
	RecordGeneration(ctx context.Context, generation int64, nodes []*nodev1.GenerationNode, lease string) error
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
	// RemoveIdentities adds identities to those the node holds as removed
	// and refuses as peers, for the operation whose lease holds the node's
	// operation lock.
	RemoveIdentities(ctx context.Context, identities []*nodev1.RemovedIdentity, lease string) error
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
	// ImportImage has the node's agent import the image in the OCI image
	// layout archive that archive reads, as it reads it, for the operation
	// whose lease holds the node's operation lock, and returns the names of
	// the images imported.
	ImportImage(ctx context.Context, archive io.Reader, lease string) ([]string, error)
	// PullImage has the node's agent pull the public image ref, a
	// normalized reference, for the operation whose lease holds the node's
	// operation lock, and returns the image pulled.
	PullImage(ctx context.Context, ref, lease string) (nodeclient.PulledImage, error)
	// Run has the node's agent run the container c, from its image, which
	// the node must have imported or pulled, replacing the one of its
	// name, for the operation whose lease holds the node's operation lock,
	// and says whether it started the container's process.
	Run(ctx context.Context, c *nodev1.Container, lease string) (bool, error)
	// Remove has the node's agent stop and delete the container called
	// name, for the operation whose lease holds the node's operation lock,
	// and says whether it removed one.
	Remove(ctx context.Context, name, lease string) (bool, error)
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
	// Names are the names of the nodes to operate on, and PrivateAddresses
	// the address other nodes reach each one at, by name, which Record
	// records.
	Names            []string
	PrivateAddresses map[string]netip.Addr
	// Partial says Names leaves out a node of the cluster directory.
	Partial bool
	// Dial connects to a node by its name.
	Dial Dial
	// ReachTimeout bounds connecting to one node and asking for its
	// status, and RecordTimeout recording a generation on one node. Zero
	// means DefaultTimeout.
	ReachTimeout, RecordTimeout time.Duration
	// ServiceTimeout is how long a cluster service that an update ran has
	// to answer its health call. Zero means DefaultServiceTimeout.
	ServiceTimeout time.Duration
	// StartCheck is how long a deploy waits once the app's container
	// started before it checks that the container still runs. Zero means
	// DefaultStartCheck.
	StartCheck time.Duration
	// LeaseTTL is how long a node holds the operation lock after the
	// operation last renewed it. Zero means DefaultLeaseTTL.
	LeaseTTL time.Duration
	// Removed are the identities the cluster directory lists as removed,
	// which the preflight pushes to every node.
	Removed []hostlayout.RemovedIdentity
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
	// Unlisted are the names of the nodes that the cluster directory
	// doesn't list, but the latest generation any node applied does, when
	// force let the preflight pass and that generation is no older than
	// the directory's. The operation can't reach them, so they keep the
	// generation they have applied.
	Unlisted []string
	// CaughtUp are the nodes behind the cluster directory that the
	// preflight had record its generation, and Behind one error for each
	// that didn't, which names it.
	CaughtUp []string
	Behind   []error
	// Latest is the latest generation any node had applied when the
	// preflight checked.
	Latest         int64
	recordTimeout  time.Duration
	reachTimeout   time.Duration
	serviceTimeout time.Duration
	startCheck     time.Duration
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

// namedConn is the connection to the node called name, at the private
// address privateAddress, and the operation's hold on its lock.
type namedConn struct {
	name           string
	privateAddress netip.Addr
	conn           Node
	lease          leased
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
	return fmt.Sprintf("it has applied generation %d, earlier than this cluster directory's %d, until the next operation has it record that generation", e.Applied, e.Generation)
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
// then, renewing the lease, has a node behind generation record it,
// unless force or nodes.Partial, has every node hold the identities
// nodes.Removed lists as removed, and fails if the operation can't be
// sure it still holds the lock once they have. A node that refuses to
// record fails nothing: Behind says so.
func Preflight(ctx context.Context, nodes Nodes, generation int64, force bool, holder Holder) (*Cluster, error) {
	c := &Cluster{
		recordTimeout:  cmp.Or(nodes.RecordTimeout, DefaultTimeout),
		reachTimeout:   cmp.Or(nodes.ReachTimeout, DefaultTimeout),
		serviceTimeout: cmp.Or(nodes.ServiceTimeout, DefaultServiceTimeout),
		startCheck:     cmp.Or(nodes.StartCheck, DefaultStartCheck),
		checkNode:      nodes.Check,
		lease:          nodeclient.Lock{Lease: newLease(), Holder: holder.Who, Machine: holder.Machine, Operation: holder.Operation},
		ttl:            cmp.Or(nodes.LeaseTTL, DefaultLeaseTTL),
	}
	reached := reach(ctx, nodes)
	for _, r := range reached {
		if r.conn != nil {
			c.conns = append(c.conns, &namedConn{name: r.Name, privateAddress: nodes.PrivateAddresses[r.Name], conn: r.conn})
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
	// Holding the lock, the generations are checked again on what the
	// nodes say now.
	refresh(ctx, reached, c.reachTimeout)
	if problems := c.check(reached, generation, force); len(problems) > 0 {
		return fail(&PreflightError{Problems: problems})
	}
	// The unlisted nodes are of a generation no older than the directory's.
	if force && c.Latest >= generation {
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
	if !force && !nodes.Partial {
		c.catchUp(reached, nodes, generation)
	}
	// A node that missed a removal gets the removed identities before
	// anything changes. The lease is renewed meanwhile, and checked again
	// once the push is done.
	problems := c.push(c.ctx, c.conns, nodes.Removed)
	var kept []string
	if len(c.CaughtUp) > 0 {
		kept = append(kept, strings.Join(c.CaughtUp, ", ")+" recorded the cluster directory's generation")
	}
	if err := c.CheckLock(); err != nil {
		c.Close()
		if len(kept) > 0 {
			return nil, fmt.Errorf("%w, though %s", err, kept[0])
		}
		return nil, err
	}
	if len(problems) > 0 {
		if len(nodes.Removed) > 0 && len(problems) < len(c.conns) {
			kept = append(kept, "the nodes that took the removed identities keep them")
		}
		c.Close()
		return nil, &PreflightError{Problems: problems, Kept: kept}
	}
	return c, nil
}

// catchUp has each of reached that applied a generation before generation
// record it, of the cluster directory's nodes, all at once, each within
// the record timeout. It sets c.CaughtUp to those that did and c.Behind
// to one error per node that didn't, which names it.
func (c *Cluster) catchUp(reached []Reached, nodes Nodes, generation int64) {
	var behind []*namedConn
	for _, r := range reached {
		if applied, ok := r.KnownGeneration(); ok && applied < generation {
			if i := slices.IndexFunc(c.conns, func(n *namedConn) bool { return n.name == r.Name }); i >= 0 {
				behind = append(behind, c.conns[i])
			}
		}
	}
	listed := make([]*nodev1.GenerationNode, len(nodes.Names))
	for i, name := range nodes.Names {
		listed[i] = &nodev1.GenerationNode{Name: name, PrivateAddress: nodes.PrivateAddresses[name].String()}
	}
	for i, err := range c.record(c.ctx, behind, generation, listed) {
		if err != nil {
			c.Behind = append(c.Behind, err)
		} else {
			c.CaughtUp = append(c.CaughtUp, behind[i].name)
		}
	}
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
// doesn't end it. Telling the nodes of a change already made runs in it.
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
	// Kept says what the nodes keep of a preflight that failed, none if
	// it changed nothing.
	Kept []string
}

func (e *PreflightError) Error() string {
	lines := make([]string, len(e.Problems))
	for i, p := range e.Problems {
		lines[i] = "  " + p.Error()
	}
	said := "the preflight failed, so nothing changed"
	if len(e.Kept) > 0 {
		said = "the preflight failed, so the operation changed nothing, but " + strings.Join(e.Kept, ", and ")
	}
	return said + ":\n" + strings.Join(lines, "\n")
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
// generation a node has applied, even when an older generation is forced.
func (c *Cluster) Next(generation int64) int64 { return max(generation, c.Latest) + 1 }

// Add adds the connection to a new node called name, whose private
// address is privateAddress, which Close closes, and takes the operation
// lock on it. A node that doesn't grant it can't record a generation, and
// Record says why. The new node gets the removed identities from its
// install, not from Add.
func (c *Cluster) Add(name string, privateAddress netip.Addr, conn Node) {
	n := &namedConn{name: name, privateAddress: privateAddress, conn: conn}
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
// can go away without the operation losing its lease. The operation's
// last node keeps the lock, until it goes away or the lease runs out.
func (c *Cluster) Remove(name string) {
	c.mu.Lock()
	i := slices.IndexFunc(c.conns, func(n *namedConn) bool { return n.name == name })
	if i < 0 {
		c.mu.Unlock()
		return
	}
	n := c.conns[i]
	c.conns = slices.Delete(c.conns, i, i+1)
	last := len(c.conns) == 0
	c.mu.Unlock()
	if !last {
		ctx, cancel := context.WithTimeout(context.Background(), min(c.reachTimeout, releaseTimeout))
		defer cancel()
		n.conn.ReleaseLock(ctx, c.lease.Lease)
	}
	n.conn.Close()
}

// RemoveIdentities has every node the cluster has a connection to hold
// identities as removed, all at once, each within the record timeout. It
// fails naming every node that didn't.
func (c *Cluster) RemoveIdentities(ctx context.Context, identities []hostlayout.RemovedIdentity) error {
	c.mu.Lock()
	conns := slices.Clone(c.conns)
	c.mu.Unlock()
	return errors.Join(c.push(ctx, conns, identities)...)
}

// push has each of conns hold identities as removed, all at once, each
// within the record timeout. It returns one error per node that didn't,
// which names it.
func (c *Cluster) push(ctx context.Context, conns []*namedConn, identities []hostlayout.RemovedIdentity) []error {
	removed := make([]*nodev1.RemovedIdentity, len(identities))
	for i, r := range identities {
		removed[i] = &nodev1.RemovedIdentity{Node: r.Node, Identity: r.Identity}
	}
	errs := make([]error, len(conns))
	var wg sync.WaitGroup
	for i, n := range conns {
		wg.Go(func() {
			err := n.lease.err
			if err == nil {
				ctx, cancel := context.WithTimeout(ctx, c.recordTimeout)
				defer cancel()
				err = n.conn.RemoveIdentities(ctx, removed, c.lease.Lease)
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
// every node the cluster has a connection to, by name and private
// address, as the one it has applied, all at once, each within the
// record timeout. It fails naming every node that didn't.
func (c *Cluster) Record(ctx context.Context, generation int64) error {
	c.mu.Lock()
	conns := slices.Clone(c.conns)
	c.mu.Unlock()
	return errors.Join(c.record(ctx, conns, generation, generationNodes(conns))...)
}

// RecordOn has the node called name alone record generation, of a cluster
// directory of every node the cluster has a connection to, as Record does.
func (c *Cluster) RecordOn(ctx context.Context, name string, generation int64) error {
	c.mu.Lock()
	conns := slices.Clone(c.conns)
	c.mu.Unlock()
	i, err := connIndex(conns, name)
	if err != nil {
		return err
	}
	return c.record(ctx, conns[i:i+1], generation, generationNodes(conns))[0]
}

// connIndex is the index of the node called name in conns.
func connIndex(conns []*namedConn, name string) (int, error) {
	i := slices.IndexFunc(conns, func(n *namedConn) bool { return n.name == name })
	if i < 0 {
		return 0, fmt.Errorf("the operation has no node %s", name)
	}
	return i, nil
}

// generationNodes are conns as a generation lists them.
func generationNodes(conns []*namedConn) []*nodev1.GenerationNode {
	nodes := make([]*nodev1.GenerationNode, len(conns))
	for i, n := range conns {
		nodes[i] = &nodev1.GenerationNode{Name: n.name, PrivateAddress: n.privateAddress.String()}
	}
	return nodes
}

// record has each of conns record generation, of a cluster directory of
// nodes, sorted by name, all at once, each within the record timeout. It
// returns an error for each of conns, in order: nil if it did, and one
// naming it if it didn't.
func (c *Cluster) record(ctx context.Context, conns []*namedConn, generation int64, nodes []*nodev1.GenerationNode) []error {
	slices.SortFunc(nodes, func(a, b *nodev1.GenerationNode) int { return strings.Compare(a.Name, b.Name) })
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
			if err := n.conn.RecordGeneration(ctx, generation, nodes, c.lease.Lease); err != nil {
				errs[i] = fmt.Errorf("%s didn't record generation %d: %w", n.name, generation, err)
			}
		})
	}
	wg.Wait()
	return errs
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
