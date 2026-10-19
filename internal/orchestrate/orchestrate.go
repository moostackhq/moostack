// Package orchestrate is what an operation does across the whole
// cluster through the node API: the preflight, which reaches every node
// before anything changes, and recording the generation every node has
// applied.
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
	// directory of the nodes called nodes, as the one it has applied.
	RecordGeneration(ctx context.Context, generation int64, nodes []string) error
	// Close closes the connection.
	Close() error
}

// Dial connects to the node API of the node called name.
type Dial func(ctx context.Context, name string) (Node, error)

// DefaultTimeout is how long reaching a node, or recording a generation on
// it, may take when Nodes doesn't say.
const DefaultTimeout = 30 * time.Second

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
// passed, with a connection to every node. Close it when done.
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
	Latest        int64
	conns         []namedConn
	recordTimeout time.Duration
}

// namedConn is the connection to the node called name.
type namedConn struct {
	name string
	conn Node
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
// anything, and checks that each can be operated from a cluster directory
// at generation: that it can be reached with its pinned host key, its
// agent and updater answer, it is in direct mode, and it can tell the
// generation it has applied, which isn't later than the directory's.
// With force, the generation checks pass, and Cluster.Forced says what
// they found. It fails naming every node that fails a check, and why.
func Preflight(ctx context.Context, nodes Nodes, generation int64, force bool) (*Cluster, error) {
	c := &Cluster{recordTimeout: cmp.Or(nodes.RecordTimeout, DefaultTimeout)}
	reached := reach(ctx, nodes)
	var problems []error
	for _, r := range reached {
		if r.conn != nil {
			c.conns = append(c.conns, namedConn{r.Name, r.conn})
		}
		if applied, ok := r.KnownGeneration(); ok {
			c.Latest = max(c.Latest, applied)
		}
		err := check(r)
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
			problems = append(problems, fmt.Errorf("%s: %w", r.Name, err))
		}
	}
	if len(problems) > 0 {
		c.Close()
		return nil, &PreflightError{Problems: problems}
	}
	if force {
		c.Unlisted = unlisted(reached, nodes.Names, c.Latest)
	}
	return c, nil
}

// check fails if the reached node r can't be operated, whatever the
// generation.
func check(r Reached) error {
	switch {
	case r.Err != nil:
		// The error says what failed: reaching the node, or its host key.
		return r.Err
	case r.Status.AgentErr != nil:
		return fmt.Errorf("the agent doesn't answer: %w", r.Status.AgentErr)
	case r.Status.UpdaterErr != nil:
		return fmt.Errorf("the updater doesn't answer: %w", r.Status.UpdaterErr)
	case r.Status.Mode == nodev1.Mode_MODE_CONTROL_PLANE:
		return errors.New("it is in control plane mode")
	case r.Status.Mode != nodev1.Mode_MODE_DIRECT:
		return fmt.Errorf("it is in mode %s, not direct mode", r.Status.Mode)
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

// PreflightError says which nodes failed the preflight, and why.
type PreflightError struct {
	// Problems are one error per node, which names it.
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

// Add adds the connection to a new node called name, which Close closes.
func (c *Cluster) Add(name string, conn Node) {
	c.conns = append(c.conns, namedConn{name, conn})
}

// Record has every node record generation, of a cluster directory of
// every node the cluster has a connection to, as the one it has applied,
// all at once, each within the record timeout. It fails naming every
// node that didn't.
func (c *Cluster) Record(ctx context.Context, generation int64) error {
	names := make([]string, len(c.conns))
	for i, n := range c.conns {
		names[i] = n.name
	}
	slices.Sort(names)
	errs := make([]error, len(c.conns))
	var wg sync.WaitGroup
	for i, n := range c.conns {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(ctx, c.recordTimeout)
			defer cancel()
			if err := n.conn.RecordGeneration(ctx, generation, names); err != nil {
				errs[i] = fmt.Errorf("%s didn't record generation %d: %w", n.name, generation, err)
			}
		})
	}
	wg.Wait()
	return errors.Join(slices.DeleteFunc(errs, func(err error) bool { return err == nil })...)
}

// Close closes the connections to the nodes.
func (c *Cluster) Close() error {
	var errs []error
	for _, n := range c.conns {
		errs = append(errs, n.conn.Close())
	}
	return errors.Join(errs...)
}
