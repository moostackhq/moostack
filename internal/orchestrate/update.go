package orchestrate

import (
	"context"
	"fmt"
	"slices"
	"time"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/nodeclient"
)

// NodeComponents are the node components that an update installs on
// each node, in the order it installs them: the agent, then the updater,
// which the new agent swaps, then containerd, which it restarts.
var NodeComponents = []string{"agent", "updater", "containerd"}

// Component is a node component as an update installs it: its version,
// and its files by name. The agent and the updater are a file each,
// named for them.
type Component struct {
	Version string
	Files   map[string][]byte
}

// NodeRelease is what an update installs on one node: a release's node
// components, for the node's architecture, by name.
type NodeRelease map[string]Component

// ReleaseOf is what an update installs on the node called name.
type ReleaseOf func(name string) (NodeRelease, error)

// Step is a step of a node's update, as UpdateNodes tells it.
type Step struct {
	// Node is the node's name, and Component the one the step updates,
	// as "the agent", "the updater" or "containerd", to Version.
	Node, Component, Version string
	// Already says the node runs Component at Version already, healthy,
	// so the step is skipped.
	Already bool
}

// updateTimeout bounds each step of updating one node: sending the
// binaries, and waiting for the new component, or the one it replaced,
// to become healthy.
const updateTimeout = 5 * time.Minute

// UpdateNodes updates every node the cluster has a connection to, one
// node at a time, in the order the preflight reached them, to what
// releaseOf gives it: on each node each of NodeComponents in turn. It
// skips the agent and the updater where the node runs them at the target
// already, healthy, so that a rerun reaches containerd on a node whose
// containerd doesn't answer, where a new agent would be refused. told is
// told each step as it starts or is skipped. Each new component has a
// fifth of a lease time to restart and become healthy, and the one it
// replaced as long again if it is rolled back. The last renewal the
// node answered may be a third of a lease time old when its agent stops
// answering, and the operation gives up a tenth early, so both end
// before it may lose the lock. Once a node's agent has restarted, the
// lease is renewed on it. UpdateNodes stops at the first step that
// fails, with a *NodeError naming the node and the step, and, before or
// after any step, once the operation can't be sure it holds the lock.
func (c *Cluster) UpdateNodes(ctx context.Context, releaseOf ReleaseOf, told func(Step)) error {
	c.mu.Lock()
	conns := slices.Clone(c.conns)
	c.mu.Unlock()
	healthTimeout := c.ttl / 5
	for _, n := range conns {
		rel, err := releaseOf(n.name)
		if err != nil {
			return &NodeError{Node: n.name, Err: err}
		}
		statusCtx, cancel := context.WithTimeout(ctx, c.reachTimeout)
		running := runningHealthy(n.conn.Status(statusCtx))
		cancel()
		update := map[string]func(ctx context.Context, c Component) error{
			"agent": func(ctx context.Context, comp Component) error {
				err := n.conn.UpdateAgent(ctx, comp.Version, comp.Files["agent"], healthTimeout)
				// The agent restarted, whether the update succeeded or
				// rolled back.
				renewErr := c.renewRestarted(ctx, n)
				if err == nil && renewErr != nil {
					return &unansweredError{renewErr}
				}
				return err
			},
			"updater": func(ctx context.Context, comp Component) error {
				return n.conn.UpdateUpdater(ctx, comp.Version, comp.Files["updater"], healthTimeout)
			},
			"containerd": func(ctx context.Context, comp Component) error {
				return n.conn.UpdateContainerd(ctx, comp.Version, comp.Files, healthTimeout)
			},
		}
		for _, name := range NodeComponents {
			comp := rel[name]
			step := Step{Node: n.name, Component: ComponentNoun(name), Version: comp.Version}
			// The lock doesn't fence the update, and keepLease may not
			// have noticed yet that the lease ran out, as after the
			// machine slept.
			if err := c.CheckLock(); err != nil {
				return err
			}
			// containerd's step runs every time, so that it puts binaries
			// that are part old and part new in place again.
			if v, ok := running[name]; ok && v == comp.Version && name != "containerd" {
				step.Already = true
				told(step)
				continue
			}
			if name != "containerd" && comp.Files[name] == nil {
				return &NodeError{Node: n.name, Err: fmt.Errorf("the release has no binary of %s", step.Component)}
			}
			told(step)
			stepCtx, cancel := context.WithTimeout(ctx, updateTimeout)
			err := update[name](stepCtx, comp)
			cancel()
			if r, ok := err.(*unansweredError); ok {
				return &NodeError{Node: n.name, Err: fmt.Errorf("%s updated to %s doesn't answer mooctl: %w", step.Component, step.Version, r.err)}
			}
			if err != nil && ctx.Err() != nil {
				return &NodeError{Node: n.name, Err: fmt.Errorf("mooctl stopped waiting for the update of %s to %s, which goes on on the node: %w", step.Component, step.Version, context.Cause(ctx))}
			}
			if err != nil {
				return &NodeError{Node: n.name, Err: fmt.Errorf("updating %s to %s: %w", step.Component, step.Version, err)}
			}
		}
		// The renewals may have found the lease lost, on this node or on
		// another, the last one too.
		if err := c.CheckLock(); err != nil {
			return err
		}
	}
	return nil
}

// ComponentNoun is the node component called name as a sentence says
// it: "the agent", "the updater" or "containerd".
func ComponentNoun(name string) string {
	if name == "containerd" {
		return name
	}
	return "the " + name
}

// runningHealthy are the versions of the node components that status
// reports healthy, by name.
func runningHealthy(status nodeclient.Status) map[string]string {
	running := map[string]string{}
	for _, c := range status.Components {
		if c.Health == nodev1.Health_HEALTH_HEALTHY {
			running[c.Name] = c.Version
		}
	}
	return running
}

// unansweredError is why the agent that an update restarted doesn't
// answer mooctl, when the update itself succeeded.
type unansweredError struct{ err error }

func (e *unansweredError) Error() string { return e.err.Error() }

// renewRestarted renews the lease on n, whose agent has just restarted,
// trying again until it answers, for up to a third of a lease time.
func (c *Cluster) renewRestarted(ctx context.Context, n *namedConn) error {
	ctx, cancel := context.WithTimeout(ctx, c.ttl/3)
	defer cancel()
	for {
		err := c.renew(ctx, n)
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(250 * time.Millisecond):
		}
	}
}
