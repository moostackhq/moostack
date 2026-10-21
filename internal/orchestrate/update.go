package orchestrate

import (
	"context"
	"fmt"
	"slices"
	"time"
)

// AgentOf is the binary of the agent that an update installs on the node
// called name, such as the one for its architecture.
type AgentOf func(name string) (binary []byte, err error)

// updateTimeout bounds updating one node's agent: sending the binary,
// and waiting for the new agent, or the one it replaced, to become
// healthy.
const updateTimeout = 5 * time.Minute

// UpdateAgents updates the agent of every node the cluster has a
// connection to, one node at a time, in the order the preflight reached
// them, to the agent at version whose binary agentOf gives it. updating
// is told each node as its update starts. The new agent has a fifth of a
// lease time to restart and become healthy, and the one it replaced as
// long again if the updater rolls back. The last renewal the node
// answered may be a third of a lease time old when its agent stops
// answering, and the operation gives up a tenth early, so both end
// before it may lose the lock. Once a node's agent has restarted, the
// lease is renewed on it. UpdateAgents stops at the first node that
// fails, with a *NodeError naming it, and, before or after any node's
// update, once the operation can't be sure it holds the lock.
func (c *Cluster) UpdateAgents(ctx context.Context, version string, agentOf AgentOf, updating func(name string)) error {
	c.mu.Lock()
	conns := slices.Clone(c.conns)
	c.mu.Unlock()
	for _, n := range conns {
		binary, err := agentOf(n.name)
		if err != nil {
			return &NodeError{Node: n.name, Err: err}
		}
		// The lock doesn't fence the update, and keepLease may not have
		// noticed yet that the lease ran out, as after the machine slept.
		if err := c.CheckLock(); err != nil {
			return err
		}
		updating(n.name)
		updateCtx, cancel := context.WithTimeout(ctx, updateTimeout)
		err = n.conn.UpdateAgent(updateCtx, version, binary, c.ttl/5)
		cancel()
		// The agent restarted, whether the update succeeded or rolled
		// back.
		renewErr := c.renewRestarted(ctx, n)
		if err != nil && ctx.Err() != nil {
			return &NodeError{Node: n.name, Err: fmt.Errorf("mooctl stopped waiting for the agent's update to %s, which goes on on the node: %w", version, context.Cause(ctx))}
		}
		if err != nil {
			return &NodeError{Node: n.name, Err: fmt.Errorf("updating the agent to %s: %w", version, err)}
		}
		if renewErr != nil {
			return &NodeError{Node: n.name, Err: fmt.Errorf("the agent updated to %s doesn't answer mooctl: %w", version, renewErr)}
		}
		// The renewal may have found the lease lost, on this node or on
		// another, the last one too.
		if err := c.CheckLock(); err != nil {
			return err
		}
	}
	return nil
}

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
