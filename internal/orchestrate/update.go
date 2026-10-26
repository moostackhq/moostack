package orchestrate

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
)

// NodeComponents are the node components that an update installs on
// each node, in the order it installs them: the agent, then the updater,
// which the new agent swaps, then containerd, which it restarts.
var NodeComponents = []string{"agent", "updater", "containerd"}

// Component is a node component as an update installs it: its version
// and build, as node status says them, and its files by name. The agent
// and the updater are a file each, named for them.
type Component struct {
	Version, Build string
	Files          map[string][]byte
	// ManyBuilds says Version names more than one build, as a
	// development build's does: the same commit built with other
	// settings, or with other changes, has the same version. A node that
	// doesn't say which build it runs isn't taken to run this one.
	ManyBuilds bool
}

// Service is a cluster service as an update runs it: its version and
// build, as node status says them, the container it runs as, and its
// image, an OCI image layout archive, which imports as the container's
// image.
type Service struct {
	Name, Version, Build string
	Container            *nodev1.Container
	Archive              []byte
	// Replaces is the name of the container a release before ran the
	// service as, which the update removes before it runs Container, since
	// no two containers serve one API, empty if none, and ReplacesImages
	// what the name of an image of the service starts with: a container of
	// that name running any other image isn't the service's, and stays.
	//
	// Temporary: they are the service's name and its image's repository,
	// as releases before the service- names ran it, until no release a
	// cluster updates from does.
	Replaces, ReplacesImages string
	// ManyBuilds is Component's.
	ManyBuilds bool
}

// NodeRelease is what an update installs on one node: a release's node
// components, for the node's architecture, by name, and its cluster
// services, in the order the update runs them. An update installs only
// the components and services it has, as a development build of some.
type NodeRelease struct {
	Components map[string]Component
	Services   []Service
}

// ReleaseOf is what an update installs on the node called name.
type ReleaseOf func(name string) (NodeRelease, error)

// Step is a step of a node's update, as Update tells it.
type Step struct {
	// Node is the node's name, and Component the node component or
	// cluster service the step updates, as "the agent", "the updater",
	// "containerd" or "the dns service", to Version.
	Node, Component, Version string
	// Already says the node runs Component at Version already, healthy,
	// so the step is skipped.
	Already bool
}

// updateTimeout bounds each step of updating one node: sending the
// binaries or the image, and waiting for the new component or service,
// or the component it replaced, to become healthy.
var updateTimeout = 5 * time.Minute

// Update updates every node the cluster has a connection to, in the
// order the preflight reached them, to what releaseOf gives it: first
// each node in turn, each of NodeComponents in turn, then, once every
// node's are done, each node in turn, each of its cluster services in
// turn, so that the services always run on updated agents. It skips a
// step where the node runs the component or service at the target
// already, healthy, and of the target's build when both say which, so
// that a rerun goes on from where an update stopped, an update of a
// cluster that is up to date changes nothing, and another build of the
// target version is replaced. told is told each step as it starts or is
// skipped. Update asks releaseOf for every node's release before any
// step, and fails naming the first node it has none for. It stops at the
// first step that fails, with a *NodeError naming the node and the step,
// and, before or after any step, once the operation can't be sure it
// holds the lock.
func (c *Cluster) Update(ctx context.Context, releaseOf ReleaseOf, told func(Step)) error {
	c.mu.Lock()
	conns := slices.Clone(c.conns)
	c.mu.Unlock()
	rels := map[string]NodeRelease{}
	for _, n := range conns {
		rel, err := releaseOf(n.name)
		if err != nil {
			return &NodeError{Node: n.name, Err: err}
		}
		rels[n.name] = rel
	}
	for _, n := range conns {
		if err := c.updateComponents(ctx, n, rels[n.name], told); err != nil {
			return err
		}
	}
	for _, n := range conns {
		if err := c.updateServices(ctx, n, rels[n.name], told); err != nil {
			return err
		}
	}
	return nil
}

// updateComponents updates the node components of n, each of
// NodeComponents that rel has in turn. It skips the agent and
// the updater where the node runs them at the target already, healthy,
// so that a rerun reaches containerd on a node whose containerd doesn't
// answer, where a new agent would be refused. Each new component has a fifth of a lease time to
// restart and become healthy, and the one it replaced as long again if
// it is rolled back. The last renewal the node answered may be a third
// of a lease time old when its agent stops answering, and the operation
// gives up a tenth early, so both end before it may lose the lock. Once
// the node's agent has restarted, the lease is renewed on it.
func (c *Cluster) updateComponents(ctx context.Context, n *namedConn, rel NodeRelease, told func(Step)) error {
	healthTimeout := c.ttl / 5
	statusCtx, cancel := context.WithTimeout(ctx, c.reachTimeout)
	running := runningHealthy(n.conn.Status(statusCtx).Components)
	cancel()
	update := map[string]func(ctx context.Context, c Component) error{
		"agent": func(stepCtx context.Context, comp Component) error {
			err := n.conn.UpdateAgent(stepCtx, comp.Version, comp.Files["agent"], healthTimeout)
			// The agent restarted, whether the update succeeded or
			// rolled back. The renewal has its own time, not what is
			// left of the step's.
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
		comp, ok := rel.Components[name]
		if !ok {
			continue
		}
		step := Step{Node: n.name, Component: ComponentNoun(name), Version: comp.Version}
		// The lock doesn't fence the update, and keepLease may not have
		// noticed yet that the lease ran out, as after the machine slept.
		if err := c.CheckLock(); err != nil {
			return err
		}
		// containerd reports itself healthy only with its shim at its
		// version, so binaries part old and part new are updated again.
		if atTarget(running[name], comp.Version, comp.Build, comp.ManyBuilds) {
			step.Already = true
			told(step)
			continue
		}
		if name != "containerd" && comp.Files[name] == nil {
			return &NodeError{Node: n.name, Err: fmt.Errorf("the release has no binary of %s", step.Component)}
		}
		told(step)
		if err := c.runStep(ctx, n, step, func(ctx context.Context) error { return update[name](ctx, comp) }); err != nil {
			return err
		}
	}
	// The renewals may have found the lease lost, on this node or on
	// another, the last one too.
	return c.CheckLock()
}

// updateServices updates the cluster services of n that rel has, each in
// turn: it imports the service's image, removes the container the
// service replaces if the node has it running one of the service's
// images, runs the service from the image, and waits for the service to
// answer its health call at the new version, for up to the cluster's
// service timeout. It skips a service the node runs at the target
// already, healthy.
func (c *Cluster) updateServices(ctx context.Context, n *namedConn, rel NodeRelease, told func(Step)) error {
	statusCtx, cancel := context.WithTimeout(ctx, c.reachTimeout)
	reported := n.conn.Status(statusCtx)
	cancel()
	running := runningHealthy(reported.Services())
	for _, svc := range rel.Services {
		step := Step{Node: n.name, Component: "the " + svc.Name + " service", Version: svc.Version}
		if err := c.CheckLock(); err != nil {
			return err
		}
		// The node runs no container in place of one it has at the target,
		// since the agent refuses a container whose API another serves.
		if atTarget(running[svc.Name], svc.Version, svc.Build, svc.ManyBuilds) {
			step.Already = true
			told(step)
			continue
		}
		replaced := svc.Replaces != "" && slices.ContainsFunc(reported.Containers, func(k *nodev1.ContainerStatus) bool {
			return k.Name == svc.Replaces && len(k.Image) > len(svc.ReplacesImages) && strings.HasPrefix(k.Image, svc.ReplacesImages)
		})
		told(step)
		if err := c.runStep(ctx, n, step, func(ctx context.Context) error { return c.updateService(ctx, n, svc, replaced) }); err != nil {
			return err
		}
	}
	return c.CheckLock()
}

// updateService imports svc's image on n, removes the container svc
// replaces if replaced says n has it, and runs svc from the image, once
// sure the operation still holds the lock, and waits for it to answer its
// health call at its version. The image is imported before the removal,
// so that one that won't import leaves the old container running, and
// again after it, since the removal deletes the old container's image,
// which is the new one's at the same version.
func (c *Cluster) updateService(ctx context.Context, n *namedConn, svc Service, replaced bool) error {
	if err := c.importImage(ctx, n, svc); err != nil {
		return err
	}
	// The import may have outlasted the lease, as when the machine slept,
	// and running the service replaces the one that runs.
	if err := c.CheckLock(); err != nil {
		return err
	}
	runCtx := ctx
	if replaced {
		// Once the old container is to go, the removal, the import and the
		// run are sent even if mooctl is stopped meanwhile, so that the
		// node isn't left without the service, as the agent finishes each
		// once it has the call.
		// They keep the step's deadline, as the rest of the step does.
		deadline, ok := ctx.Deadline()
		if !ok {
			deadline = time.Now().Add(updateTimeout)
		}
		var cancel context.CancelFunc
		runCtx, cancel = context.WithDeadline(context.WithoutCancel(ctx), deadline)
		defer cancel()
		if _, err := n.conn.Remove(runCtx, svc.Replaces, c.lease.Lease); err != nil {
			return fmt.Errorf("removing its container %s: %w", svc.Replaces, err)
		}
		if err := c.importImage(runCtx, n, svc); err != nil {
			return err
		}
	}
	if _, err := n.conn.Run(runCtx, svc.Container, c.lease.Lease); err != nil {
		return err
	}
	stepCtx := ctx
	ctx, cancel := context.WithTimeout(ctx, c.serviceTimeout)
	defer cancel()
	for {
		statusCtx, cancelStatus := context.WithTimeout(ctx, c.reachTimeout)
		reported := n.conn.Status(statusCtx)
		cancelStatus()
		if atTarget(runningHealthy(reported.Services())[svc.Name], svc.Version, svc.Build, svc.ManyBuilds) {
			return nil
		}
		select {
		case <-ctx.Done():
			// The operation stopped or lost its lock, rather than the
			// service's time ran out.
			if !errors.Is(context.Cause(ctx), context.DeadlineExceeded) {
				return ctx.Err()
			}
			// The step's time can run out before the service's does.
			if stepCtx.Err() != nil {
				return fmt.Errorf("it didn't become healthy within the step's %s", updateTimeout)
			}
			return fmt.Errorf("it didn't become healthy within %s", c.serviceTimeout)
		case <-time.After(250 * time.Millisecond):
		}
	}
}

// importImage imports svc's image on n, and fails unless it imports as
// the image svc's container runs.
func (c *Cluster) importImage(ctx context.Context, n *namedConn, svc Service) error {
	images, err := n.conn.ImportImage(ctx, bytes.NewReader(svc.Archive), c.lease.Lease)
	if err != nil {
		return err
	}
	if !slices.Contains(images, svc.Container.Image) {
		return fmt.Errorf("its image imported %s, not %s", strings.Join(images, ", "), svc.Container.Image)
	}
	return nil
}

// runStep runs the step of n, update, within updateTimeout, and says why
// it failed, naming the node and the step.
func (c *Cluster) runStep(ctx context.Context, n *namedConn, step Step, update func(ctx context.Context) error) error {
	stepCtx, cancel := context.WithTimeout(ctx, updateTimeout)
	err := update(stepCtx)
	cancel()
	// The operation ending, as when it is stopped or its lock lost, ends
	// a renewal on the restarted agent too, which is no fault of the agent.
	// A step that failed of itself meanwhile says why.
	var unanswered *unansweredError
	if err != nil && ctx.Err() != nil && (ended(err) || errors.As(err, &unanswered)) {
		return &NodeError{Node: n.name, Err: fmt.Errorf("mooctl stopped waiting for the update of %s to %s, which goes on on the node: %w", step.Component, step.Version, context.Cause(ctx))}
	}
	if r, ok := err.(*unansweredError); ok {
		return &NodeError{Node: n.name, Err: fmt.Errorf("%s updated to %s doesn't answer mooctl: %w", step.Component, step.Version, r.err)}
	}
	if err != nil {
		return &NodeError{Node: n.name, Err: fmt.Errorf("updating %s to %s: %w", step.Component, step.Version, err)}
	}
	return nil
}

// ended reports whether err is a call's or a wait's for want of time or
// for being stopped, rather than a failure of what it waited for.
func ended(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	code := status.Code(err)
	return code == codes.Canceled || code == codes.DeadlineExceeded
}

// ComponentNoun is the node component called name as a sentence says
// it: "the agent", "the updater" or "containerd".
func ComponentNoun(name string) string {
	if name == "containerd" {
		return name
	}
	return "the " + name
}

// runningHealthy are the components or services that a node reports
// healthy, by name.
func runningHealthy(reported []*nodev1.Component) map[string]*nodev1.Component {
	running := map[string]*nodev1.Component{}
	for _, c := range reported {
		if c.Health == nodev1.Health_HEALTH_HEALTHY {
			running[c.Name] = c
		}
	}
	return running
}

// atTarget reports whether running, healthy, is the target: at version,
// and the build, unless the node or the release doesn't say which, as a
// release before builds were reported doesn't, or the node hasn't hashed
// its files yet. A version of many builds is the target only by a build
// the node says.
func atTarget(running *nodev1.Component, version, build string, manyBuilds bool) bool {
	if running == nil || running.Version != version {
		return false
	}
	if manyBuilds {
		return running.Build != "" && running.Build == build
	}
	return build == "" || running.Build == "" || running.Build == build
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
