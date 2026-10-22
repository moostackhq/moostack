package cli_test

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/buildid"
	"github.com/moostackhq/moostack/internal/cluster"
	"github.com/moostackhq/moostack/internal/nodeclient"
	"github.com/moostackhq/moostack/internal/ociimage"
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
	// updating, if set, is called as an update of the agent starts, with
	// the call's context.
	updating func(ctx context.Context)
	// updateErr, if set, is why updating the agent fails, and
	// serviceErr why running a service does.
	updateErr, serviceErr error
	// updates are the components the node was updated to, as the
	// component, its version and its binaries, in order.
	updates []string
	// sent are the last binary of the agent and of the updater the node
	// was sent, and the last archive of each image it imported, by the
	// image's name.
	sent map[string][]byte
}

// sentFile is the last binary of the agent or the updater, or archive of
// the image, called name that the node was sent.
func (n *fakeNode) sentFile(name string) []byte {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.sent[name]
}

// keep keeps data as the last file called name the node was sent. The
// node's mu is held.
func (n *fakeNode) keep(name string, data []byte) {
	if n.sent == nil {
		n.sent = map[string][]byte{}
	}
	n.sent[name] = data
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

// healthyNode is a node in direct mode that has applied generation,
// whose agent runs release 1.0.0, statusCluster's.
func healthyNode(generation int64) *fakeNode {
	return &fakeNode{status: nodeclient.Status{Mode: nodev1.Mode_MODE_DIRECT, Generation: generation, Components: []*nodev1.Component{
		{Name: "agent", Version: "1.0.0", Health: nodev1.Health_HEALTH_HEALTHY},
	}}}
}

// generationUnknown is healthyNode, but it can't tell its generation,
// for problem.
func generationUnknown(problem string) *fakeNode {
	n := healthyNode(0)
	n.status.GenerationUnknown = problem
	return n
}

// runs has the node report that it runs each of components at version,
// healthy.
func (n *fakeNode) runs(version string, components ...string) *fakeNode {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, name := range components {
		n.ran(name, version)
	}
	return n
}

// ran has the node report that it runs component at version, healthy.
// The node's mu is held.
func (n *fakeNode) ran(component, version string) { n.ranBuild(component, version, "") }

// ranBuild is ran, of the build build, unknown if empty. The node's mu is
// held.
func (n *fakeNode) ranBuild(component, version, build string) {
	n.status.Components = slices.DeleteFunc(n.status.Components, func(c *nodev1.Component) bool { return c.Name == component })
	n.status.Components = append(n.status.Components, &nodev1.Component{Name: component, Version: version, Build: build, Health: nodev1.Health_HEALTH_HEALTHY})
}

// buildOf is the build of the agent's or the updater's binary, as the
// node reports it: by its checksum, as a release's SHA256SUMS gives it.
func buildOf(name string, binary []byte) string {
	sum := sha256.Sum256(binary)
	return buildid.FromSums(map[string]string{name: hex.EncodeToString(sum[:])})
}

// runsServices has the node report that it runs each of services at
// version, healthy.
func (n *fakeNode) runsServices(version string, services ...string) *fakeNode {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, name := range services {
		n.status.Services = slices.DeleteFunc(n.status.Services, func(c *nodev1.Component) bool { return c.Name == name })
		n.status.Services = append(n.status.Services, &nodev1.Component{Name: name, Version: version, Health: nodev1.Health_HEALTH_HEALTHY})
	}
	return n
}

// generations are the generations recorded on the node so far.
func (n *fakeNode) generations() []int64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return slices.Clone(n.recorded)
}

func (n *fakeNode) Status(context.Context) nodeclient.Status {
	n.mu.Lock()
	s := n.status
	s.Components = slices.Clone(s.Components)
	s.Services = slices.Clone(s.Services)
	n.mu.Unlock()
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

func (n *fakeNode) UpdateAgent(ctx context.Context, version string, binary []byte, _ time.Duration) error {
	if n.updating != nil {
		n.updating(ctx)
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.updates = append(n.updates, "agent "+version+" "+string(binary))
	n.keep("agent", binary)
	if n.updateErr == nil {
		n.ranBuild("agent", version, buildOf("agent", binary))
	}
	return n.updateErr
}

func (n *fakeNode) UpdateUpdater(_ context.Context, version string, binary []byte, _ time.Duration) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.updates = append(n.updates, "updater "+version+" "+string(binary))
	n.keep("updater", binary)
	n.ranBuild("updater", version, buildOf("updater", binary))
	return nil
}

func (n *fakeNode) UpdateContainerd(_ context.Context, version string, files map[string][]byte, _ time.Duration) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	update := "containerd " + version
	for _, name := range slices.Sorted(maps.Keys(files)) {
		update += " " + name + ": " + string(files[name])
	}
	n.updates = append(n.updates, update)
	n.ran("containerd", version)
	return nil
}

// ImportImage imports the image in the OCI image layout archive, as the
// name its index gives it.
func (n *fakeNode) ImportImage(_ context.Context, archive []byte) ([]string, error) {
	r := tar.NewReader(bytes.NewReader(archive))
	for {
		h, err := r.Next()
		if err != nil {
			return nil, fmt.Errorf("not an image: %v", err)
		}
		if h.Name != "index.json" {
			continue
		}
		var index struct {
			Manifests []struct{ Annotations map[string]string }
		}
		if err := json.NewDecoder(r).Decode(&index); err != nil || len(index.Manifests) != 1 {
			return nil, fmt.Errorf("not an image: %v", err)
		}
		name := index.Manifests[0].Annotations["io.containerd.image.name"]
		n.mu.Lock()
		n.keep(name, archive)
		n.mu.Unlock()
		return []string{name}, nil
	}
}

// RunService records an update to the service, as "service", its name,
// version and image, and has the node report it running at its version,
// healthy, of the build of the image it imported under that name, unless
// serviceErr is set. Successful updates of components show in the node's
// status too, with the build of their binaries.
func (n *fakeNode) RunService(_ context.Context, svc *nodev1.Service) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.updates = append(n.updates, "service "+svc.Name+" "+svc.Version+" "+svc.Image)
	if n.serviceErr != nil {
		return n.serviceErr
	}
	n.status.Services = slices.DeleteFunc(n.status.Services, func(c *nodev1.Component) bool { return c.Name == svc.Name })
	build, _ := ociimage.Digest(n.sent[svc.Image])
	n.status.Services = append(n.status.Services, &nodev1.Component{Name: svc.Name, Version: svc.Version, Build: build, Health: nodev1.Health_HEALTH_HEALTHY})
	return nil
}

// nodeUpdates are the components the node was updated to so far.
func (n *fakeNode) nodeUpdates() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return slices.Clone(n.updates)
}

// agentUpdates are the agents the node was updated to so far.
func (n *fakeNode) agentUpdates() []string {
	var agents []string
	for _, u := range n.nodeUpdates() {
		if a, ok := strings.CutPrefix(u, "agent "); ok {
			agents = append(agents, a)
		}
	}
	return agents
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
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		if f.err != nil {
			return nil, f.err
		}
		return f, nil
	}
}
