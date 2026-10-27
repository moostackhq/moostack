package orchestrate

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"slices"
	"strings"
	"sync"
	"time"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/appfile"
	"github.com/moostackhq/moostack/internal/hostlayout"
	"github.com/moostackhq/moostack/internal/nodeclient"
	"github.com/moostackhq/moostack/internal/ociimage"
)

// DefaultStartCheck is how long a deploy waits once the app's container
// started before it checks that the container still runs, so that an app
// that crashes as it starts isn't taken for deployed.
const DefaultStartCheck = 3 * time.Second

// App is an app as a deploy runs it: its name, as its app file gives
// it, and the image it runs, a normalized reference: a public image, or
// a local build, tagged with it, if Archive is set.
type App struct {
	Name, Image string
	// Archive, if set, opens the OCI image layout archive of the local
	// build of Image for the node called node, which a deploy imports
	// there, in place of a pull, and closes once it read it.
	Archive func(node string) (io.ReadSeekCloser, error)
}

// Placement is the node an app is deployed to, and whether it was picked
// because it runs the app already, rather than for running the fewest
// apps.
type Placement struct {
	Node      string
	RunsThere bool
}

// Place picks the node for the app called app, from the containers each
// node runs, by the node's name: the node whose containers have the
// app's, or else the node with the fewest containers of apps, the first
// by name of those with as few. It fails with a *DuplicateAppError if
// more than one node runs the app.
//
// Temporary: placement is this naive, and the nodes are the only record
// of where an app runs, until a cluster data store, service discovery
// and telemetry let placement be real.
func Place(app string, containers map[string][]*nodev1.ContainerStatus) (Placement, error) {
	if len(containers) == 0 {
		return Placement{}, errors.New("the cluster has no nodes")
	}
	name := appfile.ContainerName(app)
	var running []string
	for node, cs := range containers {
		if containerNamed(cs, name) != nil {
			running = append(running, node)
		}
	}
	slices.Sort(running)
	switch len(running) {
	case 0:
	case 1:
		return Placement{Node: running[0], RunsThere: true}, nil
	default:
		return Placement{}, &DuplicateAppError{App: app, Nodes: running}
	}
	var best string
	fewest := -1
	for node, cs := range containers {
		apps := 0
		for _, c := range cs {
			if appfile.IsApp(c.Name) {
				apps++
			}
		}
		if fewest < 0 || apps < fewest || apps == fewest && node < best {
			best, fewest = node, apps
		}
	}
	return Placement{Node: best}, nil
}

// DuplicateAppError says the app runs on more than one node, Nodes, so a
// deploy can't tell which to replace, though a removal removes it from
// each.
type DuplicateAppError struct {
	App   string
	Nodes []string
}

func (e *DuplicateAppError) Error() string {
	return fmt.Sprintf("%s runs on %s, more than one node, so mooctl can't tell which to replace; nothing changed", e.App, joinNodes(e.Nodes, "and"))
}

// joinNodes is nodes, at least one, as a sentence lists them with conj:
// "node1", "node1 and node2", or "node1, node2 and node3".
func joinNodes(nodes []string, conj string) string {
	last := len(nodes) - 1
	if last == 0 {
		return nodes[0]
	}
	return strings.Join(nodes[:last], ", ") + " " + conj + " " + nodes[last]
}

// DeployStep is a step of a deploy, as Deploy tells it: the node it
// picked, and then, with Fetching, that the node starts to get the image:
// to pull it, or to import the local build.
type DeployStep struct {
	Placement
	Fetching bool
}

// Deployed is an app as a deploy left it: on Node, running Image.
// Unchanged says it ran there from the same build of Image already, so
// the deploy changed nothing.
type Deployed struct {
	Node, Image string
	Unchanged   bool
}

// PullError says the node called Node failed to pull Image, as Err says,
// whose code tells why, so that nothing changed. If the deploy was
// stopped, or lost the lock, meanwhile, Err is why, as the operation's
// context's cause says.
type PullError struct {
	Node, Image string
	Err         error
}

func (e *PullError) Error() string {
	return fmt.Sprintf("%s failed to pull %s: %v", e.Node, e.Image, e.Err)
}

func (e *PullError) Unwrap() error { return e.Err }

// ImportError says the node called Node failed to import Image, the
// app's local build, as Err says, so that nothing changed. If the deploy
// was stopped, or lost the lock, meanwhile, Err is why, as the
// operation's context's cause says.
type ImportError struct {
	Node, Image string
	Err         error
}

func (e *ImportError) Error() string {
	return fmt.Sprintf("%s failed to import %s: %v", e.Node, e.Image, e.Err)
}

func (e *ImportError) Unwrap() error { return e.Err }

// RunError says the node called Node failed to run the app, as Err says.
// Runs is what the node said ran of the app after the failure, and Image
// the image it runs, if it runs. Stopped says the deploy was stopped, or
// lost the lock, as Err then says, before the node answered, so that the
// run may or may not go on on the node, and what runs isn't known.
type RunError struct {
	Node, App string
	Err       error
	Runs      RunOutcome
	Image     string
	Stopped   bool
}

// RunOutcome is what runs of an app once its run failed.
type RunOutcome int

const (
	// RunUnknown is an app the node doesn't say of, or can't tell of.
	RunUnknown RunOutcome = iota
	// NotRunning is an app the node keeps no running container of.
	NotRunning
	// RunsAsBefore is the build of the image the app ran before the run,
	// as the one the run was to replace runs on when the new one can't be
	// made.
	RunsAsBefore
	// RunsAsAsked is the build the run asked for, as when the new
	// container started though the run's answer was lost.
	RunsAsAsked
	// RunsOther is a build the app neither ran before nor was asked for.
	RunsOther
)

func (e *RunError) Error() string {
	return fmt.Sprintf("%s failed to run %s: %v", e.Node, e.App, e.Err)
}

func (e *RunError) Unwrap() error { return e.Err }

// NotRunningError says the app's container doesn't run on the node called
// Node once the start check's time, Within, has passed: it exited with
// ExitCode, writing its output to Log on the node, if Exited, and
// otherwise Problem says why it doesn't run.
type NotRunningError struct {
	Node, App string
	Within    time.Duration
	Exited    bool
	ExitCode  uint32
	Log       string
	Problem   string
}

func (e *NotRunningError) Error() string {
	if e.Exited {
		return fmt.Sprintf("%s exited with code %d on %s within %s of starting; its output is in %s on %s", e.App, e.ExitCode, e.Node, e.Within, e.Log, e.Node)
	}
	return fmt.Sprintf("%s doesn't run on %s once started: %s", e.App, e.Node, e.Problem)
}

// UncheckedError says the app's container started on the node called
// Node, but the deploy couldn't check that it still runs, as Err says.
type UncheckedError struct {
	Node, App string
	Err       error
}

func (e *UncheckedError) Error() string {
	return fmt.Sprintf("%s started on %s, but mooctl couldn't check that it still runs: %v", e.App, e.Node, e.Err)
}

func (e *UncheckedError) Unwrap() error { return e.Err }

// Deploy deploys app to a node the cluster has a connection to, picked
// as Place picks it from what every node says it runs: the node pulls
// the app's image, and runs the app's container from it with only its
// name and image, replacing the one that runs; for a local build, the
// node imports the build's archive for it instead. Once the start
// check's time has passed, the container must still run. An app that
// runs there already from the same build of the image, which the pull or
// the archive tells, is left as it runs, and so is one whose run starts
// no process, without the start check. told is told the node picked,
// and then the pull or the import as it starts. A pull that fails fails
// with a *PullError, and an import with an *ImportError, before anything
// changed; a run with a *RunError; and an app that doesn't run once the
// start check's time has passed with a *NotRunningError. A deploy that
// can't check that the app runs, as one stopped meanwhile, fails with an
// *UncheckedError. It fails before the run once the operation can't be
// sure it holds the lock.
//
// Temporary: placement is naive, and the nodes are the only record of
// where an app runs, as Place says.
func (c *Cluster) Deploy(ctx context.Context, app App, told func(DeployStep)) (*Deployed, error) {
	conns, containers, err := c.containers(ctx)
	if err != nil {
		return nil, err
	}
	p, err := Place(app.Name, containers)
	if err != nil {
		return nil, err
	}
	told(DeployStep{Placement: p})
	n := conns[slices.IndexFunc(conns, func(n *namedConn) bool { return n.name == p.Node })]
	name := appfile.ContainerName(app.Name)
	before := containerNamed(containers[p.Node], name)
	image, err := c.fetch(ctx, n, p, app, told)
	if err != nil {
		return nil, err
	}
	deployed := &Deployed{Node: p.Node, Image: image.Name}
	// What runs once the node got the image decides, as the app may have
	// exited meanwhile; one the node can't tell of is run again. The build
	// decides, not the name of its image: one that runs the same build
	// under another name is run under the new name, which renames
	// it on the node. A run that starts no process changes nothing.
	if now := c.status(ctx, n); now.AgentErr == nil {
		if k := containerNamed(now.Containers, name); runsBuild(k, image.Name, image.Digest) {
			deployed.Unchanged = true
			return deployed, nil
		}
	}
	// The pull or the import may have outlasted the lease, and the run
	// replaces the app that runs.
	got := "pulled"
	if app.Archive != nil {
		got = "imported"
	}
	if err := c.CheckLock(); err != nil {
		return nil, fmt.Errorf("%w, once %s %s %s, so mooctl ran nothing; nothing changed", err, p.Node, got, image.Name)
	}
	// A deploy stopped once the node got the image sends no run.
	if cause := stopped(ctx); cause != nil {
		return nil, fmt.Errorf("mooctl stopped once %s %s %s, before it ran %s: %w; nothing changed", p.Node, got, image.Name, app.Name, cause)
	}
	started, err := c.run(ctx, n, p, app, image, before)
	if err != nil {
		return nil, err
	}
	if !started {
		deployed.Unchanged = true
		return deployed, nil
	}
	select {
	case <-time.After(c.startCheck):
	case <-ctx.Done():
		return nil, &UncheckedError{Node: p.Node, App: app.Name, Err: context.Cause(ctx)}
	}
	after := c.status(ctx, n)
	if after.AgentErr != nil {
		return nil, &UncheckedError{Node: p.Node, App: app.Name, Err: after.AgentErr}
	}
	k := containerNamed(after.Containers, name)
	switch {
	case k == nil:
		return nil, &NotRunningError{Node: p.Node, App: app.Name, Within: c.startCheck, Problem: "the node no longer keeps its container"}
	case running(k):
		return deployed, nil
	case k.State == nodev1.TaskState_TASK_STATE_EXITED:
		return nil, &NotRunningError{Node: p.Node, App: app.Name, Within: c.startCheck, Exited: true, ExitCode: k.ExitCode, Log: path.Join(hostlayout.ContainerLogDir, name+".log")}
	}
	return nil, &NotRunningError{Node: p.Node, App: app.Name, Within: c.startCheck, Problem: cmp.Or(k.Problem, "the node can't tell whether it runs")}
}

// fetch has n get app's image, as p placed it there, telling told as it
// starts: the node pulls a public image, and imports a local build's
// archive for it. It returns the image's name and digest, and fails, as
// nothing changed, with a *PullError or an *ImportError if the node
// fails to.
//
// Temporary: a local build reaches a node through ImportImage, until
// nodes pull images from the cluster's own registry.
func (c *Cluster) fetch(ctx context.Context, n *namedConn, p Placement, app App, told func(DeployStep)) (nodeclient.PulledImage, error) {
	if app.Archive == nil {
		told(DeployStep{Placement: p, Fetching: true})
		pulled, err := n.conn.PullImage(ctx, app.Image, c.lease.Lease)
		if err != nil {
			return pulled, &PullError{Node: p.Node, Image: app.Image, Err: cmp.Or(stopped(ctx), err)}
		}
		return pulled, nil
	}
	archive, err := app.Archive(p.Node)
	if err != nil {
		return nodeclient.PulledImage{}, fmt.Errorf("reading the build of %s for %s: %w; nothing changed", app.Name, p.Node, err)
	}
	defer archive.Close()
	// The archive's own digest is the build's, as the node keeps it. The
	// archive is read through, then streamed to the node from its start.
	digest, err := ociimage.ReadDigest(archive)
	if err == nil {
		_, err = archive.Seek(0, io.SeekStart)
	}
	if err != nil {
		return nodeclient.PulledImage{}, fmt.Errorf("reading the build of %s for %s: %w; nothing changed", app.Name, p.Node, err)
	}
	told(DeployStep{Placement: p, Fetching: true})
	names, err := n.conn.ImportImage(ctx, archive, c.lease.Lease)
	if err != nil {
		return nodeclient.PulledImage{}, &ImportError{Node: p.Node, Image: app.Image, Err: cmp.Or(stopped(ctx), err)}
	}
	if !slices.Contains(names, app.Image) {
		return nodeclient.PulledImage{}, &ImportError{Node: p.Node, Image: app.Image, Err: fmt.Errorf("the node imported %v, not the build", names)}
	}
	return nodeclient.PulledImage{Name: app.Image, Digest: digest}, nil
}

// containers are the connections to the cluster's nodes, sorted by
// name, and the containers each node says it runs, by its name. It fails
// as askable does; with a *NodeError, as nothing may change, if a node's
// agent doesn't say; and if ctx ended meanwhile, as what the nodes said
// then says nothing of them.
func (c *Cluster) containers(ctx context.Context) ([]*namedConn, map[string][]*nodev1.ContainerStatus, error) {
	if err := c.askable(ctx); err != nil {
		return nil, nil, err
	}
	conns := c.sortedConns()
	reported := make([]nodeclient.Status, len(conns))
	var wg sync.WaitGroup
	for i, n := range conns {
		wg.Go(func() { reported[i] = c.status(ctx, n) })
	}
	wg.Wait()
	if err := stoppedBeforeAnyChange(ctx); err != nil {
		return nil, nil, err
	}
	containers := map[string][]*nodev1.ContainerStatus{}
	for i, n := range conns {
		if err := reported[i].AgentErr; err != nil {
			return nil, nil, &NodeError{Node: n.name, Err: fmt.Errorf("its agent doesn't say which containers it runs, so nothing changed: %w", err)}
		}
		containers[n.name] = reported[i].Containers
	}
	return conns, containers, nil
}

// askable fails, before any node is asked about apps, if the latest
// generation lists nodes the cluster has no connection to, as --force
// goes on past, since what they run is no one's to tell, and if ctx
// ended.
func (c *Cluster) askable(ctx context.Context) error {
	if len(c.Unlisted) > 0 {
		return fmt.Errorf("generation %d lists %s, which this cluster directory doesn't, so mooctl can't ask them which apps they run; pull the latest cluster directory; nothing changed", c.Latest, strings.Join(c.Unlisted, ", "))
	}
	return stoppedBeforeAnyChange(ctx)
}

// stoppedBeforeAnyChange fails if the operation of ctx was stopped, or
// lost the lock, before it changed anything.
func stoppedBeforeAnyChange(ctx context.Context) error {
	if cause := stopped(ctx); cause != nil {
		return fmt.Errorf("mooctl stopped before it changed anything: %w", cause)
	}
	return nil
}

// sortedConns are the connections to the cluster's nodes, sorted by name.
func (c *Cluster) sortedConns() []*namedConn {
	c.mu.Lock()
	conns := slices.Clone(c.conns)
	c.mu.Unlock()
	slices.SortFunc(conns, func(a, b *namedConn) int { return strings.Compare(a.name, b.name) })
	return conns
}

// connNames are the names of the nodes of conns, in their order.
func connNames(conns []*namedConn) []string {
	var names []string
	for _, n := range conns {
		names = append(names, n.name)
	}
	return names
}

// RemoveError says the node called Node failed to remove the app called
// App, as Err says. Stopped says the removal was stopped, or lost the
// lock, as Err then says, before the node answered, so that the node's
// removal may or may not go on. Unasked are the nodes after Node, in
// order of their names, which weren't asked to remove the app.
type RemoveError struct {
	Node, App string
	Err       error
	Stopped   bool
	Unasked   []string
}

func (e *RemoveError) Error() string {
	return fmt.Sprintf("%s failed to remove %s: %v", e.Node, e.App, e.Err)
}

func (e *RemoveError) Unwrap() error { return e.Err }

// NotAskedError says the removal of the app called App was stopped, as
// Err says, before it asked the nodes Unasked, in order of their names,
// which may run it still.
type NotAskedError struct {
	App     string
	Unasked []string
	Err     error
}

func (e *NotAskedError) Error() string {
	return fmt.Sprintf("mooctl stopped before it asked %s to remove %s: %v; nothing changed there", joinNodes(e.Unasked, "or"), e.App, e.Err)
}

func (e *NotAskedError) Unwrap() error { return e.Err }

// RemoveApp has every node the cluster has a connection to remove the
// app called app, one node after another in order of their names,
// telling told each node that kept the app's container as it removes
// it. Every node is asked, not only those whose status lists the app, as
// a node answers once a run under way ended, such as one a deploy stopped
// before its answer left going on: what the node says it removed
// decides. The node deletes the container, its image unless another
// container uses it, and its log. It returns the nodes the app was
// removed from, none if no node kept it. It fails, before any node is
// asked, as askable does; with a *RemoveError if a node's removal fails,
// asking no node after it; and before a node's removal once the
// operation can't be sure it holds the lock, or was stopped.
//
// Temporary: the nodes are the only record of where an app runs, as
// Place says.
func (c *Cluster) RemoveApp(ctx context.Context, app string, told func(node string)) ([]string, error) {
	if err := c.askable(ctx); err != nil {
		return nil, err
	}
	name := appfile.ContainerName(app)
	var removedFrom []string
	conns := c.sortedConns()
	lockLost := func(err error) ([]string, error) {
		if len(removedFrom) == 0 {
			return nil, fmt.Errorf("%w, before mooctl removed %s from any node; nothing changed", err, app)
		}
		return removedFrom, fmt.Errorf("%w, so mooctl removed %s from no more nodes", err, app)
	}
	for i, n := range conns {
		if err := c.CheckLock(); err != nil {
			return lockLost(err)
		}
		if cause := stopped(ctx); cause != nil {
			// A lost lock stops the operation, and may have been lost since
			// the check above.
			if errors.Is(cause, ErrLockLost) {
				return lockLost(cause)
			}
			if err := c.CheckLock(); err != nil {
				return lockLost(err)
			}
			return removedFrom, &NotAskedError{App: app, Unasked: connNames(conns[i:]), Err: cause}
		}
		removed, err := n.conn.Remove(ctx, name, c.lease.Lease)
		if err != nil {
			re := &RemoveError{Node: n.name, App: app, Err: err, Unasked: connNames(conns[i+1:])}
			if cause := stopped(ctx); cause != nil {
				// A removal goes on once the agent has the call.
				re.Err, re.Stopped = cause, true
			}
			return removedFrom, re
		}
		if removed {
			removedFrom = append(removedFrom, n.name)
			told(n.name)
		}
	}
	return removedFrom, nil
}

// run has n run app's container, from image, as p placed it,
// and says whether the node started its process. It fails with a
// *RunError if the node can't, saying what runs then, against before,
// the container that ran before the deploy.
func (c *Cluster) run(ctx context.Context, n *namedConn, p Placement, app App, image nodeclient.PulledImage, before *nodev1.ContainerStatus) (bool, error) {
	name := appfile.ContainerName(app.Name)
	started, err := n.conn.Run(ctx, &nodev1.Container{Name: name, Image: image.Name}, c.lease.Lease)
	if err == nil {
		return started, nil
	}
	if cause := stopped(ctx); cause != nil {
		// A run may go on once the agent has the call, and what the node
		// says meanwhile may be from before it, or partway through it.
		return false, &RunError{Node: p.Node, App: app.Name, Err: cause, Stopped: true}
	}
	runErr := &RunError{Node: p.Node, App: app.Name, Err: err}
	if after := c.status(ctx, n); after.AgentErr == nil {
		runErr.Runs, runErr.Image = runOutcome(containerNamed(after.Containers, name), before, image)
	}
	return false, runErr
}

// runOutcome is what runs of an app whose container is k, as a node says
// once a run of the build image failed, and its image if it runs: a
// container the node can't tell the state of is unknown, and one that runs
// is the build it ran before, as before was, the one asked for, or another.
func runOutcome(k, before *nodev1.ContainerStatus, image nodeclient.PulledImage) (RunOutcome, string) {
	switch {
	case k != nil && k.State == nodev1.TaskState_TASK_STATE_UNSPECIFIED:
		return RunUnknown, ""
	case !running(k):
		return NotRunning, ""
	case runsBuild(k, image.Name, image.Digest):
		return RunsAsAsked, k.Image
	case before != nil && runsBuild(k, before.Image, before.Digest):
		return RunsAsBefore, k.Image
	}
	return RunsOther, k.Image
}

// status is what n says, within the reach timeout.
func (c *Cluster) status(ctx context.Context, n *namedConn) nodeclient.Status {
	ctx, cancel := context.WithTimeout(ctx, c.reachTimeout)
	defer cancel()
	return n.conn.Status(ctx)
}

// running reports whether the container k, nil if there is none, runs.
func running(k *nodev1.ContainerStatus) bool {
	return k != nil && k.State == nodev1.TaskState_TASK_STATE_RUNNING
}

// runsBuild reports whether the container k, nil if there is none, runs
// the build digest of image.
func runsBuild(k *nodev1.ContainerStatus, image, digest string) bool {
	return running(k) && k.Image == image && k.Digest == digest
}

// stopped is why the operation of ctx was stopped, or lost the lock, nil
// if it goes on.
func stopped(ctx context.Context) error {
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	return nil
}

// containerNamed is the container called name among cs, nil if none is.
func containerNamed(cs []*nodev1.ContainerStatus, name string) *nodev1.ContainerStatus {
	if i := slices.IndexFunc(cs, func(c *nodev1.ContainerStatus) bool { return c.Name == name }); i >= 0 {
		return cs[i]
	}
	return nil
}
