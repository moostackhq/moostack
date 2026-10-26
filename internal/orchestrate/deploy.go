package orchestrate

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"path"
	"slices"
	"strings"
	"sync"
	"time"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/appfile"
	"github.com/moostackhq/moostack/internal/hostlayout"
	"github.com/moostackhq/moostack/internal/nodeclient"
)

// DefaultStartCheck is how long a deploy waits once the app's container
// started before it checks that the container still runs, so that an app
// that crashes as it starts isn't taken for deployed.
const DefaultStartCheck = 3 * time.Second

// App is an app as a deploy runs it: its name, as its app file gives
// it, and the public image it runs, a normalized reference.
type App struct {
	Name, Image string
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
// deploy can't tell which to replace.
type DuplicateAppError struct {
	App   string
	Nodes []string
}

func (e *DuplicateAppError) Error() string {
	return fmt.Sprintf("%s runs on %s, more than one node, so mooctl can't tell which to replace; nothing changed", e.App, strings.Join(e.Nodes, " and "))
}

// DeployStep is a step of a deploy, as Deploy tells it: the node it
// picked, and then, with Pulling, that the node starts to pull the image.
type DeployStep struct {
	Placement
	Pulling bool
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
// name and image, replacing the one that runs. Once the start check's
// time has passed, the container must still run. An app that runs there
// already from the same build of the image, which the pull tells, is
// left as it runs, and so is one whose run starts no process, without
// the start check. told is told the node picked, and then the pull as it
// starts. A pull that fails fails with a *PullError, before anything
// changed; a run with a *RunError; and an app that doesn't run once the
// start check's time has passed with a *NotRunningError. A deploy that
// can't check that the app runs, as one stopped meanwhile, fails with an
// *UncheckedError. It fails before the run once the operation can't be
// sure it holds the lock.
//
// Temporary: placement is naive, and the nodes are the only record of
// where an app runs, as Place says.
func (c *Cluster) Deploy(ctx context.Context, app App, told func(DeployStep)) (*Deployed, error) {
	c.mu.Lock()
	conns := slices.Clone(c.conns)
	c.mu.Unlock()
	reported := make([]nodeclient.Status, len(conns))
	var wg sync.WaitGroup
	for i, n := range conns {
		wg.Go(func() { reported[i] = c.status(ctx, n) })
	}
	wg.Wait()
	// The statuses of a deploy stopped meanwhile say nothing of the nodes.
	if cause := stopped(ctx); cause != nil {
		return nil, fmt.Errorf("mooctl stopped before it changed anything: %w", cause)
	}
	containers := map[string][]*nodev1.ContainerStatus{}
	for i, n := range conns {
		if err := reported[i].AgentErr; err != nil {
			return nil, &NodeError{Node: n.name, Err: fmt.Errorf("its agent doesn't say which containers it runs, so nothing changed: %w", err)}
		}
		containers[n.name] = reported[i].Containers
	}
	p, err := Place(app.Name, containers)
	if err != nil {
		return nil, err
	}
	told(DeployStep{Placement: p})
	n := conns[slices.IndexFunc(conns, func(n *namedConn) bool { return n.name == p.Node })]
	name := appfile.ContainerName(app.Name)
	before := containerNamed(containers[p.Node], name)
	told(DeployStep{Placement: p, Pulling: true})
	pulled, err := n.conn.PullImage(ctx, app.Image, c.lease.Lease)
	if err != nil {
		return nil, &PullError{Node: p.Node, Image: app.Image, Err: cmp.Or(stopped(ctx), err)}
	}
	deployed := &Deployed{Node: p.Node, Image: pulled.Name}
	// What runs once the pull ended decides, as the app may have exited
	// while the node pulled; one the node can't tell of is run again. The
	// build decides, not the name of its image: one that runs the same
	// build under another name is run under the pulled name, which renames
	// it on the node. A run that starts no process changes nothing.
	if now := c.status(ctx, n); now.AgentErr == nil {
		if k := containerNamed(now.Containers, name); runsBuild(k, pulled.Name, pulled.Digest) {
			deployed.Unchanged = true
			return deployed, nil
		}
	}
	// The pull may have outlasted the lease, and the run replaces the app
	// that runs.
	if err := c.CheckLock(); err != nil {
		return nil, fmt.Errorf("%w, once %s pulled %s, so mooctl ran nothing; nothing changed", err, p.Node, pulled.Name)
	}
	// A deploy stopped once the pull ended sends no run.
	if cause := stopped(ctx); cause != nil {
		return nil, fmt.Errorf("mooctl stopped once %s pulled %s, before it ran %s: %w; nothing changed", p.Node, pulled.Name, app.Name, cause)
	}
	started, err := c.run(ctx, n, p, app, pulled, before)
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

// run has n run app's container, from the image pulled, as p placed it,
// and says whether the node started its process. It fails with a
// *RunError if the node can't, saying what runs then, against before,
// the container that ran before the deploy.
func (c *Cluster) run(ctx context.Context, n *namedConn, p Placement, app App, pulled nodeclient.PulledImage, before *nodev1.ContainerStatus) (bool, error) {
	name := appfile.ContainerName(app.Name)
	started, err := n.conn.Run(ctx, &nodev1.Container{Name: name, Image: pulled.Name}, c.lease.Lease)
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
		runErr.Runs, runErr.Image = runOutcome(containerNamed(after.Containers, name), before, pulled)
	}
	return false, runErr
}

// runOutcome is what runs of an app whose container is k, as a node says
// once a run of the build pulled failed, and its image if it runs: a
// container the node can't tell the state of is unknown, and one that runs
// is the build it ran before, as before was, the one pulled, or another.
func runOutcome(k, before *nodev1.ContainerStatus, pulled nodeclient.PulledImage) (RunOutcome, string) {
	switch {
	case k != nil && k.State == nodev1.TaskState_TASK_STATE_UNSPECIFIED:
		return RunUnknown, ""
	case !running(k):
		return NotRunning, ""
	case runsBuild(k, pulled.Name, pulled.Digest):
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
