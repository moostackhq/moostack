// Package orchestrator is the port to what runs a cluster's containers.
package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/netip"
	"strings"

	"github.com/moostackhq/moostack/internal/model"
)

// A call on an ended context fails and changes nothing.
type Orchestrator interface {
	// Check refuses, before anything is built, an app this orchestrator
	// can't run: a name it can't have, manifests it can't read.
	Check(app model.App) error
	// Deploy makes the cluster run app as given, removes what the app
	// no longer has, and returns once it runs. It calls step as it
	// starts each step.
	Deploy(ctx context.Context, app model.App, step func(Step)) error
	// Certificate waits until the host of the app's container has its
	// certificate. ErrNoApp if there is no such app.
	Certificate(ctx context.Context, app, container string) error
	// Remove removes the app. ErrNoApp if there is none.
	Remove(ctx context.Context, app string) error
	// Logs writes the last lines of each container of the app's
	// replicas to w, each line after its replica and container.
	// ErrNoApp if there is no such app, ErrNoReplicas if it has no
	// replicas.
	Logs(ctx context.Context, app string, o LogOptions, w io.Writer) error
	// Apps are the cluster's apps, ordered by name.
	Apps(ctx context.Context) ([]model.AppState, error)
	// Running are the cluster's running containers, ordered by app,
	// then instance, then container.
	Running(ctx context.Context) ([]model.Running, error)

	// Preflight reads what host is, and refuses one that can't be the
	// node asked for.
	Preflight(ctx context.Context, host Host, want NodeRequest) (model.Node, error)
	// AddNode makes host the node n and returns once the cluster has
	// it. It refuses, telling no step, a node under the name of one
	// the cluster has. It calls step as it starts each step: Installing, Starting,
	// WaitingForNode, and Uninstalling if it fails. If it fails,
	// nothing of it is left on host.
	AddNode(ctx context.Context, host Host, n model.Node, step func(Step)) error
	// RemoveNode moves what runs on the node elsewhere, uninstalls it
	// from host and removes it from the cluster; host is nil if there
	// is none to uninstall from. It calls step as it starts each step
	// it has to do: Draining, Uninstalling, Deleting. It fails with a
	// *VolumesError for a node that holds volumes.
	RemoveNode(ctx context.Context, name string, host Host, o RemoveOptions, step func(Step)) error
	// Uninstall removes what AddNode put on host.
	Uninstall(ctx context.Context, host Host, n model.Node) error
	// Nodes are the cluster's nodes, ordered by name.
	Nodes(ctx context.Context) ([]model.NodeState, error)

	// RunConsole runs the console from image and returns once it is
	// ready. ConsoleToken has it issue a login token.
	RunConsole(ctx context.Context, image string, addresses []netip.Addr) error
	ConsoleToken(ctx context.Context) (string, error)

	Can() Capabilities
}

// ErrNoApp is the error of an operation on an app the cluster doesn't
// have.
var ErrNoApp = errors.New("no such app")

// ErrNoReplicas is the error of Logs for an app that has no replicas.
var ErrNoReplicas = errors.New("the app has no replicas")

// Step is a step of an operation, which a backend tells the command as
// it starts it.
type Step int

const (
	// The steps of a deploy.

	// WaitingForCluster: the cluster's own services aren't ready.
	WaitingForCluster Step = iota
	Applying
	WaitingForReady

	// The steps of adding and removing a node.

	Installing
	// Starting: the host has what it downloads to be a node.
	Starting
	WaitingForNode
	Draining
	Uninstalling
	// Deleting: the node is removed from the cluster.
	Deleting
)

type LogOptions struct {
	// Tail is how many of each container's last lines to start with, or
	// -1 for all.
	Tail int
	// Follow keeps writing new lines until ctx ends or each log there
	// at the call does.
	Follow bool
}

// NodeRequest is the node a host is asked to be.
type NodeRequest struct {
	Name         string
	ControlPlane bool
	// PrivateAddress, if valid, is the address of the host to use as
	// the node's.
	PrivateAddress netip.Addr
}

type RemoveOptions struct {
	// LoseVolumes removes a node that holds volumes.
	LoseVolumes bool
	// Unreachable: the node's host can't be reached, and what runs on
	// it isn't moved elsewhere.
	Unreachable bool
}

// VolumesError is RemoveNode's refusal of a node that holds volumes,
// which go with it.
type VolumesError struct {
	Node    string
	Volumes []string
}

func (e *VolumesError) Error() string {
	return fmt.Sprintf("%s holds the volumes %s, which go with it", e.Node, strings.Join(e.Volumes, ", "))
}

// Host is a server a backend acts on, as root.
type Host interface {
	Run(ctx context.Context, cmd string, stdin io.Reader) (string, error)
	RunStoppable(ctx context.Context, cmd string) (string, error)
	Upload(ctx context.Context, file string, r io.Reader, mode fs.FileMode) error
	DialTCP(ctx context.Context, addr string) (net.Conn, error)
}

type Capabilities struct {
	// KubernetesObjects: an app may bring its own manifests or chart.
	KubernetesObjects bool
}
