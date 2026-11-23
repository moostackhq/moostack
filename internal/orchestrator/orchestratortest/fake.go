// Package orchestratortest holds a fake Orchestrator, and Contract,
// which tests an Orchestrator.
package orchestratortest

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"net"
	"net/netip"
	"reflect"
	"slices"
	"strings"
	"sync"

	"github.com/moostackhq/moostack/internal/model"
	"github.com/moostackhq/moostack/internal/orchestrator"
)

// Fake is an Orchestrator that keeps a cluster's apps and nodes in
// memory. Every app is ready as soon as it is deployed. A call on an
// ended context fails with the context's cause and changes nothing.
type Fake struct {
	// Own are the cluster's own running containers. Set it before the
	// first call.
	Own []model.Running
	// Logged is what Logs writes, by app, in place of a line for each
	// running container. Set it before the first call.
	Logged map[string]string

	mu       sync.Mutex
	apps     map[string]model.App
	nodes    map[string]model.Node
	console  string
	failures map[string]error
}

// Fail makes the method named method fail with err from now on, or
// succeed again if err is nil. It panics for a name that isn't a method
// of Orchestrator returning an error.
func (f *Fake) Fail(method string, err error) {
	m, ok := reflect.TypeFor[orchestrator.Orchestrator]().MethodByName(method)
	if !ok || m.Type.NumOut() == 0 || m.Type.Out(m.Type.NumOut()-1) != reflect.TypeFor[error]() {
		panic("orchestratortest: Orchestrator has no method " + method + " that returns an error")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failures == nil {
		f.failures = map[string]error{}
	}
	f.failures[method] = err
}

// begin locks f and returns ctx's cause if it has ended, or else the
// failure set for method.
func (f *Fake) begin(ctx context.Context, method string) error {
	f.mu.Lock()
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	return f.failures[method]
}

// Check refuses an app without a name.
func (f *Fake) Check(app model.App) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.failures["Check"]; err != nil {
		return err
	}
	if app.Name == "" {
		return errors.New("the app has no name")
	}
	return nil
}

// Deploy refuses an app without a name. It calls step without holding
// the fake, and fails, deploying nothing, if ctx has ended once the
// first step is told.
func (f *Fake) Deploy(ctx context.Context, app model.App, step func(orchestrator.Step)) error {
	err := f.begin(ctx, "Deploy")
	f.mu.Unlock()
	if err != nil {
		return err
	}
	if app.Name == "" {
		return errors.New("the app has no name")
	}
	step(orchestrator.Applying)
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	app.Containers = slices.Clone(app.Containers)
	f.mu.Lock()
	if f.apps == nil {
		f.apps = map[string]model.App{}
	}
	f.apps[app.Name] = app
	f.mu.Unlock()
	step(orchestrator.WaitingForReady)
	return nil
}

// Certificate fails for a container the app doesn't have, or one
// without a host.
func (f *Fake) Certificate(ctx context.Context, app, container string) error {
	defer f.mu.Unlock()
	if err := f.begin(ctx, "Certificate"); err != nil {
		return err
	}
	a, ok := f.apps[app]
	if !ok {
		return orchestrator.ErrNoApp
	}
	for _, c := range a.Containers {
		if c.Name == container && c.Host != "" {
			return nil
		}
	}
	return fmt.Errorf("%s has no container %s with a host", app, container)
}

func (f *Fake) Remove(ctx context.Context, app string) error {
	defer f.mu.Unlock()
	if err := f.begin(ctx, "Remove"); err != nil {
		return err
	}
	if _, ok := f.apps[app]; !ok {
		return orchestrator.ErrNoApp
	}
	delete(f.apps, app)
	return nil
}

// Logs writes Logged's text for the app, or else a line for each of its
// running containers, as "web-1 web: running". It writes without
// holding the fake.
func (f *Fake) Logs(ctx context.Context, app string, _ orchestrator.LogOptions, w io.Writer) error {
	text, err := f.logs(ctx, app)
	if err != nil {
		return err
	}
	_, err = io.WriteString(w, text)
	return err
}

func (f *Fake) logs(ctx context.Context, app string) (string, error) {
	defer f.mu.Unlock()
	if err := f.begin(ctx, "Logs"); err != nil {
		return "", err
	}
	if _, ok := f.apps[app]; !ok {
		return "", orchestrator.ErrNoApp
	}
	if text, ok := f.Logged[app]; ok {
		return text, nil
	}
	var text strings.Builder
	for _, r := range f.running() {
		if r.App == app {
			fmt.Fprintf(&text, "%s %s: running\n", r.Replica, r.Container)
		}
	}
	return text.String(), nil
}

func (f *Fake) Apps(ctx context.Context) ([]model.AppState, error) {
	defer f.mu.Unlock()
	if err := f.begin(ctx, "Apps"); err != nil {
		return nil, err
	}
	var apps []model.AppState
	for _, name := range slices.Sorted(maps.Keys(f.apps)) {
		a := f.apps[name]
		s := model.AppState{Name: name}
		if a.Image != "" {
			s.Images = append(s.Images, a.Image)
		}
		for _, c := range a.Containers {
			s.Want += replicas(c)
			if !slices.Contains(s.Images, c.Image) {
				s.Images = append(s.Images, c.Image)
			}
			if c.Host != "" {
				s.Hosts = append(s.Hosts, c.Host)
			}
		}
		s.Ready = s.Want
		apps = append(apps, s)
	}
	return apps, nil
}

// Running has Own, the console once it runs, and each replica of each
// app's containers, the replicas of a container web named web-1, web-2
// and so on.
func (f *Fake) Running(ctx context.Context) ([]model.Running, error) {
	defer f.mu.Unlock()
	if err := f.begin(ctx, "Running"); err != nil {
		return nil, err
	}
	return f.running(), nil
}

func (f *Fake) running() []model.Running {
	running := slices.Clone(f.Own)
	if f.console != "" {
		running = append(running, model.Running{Replica: "console-1", Container: "console", Image: f.console})
	}
	for name, a := range f.apps {
		for _, c := range a.Containers {
			for i := range replicas(c) {
				running = append(running, model.Running{App: name, Replica: fmt.Sprintf("%s-%d", c.Name, i+1), Container: c.Name, Image: c.Image})
			}
		}
	}
	slices.SortFunc(running, func(a, b model.Running) int {
		return cmp.Or(cmp.Compare(a.App, b.App), cmp.Compare(a.Replica, b.Replica), cmp.Compare(a.Container, b.Container))
	})
	return running
}

// replicas is how many replicas of c run: one, if c doesn't say.
func replicas(c model.Container) int {
	return max(c.Replicas, 1)
}

// Preflight finds every host to be an amd64 one of 4096 MiB whose
// private address is 10.0.0.1 on eth0, unless want names another.
func (f *Fake) Preflight(ctx context.Context, _ orchestrator.Host, want orchestrator.NodeRequest) (model.Node, error) {
	defer f.mu.Unlock()
	if err := f.begin(ctx, "Preflight"); err != nil {
		return model.Node{}, err
	}
	n := model.Node{
		Name:           want.Name,
		ControlPlane:   want.ControlPlane,
		PrivateAddress: netip.MustParseAddr("10.0.0.1"),
		Interface:      "eth0",
		Architecture:   "amd64",
		MemoryMiB:      4096,
	}
	if want.PrivateAddress.IsValid() {
		n.PrivateAddress = want.PrivateAddress
	}
	return n, nil
}

// AddNode fails for a node the cluster has already.
func (f *Fake) AddNode(ctx context.Context, _ orchestrator.Host, n model.Node) error {
	defer f.mu.Unlock()
	if err := f.begin(ctx, "AddNode"); err != nil {
		return err
	}
	if _, ok := f.nodes[n.Name]; ok {
		return fmt.Errorf("the cluster has a node %s already", n.Name)
	}
	if f.nodes == nil {
		f.nodes = map[string]model.Node{}
	}
	f.nodes[n.Name] = n
	return nil
}

// RemoveNode succeeds for a node the cluster doesn't have.
func (f *Fake) RemoveNode(ctx context.Context, name string, _ orchestrator.Host, _ orchestrator.RemoveOptions) error {
	defer f.mu.Unlock()
	if err := f.begin(ctx, "RemoveNode"); err != nil {
		return err
	}
	delete(f.nodes, name)
	return nil
}

func (f *Fake) Uninstall(ctx context.Context, _ orchestrator.Host, _ model.Node) error {
	defer f.mu.Unlock()
	return f.begin(ctx, "Uninstall")
}

// Nodes gives a node its private address as its address, or none if it
// has none.
func (f *Fake) Nodes(ctx context.Context) ([]model.NodeState, error) {
	defer f.mu.Unlock()
	if err := f.begin(ctx, "Nodes"); err != nil {
		return nil, err
	}
	var nodes []model.NodeState
	for _, name := range slices.Sorted(maps.Keys(f.nodes)) {
		n := f.nodes[name]
		address := ""
		if n.PrivateAddress.IsValid() {
			address = n.PrivateAddress.String()
		}
		nodes = append(nodes, model.NodeState{
			Name:         name,
			Address:      address,
			Architecture: n.Architecture,
			MemoryMiB:    n.MemoryMiB,
			ControlPlane: n.ControlPlane,
			Ready:        "yes",
		})
	}
	return nodes, nil
}

func (f *Fake) RunConsole(ctx context.Context, image string, _ []netip.Addr) error {
	defer f.mu.Unlock()
	if err := f.begin(ctx, "RunConsole"); err != nil {
		return err
	}
	f.console = image
	return nil
}

// ConsoleToken fails until RunConsole has run the console.
func (f *Fake) ConsoleToken(ctx context.Context) (string, error) {
	defer f.mu.Unlock()
	if err := f.begin(ctx, "ConsoleToken"); err != nil {
		return "", err
	}
	if f.console == "" {
		return "", errors.New("the console isn't running")
	}
	return "fake-console-token", nil
}

func (f *Fake) Can() orchestrator.Capabilities {
	return orchestrator.Capabilities{KubernetesObjects: true}
}

// Host is a host on which every command, upload and dial fails.
type Host struct{}

func (Host) Run(context.Context, string, io.Reader) (string, error) {
	return "", errNoHost
}

func (Host) RunStoppable(context.Context, string) (string, error) {
	return "", errNoHost
}

func (Host) Upload(context.Context, string, io.Reader, fs.FileMode) error {
	return errNoHost
}

func (Host) DialTCP(context.Context, string) (net.Conn, error) {
	return nil, errNoHost
}

var errNoHost = errors.New("orchestratortest.Host runs nothing")

var (
	_ orchestrator.Orchestrator = (*Fake)(nil)
	_ orchestrator.Host         = Host{}
)
