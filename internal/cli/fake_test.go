package cli

import (
	"archive/tar"
	"bufio"
	"bytes"
	"cmp"
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"maps"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"

	ggcrregistry "github.com/google/go-containerregistry/pkg/registry"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/google/go-containerregistry/pkg/v1/random"

	"github.com/moostackhq/moostack/internal/model"
	"github.com/moostackhq/moostack/internal/orchestrator"
	"github.com/moostackhq/moostack/internal/orchestrator/orchestratortest"
	"github.com/moostackhq/moostack/internal/sshclient"
)

func testHostKey(t testing.TB) ssh.PublicKey {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	k, err := ssh.NewPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// fakeCluster is a set of hosts and the cluster the orchestrator makes
// of them, as mooctl's commands see them.
type fakeCluster struct {
	// unissued is the name of a certificate that is never issued.
	unissued string
	t        testing.TB
	mu       sync.Mutex
	// hosts are the hosts by SSH target.
	hosts map[string]*fakeNode
	// nodes are the nodes the cluster has, by name.
	nodes map[string]fakeClusterNode
	// apps holds an entry for each app the cluster has, by name.
	apps map[string]map[string]bool
	orch *fakeOrchestrator
	// locked says the operation lock is held.
	locked bool
	// lock is the operation lock as last taken.
	lock *fakeLock
	// runs are the scripts run, as "target: first line".
	runs []string
	// registry is what DialTCP connects to.
	registry string
	// release is the URL of the release the console is downloaded from,
	// and downloads counts the requests to it.
	release   string
	downloads atomic.Int32
	// consoles are the digests of the console's images in the release,
	// by architecture.
	consoles map[string]string
	// consoleRuns is the image the console runs from, and consoleAt the
	// addresses it was last run on.
	consoleRuns string
	consoleAt   []string
	// tokens are the login tokens the console issued.
	tokens []string
	// consolePageAfter is how many requests for the console's login page
	// the ingress answers with 404 first, of pageProbes so far.
	consolePageAfter, pageProbes int
	// consolePageStalls makes the ingress leave the requests unanswered
	// that it would answer with 200.
	consolePageStalls bool
	// ingressDials are the addresses the ingress was connected to at.
	ingressDials []string
	// consoleFails makes the console never get ready, and zotFails zot's
	// install fail.
	consoleFails, zotFails bool
	// cordoned are the cluster's nodes that are cordoned.
	cordoned map[string]bool
	// servicesDown makes the cluster's services not ready, until a wait
	// for them if servicesComeUp.
	servicesDown, servicesComeUp bool
	// commitUnread makes the read of the applied commit fail.
	commitUnread bool
}

// fakeOrchestrator is the cluster's orchestrator: a fake one that notes
// the calls made to it, with what the fake cluster adds to them.
type fakeOrchestrator struct {
	*orchestratortest.Fake
	c *fakeCluster
	// calls are the calls made, as "Deploy shop".
	calls []string
	// deployed are the apps as last deployed, by name.
	deployed map[string]model.App
	// hosts counts the orchestrators built to act through a host.
	hosts int
	// onCertificate, if set, runs at each call of Certificate.
	onCertificate func()
	// onDeploy, if set, runs at each call of Deploy, whose result is its
	// error if it returns one.
	onDeploy func(ctx context.Context) error
	// onApps, onRemove and onLogs, if set, run at each call of Apps, of
	// Remove and of Logs.
	onApps, onRemove, onLogs func()
	// states, if set, are the apps Apps answers with.
	states []model.AppState
	// asked are the nodes Preflight was asked for, and added the nodes
	// AddNode made, by name.
	asked []orchestrator.NodeRequest
	added map[string]model.Node
	// removals are the options of each RemoveNode, by node.
	removals map[string]orchestrator.RemoveOptions
	// onUninstall, if set, runs as the orchestrator uninstalls from the
	// host at target, by Uninstall or by RemoveNode, which fails with its
	// error if it returns one.
	onUninstall func(target string) error
	// failRemovalAt makes RemoveNode fail with the error once it told the
	// step.
	failRemovalAt map[orchestrator.Step]error
	// onRunConsole, if set, runs at each call of RunConsole.
	onRunConsole func()
	// zotAtInstalling says zot's install ran on a host a moment after
	// AddNode told Installing, before it told Starting.
	zotAtInstalling bool
	// lingers makes AddNode wait, once it told Starting, until its
	// context ends or 5 seconds passed; addEnded is what ended it, nil
	// for the time.
	lingers  bool
	addEnded error
}

// hostOf is the fake host that host is a connection to.
func hostOf(host orchestrator.Host) *fakeNode {
	return host.(orchestratorHost).r.(interface{ node() *fakeNode }).node()
}

// sync makes the fake orchestrator's nodes those of the fake cluster,
// which tests add to and take from.
func (o *fakeOrchestrator) sync() {
	ctx := context.Background()
	has, err := o.Fake.Nodes(ctx)
	if err != nil {
		return
	}
	o.c.mu.Lock()
	want := maps.Clone(o.c.nodes)
	o.c.mu.Unlock()
	silent := func(orchestrator.Step) {}
	for _, n := range has {
		if _, ok := want[n.Name]; !ok {
			o.Fake.RemoveNode(ctx, n.Name, nil, orchestrator.RemoveOptions{Unreachable: true}, silent)
		}
		delete(want, n.Name)
	}
	for name := range want {
		o.Fake.AddNode(ctx, orchestratortest.Host{}, model.Node{Name: name}, silent)
	}
}

// Preflight finds the host to be what the fake host is, and refuses one
// that is a node already, has less than 806 MiB, is asked for an
// address that isn't its private one, doesn't reach the control plane,
// or is to join a control plane whose version isn't mooctl's
// K3sVersion.
func (o *fakeOrchestrator) Preflight(ctx context.Context, host orchestrator.Host, want orchestrator.NodeRequest) (model.Node, error) {
	o.called("Preflight %s", want.Name)
	if _, err := o.Fake.Preflight(ctx, host, want); err != nil {
		return model.Node{}, err
	}
	h := hostOf(host)
	o.c.mu.Lock()
	defer o.c.mu.Unlock()
	o.asked = append(o.asked, want)
	switch {
	case h.k3s:
		return model.Node{}, errors.New("it can't be a node: it has k3s, or k3s's files, already")
	case h.memMiB < 806:
		return model.Node{}, fmt.Errorf("it can't be a node: it has %d MiB of memory, and a node needs at least 806 MiB", h.memMiB)
	case want.PrivateAddress.IsValid() && want.PrivateAddress != h.private:
		return model.Node{}, fmt.Errorf("%s isn't a private IPv4 address", want.PrivateAddress)
	case !want.ControlPlane && h.noReach:
		return model.Node{}, errors.New("exit 1: it doesn't reach the control plane")
	}
	if !want.ControlPlane {
		for _, cp := range o.c.hosts {
			if cp.server && cp.version != K3sVersion {
				return model.Node{}, fmt.Errorf("the control plane runs k3s %s, and the cluster is to run %s", cp.version, K3sVersion)
			}
		}
	}
	n := model.Node{
		Name: want.Name, ControlPlane: want.ControlPlane,
		PrivateAddress: h.private, Interface: "ens10", PublicAddress: h.public,
		Architecture: goArchOf(h.arch), MemoryMiB: h.memMiB, GOGC: "25", GOMEMLIMIT: fmt.Sprintf("%dMiB", h.memMiB/10),
	}
	return n, nil
}

// AddNode makes the fake host a node of the fake cluster, unless the
// host fails its install or its start: then it tells Uninstalling and
// leaves the host as it was.
func (o *fakeOrchestrator) AddNode(ctx context.Context, host orchestrator.Host, n model.Node, step func(orchestrator.Step)) error {
	o.called("AddNode %s", n.Name)
	o.sync()
	h := hostOf(host)
	o.c.mu.Lock()
	failInstall, failStart := h.failInstall, h.failStart
	o.c.mu.Unlock()
	if ctx.Err() == nil && (failInstall || failStart) {
		step(orchestrator.Installing)
		if failInstall {
			step(orchestrator.Uninstalling)
			return errors.New("installing k3s: exit 1: install failed")
		}
		step(orchestrator.Starting)
		step(orchestrator.WaitingForNode)
		step(orchestrator.Uninstalling)
		return errors.New("starting k3s: exit 1: k3s-agent: token is invalid")
	}
	err := o.Fake.AddNode(ctx, host, n, func(s orchestrator.Step) {
		step(s)
		if s == orchestrator.Installing {
			time.Sleep(20 * time.Millisecond)
			if o.c.ran(h.target, "zot serve") {
				o.zotAtInstalling = true
			}
		}
		if s == orchestrator.Starting && o.lingers {
			select {
			case <-ctx.Done():
				o.addEnded = context.Cause(ctx)
			case <-time.After(5 * time.Second):
			}
		}
	})

	if err != nil {
		return err
	}
	o.c.mu.Lock()
	defer o.c.mu.Unlock()
	h.k3s, h.server, h.version, h.nodeName = true, n.ControlPlane, K3sVersion, n.Name
	o.c.nodes[n.Name] = fakeClusterNode{ip: n.PrivateAddress.String(), arch: n.Architecture, controlPlane: n.ControlPlane}
	o.added[n.Name] = n
	return nil
}

// uninstalled makes the fake host no node, and takes the cluster's
// nodes if the host was the control plane.
func (o *fakeOrchestrator) uninstalled(host orchestrator.Host) error {
	h := hostOf(host)
	if o.onUninstall != nil {
		if err := o.onUninstall(h.target); err != nil {
			return err
		}
	}
	o.c.mu.Lock()
	defer o.c.mu.Unlock()
	if h.server {
		o.c.nodes = map[string]fakeClusterNode{}
	}
	h.k3s, h.server = false, false
	return nil
}

// RemoveNode takes the node from the fake cluster, unless a host that
// can be reached and wasn't uninstalled from is still that node: the
// host registers the node again.
func (o *fakeOrchestrator) RemoveNode(ctx context.Context, name string, host orchestrator.Host, opts orchestrator.RemoveOptions, step func(orchestrator.Step)) error {
	o.called("RemoveNode %s", name)
	o.sync()
	o.c.mu.Lock()
	o.removals[name] = opts
	o.c.mu.Unlock()
	var failed error
	err := o.Fake.RemoveNode(ctx, name, host, opts, func(s orchestrator.Step) {
		if failed != nil {
			return
		}
		step(s)
		if failed = o.failRemovalAt[s]; failed != nil {
			return
		}
		if s == orchestrator.Uninstalling {
			if err := o.uninstalled(host); err != nil {
				failed = fmt.Errorf("uninstalling from %s: %w", name, err)
			}
		}
	})
	if err != nil {
		return err
	}
	if failed != nil {
		o.sync()
		return failed
	}
	o.c.mu.Lock()
	defer o.c.mu.Unlock()
	for _, h := range o.c.hosts {
		if h.k3s && !h.unreachable && h.nodeName == name {
			return nil
		}
	}
	delete(o.c.nodes, name)
	return nil
}

func (o *fakeOrchestrator) Uninstall(ctx context.Context, host orchestrator.Host, n model.Node) error {
	o.called("Uninstall %s", n.Name)
	if err := o.Fake.Uninstall(ctx, host, n); err != nil {
		return err
	}
	return o.uninstalled(host)
}

// RunConsole fails for a console that never gets ready.
func (o *fakeOrchestrator) RunConsole(ctx context.Context, image string, addresses []netip.Addr) error {
	o.called("RunConsole %s", image)
	if o.onRunConsole != nil {
		o.onRunConsole()
	}
	o.c.mu.Lock()
	fails := o.c.consoleFails
	o.c.mu.Unlock()
	if fails && ctx.Err() == nil {
		return errors.New("the console isn't ready: deployment moostack-console: 0 of 1 replicas are updated and available")
	}
	if err := o.Fake.RunConsole(ctx, image, addresses); err != nil {
		return err
	}
	o.c.mu.Lock()
	defer o.c.mu.Unlock()
	o.c.consoleRuns, o.c.consoleAt = image, nil
	for _, a := range addresses {
		o.c.consoleAt = append(o.c.consoleAt, a.String())
	}
	return nil
}

// ConsoleToken issues another token each time.
func (o *fakeOrchestrator) ConsoleToken(ctx context.Context) (string, error) {
	o.called("ConsoleToken")
	if _, err := o.Fake.ConsoleToken(ctx); err != nil {
		return "", err
	}
	o.c.mu.Lock()
	defer o.c.mu.Unlock()
	token := strings.Repeat("A", 25) + string(rune('A'+len(o.c.tokens)))
	o.c.tokens = append(o.c.tokens, token)
	return token, nil
}

// stopConsole makes the cluster run no console.
func (c *fakeCluster) stopConsole() {
	c.orch.Fake.RunConsole(context.Background(), "", nil)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.consoleRuns = ""
}

func (o *fakeOrchestrator) called(format string, args ...any) {
	o.c.mu.Lock()
	defer o.c.mu.Unlock()
	o.calls = append(o.calls, fmt.Sprintf(format, args...))
}

func (o *fakeOrchestrator) Check(app model.App) error {
	o.called("Check %s", app.Name)
	return o.Fake.Check(app)
}

// Deploy tells WaitingForCluster and waits while the cluster's services
// are down, and fails if they don't come up. A deployed app is one the
// fake cluster has.
func (o *fakeOrchestrator) Deploy(ctx context.Context, app model.App, step func(orchestrator.Step)) error {
	o.called("Deploy %s", app.Name)
	if o.onDeploy != nil {
		if err := o.onDeploy(ctx); err != nil {
			return err
		}
	}
	o.c.mu.Lock()
	down, comeUp := o.c.servicesDown, o.c.servicesComeUp
	o.c.mu.Unlock()
	if down {
		step(orchestrator.WaitingForCluster)
		if !comeUp {
			return errors.New("the cluster isn't ready for apps: still waiting for the issuer moostack, Traefik")
		}
		o.c.mu.Lock()
		o.c.servicesDown = false
		o.c.mu.Unlock()
	}
	if err := o.Fake.Deploy(ctx, app, step); err != nil {
		return err
	}
	o.c.mu.Lock()
	defer o.c.mu.Unlock()
	o.deployed[app.Name] = app
	if o.c.apps[app.Name] == nil {
		o.c.apps[app.Name] = map[string]bool{}
	}
	return nil
}

// Certificate fails for the certificate the fake cluster never issues.
func (o *fakeOrchestrator) Certificate(ctx context.Context, app, container string) error {
	o.called("Certificate %s %s", app, container)
	if o.onCertificate != nil {
		o.onCertificate()
	}
	if o.c.unissued == container+"-tls" {
		return errors.New("the challenge is pending")
	}
	return o.Fake.Certificate(ctx, app, container)
}

func (o *fakeOrchestrator) Apps(ctx context.Context) ([]model.AppState, error) {
	o.called("Apps")
	if o.onApps != nil {
		o.onApps()
	}
	apps, err := o.Fake.Apps(ctx)
	if err != nil || o.states == nil {
		return apps, err
	}
	return slices.Clone(o.states), nil
}

// Remove removes the app from the fake cluster as well.
func (o *fakeOrchestrator) Remove(ctx context.Context, app string) error {
	o.called("Remove %s", app)
	if o.onRemove != nil {
		o.onRemove()
	}
	if err := o.Fake.Remove(ctx, app); err != nil {
		return err
	}
	o.c.mu.Lock()
	defer o.c.mu.Unlock()
	delete(o.c.apps, app)
	delete(o.deployed, app)
	return nil
}

func (o *fakeOrchestrator) Logs(ctx context.Context, app string, opts orchestrator.LogOptions, w io.Writer) error {
	o.called("Logs %s, tail %d, follow %t", app, opts.Tail, opts.Follow)
	if o.onLogs != nil {
		o.onLogs()
	}
	return o.Fake.Logs(ctx, app, opts, w)
}

// Nodes are the fake cluster's nodes, each with 1974 MiB.
func (o *fakeOrchestrator) Nodes(ctx context.Context) ([]model.NodeState, error) {
	o.called("Nodes")
	if _, err := o.Fake.Nodes(ctx); err != nil {
		return nil, err
	}
	o.c.mu.Lock()
	defer o.c.mu.Unlock()
	var nodes []model.NodeState
	for name, n := range o.c.nodes {
		s := model.NodeState{Name: name, Address: n.ip, Architecture: n.arch, Version: K3sVersion, MemoryMiB: 1974, Ready: cmp.Or(n.ready, "yes"), ControlPlane: n.controlPlane}
		if n.noMemory {
			s.MemoryMiB = 0
		}
		if o.c.cordoned[name] {
			s.Ready += ", cordoned"
		}
		nodes = append(nodes, s)
	}
	slices.SortFunc(nodes, func(a, b model.NodeState) int { return cmp.Compare(a.Name, b.Name) })
	return nodes, nil
}

// made are the calls made to the orchestrator whose names start with
// prefix.
func (o *fakeOrchestrator) made(prefix string) []string {
	o.c.mu.Lock()
	defer o.c.mu.Unlock()
	var out []string
	for _, call := range o.calls {
		if strings.HasPrefix(call, prefix) {
			out = append(out, call)
		}
	}
	return out
}

type fakeClusterNode struct {
	ip, arch     string
	controlPlane bool
	// ready is the node's Ready, "yes" if empty.
	ready string
	// noMemory makes the cluster know no memory of the node.
	noMemory bool
}

type fakeNode struct {
	target string
	key    ssh.PublicKey
	// arch is the host's architecture as uname prints it.
	arch   string
	memMiB int
	// private and public are the host's addresses; public is invalid for
	// a host behind NAT.
	private, public netip.Addr
	// k3s says the orchestrator has made the host a node, server that it
	// is the control plane, and nodeName what the node is called.
	k3s, server bool
	nodeName    string
	// zot says zot is installed.
	zot     bool
	version string
	// files are the files the commands wrote on the host themselves.
	files       map[string]string
	unreachable bool
	// failInstall makes the orchestrator's install fail on the host, and
	// failStart its start.
	failInstall, failStart bool
	// noReach makes the host fail to reach the control plane.
	noReach bool
}

// imageArchive is an OCI image layout archive of a random image, as
// buildx writes one, and the image's digest.
func imageArchive(t testing.TB) ([]byte, string) {
	t.Helper()
	img, err := random.Image(256, 1)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	p, err := layout.Write(dir, empty.Index)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.AppendImage(img); err != nil {
		t.Fatal(err)
	}
	var archive bytes.Buffer
	tw := tar.NewWriter(&archive)
	if err := tw.AddFS(os.DirFS(dir)); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	digest, _ := img.Digest()
	return archive.Bytes(), digest.String()
}

// newFakeCluster is a cluster with no hosts, a registry, and a release
// that holds the console's image for amd64 and arm64.
func newFakeCluster(t testing.TB) *fakeCluster {
	c := &fakeCluster{t: t, hosts: map[string]*fakeNode{}, nodes: map[string]fakeClusterNode{}, apps: map[string]map[string]bool{}}
	c.orch = &fakeOrchestrator{Fake: &orchestratortest.Fake{Volumes: map[string][]string{}}, c: c, deployed: map[string]model.App{}, added: map[string]model.Node{}, removals: map[string]orchestrator.RemoveOptions{}}
	registry := httptest.NewServer(ggcrregistry.New(ggcrregistry.Logger(log.New(io.Discard, "", 0))))
	t.Cleanup(registry.Close)
	c.registry = registry.Listener.Addr().String()

	files := map[string][]byte{}
	var sums strings.Builder
	c.consoles = map[string]string{}
	for _, arch := range []string{"amd64", "arm64"} {
		archive, digest := imageArchive(t)
		c.consoles[arch] = digest
		name := "console-linux-" + arch + ".tar"
		files[name] = archive
		fmt.Fprintf(&sums, "%x  %s\n", sha256.Sum256(archive), name)
	}
	files["sha256sums.txt"] = []byte(sums.String())
	release := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.downloads.Add(1)
		content, ok := files[strings.TrimPrefix(r.URL.Path, "/edge/")]
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Write(content)
	}))
	t.Cleanup(release.Close)
	c.release = release.URL + "/edge"
	return c
}

// host adds a 2 GB amd64 host at target, with the private address addr
// and the public address 192.0.2.10.
func (c *fakeCluster) host(target, addr string) *fakeNode {
	n := &fakeNode{target: target, key: testHostKey(c.t), arch: "x86_64", memMiB: 1974,
		private: netip.MustParseAddr(addr), public: netip.MustParseAddr("192.0.2.10"), files: map[string]string{}}
	c.hosts[target] = n
	return n
}

// connect is Env.Connect.
func (c *fakeCluster) connect(_ context.Context, target string, hostKey ssh.PublicKey) (remote, ssh.PublicKey, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	n, ok := c.hosts[target]
	if !ok || n.unreachable {
		return nil, nil, fmt.Errorf("connecting to %s: no route to host", target)
	}
	if hostKey != nil && !bytes.Equal(hostKey.Marshal(), n.key.Marshal()) {
		return nil, nil, fmt.Errorf("connecting to %s: host key mismatch", target)
	}
	return &fakeConn{c: c, n: n}, n.key, nil
}

// orchestrator is Env.Orchestrator.
func (c *fakeCluster) orchestrator(host orchestrator.Host) orchestrator.Orchestrator {
	if host != nil {
		c.mu.Lock()
		c.orch.hosts++
		c.mu.Unlock()
	}
	return c.orch
}

// ran reports whether a script whose first line contains s ran on
// target.
func (c *fakeCluster) ran(target, s string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.ContainsFunc(c.runs, func(r string) bool { return strings.HasPrefix(r, target+": ") && strings.Contains(r, s) })
}

// fakeConn is a connection to a fakeNode.
type fakeConn struct {
	c *fakeCluster
	n *fakeNode
}

func (f *fakeConn) Close() error { return nil }

func (f *fakeConn) node() *fakeNode { return f.n }

func (f *fakeConn) RunStoppable(ctx context.Context, cmd string) (sshclient.Result, error) {
	return f.Run(ctx, cmd, nil)
}

// left is what an uninstall left on n, empty if nothing.
func left(n *fakeNode) string {
	var has []string
	for path := range n.files {
		has = append(has, path)
	}
	sort.Strings(has)
	if n.k3s {
		has = append(has, "k3s")
	}
	if n.zot {
		has = append(has, "zot")
	}
	return strings.Join(has, " ")
}

func (f *fakeConn) Upload(ctx context.Context, file string, r io.Reader, _ fs.FileMode, _ string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	f.c.mu.Lock()
	defer f.c.mu.Unlock()
	f.n.files[file] = string(data)
	return nil
}

func (f *fakeConn) DialTCP(ctx context.Context, addr string) (net.Conn, error) {
	if _, port, _ := net.SplitHostPort(addr); port == "443" {
		return f.c.ingress(addr), nil
	}
	if f.c.registry == "" {
		return nil, errors.New("no registry")
	}
	return (&net.Dialer{}).DialContext(ctx, "tcp", f.c.registry)
}

func (f *fakeConn) Lock(context.Context) (held, error) {
	f.c.mu.Lock()
	defer f.c.mu.Unlock()
	if f.c.locked {
		return nil, errors.New("another mooctl operation is running on this cluster; wait for it to finish")
	}
	f.c.locked = true
	f.c.lock = &fakeLock{c: f.c, done: make(chan struct{})}
	return f.c.lock, nil
}

type fakeLock struct {
	c    *fakeCluster
	done chan struct{}
	once sync.Once
}

func (l *fakeLock) Done() <-chan struct{} { return l.done }
func (l *fakeLock) Close() error {
	l.once.Do(func() {
		l.c.mu.Lock()
		l.c.locked = false
		l.c.mu.Unlock()
		close(l.done)
	})
	return nil
}

func ok(stdout string) (sshclient.Result, error) {
	return sshclient.Result{Stdout: []byte(stdout)}, nil
}

func fail(code int, stderr string) (sshclient.Result, error) {
	return sshclient.Result{ExitCode: code, Stderr: []byte(stderr)}, nil
}

// Run answers the scripts the commands run themselves, and fails any
// other.
func (f *fakeConn) Run(ctx context.Context, cmd string, _ io.Reader) (sshclient.Result, error) {
	if ctx.Err() != nil {
		return sshclient.Result{}, ctx.Err()
	}
	c, n := f.c, f.n
	c.mu.Lock()
	defer c.mu.Unlock()
	c.runs = append(c.runs, n.target+": "+cmd)
	switch {
	case strings.HasPrefix(cmd, "mkdir -p "):
		return ok("")
	case cmd == AppliedCommitScript:
		if c.commitUnread {
			return fail(1, "cat: "+AppliedCommitFile+": Input/output error")
		}
		return ok(n.files[AppliedCommitFile])
	case strings.Contains(cmd, "zot serve"):
		if c.zotFails {
			return fail(1, "zot doesn't answer at 10.0.0.2:5000")
		}
		n.zot = true
		return ok("")
	case cmd == ownUninstallScript:
		n.zot = false
		delete(n.files, AppliedCommitFile)
		return ok("")
	}
	c.t.Errorf("the fake cluster has no answer to a script on %s:\n%s", n.target, cmd)
	return fail(127, "fake: unknown script")
}

// goArchOf is the Go architecture of what uname prints.
func goArchOf(uname string) string {
	if uname == "x86_64" {
		return "amd64"
	}
	return "arm64"
}

// fakeEnvFor is an Env that reaches the hosts of c, in home.
func fakeEnvFor(c *fakeCluster, home string, out *bytes.Buffer) Env {
	return Env{Home: home, LocalUser: "op", Stdout: out, Stderr: out, Root: home + "/clusters", Connect: c.connect, Exit: func(int) {}, Release: c.release, Orchestrator: c.orchestrator}
}

// ingress is a connection to the cluster's ingress at addr, which
// answers one request over TLS: the console's login page with 200 once
// consolePageAfter requests for it were answered 404, and anything else
// with 404.
func (c *fakeCluster) ingress(addr string) net.Conn {
	client, server := net.Pipe()
	c.mu.Lock()
	c.ingressDials = append(c.ingressDials, addr)
	c.mu.Unlock()
	go func() {
		defer server.Close()
		server.SetDeadline(time.Now().Add(time.Minute))
		conn := tls.Server(server, &tls.Config{Certificates: []tls.Certificate{ingressCert()}})
		req, err := http.ReadRequest(bufio.NewReader(conn))
		if err != nil {
			return
		}
		status := http.StatusNotFound
		c.mu.Lock()
		stalls := false
		if c.consoleRuns != "" && req.URL.Path == "/console/login" {
			c.pageProbes++
			if c.pageProbes > c.consolePageAfter {
				status, stalls = http.StatusOK, c.consolePageStalls
			}
		}
		c.mu.Unlock()
		if stalls {
			conn.Read(make([]byte, 1))
			return
		}
		fmt.Fprintf(conn, "HTTP/1.1 %d %s\r\nContent-Length: 0\r\nConnection: close\r\n\r\n", status, http.StatusText(status))
	}()
	return client
}

// ingressCert is a certificate signed by itself, as the ingress's
// default one is.
var ingressCert = sync.OnceValue(func() tls.Certificate {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		panic(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		panic(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
})

// hasApp makes the cluster of c have an app.
func hasApp(t testing.TB, c *fakeCluster) {
	t.Helper()
	app := model.App{Name: "web", Containers: []model.Container{{Name: "web", Image: "example.test/web:1"}}}
	if err := c.orch.Fake.Deploy(t.Context(), app, func(orchestrator.Step) {}); err != nil {
		t.Fatal(err)
	}
}
