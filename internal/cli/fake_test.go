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
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"
	"gopkg.in/yaml.v3"

	ggcrregistry "github.com/google/go-containerregistry/pkg/registry"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/google/go-containerregistry/pkg/v1/random"

	"github.com/moostackhq/moostack/internal/k3s"
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

// fakeCluster is a set of hosts and the Kubernetes cluster their k3s
// makes, as mooctl's scripts see them.
type fakeCluster struct {
	// unissued is the name of a certificate that is never issued.
	unissued string
	t        testing.TB
	mu       sync.Mutex
	// hosts are the hosts by SSH target.
	hosts map[string]*fakeNode
	// nodes are the Kubernetes nodes by name.
	nodes map[string]fakeKubeNode
	// apps are the app namespaces.
	apps map[string]map[string]bool
	// orch is the cluster's orchestrator.
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
	// consoleRuns is the image the console's Deployment runs, and
	// consoleSeen the one a command was last told it runs.
	consoleRuns, consoleSeen string
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
	// consoleFails makes the console never get ready.
	consoleFails bool
	// cordoned are the Kubernetes nodes marked unschedulable.
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
// fake cluster's namespaces have.
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

// Remove removes the app from the fake cluster's namespaces as well.
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

// Nodes are the fake cluster's Kubernetes nodes, each with 1974 MiB. A
// node is a control plane if a host that runs a k3s server has its name.
func (o *fakeOrchestrator) Nodes(ctx context.Context) ([]model.NodeState, error) {
	o.called("Nodes")
	if _, err := o.Fake.Nodes(ctx); err != nil {
		return nil, err
	}
	o.c.mu.Lock()
	defer o.c.mu.Unlock()
	var nodes []model.NodeState
	for name, n := range o.c.nodes {
		s := model.NodeState{Name: name, Address: n.ip, Architecture: n.arch, Version: K3sVersion, MemoryMiB: 1974, Ready: cmp.Or(n.ready, "yes")}
		for _, h := range o.c.hosts {
			if h.server && h.k3s && strings.Contains(h.files[k3s.ConfigFile], "node-name: "+name+"\n") {
				s.ControlPlane = true
			}
		}
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

type fakeKubeNode struct {
	ip, arch string
	// ready is the node's Ready, "yes" if empty.
	ready string
	// noMemory makes the cluster know no memory of the node.
	noMemory bool
}

type fakeNode struct {
	target string
	key    ssh.PublicKey
	arch   string
	memMiB int
	addrs  []string
	routes []string
	k3s    bool
	server bool
	// zot says zot is installed.
	zot         bool
	version     string
	files       map[string]string
	unreachable bool
	// failInstall makes k3s's install fail on the host, and failStart
	// its start.
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
	c := &fakeCluster{t: t, hosts: map[string]*fakeNode{}, nodes: map[string]fakeKubeNode{}, apps: map[string]map[string]bool{}}
	c.orch = &fakeOrchestrator{Fake: &orchestratortest.Fake{}, c: c, deployed: map[string]model.App{}}
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

// host adds a 2 GB amd64 host at target, with the private address addr.
func (c *fakeCluster) host(target, addr string) *fakeNode {
	n := &fakeNode{target: target, key: testHostKey(c.t), arch: "x86_64", memMiB: 1974,
		addrs: []string{"eth0 192.0.2.10/32", "ens10 " + addr + "/32"}, routes: []string{"eth0"}, files: map[string]string{}}
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

func (f *fakeConn) RunStoppable(ctx context.Context, cmd string) (sshclient.Result, error) {
	// A node whose k3s fails to start never becomes Ready.
	if strings.Contains(cmd, "isn't Ready at") {
		f.c.mu.Lock()
		stuck := slices.ContainsFunc(hostsOf(f.c.hosts), func(h *fakeNode) bool { return h.failStart })
		f.c.mu.Unlock()
		if stuck {
			<-ctx.Done()
			return sshclient.Result{}, ctx.Err()
		}
	}
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

func hostsOf(hosts map[string]*fakeNode) []*fakeNode {
	var out []*fakeNode
	for _, h := range hosts {
		out = append(out, h)
	}
	return out
}

func (f *fakeConn) Upload(_ context.Context, file string, r io.Reader, _ fs.FileMode, _ string) error {
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

var (
	nodeNameInScript = regexp.MustCompile(`get node '([^']+)'`)
	readyAtInScript  = regexp.MustCompile(`= "([0-9.]+) True"`)
	quotedName       = regexp.MustCompile(`(?:delete node|drain|uncordon) '([^']+)'`)
)

func ok(stdout string) (sshclient.Result, error) {
	return sshclient.Result{Stdout: []byte(stdout)}, nil
}

func fail(code int, stderr string) (sshclient.Result, error) {
	return sshclient.Result{ExitCode: code, Stderr: []byte(stderr)}, nil
}

// Run answers the scripts mooctl runs, and fails any other.
func (f *fakeConn) Run(_ context.Context, cmd string, _ io.Reader) (sshclient.Result, error) {
	c, n := f.c, f.n
	c.mu.Lock()
	defer c.mu.Unlock()
	first, _, _ := strings.Cut(cmd, "\n")
	c.runs = append(c.runs, n.target+": "+cmd)
	switch {
	case cmd == k3s.FactsScript:
		var b strings.Builder
		fmt.Fprintf(&b, "arch=%s\nmem_kib=%d\nsystemd=yes\nhas=curl\nhas=flock\nhas=ip\n", n.arch, n.memMiB*1024)
		if n.k3s {
			b.WriteString("k3s=yes\n")
		}
		for _, a := range n.addrs {
			b.WriteString("addr=" + a + "\n")
		}
		for _, r := range n.routes {
			b.WriteString("route=" + r + "\n")
		}
		return ok(b.String())
	case strings.HasPrefix(cmd, "mkdir -p "):
		return ok("")
	case cmd == AppliedCommitScript:
		if c.commitUnread {
			return fail(1, "cat: "+AppliedCommitFile+": Input/output error")
		}
		return ok(n.files[AppliedCommitFile])
	case cmd == k3s.NodeNamesScript:
		var out []string
		for name := range c.nodes {
			out = append(out, "node/"+name)
		}
		sort.Strings(out)
		return ok(strings.Join(out, "\n"))
	case cmd == k3s.VersionScript:
		return ok(n.version + "\n")
	case strings.HasPrefix(first, "curl -sk --max-time 10"):
		if n.noReach {
			return fail(1, "it doesn't reach the control plane")
		}
		return ok("")
	case cmd == "cat "+k3s.AgentTokenFile:
		return ok("K10agent::node:token\n")
	case strings.Contains(cmd, "zot serve"):
		n.zot = true
		return ok("")
	case cmd == k3s.StartScript:
		if n.failStart {
			return fail(1, "k3s-agent: token is invalid")
		}
		return ok("")
	case strings.Contains(cmd, "INSTALL_K3S_EXEC="):
		if n.failInstall {
			return fail(1, "install failed")
		}
		var config struct {
			Name string `yaml:"node-name"`
			IP   string `yaml:"node-ip"`
		}
		yaml.Unmarshal([]byte(n.files[k3s.ConfigFile]), &config)
		n.k3s, n.server, n.version = true, strings.Contains(cmd, "INSTALL_K3S_EXEC=server"), K3sVersion
		c.nodes[config.Name] = fakeKubeNode{ip: config.IP, arch: k3s.RoleOf(true).String()[:0] + goArchOf(n.arch)}
		return ok("")
	case strings.Contains(cmd, "isn't Ready at"):
		name := nodeNameInScript.FindStringSubmatch(cmd)[1]
		ip := readyAtInScript.FindStringSubmatch(cmd)[1]
		if kn, found := c.nodes[name]; found && kn.ip == ip {
			return ok("")
		}
		return fail(1, name+" isn't Ready at "+ip+" after 10 minutes")
	case strings.Contains(cmd, "uninstall.sh"):
		// The files under k3s's directory go, and those the script names.
		n.k3s = false
		for path := range n.files {
			if strings.HasPrefix(path, "/var/lib/rancher/k3s/") || strings.Contains(cmd, " "+path) {
				delete(n.files, path)
			}
		}
		if strings.Contains(cmd, "systemctl disable --now moostack-zot.service") && strings.Contains(cmd, "rm -rf /var/lib/zot /var/lib/private/zot") {
			n.zot = false
		}
		if n.server {
			c.nodes = map[string]fakeKubeNode{}
		}
		return ok("")
	case strings.Contains(first, "k3s kubectl get pv"):
		return ok("")
	case strings.Contains(first, "k3s kubectl drain"):
		name := quotedName.FindStringSubmatch(first)[1]
		if _, found := c.nodes[name]; !found {
			return fail(1, `Error from server (NotFound): nodes "`+name+`" not found`)
		}
		return ok("")
	case strings.Contains(first, "k3s kubectl uncordon"):
		return ok("")
	case strings.Contains(first, "k3s kubectl delete node"):
		name := quotedName.FindStringSubmatch(first)[1]
		// A kubelet still running registers its node again.
		for _, h := range c.hosts {
			if h.k3s && !h.unreachable && strings.Contains(h.files[k3s.ConfigFile], "node-name: "+name+"\n") {
				return ok("")
			}
		}
		delete(c.nodes, name)
		return ok("")
	case strings.Contains(first, "k3s kubectl get namespaces -l"):
		var out []string
		for a := range c.apps {
			out = append(out, "namespace/"+a)
		}
		return ok(strings.Join(out, "\n"))
	case cmd == k3s.ConsoleImageScript:
		// k3s applies the objects it is given a moment after they are
		// written: the first answer after a change is the image before.
		objects, written := n.files[k3s.AutoDeployFile("console")]
		if !written {
			return fail(1, `deployments.apps "moostack-console" not found`)
		}
		_, image, _ := strings.Cut(objects, "          image: ")
		image, _, _ = strings.Cut(image, "\n")
		answer := cmp.Or(c.consoleRuns, image)
		c.consoleRuns, c.consoleSeen = image, answer
		if answer == "" {
			return fail(1, `deployments.apps "moostack-console" not found`)
		}
		return ok(answer)
	case cmd == k3s.ConsoleTokenScript:
		if _, written := n.files[k3s.AutoDeployFile("console")]; !written {
			return fail(1, `Error from server (NotFound): deployments.apps "moostack-console" not found`)
		}
		token := strings.Repeat("A", 25) + string(rune('A'+len(c.tokens)))
		c.tokens = append(c.tokens, token)
		return ok(token + "\n")
	case cmd == k3s.ConsoleReadyScript:
		if c.consoleFails {
			return fail(1, "error: timed out waiting for the condition")
		}
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
		if _, installed := c.hosts["root@h1"].files[k3s.AutoDeployFile("console")]; installed && req.URL.Path == "/console/login" {
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
