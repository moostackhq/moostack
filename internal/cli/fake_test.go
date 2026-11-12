package cli

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"

	"golang.org/x/crypto/ssh"
	"gopkg.in/yaml.v3"

	"github.com/moostackhq/moostack/internal/k3s"
	"github.com/moostackhq/moostack/internal/sshclient"
)

// testHostKey is a new host key.
func testHostKey(t *testing.T) ssh.PublicKey {
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
	// workloads are the Deployments, StatefulSets, DaemonSets and
	// Ingresses of every namespace, as kubectl prints them.
	workloads []map[string]any
	// unissued is the name of a certificate that is never issued.
	unissued string
	t        *testing.T
	mu       sync.Mutex
	// hosts are the hosts by SSH target.
	hosts map[string]*fakeNode
	// nodes are the Kubernetes nodes by name.
	nodes map[string]fakeKubeNode
	// apps are the app namespaces, with the objects applied in each.
	apps map[string]map[string]bool
	// deleted are the objects deleted from app namespaces, as
	// namespace/object.
	deleted []string
	// locked says the operation lock is held.
	locked bool
	// runs are the scripts run, as "target: first line".
	runs []string
	// registry is what DialTCP connects to.
	registry string
	// manifests are what was applied, in order.
	manifests []string
	// noPods makes every app have no pods.
	noPods bool
	// rolloutFails makes every app's rollout fail.
	rolloutFails bool
	// cordoned are the Kubernetes nodes marked unschedulable.
	cordoned map[string]bool
	// servicesDown makes the cluster's services not ready, until a wait
	// for them if servicesComeUp.
	servicesDown, servicesComeUp bool
}

// fakeKubeNode is a Kubernetes node.
type fakeKubeNode struct {
	ip, arch string
}

// fakeNode is a host.
type fakeNode struct {
	target      string
	key         ssh.PublicKey
	arch        string
	memMiB      int
	addrs       []string
	routes      []string
	k3s         bool
	server      bool
	version     string
	files       map[string]string
	unreachable bool
	// failInstall makes k3s's install fail on the host, and failStart
	// its start.
	failInstall, failStart bool
	// noReach makes the host fail to reach the control plane.
	noReach bool
}

func newFakeCluster(t *testing.T) *fakeCluster {
	return &fakeCluster{t: t, hosts: map[string]*fakeNode{}, nodes: map[string]fakeKubeNode{}, apps: map[string]map[string]bool{}}
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

// hostsOf are the hosts of a map.
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

func (f *fakeConn) DialTCP(ctx context.Context, _ string) (net.Conn, error) {
	if f.c.registry == "" {
		return nil, errors.New("no registry")
	}
	return (&net.Dialer{}).DialContext(ctx, "tcp", f.c.registry)
}

func (f *fakeConn) Stream(_ context.Context, cmd string, _ io.Reader, stdout, _ io.Writer) (int, error) {
	f.c.mu.Lock()
	f.c.runs = append(f.c.runs, f.n.target+": "+cmd)
	f.c.mu.Unlock()
	fmt.Fprintln(stdout, "[pod/web-1/web] a log line")
	return 0, nil
}

func (f *fakeConn) Lock(context.Context) (held, error) {
	f.c.mu.Lock()
	defer f.c.mu.Unlock()
	if f.c.locked {
		return nil, errors.New("another mooctl operation is running on this cluster; wait for it to finish")
	}
	f.c.locked = true
	return &fakeLock{c: f.c, done: make(chan struct{})}, nil
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
	labelInScript    = regexp.MustCompile(`moostack\.dev/app=([a-z0-9-]+)`)
	namespaceFlag    = regexp.MustCompile(`-n '([^']+)'`)
	quotedObject     = regexp.MustCompile(`--request-timeout=120s '([^']+)'`)
	deletedNamespace = regexp.MustCompile(`(?m)^k3s kubectl delete namespace '([^']+)'`)
)

func ok(stdout string) (sshclient.Result, error) {
	return sshclient.Result{Stdout: []byte(stdout)}, nil
}

func okJSON(items []map[string]any) (sshclient.Result, error) {
	data, err := json.Marshal(map[string]any{"items": items})
	if err != nil {
		return fail(1, err.Error())
	}
	return ok(string(data))
}

func fail(code int, stderr string) (sshclient.Result, error) {
	return sshclient.Result{ExitCode: code, Stderr: []byte(stderr)}, nil
}

// Run answers the scripts mooctl runs, and fails any other.
func (f *fakeConn) Run(_ context.Context, cmd string, stdin io.Reader) (sshclient.Result, error) {
	var in string
	if stdin != nil {
		data, _ := io.ReadAll(stdin)
		in = string(data)
	}
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
	case strings.HasPrefix(cmd, "k3s kubectl get nodes -o json"):
		var items []map[string]any
		for name, kn := range c.nodes {
			labels := map[string]string{}
			for _, h := range c.hosts {
				if h.server && h.k3s && strings.Contains(h.files[k3s.ConfigFile], "node-name: "+name+"\n") {
					labels["node-role.kubernetes.io/control-plane"] = "true"
				}
			}
			items = append(items, map[string]any{
				"metadata": map[string]any{"name": name, "labels": labels},
				"spec":     map[string]any{"unschedulable": c.cordoned[name]},
				"status": map[string]any{
					"addresses":  []map[string]string{{"type": "InternalIP", "address": kn.ip}},
					"capacity":   map[string]string{"memory": "2021376Ki"},
					"conditions": []map[string]string{{"type": "Ready", "status": "True"}},
					"nodeInfo":   map[string]string{"kubeletVersion": K3sVersion, "architecture": kn.arch},
				},
			})
		}
		return okJSON(items)
	case strings.HasPrefix(cmd, "k3s kubectl get namespaces -l") && strings.Contains(cmd, "-o json"):
		var items []map[string]any
		for a := range c.apps {
			items = append(items, map[string]any{"metadata": map[string]string{"name": a}})
		}
		return okJSON(items)
	case strings.HasPrefix(cmd, "k3s kubectl get deployments,statefulsets,daemonsets,ingresses -A -o json"):
		return okJSON(c.workloads)
	case cmd == k3s.ServicesReadyScript:
		if c.servicesDown {
			return fail(1, "")
		}
		return ok("")
	case cmd == k3s.WaitServicesScript:
		if c.servicesDown && !c.servicesComeUp {
			return fail(1, "not ready after 5 minutes: the issuer moostack, Traefik")
		}
		c.servicesDown = false
		return ok("")
	case strings.HasPrefix(cmd, "k3s kubectl get pods -n "):
		if c.noPods {
			return ok("")
		}
		return ok("pod/web-1\n")
	case strings.HasPrefix(cmd, "mkdir -p "):
		return ok("")
	case cmd == k3s.AppliedCommitScript:
		return ok(n.files[k3s.AppliedCommitFile])
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
		n.k3s = false
		n.files = map[string]string{}
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
		if m := labelInScript.FindStringSubmatch(first); m != nil {
			if _, found := c.apps[m[1]]; found {
				return ok("namespace/" + m[1] + "\n")
			}
			return ok("")
		}
		var out []string
		for a := range c.apps {
			out = append(out, "namespace/"+a)
		}
		return ok(strings.Join(out, "\n"))
	case strings.Contains(first, "k3s kubectl get nodes") && strings.Contains(first, "architecture"):
		var arches []string
		for _, kn := range c.nodes {
			arches = append(arches, kn.arch)
		}
		return ok(strings.Join(arches, " "))
	case strings.HasPrefix(first, "k3s kubectl api-resources --namespaced=false"):
		return ok("namespaces                ns    v1                          false   Namespace\nnodes                     no    v1                          false   Node\nclusterroles                    rbac.authorization.k8s.io/v1   false   ClusterRole\n")
	case strings.HasPrefix(first, "k3s kubectl apply --server-side"):
		return f.apply(first, in)
	case strings.HasPrefix(first, "k3s kubectl delete helmchart"):
		return ok("")
	case strings.Contains(first, "get configmap "+objectsConfigMap):
		ns := namespaceFlag.FindStringSubmatch(first)[1]
		var objs []string
		for o := range c.apps[ns] {
			objs = append(objs, o)
		}
		sort.Strings(objs)
		if len(objs) == 0 {
			return ok("")
		}
		return ok(strings.Join(objs, "\n"))
	case strings.HasPrefix(first, "out=$(k3s kubectl delete -n"):
		ns := namespaceFlag.FindStringSubmatch(first)[1]
		o := quotedObject.FindStringSubmatch(first)[1]
		c.deleted = append(c.deleted, ns+"/"+o)
		delete(c.apps[ns], o)
		return ok("")
	case strings.HasPrefix(first, "k3s kubectl delete -n"):
		ns := namespaceFlag.FindStringSubmatch(first)[1]
		for _, o := range strings.Fields(first)[5:] {
			if strings.HasPrefix(o, "-") {
				continue
			}
			o = strings.Trim(o, "'")
			c.deleted = append(c.deleted, ns+"/"+o)
			delete(c.apps[ns], o)
		}
		return ok("")
	case strings.HasPrefix(first, "set -e") && strings.Contains(cmd, "rollout status"):
		if c.rolloutFails {
			return fail(1, "the rollout timed out")
		}
		return ok("")
	case strings.Contains(cmd, "kubectl wait --for=condition=Ready 'certificate/"):
		if c.unissued != "" && strings.Contains(cmd, "'certificate/"+c.unissued+"'") {
			return fail(1, "the challenge is pending")
		}
		return ok("")
	case strings.HasPrefix(first, `g=$(k3s kubectl get helmchart`):
		return ok("|\n")
	case strings.HasPrefix(first, "set -e") && strings.Contains(cmd, "delete namespace"):
		if m := deletedNamespace.FindStringSubmatch(cmd); m != nil {
			delete(c.apps, m[1])
		}
		return ok("")
	}
	c.t.Errorf("the fake cluster has no answer to a script on %s:\n%s", n.target, cmd)
	return fail(127, "fake: unknown script")
}

// apply applies, server side, the objects in manifest: an app's
// namespace is kept, its moostack-objects ConfigMap becomes its applied
// objects, and, with -n, the names of the objects are printed.
func (f *fakeConn) apply(cmd, manifest string) (sshclient.Result, error) {
	f.c.manifests = append(f.c.manifests, manifest)
	ns := ""
	if m := namespaceFlag.FindStringSubmatch(cmd); m != nil {
		ns = m[1]
	}
	dec := yaml.NewDecoder(strings.NewReader(manifest))
	var names []string
	for {
		var doc struct {
			Kind     string
			Metadata struct {
				Name      string
				Namespace string
			}
			Data map[string]string
		}
		err := dec.Decode(&doc)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fail(1, err.Error())
		}
		switch {
		case doc.Kind == "Bad":
			return fail(1, `error: resource mapping not found for name: "bad"`)
		case doc.Kind == "Namespace":
			if f.c.apps[doc.Metadata.Name] == nil {
				f.c.apps[doc.Metadata.Name] = map[string]bool{}
			}
		case doc.Kind == "ConfigMap" && doc.Metadata.Name == objectsConfigMap:
			objs := map[string]bool{}
			for _, o := range strings.Fields(doc.Data["objects"]) {
				objs[o] = true
			}
			f.c.apps[doc.Metadata.Namespace] = objs
		case ns != "":
			name := strings.ToLower(doc.Kind) + "/" + doc.Metadata.Name
			names = append(names, name)
		}
	}
	return ok(strings.Join(names, "\n"))
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
	return Env{Home: home, LocalUser: "op", Stdout: out, Stderr: out, Root: home + "/clusters", Connect: c.connect, Exit: func(int) {}}
}
