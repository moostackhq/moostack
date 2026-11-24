package k3s_test

import (
	"context"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	"github.com/moostackhq/moostack/internal/model"
	"github.com/moostackhq/moostack/internal/orchestrator"
	"github.com/moostackhq/moostack/internal/orchestrator/k3s"
)

// version is the k3s version of the test cluster's server.
const version = "v1.37.2+k3s1"

// joinToken is the token the tests' control plane has nodes join with.
const joinToken = "K10agent::node:token"

// theCluster is a cluster of the server's version whose control plane
// is at 10.0.0.2, as the backend is told of it.
func theCluster() k3s.Cluster {
	return k3s.Cluster{Version: version, Registry: "moostack.local", RegistryPort: 5000, ControlPlane: netip.MustParseAddr("10.0.0.2")}
}

// golden compares got with the golden file name of testdata, or writes
// it there with -update.
func golden(t *testing.T, name, got string) {
	t.Helper()
	path := filepath.Join("testdata", name)
	if *update {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got != string(want) {
		t.Errorf("%s; run with -update and review the change:\n got:\n%s\nwant:\n%s", path, got, want)
	}
}

func TestTheScriptsRunOnAHostAreTheGoldenOnes(t *testing.T) {
	scripts, err := k3s.Scripts(version, netip.MustParseAddr("10.0.0.2"))
	if err != nil {
		t.Fatal(err)
	}
	for name, script := range scripts {
		golden(t, "host/"+name+".golden.sh", script)
		for _, sh := range []string{"sh", "bash", "dash"} {
			if _, err := exec.LookPath(sh); err != nil {
				continue
			}
			if out, err := exec.Command(sh, "-n", "-c", script).CombinedOutput(); err != nil {
				t.Errorf("%s: %s doesn't parse: %v\n%s", sh, name, err, out)
			}
		}
	}
	if _, err := k3s.Scripts("v0.0.0+k3s1", netip.MustParseAddr("10.0.0.2")); err == nil || err.Error() != "mooctl has no checksum of k3s v0.0.0+k3s1's install script" {
		t.Errorf("the scripts of a version without a checksum: %v", err)
	}
}

// listed is files as one text: each path, then its content, ordered by
// path.
func listed(files map[string]string) string {
	var b strings.Builder
	for _, path := range slices.Sorted(maps.Keys(files)) {
		fmt.Fprintf(&b, "==> %s <==\n%s\n", path, files[path])
	}
	return b.String()
}

func TestTheFilesOfANodeAreTheGoldenOnes(t *testing.T) {
	cp := netip.MustParseAddr("10.0.0.2")
	for name, tc := range map[string]struct {
		node    model.Node
		cluster k3s.Cluster
	}{
		"control-plane": {
			model.Node{Name: "node1", ControlPlane: true, PrivateAddress: cp, Interface: "ens10", PublicAddress: netip.MustParseAddr("192.0.2.10"), Architecture: "amd64", MemoryMiB: 1966, GOGC: "25", GOMEMLIMIT: "491MiB"},
			k3s.Cluster{Version: version, Email: "ops@example.com", Registry: "moostack.local", RegistryPort: 5000},
		},
		"control-plane-self-signed": {
			model.Node{Name: "node1", ControlPlane: true, PrivateAddress: cp, Interface: "ens10", Architecture: "arm64", MemoryMiB: 16000, GOMEMLIMIT: "4000MiB"},
			k3s.Cluster{Version: version, Registry: "moostack.local", RegistryPort: 5000},
		},
		"node": {
			model.Node{Name: "node2", PrivateAddress: netip.MustParseAddr("10.0.0.3"), Interface: "ens10", PublicAddress: netip.MustParseAddr("192.0.2.11"), Architecture: "amd64", MemoryMiB: 1966, GOGC: "25", GOMEMLIMIT: "196MiB"},
			k3s.Cluster{Version: version, Email: "ops@example.com", Registry: "moostack.local", RegistryPort: 5000, ControlPlane: cp},
		},
		"node-behind-nat": {
			model.Node{Name: "node3", PrivateAddress: netip.MustParseAddr("10.0.0.4"), Interface: "eth1", Architecture: "arm64", MemoryMiB: 16000, GOMEMLIMIT: "1600MiB"},
			k3s.Cluster{Version: version, Registry: "registry.test", RegistryPort: 5001, ControlPlane: cp},
		},
	} {
		golden(t, "host/files."+name+".golden.txt", listed(k3s.NodeFiles(tc.node, tc.cluster, cp, "the-token")))
	}
}

func TestTheConsolesObjects(t *testing.T) {
	got := k3s.ConsoleObjects("moostack.local/kube-system/console@sha256:0a1b2c", []netip.Addr{netip.MustParseAddr("203.0.113.7"), netip.MustParseAddr("10.0.0.2")})
	golden(t, "console.golden.yaml", got)
	kinds := []string{}
	documents := yaml.NewDecoder(strings.NewReader(got))
	for {
		var object struct {
			Kind     string
			Metadata struct{ Namespace string }
		}
		if err := documents.Decode(&object); err != nil {
			if err != io.EOF {
				t.Fatalf("the objects aren't YAML: %v", err)
			}
			break
		}
		if object.Metadata.Namespace != "kube-system" && !strings.HasPrefix(object.Kind, "ClusterRole") {
			t.Errorf("the %s is in the namespace %q", object.Kind, object.Metadata.Namespace)
		}
		kinds = append(kinds, object.Kind)
	}
	if got := strings.Join(kinds, " "); got != "ServiceAccount ClusterRole ClusterRoleBinding Deployment Service IngressRoute" {
		t.Errorf("the objects are %s", got)
	}
	if one := k3s.ConsoleObjects("moostack.local/kube-system/console@sha256:0a1b2c", []netip.Addr{netip.MustParseAddr("10.0.0.2")}); !strings.Contains(one, "match: (Host(`10.0.0.2`)) && (Path(`/console`) || PathPrefix(`/console/`))") {
		t.Errorf("the route of a node with one address:\n%s", one)
	}
}

// host is a host as the backend's scripts find it and leave it. It
// answers a control plane's kubeconfig and join token, and connects to
// the test cluster's API.
type host struct {
	t *testing.T
	// facts is what the host says it is.
	facts string
	// failAt is the step that fails on the host: facts, reach, mkdir,
	// write N for the Nth file, install, start or uninstall.
	failAt string
	// onInstall and onStart, if set, run as k3s is installed and
	// started.
	onInstall, onStart func()
	// hangs makes the start of k3s wait until its context ends, and slow
	// makes the wait for its download give up before it is there.
	hangs, slow bool
	// downloaded, if set, holds the wait for k3s's download until it is
	// closed.
	downloaded chan struct{}

	// attempted is closed once k3s's install has run, or failed.
	attempted chan struct{}
	once      sync.Once

	mu      sync.Mutex
	files   map[string]string
	k3s     bool
	ran     []string
	uploads int
}

// aHost is a 2 GB amd64 host with a public address on eth0 and the
// private address on ens10.
func aHost(t *testing.T, private string) *host {
	return &host{t: t, files: map[string]string{}, attempted: make(chan struct{}), facts: hostFacts("x86_64", 1966, "eth0 192.0.2.10/32", "ens10 "+private+"/32") + "route=eth0\n"}
}

// hostFacts is what a host of the architecture, memory and addresses
// prints of itself, with systemd and every command a node needs.
func hostFacts(arch string, memMiB int, addrs ...string) string {
	out := fmt.Sprintf("arch=%s\nmem_kib=%d\nsystemd=yes\nhas=curl\nhas=flock\nhas=ip\n", arch, memMiB*1024)
	for _, a := range addrs {
		out += "addr=" + a + "\n"
	}
	return out
}

func (h *host) fails(step string) error {
	if h.failAt == step {
		return fmt.Errorf("exit 1: %s failed", step)
	}
	return nil
}

func (h *host) Run(ctx context.Context, cmd string, _ io.Reader) (string, error) {
	if ctx.Err() != nil {
		return "", ctx.Err()
	}
	// The wait for k3s's download ends once the install has it, or has
	// ended without it, or at once on a host that downloads slowly.
	if strings.HasPrefix(cmd, "i=0\nuntil [ -e /usr/local/bin/k3s ]") && !h.slow {
		for _, wait := range []chan struct{}{h.attempted, h.downloaded} {
			if wait == nil {
				continue
			}
			select {
			case <-wait:
			case <-ctx.Done():
				return "", ctx.Err()
			}
		}
	}
	if strings.Contains(cmd, "INSTALL_K3S_EXEC=") {
		defer h.once.Do(func() { close(h.attempted) })
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.ran = append(h.ran, cmd)
	switch {
	case cmd == "cat /etc/rancher/k3s/k3s.yaml":
		return string(kubeconfig), nil
	case cmd == "cat /var/lib/rancher/k3s/server/agent-token":
		return joinToken + "\n", nil
	case strings.HasPrefix(cmd, `echo "arch=`):
		return h.facts, h.fails("facts")
	case strings.HasPrefix(cmd, "mkdir -p "):
		return "", h.fails("mkdir")
	case strings.HasPrefix(cmd, "curl -sk --max-time 10"):
		return "", h.fails("reach")
	case strings.HasPrefix(cmd, "i=0\nuntil [ -e /usr/local/bin/k3s ]"):
		if !h.k3s || h.slow {
			return "", errors.New("exit 1")
		}
		return "", nil
	case strings.Contains(cmd, "INSTALL_K3S_EXEC="):
		if err := h.fails("install"); err != nil {
			return "", err
		}
		h.k3s = true
		if h.onInstall != nil {
			h.onInstall()
		}
		return "", nil
	case strings.HasPrefix(cmd, "systemctl start k3s-agent"):
		if h.hangs {
			h.mu.Unlock()
			<-ctx.Done()
			h.mu.Lock()
			return "", ctx.Err()
		}
		if err := h.fails("start"); err != nil {
			return "", err
		}
		if h.onStart != nil {
			h.onStart()
		}
		return "", nil
	case strings.Contains(cmd, "uninstall.sh"):
		if err := h.fails("uninstall"); err != nil {
			return "", err
		}
		h.k3s = false
		for path := range h.files {
			if strings.HasPrefix(path, "/var/lib/rancher/k3s/") || strings.Contains(cmd, " "+path) {
				delete(h.files, path)
			}
		}
		return "", nil
	}
	h.t.Errorf("the host has no answer to:\n%s", cmd)
	return "", errors.New("exit 127: unknown command")
}

func (h *host) RunStoppable(ctx context.Context, cmd string) (string, error) {
	return h.Run(ctx, cmd, nil)
}

func (h *host) Upload(ctx context.Context, file string, r io.Reader, mode fs.FileMode) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	content, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.uploads++
	if err := h.fails(fmt.Sprintf("write %d", h.uploads)); err != nil {
		return err
	}
	if mode != 0o600 {
		h.t.Errorf("%s is written with the mode %o, want 600", file, mode)
	}
	h.files[file] = string(content)
	return nil
}

func (h *host) DialTCP(ctx context.Context, _ string) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(ctx, "tcp", strings.TrimPrefix(adminConfig.Host, "https://"))
}

// left is what the host has: its files, and k3s if it is installed.
func (h *host) left() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	has := slices.Sorted(maps.Keys(h.files))
	if h.k3s {
		has = append(has, "k3s")
	}
	return strings.Join(has, " ")
}

// commands are the commands run on the host, each cut to its first
// line.
func (h *host) commands() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []string
	for _, cmd := range h.ran {
		first, _, _ := strings.Cut(cmd, "\n")
		out = append(out, first)
	}
	return out
}

// registers makes the cluster have the node called name, Ready at
// address and with room for pods, until the test ends.
func registers(t *testing.T, name, address string) {
	t.Helper()
	n := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{"moostack.test/node": name}},
		// Only the pods a test makes for the node are scheduled on it.
		Spec: corev1.NodeSpec{Taints: []corev1.Taint{{Key: "moostack.test/node", Effect: corev1.TaintEffectNoSchedule}}},
	}
	if _, err := admin.CoreV1().Nodes().Create(context.Background(), n, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Errorf("registering %s: %v", name, err)
		return
	}
	t.Cleanup(func() { admin.CoreV1().Nodes().Delete(context.Background(), name, metav1.DeleteOptions{}) })
	keep(t, func(ctx context.Context) error {
		node, err := admin.CoreV1().Nodes().Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		room := corev1.ResourceList{corev1.ResourcePods: resource.MustParse("110"), corev1.ResourceCPU: resource.MustParse("4"), corev1.ResourceMemory: resource.MustParse("8Gi")}
		node.Status.Capacity, node.Status.Allocatable = room, room
		node.Status.Addresses = []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: address}}
		node.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue, Reason: "KubeletReady", LastHeartbeatTime: metav1.Now(), LastTransitionTime: node.CreationTimestamp}}
		if _, err := admin.CoreV1().Nodes().UpdateStatus(ctx, node, metav1.UpdateOptions{}); err != nil && !apierrors.IsConflict(err) && !apierrors.IsNotFound(err) {
			return err
		}
		return nil
	})
}

func hasNode(t *testing.T, name string) bool {
	t.Helper()
	_, err := admin.CoreV1().Nodes().Get(context.Background(), name, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		t.Fatal(err)
	}
	return err == nil
}

// told collects the steps a call tells.
type told struct {
	mu    sync.Mutex
	steps []orchestrator.Step
}

func (s *told) step(step orchestrator.Step) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.steps = append(s.steps, step)
}

func (s *told) all() []orchestrator.Step {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.steps)
}

func TestPreflightReadsTheNodeAHostCanBeAndChangesNothingOnIt(t *testing.T) {
	h := aHost(t, "10.0.0.2")
	b := k3s.Through(nil, k3s.Cluster{Version: version})
	got, err := b.Preflight(within(t, time.Minute), h, orchestrator.NodeRequest{Name: "node1", ControlPlane: true})
	if err != nil {
		t.Fatalf("Preflight: %v", err)
	}
	want := model.Node{
		Name: "node1", ControlPlane: true,
		PrivateAddress: netip.MustParseAddr("10.0.0.2"), Interface: "ens10", PublicAddress: netip.MustParseAddr("192.0.2.10"),
		Architecture: "amd64", MemoryMiB: 1966, GOGC: "25", GOMEMLIMIT: "491MiB",
	}
	if got != want {
		t.Errorf("Preflight:\n got %+v\nwant %+v", got, want)
	}
	if cmds := h.commands(); len(cmds) != 1 || h.left() != "" {
		t.Errorf("Preflight ran %q and left %q on the host, want the read of its facts alone", cmds, h.left())
	}

	// A host that joins is asked to reach the control plane.
	joining := aHost(t, "10.0.0.3")
	joining.facts = hostFacts("aarch64", 16000, "eth0 10.0.0.3/24") + "route=eth0\n"
	b = k3s.Through(aHost(t, "10.0.0.2"), theCluster())
	got, err = b.Preflight(within(t, time.Minute), joining, orchestrator.NodeRequest{Name: "node2"})
	if err != nil {
		t.Fatalf("Preflight of a host that joins: %v", err)
	}
	want = model.Node{Name: "node2", PrivateAddress: netip.MustParseAddr("10.0.0.3"), Interface: "eth0", Architecture: "arm64", MemoryMiB: 16000, GOMEMLIMIT: "1600MiB"}
	if got != want {
		t.Errorf("Preflight of a host that joins:\n got %+v\nwant %+v", got, want)
	}
	cmds := joining.commands()
	if len(cmds) != 2 || cmds[1] != "curl -sk --max-time 10 -o /dev/null https://10.0.0.2:6443/ping || {" || joining.left() != "" {
		t.Errorf("Preflight ran %q and left %q on the host that joins, want its facts read and the control plane reached at 10.0.0.2", cmds, joining.left())
	}
}

// apiOfVersion is a control plane whose API says it runs the k3s
// version.
func apiOfVersion(t *testing.T, version string) orchestrator.Host {
	t.Helper()
	api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/version" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"gitVersion":%q}`, version)
	}))
	t.Cleanup(api.Close)
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: api.Certificate().Raw})
	config := clientcmdapi.NewConfig()
	config.Clusters["c"] = &clientcmdapi.Cluster{Server: "https://127.0.0.1:6443", CertificateAuthorityData: ca}
	config.AuthInfos["u"] = &clientcmdapi.AuthInfo{Token: "t"}
	config.Contexts["x"] = &clientcmdapi.Context{Cluster: "c", AuthInfo: "u"}
	config.CurrentContext = "x"
	text, err := clientcmd.Write(*config)
	if err != nil {
		t.Fatal(err)
	}
	return hostTo{&controlPlane{kubeconfig: string(text)}, api.Listener.Addr().String()}
}

func TestPreflightRefusesAHostThatCantBeTheNodeAskedFor(t *testing.T) {
	facts := func(lines string) func(h *host) { return func(h *host) { h.facts = lines } }
	with := hostFacts("x86_64", 1966, "eth0 192.0.2.10/32", "ens10 10.0.0.3/32")
	for name, tc := range map[string]struct {
		host    func(h *host)
		want    orchestrator.NodeRequest
		cluster func(c *k3s.Cluster)
		cp      func(t *testing.T) orchestrator.Host
		err     string
		// first makes the refusal come before anything is run on the host.
		first bool
	}{
		"a host with k3s":             {host: facts(with + "k3s=yes\n"), err: "it can't be a node: it has k3s, or k3s's files, already: run k3s-uninstall.sh or k3s-agent-uninstall.sh on it, and remove /etc/rancher/k3s"},
		"a host too small for a node": {host: facts(hostFacts("x86_64", 805, "ens10 10.0.0.3/32")), err: "it can't be a node: it has 805 MiB of memory, and a node needs at least 806 MiB"},
		"a host too small for a control plane": {
			host: facts(hostFacts("x86_64", 1205, "ens10 10.0.0.3/32")), want: orchestrator.NodeRequest{ControlPlane: true},
			err: "it can't be a control plane: it has 1205 MiB of memory, and a control plane needs at least 1206 MiB",
		},
		"another architecture, no systemd and no curl": {
			host: facts("arch=riscv64\nmem_kib=2013456\nhas=flock\nhas=ip\naddr=ens10 10.0.0.3/32\n"),
			err:  "it can't be a node: its architecture is riscv64, not amd64 or arm64; it doesn't run systemd; it has no curl command",
		},
		"facts that can't be read":            {host: func(h *host) { h.failAt = "facts" }, err: "exit 1: facts failed"},
		"a memory that is no number":          {host: facts("arch=x86_64\nmem_kib=lots\n"), err: `the host's memory: "lots"`},
		"a public private address":            {want: orchestrator.NodeRequest{PrivateAddress: netip.MustParseAddr("203.0.113.5")}, err: "203.0.113.5 isn't a private IPv4 address (10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16)"},
		"a private address the host lacks":    {want: orchestrator.NodeRequest{PrivateAddress: netip.MustParseAddr("10.9.9.9")}, err: "the host has no address 10.9.9.9"},
		"an interface with a tab in its name": {host: facts("arch=x86_64\nmem_kib=2013456\nsystemd=yes\nhas=curl\nhas=flock\nhas=ip\naddr=ens\t10 10.0.0.3/32\n"), err: `"ens\t10" is no name of a network interface`},
		"no private address":                  {host: facts(hostFacts("x86_64", 1966, "eth0 192.0.2.10/32")), err: "the host has no private IPv4 address (10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16); attach it to a private network, or name its address with --private-address"},
		"several private addresses":           {host: facts(hostFacts("x86_64", 1966, "ens10 10.0.0.3/32", "ens11 10.1.0.3/32")), err: "the host has several private addresses, 10.0.0.3 on ens10, 10.1.0.3 on ens11; name the one its nodes share with --private-address"},
		"a host that doesn't reach the API":   {host: func(h *host) { h.failAt = "reach" }, err: "exit 1: reach failed"},
		"a k3s version without a checksum":    {cluster: func(c *k3s.Cluster) { c.Version = "v1.38.0+k3s1" }, err: "mooctl has no checksum of k3s v1.38.0+k3s1's install script", first: true},
		"a cluster of another k3s version":    {cp: func(t *testing.T) orchestrator.Host { return apiOfVersion(t, "v1.36.0+k3s1") }, err: "the control plane runs k3s v1.36.0+k3s1, and the cluster is to run " + version},
		"a control plane that can't be read":  {cp: func(*testing.T) orchestrator.Host { return &controlPlane{} }, err: "reading the control plane's /etc/rancher/k3s/k3s.yaml: exit 1: cat: /etc/rancher/k3s/k3s.yaml: No such file or directory"},
		"a cluster without a control plane":   {cp: func(*testing.T) orchestrator.Host { return nil }, err: "the cluster has no control plane to reach it through"},
	} {
		t.Run(name, func(t *testing.T) {
			h := aHost(t, "10.0.0.3")
			if tc.host != nil {
				tc.host(h)
			}
			cluster := theCluster()
			if tc.cluster != nil {
				tc.cluster(&cluster)
			}
			var cp orchestrator.Host = aHost(t, "10.0.0.2")
			if tc.cp != nil {
				cp = tc.cp(t)
			}
			tc.want.Name = "node2"
			got, err := k3s.Through(cp, cluster).Preflight(within(t, time.Minute), h, tc.want)
			if err == nil || err.Error() != tc.err {
				t.Errorf("Preflight: %v\nwant %s", err, tc.err)
			}
			if got != (model.Node{}) {
				t.Errorf("Preflight refused and returned the node %+v", got)
			}
			if h.left() != "" || (tc.first && len(h.commands()) != 0) {
				t.Errorf("the refusal left %q on the host after the commands %q", h.left(), h.commands())
			}
			for _, cmd := range h.commands() {
				if !strings.HasPrefix(cmd, `echo "arch=`) && !strings.HasPrefix(cmd, "curl -sk --max-time 10") {
					t.Errorf("Preflight ran %q on the host", cmd)
				}
			}
		})
	}
}

func TestACallOfANodeOperationOnAnEndedContextFailsWithItsCauseAndRunsNothing(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("the operation lock is lost")
	cancel(cause)
	h, cp := aHost(t, "10.0.0.3"), aHost(t, "10.0.0.2")
	b := k3s.Through(cp, theCluster())
	var steps told
	_, preflight := b.Preflight(ctx, h, orchestrator.NodeRequest{Name: "node2"})
	_, token := b.ConsoleToken(ctx)
	for method, err := range map[string]error{
		"Preflight":    preflight,
		"AddNode":      b.AddNode(ctx, h, model.Node{Name: "node2", PrivateAddress: netip.MustParseAddr("10.0.0.3")}, steps.step),
		"RemoveNode":   b.RemoveNode(ctx, "node2", h, orchestrator.RemoveOptions{}, steps.step),
		"Uninstall":    b.Uninstall(ctx, h, model.Node{Name: "node2"}),
		"RunConsole":   b.RunConsole(ctx, "example.test/console:1", nil),
		"ConsoleToken": token,
	} {
		if !errors.Is(err, cause) {
			t.Errorf("%s on an ended context: %v, want its cause", method, err)
		}
	}
	if len(h.commands())+len(cp.commands()) != 0 || len(steps.all()) != 0 {
		t.Errorf("the calls ran %q on the host and %q on the control plane, and told the steps %v", h.commands(), cp.commands(), steps.all())
	}
}

func TestAddNodeMakesAHostTheControlPlaneTheBackendThenReachesTheClusterThrough(t *testing.T) {
	name := unique("cp")
	h := aHost(t, "10.0.0.2")
	h.onInstall = func() { registers(t, name, "10.0.0.2") }
	cluster := k3s.Cluster{Version: version, Email: "ops@example.com", Registry: "moostack.local", RegistryPort: 5000}
	b := k3s.Through(nil, cluster)
	if _, err := b.Nodes(within(t, time.Minute)); err == nil || err.Error() != "the cluster has no control plane to reach it through" {
		t.Fatalf("Nodes before the cluster has a control plane: %v", err)
	}
	n, err := b.Preflight(within(t, time.Minute), h, orchestrator.NodeRequest{Name: name, ControlPlane: true})
	if err != nil {
		t.Fatal(err)
	}
	var steps told
	if err := b.AddNode(within(t, time.Minute), h, n, steps.step); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if want := []orchestrator.Step{orchestrator.Installing, orchestrator.Starting, orchestrator.WaitingForNode}; !slices.Equal(steps.all(), want) {
		t.Errorf("the steps were %v, want Installing, Starting, WaitingForNode", steps.all())
	}
	h.mu.Lock()
	config := h.files["/etc/rancher/k3s/config.yaml"]
	h.mu.Unlock()
	token := regexp.MustCompile(`(?m)^agent-token: (\S{20,})$`).FindStringSubmatch(config)
	if token == nil {
		t.Fatalf("the control plane's config has no token for nodes to join with:\n%s", config)
	}
	h.mu.Lock()
	if want := k3s.NodeFiles(n, cluster, n.PrivateAddress, token[1]); !maps.Equal(h.files, want) {
		t.Errorf("the host has the files\n%s\nwant\n%s", listed(h.files), listed(want))
	}
	h.mu.Unlock()
	var installs []string
	for _, cmd := range h.ran {
		if strings.Contains(cmd, "INSTALL_K3S_EXEC=") {
			installs = append(installs, cmd)
		}
	}
	scripts, _ := k3s.Scripts(version, n.PrivateAddress)
	if len(installs) != 1 || installs[0] != scripts["install-control-plane"] || !h.k3s {
		t.Errorf("the installs run on the host were %q, want the control plane's once", installs)
	}
	if slices.ContainsFunc(h.commands(), func(cmd string) bool { return strings.HasPrefix(cmd, "systemctl start") }) {
		t.Error("k3s was started on the control plane beside its install")
	}

	nodes, err := b.Nodes(within(t, time.Minute))
	if err != nil || !slices.ContainsFunc(nodes, func(s model.NodeState) bool { return s.Name == name && s.Address == "10.0.0.2" }) {
		t.Errorf("Nodes through the new control plane: %v, %v, want %s among them", nodes, err, name)
	}
	joinName := unique("joins")
	joining := aHost(t, "10.0.0.3")
	joining.onStart = func() { registers(t, joinName, "10.0.0.3") }
	joined := model.Node{Name: joinName, PrivateAddress: netip.MustParseAddr("10.0.0.3"), Interface: "ens10", MemoryMiB: 1966}
	if err := b.AddNode(within(t, time.Minute), joining, joined, func(orchestrator.Step) {}); err != nil {
		t.Fatalf("AddNode of a node, by the backend that added the control plane: %v", err)
	}
	if want := k3s.NodeFiles(joined, cluster, n.PrivateAddress, joinToken); !maps.Equal(joining.files, want) {
		t.Errorf("the node that joins the new control plane has the files\n%s\nwant\n%s", listed(joining.files), listed(want))
	}
	other := aHost(t, "10.0.0.9")
	if err := b.AddNode(within(t, time.Minute), other, model.Node{Name: unique("cp"), ControlPlane: true, PrivateAddress: netip.MustParseAddr("10.0.0.9")}, steps.step); err == nil || err.Error() != "the cluster has a control plane already" || other.left() != "" {
		t.Errorf("AddNode of a second control plane: %v, with %q on the host", err, other.left())
	}
}

func TestTwoControlPlanesGetTwoTokens(t *testing.T) {
	tokens := map[string]bool{}
	for range 2 {
		name := unique("cp")
		h := aHost(t, "10.0.0.2")
		h.onInstall = func() { registers(t, name, "10.0.0.2") }
		n := model.Node{Name: name, ControlPlane: true, PrivateAddress: netip.MustParseAddr("10.0.0.2"), Interface: "ens10", MemoryMiB: 1966}
		if err := k3s.Through(nil, k3s.Cluster{Version: version}).AddNode(within(t, time.Minute), h, n, func(orchestrator.Step) {}); err != nil {
			t.Fatal(err)
		}
		tokens[h.files["/etc/rancher/k3s/config.yaml"]] = true
	}
	if len(tokens) != 2 {
		t.Error("two control planes have the same config, token included")
	}
}

func TestAddNodeJoinsAHostToTheControlPlane(t *testing.T) {
	name := unique("joins")
	cp, h := aHost(t, "10.0.0.2"), aHost(t, "10.0.0.3")
	h.onStart = func() { registers(t, name, "10.0.0.3") }
	b := k3s.Through(cp, theCluster())
	n, err := b.Preflight(within(t, time.Minute), h, orchestrator.NodeRequest{Name: name})
	if err != nil {
		t.Fatal(err)
	}
	var steps told
	if err := b.AddNode(within(t, time.Minute), h, n, steps.step); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if want := []orchestrator.Step{orchestrator.Installing, orchestrator.Starting, orchestrator.WaitingForNode}; !slices.Equal(steps.all(), want) {
		t.Errorf("the steps were %v, want Installing, Starting, WaitingForNode", steps.all())
	}
	if want := k3s.NodeFiles(n, theCluster(), netip.MustParseAddr("10.0.0.2"), joinToken); !maps.Equal(h.files, want) {
		t.Errorf("the host has the files\n%s\nwant\n%s", listed(h.files), listed(want))
	}
	scripts, _ := k3s.Scripts(version, netip.MustParseAddr("10.0.0.2"))
	var ran []string
	for _, cmd := range h.ran {
		switch cmd {
		case scripts["install-node"]:
			ran = append(ran, "install")
		case scripts["start"]:
			ran = append(ran, "start")
		}
	}
	if !slices.Equal(ran, []string{"install", "start"}) || !h.k3s {
		t.Errorf("of the node's install and start the host ran %q, want the install, then the start", ran)
	}
	if cp.left() != "" || slices.ContainsFunc(cp.commands(), func(cmd string) bool { return !strings.HasPrefix(cmd, "cat ") }) {
		t.Errorf("the join left %q on the control plane and ran %q there, want its token and kubeconfig read", cp.left(), cp.commands())
	}
}

func TestAddNodeReturnsOnceTheNodeIsReadyAtItsPrivateAddress(t *testing.T) {
	name := unique("elsewhere")
	h := aHost(t, "10.0.0.3")
	h.onStart = func() { registers(t, name, "10.0.0.99") }
	b := k3s.Through(aHost(t, "10.0.0.2"), theCluster())
	n := model.Node{Name: name, PrivateAddress: netip.MustParseAddr("10.0.0.3"), Interface: "ens10", MemoryMiB: 1966}
	err := b.AddNode(within(t, 5*time.Second), h, n, func(orchestrator.Step) {})
	if err == nil || !strings.HasPrefix(err.Error(), name+" isn't Ready at 10.0.0.3 after 10m0s\n") || !strings.Contains(err.Error(), "\nnode "+name+" at 10.0.0.99: ready\n") {
		t.Errorf("AddNode of a node that is Ready at another address: %v\nwant that it isn't Ready at 10.0.0.3, and the cluster's nodes", err)
	}
	if h.left() != "" || hasNode(t, name) {
		t.Errorf("the failed join left %q on the host, and the node in the cluster: %t", h.left(), hasNode(t, name))
	}
}

func TestAStartThatHangsEndsOnceTheNodeIsReady(t *testing.T) {
	name := unique("hangs")
	h := aHost(t, "10.0.0.3")
	h.hangs = true
	h.onInstall = func() { registers(t, name, "10.0.0.3") }
	b := k3s.Through(aHost(t, "10.0.0.2"), theCluster())
	n := model.Node{Name: name, PrivateAddress: netip.MustParseAddr("10.0.0.3"), Interface: "ens10", MemoryMiB: 1966}
	if err := b.AddNode(within(t, time.Minute), h, n, func(orchestrator.Step) {}); err != nil {
		t.Errorf("AddNode of a node that is Ready while its start still waits: %v", err)
	}
}

// A join fails at each of its steps in turn; a control plane's install
// at each of its own.
func TestAnAddNodeThatFailsAtAnyStepLeavesNothingOnTheHostOrInTheCluster(t *testing.T) {
	type failure struct {
		controlPlane bool
		at           string
		// never makes the node never register.
		never bool
		err   string
	}
	var cases []failure
	for i := range 4 {
		cases = append(cases, failure{at: fmt.Sprintf("write %d", i+1), err: "writing "})
	}
	for i := range 7 {
		cases = append(cases, failure{controlPlane: true, at: fmt.Sprintf("write %d", i+1), err: "writing "})
	}
	cases = append(cases,
		failure{at: "mkdir", err: "writing /etc/rancher/k3s/config.yaml: exit 1: mkdir failed"},
		failure{at: "install", err: "installing k3s: exit 1: install failed"},
		failure{controlPlane: true, at: "install", err: "installing k3s: exit 1: install failed"},
		failure{at: "start", err: "starting k3s: exit 1: start failed"},
		failure{never: true, err: " isn't Ready at 10.0.0.3 after 10m0s"},
		failure{controlPlane: true, never: true, err: " isn't Ready at 10.0.0.3 after 10m0s"},
	)
	for _, tc := range cases {
		t.Run(fmt.Sprintf("control plane %t, %s, never ready %t", tc.controlPlane, tc.at, tc.never), func(t *testing.T) {
			name := unique("fails")
			h := aHost(t, "10.0.0.3")
			h.failAt = tc.at
			// A node that joins is in the cluster from its start on.
			if !tc.never {
				h.onStart = func() { registers(t, name, "10.0.0.3") }
			}
			var cp orchestrator.Host
			cluster := k3s.Cluster{Version: version}
			if !tc.controlPlane {
				cp, cluster = aHost(t, "10.0.0.2"), theCluster()
				if tc.at == "start" {
					h.onInstall = func() { registers(t, name, "10.0.0.99") }
				}
			}
			b := k3s.Through(cp, cluster)
			n := model.Node{Name: name, ControlPlane: tc.controlPlane, PrivateAddress: netip.MustParseAddr("10.0.0.3"), Interface: "ens10", MemoryMiB: 1966, GOMEMLIMIT: "196MiB"}
			var steps told
			d := time.Minute
			if tc.never {
				d = 4 * time.Second
			}
			err := b.AddNode(within(t, d), h, n, steps.step)
			if err == nil || !strings.Contains(err.Error(), tc.err) {
				t.Fatalf("AddNode: %v, want an error with %q", err, tc.err)
			}
			if h.left() != "" {
				t.Errorf("the host still has %s", h.left())
			}
			if hasNode(t, name) {
				t.Error("the cluster still has the node")
			}
			if all := steps.all(); len(all) == 0 || all[len(all)-1] != orchestrator.Uninstalling {
				t.Errorf("the steps were %v, want Uninstalling last", all)
			}
			if all := steps.all(); tc.at == "install" && !slices.Equal(all, []orchestrator.Step{orchestrator.Installing, orchestrator.Uninstalling}) {
				t.Errorf("the steps of an install that failed were %v, want Installing, Uninstalling", all)
			}
			if tc.controlPlane {
				if _, err := b.Nodes(within(t, time.Minute)); err == nil || err.Error() != "the cluster has no control plane to reach it through" {
					t.Errorf("Nodes after a control plane's failed install: %v, want no control plane to reach the cluster through", err)
				}
			}
		})
	}
}

func TestAnAddNodeWhoseContextEndsUninstallsAllTheSameAndFailsWithTheCause(t *testing.T) {
	name := unique("cut")
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("the operation lock is lost")
	h := aHost(t, "10.0.0.3")
	h.onInstall = func() { cancel(cause) }
	b := k3s.Through(aHost(t, "10.0.0.2"), theCluster())
	n := model.Node{Name: name, PrivateAddress: netip.MustParseAddr("10.0.0.3"), Interface: "ens10", MemoryMiB: 1966}
	var steps told
	if err := b.AddNode(ctx, h, n, steps.step); err == nil || err.Error() != cause.Error() {
		t.Errorf("AddNode: %v, want the context's cause alone", err)
	}
	if h.left() != "" || !slices.Contains(steps.all(), orchestrator.Uninstalling) {
		t.Errorf("the host still has %q after the steps %v", h.left(), steps.all())
	}
}

func TestAnUndoThatFailsIsInTheErrorBesideWhatFailedFirst(t *testing.T) {
	h := aHost(t, "10.0.0.3")
	h.failAt = "install"
	bad := &failingHost{host: h, at: "uninstall.sh"}
	b := k3s.Through(aHost(t, "10.0.0.2"), theCluster())
	name := unique("stuck")
	n := model.Node{Name: name, PrivateAddress: netip.MustParseAddr("10.0.0.3"), Interface: "ens10", MemoryMiB: 1966}
	err := b.AddNode(within(t, time.Minute), bad, n, func(orchestrator.Step) {})
	want := "installing k3s: exit 1: install failed\nuninstalling from " + name + ": exit 3: k3s-agent-uninstall.sh failed"
	if err == nil || err.Error() != want {
		t.Errorf("AddNode: %v\nwant %s", err, want)
	}
}

// failingHost is a host on which the commands that hold at fail.
type failingHost struct {
	*host
	at string
}

func (f *failingHost) Run(ctx context.Context, cmd string, stdin io.Reader) (string, error) {
	if strings.Contains(cmd, f.at) {
		return "", errors.New("exit 3: k3s-agent-uninstall.sh failed")
	}
	return f.host.Run(ctx, cmd, stdin)
}

func TestANameANodeCantHaveIsRefusedBeforeAnythingRunsAndIsInNoCommand(t *testing.T) {
	for _, name := range []string{
		"x'; touch /tmp/pwned; '", `$(reboot)`, "`id`", "a b", "a\nrm -rf /", "\nnode1", "node1\t", `"; halt; "`, "node;halt", "-rf", "a|b", "a&b", "${HOME}", "~root", "*", "node1 # x", `a\b`, "Node1", "", strings.Repeat("a", 254),
	} {
		cp, h := aHost(t, "10.0.0.2"), aHost(t, "10.0.0.3")
		b := k3s.Through(cp, theCluster())
		_, err := b.Preflight(within(t, time.Minute), h, orchestrator.NodeRequest{Name: name})
		if err == nil || !strings.HasPrefix(err.Error(), fmt.Sprintf("a node can't be called %q: ", name)) {
			t.Errorf("%q: Preflight: %v, want a refusal of the name", name, err)
		}
		var steps told
		n := model.Node{Name: name, PrivateAddress: netip.MustParseAddr("10.0.0.3"), Interface: "ens10", MemoryMiB: 1966}
		if err := b.AddNode(within(t, time.Minute), h, n, steps.step); err == nil || !strings.HasPrefix(err.Error(), fmt.Sprintf("a node can't be called %q: ", name)) {
			t.Errorf("%q: AddNode: %v, want a refusal of the name", name, err)
		}
		if len(h.commands())+len(cp.commands()) != 0 || h.left() != "" || len(steps.all()) != 0 {
			t.Errorf("%q: the refusals ran %q on the host and %q on the control plane, after the steps %v", name, h.commands(), cp.commands(), steps.all())
		}
		b.Uninstall(within(t, time.Minute), h, n)
		b.RemoveNode(within(t, time.Minute), name, h, orchestrator.RemoveOptions{}, func(orchestrator.Step) {})
		for _, on := range []*host{cp, h} {
			for _, cmd := range on.ran {
				if name != "" && strings.Contains(cmd, name) {
					t.Errorf("%q is in a command run on a host:\n%s", name, cmd)
				}
			}
		}
	}
	h := aHost(t, "10.0.0.3")
	n := model.Node{Name: "node2", PrivateAddress: netip.MustParseAddr("10.0.0.3"), Interface: "ens10\nserver: https://evil", MemoryMiB: 1966}
	if err := k3s.Through(aHost(t, "10.0.0.2"), theCluster()).AddNode(within(t, time.Minute), h, n, func(orchestrator.Step) {}); err == nil || err.Error() != `"ens10\nserver: https://evil" is no name of a network interface` || len(h.commands()) != 0 {
		t.Errorf("AddNode on an interface with a line break in its name: %v, after %q on the host", err, h.commands())
	}
}

func TestUninstallRunsTheRolesUninstallAndFailsAsItDoes(t *testing.T) {
	scripts, _ := k3s.Scripts(version, netip.MustParseAddr("10.0.0.2"))
	b := k3s.Through(nil, k3s.Cluster{Version: version})
	for name, controlPlane := range map[string]bool{"uninstall-control-plane": true, "uninstall-node": false} {
		h := aHost(t, "10.0.0.3")
		h.k3s = true
		maps.Copy(h.files, k3s.NodeFiles(model.Node{Name: "n", ControlPlane: controlPlane, PrivateAddress: netip.MustParseAddr("10.0.0.3")}, theCluster(), netip.MustParseAddr("10.0.0.2"), "t"))
		if err := b.Uninstall(within(t, time.Minute), h, model.Node{Name: "n", ControlPlane: controlPlane}); err != nil {
			t.Errorf("%s: %v", name, err)
		}
		if len(h.ran) != 1 || h.ran[0] != scripts[name] || h.left() != "" {
			t.Errorf("%s: the host ran %q and still has %q", name, h.ran, h.left())
		}
		h.failAt = "uninstall"
		if err := b.Uninstall(within(t, time.Minute), h, model.Node{Name: "n", ControlPlane: controlPlane}); err == nil || err.Error() != "exit 1: uninstall failed" {
			t.Errorf("%s that fails: %v", name, err)
		}
	}
}

// writeUnder writes the file at path under root.
func writeUnder(t *testing.T, root, path, content string, mode os.FileMode) {
	t.Helper()
	path = filepath.Join(root, path)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatal(err)
	}
}

// uninstallUnder runs the uninstall script with its paths under root and
// the commands of root's fake directory first on its PATH, and returns
// its exit status and output. It refuses a script that names a path
// elsewhere.
func uninstallUnder(root, script string) (int, string) {
	script = regexp.MustCompile(`(^|\s)(/usr/local/bin/|/etc/|/var/lib/)`).ReplaceAllString(script, "${1}"+root+"${2}")
	for _, m := range regexp.MustCompile(`(?:^|[\s>])(/[^\s;)]*)`).FindAllStringSubmatch(script, -1) {
		if m[1] != "/dev/null" && !strings.HasPrefix(m[1], root+"/") {
			return -1, "the script names " + m[1] + ", outside " + root
		}
	}
	cmd := exec.Command("sh", "-c", script)
	cmd.Env = []string{"PATH=" + filepath.Join(root, "fake") + ":/usr/bin:/bin"}
	out, _ := cmd.CombinedOutput()
	return cmd.ProcessState.ExitCode(), string(out)
}

// Each script runs with its paths under a directory of the test's, and a
// systemctl and a pgrep that log their calls. k3s's uninstall script
// logs its call and exits with status, or is absent for status -1. pgrep
// finds an install running the first installing times it is asked.
func TestUninstallRemovesWhatIsThereAndFailsAsK3ssDoes(t *testing.T) {
	scripts, _ := k3s.Scripts(version, netip.MustParseAddr("10.0.0.2"))
	const config, registries, issuer = "/etc/rancher/k3s/config.yaml", "/etc/rancher/k3s/registries.yaml", "/var/lib/rancher/k3s/server/manifests/moostack-issuer.yaml"
	others := []string{"/etc/moostack/zot.json", "/var/lib/moostack/applied-commit", "/usr/local/bin/zot", "/var/lib/zot/blob"}
	for name, c := range map[string]struct {
		controlPlane bool
		status       int
		installing   int
		// kept are the files there that the script leaves, and removed
		// the files there, and their directories, that it removes.
		kept, removed []string
		// calls are what the script runs, in order.
		calls string
	}{
		"a control plane": {
			controlPlane: true,
			kept:         append([]string{"/etc/other"}, others...),
			removed:      []string{config, registries, issuer, "/etc/systemd/system/k3s.service.d/memory.conf", "/etc/rancher/k3s", "/etc/systemd/system/k3s.service.d"},
			calls:        "pgrep k3s-uninstall daemon-reload",
		},
		"a control plane whose k3s fails to uninstall": {
			controlPlane: true,
			status:       3,
			removed:      []string{config, issuer},
			calls:        "pgrep k3s-uninstall daemon-reload",
		},
		"a control plane whose k3s is gone": {
			controlPlane: true,
			status:       -1,
			removed:      []string{config, issuer},
			calls:        "pgrep daemon-reload",
		},
		"a control plane with files of others beside k3s's": {
			controlPlane: true,
			kept:         []string{"/etc/rancher/k3s/other", "/etc/systemd/system/k3s.service.d/other.conf"},
			removed:      []string{config, "/etc/systemd/system/k3s.service.d/memory.conf"},
			calls:        "pgrep k3s-uninstall daemon-reload",
		},
		"a control plane with an install still running": {
			controlPlane: true,
			installing:   1,
			removed:      []string{config},
			calls:        "pgrep pgrep k3s-uninstall daemon-reload",
		},
		"a control plane on which k3s was never installed": {
			controlPlane: true,
			status:       -1,
			kept:         []string{"/etc/other", "/var/lib/other/blob"},
			removed: []string{config, registries, issuer, "/var/lib/rancher/k3s/agent/etc/kubelet.conf.d/10-moostack.conf", "/etc/systemd/system/k3s.service.d/memory.conf",
				"/etc/rancher", "/var/lib/rancher", "/etc/systemd/system/k3s.service.d"},
			calls: "pgrep daemon-reload",
		},
		"a node on which k3s was never installed": {
			status:  -1,
			kept:    []string{"/var/lib/rancher/k3s/other"},
			removed: []string{config, registries, "/var/lib/rancher/k3s/agent/etc/kubelet.conf.d/10-moostack.conf", "/etc/systemd/system/k3s-agent.service.d/memory.conf", "/etc/rancher", "/var/lib/rancher/k3s/agent", "/etc/systemd/system/k3s-agent.service.d"},
			calls:   "pgrep daemon-reload",
		},
		"a node": {
			kept:    append([]string{"/etc/systemd/system/k3s.service.d/memory.conf"}, others...),
			removed: []string{config, registries, "/etc/systemd/system/k3s-agent.service.d/memory.conf", "/var/lib/rancher/k3s/agent/etc/kubelet.conf.d/10-moostack.conf", "/etc/rancher/k3s"},
			calls:   "pgrep k3s-uninstall daemon-reload",
		},
	} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			write := func(path, content string, mode os.FileMode) { writeUnder(t, root, path, content, mode) }
			all := append(slices.Clone(c.kept), c.removed...)
			for _, f := range all {
				if !slices.ContainsFunc(all, func(o string) bool { return strings.HasPrefix(o, f+"/") }) {
					write(f, "x", 0o644)
				}
			}
			log := filepath.Join(root, "calls")
			script, own := scripts["uninstall-node"], "k3s-agent-uninstall.sh"
			if c.controlPlane {
				script, own = scripts["uninstall-control-plane"], "k3s-uninstall.sh"
			}
			if c.status >= 0 {
				write("/usr/local/bin/"+own, fmt.Sprintf("#!/bin/sh\necho k3s-uninstall >> %s\nexit %d\n", log, c.status), 0o755)
			}
			write("/fake/systemctl", fmt.Sprintf("#!/bin/sh\necho \"$*\" | tr ' ' - >> %s\n", log), 0o755)
			write("/fake/pgrep", fmt.Sprintf("#!/bin/sh\necho pgrep >> %[1]s\n[ \"$(grep -c pgrep %[1]s)\" -le %[2]d ]\n", log, c.installing), 0o755)
			write("/fake/sleep", "#!/bin/sh\n", 0o755)

			if got, out := uninstallUnder(root, script); got != max(c.status, 0) {
				t.Errorf("the script exits %d, want %d:\n%s", got, max(c.status, 0), out)
			}
			for _, f := range c.removed {
				if _, err := os.Stat(filepath.Join(root, f)); err == nil {
					t.Errorf("%s is left", f)
				}
			}
			for _, f := range c.kept {
				if _, err := os.Stat(filepath.Join(root, f)); err != nil {
					t.Errorf("%s is gone", f)
				}
			}
			calls, _ := os.ReadFile(log)
			if got := strings.Join(strings.Fields(string(calls)), " "); got != c.calls {
				t.Errorf("the script ran\n%s\nwant\n%s", got, c.calls)
			}
		})
	}
}

// A host goes through installs that put some of a node's files there,
// and uninstalls, with k3s's own uninstall script there or not, passing
// or failing. After each uninstall the host has none of the role's
// files, keeps every other, and the script exited as k3s's did.
func TestSequencesOfInstallsAndUninstallsLeaveOnlyWhatIsntANodes(t *testing.T) {
	scripts, _ := k3s.Scripts(version, netip.MustParseAddr("10.0.0.2"))
	others := []string{"/etc/other", "/etc/moostack/zot.json", "/var/lib/moostack/applied-commit", "/etc/rancher/k3s/other", "/usr/local/bin/other", "/usr/local/bin/zot", "/var/lib/other/blob"}
	for seed := range uint64(40) {
		rng := rand.New(rand.NewPCG(seed, 1))
		controlPlane := rng.IntN(2) == 0
		root := t.TempDir()
		mine := slices.Collect(maps.Keys(k3s.NodeFiles(model.Node{ControlPlane: controlPlane, PrivateAddress: netip.MustParseAddr("10.0.0.3")}, k3s.Cluster{}, netip.MustParseAddr("10.0.0.2"), "t")))
		script, own := scripts["uninstall-node"], "/usr/local/bin/k3s-agent-uninstall.sh"
		if controlPlane {
			script, own = scripts["uninstall-control-plane"], "/usr/local/bin/k3s-uninstall.sh"
		}
		writeUnder(t, root, "/fake/systemctl", "#!/bin/sh\n", 0o755)
		writeUnder(t, root, "/fake/pgrep", "#!/bin/sh\nexit 1\n", 0o755)
		kept := map[string]bool{}
		for step := range 6 {
			for _, f := range mine {
				if rng.IntN(2) == 0 {
					writeUnder(t, root, f, "x", 0o644)
				}
			}
			for _, f := range others {
				if rng.IntN(3) == 0 {
					writeUnder(t, root, f, "x", 0o644)
					kept[f] = true
				}
			}
			status := 0
			if rng.IntN(3) > 0 {
				status = rng.IntN(2) * 3
				writeUnder(t, root, own, fmt.Sprintf("#!/bin/sh\nrm -f \"$0\"\nexit %d\n", status), 0o755)
			}
			got, out := uninstallUnder(root, script)
			if got != status {
				t.Fatalf("seed %d, step %d, control plane %t: the script exits %d, want %d:\n%s", seed, step, controlPlane, got, status, out)
			}
			for _, f := range mine {
				if _, err := os.Stat(filepath.Join(root, f)); err == nil {
					t.Fatalf("seed %d, step %d, control plane %t: %s is left", seed, step, controlPlane, f)
				}
			}
			for f := range kept {
				if _, err := os.Stat(filepath.Join(root, f)); err != nil {
					t.Fatalf("seed %d, step %d, control plane %t: %s is gone", seed, step, controlPlane, f)
				}
			}
		}
	}
}

// podsOn are the pods on the node called name, each as namespace/name,
// ordered.
func podsOn(t *testing.T, name string) []string {
	t.Helper()
	pods, err := admin.CoreV1().Pods(metav1.NamespaceAll).List(context.Background(), metav1.ListOptions{FieldSelector: "spec.nodeName=" + name})
	if err != nil {
		t.Fatal(err)
	}
	var on []string
	for _, p := range pods.Items {
		on = append(on, p.Namespace+"/"+p.Name)
	}
	slices.Sort(on)
	return on
}

// worker is a node of the cluster, Ready, with what the manifests make
// for it in a namespace of its name: in them NODE is the node's name.
// It returns once count pods are on the node.
func worker(t *testing.T, count int, manifests string) string {
	t.Helper()
	name := unique("worker")
	registers(t, name, "10.0.9.9")
	createNamespace(t, name)
	t.Cleanup(func() { admin.CoreV1().Namespaces().Delete(context.Background(), name, metav1.DeleteOptions{}) })
	if err := create(context.Background(), strings.ReplaceAll(manifests, "NODE", name)); err != nil {
		t.Fatal(err)
	}
	ctx := within(t, time.Minute)
	err := retry(ctx, func() error {
		if on := podsOn(t, name); len(on) != count {
			return fmt.Errorf("the pods on %s are %q, want %d", name, on, count)
		}
		return nil
	})
	if err != nil {
		pods, _ := admin.CoreV1().Pods(name).List(context.Background(), metav1.ListOptions{})
		for _, p := range pods.Items {
			t.Logf("pod %s on %q: %s %+v", p.Name, p.Spec.NodeName, p.Status.Phase, p.Status.Conditions)
		}
		node, _ := admin.CoreV1().Nodes().Get(context.Background(), name, metav1.GetOptions{})
		t.Logf("node %s: taints %+v, conditions %+v, allocatable %v", name, node.Spec.Taints, node.Status.Conditions, node.Status.Allocatable)
		t.Fatal(err)
	}
	return name
}

// Manifests of what runs on a worker. Their pods end at once when they
// are deleted.
const (
	// twoReplicas is a Deployment of two replicas that run only on the
	// node.
	twoReplicas = `
apiVersion: apps/v1
kind: Deployment
metadata: {name: web, namespace: NODE}
spec:
  replicas: 2
  selector: {matchLabels: {app: web}}
  template:
    metadata: {labels: {app: web}}
    spec:
      terminationGracePeriodSeconds: 0
      nodeSelector: {moostack.test/node: NODE}
      tolerations: [{key: moostack.test/node, operator: Exists}]
      containers: [{name: web, image: example.test/web}]
`
	// oneReplica is a Deployment of one replica that runs only on the
	// node.
	oneReplica = `
apiVersion: apps/v1
kind: Deployment
metadata: {name: one, namespace: NODE}
spec:
  replicas: 1
  selector: {matchLabels: {app: one}}
  template:
    metadata: {labels: {app: one}}
    spec:
      terminationGracePeriodSeconds: 0
      nodeSelector: {moostack.test/node: NODE}
      tolerations: [{key: moostack.test/node, operator: Exists}]
      containers: [{name: one, image: example.test/one}]
`
	// daemon is a DaemonSet that runs on the node.
	daemon = `
---
apiVersion: apps/v1
kind: DaemonSet
metadata: {name: agent, namespace: NODE}
spec:
  selector: {matchLabels: {app: agent}}
  template:
    metadata: {labels: {app: agent}}
    spec:
      terminationGracePeriodSeconds: 0
      nodeSelector: {moostack.test/node: NODE}
      tolerations: [{key: moostack.test/node, operator: Exists}]
      containers: [{name: agent, image: example.test/agent}]
`
	// static is a static pod of the node, as its kubelet tells of it.
	static = `
---
apiVersion: v1
kind: Pod
metadata: {name: static, namespace: NODE, annotations: {kubernetes.io/config.mirror: abc}}
spec:
  nodeName: NODE
  terminationGracePeriodSeconds: 0
  automountServiceAccountToken: false
  containers: [{name: static, image: example.test/static}]
`
	// alone is a pod that no controller would start again.
	alone = `
---
apiVersion: v1
kind: Pod
metadata: {name: alone, namespace: NODE}
spec:
  nodeName: NODE
  terminationGracePeriodSeconds: 0
  containers: [{name: alone, image: example.test/alone}]
`
	// slow is a pod of a controller that takes 30 seconds to end, which
	// nothing on its node tells the cluster of.
	slow = `
---
apiVersion: apps/v1
kind: Deployment
metadata: {name: slow, namespace: NODE}
spec:
  replicas: 1
  selector: {matchLabels: {app: slow}}
  template:
    metadata: {labels: {app: slow}}
    spec:
      terminationGracePeriodSeconds: 30
      nodeSelector: {moostack.test/node: NODE}
      tolerations: [{key: moostack.test/node, operator: Exists}]
      containers: [{name: slow, image: example.test/slow}]
`
	// budget lets none of oneReplica's pods be disrupted.
	budget = `
---
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata: {name: one, namespace: NODE}
spec:
  minAvailable: 1
  selector: {matchLabels: {app: one}}
`
	// volume is a volume on the node.
	volume = `
---
apiVersion: v1
kind: PersistentVolume
metadata: {name: NODE-data}
spec:
  capacity: {storage: 1Gi}
  accessModes: [ReadWriteOnce]
  storageClassName: local
  local: {path: /var/data}
  nodeAffinity:
    required:
      nodeSelectorTerms:
        - matchExpressions: [{key: kubernetes.io/hostname, operator: In, values: [NODE]}]
`
)

func init() {
	resources["PodDisruptionBudget"] = schema.GroupVersionResource{Group: "policy", Version: "v1", Resource: "poddisruptionbudgets"}
	resources["PersistentVolume"] = schema.GroupVersionResource{Version: "v1", Resource: "persistentvolumes"}
}

// budgetHolds waits until the budget of the namespace lets no pod of
// its one healthy pod be disrupted.
func budgetHolds(t *testing.T, namespace string) {
	t.Helper()
	err := retry(within(t, time.Minute), func() error {
		b, err := admin.PolicyV1().PodDisruptionBudgets(namespace).Get(context.Background(), "one", metav1.GetOptions{})
		if err != nil {
			return err
		}
		if b.Status.ObservedGeneration < b.Generation || b.Status.CurrentHealthy != 1 || b.Status.DisruptionsAllowed != 0 {
			return fmt.Errorf("the budget counts %d healthy pods and allows %d disruptions", b.Status.CurrentHealthy, b.Status.DisruptionsAllowed)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// takesPods reports whether the node called name takes new pods.
func takesPods(t *testing.T, name string) bool {
	t.Helper()
	n, err := admin.CoreV1().Nodes().Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return !n.Spec.Unschedulable
}

// installed is a host with k3s and a node's files on it.
func installed(t *testing.T) *host {
	h := aHost(t, "10.0.9.9")
	h.k3s = true
	maps.Copy(h.files, k3s.NodeFiles(model.Node{Name: "n", PrivateAddress: netip.MustParseAddr("10.0.9.9")}, theCluster(), netip.MustParseAddr("10.0.0.2"), joinToken))
	return h
}

func TestRemoveNodeDrainsTheNodeUninstallsFromItsHostAndDeletesIt(t *testing.T) {
	name := worker(t, 4, twoReplicas+daemon+static)
	// A pod that has ended is evicted though no controller would start it
	// again.
	done := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "done", Namespace: name},
		Spec:       corev1.PodSpec{NodeName: name, Containers: []corev1.Container{{Name: "job", Image: "example.test/job"}}},
	}
	createPod(t, done, corev1.PodStatus{Phase: corev1.PodSucceeded})
	h := installed(t)
	var steps told
	var drained []string
	var draining, uninstalledBeforeDelete bool
	err := k3s.Through(aHost(t, "10.0.0.2"), theCluster()).RemoveNode(within(t, time.Minute), name, h, orchestrator.RemoveOptions{}, func(s orchestrator.Step) {
		steps.step(s)
		switch s {
		case orchestrator.Uninstalling:
			drained, draining = podsOn(t, name), !takesPods(t, name)
		case orchestrator.Deleting:
			uninstalledBeforeDelete = h.left() == "" && hasNode(t, name)
		}
	})
	if err != nil {
		t.Fatalf("RemoveNode: %v", err)
	}
	if want := []orchestrator.Step{orchestrator.Draining, orchestrator.Uninstalling, orchestrator.Deleting}; !slices.Equal(steps.all(), want) {
		t.Errorf("the steps were %v, want Draining, Uninstalling, Deleting", steps.all())
	}
	if len(drained) != 2 || !strings.HasPrefix(drained[0], name+"/agent-") || drained[1] != name+"/static" || !draining {
		t.Errorf("after the drain the node had the pods %q and kept new ones off: %t, want the DaemonSet's and the static one", drained, draining)
	}
	if !uninstalledBeforeDelete {
		t.Error("the node was deleted before its host was uninstalled from")
	}
	scripts, _ := k3s.Scripts(version, netip.MustParseAddr("10.0.0.2"))
	if len(h.ran) != 1 || h.ran[0] != scripts["uninstall-node"] || h.left() != "" {
		t.Errorf("the host ran %q and still has %q, want a node's uninstall alone", h.commands(), h.left())
	}
	if hasNode(t, name) {
		t.Error("the cluster still has the node once RemoveNode returned")
	}
}

func TestADrainRefusesAPodNoControllerWouldStartAgainAndEvictsNone(t *testing.T) {
	name := worker(t, 3, twoReplicas+alone)
	before := podsOn(t, name)
	h := installed(t)
	var steps told
	b := k3s.Through(aHost(t, "10.0.0.2"), theCluster())
	err := b.RemoveNode(within(t, time.Minute), name, h, orchestrator.RemoveOptions{}, steps.step)
	want := "draining " + name + ": no controller would start these pods again: " + name + "/alone; delete them, or give them one"
	if err == nil || err.Error() != want {
		t.Errorf("RemoveNode: %v\nwant %s", err, want)
	}
	if after := podsOn(t, name); !slices.Equal(after, before) {
		t.Errorf("the refused drain left the pods %q, of %q", after, before)
	}
	if !takesPods(t, name) || !hasNode(t, name) || !h.k3s || !slices.Equal(steps.all(), []orchestrator.Step{orchestrator.Draining}) {
		t.Errorf("after the refused drain the node takes pods: %t, is in the cluster: %t, has k3s: %t, after the steps %v", takesPods(t, name), hasNode(t, name), h.k3s, steps.all())
	}

	// Run again without the pod, the removal finishes.
	if err := admin.CoreV1().Pods(name).Delete(context.Background(), "alone", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := b.RemoveNode(within(t, time.Minute), name, h, orchestrator.RemoveOptions{}, steps.step); err != nil || hasNode(t, name) || h.left() != "" {
		t.Errorf("RemoveNode again: %v, the node in the cluster: %t, %q on the host", err, hasNode(t, name), h.left())
	}
}

func TestAnEvictionABudgetHoldsBackIsTriedAgainUntilItPasses(t *testing.T) {
	name := worker(t, 1, oneReplica+budget)
	kubelet(t, name)
	budgetHolds(t, name)
	done := make(chan error, 1)
	go func() {
		done <- k3s.Through(aHost(t, "10.0.0.2"), theCluster()).RemoveNode(within(t, time.Minute), name, nil, orchestrator.RemoveOptions{}, func(orchestrator.Step) {})
	}()
	select {
	case err := <-done:
		t.Fatalf("RemoveNode returned with the budget in place: %v", err)
	case <-time.After(4 * time.Second):
	}
	if on := podsOn(t, name); len(on) != 1 || takesPods(t, name) {
		t.Errorf("held back by the budget, the node has the pods %q and takes new ones: %t", on, takesPods(t, name))
	}
	if err := objects.Resource(resources["PodDisruptionBudget"]).Namespace(name).Delete(context.Background(), "one", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil || hasNode(t, name) {
		t.Errorf("RemoveNode once the budget is gone: %v, the node in the cluster: %t", err, hasNode(t, name))
	}
}

func TestADrainWhoseTimeIsUpNamesWhatHeldItAndLetsTheNodeTakePodsAgain(t *testing.T) {
	for what, tc := range map[string]struct {
		manifests string
		ready     bool
		want      string
	}{
		"a budget":                  {oneReplica + budget, true, "these pods can't be evicted in time:\nNODE/one-"},
		"a pod that takes its time": {slow, false, "these pods are still there: NODE/slow-"},
	} {
		t.Run(what, func(t *testing.T) {
			name := worker(t, 1, tc.manifests)
			if tc.ready {
				kubelet(t, name)
				budgetHolds(t, name)
			}
			h := installed(t)
			err := k3s.Through(aHost(t, "10.0.0.2"), theCluster()).RemoveNode(within(t, 6*time.Second), name, h, orchestrator.RemoveOptions{}, func(orchestrator.Step) {})
			want := "draining " + name + ": " + strings.ReplaceAll(tc.want, "NODE", name)
			if err == nil || !strings.HasPrefix(err.Error(), want) {
				t.Errorf("RemoveNode: %v\nwant it to start with %s", err, want)
			}
			if tc.ready && !strings.Contains(err.Error(), "disruption budget") {
				t.Errorf("the error doesn't say what holds the eviction back: %v", err)
			}
			if !takesPods(t, name) || !hasNode(t, name) || !h.k3s {
				t.Errorf("after the failed drain the node takes pods: %t, is in the cluster: %t, has k3s: %t", takesPods(t, name), hasNode(t, name), h.k3s)
			}
		})
	}
}

func TestADrainCutOffLetsTheNodeTakePodsAgainAndFailsWithTheCause(t *testing.T) {
	name := worker(t, 1, slow)
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("the operation lock is lost")
	time.AfterFunc(3*time.Second, func() { cancel(cause) })
	err := k3s.Through(aHost(t, "10.0.0.2"), theCluster()).RemoveNode(ctx, name, nil, orchestrator.RemoveOptions{}, func(orchestrator.Step) {})
	if err == nil || err.Error() != cause.Error() {
		t.Errorf("RemoveNode: %v, want the context's cause alone", err)
	}
	if !takesPods(t, name) || !hasNode(t, name) {
		t.Errorf("after the drain was cut off the node takes pods: %t, is in the cluster: %t", takesPods(t, name), hasNode(t, name))
	}
}

func TestRemoveNodeRefusesANodeThatHoldsVolumesUnlessToldToLoseThem(t *testing.T) {
	name := worker(t, 2, twoReplicas+volume)
	t.Cleanup(func() {
		objects.Resource(resources["PersistentVolume"]).Delete(context.Background(), name+"-data", metav1.DeleteOptions{})
	})
	b := k3s.Through(aHost(t, "10.0.0.2"), theCluster())
	var steps told
	err := b.RemoveNode(within(t, time.Minute), name, nil, orchestrator.RemoveOptions{}, steps.step)
	var held *orchestrator.VolumesError
	if !errors.As(err, &held) || held.Node != name || !slices.Equal(held.Volumes, []string{name + "-data"}) {
		t.Fatalf("RemoveNode of a node that holds a volume: %v, want a VolumesError of %s-data", err, name)
	}
	if on := podsOn(t, name); len(on) != 2 || !takesPods(t, name) || len(steps.all()) != 0 {
		t.Errorf("the refusal left the pods %q, the node taking pods: %t, after the steps %v", on, takesPods(t, name), steps.all())
	}
	if err := b.RemoveNode(within(t, time.Minute), name, nil, orchestrator.RemoveOptions{LoseVolumes: true}, steps.step); err != nil || hasNode(t, name) {
		t.Errorf("RemoveNode, losing volumes: %v, the node in the cluster: %t", err, hasNode(t, name))
	}
	if want := []orchestrator.Step{orchestrator.Draining, orchestrator.Deleting}; !slices.Equal(steps.all(), want) {
		t.Errorf("the steps without a host were %v, want Draining, Deleting", steps.all())
	}
}

func TestRemoveNodeOfANodeWhoseHostCantBeReachedDeletesItAndMovesNothing(t *testing.T) {
	name := worker(t, 3, twoReplicas+alone+volume)
	t.Cleanup(func() {
		objects.Resource(resources["PersistentVolume"]).Delete(context.Background(), name+"-data", metav1.DeleteOptions{})
	})
	var steps told
	var before []string
	var cordoned bool
	err := k3s.Through(aHost(t, "10.0.0.2"), theCluster()).RemoveNode(within(t, time.Minute), name, nil, orchestrator.RemoveOptions{Unreachable: true}, func(s orchestrator.Step) {
		steps.step(s)
		before, cordoned = podsOn(t, name), !takesPods(t, name)
	})
	if err != nil || hasNode(t, name) {
		t.Fatalf("RemoveNode: %v, the node in the cluster: %t", err, hasNode(t, name))
	}
	if !slices.Equal(steps.all(), []orchestrator.Step{orchestrator.Deleting}) || len(before) != 3 || cordoned {
		t.Errorf("the steps were %v, and as the node was deleted it had the pods %q and kept new ones off: %t; want Deleting alone, with its 3 pods", steps.all(), before, cordoned)
	}
}

func TestRemoveNodeOfANodeTheClusterDoesntHaveUninstallsFromItsHostAlone(t *testing.T) {
	b := k3s.Through(aHost(t, "10.0.0.2"), theCluster())
	h := installed(t)
	var steps told
	if err := b.RemoveNode(within(t, time.Minute), unique("absent"), h, orchestrator.RemoveOptions{}, steps.step); err != nil {
		t.Fatalf("RemoveNode: %v", err)
	}
	if h.left() != "" || !slices.Equal(steps.all(), []orchestrator.Step{orchestrator.Uninstalling}) {
		t.Errorf("the host still has %q after the steps %v, want Uninstalling alone", h.left(), steps.all())
	}
	steps = told{}
	if err := b.RemoveNode(within(t, time.Minute), unique("absent"), nil, orchestrator.RemoveOptions{}, steps.step); err != nil || len(steps.all()) != 0 {
		t.Errorf("RemoveNode without a node or a host: %v, after the steps %v", err, steps.all())
	}
}

func TestARemovalWhoseUninstallFailsKeepsTheNodeAndFinishesWhenRunAgain(t *testing.T) {
	name := worker(t, 2, twoReplicas)
	h := installed(t)
	h.failAt = "uninstall"
	b := k3s.Through(aHost(t, "10.0.0.2"), theCluster())
	var steps told
	err := b.RemoveNode(within(t, time.Minute), name, h, orchestrator.RemoveOptions{}, steps.step)
	if want := "uninstalling from " + name + ": exit 1: uninstall failed"; err == nil || err.Error() != want {
		t.Errorf("RemoveNode: %v\nwant %s", err, want)
	}
	if !hasNode(t, name) || !h.k3s || !slices.Equal(steps.all(), []orchestrator.Step{orchestrator.Draining, orchestrator.Uninstalling}) {
		t.Errorf("after the failed uninstall the node is in the cluster: %t, k3s on its host: %t, after the steps %v", hasNode(t, name), h.k3s, steps.all())
	}
	h.failAt = ""
	if err := b.RemoveNode(within(t, time.Minute), name, h, orchestrator.RemoveOptions{}, steps.step); err != nil || hasNode(t, name) || h.left() != "" {
		t.Errorf("RemoveNode again: %v, the node in the cluster: %t, %q on the host", err, hasNode(t, name), h.left())
	}
}

func TestDeletingANodeReturnsOnceItIsGone(t *testing.T) {
	name := unique("held")
	registers(t, name, "10.0.9.9")
	nodes := admin.CoreV1().Nodes()
	hold := []byte(`{"metadata":{"finalizers":["moostack.test/hold"]}}`)
	if _, err := nodes.Patch(context.Background(), name, types.MergePatchType, hold, metav1.PatchOptions{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		nodes.Patch(context.Background(), name, types.MergePatchType, []byte(`{"metadata":{"finalizers":null}}`), metav1.PatchOptions{})
	})
	done := make(chan error, 1)
	go func() {
		done <- k3s.Through(aHost(t, "10.0.0.2"), theCluster()).RemoveNode(within(t, time.Minute), name, nil, orchestrator.RemoveOptions{Unreachable: true}, func(orchestrator.Step) {})
	}()
	select {
	case err := <-done:
		t.Fatalf("RemoveNode returned while the node was still there: %v", err)
	case <-time.After(4 * time.Second):
	}
	if _, err := nodes.Patch(context.Background(), name, types.MergePatchType, []byte(`{"metadata":{"finalizers":null}}`), metav1.PatchOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil || hasNode(t, name) {
		t.Errorf("RemoveNode once the node could go: %v, the node in the cluster: %t", err, hasNode(t, name))
	}
}

func TestANodeThatStaysAfterItsDeletionIsAnErrorInTime(t *testing.T) {
	name := unique("stays")
	registers(t, name, "10.0.9.9")
	nodes := admin.CoreV1().Nodes()
	if _, err := nodes.Patch(context.Background(), name, types.MergePatchType, []byte(`{"metadata":{"finalizers":["moostack.test/hold"]}}`), metav1.PatchOptions{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		nodes.Patch(context.Background(), name, types.MergePatchType, []byte(`{"metadata":{"finalizers":null}}`), metav1.PatchOptions{})
	})
	err := k3s.Through(aHost(t, "10.0.0.2"), theCluster()).RemoveNode(within(t, 4*time.Second), name, nil, orchestrator.RemoveOptions{Unreachable: true}, func(orchestrator.Step) {})
	if want := "deleting node " + name + ": it is still there after 1m0s"; err == nil || err.Error() != want {
		t.Errorf("RemoveNode: %v\nwant %s", err, want)
	}

	// Run again once the node can go, the removal finishes.
	if _, err := nodes.Patch(context.Background(), name, types.MergePatchType, []byte(`{"metadata":{"finalizers":null}}`), metav1.PatchOptions{}); err != nil {
		t.Fatal(err)
	}
	registers(t, name, "10.0.9.9")
	h := installed(t)
	if err := k3s.Through(aHost(t, "10.0.0.2"), theCluster()).RemoveNode(within(t, time.Minute), name, h, orchestrator.RemoveOptions{}, func(orchestrator.Step) {}); err != nil || hasNode(t, name) || h.left() != "" {
		t.Errorf("RemoveNode again: %v, the node in the cluster: %t, %q on the host", err, hasNode(t, name), h.left())
	}
}

func TestANodeOfTheSameNameThatRegistersAgainIsNotWaitedFor(t *testing.T) {
	name := unique("back")
	registers(t, name, "10.0.9.9")
	// Its kubelet registers the node again as soon as it is deleted.
	keep(t, func(ctx context.Context) error {
		n := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}
		if _, err := admin.CoreV1().Nodes().Create(ctx, n, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			return err
		}
		return nil
	})
	if err := k3s.Through(aHost(t, "10.0.0.2"), theCluster()).RemoveNode(within(t, 20*time.Second), name, nil, orchestrator.RemoveOptions{Unreachable: true}, func(orchestrator.Step) {}); err != nil {
		t.Errorf("RemoveNode of a node that registers again: %v", err)
	}
}

func TestAnAddNodeCutOffWhoseUndoFailsSaysWhatIsLeftBesideTheCause(t *testing.T) {
	name := unique("cut")
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("the operation lock is lost")
	h := aHost(t, "10.0.0.3")
	h.onInstall = func() { cancel(cause) }
	bad := &failingHost{host: h, at: "uninstall.sh"}
	n := model.Node{Name: name, PrivateAddress: netip.MustParseAddr("10.0.0.3"), Interface: "ens10", MemoryMiB: 1966}
	err := k3s.Through(aHost(t, "10.0.0.2"), theCluster()).AddNode(ctx, bad, n, func(orchestrator.Step) {})
	want := "the operation lock is lost\nuninstalling from " + name + ": exit 3: k3s-agent-uninstall.sh failed"
	if err == nil || err.Error() != want || !errors.Is(err, cause) {
		t.Errorf("AddNode: %v\nwant %s", err, want)
	}
	if h.left() == "" {
		t.Error("the host has nothing left, so the test's uninstall didn't fail")
	}
}

func TestAJoinCutOffAfterItsNodeRegisteredUninstallsAndDeletesTheNode(t *testing.T) {
	name := unique("cut")
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("interrupted")
	h := aHost(t, "10.0.0.3")
	h.onStart = func() {
		registers(t, name, "10.0.0.99")
		cancel(cause)
	}
	n := model.Node{Name: name, PrivateAddress: netip.MustParseAddr("10.0.0.3"), Interface: "ens10", MemoryMiB: 1966}
	if err := k3s.Through(aHost(t, "10.0.0.2"), theCluster()).AddNode(ctx, h, n, func(orchestrator.Step) {}); err == nil || err.Error() != "interrupted" {
		t.Errorf("AddNode: %v, want the cause alone", err)
	}
	if h.left() != "" || hasNode(t, name) {
		t.Errorf("after the cut the host has %q and the cluster the node: %t", h.left(), hasNode(t, name))
	}
}

func TestAddNodeRefusesTheNameOfANodeTheClusterHasAndLeavesThatNode(t *testing.T) {
	name := unique("there")
	registers(t, name, "10.0.0.7")
	h := aHost(t, "10.0.0.3")
	var steps told
	n := model.Node{Name: name, PrivateAddress: netip.MustParseAddr("10.0.0.3"), Interface: "ens10", MemoryMiB: 1966}
	err := k3s.Through(aHost(t, "10.0.0.2"), theCluster()).AddNode(within(t, time.Minute), h, n, steps.step)
	if want := "the cluster has a node " + name + " already"; err == nil || err.Error() != want {
		t.Errorf("AddNode: %v\nwant %s", err, want)
	}
	if !hasNode(t, name) || len(h.commands()) != 0 || len(steps.all()) != 0 {
		t.Errorf("the cluster has the node: %t; the host ran %q; the steps were %v", hasNode(t, name), h.commands(), steps.all())
	}
}

func TestAControlPlanesStartingIsToldOnlyOnceTheWaitForItsDownloadEnded(t *testing.T) {
	name := unique("cp")
	h := aHost(t, "10.0.0.2")
	h.downloaded = make(chan struct{})
	h.onInstall = func() { registers(t, name, "10.0.0.2") }
	n := model.Node{Name: name, ControlPlane: true, PrivateAddress: netip.MustParseAddr("10.0.0.2"), Interface: "ens10", MemoryMiB: 1966}
	var steps told
	done := make(chan error, 1)
	go func() {
		done <- k3s.Through(nil, k3s.Cluster{Version: version}).AddNode(within(t, time.Minute), h, n, steps.step)
	}()
	select {
	case err := <-done:
		t.Fatalf("AddNode returned while the wait for the download ran: %v", err)
	case <-time.After(2 * time.Second):
	}
	if got := steps.all(); !slices.Equal(got, []orchestrator.Step{orchestrator.Installing}) {
		t.Errorf("while the wait for the download ran the steps were %v, want Installing alone", got)
	}
	close(h.downloaded)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if want := []orchestrator.Step{orchestrator.Installing, orchestrator.Starting, orchestrator.WaitingForNode}; !slices.Equal(steps.all(), want) {
		t.Errorf("the steps were %v, want Installing, Starting, WaitingForNode", steps.all())
	}
}

func TestPreflightEndsWithItsContextWhileTheControlPlanesVersionIsRead(t *testing.T) {
	release := make(chan struct{})
	api := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { <-release }))
	defer api.Close()
	defer close(release)
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: api.Certificate().Raw})
	config := clientcmdapi.NewConfig()
	config.Clusters["c"] = &clientcmdapi.Cluster{Server: "https://127.0.0.1:6443", CertificateAuthorityData: ca}
	config.AuthInfos["u"] = &clientcmdapi.AuthInfo{Token: "t"}
	config.Contexts["x"] = &clientcmdapi.Context{Cluster: "c", AuthInfo: "u"}
	config.CurrentContext = "x"
	text, err := clientcmd.Write(*config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("interrupted")
	time.AfterFunc(time.Second, func() { cancel(cause) })
	start := time.Now()
	b := k3s.Through(hostTo{&controlPlane{kubeconfig: string(text)}, api.Listener.Addr().String()}, theCluster())
	_, err = b.Preflight(ctx, aHost(t, "10.0.0.3"), orchestrator.NodeRequest{Name: "node2"})
	if took := time.Since(start); !errors.Is(err, cause) || took > 10*time.Second {
		t.Errorf("Preflight: %v after %s, want its cause within seconds of the cut", err, took)
	}
}

func TestPreflightRefusesAControlPlaneWhoseAddressIsntIPv4AndRunsNothingWithIt(t *testing.T) {
	cluster := theCluster()
	cluster.ControlPlane = netip.MustParseAddr("fe80::1%$(touch pwned)")
	h := aHost(t, "10.0.0.3")
	_, err := k3s.Through(aHost(t, "10.0.0.2"), cluster).Preflight(within(t, time.Minute), h, orchestrator.NodeRequest{Name: "node2"})
	if want := `the control plane's private address "fe80::1%$(touch pwned)" isn't an IPv4 address`; err == nil || err.Error() != want {
		t.Errorf("Preflight: %v\nwant %s", err, want)
	}
	for _, cmd := range h.ran {
		if strings.Contains(cmd, "pwned") {
			t.Errorf("the address is in a command run on the host:\n%s", cmd)
		}
	}
}

// everywhere is a Deployment whose replica runs on the node though it
// takes no new pods.
const everywhere = `
apiVersion: apps/v1
kind: Deployment
metadata: {name: everywhere, namespace: NODE}
spec:
  replicas: 1
  selector: {matchLabels: {app: everywhere}}
  template:
    metadata: {labels: {app: everywhere}}
    spec:
      terminationGracePeriodSeconds: 0
      nodeSelector: {moostack.test/node: NODE}
      tolerations: [{operator: Exists}]
      containers: [{name: everywhere, image: example.test/everywhere}]
`

func TestADrainEndsOnceThePodsItFoundAreGoneThoughOthersCome(t *testing.T) {
	name := worker(t, 1, everywhere)
	before := podsOn(t, name)
	var after []string
	err := k3s.Through(aHost(t, "10.0.0.2"), theCluster()).RemoveNode(within(t, 30*time.Second), name, nil, orchestrator.RemoveOptions{}, func(s orchestrator.Step) {
		if s == orchestrator.Deleting {
			after = podsOn(t, name)
		}
	})
	if err != nil {
		t.Fatalf("RemoveNode of a node whose pod comes back: %v", err)
	}
	if slices.Contains(after, before[0]) {
		t.Errorf("after the drain the node still had %s, of the pods %q", before[0], after)
	}
}

// byField is a volume bound to the node by its name as a field.
const byField = `
---
apiVersion: v1
kind: PersistentVolume
metadata: {name: NODE-field}
spec:
  capacity: {storage: 1Gi}
  accessModes: [ReadWriteOnce]
  storageClassName: local
  local: {path: /var/data}
  nodeAffinity:
    required:
      nodeSelectorTerms:
        - matchFields: [{key: metadata.name, operator: In, values: [NODE]}]
`

func TestAVolumeBoundToANodeByAFieldIsOneItHolds(t *testing.T) {
	name := worker(t, 1, oneReplica+byField)
	t.Cleanup(func() {
		objects.Resource(resources["PersistentVolume"]).Delete(context.Background(), name+"-field", metav1.DeleteOptions{})
	})
	err := k3s.Through(aHost(t, "10.0.0.2"), theCluster()).RemoveNode(within(t, time.Minute), name, nil, orchestrator.RemoveOptions{}, func(orchestrator.Step) {})
	var held *orchestrator.VolumesError
	if !errors.As(err, &held) || !slices.Equal(held.Volumes, []string{name + "-field"}) {
		t.Errorf("RemoveNode: %v, want a VolumesError of %s-field", err, name)
	}
}

func TestAControlPlaneWhoseDownloadOutlastsTheWaitForItIsStillToldToStart(t *testing.T) {
	name := unique("cp")
	h := aHost(t, "10.0.0.2")
	h.slow = true
	h.onInstall = func() {
		time.Sleep(500 * time.Millisecond)
		registers(t, name, "10.0.0.2")
	}
	n := model.Node{Name: name, ControlPlane: true, PrivateAddress: netip.MustParseAddr("10.0.0.2"), Interface: "ens10", MemoryMiB: 1966}
	var steps told
	if err := k3s.Through(nil, k3s.Cluster{Version: version}).AddNode(within(t, time.Minute), h, n, steps.step); err != nil {
		t.Fatal(err)
	}
	if want := []orchestrator.Step{orchestrator.Installing, orchestrator.Starting, orchestrator.WaitingForNode}; !slices.Equal(steps.all(), want) {
		t.Errorf("the steps were %v, want Installing, Starting, WaitingForNode", steps.all())
	}
}

func TestAnAddNodeThatFailsAndIsThenCutOffDuringItsUndoStillSaysWhatFailed(t *testing.T) {
	name := unique("cut")
	ctx, cancel := context.WithCancelCause(context.Background())
	h := aHost(t, "10.0.0.3")
	h.failAt = "install"
	cutting := &cutAt{host: h, at: "uninstall.sh", cut: func() { cancel(errors.New("interrupted")) }}
	n := model.Node{Name: name, PrivateAddress: netip.MustParseAddr("10.0.0.3"), Interface: "ens10", MemoryMiB: 1966}
	err := k3s.Through(aHost(t, "10.0.0.2"), theCluster()).AddNode(ctx, cutting, n, func(orchestrator.Step) {})
	if want := "interrupted\ninstalling k3s: exit 1: install failed"; err == nil || err.Error() != want {
		t.Errorf("AddNode: %v\nwant %s", err, want)
	}
	if h.left() != "" {
		t.Errorf("the host still has %s", h.left())
	}
}

// cutAt is a host that calls cut as a command that holds at is run.
type cutAt struct {
	*host
	at  string
	cut func()
}

func (c *cutAt) Run(ctx context.Context, cmd string, stdin io.Reader) (string, error) {
	if strings.Contains(cmd, c.at) {
		c.cut()
	}
	return c.host.Run(ctx, cmd, stdin)
}

// The wait for k3s's download runs with its paths under a directory of
// the test's, a sleep that takes no time, and a pgrep that finds an
// install running the first installing times it is asked. k3s is there
// from the start, comes after the script slept appears times, or never.
func TestTheWaitForK3ssDownloadEndsOnceItIsThereOrNoInstallRuns(t *testing.T) {
	scripts, _ := k3s.Scripts(version, netip.MustParseAddr("10.0.0.2"))
	for name, c := range map[string]struct {
		installing, appears int
		exit                int
		// sleeps is how often the script sleeps, at least.
		sleeps int
	}{
		"k3s is there already":                       {installing: 1000, appears: 0, exit: 0, sleeps: 0},
		"k3s comes while the install runs":           {installing: 1000, appears: 25, exit: 0, sleeps: 25},
		"no install runs and k3s isn't there":        {installing: 0, appears: -1, exit: 1, sleeps: 10},
		"the install ends without k3s":               {installing: 40, appears: -1, exit: 1, sleeps: 40},
		"the install outlasts the wait, without k3s": {installing: 1000, appears: -1, exit: 1, sleeps: 300},
	} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			log := filepath.Join(root, "sleeps")
			bin := filepath.Join(root, "usr/local/bin/k3s")
			writeUnder(t, root, "/usr/local/bin/other", "x", 0o644)
			if c.appears == 0 {
				writeUnder(t, root, "/usr/local/bin/k3s", "x", 0o755)
			}
			appear := ""
			if c.appears > 0 {
				appear = fmt.Sprintf("[ \"$(grep -c . %s)\" -ge %d ] && : > %s\n", log, c.appears, bin)
			}
			writeUnder(t, root, "/fake/sleep", fmt.Sprintf("#!/bin/sh\necho s >> %s\n%s", log, appear), 0o755)
			writeUnder(t, root, "/fake/pgrep", fmt.Sprintf("#!/bin/sh\n[ \"$(grep -c . %s 2>/dev/null || echo 0)\" -lt %d ]\n", log, c.installing), 0o755)
			script := strings.ReplaceAll(scripts["downloaded"], "/usr/local/bin/", root+"/usr/local/bin/")
			cmd := exec.Command("sh", "-c", script)
			cmd.Env = []string{"PATH=" + filepath.Join(root, "fake") + ":/usr/bin:/bin"}
			out, _ := cmd.CombinedOutput()
			slept, _ := os.ReadFile(log)
			if got := cmd.ProcessState.ExitCode(); got != c.exit || strings.Count(string(slept), "s") < c.sleeps || strings.Count(string(slept), "s") > c.sleeps+1 {
				t.Errorf("the wait exits %d after %d sleeps, want %d after %d:\n%s", got, strings.Count(string(slept), "s"), c.exit, c.sleeps, out)
			}
		})
	}
}
