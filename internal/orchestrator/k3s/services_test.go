package k3s_test

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	"github.com/moostackhq/moostack/internal/model"
	"github.com/moostackhq/moostack/internal/orchestrator"
	"github.com/moostackhq/moostack/internal/orchestrator/k3s"
)

// serverHost is the container the test cluster's k3s server runs in, as
// a host: commands run in it, files are written in it, and connections
// reach its API.
type serverHost struct{}

func (serverHost) Run(ctx context.Context, cmd string, stdin io.Reader) (string, error) {
	c := exec.CommandContext(ctx, "docker", "exec", "-i", server, "sh", "-c", cmd)
	c.Stdin = stdin
	var stderr bytes.Buffer
	c.Stderr = &stderr
	out, err := c.Output()
	if err != nil {
		return "", fmt.Errorf("%v: %s", err, strings.TrimSpace(stderr.String()))
	}
	return string(out), nil
}

func (h serverHost) RunStoppable(ctx context.Context, cmd string) (string, error) {
	return h.Run(ctx, cmd, nil)
}

func (h serverHost) Upload(ctx context.Context, file string, r io.Reader, _ fs.FileMode) error {
	_, err := h.Run(ctx, "cat > "+file, r)
	return err
}

func (serverHost) DialTCP(ctx context.Context, _ string) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(ctx, "tcp", strings.TrimPrefix(adminConfig.Host, "https://"))
}

const consoleManifest = "/var/lib/rancher/k3s/server/manifests/moostack-console.yaml"

// noConsole removes the console from the test cluster, now and when the
// test ends, and has the cluster know the kind of its route.
func noConsole(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	route := definition("traefik.io", "IngressRoute", "ingressroutes", "Namespaced")
	route.Object["spec"].(map[string]any)["versions"].([]any)[0].(map[string]any)["name"] = "v1alpha1"
	if _, err := objects.Resource(crds).Create(ctx, route, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatal(err)
	}
	remove := func() {
		serverHost{}.Run(ctx, "rm -f "+consoleManifest, nil)
		deployments := admin.AppsV1().Deployments("kube-system")
		deployments.Delete(ctx, "moostack-console", metav1.DeleteOptions{})
		gone, cancel := context.WithTimeout(ctx, time.Minute)
		defer cancel()
		retry(gone, func() error {
			if _, err := deployments.Get(ctx, "moostack-console", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
				return fmt.Errorf("the console's deployment is still there: %v", err)
			}
			return nil
		})
	}
	remove()
	t.Cleanup(remove)
}

// consoleRuns makes the console's pods run and be ready, as they come,
// until the test ends.
func consoleRuns(t *testing.T) {
	keep(t, func(ctx context.Context) error { return setPods(ctx, "kube-system", "app=moostack-console", isReady) })
}

// consoles are the cluster's own running containers called console.
func consoles(t *testing.T, b *k3s.Backend) []model.Running {
	t.Helper()
	all, err := b.Running(within(t, time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	return slices.DeleteFunc(all, func(r model.Running) bool { return r.App != "" || r.Container != "console" })
}

func TestRunConsoleWritesItsObjectsWhereK3sAppliesThemAndReturnsOnceItRuns(t *testing.T) {
	noConsole(t)
	consoleRuns(t)
	b := k3s.Through(serverHost{}, theCluster())
	addresses := []netip.Addr{netip.MustParseAddr("192.0.2.10"), netip.MustParseAddr("10.0.0.2")}
	for _, image := range []string{"example.test/console@sha256:1111", "example.test/console@sha256:2222"} {
		if err := b.RunConsole(within(t, 2*time.Minute), image, addresses); err != nil {
			t.Fatalf("RunConsole of %s: %v", image, err)
		}
		written, err := serverHost{}.Run(context.Background(), "cat "+consoleManifest, nil)
		if err != nil || written != k3s.ConsoleObjects(image, addresses) {
			t.Errorf("the console's objects on the control plane (%v):\n%s\nwant those of %s on the two addresses", err, written, image)
		}
		running := consoles(t, b)
		if len(running) != 1 || running[0].Image != image {
			t.Errorf("once RunConsole returned the consoles running were %+v, want one, from %s", running, image)
		}
	}
}

func TestRunConsoleFailsInTimeForAConsoleThatDoesntGetReadyWithItsPods(t *testing.T) {
	noConsole(t)
	b := k3s.Through(serverHost{}, theCluster())
	err := b.RunConsole(within(t, 20*time.Second), "example.test/console@sha256:3333", []netip.Addr{netip.MustParseAddr("10.0.0.2")})
	if err == nil || !strings.HasPrefix(err.Error(), "the console isn't ready: deployment moostack-console: 0 of 1 replicas are updated and available\n") || !strings.Contains(err.Error(), "pod moostack-console-") {
		t.Errorf("RunConsole of a console that doesn't get ready: %v\nwant how far its deployment is, and its pods", err)
	}
}

func TestRunConsoleFailsWhenItsObjectsArentWrittenOrArentApplied(t *testing.T) {
	noConsole(t)
	nowhere := aHost(t, "10.0.0.2")
	err := k3s.Through(nowhere, theCluster()).RunConsole(within(t, 5*time.Second), "example.test/console@sha256:4444", nil)
	if want := `the console's objects weren't applied: deployments.apps "moostack-console" not found`; err == nil || err.Error() != want {
		t.Errorf("RunConsole on a control plane that applies nothing: %v\nwant %s", err, want)
	}
	if _, written := nowhere.files[consoleManifest]; !written {
		t.Errorf("the console's objects weren't written to %s: the host has %q", consoleManifest, nowhere.left())
	}

	// Another console's objects are there, and these never take.
	consoleRuns(t)
	if err := k3s.Through(serverHost{}, theCluster()).RunConsole(within(t, 2*time.Minute), "example.test/console@sha256:5555", nil); err != nil {
		t.Fatal(err)
	}
	err = k3s.Through(aHost(t, "10.0.0.2"), theCluster()).RunConsole(within(t, 5*time.Second), "example.test/console@sha256:6666", nil)
	if want := "the console's objects weren't applied: the console runs example.test/console@sha256:5555"; err == nil || err.Error() != want {
		t.Errorf("RunConsole whose objects never take: %v\nwant %s", err, want)
	}

	unwritable := aHost(t, "10.0.0.2")
	unwritable.failAt = "write 1"
	err = k3s.Through(unwritable, theCluster()).RunConsole(within(t, time.Minute), "example.test/console@sha256:7777", nil)
	if want := "writing the console's objects: exit 1: write 1 failed"; err == nil || err.Error() != want {
		t.Errorf("RunConsole on a control plane that takes no file: %v\nwant %s", err, want)
	}
	if err := k3s.Through(nil, theCluster()).RunConsole(within(t, time.Minute), "example.test/console@sha256:8888", nil); err == nil || err.Error() != "the cluster has no control plane to reach it through" {
		t.Errorf("RunConsole without a control plane: %v", err)
	}
}

// consoleAPI is an API that lists pods as the console's and runs the
// command asked for in them, as exec says.
type consoleAPI struct {
	// pods are the pods listed, by name: true for one being deleted.
	pods map[string]bool
	// refuses makes the API answer a command with 403, and silent makes
	// it never answer one, until the channel is closed.
	refuses bool
	silent  chan struct{}
	// exec sends what the command prints and how it ended.
	exec func(conn *websocket.Conn)

	mu sync.Mutex
	// lists and runs are the queries of the lists and of the commands
	// asked for, and tokens the Authorization of each command.
	lists, runs, tokens []string
}

func (a *consoleAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()
	const pods = "/api/v1/namespaces/kube-system/pods"
	switch {
	case r.URL.Path == pods:
		a.lists = append(a.lists, r.URL.RawQuery)
		var items []map[string]any
		for _, name := range slices.Sorted(maps.Keys(a.pods)) {
			meta := map[string]any{"name": name, "namespace": "kube-system"}
			if a.pods[name] {
				meta["deletionTimestamp"] = "2026-11-24T10:00:00Z"
			}
			items = append(items, map[string]any{"metadata": meta})
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"apiVersion": "v1", "kind": "PodList", "items": items})
	case strings.HasPrefix(r.URL.Path, pods+"/") && strings.HasSuffix(r.URL.Path, "/exec"):
		a.runs = append(a.runs, strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, pods+"/"), "/exec")+"?"+r.URL.RawQuery)
		a.tokens = append(a.tokens, r.Header.Get("Authorization"))
		if a.silent != nil {
			a.mu.Unlock()
			<-a.silent
			a.mu.Lock()
			return
		}
		if a.refuses {
			http.Error(w, "pods/exec is forbidden", http.StatusForbidden)
			return
		}
		conn, err := (&websocket.Upgrader{Subprotocols: []string{"v4.channel.k8s.io"}}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		a.exec(conn)
	default:
		http.NotFound(w, r)
	}
}

// through is a backend that reaches api through a control plane, as the
// user of the token t.
func (a *consoleAPI) through(t *testing.T) *k3s.Backend {
	t.Helper()
	server := httptest.NewTLSServer(a)
	t.Cleanup(server.Close)
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	config := clientcmdapi.NewConfig()
	config.Clusters["c"] = &clientcmdapi.Cluster{Server: "https://127.0.0.1:6443", CertificateAuthorityData: ca}
	config.AuthInfos["u"] = &clientcmdapi.AuthInfo{Token: "t"}
	config.Contexts["x"] = &clientcmdapi.Context{Cluster: "c", AuthInfo: "u"}
	config.CurrentContext = "x"
	text, err := clientcmd.Write(*config)
	if err != nil {
		t.Fatal(err)
	}
	return k3s.Through(tunnel{&controlPlane{kubeconfig: string(text)}, server.Listener.Addr().String()}, k3s.Cluster{})
}

// tunnel is a control plane whose connections reach address and take no
// deadline, as those through SSH take none.
type tunnel struct {
	*controlPlane
	address string
}

func (h tunnel) DialTCP(ctx context.Context, _ string) (net.Conn, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", h.address)
	if err != nil {
		return nil, err
	}
	return undated{conn}, nil
}

type undated struct{ net.Conn }

func (undated) SetDeadline(time.Time) error {
	return errors.New("ssh: tcpChan: deadline not supported")
}
func (undated) SetReadDeadline(time.Time) error {
	return errors.New("ssh: tcpChan: deadline not supported")
}
func (undated) SetWriteDeadline(time.Time) error {
	return errors.New("ssh: tcpChan: deadline not supported")
}

// frames sends each frame on its stream: 1 is the command's output, 2
// its error output and 3 how it ended.
func frames(frames ...string) func(conn *websocket.Conn) {
	return func(conn *websocket.Conn) {
		for _, f := range frames {
			conn.WriteMessage(websocket.BinaryMessage, []byte(f))
		}
		conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	}
}

func TestConsoleTokenRunsTheConsolesTokenCommandInAPodOfItsThatRuns(t *testing.T) {
	api := &consoleAPI{
		pods: map[string]bool{"moostack-console-a": true, "moostack-console-b": false},
		exec: frames("", "\x01AAAAAAAAAAAA", "\x01BBBB\n", "\x02a warning\n", "\x03"+`{"metadata":{},"status":"Success"}`),
	}
	token, err := api.through(t).ConsoleToken(within(t, time.Minute))
	if err != nil || token != "AAAAAAAAAAAABBBB" {
		t.Errorf("ConsoleToken: %q, %v, want what the command printed", token, err)
	}
	if want := []string{"fieldSelector=status.phase%3DRunning&labelSelector=app%3Dmoostack-console"}; !slices.Equal(api.lists, want) {
		t.Errorf("the pods were listed with %q, want the console's that run", api.lists)
	}
	if want := []string{"moostack-console-b?command=console&command=token&container=console&stderr=true&stdout=true"}; !slices.Equal(api.runs, want) {
		t.Errorf("the commands run were %q, want %q", api.runs, want)
	}
	if !slices.Equal(api.tokens, []string{"Bearer t"}) {
		t.Errorf("the command was asked for as %q, want the kubeconfig's user", api.tokens)
	}
}

func TestConsoleTokenFailsAsTheCommandOrTheClusterDoes(t *testing.T) {
	for name, tc := range map[string]struct {
		api  *consoleAPI
		want string
	}{
		"a command that fails": {
			&consoleAPI{exec: frames("\x01partial", "\x02the socket isn't there\n", "\x03"+`{"status":"Failure","message":"command terminated with non-zero exit code","reason":"NonZeroExitCode","details":{"causes":[{"reason":"ExitCode","message":"1"}]}}`)},
			"running console token in moostack-console-b: the socket isn't there",
		},
		"a command that fails and prints nothing": {
			&consoleAPI{exec: frames("\x03" + `{"status":"Failure","message":"container not found (\"console\")"}`)},
			`running console token in moostack-console-b: container not found ("console")`,
		},
		"a connection that ends before the command": {
			&consoleAPI{exec: frames("\x01AAAA")},
			"running console token in moostack-console-b: the connection ended before the command did",
		},
		"a status that can't be read": {
			&consoleAPI{exec: frames("\x03{")},
			"running console token in moostack-console-b: its status can't be read: unexpected end of JSON input",
		},
		"a cluster that refuses the command": {
			&consoleAPI{refuses: true},
			"running console token in moostack-console-b: 403 Forbidden: pods/exec is forbidden",
		},
		"no pod of the console that runs": {
			&consoleAPI{pods: map[string]bool{"moostack-console-a": true}},
			"the console isn't running",
		},
	} {
		t.Run(name, func(t *testing.T) {
			if tc.api.pods == nil {
				tc.api.pods = map[string]bool{"moostack-console-b": false}
			}
			token, err := tc.api.through(t).ConsoleToken(within(t, time.Minute))
			if err == nil || err.Error() != tc.want || token != "" {
				t.Errorf("ConsoleToken: %q, %v\nwant %s", token, err, tc.want)
			}
		})
	}
}

func TestConsoleTokenEndsWithItsContextWhateverTheClusterIsDoing(t *testing.T) {
	for name, api := range map[string]*consoleAPI{
		"a command that never ends":        {exec: func(conn *websocket.Conn) { conn.ReadMessage() }},
		"a request that is never answered": {silent: make(chan struct{})},
	} {
		api.pods = map[string]bool{"moostack-console-b": false}
		if api.silent != nil {
			defer close(api.silent)
		}
		b := api.through(t)
		start := time.Now()
		_, err := b.ConsoleToken(within(t, 2*time.Second))
		if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 20*time.Second {
			t.Errorf("%s: ConsoleToken: %v after %s, want that its time was up, once it was", name, err, time.Since(start))
		}
		ctx, cancel := context.WithCancelCause(context.Background())
		cause := errors.New("interrupted")
		time.AfterFunc(time.Second, func() { cancel(cause) })
		start = time.Now()
		_, err = api.through(t).ConsoleToken(ctx)
		if err == nil || err.Error() != "interrupted" || time.Since(start) > 20*time.Second {
			t.Errorf("%s: ConsoleToken interrupted: %v after %s, want the cause alone, at once", name, err, time.Since(start))
		}
	}
}

// consoleDeployments is an API that answers each read of the console's
// Deployment with the next of answers, and with the last from then on:
// "ready" for one that runs image as applied, anything else as an error
// of the API's.
type consoleDeployments struct {
	image   string
	answers []string
	mu      sync.Mutex
	reads   int
}

func (a *consoleDeployments) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/apis/apps/v1/namespaces/kube-system/deployments/moostack-console" {
		http.NotFound(w, r)
		return
	}
	a.mu.Lock()
	answer := a.answers[min(a.reads, len(a.answers)-1)]
	a.reads++
	a.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	if answer != "ready" {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","message":%q,"code":500}`, answer)
		return
	}
	fmt.Fprintf(w, `{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"moostack-console","namespace":"kube-system","generation":1},
		"spec":{"replicas":1,"template":{"spec":{"containers":[{"name":"console","image":%q}]}}},
		"status":{"observedGeneration":1,"replicas":1,"updatedReplicas":1,"availableReplicas":1}}`, a.image)
}

// apiHost is a control plane that takes files and whose connections
// reach address.
type apiHost struct {
	*host
	kubeconfig, address string
}

func (h apiHost) Run(ctx context.Context, cmd string, stdin io.Reader) (string, error) {
	if cmd == "cat /etc/rancher/k3s/k3s.yaml" {
		return h.kubeconfig, nil
	}
	return h.host.Run(ctx, cmd, stdin)
}

func (h apiHost) DialTCP(ctx context.Context, _ string) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(ctx, "tcp", h.address)
}

func TestRunConsoleOutlivesAReadOfItsDeploymentThatFailsWhileItWaits(t *testing.T) {
	api := &consoleDeployments{image: "example.test/console@sha256:9999", answers: []string{"ready", "etcdserver: request timed out", "ready"}}
	server := httptest.NewTLSServer(api)
	defer server.Close()
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	config := clientcmdapi.NewConfig()
	config.Clusters["c"] = &clientcmdapi.Cluster{Server: "https://127.0.0.1:6443", CertificateAuthorityData: ca}
	config.AuthInfos["u"] = &clientcmdapi.AuthInfo{Token: "t"}
	config.Contexts["x"] = &clientcmdapi.Context{Cluster: "c", AuthInfo: "u"}
	config.CurrentContext = "x"
	text, err := clientcmd.Write(*config)
	if err != nil {
		t.Fatal(err)
	}
	b := k3s.Through(apiHost{aHost(t, "10.0.0.2"), string(text), server.Listener.Addr().String()}, theCluster())
	if err := b.RunConsole(within(t, time.Minute), api.image, nil); err != nil {
		t.Errorf("RunConsole with one read that fails while it waits: %v", err)
	}
	if api.reads != 3 {
		t.Errorf("the console's Deployment was read %d times, want 3: applied, the failure, ready", api.reads)
	}

	never := &consoleDeployments{image: api.image, answers: []string{"ready", "etcdserver: request timed out"}}
	failing := httptest.NewTLSServer(never)
	defer failing.Close()
	b = k3s.Through(apiHost{aHost(t, "10.0.0.2"), string(text), failing.Listener.Addr().String()}, theCluster())
	err = b.RunConsole(within(t, 4*time.Second), api.image, nil)
	if err == nil || !strings.HasPrefix(err.Error(), "the console isn't ready: its deployment can't be read: ") || !strings.Contains(err.Error(), "etcdserver: request timed out") {
		t.Errorf("RunConsole whose reads keep failing: %v\nwant that its deployment can't be read, and why", err)
	}
}

func TestABackendInsideTheClusterMakesNoHostANodeAndRunsNoConsole(t *testing.T) {
	b := deployer(t)
	ctx := within(t, time.Minute)
	if err := b.RunConsole(ctx, "example.test/console:1", nil); err == nil || err.Error() != "the cluster has no control plane to reach it through" {
		t.Errorf("RunConsole: %v", err)
	}
	h := aHost(t, "10.0.0.3")
	if _, err := b.Preflight(ctx, h, orchestrator.NodeRequest{Name: "n"}); err == nil {
		t.Error("Preflight succeeded")
	}
	if err := b.AddNode(ctx, h, model.Node{Name: "n", PrivateAddress: netip.MustParseAddr("10.0.0.3")}, func(orchestrator.Step) {}); err == nil {
		t.Error("AddNode succeeded")
	}
	if len(h.commands()) != 0 {
		t.Errorf("the host ran %q", h.commands())
	}
}
