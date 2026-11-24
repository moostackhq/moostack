package cli

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"slices"
	"strings"
	"testing"

	"golang.org/x/crypto/ssh"

	"github.com/moostackhq/moostack/internal/clusterdir"
	"github.com/moostackhq/moostack/internal/model"
	"github.com/moostackhq/moostack/internal/orchestrator"
	"github.com/moostackhq/moostack/internal/orchestrator/orchestratortest"
	"github.com/moostackhq/moostack/internal/registry"
	"github.com/moostackhq/moostack/internal/sshclient"
)

// stubRemote is a remote that answers each command with result, and
// notes what it is asked.
type stubRemote struct {
	remote
	result  sshclient.Result
	err     error
	asked   []string
	stopped []string
	conn    net.Conn
}

func (r *stubRemote) Run(_ context.Context, cmd string, stdin io.Reader) (sshclient.Result, error) {
	in := ""
	if stdin != nil {
		data, _ := io.ReadAll(stdin)
		in = " < " + string(data)
	}
	r.asked = append(r.asked, cmd+in)
	return r.result, r.err
}

func (r *stubRemote) RunStoppable(_ context.Context, cmd string) (sshclient.Result, error) {
	r.stopped = append(r.stopped, cmd)
	return r.result, r.err
}

func (r *stubRemote) Upload(_ context.Context, file string, content io.Reader, mode fs.FileMode, owner string) error {
	data, _ := io.ReadAll(content)
	r.asked = append(r.asked, "upload "+file+" "+mode.String()+" "+owner+" "+string(data))
	return nil
}

func (r *stubRemote) DialTCP(_ context.Context, addr string) (net.Conn, error) {
	r.asked = append(r.asked, "dial "+addr)
	return r.conn, nil
}

func TestTheOrchestratorActsOnTheControlPlaneThroughItsConnection(t *testing.T) {
	ctx := context.Background()
	near, far := net.Pipe()
	defer near.Close()
	defer far.Close()
	cp := &stubRemote{result: sshclient.Result{Stdout: []byte("out\n"), Stderr: []byte("a warning\n")}, conn: near}
	var host orchestrator.Host
	env := Env{Orchestrator: func(h orchestrator.Host) orchestrator.Orchestrator {
		host = h
		return &orchestratortest.Fake{}
	}}
	orchestratorThrough(env, cp)
	if host == nil {
		t.Fatal("the orchestrator was built without the control plane")
	}
	if out, err := host.Run(ctx, "cat file", strings.NewReader("input")); out != "out\n" || err != nil {
		t.Errorf("Run: %q, %v, want the command's stdout", out, err)
	}
	if out, err := host.RunStoppable(ctx, "wait"); out != "out\n" || err != nil {
		t.Errorf("RunStoppable: %q, %v, want the command's stdout", out, err)
	}
	if err := host.Upload(ctx, "/etc/file", strings.NewReader("content"), 0o600); err != nil {
		t.Errorf("Upload: %v", err)
	}
	if conn, err := host.DialTCP(ctx, "127.0.0.1:6443"); conn != near || err != nil {
		t.Errorf("DialTCP: %v, %v, want the control plane's connection", conn, err)
	}
	want := []string{"cat file < input", "upload /etc/file -rw------- root content", "dial 127.0.0.1:6443"}
	if strings.Join(cp.asked, "|") != strings.Join(want, "|") || strings.Join(cp.stopped, "|") != "wait" {
		t.Errorf("the control plane was asked %q and, to stop with the context, %q, want %q and wait", cp.asked, cp.stopped, want)
	}

	cp.result = sshclient.Result{ExitCode: 3, Stdout: []byte("out\n"), Stderr: []byte("no such file\n")}
	for name, run := range map[string]func() (string, error){
		"Run":          func() (string, error) { return host.Run(ctx, "cat file", nil) },
		"RunStoppable": func() (string, error) { return host.RunStoppable(ctx, "wait") },
	} {
		if out, err := run(); out != "" || err == nil || err.Error() != "exit 3: no such file" {
			t.Errorf("%s of a command that fails: %q, %v, want its exit code and stderr", name, out, err)
		}
	}
}

func TestTheErrorOfACommandThatFailsOnTheControlPlaneHoldsNothingItPrintedOnStdout(t *testing.T) {
	ctx := context.Background()
	cp := &stubRemote{result: sshclient.Result{ExitCode: 143, Stdout: []byte("client-key-data: SECRETKEY\n")}}
	host := orchestratorHost{cp}
	for name, run := range map[string]func() (string, error){
		"Run":          func() (string, error) { return host.Run(ctx, "cat file", nil) },
		"RunStoppable": func() (string, error) { return host.RunStoppable(ctx, "cat file") },
	} {
		if out, err := run(); out != "" || err == nil || err.Error() != "exit 143" {
			t.Errorf("%s of a command that fails with nothing on stderr: %q, %v, want its exit code alone", name, out, err)
		}
	}
}

func TestACommandOnTheControlPlaneWhoseContextEndsFailsWithTheCause(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("the operation lock is lost")
	cancel(cause)
	host := orchestratorHost{&stubRemote{err: errors.New("the session is closed")}}
	for name, run := range map[string]func() (string, error){
		"Run":          func() (string, error) { return host.Run(ctx, "cat file", nil) },
		"RunStoppable": func() (string, error) { return host.RunStoppable(ctx, "cat file") },
	} {
		if _, err := run(); !errors.Is(err, cause) {
			t.Errorf("%s on an ended context: %v, want its cause", name, err)
		}
	}
	if _, err := host.Run(context.Background(), "cat file", nil); err == nil || err.Error() != "the session is closed" {
		t.Errorf("Run over a connection that fails: %v, want the connection's error", err)
	}
}

func TestWithoutAControlPlaneTheOrchestratorHasNoHost(t *testing.T) {
	built := false
	env := Env{Orchestrator: func(h orchestrator.Host) orchestrator.Orchestrator {
		built = true
		if h != nil {
			t.Errorf("the orchestrator was built with the host %v, want none", h)
		}
		return &orchestratortest.Fake{}
	}}
	orchestratorThrough(env, nil)
	if !built {
		t.Error("the orchestrator wasn't built")
	}
}

func TestTheOrchestratorOfAnEnvThatBuildsNoneRunsAnAppsOwnManifestsAndReachesNoClusterWithoutAControlPlane(t *testing.T) {
	o := orchestratorThrough(Env{}, nil)
	if !o.Can().KubernetesObjects {
		t.Error("the orchestrator takes no app with its own manifests or chart")
	}
	app := model.App{Name: "shop", Containers: []model.Container{{Name: "web", Image: "example.test/web:1", Replicas: 1}}}
	if err := o.Check(app); err != nil {
		t.Errorf("Check: %v", err)
	}
	app.Name = "default"
	if err := o.Check(app); err == nil {
		t.Error("Check accepted an app named default")
	}
	app.Name = "shop"
	if err := o.Deploy(context.Background(), app, func(orchestrator.Step) {}); err == nil || err.Error() != "the cluster has no control plane to reach it through" {
		t.Errorf("Deploy: %v, want no control plane to reach the cluster through", err)
	}
}

func TestTheOrchestratorOfAClusterIsToldItsVersionEmailRegistryAndControlPlane(t *testing.T) {
	ctx := context.Background()
	cluster := clusterdir.Cluster{Ref: "prod", K3s: K3sVersion, Email: "ops@example.com"}
	host := &stubRemote{result: sshclient.Result{Stdout: []byte("not a kubeconfig\n")}}
	n := model.Node{Name: "node1", ControlPlane: true, PrivateAddress: netip.MustParseAddr("10.0.0.2"), Interface: "ens10", MemoryMiB: 1974}
	if err := orchestratorOf(Env{}, cluster, netip.Addr{}, nil).AddNode(ctx, orchestratorHost{host}, n, func(orchestrator.Step) {}); err == nil {
		t.Fatal("AddNode on a host without a kubeconfig succeeded")
	}
	written := strings.Join(host.asked, "\n")
	for _, want := range []string{
		"upload /etc/rancher/k3s/registries.yaml -rw------- root mirrors:\n    " + registry.Name + ":\n        endpoint:\n            - http://10.0.0.2:5000\n",
		`email: "ops@example.com"`,
	} {
		if !strings.Contains(written, want) {
			t.Errorf("the host wasn't written %q:\n%s", want, written)
		}
	}
	if !slices.ContainsFunc(host.stopped, func(cmd string) bool { return strings.Contains(cmd, "INSTALL_K3S_VERSION='"+K3sVersion+"'") }) {
		t.Errorf("the host wasn't asked to install k3s %s: %q", K3sVersion, host.stopped)
	}

	joining := &stubRemote{result: sshclient.Result{Stdout: []byte("arch=x86_64\nmem_kib=2013456\nsystemd=yes\nhas=curl\nhas=flock\nhas=ip\naddr=ens10 10.0.0.3/32\n")}}
	cp := &stubRemote{result: sshclient.Result{Stdout: []byte("not a kubeconfig\n")}}
	orchestratorOf(Env{}, cluster, netip.MustParseAddr("10.0.0.2"), cp).Preflight(ctx, orchestratorHost{joining}, orchestrator.NodeRequest{Name: "node2"})
	if !slices.ContainsFunc(joining.asked, func(cmd string) bool { return strings.Contains(cmd, "https://10.0.0.2:6443/ping") }) {
		t.Errorf("the host that joins wasn't asked to reach the control plane at 10.0.0.2: %q", joining.asked)
	}

	unknown := clusterdir.Cluster{Ref: "prod", K3s: "v1.38.0+k3s1"}
	if _, err := orchestratorOf(Env{}, unknown, netip.Addr{}, nil).Preflight(ctx, orchestratorHost{host}, orchestrator.NodeRequest{Name: "node1", ControlPlane: true}); err == nil || err.Error() != "mooctl has no checksum of k3s v1.38.0+k3s1's install script" {
		t.Errorf("Preflight for a cluster of another k3s version: %v", err)
	}
}

// joiningConn is a fake connection to a host of a cluster whose API is
// at api: it answers what the k3s backend asks of a control plane and of
// a host that is to join, whose install of k3s fails, and notes it.
type joiningConn struct {
	*fakeConn
	api, kubeconfig string
	asked           *[]string
}

func (j joiningConn) Run(ctx context.Context, cmd string, stdin io.Reader) (sshclient.Result, error) {
	if cmd == AppliedCommitScript {
		return j.fakeConn.Run(ctx, cmd, stdin)
	}
	*j.asked = append(*j.asked, j.n.target+": "+cmd)
	switch {
	case strings.HasPrefix(cmd, `echo "arch=`):
		return sshclient.Result{Stdout: []byte("arch=x86_64\nmem_kib=2013456\nsystemd=yes\nhas=curl\nhas=flock\nhas=ip\naddr=ens10 10.0.0.3/32\n")}, nil
	case cmd == "cat /etc/rancher/k3s/k3s.yaml":
		return sshclient.Result{Stdout: []byte(j.kubeconfig)}, nil
	case strings.Contains(cmd, "INSTALL_K3S_EXEC="):
		return sshclient.Result{ExitCode: 1, Stderr: []byte("no network")}, nil
	}
	return sshclient.Result{Stdout: []byte("token\n")}, nil
}

func (j joiningConn) RunStoppable(ctx context.Context, cmd string) (sshclient.Result, error) {
	return j.Run(ctx, cmd, nil)
}

func (j joiningConn) DialTCP(ctx context.Context, _ string) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(ctx, "tcp", j.api)
}

func TestNodeAddBuildsItsOrchestratorFromTheClustersFileAndItsControlPlane(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/version":
			fmt.Fprintf(w, `{"gitVersion":%q}`, K3sVersion)
		case "/api/v1/nodes":
			io.WriteString(w, `{"apiVersion":"v1","kind":"NodeList","items":[{"metadata":{"name":"node1"}}]}`)
		default:
			w.WriteHeader(http.StatusNotFound)
			io.WriteString(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404}`)
		}
	}))
	defer api.Close()
	ca := base64.StdEncoding.EncodeToString(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: api.Certificate().Raw}))
	kubeconfig := "apiVersion: v1\nkind: Config\ncurrent-context: x\nclusters: [{name: c, cluster: {server: \"https://127.0.0.1:6443\", certificate-authority-data: " + ca + "}}]\nusers: [{name: u, user: {token: t}}]\ncontexts: [{name: x, context: {cluster: c, user: u}}]\n"
	var out bytes.Buffer
	var asked []string
	env := fakeEnvFor(c, home, &out)
	env.Orchestrator = nil
	env.Connect = func(ctx context.Context, target string, key ssh.PublicKey) (remote, ssh.PublicKey, error) {
		r, k, err := c.connect(ctx, target, key)
		if err != nil {
			return r, k, err
		}
		return joiningConn{r.(*fakeConn), api.Listener.Addr().String(), kubeconfig, &asked}, k, nil
	}
	err := New(env).Run(t.Context(), []string{"mooctl", "-c", "prod", "cluster", "node-add", "root@h2"})
	if err == nil || err.Error() != "installing k3s: exit 1: no network" {
		t.Fatalf("node-add through the k3s backend, of a host whose install fails: %v\n%s", err, out.String())
	}
	if want := "Adding node2 to prod (node, amd64, 1966 MiB, 10.0.0.3)\nInstalling k3s...\nUninstalling...\n"; out.String() != want {
		t.Errorf("output:\n%s\nwant:\n%s", out.String(), want)
	}
	all := strings.Join(asked, "\n")
	for what, want := range map[string]string{
		"the host that joins asked to reach the control plane at its address": "root@h2: curl -sk --max-time 10 -o /dev/null https://10.0.0.2:6443/ping",
		"the cluster's version of k3s installed":                              "INSTALL_K3S_VERSION='" + K3sVersion + "' INSTALL_K3S_EXEC=agent",
		"the join token read on the control plane":                            "root@h1: cat /var/lib/rancher/k3s/server/agent-token",
	} {
		if !strings.Contains(all, want) {
			t.Errorf("not %s: the hosts were asked\n%s", what, all)
		}
	}
}

func TestTheCommandsFakeConnectionRunsNothingOnAnEndedContext(t *testing.T) {
	c, _ := fakeProd(t)
	conn, _, err := c.connect(t.Context(), "root@h1", nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := conn.Run(ctx, ownUninstallScript, nil); !errors.Is(err, context.Canceled) || len(c.runs) != 0 {
		t.Errorf("a script on an ended context: %v, with %d scripts run", err, len(c.runs))
	}
	if err := conn.Upload(ctx, AppliedCommitFile, strings.NewReader("x"), 0o644, "root"); !errors.Is(err, context.Canceled) || len(c.hosts["root@h1"].files) != 0 {
		t.Errorf("an upload on an ended context: %v, with the files %v written", err, c.hosts["root@h1"].files)
	}
}
