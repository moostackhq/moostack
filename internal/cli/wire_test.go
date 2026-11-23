package cli

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"net"
	"strings"
	"testing"

	"github.com/moostackhq/moostack/internal/model"
	"github.com/moostackhq/moostack/internal/orchestrator"
	"github.com/moostackhq/moostack/internal/orchestrator/orchestratortest"
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
