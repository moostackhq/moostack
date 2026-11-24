package cli

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	ggcrregistry "github.com/google/go-containerregistry/pkg/registry"

	"github.com/moostackhq/moostack/internal/model"
	"github.com/moostackhq/moostack/internal/orchestrator"
)

const webDeployment = `apiVersion: apps/v1
kind: Deployment
metadata: {name: web}
spec:
  selector: {matchLabels: {app: web}}
  template:
    metadata: {labels: {app: web}}
    spec: {containers: [{name: web, image: nginx}]}
`

var update = flag.Bool("update", false, "write the golden files")

// golden compares got with the golden file called name, or writes it
// there under -update.
func golden(t *testing.T, name, got string) {
	t.Helper()
	path := filepath.Join(goldenDir, name)
	if *update {
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
		t.Errorf("%s:\n got:\n%s\nwant:\n%s", name, got, want)
	}
}

// goldenDir is the directory of the golden files.
var goldenDir = func() string {
	dir, err := filepath.Abs("testdata")
	if err != nil {
		panic(err)
	}
	return dir
}()

// manifestsApp is an app directory deploying the manifests given, to
// prod, without a build.
func manifestsApp(t *testing.T, manifests ...string) string {
	t.Helper()
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "k8s"), 0o755)
	os.WriteFile(filepath.Join(dir, "deploy.yaml"), []byte("cluster: prod\nname: web\nmanifests: k8s\n"), 0o644)
	writeManifests(t, dir, manifests...)
	return dir
}

func writeManifests(t *testing.T, dir string, manifests ...string) {
	t.Helper()
	os.RemoveAll(filepath.Join(dir, "k8s", "app.yaml"))
	if err := os.WriteFile(filepath.Join(dir, "k8s", "app.yaml"), []byte(strings.Join(manifests, "---\n")), 0o644); err != nil {
		t.Fatal(err)
	}
}

// deployingProd is prod with one node and a registry.
func deployingProd(t *testing.T) (*fakeCluster, string) {
	t.Helper()
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	server := httptest.NewServer(ggcrregistry.New())
	t.Cleanup(server.Close)
	c.registry = server.Listener.Addr().String()
	return c, home
}

func TestDeployRefusesWhatTheOrchestratorCantRunBeforeAnythingElse(t *testing.T) {
	c, home := deployingProd(t)
	docker, dir := fakeDocker(t)
	app := t.TempDir()
	os.WriteFile(filepath.Join(app, "Dockerfile"), []byte("FROM scratch\n"), 0o644)
	os.WriteFile(filepath.Join(app, "deploy.yaml"), []byte("cluster: prod\nname: web\ncontainers: {web: {build: {dockerfile: .}, port: 80}}\nsecrets: [NOT_IN_THE_STORE]\n"), 0o644)
	t.Chdir(app)
	c.orch.Fail("Check", errors.New(`name: "web" is a name the cluster keeps`))
	before := len(c.runs)
	var out strings.Builder
	env := fakeEnvFor(c, home, nil)
	env.Stdout, env.Stderr, env.Docker = &out, &out, docker
	err := New(env).Run(t.Context(), []string{"mooctl", "-c", "prod", "deploy"})
	if want := `name: "web" is a name the cluster keeps`; err == nil || err.Error() != want {
		t.Fatalf("deploy: %v, want %q\n%s", err, want, out.String())
	}
	if out.Len() != 0 {
		t.Errorf("a refused deploy printed:\n%s", out.String())
	}
	if len(c.runs) != before || c.orch.hosts != 0 || c.locked {
		t.Errorf("a refused deploy ran on the cluster: %v, with %d orchestrators through its control plane", c.runs[before:], c.orch.hosts)
	}
	if _, err := os.Stat(filepath.Join(dir, "builds")); err == nil {
		t.Error("a refused deploy built an image")
	}
	if calls := c.orch.made(""); !slices.Equal(calls, []string{"Check web"}) {
		t.Errorf("the calls to the orchestrator were %q, want Check alone", calls)
	}
}

// What the orchestrator is asked to check is the app with an image for
// each build, before any is built, and each secret without its value.
func TestDeployChecksTheAppAsItWillRunBeforeItIsBuilt(t *testing.T) {
	c, home := deployingProd(t)
	checked := make(chan model.App, 1)
	env := fakeEnvFor(c, home, &bytes.Buffer{})
	env.Orchestrator = func(orchestrator.Host) orchestrator.Orchestrator { return checking{c.orch, checked} }
	for file, want := range map[string]model.App{
		"name: shop\ncontainers:\n  api: {build: {dockerfile: .}, port: 3000, host: api.example.com, replicas: 2}\n  cache: {image: \"valkey/valkey:8\", command: [valkey, --save]}\nsecrets: [B, A]\n": {
			Name: "shop",
			Containers: []model.Container{
				{Name: "api", Image: "moostack.local/shop/api", Port: 3000, Host: "api.example.com", Replicas: 2},
				{Name: "cache", Image: "valkey/valkey:8", Command: []string{"valkey", "--save"}, Replicas: 1},
			},
			Secrets: map[string]string{"A": "", "B": ""},
		},
		"name: shop\nmanifests: k8s\nbuild: {dockerfile: .}\n": {Name: "shop", Manifests: "k8s", Image: "moostack.local/shop"},
		"name: shop\nchart: k8s\n":                             {Name: "shop", Chart: "k8s"},
	} {
		app := t.TempDir()
		os.MkdirAll(filepath.Join(app, "k8s"), 0o755)
		os.WriteFile(filepath.Join(app, "Dockerfile"), []byte("FROM scratch\n"), 0o644)
		os.WriteFile(filepath.Join(app, "k8s", "Chart.yaml"), nil, 0o644)
		os.WriteFile(filepath.Join(app, "deploy.yaml"), []byte(file), 0o644)
		t.Chdir(app)
		if want.Manifests != "" {
			want.Manifests = filepath.Join(app, want.Manifests)
		}
		if want.Chart != "" {
			want.Chart = filepath.Join(app, want.Chart)
		}
		if err := New(env).Run(t.Context(), []string{"mooctl", "-c", "prod", "deploy"}); err == nil || err.Error() != "checked" {
			t.Fatalf("deploy: %v, want the check's refusal", err)
		}
		if got := <-checked; !reflect.DeepEqual(got, want) {
			t.Errorf("the app checked was\n%+v, want\n%+v", got, want)
		}
	}
}

// checking is an orchestrator whose Check sends the app and refuses it.
type checking struct {
	orchestrator.Orchestrator
	apps chan<- model.App
}

func (o checking) Check(app model.App) error {
	o.apps <- app
	return errors.New("checked")
}

// digest matches an image's digest.
var digest = regexp.MustCompile(`sha256:[0-9a-f]{64}`)

func TestDeployPrintsItsStepsInOrderAndRunsTheAppBuilt(t *testing.T) {
	golden, err := filepath.Abs("testdata")
	if err != nil {
		t.Fatal(err)
	}
	c, home := deployingProd(t)
	docker, _ := fakeDocker(t)
	editorWriting(t, "GREETING:\n  value: hello\nUNUSED:\n  value: no\n")
	key := filepath.Join(home, "id_ed25519")
	if out, err := mooctl(t, home, home, "-c", "prod", "--ssh-key", key, "secret", "edit"); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	app := t.TempDir()
	os.MkdirAll(filepath.Join(app, "api"), 0o755)
	os.WriteFile(filepath.Join(app, "Dockerfile"), []byte("FROM scratch\n"), 0o644)
	os.WriteFile(filepath.Join(app, "api", "Dockerfile"), []byte("FROM scratch\n"), 0o644)
	os.WriteFile(filepath.Join(app, "deploy.yaml"), []byte(`name: shop
cluster: prod
containers:
  api: {build: {dockerfile: api}, port: 3000, host: api.example.com}
  cache: {image: "valkey/valkey:8", port: 6379}
  jobs: {build: {dockerfile: .}, command: [bin/jobs], replicas: 3}
  web: {build: {dockerfile: .}, port: 8080, host: shop.example.com}
secrets: [GREETING]
`), 0o644)
	t.Chdir(app)
	c.unissued = "api-tls"
	c.servicesDown, c.servicesComeUp = true, true

	var stdout, stderr bytes.Buffer
	env := fakeEnvFor(c, home, nil)
	env.Stdout, env.Stderr, env.Docker = &stdout, &stderr, docker
	if err := New(env).Run(t.Context(), []string{"mooctl", "-c", "prod", "--ssh-key", key, "deploy"}); err != nil {
		t.Fatalf("%v\n%s%s", err, stderr.String(), stdout.String())
	}
	for file, got := range map[string]string{"deploy.stdout.golden": stdout.String(), "deploy.stderr.golden": stderr.String()} {
		got = digest.ReplaceAllString(got, "sha256:<digest>")
		if *update {
			if err := os.WriteFile(filepath.Join(golden, file), []byte(got), 0o644); err != nil {
				t.Fatal(err)
			}
			continue
		}
		want, err := os.ReadFile(filepath.Join(golden, file))
		if err != nil {
			t.Fatal(err)
		}
		if got != string(want) {
			t.Errorf("%s:\n got:\n%s\nwant:\n%s", file, got, want)
		}
	}

	ref := func(container string) string {
		return "moostack.local/shop/" + container + "@" + liveOf(t, c, "shop/"+container)
	}
	want := model.App{
		Name: "shop",
		Containers: []model.Container{
			{Name: "api", Image: ref("api"), Port: 3000, Host: "api.example.com", Replicas: 1},
			{Name: "cache", Image: "valkey/valkey:8", Port: 6379, Replicas: 1},
			{Name: "jobs", Image: ref("jobs"), Command: []string{"bin/jobs"}, Replicas: 3},
			{Name: "web", Image: ref("jobs"), Port: 8080, Host: "shop.example.com", Replicas: 1},
		},
		Secrets: map[string]string{"GREETING": "hello"},
	}
	if got := c.orch.deployed["shop"]; !reflect.DeepEqual(got, want) {
		t.Errorf("the app deployed was\n%+v, want\n%+v", got, want)
	}
	if calls, want := c.orch.made(""), []string{"Check shop", "Nodes", "Deploy shop", "Certificate shop api", "Certificate shop web"}; !slices.Equal(calls, want) {
		t.Errorf("the calls to the orchestrator were %q, want %q", calls, want)
	}
	if c.orch.hosts != 1 {
		t.Errorf("%d orchestrators were built to act through the control plane, want the deploy's one", c.orch.hosts)
	}
}

func TestDeployMarksTheImagesAsRunningBeforeItWaitsForACertificate(t *testing.T) {
	c, home := deployingProd(t)
	docker, _ := fakeDocker(t)
	app := t.TempDir()
	os.WriteFile(filepath.Join(app, "Dockerfile"), []byte("FROM scratch\n"), 0o644)
	os.WriteFile(filepath.Join(app, "deploy.yaml"), []byte("cluster: prod\nname: web\ncontainers: {web: {build: {dockerfile: .}, port: 80, host: web.example.com}}\n"), 0o644)
	t.Chdir(app)
	marked := "never asked"
	c.orch.onCertificate = func() { marked = liveOf(t, c, "web/web") }
	deployWith(t, c, home, docker)
	if _, running, _ := strings.Cut(imageOf(t, c, "web"), "@"); marked != running {
		t.Errorf("when the certificate was waited for the registry kept %q as the image that runs, and %s ran", marked, running)
	}
}

func TestADeployInterruptedWhileItWaitsForACertificateFailsWithoutAWarning(t *testing.T) {
	c, home := deployingProd(t)
	app := t.TempDir()
	os.WriteFile(filepath.Join(app, "deploy.yaml"), []byte("cluster: prod\nname: web\ncontainers:\n  api: {image: nginx, port: 80, host: api.example.com}\n  web: {image: nginx, port: 80, host: web.example.com}\n"), 0o644)
	t.Chdir(app)
	ctx, interrupt := context.WithCancelCause(t.Context())
	c.orch.onCertificate = func() { interrupt(errors.New("interrupted")) }
	var stdout, stderr bytes.Buffer
	env := fakeEnvFor(c, home, nil)
	env.Stdout, env.Stderr = &stdout, &stderr
	err := New(env).Run(ctx, []string{"mooctl", "-c", "prod", "deploy"})
	if err == nil || err.Error() != "interrupted" {
		t.Errorf("deploy: %v, want the interrupt", err)
	}
	if strings.Contains(stderr.String(), "Warning") || stdout.String() != "Deployed web to prod\n" {
		t.Errorf("the interrupted deploy printed %q on stdout and %q on stderr, want the result and no warning", stdout.String(), stderr.String())
	}
	if calls := c.orch.made("Certificate"); len(calls) != 1 {
		t.Errorf("the certificates waited for after the interrupt were %q, want the first alone", calls)
	}
}

func TestADeployWhoseLockIsLostEndsWithThatAsItsCause(t *testing.T) {
	c, home := deployingProd(t)
	t.Chdir(manifestsApp(t, webDeployment))
	c.orch.onDeploy = func(ctx context.Context) error {
		// The connection that holds the lock drops.
		c.lock.Close()
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-time.After(10 * time.Second):
			return errors.New("the deploy's context didn't end with its lock")
		}
	}
	var stdout, stderr bytes.Buffer
	env := fakeEnvFor(c, home, nil)
	env.Stdout, env.Stderr = &stdout, &stderr
	err := New(env).Run(t.Context(), []string{"mooctl", "-c", "prod", "deploy"})
	if err == nil || err.Error() != "the connection that holds the operation lock dropped" {
		t.Errorf("deploy: %v, want the lost lock", err)
	}
	if stdout.Len() != 0 {
		t.Errorf("the deploy printed a result: %q", stdout.String())
	}
}

func TestADeployThatFailsPrintsNoResultAndWaitsForNoCertificate(t *testing.T) {
	c, home := deployingProd(t)
	app := t.TempDir()
	os.WriteFile(filepath.Join(app, "deploy.yaml"), []byte("cluster: prod\nname: web\ncontainers: {web: {image: nginx, port: 80, host: web.example.com}}\n"), 0o644)
	t.Chdir(app)
	c.orch.Fail("Deploy", errors.New("web isn't ready: deployment web: 0 of 1 replicas are updated and available"))
	var stdout, stderr bytes.Buffer
	env := fakeEnvFor(c, home, nil)
	env.Stdout, env.Stderr = &stdout, &stderr
	err := New(env).Run(t.Context(), []string{"mooctl", "-c", "prod", "deploy"})
	if want := "web isn't ready: deployment web: 0 of 1 replicas are updated and available"; err == nil || err.Error() != want {
		t.Errorf("deploy: %v, want %q", err, want)
	}
	if stdout.Len() != 0 || stderr.String() != "Deploying web to prod\n" {
		t.Errorf("a failed deploy printed %q on stdout and %q on stderr", stdout.String(), stderr.String())
	}
	if calls := c.orch.made("Certificate"); len(calls) != 0 {
		t.Errorf("a failed deploy made the calls %q", calls)
	}
	if c.locked {
		t.Error("a failed deploy kept the lock")
	}
}

func TestDeployWaitsForTheClustersServices(t *testing.T) {
	c, home := deployingProd(t)
	app := manifestsApp(t, webDeployment)
	t.Chdir(app)

	if out := mustRun(t, c, home, "deploy"); strings.Contains(out, "Waiting for cert-manager") {
		t.Errorf("a deploy to a ready cluster waited:\n%s", out)
	}

	c.servicesDown, c.servicesComeUp = true, true
	if out := mustRun(t, c, home, "deploy"); !strings.Contains(out, "Deploying web to prod\nWaiting for cert-manager and Traefik...\nApplying web...\n") {
		t.Errorf("output:\n%s", out)
	}

	c, home = deployingProd(t)
	c.servicesDown = true
	t.Chdir(app)
	out, err := onFake(t, c, home, "deploy")
	if err == nil || !strings.Contains(err.Error(), "the cluster isn't ready for apps: ") || !strings.Contains(err.Error(), "Traefik") {
		t.Fatalf("deploy = %v\n%s", err, out)
	}
	if out != "Deploying web to prod\nWaiting for cert-manager and Traefik...\n" {
		t.Errorf("output:\n%s", out)
	}
}

func TestDeployAndRemoveWaitForNoOtherOperation(t *testing.T) {
	c, home := deployingProd(t)
	app := manifestsApp(t, webDeployment)
	t.Chdir(app)
	mustRun(t, c, home, "deploy")
	deploys := len(c.orch.made("Deploy"))

	c.locked = true
	if out, err := onFake(t, c, home, "deploy"); err == nil || !strings.Contains(err.Error(), "another mooctl operation") {
		t.Errorf("deploy = %v\n%s", err, out)
	}
	if out, err := onFake(t, c, home, "deploy", "--remove"); err == nil || !strings.Contains(err.Error(), "another mooctl operation") {
		t.Errorf("deploy --remove = %v\n%s", err, out)
	}
	if got := len(c.orch.made("Deploy")); got != deploys || c.apps["web"] == nil {
		t.Errorf("the cluster changed while another operation ran: %d deploys more, apps %v", got-deploys, c.apps)
	}
	c.locked = false
	mustRun(t, c, home, "deploy")
	if c.locked {
		t.Error("deploy kept the lock")
	}
}

func TestPushBarFillsWithThePush(t *testing.T) {
	for _, c := range []struct {
		complete, total int64
		want            string
	}{
		{0, 0, "[--------------------] 0/0 MB"},
		{0, 218_400_000, "[--------------------] 0/218 MB"},
		{98_000_000, 218_400_000, "[########------------] 98/218 MB"},
		{218_399_999, 218_400_000, "[###################-] 218/218 MB"},
		{218_400_000, 218_400_000, "[####################] 218/218 MB"},
		{300_000_000, 218_400_000, "[####################] 218/218 MB"},
		{-5, 218_400_000, "[--------------------] 0/218 MB"},
	} {
		if got := pushBar(c.complete, c.total); got != c.want {
			t.Errorf("pushBar(%d, %d) = %q, not %q", c.complete, c.total, got, c.want)
		}
	}
}
