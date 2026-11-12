package cli

import (
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	ggcrregistry "github.com/google/go-containerregistry/pkg/registry"
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

const webService = `apiVersion: v1
kind: Service
metadata: {name: web}
spec: {selector: {app: web}, ports: [{port: 80}]}
`

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

func TestDeployPrunesWhatTheAppNoLongerHas(t *testing.T) {
	c, home := deployingProd(t)
	app := manifestsApp(t, webDeployment, webService)
	t.Chdir(app)
	out := mustRun(t, c, home, "deploy")
	if !strings.Contains(out, "Deployed web to prod") {
		t.Errorf("output:\n%s", out)
	}
	if !c.apps["web"]["service/web"] || !c.apps["web"]["deployment/web"] {
		t.Fatalf("applied = %v", c.apps["web"])
	}

	writeManifests(t, app, webDeployment)
	mustRun(t, c, home, "deploy")
	if !slices.Equal(c.deleted, []string{"web/service/web"}) {
		t.Errorf("deleted = %v", c.deleted)
	}
	if c.apps["web"]["service/web"] || !c.apps["web"]["deployment/web"] {
		t.Errorf("applied = %v", c.apps["web"])
	}
}

func TestDeployRemove(t *testing.T) {
	c, home := deployingProd(t)
	app := manifestsApp(t, webDeployment)
	t.Chdir(app)
	if out := mustRun(t, c, home, "deploy"); !strings.HasPrefix(out, "Deploying web to prod\n") {
		t.Errorf("deploy's output:\n%s", out)
	}

	t.Chdir(home)
	if out, err := onFake(t, c, home, "deploy", "--remove", "other"); err == nil || !strings.Contains(err.Error(), "no app other") {
		t.Errorf("an app that isn't there: %v\n%s", err, out)
	}
	if out, err := onFake(t, c, home, "deploy", "--remove"); err == nil || !strings.Contains(err.Error(), "which app?") {
		t.Errorf("no app named: %v\n%s", err, out)
	}
	if out := mustRun(t, c, home, "deploy", "--remove", "web"); !strings.HasPrefix(out, "Removing web from prod...\n") || !strings.Contains(out, "Removed web from prod") {
		t.Errorf("output:\n%s", out)
	}
	if _, found := c.apps["web"]; found {
		t.Error("web's namespace is still there")
	}

	t.Chdir(app)
	mustRun(t, c, home, "deploy")
	mustRun(t, c, home, "deploy", "--remove")
	if _, found := c.apps["web"]; found {
		t.Error("web's namespace is still there after a remove from its directory")
	}
}

func TestDeployRefusesBadObjectsBeforeConnecting(t *testing.T) {
	c, home := deployingProd(t)
	app := manifestsApp(t, "kind: Deployment\n")
	t.Chdir(app)
	before := len(c.runs)
	if out, err := onFake(t, c, home, "deploy"); err == nil {
		t.Fatalf("deployed objects without metadata:\n%s", out)
	}
	if len(c.runs) != before {
		t.Errorf("ran on the cluster: %v", c.runs[before:])
	}
}

func TestLogsOfAnApp(t *testing.T) {
	c, home := deployingProd(t)
	app := manifestsApp(t, webDeployment)
	t.Chdir(app)
	mustRun(t, c, home, "deploy")

	if out := mustRun(t, c, home, "logs", "--tail", "5"); !strings.Contains(out, "a log line") || !c.ran("root@h1", "logs -n 'web'") || !c.ran("root@h1", "--tail=5") {
		t.Errorf("logs:\n%s", out)
	}
	if out := mustRun(t, c, home, "logs", "-f", "web"); !strings.Contains(out, "Stopped: the pods followed have ended.") || !c.ran("root@h1", "--follow") {
		t.Errorf("logs -f:\n%s", out)
	}
	if _, err := onFake(t, c, home, "logs", "other"); err == nil || !strings.Contains(err.Error(), "no app other") {
		t.Errorf("an app that isn't there: %v", err)
	}
	if _, err := onFake(t, c, home, "logs", "Web;rm"); err == nil {
		t.Error("a name that isn't an app's was taken")
	}
	c.noPods = true
	if _, err := onFake(t, c, home, "logs", "-f", "web"); err == nil || !strings.Contains(err.Error(), "web has no pods") {
		t.Errorf("an app without pods: %v", err)
	}
	c.noPods = false
	t.Chdir(home)
	if _, err := onFake(t, c, home, "logs"); err == nil || !strings.Contains(err.Error(), "which app?") {
		t.Errorf("no app named: %v", err)
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
	if out := mustRun(t, c, home, "deploy"); !strings.Contains(out, "Waiting for cert-manager and Traefik...") {
		t.Errorf("output:\n%s", out)
	}

	c, home = deployingProd(t)
	c.servicesDown = true
	t.Chdir(app)
	out, err := onFake(t, c, home, "deploy")
	if err == nil || !strings.Contains(err.Error(), "the cluster isn't ready for apps: ") || !strings.Contains(err.Error(), "Traefik") {
		t.Fatalf("deploy = %v\n%s", err, out)
	}
	if _, found := c.apps["web"]; found || len(c.manifests) > 0 {
		t.Errorf("applied %d manifests to a cluster that isn't ready", len(c.manifests))
	}
}

const badObject = `apiVersion: v1
kind: Bad
metadata: {name: bad}
`

func TestAnObjectOfAFailedDeployIsPrunedLater(t *testing.T) {
	c, home := deployingProd(t)
	app := manifestsApp(t, webDeployment)
	t.Chdir(app)
	mustRun(t, c, home, "deploy")

	writeManifests(t, app, webDeployment, webService, badObject)
	if out, err := onFake(t, c, home, "deploy"); err == nil {
		t.Fatalf("a deploy with a bad object passed:\n%s", out)
	}
	if !c.apps["web"]["service/web"] {
		t.Fatalf("the failed deploy didn't record what it applied: %v", c.apps["web"])
	}

	writeManifests(t, app, webDeployment)
	mustRun(t, c, home, "deploy")
	if !slices.Contains(c.deleted, "web/service/web") || !slices.Contains(c.deleted, "web/bad/bad") {
		t.Errorf("deleted = %v", c.deleted)
	}
	if got := c.apps["web"]; len(got) != 1 || !got["deployment/web"] {
		t.Errorf("recorded = %v", got)
	}
}

func TestARemoveThatCantReachTheRegistryRemovesNothing(t *testing.T) {
	c, home := deployingProd(t)
	t.Chdir(manifestsApp(t, webDeployment))
	mustRun(t, c, home, "deploy")
	registry := c.registry
	c.registry = ""
	if out, err := onFake(t, c, home, "deploy", "--remove", "web"); err == nil {
		t.Fatalf("--remove passed without the registry:\n%s", out)
	}
	if _, found := c.apps["web"]; !found {
		t.Fatal("web's namespace went though its images stayed")
	}
	c.registry = registry
	mustRun(t, c, home, "deploy", "--remove", "web")
	if _, found := c.apps["web"]; found {
		t.Error("the rerun left web's namespace")
	}
}

// A record with an object that has no name, as an earlier mooctl could
// write, is deployed over without deleting it.
func TestARecordedObjectWithoutANameIsDropped(t *testing.T) {
	c, home := deployingProd(t)
	app := manifestsApp(t, webDeployment)
	t.Chdir(app)
	mustRun(t, c, home, "deploy")
	c.apps["web"]["deployment/"] = true

	mustRun(t, c, home, "deploy")
	if slices.Contains(c.deleted, "web/deployment/") {
		t.Errorf("deleted = %v", c.deleted)
	}
	if got := c.apps["web"]; len(got) != 1 || !got["deployment/web"] {
		t.Errorf("recorded = %v", got)
	}
}

const webNamespace = `apiVersion: v1
kind: Namespace
metadata: {name: web}
`

func TestDeployRefusesAnObjectThatIsntInANamespace(t *testing.T) {
	c, home := deployingProd(t)
	app := manifestsApp(t, webDeployment, webNamespace)
	t.Chdir(app)
	out, err := onFake(t, c, home, "deploy")
	if err == nil || !strings.Contains(err.Error(), "namespace/web isn't in a namespace") {
		t.Fatalf("deploy = %v\n%s", err, out)
	}
	if len(c.manifests) > 0 || len(c.apps["web"]) > 0 {
		t.Errorf("applied %d manifests, recorded %v", len(c.manifests), c.apps["web"])
	}
}

// A record with an object that isn't in a namespace, as an earlier
// mooctl could write, is deployed over without deleting the object.
func TestARecordedObjectThatIsntInANamespaceIsNeverDeleted(t *testing.T) {
	c, home := deployingProd(t)
	app := manifestsApp(t, webDeployment)
	t.Chdir(app)
	mustRun(t, c, home, "deploy")
	c.apps["web"]["namespace/web"] = true

	mustRun(t, c, home, "deploy")
	if slices.Contains(c.deleted, "web/namespace/web") {
		t.Errorf("deleted = %v", c.deleted)
	}
	if got := c.apps["web"]; len(got) != 1 || !got["deployment/web"] {
		t.Errorf("recorded = %v", got)
	}
}

func TestDeployAndRemoveWaitForNoOtherOperation(t *testing.T) {
	c, home := deployingProd(t)
	app := manifestsApp(t, webDeployment)
	t.Chdir(app)
	mustRun(t, c, home, "deploy")
	applied := len(c.manifests)

	c.locked = true
	if out, err := onFake(t, c, home, "deploy"); err == nil || !strings.Contains(err.Error(), "another mooctl operation") {
		t.Errorf("deploy = %v\n%s", err, out)
	}
	if out, err := onFake(t, c, home, "deploy", "--remove"); err == nil || !strings.Contains(err.Error(), "another mooctl operation") {
		t.Errorf("deploy --remove = %v\n%s", err, out)
	}
	if len(c.manifests) != applied || c.apps["web"] == nil {
		t.Errorf("the cluster changed while another operation ran: %d manifests, apps %v", len(c.manifests), c.apps)
	}
	c.locked = false
	mustRun(t, c, home, "deploy")
	if c.locked {
		t.Error("deploy kept the lock")
	}
}
