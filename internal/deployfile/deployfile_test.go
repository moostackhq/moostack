package deployfile_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/moostackhq/moostack/internal/deployfile"
)

// appDir is an app directory with a Dockerfile, a deploy/ of manifests,
// a chart/ and the deploy file content.
func appDir(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	for name, data := range map[string]string{
		"Dockerfile":        "FROM scratch\n",
		"deploy/app.yaml":   "kind: ConfigMap\n",
		"chart/Chart.yaml":  "name: web\n",
		deployfile.FileName: content,
	} {
		p := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(data), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func TestReadContainers(t *testing.T) {
	dir := appDir(t, `name: shop
cluster: acme/prod
containers:
  web:
    build: {dockerfile: .}
    port: 8080
    host: shop.example.com
    replicas: 2
  jobs:
    build: {railpack: .}
    command: [bin/jobs, --queue, mail]
  cache:
    image: valkey/valkey:8
    port: 6379
secrets: [GREETING, DB_URL]
`)
	app, err := deployfile.Read(dir)
	if err != nil {
		t.Fatal(err)
	}
	if app.Cluster != "acme/prod" || app.Name != "shop" || app.Build != nil || app.Manifests != "" || app.Chart != "" || app.Dir != dir {
		t.Errorf("app = %+v", app)
	}
	if strings.Join(app.Secrets, ",") != "GREETING,DB_URL" {
		t.Errorf("secrets = %v", app.Secrets)
	}
	if len(app.Containers) != 3 {
		t.Fatalf("containers = %+v", app.Containers)
	}
	cache, jobs, web := app.Containers[0], app.Containers[1], app.Containers[2]
	if cache.Name != "cache" || cache.Image != "valkey/valkey:8" || cache.Build != nil || cache.Port != 6379 || cache.Host != "" || cache.Replicas != 1 || cache.Command != nil {
		t.Errorf("cache = %+v", cache)
	}
	if jobs.Name != "jobs" || jobs.Image != "" || jobs.Build == nil || jobs.Build.Railpack != dir || jobs.Port != 0 || jobs.Replicas != 1 || strings.Join(jobs.Command, " ") != "bin/jobs --queue mail" {
		t.Errorf("jobs = %+v", jobs)
	}
	if web.Name != "web" || web.Build == nil || web.Build.Dockerfile != dir || web.Port != 8080 || web.Host != "shop.example.com" || web.Replicas != 2 {
		t.Errorf("web = %+v", web)
	}
}

func TestReadManifestsAndChart(t *testing.T) {
	dir := appDir(t, "name: web\nbuild: {railpack: .}\nmanifests: deploy\n")
	app, err := deployfile.Read(dir)
	if err != nil {
		t.Fatal(err)
	}
	if app.Cluster != "" || app.Build.Railpack != dir || app.Manifests != filepath.Join(dir, "deploy") || app.Containers != nil {
		t.Errorf("app = %+v", app)
	}

	dir = appDir(t, "name: web\nchart: chart\n")
	app, err = deployfile.Read(dir)
	if err != nil {
		t.Fatal(err)
	}
	if app.Build != nil || app.Chart != filepath.Join(dir, "chart") || app.Containers != nil {
		t.Errorf("app = %+v", app)
	}

	dir = appDir(t, "name: web\nbuild: {dockerfile: .}\nchart: chart\n")
	if app, err = deployfile.Read(dir); err != nil || app.Build.Dockerfile != dir {
		t.Errorf("a chart with a build: %+v, %v", app, err)
	}
}

func TestReadRefuses(t *testing.T) {
	const web = "name: shop\ncontainers: {web: {build: {dockerfile: .}, port: 80"
	for name, tc := range map[string]struct{ content, want string }{
		"no name":               {"chart: chart\n", "name:"},
		"bad name":              {"name: Web\nchart: chart\n", "name:"},
		"kube- name":            {"name: kube-web\nchart: chart\n", "name:"},
		"cert-manager":          {"name: cert-manager\nchart: chart\n", "name:"},
		"bad cluster":           {"cluster: a/b/c\nname: web\nchart: chart\n", "cluster:"},
		"two builds":            {"name: web\nbuild: {dockerfile: ., railpack: .}\nchart: chart\n", "build: give one of dockerfile and railpack"},
		"empty build":           {"name: web\nbuild: {}\nchart: chart\n", "build: give one of dockerfile and railpack"},
		"no Dockerfile":         {"name: web\nbuild: {dockerfile: deploy}\nchart: chart\n", "has no Dockerfile"},
		"none of the three":     {"name: web\n", "give one of containers, manifests and chart"},
		"chart and manifests":   {"name: web\nchart: chart\nmanifests: deploy\n", "give one of containers, manifests and chart"},
		"containers and chart":  {web + "}}\nchart: chart\n", "give one of containers, manifests and chart"},
		"containers, manifests": {web + "}}\nmanifests: deploy\n", "give one of containers, manifests and chart"},
		"the old objects key":   {"name: web\nobjects: {chart: chart}\n", "objects"},
		"no containers":         {"name: shop\ncontainers: {}\n", "containers: give at least one"},
		"a top-level build":     {web + "}}\nbuild: {dockerfile: .}\n", "build: each container gives its own"},
		"an empty container":    {"name: shop\ncontainers: {web: }\n", "containers: web: give one of build and image"},
		"no build nor image":    {"name: shop\ncontainers: {web: {port: 80}}\n", "containers: web: give one of build and image"},
		"a build and an image":  {web + ", image: nginx}}\n", "containers: web: give one of build and image"},
		"a container's builds":  {"name: shop\ncontainers: {web: {build: {dockerfile: ., railpack: .}}}\n", "containers: web: build: give one of dockerfile and railpack"},
		"a build's directory":   {"name: shop\ncontainers: {web: {build: {dockerfile: deploy}}}\n", "containers: web: build: dockerfile:"},
		"an image with a space": {"name: shop\ncontainers: {web: {image: 'nginx latest'}}\n", "containers: web: image:"},
		"a container's name":    {"name: shop\ncontainers: {Web: {image: nginx}}\n", "containers:"},
		"a name with a digit":   {"name: shop\ncontainers: {1web: {image: nginx}}\n", "must start with a letter"},
		"an empty command":      {web + ", command: []}}\n", "containers: web: command:"},
		"an empty program":      {web + ", command: ['', -v]}}\n", "containers: web: command:"},
		"port zero":             {"name: shop\ncontainers: {web: {image: nginx, port: 0}}\n", "containers: web: port:"},
		"a port too large":      {"name: shop\ncontainers: {web: {image: nginx, port: 65536}}\n", "containers: web: port:"},
		"bad host":              {web + ", host: 'a b'}}\n", "containers: web: host:"},
		"a host without a port": {"name: shop\ncontainers: {web: {image: nginx, host: a.example.com}}\n", "containers: web: host: give the port"},
		"one host twice":        {"name: shop\ncontainers: {api: {image: a, port: 80, host: a.example.com}, web: {image: b, port: 80, host: a.example.com}}\n", "containers: web: host: a.example.com is api's already"},
		"negative replicas":     {web + ", replicas: -1}}\n", "containers: web: replicas:"},
		"the secret's name":     {"name: web-tls\ncontainers: {web: {image: nginx, port: 80, host: a.example.com}}\nsecrets: [A]\n", "containers: web: host:"},
		"no Chart.yaml":         {"name: web\nchart: deploy\n", "has no Chart.yaml"},
		"missing manifests":     {"name: web\nmanifests: nowhere\n", "manifests:"},
		"secret name":           {"name: web\nchart: chart\nsecrets: [db-url]\n", "secrets:"},
		"secret twice":          {"name: web\nchart: chart\nsecrets: [A, A]\n", "secrets:"},
		"unknown field":         {"name: web\nchart: chart\nport: 80\n", "port"},
		"a container's field":   {web + ", ports: [80]}}\n", "ports"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := deployfile.Read(appDir(t, tc.content))
			if err == nil {
				t.Fatal("taken")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("%v, which doesn't say %q", err, tc.want)
			}
		})
	}
}

func TestReadTakes(t *testing.T) {
	for name, content := range map[string]string{
		"an app named as its certificate's secret, without secrets": "name: web-tls\ncontainers: {web: {image: nginx, port: 80, host: a.example.com}}\n",
		"a container without a port":                                "name: shop\ncontainers: {jobs: {image: nginx}}\n",
		"two hosts":                                                 "name: shop\ncontainers: {api: {image: a, port: 80, host: a.example.com}, web: {image: b, port: 80, host: b.example.com}}\n",
		"replicas of zero as one":                                   "name: shop\ncontainers: {web: {image: nginx, replicas: 0}}\n",
	} {
		t.Run(name, func(t *testing.T) {
			app, err := deployfile.Read(appDir(t, content))
			if err != nil {
				t.Fatal(err)
			}
			if app.Containers[len(app.Containers)-1].Replicas != 1 {
				t.Errorf("replicas = %d", app.Containers[0].Replicas)
			}
		})
	}
}

func TestNamesKubernetesAccepts(t *testing.T) {
	for name, content := range map[string]string{
		"leading digit":           "name: 1web\nchart: chart\n",
		"chart name of 47":        "name: " + strings.Repeat("a", 47) + "\nchart: chart\n",
		"host label of 64":        "name: web\ncontainers: {web: {image: nginx, port: 80, host: " + strings.Repeat("a", 64) + ".example.com}}\n",
		"container name of 64":    "name: web\ncontainers: {" + strings.Repeat("a", 64) + ": {image: nginx}}\n",
		"container name's hyphen": "name: web\ncontainers: {web-: {image: nginx}}\n",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := deployfile.Read(appDir(t, content)); err == nil {
				t.Error("taken")
			}
		})
	}
	for name, content := range map[string]string{
		"chart name of 46":     "name: " + strings.Repeat("a", 46) + "\nchart: chart\n",
		"app name of 63":       "name: " + strings.Repeat("a", 63) + "\ncontainers: {web: {image: nginx, port: 80}}\n",
		"container name of 63": "name: web\ncontainers: {" + strings.Repeat("a", 63) + ": {image: nginx}}\n",
		"host label of 63":     "name: web\ncontainers: {web: {image: nginx, port: 80, host: " + strings.Repeat("a", 63) + ".example.com}}\n",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := deployfile.Read(appDir(t, content)); err != nil {
				t.Error(err)
			}
		})
	}
}

func TestReadRefReadsOnlyTheClusterAndName(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, deployfile.FileName), []byte("cluster: acme/prod\nname: web\nmanifests: gone\nunknown: 1\n"), 0o644)
	cluster, name, err := deployfile.ReadRef(dir)
	if err != nil || cluster != "acme/prod" || name != "web" {
		t.Errorf("ReadRef = %q, %q, %v", cluster, name, err)
	}
	os.WriteFile(filepath.Join(dir, deployfile.FileName), []byte("name: Web\n"), 0o644)
	if _, _, err := deployfile.ReadRef(dir); err == nil {
		t.Error("a bad name was taken")
	}
	os.WriteFile(filepath.Join(dir, deployfile.FileName), []byte("cluster: a/b/c\nname: web\n"), 0o644)
	if _, _, err := deployfile.ReadRef(dir); err == nil {
		t.Error("a bad cluster was taken")
	}
}
