package objects_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/moostackhq/moostack/internal/deployfile"
	"github.com/moostackhq/moostack/internal/objects"
)

// docs are the YAML documents of a manifest.
func docs(t *testing.T, manifest string) []map[string]any {
	t.Helper()
	var out []map[string]any
	dec := yaml.NewDecoder(strings.NewReader(manifest))
	for {
		var m map[string]any
		err := dec.Decode(&m)
		if err == io.EOF {
			return out
		}
		if err != nil {
			t.Fatalf("%v in:\n%s", err, manifest)
		}
		out = append(out, m)
	}
}

func kinds(ds []map[string]any) string {
	var k []string
	for _, d := range ds {
		k = append(k, d["kind"].(string))
	}
	return strings.Join(k, ",")
}

func TestAContainerWithAPortAndAHost(t *testing.T) {
	app := &deployfile.App{Name: "shop", Secrets: []string{"GREETING"}, Containers: []deployfile.Container{{Name: "web", Build: &deployfile.Build{Dockerfile: "."}, Port: 8080, Host: "web.example.com", Replicas: 2}}}
	objs, err := objects.Make(objects.App{App: app, Images: map[string]string{"web": "moostack.local/shop/web:abc"}, Secrets: map[string]string{"GREETING": "hi"}, Issuer: "moostack"})
	if err != nil {
		t.Fatal(err)
	}
	if got := kinds(docs(t, objs.Cluster)); got != "Namespace" {
		t.Errorf("cluster kinds = %s", got)
	}
	own := objs.Namespaced
	ds := append(docs(t, objs.Cluster), docs(t, own)...)
	if got := kinds(ds); got != "Namespace,Secret,Deployment,Service,Ingress" {
		t.Fatalf("kinds = %s", got)
	}
	for _, d := range ds[1:] {
		meta := d["metadata"].(map[string]any)
		if ns := meta["namespace"]; ns != "shop" {
			t.Errorf("%s in namespace %v", d["kind"], ns)
		}
		if d["kind"] != "Secret" && meta["name"] != "web" {
			t.Errorf("%s is named %v", d["kind"], meta["name"])
		}
		if app := meta["labels"].(map[string]any)[objects.AppLabel]; app != "shop" {
			t.Errorf("%s is labelled as of app %v", d["kind"], app)
		}
	}
	if v := ds[1]["data"].(map[string]any)["GREETING"]; v != base64.StdEncoding.EncodeToString([]byte("hi")) {
		t.Errorf("secret data = %v", v)
	}
	for _, want := range []string{"image: moostack.local/shop/web:abc", "replicas: 2", "containerPort: 8080", "secretRef:", "cert-manager.io/cluster-issuer: moostack", "host: web.example.com", "secretName: web-tls", "port: 8080\n", "targetPort: 8080", "number: 8080"} {
		if !strings.Contains(own, want) {
			t.Errorf("no %q in:\n%s", want, own)
		}
	}
	if strings.Contains(own, "command:") {
		t.Errorf("a command without one in the deploy file:\n%s", own)
	}
}

func TestAContainerWithoutHostOrSecretsHasNoIngressNorSecret(t *testing.T) {
	app := &deployfile.App{Name: "web", Containers: []deployfile.Container{{Name: "web", Image: "i", Port: 80, Replicas: 1}}}
	objs, err := objects.Make(objects.App{App: app})
	if err != nil {
		t.Fatal(err)
	}
	own := objs.Namespaced
	if got := kinds(docs(t, own)); got != "Deployment,Service" {
		t.Errorf("kinds = %s", got)
	}
	if strings.Contains(own, "envFrom") {
		t.Errorf("envFrom without secrets:\n%s", own)
	}
}

// container is the first container of the Deployment called name in ds.
func container(t *testing.T, ds []map[string]any, name string) map[string]any {
	t.Helper()
	for _, d := range ds {
		if d["kind"] == "Deployment" && d["metadata"].(map[string]any)["name"] == name {
			pod := d["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
			return pod["containers"].([]any)[0].(map[string]any)
		}
	}
	t.Fatalf("no Deployment %s", name)
	return nil
}

func TestEachContainerHasItsOwnObjects(t *testing.T) {
	build := &deployfile.Build{Dockerfile: "."}
	app := &deployfile.App{Name: "shop", Containers: []deployfile.Container{
		{Name: "cache", Image: "valkey/valkey:8", Port: 6379, Replicas: 1},
		{Name: "jobs", Build: build, Command: []string{"bin/jobs", "--queue", "mail"}, Replicas: 3},
		{Name: "web", Build: build, Port: 8080, Host: "shop.example.com", Replicas: 2},
	}}
	objs, err := objects.Make(objects.App{App: app, Images: map[string]string{"jobs": "built", "web": "built"}, Issuer: "moostack"})
	if err != nil {
		t.Fatal(err)
	}
	ds := docs(t, objs.Namespaced)
	if got := kinds(ds); got != "Deployment,Service,Deployment,Deployment,Service,Ingress" {
		t.Fatalf("kinds = %s", got)
	}
	for name, image := range map[string]string{"cache": "valkey/valkey:8", "jobs": "built", "web": "built"} {
		if got := container(t, ds, name)["image"]; got != image {
			t.Errorf("%s runs %v, not %s", name, got, image)
		}
	}
	jobs := container(t, ds, "jobs")
	if got := fmt.Sprint(jobs["command"]); got != "[bin/jobs --queue mail]" {
		t.Errorf("jobs' command = %s", got)
	}
	if jobs["ports"] != nil || jobs["readinessProbe"] != nil {
		t.Errorf("jobs, which has no port, has ports or a probe: %v", jobs)
	}
	if container(t, ds, "web")["command"] != nil {
		t.Error("web has a command")
	}
	for _, d := range ds {
		name := d["metadata"].(map[string]any)["name"].(string)
		spec := d["spec"].(map[string]any)
		switch d["kind"] {
		case "Deployment":
			sel := spec["selector"].(map[string]any)["matchLabels"].(map[string]any)
			pod := spec["template"].(map[string]any)["metadata"].(map[string]any)["labels"].(map[string]any)
			for _, l := range []map[string]any{sel, pod} {
				if l[objects.ContainerLabel] != name || l[objects.AppLabel] != "shop" {
					t.Errorf("Deployment %s selects or labels %v", name, l)
				}
			}
			if want := map[string]int{"cache": 1, "jobs": 3, "web": 2}[name]; spec["replicas"] != want {
				t.Errorf("Deployment %s has %v replicas, not %d", name, spec["replicas"], want)
			}
		case "Service":
			if sel := spec["selector"].(map[string]any); sel[objects.ContainerLabel] != name || sel[objects.AppLabel] != "shop" {
				t.Errorf("Service %s selects %v", name, sel)
			}
			port := spec["ports"].([]any)[0].(map[string]any)
			if want := map[string]int{"cache": 6379, "web": 8080}[name]; port["port"] != want || port["targetPort"] != want {
				t.Errorf("Service %s's port = %v", name, port)
			}
		}
	}
}

func TestManifestsGetTheImageInPathOrder(t *testing.T) {
	dir := t.TempDir()
	for name, data := range map[string]string{
		"b/deploy.yaml": "apiVersion: v1\nkind: Deployment\nmetadata: {name: web}\nimage: ${IMAGE}",
		"a.yml":         "---\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: config, labels: {tier: web}}\n",
		"notes.txt":     "not a manifest",
	} {
		p := filepath.Join(dir, name)
		os.MkdirAll(filepath.Dir(p), 0o755)
		os.WriteFile(p, []byte(data), 0o644)
	}
	app := &deployfile.App{Name: "web", Manifests: dir}
	objs, err := objects.Make(objects.App{App: app, Image: "moostack.local/web:abc"})
	if err != nil {
		t.Fatal(err)
	}
	if got := kinds(docs(t, objs.Cluster)); got != "Namespace" {
		t.Errorf("cluster kinds = %s", got)
	}
	ds := docs(t, objs.Namespaced)
	for _, d := range ds {
		if l := d["metadata"].(map[string]any)["labels"].(map[string]any)[objects.AppLabel]; l != "web" {
			t.Errorf("%s isn't labelled with the app: %v", d["kind"], l)
		}
	}
	if got := kinds(ds); got != "ConfigMap,Deployment" {
		t.Errorf("manifest kinds = %s", got)
	}
	if ds[1]["image"] != "moostack.local/web:abc" {
		t.Errorf("image = %v", ds[1]["image"])
	}

	if _, err := objects.Make(objects.App{App: app}); err == nil {
		t.Error("${IMAGE} without a build was taken")
	}
}

func TestChartIsPackedIntoAHelmChart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "web-chart")
	for name, data := range map[string]string{
		"Chart.yaml":            "apiVersion: v2\nname: web\nversion: 0.1.0\n",
		"templates/deploy.yaml": "apiVersion: v1\nkind: Deployment\n",
	} {
		p := filepath.Join(dir, name)
		os.MkdirAll(filepath.Dir(p), 0o755)
		os.WriteFile(p, []byte(data), 0o644)
	}
	app := &deployfile.App{Name: "web", Chart: dir}
	objs, err := objects.Make(objects.App{App: app, Image: "moostack.local/web:abc"})
	if err != nil {
		t.Fatal(err)
	}
	ds := docs(t, objs.Cluster)
	if got := kinds(ds); got != "Namespace,HelmChart" {
		t.Fatalf("kinds = %s", got)
	}
	meta := ds[1]["metadata"].(map[string]any)
	spec := ds[1]["spec"].(map[string]any)
	if meta["name"] != "app-web" || meta["namespace"] != "kube-system" || spec["targetNamespace"] != "web" || spec["valuesContent"] != "image: moostack.local/web:abc\n" {
		t.Errorf("HelmChart = %v", ds[1])
	}
	data, err := base64.StdEncoding.DecodeString(spec["chartContent"].(string))
	if err != nil {
		t.Fatal(err)
	}
	gz, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	tr := tar.NewReader(gz)
	var names []string
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		names = append(names, h.Name)
	}
	sort.Strings(names)
	if got := strings.Join(names, ","); got != "web-chart/Chart.yaml,web-chart/templates/deploy.yaml" {
		t.Errorf("chart files = %s", got)
	}
}

func TestAChangedSecretChangesThePodTemplate(t *testing.T) {
	app := &deployfile.App{Name: "web", Secrets: []string{"A", "B"}, Containers: []deployfile.Container{{Name: "web", Image: "i", Port: 80, Replicas: 1}}}
	hash := func(values map[string]string) string {
		objs, err := objects.Make(objects.App{App: app, Secrets: values})
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range docs(t, objs.Namespaced) {
			if d["kind"] == "Deployment" {
				meta := d["spec"].(map[string]any)["template"].(map[string]any)["metadata"].(map[string]any)
				return meta["annotations"].(map[string]any)[objects.SecretsAnnotation].(string)
			}
		}
		t.Fatal("no Deployment")
		return ""
	}
	first := hash(map[string]string{"A": "1", "B": "2"})
	if again := hash(map[string]string{"A": "1", "B": "2"}); again != first {
		t.Error("the same secrets hash differently")
	}
	if changed := hash(map[string]string{"A": "1", "B": "3"}); changed == first {
		t.Error("a changed value keeps the hash")
	}
	if swapped := hash(map[string]string{"A": "2", "B": "1"}); swapped == first {
		t.Error("values swapped between names keep the hash")
	}
}

func TestManifestsKeepTheirOwnLabelsAndNeedMetadata(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "a.yaml"), []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: c, labels: {tier: web}}\n---\n---\napiVersion: v1\nkind: Secret\nmetadata: {name: s}\n"), 0o644)
	app := &deployfile.App{Name: "web", Manifests: dir}
	objs, err := objects.Make(objects.App{App: app})
	if err != nil {
		t.Fatal(err)
	}
	ds := docs(t, objs.Namespaced)
	if got := kinds(ds); got != "ConfigMap,Secret" {
		t.Fatalf("kinds = %s", got)
	}
	if l := ds[0]["metadata"].(map[string]any)["labels"].(map[string]any); l["tier"] != "web" || l[objects.AppLabel] != "web" {
		t.Errorf("labels = %v", l)
	}
	os.WriteFile(filepath.Join(dir, "b.yaml"), []byte("apiVersion: v1\nkind: ConfigMap\n"), 0o644)
	if _, err := objects.Make(objects.App{App: app}); err == nil {
		t.Error("an object without metadata was taken")
	}
}

func TestCheckRefusesWhatCantBeMade(t *testing.T) {
	empty := t.TempDir()
	os.WriteFile(filepath.Join(empty, "notes.txt"), []byte("x"), 0o644)
	if err := objects.Check(&deployfile.App{Name: "web", Manifests: empty}); err == nil {
		t.Error("a manifests directory without manifests was taken")
	}
	onlyDashes := t.TempDir()
	os.WriteFile(filepath.Join(onlyDashes, "a.yaml"), []byte("---\n---\n"), 0o644)
	if err := objects.Check(&deployfile.App{Name: "web", Manifests: onlyDashes}); err == nil {
		t.Error("manifests without objects were taken")
	}
	built := t.TempDir()
	os.WriteFile(filepath.Join(built, "a.yaml"), []byte("apiVersion: v1\nkind: Pod\nmetadata: {name: p}\nimage: ${IMAGE}\n"), 0o644)
	if err := objects.Check(&deployfile.App{Name: "web", Build: &deployfile.Build{Dockerfile: "."}, Manifests: built}); err != nil {
		t.Errorf("a built app's ${IMAGE}: %v", err)
	}
}

func TestChartFollowsSymlinksAndLeavesOutWhatHelmignoreNames(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "web")
	shared := filepath.Join(t.TempDir(), "shared.yaml")
	os.WriteFile(shared, []byte("apiVersion: v1\nkind: ConfigMap\n"), 0o644)
	for name, data := range map[string]string{
		"Chart.yaml":         "apiVersion: v2\nname: web\nversion: 0.1.0\n",
		".helmignore":        "# comment\n*.md\nsecrets/\n",
		"README.md":          "x",
		"secrets/key":        "x",
		"templates/a.yaml":   "apiVersion: v1\nkind: Deployment\n",
		".git/HEAD":          "ref",
		"templates/notes.md": "x",
	} {
		p := filepath.Join(dir, name)
		os.MkdirAll(filepath.Dir(p), 0o755)
		os.WriteFile(p, []byte(data), 0o644)
	}
	if err := os.Symlink(shared, filepath.Join(dir, "templates", "shared.yaml")); err != nil {
		t.Fatal(err)
	}
	app := &deployfile.App{Name: "web", Chart: dir}
	objs, err := objects.Make(objects.App{App: app})
	if err != nil {
		t.Fatal(err)
	}
	spec := docs(t, objs.Cluster)[1]["spec"].(map[string]any)
	data, _ := base64.StdEncoding.DecodeString(spec["chartContent"].(string))
	gz, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	tr := tar.NewReader(gz)
	var names []string
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		names = append(names, h.Name)
	}
	sort.Strings(names)
	if got := strings.Join(names, ","); got != "web/.helmignore,web/Chart.yaml,web/templates/a.yaml,web/templates/shared.yaml" {
		t.Errorf("chart files = %s", got)
	}
}

func TestManifestValuesStayAsWritten(t *testing.T) {
	dir := t.TempDir()
	manifest := `apiVersion: v1
kind: ConfigMap
metadata:
  name: c
  annotations: {released: 2024-01-02, mask: 0x1F, mode: 0755, flag: yes, empty: ~}
data:
  date: 2024-01-02
  hex: 0x1F
  octal: 0755
  word: on
  float: 1e3
  quoted: "2024-01-02"
---
{"apiVersion": "v1", "kind": "Secret", "metadata": {"name": "s", "labels": null}}
`
	os.WriteFile(filepath.Join(dir, "a.yaml"), []byte(manifest), 0o644)
	app := &deployfile.App{Name: "web", Manifests: dir}
	objs, err := objects.Make(objects.App{App: app})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"date: 2024-01-02\n", "hex: 0x1F\n", "octal: 0755\n", "word: on\n", "float: 1e3\n", `quoted: "2024-01-02"`, "released: 2024-01-02", "mask: 0x1F", "flag: yes"} {
		if !strings.Contains(objs.Namespaced, want) {
			t.Errorf("no %q in\n%s", want, objs.Namespaced)
		}
	}
	ds := docs(t, objs.Namespaced)
	for _, d := range ds {
		if l := d["metadata"].(map[string]any)["labels"].(map[string]any); l[objects.AppLabel] != "web" {
			t.Errorf("%s's labels = %v", d["kind"], l)
		}
	}
}

func TestManifestsNeedAKindAnAPIVersionAndTheAppsNamespace(t *testing.T) {
	for manifest, want := range map[string]string{
		"kind: ConfigMap\nmetadata: {name: c}\n":                                   "no apiVersion",
		"apiVersion: v1\nmetadata: {name: c}\n":                                    "no kind",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: c, namespace: other}\n": "in namespace other",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {}\n":                          "no name",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: \"\"}\n":                "no name",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: [c]}\n":                 "no name",
	} {
		dir := t.TempDir()
		os.WriteFile(filepath.Join(dir, "a.yaml"), []byte(manifest), 0o644)
		err := objects.Check(&deployfile.App{Name: "web", Manifests: dir})
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%q: %v, want %q", manifest, err, want)
		}
	}
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "a.yaml"), []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: c, namespace: web}\n"), 0o644)
	if err := objects.Check(&deployfile.App{Name: "web", Manifests: dir}); err != nil {
		t.Errorf("an object in the app's namespace: %v", err)
	}
}

func TestAChartThatLinksBackToItselfIsRefused(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "web")
	os.MkdirAll(filepath.Join(dir, "templates"), 0o755)
	os.WriteFile(filepath.Join(dir, "Chart.yaml"), []byte("apiVersion: v2\nname: web\nversion: 0.1.0\n"), 0o644)
	os.Symlink("..", filepath.Join(dir, "templates", "up"))
	done := make(chan error, 1)
	go func() {
		done <- objects.Check(&deployfile.App{Name: "web", Chart: dir})
	}()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "links back to") {
			t.Errorf("Check = %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("packing a chart that links back to itself didn't end")
	}
}
