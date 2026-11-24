package k3s_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/yaml"

	"github.com/moostackhq/moostack/internal/model"
	"github.com/moostackhq/moostack/internal/orchestrator/k3s"
)

var update = flag.Bool("update", false, "write the golden files")

// files writes the files, by path, below root.
func files(t *testing.T, root string, content map[string]string) string {
	t.Helper()
	for name, data := range content {
		p := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(data), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

// webChart is a chart's directory, named web-chart.
func webChart(t *testing.T) string {
	return files(t, filepath.Join(t.TempDir(), "web-chart"), map[string]string{
		"Chart.yaml":            "apiVersion: v2\nname: web\nversion: 0.1.0\n",
		"templates/deploy.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: c}\n",
		".helmignore":           "*.md\n",
		"README.md":             "x",
	})
}

func TestTheObjectsOfAnAppAreTheGoldenOnes(t *testing.T) {
	manifests := files(t, t.TempDir(), map[string]string{
		"b/deploy.yaml": "apiVersion: apps/v1\nkind: Deployment\nmetadata: {name: web}\nspec:\n  selector: {matchLabels: {app: web}}\n  template:\n    metadata: {labels: {app: web}}\n    spec: {containers: [{name: web, image: \"${IMAGE}\"}]}\n",
		"a.yml":         "---\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: config, labels: {tier: web}}\ndata:\n  date: 2024-01-02\n  octal: 0755\n  word: on\n---\n---\n{\"apiVersion\": \"v1\", \"kind\": \"Secret\", \"metadata\": {\"name\": \"s\", \"labels\": null}}\n",
		"notes.txt":     "not a manifest",
	})
	chart := webChart(t)
	for name, app := range map[string]model.App{
		"containers": {
			Name: "shop",
			Containers: []model.Container{
				{Name: "cache", Image: "valkey/valkey:8", Port: 6379, Replicas: 1},
				{Name: "jobs", Image: "moostack.local/shop/jobs@sha256:1111", Command: []string{"bin/jobs", "--queue", "mail"}, Replicas: 3},
				{Name: "web", Image: "moostack.local/shop/jobs@sha256:1111", Port: 8080, Host: "shop.example.com", Replicas: 2},
			},
			Secrets: map[string]string{"GREETING": "hi", "API_KEY": "s3cr3t: \"x\"\n"},
		},
		"plain":          {Name: "plain", Containers: []model.Container{{Name: "worker", Image: "example.test/worker:1", Replicas: 1}}},
		"manifests":      {Name: "web", Manifests: manifests, Image: "moostack.local/web@sha256:2222", Secrets: map[string]string{"TOKEN": "t"}},
		"chart":          {Name: "web", Chart: chart, Image: "moostack.local/web@sha256:3333"},
		"chart-no-image": {Name: "web", Chart: chart},
	} {
		cluster, own, err := k3s.Objects(app)
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		for file, got := range map[string]string{name + ".cluster.golden.yaml": cluster, name + ".app.golden.yaml": own} {
			path := filepath.Join("testdata", file)
			if *update {
				if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
					t.Fatal(err)
				}
				continue
			}
			want, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if got != string(want) {
				t.Errorf("%s:\n got:\n%s\nwant:\n%s", file, got, want)
			}
		}
	}
}

func TestCheckRefusesWhatTheClusterCantRun(t *testing.T) {
	web := []model.Container{{Name: "web", Image: "example.test/web:1", Port: 80, Host: "web.example.com", Replicas: 1}}
	chart := webChart(t)
	noManifests := files(t, t.TempDir(), map[string]string{"notes.txt": "x"})
	noObjects := files(t, t.TempDir(), map[string]string{"a.yaml": "---\n---\n"})
	noMetadata := files(t, t.TempDir(), map[string]string{"a.yaml": "apiVersion: v1\nkind: ConfigMap\n"})
	notYAML := files(t, t.TempDir(), map[string]string{"a.yaml": "a: [\n"})
	withImage := files(t, t.TempDir(), map[string]string{"a.yaml": "apiVersion: v1\nkind: Pod\nmetadata: {name: p}\nimage: ${IMAGE}\n"})
	looping := files(t, filepath.Join(t.TempDir(), "web"), map[string]string{"Chart.yaml": "apiVersion: v2\nname: web\nversion: 0.1.0\n", "templates/a.yaml": ""})
	if err := os.Symlink("..", filepath.Join(looping, "templates", "up")); err != nil {
		t.Fatal(err)
	}
	long := strings.Repeat("a", 63)
	for name, c := range map[string]struct {
		app  model.App
		want string
	}{
		"an app of containers":                   {model.App{Name: "shop", Containers: web}, ""},
		"no name":                                {model.App{Containers: web}, "the app has no name"},
		"a name of 63 characters":                {model.App{Name: long, Containers: web}, ""},
		"a name of 64 characters":                {model.App{Name: long + "a", Containers: web}, `name: "` + long + `a": must be no more than 63 characters`},
		"a name with a capital":                  {model.App{Name: "Shop", Containers: web}, `name: "Shop": a lowercase RFC 1123 label`},
		"a name that starts with a digit":        {model.App{Name: "1shop", Containers: web}, ""},
		"the namespace default":                  {model.App{Name: "default", Containers: web}, `name: "default" is a namespace of the cluster's own`},
		"the namespace cert-manager":             {model.App{Name: "cert-manager", Containers: web}, `name: "cert-manager" is a namespace of the cluster's own`},
		"the namespace kube-system":              {model.App{Name: "kube-system", Containers: web}, `name: "kube-system" is a namespace of the cluster's own`},
		"a name that starts with kube-":          {model.App{Name: "kube-web", Containers: web}, `name: "kube-web" is a namespace of the cluster's own`},
		"a name that starts with kube":           {model.App{Name: "kubernetes", Containers: web}, ""},
		"a chart's name of 46 characters":        {model.App{Name: long[:46], Chart: chart}, ""},
		"a chart's name of 47 characters":        {model.App{Name: long[:47], Chart: chart}, `name: "` + long[:47] + `" is longer than 46 characters, the most an app with a chart can have`},
		"a name of 47 characters without chart":  {model.App{Name: long[:47], Containers: web}, ""},
		"a container's name with a capital":      {model.App{Name: "shop", Containers: []model.Container{{Name: "Web", Image: "i"}}}, `containers: "Web": a DNS-1035 label`},
		"a container's name that starts a digit": {model.App{Name: "shop", Containers: []model.Container{{Name: "1web", Image: "i"}}}, `containers: "1web": a DNS-1035 label`},
		"a certificate's Secret named as the app's, with secrets": {
			model.App{Name: "web-tls", Containers: web, Secrets: map[string]string{"A": "1"}},
			"containers: web: host: its certificate's Secret would be named as the app's, web-tls; rename one",
		},
		"a certificate's Secret named as the app's, without secrets": {model.App{Name: "web-tls", Containers: web}, ""},
		"a container named as the app's Secret would be, without a host": {
			model.App{Name: "web-tls", Containers: []model.Container{{Name: "web", Image: "i", Port: 80}}, Secrets: map[string]string{"A": "1"}}, "",
		},
		"nothing to run":                                  {model.App{Name: "shop"}, "the app has no containers, manifests or chart"},
		"a directory without manifests":                   {model.App{Name: "shop", Manifests: noManifests}, noManifests + " has no manifests, files ending in .yaml, .yml or .json"},
		"manifests without objects":                       {model.App{Name: "shop", Manifests: noObjects}, noObjects + " has no objects"},
		"an object without metadata":                      {model.App{Name: "shop", Manifests: noMetadata}, filepath.Join(noMetadata, "a.yaml") + ": an object has no metadata"},
		"manifests that aren't YAML":                      {model.App{Name: "shop", Manifests: notYAML}, filepath.Join(notYAML, "a.yaml") + ": yaml: "},
		"manifests that aren't there":                     {model.App{Name: "shop", Manifests: filepath.Join(noObjects, "absent")}, "no such file or directory"},
		"the image named without a build":                 {model.App{Name: "shop", Manifests: withImage}, filepath.Join(withImage, "a.yaml") + " names ${IMAGE}, but the app has no build"},
		"the image named with a build":                    {model.App{Name: "shop", Manifests: withImage, Image: "example.test/shop"}, ""},
		"a chart that isn't there":                        {model.App{Name: "shop", Chart: filepath.Join(noObjects, "absent")}, "no such file or directory"},
		"a chart that links back to itself":               {model.App{Name: "shop", Chart: looping}, "links back to"},
		"an object in another namespace":                  {model.App{Name: "shop", Manifests: files(t, t.TempDir(), map[string]string{"a.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: c, namespace: other}\n"})}, "an object is in namespace other, not the app's"},
		"an object in the app's namespace":                {model.App{Name: "shop", Manifests: files(t, t.TempDir(), map[string]string{"a.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: c, namespace: shop}\n"})}, ""},
		"an object without a kind":                        {model.App{Name: "shop", Manifests: files(t, t.TempDir(), map[string]string{"a.yaml": "apiVersion: v1\nmetadata: {name: c}\n"})}, "an object has no kind"},
		"an object without an apiVersion":                 {model.App{Name: "shop", Manifests: files(t, t.TempDir(), map[string]string{"a.yaml": "kind: ConfigMap\nmetadata: {name: c}\n"})}, "an object has no apiVersion"},
		"an object without a name":                        {model.App{Name: "shop", Manifests: files(t, t.TempDir(), map[string]string{"a.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {}\n"})}, "an object has no name"},
		"a ConfigMap named as the record":                 {model.App{Name: "shop", Manifests: files(t, t.TempDir(), map[string]string{"a.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: moostack-objects}\n"})}, "the ConfigMap moostack-objects is the record of the app's objects; name it otherwise"},
		"a Secret named as the record":                    {model.App{Name: "shop", Manifests: files(t, t.TempDir(), map[string]string{"a.yaml": "apiVersion: v1\nkind: Secret\nmetadata: {name: moostack-objects}\n"})}, ""},
		"an object named with a slash":                    {model.App{Name: "shop", Manifests: files(t, t.TempDir(), map[string]string{"a.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: \"a/b\"}\n"})}, `an object's name is "a/b", which no object can have`},
		"an object named with a space":                    {model.App{Name: "shop", Manifests: files(t, t.TempDir(), map[string]string{"a.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: \"my config\"}\n"})}, ""},
		"an object named with a line break":               {model.App{Name: "shop", Manifests: files(t, t.TempDir(), map[string]string{"a.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: \"my\\nconfig\"}\n"})}, `an object's name is "my\nconfig", which no object can have`},
		"an object named with a percent sign":             {model.App{Name: "shop", Manifests: files(t, t.TempDir(), map[string]string{"a.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: \"a%b\"}\n"})}, `an object's name is "a%b", which no object can have`},
		"an object named with two dots":                   {model.App{Name: "shop", Manifests: files(t, t.TempDir(), map[string]string{"a.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: \"..\"}\n"})}, `an object's name is "..", which no object can have`},
		"a Secret named as the one of the app's secrets":  {model.App{Name: "shop", Secrets: map[string]string{"A": "1"}, Manifests: files(t, t.TempDir(), map[string]string{"a.yaml": "apiVersion: v1\nkind: Secret\nmetadata: {name: shop}\n"})}, "the Secret shop is the one of the app's secrets; name it otherwise"},
		"a Secret named as the app, which has no secrets": {model.App{Name: "shop", Manifests: files(t, t.TempDir(), map[string]string{"a.yaml": "apiVersion: v1\nkind: Secret\nmetadata: {name: shop}\n"})}, ""},
		"two containers of one name":                      {model.App{Name: "shop", Containers: []model.Container{{Name: "web", Image: "i"}, {Name: "api", Image: "i"}, {Name: "web", Image: "j"}}}, "containers: web is the name of two containers"},
		"the app's label given twice":                     {model.App{Name: "shop", Manifests: files(t, t.TempDir(), map[string]string{"a.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: c, labels: {moostack.dev/app: a, moostack.dev/app: b}}\n"})}, "an object has moostack.dev/app twice"},
		"a kind that is a number":                         {model.App{Name: "shop", Manifests: files(t, t.TempDir(), map[string]string{"a.yaml": "apiVersion: v1\nkind: 5\nmetadata: {name: c}\n"})}, "an object has no kind or no apiVersion"},
		"a kind in lower case":                            {model.App{Name: "shop", Manifests: files(t, t.TempDir(), map[string]string{"a.yaml": "apiVersion: v1\nkind: configmap\nmetadata: {name: c}\n"})}, "an object's kind is configmap, which is no kind of one object"},
		"a list":                                          {model.App{Name: "shop", Manifests: files(t, t.TempDir(), map[string]string{"a.yaml": "apiVersion: v1\nkind: ConfigMapList\nmetadata: {name: c}\n"})}, "an object's kind is ConfigMapList, which is no kind of one object"},
		"a label that is a number":                        {model.App{Name: "shop", Manifests: files(t, t.TempDir(), map[string]string{"a.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: c, labels: {tier: 1}}\n"})}, "configmap/c has a label that isn't a string; quote its value"},
		"manifests of an app named on":                    {model.App{Name: "on", Manifests: files(t, t.TempDir(), map[string]string{"a.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: c}\n"})}, ""},
		"manifests of an app named no":                    {model.App{Name: "no", Manifests: files(t, t.TempDir(), map[string]string{"a.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: c}\n"})}, ""},
		"manifests of an app named yes":                   {model.App{Name: "yes", Manifests: files(t, t.TempDir(), map[string]string{"a.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: c}\n"})}, ""},
		"manifests of an app named off":                   {model.App{Name: "off", Manifests: files(t, t.TempDir(), map[string]string{"a.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: c}\n"})}, ""},
		"manifests of an app named y":                     {model.App{Name: "y", Manifests: files(t, t.TempDir(), map[string]string{"a.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: c}\n"})}, ""},
		"manifests of an app named n":                     {model.App{Name: "n", Manifests: files(t, t.TempDir(), map[string]string{"a.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: c}\n"})}, ""},
		"manifests of an app named true":                  {model.App{Name: "true", Manifests: files(t, t.TempDir(), map[string]string{"a.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: c}\n"})}, ""},
		"manifests of an app named null":                  {model.App{Name: "null", Manifests: files(t, t.TempDir(), map[string]string{"a.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: c}\n"})}, ""},
		"an object that is a sequence":                    {model.App{Name: "shop", Manifests: files(t, t.TempDir(), map[string]string{"a.yaml": "- apiVersion: v1\n"})}, "an object isn't a mapping"},
		"metadata given twice":                            {model.App{Name: "shop", Manifests: files(t, t.TempDir(), map[string]string{"a.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: c}\nmetadata: {name: d}\n"})}, "an object has metadata twice"},
		"a label given twice":                             {model.App{Name: "shop", Manifests: files(t, t.TempDir(), map[string]string{"a.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: c, labels: {a: b}, labels: {c: d}}\n"})}, "an object has labels twice"},
		"an object whose name is a sequence":              {model.App{Name: "shop", Manifests: files(t, t.TempDir(), map[string]string{"a.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: [c]}\n"})}, "an object has no name"},
	} {
		// Check runs no command and connects to nothing.
		done := make(chan error, 1)
		go func() { done <- k3s.Through(nil, k3s.Cluster{}).Check(c.app) }()
		var err error
		select {
		case err = <-done:
		case <-time.After(10 * time.Second):
			t.Fatalf("%s: Check didn't end", name)
		}
		switch {
		case c.want == "" && err != nil:
			t.Errorf("%s: %v", name, err)
		case c.want != "" && (err == nil || !strings.Contains(err.Error(), c.want)):
			t.Errorf("%s: %v, want %q", name, err, c.want)
		}
	}
}

// packed are the paths of the files in the chart that the HelmChart of
// cluster, an app's objects, holds, in order.
func packed(t *testing.T, cluster string) []string {
	t.Helper()
	for _, o := range decode(t, cluster) {
		if o.GetKind() != "HelmChart" {
			continue
		}
		content, _, _ := unstructured.NestedString(o.Object, "spec", "chartContent")
		data, err := base64.StdEncoding.DecodeString(content)
		if err != nil {
			t.Fatal(err)
		}
		gz, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		var names []string
		for tr := tar.NewReader(gz); ; {
			h, err := tr.Next()
			if errors.Is(err, io.EOF) {
				slices.Sort(names)
				return names
			}
			if err != nil {
				t.Fatal(err)
			}
			names = append(names, h.Name)
		}
	}
	t.Fatalf("no HelmChart in:\n%s", cluster)
	return nil
}

func TestAChartIsPackedWithWhatItLinksToAndWithoutWhatItsHelmignoreNames(t *testing.T) {
	shared := filepath.Join(files(t, t.TempDir(), map[string]string{"shared.yaml": "apiVersion: v1\nkind: ConfigMap\n"}), "shared.yaml")
	linked := files(t, t.TempDir(), map[string]string{"extra.yaml": "x"})
	dir := files(t, filepath.Join(t.TempDir(), "web"), map[string]string{
		"Chart.yaml":            "apiVersion: v2\nname: web\nversion: 0.1.0\n",
		".helmignore":           "# README.md is a comment\n*.md\nsecrets/\ntemplates/skip.yaml\n\n  drafts  \n",
		"README.md":             "x",
		"secrets/key":           "x",
		"templates/a.yaml":      "apiVersion: v1\nkind: ConfigMap\n",
		"templates/notes.md":    "x",
		"templates/skip.yaml":   "x",
		"templates/secrets":     "a file, which secrets/ doesn't name",
		"drafts/a.yaml":         "x",
		".git/HEAD":             "ref",
		"templates/.git/HEAD":   "ref",
		"charts/sub/README.md":  "x",
		"charts/sub/Chart.yaml": "apiVersion: v2\nname: sub\nversion: 0.1.0\n",
	})
	if err := os.Symlink(shared, filepath.Join(dir, "templates", "shared.yaml")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(linked, filepath.Join(dir, "templates", "more")); err != nil {
		t.Fatal(err)
	}
	cluster, _, err := k3s.Objects(model.App{Name: "web", Chart: dir})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"web/.helmignore", "web/Chart.yaml", "web/charts/sub/Chart.yaml", "web/templates/a.yaml", "web/templates/more/extra.yaml", "web/templates/secrets", "web/templates/shared.yaml"}
	if got := packed(t, cluster); !slices.Equal(got, want) {
		t.Errorf("the chart is packed with\n%q, want\n%q", got, want)
	}
}

func TestAnObjectOfManifestsCarriesTheAppsLabelWhateverItSaysItself(t *testing.T) {
	dir := files(t, t.TempDir(), map[string]string{
		"a.json":        `{"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "j", "labels": {"moostack.dev/app": "other", "tier": "web"}}}`,
		"b.yaml":        "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: s, labels: web}\n---\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: l, labels: [a]}\n",
		"c.yaml/d.yml":  "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: d}\n",
		"e.txt":         "not: a manifest",
		"f.yaml.backup": "not: a manifest",
	})
	_, own, err := k3s.Objects(model.App{Name: "shop", Manifests: dir})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, o := range decode(t, own) {
		got = append(got, fmt.Sprintf("%s %v", o.GetName(), o.GetLabels()))
	}
	want := []string{"j map[moostack.dev/app:shop tier:web]", "s map[moostack.dev/app:shop]", "l map[moostack.dev/app:shop]", "d map[moostack.dev/app:shop]"}
	if !slices.Equal(got, want) {
		t.Errorf("the objects and their labels are\n%q, want\n%q", got, want)
	}
}

func TestAnEventsTimeIsTheLatestOfItsLastItsOwnAndItsSeries(t *testing.T) {
	at := func(minute int) time.Time { return time.Date(2026, 11, 23, 12, minute, 0, 0, time.UTC) }
	for name, c := range map[string]struct {
		event corev1.Event
		want  time.Time
	}{
		"none":                         {corev1.Event{}, time.Time{}},
		"a last timestamp":             {corev1.Event{FirstTimestamp: metav1.NewTime(at(1)), LastTimestamp: metav1.NewTime(at(2))}, at(2)},
		"an event time alone":          {corev1.Event{EventTime: metav1.NewMicroTime(at(3))}, at(3)},
		"an event time after the last": {corev1.Event{LastTimestamp: metav1.NewTime(at(2)), EventTime: metav1.NewMicroTime(at(3))}, at(3)},
		"a last after the event time":  {corev1.Event{LastTimestamp: metav1.NewTime(at(4)), EventTime: metav1.NewMicroTime(at(3))}, at(4)},
		"a series seen later":          {corev1.Event{EventTime: metav1.NewMicroTime(at(3)), Series: &corev1.EventSeries{LastObservedTime: metav1.NewMicroTime(at(5))}}, at(5)},
		"a series seen earlier":        {corev1.Event{EventTime: metav1.NewMicroTime(at(3)), Series: &corev1.EventSeries{LastObservedTime: metav1.NewMicroTime(at(1))}}, at(3)},
	} {
		if got := k3s.EventTime(c.event); !got.Equal(c.want) {
			t.Errorf("%s: %s, want %s", name, got, c.want)
		}
	}
}

func TestAWorkloadRollsOutUntilEachConditionOfItsKindHolds(t *testing.T) {
	two, one := int32(2), int32(1)
	rolled := appsv1.DeploymentStatus{ObservedGeneration: 3, Replicas: 2, UpdatedReplicas: 2, AvailableReplicas: 2}
	deployment := func(change func(*appsv1.Deployment)) appsv1.Deployment {
		d := appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "web", Generation: 3}, Spec: appsv1.DeploymentSpec{Replicas: &two}, Status: rolled}
		change(&d)
		return d
	}
	set := func(change func(*appsv1.StatefulSet)) appsv1.StatefulSet {
		s := appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: "db", Generation: 3},
			Spec:       appsv1.StatefulSetSpec{Replicas: &two, UpdateStrategy: appsv1.StatefulSetUpdateStrategy{Type: appsv1.RollingUpdateStatefulSetStrategyType}},
			Status:     appsv1.StatefulSetStatus{ObservedGeneration: 3, ReadyReplicas: 2, UpdatedReplicas: 2, CurrentRevision: "b", UpdateRevision: "b"},
		}
		change(&s)
		return s
	}
	daemon := func(change func(*appsv1.DaemonSet)) appsv1.DaemonSet {
		d := appsv1.DaemonSet{
			ObjectMeta: metav1.ObjectMeta{Name: "agent", Generation: 3},
			Spec:       appsv1.DaemonSetSpec{UpdateStrategy: appsv1.DaemonSetUpdateStrategy{Type: appsv1.RollingUpdateDaemonSetStrategyType}},
			Status:     appsv1.DaemonSetStatus{ObservedGeneration: 3, DesiredNumberScheduled: 2, UpdatedNumberScheduled: 2, NumberAvailable: 2},
		}
		change(&d)
		return d
	}
	for name, c := range map[string]struct {
		workload      any
		waiting, fail string
	}{
		"a Deployment rolled out": {deployment(func(*appsv1.Deployment) {}), "", ""},
		"a Deployment that wants one replica by default": {deployment(func(d *appsv1.Deployment) {
			d.Spec.Replicas, d.Status = nil, appsv1.DeploymentStatus{ObservedGeneration: 3, Replicas: 1, UpdatedReplicas: 1, AvailableReplicas: 1}
		}), "", ""},
		"a Deployment whose change isn't seen":    {deployment(func(d *appsv1.Deployment) { d.Generation = 4 }), "deployment web: its change isn't seen yet", ""},
		"a Deployment with a replica not updated": {deployment(func(d *appsv1.Deployment) { d.Status.UpdatedReplicas, d.Status.AvailableReplicas = 1, 1 }), "deployment web: 1 of 2 replicas are updated and available", ""},
		"a Deployment with a replica of before":   {deployment(func(d *appsv1.Deployment) { d.Status.Replicas = 3 }), "deployment web: 1 replicas of before are still there", ""},
		"a Deployment with one replica of two, updated": {deployment(func(d *appsv1.Deployment) {
			d.Status = appsv1.DeploymentStatus{ObservedGeneration: 3, Replicas: 1, UpdatedReplicas: 1, AvailableReplicas: 1}
		}), "deployment web: 1 of 2 replicas are updated and available", ""},
		"a Deployment that wants one replica by default and has none": {deployment(func(d *appsv1.Deployment) {
			d.Spec.Replicas, d.Status = nil, appsv1.DeploymentStatus{ObservedGeneration: 3}
		}), "deployment web: 0 of 1 replicas are updated and available", ""},
		"a StatefulSet that wants one replica by default and has none": {set(func(s *appsv1.StatefulSet) {
			s.Spec.Replicas, s.Status.ReadyReplicas = nil, 0
		}), "statefulset db: 0 of 1 replicas are ready", ""},
		"a Deployment with a replica not available": {deployment(func(d *appsv1.Deployment) { d.Status.AvailableReplicas = 1 }), "deployment web: 1 of 2 replicas are updated and available", ""},
		"a Deployment past its progress deadline": {deployment(func(d *appsv1.Deployment) {
			d.Status.Conditions = []appsv1.DeploymentCondition{{Type: appsv1.DeploymentProgressing, Reason: "ProgressDeadlineExceeded"}}
		}), "", "deployment web exceeded its progress deadline"},
		"a Deployment that progresses": {deployment(func(d *appsv1.Deployment) {
			d.Status.Conditions = []appsv1.DeploymentCondition{{Type: appsv1.DeploymentProgressing, Reason: "NewReplicaSetAvailable"}, {Type: appsv1.DeploymentAvailable, Reason: "ProgressDeadlineExceeded"}}
		}), "", ""},

		"a StatefulSet rolled out":                {set(func(*appsv1.StatefulSet) {}), "", ""},
		"a StatefulSet whose change isn't seen":   {set(func(s *appsv1.StatefulSet) { s.Generation = 4 }), "statefulset db: its change isn't seen yet", ""},
		"a StatefulSet never seen":                {set(func(s *appsv1.StatefulSet) { s.Generation, s.Status.ObservedGeneration = 0, 0 }), "statefulset db: its change isn't seen yet", ""},
		"a StatefulSet with a replica not ready":  {set(func(s *appsv1.StatefulSet) { s.Status.ReadyReplicas = 1 }), "statefulset db: 1 of 2 replicas are ready", ""},
		"a StatefulSet with a revision of before": {set(func(s *appsv1.StatefulSet) { s.Status.CurrentRevision, s.Status.UpdatedReplicas = "a", 1 }), "statefulset db: 1 replicas are updated", ""},
		"a StatefulSet updated from its partition on": {set(func(s *appsv1.StatefulSet) {
			s.Spec.UpdateStrategy.RollingUpdate = &appsv1.RollingUpdateStatefulSetStrategy{Partition: &one}
			s.Status.CurrentRevision, s.Status.UpdatedReplicas = "a", 1
		}), "", ""},
		"a StatefulSet not updated from its partition on": {set(func(s *appsv1.StatefulSet) {
			s.Spec.UpdateStrategy.RollingUpdate = &appsv1.RollingUpdateStatefulSetStrategy{Partition: &one}
			s.Status.CurrentRevision, s.Status.UpdatedReplicas = "a", 0
		}), "statefulset db: 0 of 1 replicas are updated", ""},
		"a StatefulSet that isn't rolled": {set(func(s *appsv1.StatefulSet) { s.Spec.UpdateStrategy.Type = appsv1.OnDeleteStatefulSetStrategyType }), "", "statefulset db: a rollout is waited for only with the update strategy RollingUpdate"},

		"a DaemonSet rolled out":                   {daemon(func(*appsv1.DaemonSet) {}), "", ""},
		"a DaemonSet with no node to run on":       {daemon(func(d *appsv1.DaemonSet) { d.Status = appsv1.DaemonSetStatus{ObservedGeneration: 3} }), "", ""},
		"a DaemonSet whose change isn't seen":      {daemon(func(d *appsv1.DaemonSet) { d.Generation = 4 }), "daemonset agent: its change isn't seen yet", ""},
		"a DaemonSet with a replica not updated":   {daemon(func(d *appsv1.DaemonSet) { d.Status.UpdatedNumberScheduled = 1 }), "daemonset agent: 1 of 2 replicas are updated and available", ""},
		"a DaemonSet with a replica not available": {daemon(func(d *appsv1.DaemonSet) { d.Status.NumberAvailable = 1 }), "daemonset agent: 1 of 2 replicas are updated and available", ""},
		"a DaemonSet that isn't rolled":            {daemon(func(d *appsv1.DaemonSet) { d.Spec.UpdateStrategy.Type = appsv1.OnDeleteDaemonSetStrategyType }), "", "daemonset agent: a rollout is waited for only with the update strategy RollingUpdate"},
	} {
		waiting, err := k3s.RollingOut(c.workload)
		if waiting != c.waiting || (err == nil) != (c.fail == "") || err != nil && err.Error() != c.fail {
			t.Errorf("%s: %q, %v, want %q, %q", name, waiting, err, c.waiting, c.fail)
		}
	}
}

// TestEveryNameOfAnAcceptedAppIsOneTheAPITakes generates apps with
// names of every length and character, and has the API validate, in a
// dry run, each object of those Check accepts, and a Job named as a
// chart's install job.
func TestEveryNameOfAnAcceptedAppIsOneTheAPITakes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	ns := unique("names")
	createNamespace(t, ns)
	chart := webChart(t)
	const alphabet = "abcxyz019-._AZ/"
	accepted := 0
	for seed := range uint64(300) {
		rng := rand.New(rand.NewPCG(seed, 3))
		name := func(longest int) string {
			var b strings.Builder
			// Half of the names are valid ones.
			letters := alphabet
			if rng.IntN(2) == 0 {
				letters = alphabet[:10]
			}
			for range 1 + rng.IntN(longest) {
				b.WriteByte(letters[rng.IntN(len(letters))])
			}
			return b.String()
		}
		app := model.App{Name: name(70)}
		if rng.IntN(3) == 0 {
			app.Chart = chart
		} else {
			if rng.IntN(2) == 0 {
				app.Secrets = map[string]string{"A": "1"}
			}
			for range 1 + rng.IntN(2) {
				c := model.Container{Name: name(70), Image: "example.test/i:1", Replicas: 1}
				if rng.IntN(3) > 0 {
					c.Port = 8080
					if rng.IntN(2) == 0 {
						c.Host = "a.example.com"
					}
				}
				if !containsName(app.Containers, c.Name) {
					app.Containers = append(app.Containers, c)
				}
			}
		}
		cluster, own, err := k3s.Objects(app)
		if err != nil {
			continue
		}
		accepted++
		objs := append(decode(t, cluster), decode(t, own)...)
		if app.Chart != "" {
			objs = append(objs, &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "batch/v1", "kind": "Job",
				"metadata": map[string]any{"name": "helm-install-app-" + app.Name, "namespace": "kube-system"},
				"spec": map[string]any{"template": map[string]any{"spec": map[string]any{
					"restartPolicy": "Never", "containers": []any{map[string]any{"name": "helm", "image": "example.test/helm"}},
				}}},
			}})
		}
		for _, o := range objs {
			if o.GetNamespace() == app.Name {
				o.SetNamespace(ns)
			}
			resource := map[string]schema.GroupVersionResource{
				"Namespace":  {Version: "v1", Resource: "namespaces"},
				"Secret":     {Version: "v1", Resource: "secrets"},
				"Service":    {Version: "v1", Resource: "services"},
				"Deployment": {Group: "apps", Version: "v1", Resource: "deployments"},
				"Ingress":    {Group: "networking.k8s.io", Version: "v1", Resource: "ingresses"},
				"HelmChart":  {Group: "helm.cattle.io", Version: "v1", Resource: "helmcharts"},
				"Job":        {Group: "batch", Version: "v1", Resource: "jobs"},
			}[o.GetKind()]
			var err error
			if o.GetKind() == "Namespace" {
				_, err = objects.Resource(resource).Create(ctx, o, metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}})
			} else {
				_, err = objects.Resource(resource).Namespace(o.GetNamespace()).Create(ctx, o, metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}})
			}
			if err != nil {
				t.Errorf("seed %d: Check accepted the app %q with the containers %+v, and the API refuses its %s %q: %v", seed, app.Name, app.Containers, o.GetKind(), o.GetName(), err)
			}
		}
	}
	if accepted < 30 {
		t.Errorf("Check accepted %d of the 300 apps generated, too few to say anything", accepted)
	}
}

func containsName(containers []model.Container, name string) bool {
	for _, c := range containers {
		if c.Name == name {
			return true
		}
	}
	return false
}

// decode is the objects of manifests.
func decode(t *testing.T, manifests string) []*unstructured.Unstructured {
	t.Helper()
	var objs []*unstructured.Unstructured
	dec := yamlDecoder(manifests)
	for {
		var doc map[string]any
		if err := dec.Decode(&doc); err != nil {
			if errors.Is(err, io.EOF) {
				return objs
			}
			t.Fatalf("%v in:\n%s", err, manifests)
		}
		if len(doc) > 0 {
			objs = append(objs, &unstructured.Unstructured{Object: doc})
		}
	}
}

// yamlDecoder reads the objects of manifests one by one.
func yamlDecoder(manifests string) *yaml.YAMLOrJSONDecoder {
	return yaml.NewYAMLOrJSONDecoder(strings.NewReader(manifests), 4096)
}

// resourceKi is a quantity of n kibibytes.
func resourceKi(n int64) resource.Quantity {
	return *resource.NewQuantity(n*1024, resource.BinarySI)
}

// FuzzTheManifestsOfAnApp gives the reader of an app's manifests any
// file. Whatever it takes, the decoder the objects are applied with
// reads too, as objects that each carry the app's label, have a name
// the record can hold, and aren't the record or the app's own Secret.
func FuzzTheManifestsOfAnApp(f *testing.F) {
	for _, seed := range []string{
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: c}\n",
		"---\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: c, labels: {tier: web}}\ndata:\n  date: 2024-01-02\n  octal: 0755\n---\n---\n{\"apiVersion\": \"v1\", \"kind\": \"Secret\", \"metadata\": {\"name\": \"s\", \"labels\": null}}\n",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: x, <<: {namespace: kube-system, labels: {moostack.dev/app: other}}}\n",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: &m {name: c}\n---\napiVersion: v1\nkind: Secret\nmetadata: *m\n",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: c}\nmetadata: {name: d}\n",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: \"a/b\"}\n",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: moostack-objects}\n",
		"apiVersion: v1\nkind: Secret\nmetadata: {name: shop}\n",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: c, labels: {moostack.dev/app: a, moostack.dev/app: b}}\n",
		"apiVersion: v1\nkind: 5\nmetadata: {name: c}\n",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: c, labels: {tier: 1}}\n",
		"- a\n", "a: [\n", "", "\xff\xfe", "apiVersion: v1\nkind: List\nitems: []\nmetadata: {name: l, labels: [a]}\n",
		"? [a]\n: b\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: c}\n",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, manifest string) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "a.yaml"), []byte(manifest), 0o644); err != nil {
			t.Fatal(err)
		}
		_, own, err := k3s.Objects(model.App{Name: "shop", Manifests: dir, Secrets: map[string]string{"A": "1"}})
		if err != nil {
			return
		}
		dec := yamlDecoder(own)
		objs := 0
		for {
			var doc map[string]any
			if err := dec.Decode(&doc); errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				t.Fatalf("the manifests were taken, and what is applied can't be read: %v\n%s", err, own)
			}
			if len(doc) == 0 {
				continue
			}
			objs++
			o := &unstructured.Unstructured{Object: doc}
			if objs == 1 {
				// The first object is the Secret of the app's secrets.
				continue
			}
			if o.GetLabels()["moostack.dev/app"] != "shop" {
				t.Errorf("an object taken doesn't carry the app's label as it is applied: %v\n%s", o.GetLabels(), own)
			}
			name := o.GetName()
			if name == "" || strings.ContainsAny(name, "\n\r/%") || name == "." || name == ".." {
				t.Errorf("an object taken is named %q as it is applied\n%s", name, own)
			}
			if o.GetKind() == "ConfigMap" && name == "moostack-objects" || o.GetKind() == "Secret" && name == "shop" {
				t.Errorf("an object taken is the %s %s as it is applied\n%s", o.GetKind(), name, own)
			}
		}
		if objs < 2 {
			t.Errorf("manifests without an object were taken:\n%s", own)
		}
	})
}
