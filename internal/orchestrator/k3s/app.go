package k3s

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	stdpath "path"
	"path/filepath"
	"slices"
	"strings"
	"unicode"

	"gopkg.in/yaml.v3"
	"k8s.io/apimachinery/pkg/api/validation/path"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/validation"

	"github.com/moostackhq/moostack/internal/model"
)

const (
	// appLabel is the label that marks what is an app's with the app's
	// name.
	appLabel = "moostack.dev/app"
	// containerLabel is the label that marks a container's objects and
	// pods with its name.
	containerLabel = "moostack.dev/container"
	// secretsAnnotation is the pod annotation that holds the hash of the
	// app's secrets.
	secretsAnnotation = "moostack.dev/secrets"
	// imageVar is what manifests name the built image with.
	imageVar = "${IMAGE}"
	// chartNamespace is where an app's HelmChart goes, for k3s's Helm
	// controller.
	chartNamespace = "kube-system"
	// recordName is the ConfigMap in an app's namespace that lists the
	// objects its last deploy applied there.
	recordName = "moostack-objects"
	// maxChartApp is the longest name of an app with a chart, whose
	// install job's name is at most 63 characters.
	maxChartApp = 63 - len("helm-install-app-")
)

// ownNamespaces are the cluster's own namespaces that don't start with
// kube-.
var ownNamespaces = []string{"default", "cert-manager"}

func namespaceOf(app string) string { return app }

// secretOf is the name of the Secret that holds the secrets of the app
// called app.
func secretOf(app string) string { return app }

// tlsSecret is the name of the Secret, and of the Certificate, of the
// certificate of the container called container.
func tlsSecret(container string) string { return container + "-tls" }

// chartName is the name of the HelmChart of the app called app.
func chartName(app string) string { return "app-" + app }

// chartJob is the name of the job that installs the chart of the app
// called app.
func chartJob(app string) string { return "helm-install-" + chartName(app) }

// Check refuses an app without a name or with a name that isn't a DNS
// label, one named as a namespace of the cluster's own, one with a
// chart whose name is longer than maxChartApp, a container whose name
// isn't a DNS label that starts with a letter, two containers of one
// name, a container whose
// certificate's Secret would be named as the app's, and an app whose
// manifests can't be read or checkApplied refuses, one whose chart
// can't be packed, and one with no containers, manifests or chart.
func (b *Backend) Check(app model.App) error {
	_, err := objectsOf(app)
	return err
}

func checkNames(app model.App) error {
	if app.Name == "" {
		return errors.New("the app has no name")
	}
	if problems := validation.IsDNS1123Label(app.Name); len(problems) > 0 {
		return fmt.Errorf("name: %q: %s", app.Name, strings.Join(problems, "; "))
	}
	if strings.HasPrefix(app.Name, "kube-") || slices.Contains(ownNamespaces, app.Name) {
		return fmt.Errorf("name: %q is a namespace of the cluster's own", app.Name)
	}
	if app.Chart != "" && len(app.Name) > maxChartApp {
		return fmt.Errorf("name: %q is longer than %d characters, the most an app with a chart can have", app.Name, maxChartApp)
	}
	for i, c := range app.Containers {
		if slices.ContainsFunc(app.Containers[:i], func(other model.Container) bool { return other.Name == c.Name }) {
			return fmt.Errorf("containers: %s is the name of two containers", c.Name)
		}
		if problems := validation.IsDNS1035Label(c.Name); len(problems) > 0 {
			return fmt.Errorf("containers: %q: %s", c.Name, strings.Join(problems, "; "))
		}
		if c.Host != "" && tlsSecret(c.Name) == secretOf(app.Name) && len(app.Secrets) > 0 {
			return fmt.Errorf("containers: %s: host: its certificate's Secret would be named as the app's, %s; rename one", c.Name, app.Name)
		}
	}
	return nil
}

// objects are an app's objects, as YAML documents: cluster holds its
// namespace and its HelmChart, each with its namespace, and own the
// objects of the app's namespace: its Secret, and its containers'
// objects or its own manifests, each labelled with appLabel.
type objects struct {
	cluster, own string
}

// objectsOf is the objects of app, which it refuses as Check does.
func objectsOf(app model.App) (objects, error) {
	if err := checkNames(app); err != nil {
		return objects{}, err
	}
	var cluster, own strings.Builder
	writeDoc(&cluster, map[string]any{
		"apiVersion": "v1",
		"kind":       "Namespace",
		"metadata":   map[string]any{"name": namespaceOf(app.Name), "labels": labels(app)},
	})
	if len(app.Secrets) > 0 {
		writeDoc(&own, secret(app))
	}
	switch {
	case len(app.Containers) > 0:
		for _, c := range app.Containers {
			for _, d := range containerObjects(app, c) {
				writeDoc(&own, d)
			}
		}
	case app.Manifests != "":
		m, err := readManifests(app)
		if err != nil {
			return objects{}, err
		}
		if err := checkApplied(app, m); err != nil {
			return objects{}, fmt.Errorf("%s: %w", app.Manifests, err)
		}
		own.WriteString(m)
	case app.Chart != "":
		c, err := chart(app)
		if err != nil {
			return objects{}, err
		}
		writeDoc(&cluster, c)
	default:
		return objects{}, errors.New("the app has no containers, manifests or chart")
	}
	return objects{cluster: cluster.String(), own: own.String()}, nil
}

func writeDoc(b *strings.Builder, v any) {
	data, err := yaml.Marshal(v)
	if err != nil {
		panic(err)
	}
	b.WriteString("---\n")
	b.Write(data)
}

func labels(app model.App) map[string]string { return map[string]string{appLabel: app.Name} }

func secret(app model.App) any {
	data := map[string]string{}
	for name, value := range app.Secrets {
		data[name] = base64.StdEncoding.EncodeToString([]byte(value))
	}
	return map[string]any{
		"apiVersion": "v1",
		"kind":       "Secret",
		"metadata":   map[string]any{"name": secretOf(app.Name), "namespace": namespaceOf(app.Name), "labels": labels(app)},
		"type":       "Opaque",
		"data":       data,
	}
}

// secretsHash is the SHA-256 of the app's secrets, names and values.
func secretsHash(app model.App) string {
	h := sha256.New()
	for _, name := range slices.Sorted(maps.Keys(app.Secrets)) {
		fmt.Fprintf(h, "%s=%q\n", name, app.Secrets[name])
	}
	return hex.EncodeToString(h.Sum(nil))
}

// containerObjects are the objects of the container c: its Deployment,
// its Service if it has a port, and its Ingress if it has a host.
func containerObjects(app model.App, c model.Container) []any {
	labels := labels(app)
	labels[containerLabel] = c.Name
	meta := func() map[string]any {
		return map[string]any{"name": c.Name, "namespace": namespaceOf(app.Name), "labels": labels}
	}
	container := map[string]any{"name": c.Name, "image": c.Image}
	if len(c.Command) > 0 {
		container["command"] = c.Command
	}
	if c.Port != 0 {
		container["ports"] = []any{map[string]any{"containerPort": c.Port}}
		container["readinessProbe"] = map[string]any{
			"tcpSocket":     map[string]any{"port": c.Port},
			"periodSeconds": 2,
		}
	}
	podMeta := map[string]any{"labels": labels}
	if len(app.Secrets) > 0 {
		container["envFrom"] = []any{map[string]any{"secretRef": map[string]any{"name": secretOf(app.Name)}}}
		podMeta["annotations"] = map[string]string{secretsAnnotation: secretsHash(app)}
	}
	docs := []any{map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata":   meta(),
		"spec": map[string]any{
			"replicas": c.Replicas,
			"selector": map[string]any{"matchLabels": labels},
			"strategy": map[string]any{"rollingUpdate": map[string]any{"maxUnavailable": 0, "maxSurge": 1}},
			"template": map[string]any{
				"metadata": podMeta,
				"spec":     map[string]any{"containers": []any{container}},
			},
		},
	}}
	if c.Port == 0 {
		return docs
	}
	docs = append(docs, map[string]any{
		"apiVersion": "v1",
		"kind":       "Service",
		"metadata":   meta(),
		"spec": map[string]any{
			"selector": labels,
			"ports":    []any{map[string]any{"port": c.Port, "targetPort": c.Port}},
		},
	})
	if c.Host == "" {
		return docs
	}
	ingress := meta()
	ingress["annotations"] = map[string]string{"cert-manager.io/cluster-issuer": issuer}
	return append(docs, map[string]any{
		"apiVersion": "networking.k8s.io/v1",
		"kind":       "Ingress",
		"metadata":   ingress,
		"spec": map[string]any{
			"ingressClassName": "traefik",
			"tls":              []any{map[string]any{"hosts": []string{c.Host}, "secretName": tlsSecret(c.Name)}},
			"rules": []any{map[string]any{
				"host": c.Host,
				"http": map[string]any{"paths": []any{map[string]any{
					"path":     "/",
					"pathType": "Prefix",
					"backend":  map[string]any{"service": map[string]any{"name": c.Name, "port": map[string]any{"number": c.Port}}},
				}}},
			}},
		},
	})
}

// readManifests are the manifests in app's directory of them and its
// subdirectories, in the order of their paths, with imageVar replaced
// by the app's image and each object labelled as the app's.
func readManifests(app model.App) (string, error) {
	dir := app.Manifests
	var paths []string
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		switch filepath.Ext(p) {
		case ".yaml", ".yml", ".json":
			if !d.IsDir() {
				paths = append(paths, p)
			}
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	if len(paths) == 0 {
		return "", fmt.Errorf("%s has no manifests, files ending in .yaml, .yml or .json", dir)
	}
	slices.Sort(paths)
	var b strings.Builder
	for _, p := range paths {
		data, err := os.ReadFile(p)
		if err != nil {
			return "", err
		}
		text := string(data)
		if strings.Contains(text, imageVar) && app.Image == "" {
			return "", fmt.Errorf("%s names %s, but the app has no build", p, imageVar)
		}
		text = strings.ReplaceAll(text, imageVar, app.Image)
		dec := yaml.NewDecoder(strings.NewReader(text))
		for {
			var doc yaml.Node
			err := dec.Decode(&doc)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return "", fmt.Errorf("%s: %w", p, err)
			}
			if len(doc.Content) == 0 || doc.Content[0].Kind == yaml.ScalarNode && doc.Content[0].Tag == "!!null" {
				continue
			}
			if err := label(&doc, app); err != nil {
				return "", fmt.Errorf("%s: %w", p, err)
			}
			writeDoc(&b, &doc)
		}
	}
	if b.Len() == 0 {
		return "", fmt.Errorf("%s has no objects", dir)
	}
	return b.String(), nil
}

// checkApplied refuses manifests of app that can't be read as they are
// applied, and an object of them that, read so, has no kind or
// apiVersion, a kind that doesn't start with a capital or ends in List,
// a label that isn't a string, no name, or one with a line break, a
// slash or a percent sign, or . or .., a ConfigMap named recordName,
// and, for an app with secrets, a Secret named as the one of them.
func checkApplied(app model.App, manifests string) error {
	objs, err := parse(manifests)
	if err != nil {
		return err
	}
	for _, o := range objs {
		kind, name := o.GetKind(), o.GetName()
		_, _, err := unstructured.NestedStringMap(o.Object, "metadata", "labels")
		switch {
		case kind == "" || o.GetAPIVersion() == "":
			return errors.New("an object has no kind or no apiVersion")
		case !unicode.IsUpper(rune(kind[0])) || strings.HasSuffix(kind, "List"):
			return fmt.Errorf("an object's kind is %s, which is no kind of one object", kind)
		case err != nil:
			return fmt.Errorf("%s has a label that isn't a string; quote its value", kindAndName(o))
		case name == "":
			return errors.New("an object has no name")
		case strings.ContainsAny(name, "\n\r") || len(path.IsValidPathSegmentName(name)) > 0:
			return fmt.Errorf("an object's name is %q, which no object can have", name)
		case kind == "ConfigMap" && name == recordName:
			return fmt.Errorf("the ConfigMap %s is the record of the app's objects; name it otherwise", recordName)
		case kind == "Secret" && name == secretOf(app.Name) && len(app.Secrets) > 0:
			return fmt.Errorf("the Secret %s is the one of the app's secrets; name it otherwise", name)
		}
	}
	return nil
}

// label adds app's labels to the metadata of doc, a YAML document, and
// keeps every other value as it was written. It refuses an object
// without an apiVersion, a kind or a name, one in a namespace other
// than the app's, and one with a key of its own, of its metadata or of
// its labels given twice.
func label(doc *yaml.Node, app model.App) error {
	obj := doc.Content[0]
	if obj.Kind != yaml.MappingNode {
		return fmt.Errorf("an object isn't a mapping")
	}
	for _, key := range []string{"apiVersion", "kind"} {
		if v := value(obj, key); v == nil || v.Kind != yaml.ScalarNode || v.Value == "" {
			return fmt.Errorf("an object has no %s", key)
		}
	}
	meta := value(obj, "metadata")
	if meta == nil || meta.Kind != yaml.MappingNode {
		return fmt.Errorf("an object has no metadata")
	}
	for _, m := range []*yaml.Node{obj, meta, value(meta, "labels")} {
		if m == nil || m.Kind != yaml.MappingNode {
			continue
		}
		seen := map[string]bool{}
		for i := 0; i+1 < len(m.Content); i += 2 {
			key := m.Content[i].Value
			if seen[key] {
				return fmt.Errorf("an object has %s twice", key)
			}
			seen[key] = true
		}
	}
	if name := value(meta, "name"); name == nil || name.Kind != yaml.ScalarNode || name.Value == "" {
		return fmt.Errorf("an object has no name")
	}
	if ns := value(meta, "namespace"); ns != nil && ns.Value != "" && ns.Value != namespaceOf(app.Name) {
		return fmt.Errorf("an object is in namespace %s, not the app's", ns.Value)
	}
	l := value(meta, "labels")
	if l == nil || l.Kind != yaml.MappingNode || l.Tag == "!!null" {
		fresh := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
		if l == nil {
			meta.Content = append(meta.Content, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: "labels"}, fresh)
		} else {
			*l = *fresh
		}
		l = value(meta, "labels")
	}
	labels := labels(app)
	for _, k := range slices.Sorted(maps.Keys(labels)) {
		v := &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Style: yaml.DoubleQuotedStyle, Value: labels[k]}
		if existing := value(l, k); existing != nil {
			*existing = *v
			continue
		}
		l.Content = append(l.Content, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: k}, v)
	}
	return nil
}

// value is the value of key in the mapping m, or nil.
func value(m *yaml.Node, key string) *yaml.Node {
	for i := 0; i+1 < len(m.Content); i += 2 {
		if m.Content[i].Value == key {
			return m.Content[i+1]
		}
	}
	return nil
}

// chart is the HelmChart that installs app's chart into the app's
// namespace, with the app's image as its value image.
func chart(app model.App) (any, error) {
	content, err := packChart(app.Chart)
	if err != nil {
		return nil, err
	}
	spec := map[string]any{
		"chartContent":    content,
		"targetNamespace": namespaceOf(app.Name),
	}
	if app.Image != "" {
		spec["valuesContent"] = "image: " + app.Image + "\n"
	}
	return map[string]any{
		"apiVersion": "helm.cattle.io/v1",
		"kind":       "HelmChart",
		"metadata":   map[string]any{"name": chartName(app.Name), "namespace": chartNamespace, "labels": labels(app)},
		"spec":       spec,
	}, nil
}

// packChart is the chart in dir as Helm packs it, a gzipped tar with the
// chart in a directory named as dir is, in base64. It follows symlinks,
// and leaves out .git and what the chart's .helmignore names.
func packChart(dir string) (string, error) {
	ignore, err := readHelmignore(dir)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	top := filepath.Base(dir)
	var add func(path, rel string, parents []string) error
	add = func(path, rel string, parents []string) error {
		info, err := os.Stat(path)
		if err != nil {
			return err
		}
		if rel != "." && (filepath.Base(rel) == ".git" || ignore.matches(rel, info.IsDir())) {
			return nil
		}
		if info.IsDir() {
			real, err := filepath.EvalSymlinks(path)
			if err != nil {
				return err
			}
			if slices.Contains(parents, real) {
				return fmt.Errorf("%s links back to %s", filepath.Join(dir, rel), real)
			}
			parents = append(parents, real)
			entries, err := os.ReadDir(path)
			if err != nil {
				return err
			}
			for _, e := range entries {
				if err := add(filepath.Join(path, e.Name()), filepath.Join(rel, e.Name()), parents); err != nil {
					return err
				}
			}
			return nil
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if err := tw.WriteHeader(&tar.Header{Name: filepath.ToSlash(filepath.Join(top, rel)), Mode: 0o644, Size: int64(len(data)), Typeflag: tar.TypeReg}); err != nil {
			return err
		}
		_, err = tw.Write(data)
		return err
	}
	if err := add(dir, ".", nil); err != nil {
		return "", err
	}
	if err := tw.Close(); err != nil {
		return "", err
	}
	if err := gz.Close(); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(buf.Bytes()), nil
}

// helmignore are the patterns of a chart's .helmignore.
type helmignore []string

// readHelmignore reads dir's .helmignore, if it has one.
func readHelmignore(dir string) (helmignore, error) {
	data, err := os.ReadFile(filepath.Join(dir, ".helmignore"))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var h helmignore
	for _, line := range strings.Split(string(data), "\n") {
		if line = strings.TrimSpace(line); line != "" && !strings.HasPrefix(line, "#") {
			h = append(h, line)
		}
	}
	return h, nil
}

// matches reports whether a pattern matches the file at rel, a
// directory if dir: a pattern ending in / matches directories only, and
// a pattern matches the whole path or its last element.
func (h helmignore) matches(rel string, dir bool) bool {
	rel = filepath.ToSlash(rel)
	for _, p := range h {
		if strings.HasSuffix(p, "/") {
			if !dir {
				continue
			}
			p = strings.TrimSuffix(p, "/")
		}
		if ok, _ := stdpath.Match(p, rel); ok {
			return true
		}
		if ok, _ := stdpath.Match(p, stdpath.Base(rel)); ok {
			return true
		}
	}
	return false
}
