// Package objects makes the Kubernetes objects of an app: its namespace
// and Secret, and the objects of a deploy file's containers, manifests
// or chart.
package objects

import (
	"archive/tar"
	"bytes"
	"cmp"
	"compress/gzip"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/moostackhq/moostack/internal/deployfile"
)

// AppLabel is the label that marks an app's namespace and objects with
// its name.
const AppLabel = "moostack.dev/app"

// ChartNamespace is where an app's HelmChart goes, for k3s's Helm
// controller.
const ChartNamespace = "kube-system"

// SecretsAnnotation is the pod annotation that holds the hash of the
// app's secrets.
const SecretsAnnotation = "moostack.dev/secrets"

// ContainerLabel is the label that marks a container's objects and pods
// with its name.
const ContainerLabel = "moostack.dev/container"

// ImageVar is what manifests name the built image with.
const ImageVar = "${IMAGE}"

// App is an app to make the objects of.
type App struct {
	*deployfile.App
	// Image is the reference of the image built for the app's manifests
	// or chart, empty if none is.
	Image string
	// Images are the references of the images built for the app's
	// containers, by container name.
	Images map[string]string
	// Secrets are the values of the app's secrets, by name.
	Secrets map[string]string
	// Issuer is the ClusterIssuer of the ingress's certificate.
	Issuer string
}

// Objects are an app's Kubernetes objects: Cluster, its namespace and
// its HelmChart, each with its namespace, and Namespaced, the objects to
// apply in the app's namespace: its Secret, and its containers' objects
// or its own manifests, each labelled with AppLabel.
type Objects struct {
	Cluster, Namespaced string
}

// Make is the app's objects.
func Make(a App) (Objects, error) {
	var cluster, namespaced strings.Builder
	writeDoc(&cluster, a.namespace())
	if len(a.App.Secrets) > 0 {
		writeDoc(&namespaced, a.secret())
	}
	switch {
	case len(a.Containers) > 0:
		for _, c := range a.Containers {
			for _, d := range a.container(c) {
				writeDoc(&namespaced, d)
			}
		}
	case a.Manifests != "":
		m, err := readManifests(a.Manifests, a.Image, a.labels())
		if err != nil {
			return Objects{}, err
		}
		namespaced.WriteString(m)
	default:
		c, err := a.chart(a.Chart)
		if err != nil {
			return Objects{}, err
		}
		writeDoc(&cluster, c)
	}
	return Objects{Cluster: cluster.String(), Namespaced: namespaced.String()}, nil
}

// Check fails if the app's objects can't be made: manifests that can't
// be read, or a chart that can't be packed.
func Check(app *deployfile.App) error {
	image := ""
	if app.Build != nil {
		image = "check"
	}
	images := map[string]string{}
	for _, c := range app.Containers {
		if c.Build != nil {
			images[c.Name] = "check"
		}
	}
	_, err := Make(App{App: app, Image: image, Images: images})
	return err
}

// TLSSecret is the name of the Secret, and of the Certificate, of the
// certificate of the container called name.
func TLSSecret(name string) string { return name + "-tls" }

// ChartName is the name of the HelmChart of the app called app.
func ChartName(app string) string { return "app-" + app }

func writeDoc(b *strings.Builder, v any) {
	data, err := yaml.Marshal(v)
	if err != nil {
		panic(err)
	}
	b.WriteString("---\n")
	b.Write(data)
}

func (a App) labels() map[string]string { return map[string]string{AppLabel: a.Name} }

func (a App) meta(name string) map[string]any {
	return map[string]any{"name": name, "namespace": a.Name, "labels": a.labels()}
}

func (a App) namespace() any {
	return map[string]any{
		"apiVersion": "v1",
		"kind":       "Namespace",
		"metadata":   map[string]any{"name": a.Name, "labels": a.labels()},
	}
}

func (a App) secret() any {
	data := map[string]string{}
	for _, name := range a.App.Secrets {
		data[name] = base64.StdEncoding.EncodeToString([]byte(a.Secrets[name]))
	}
	return map[string]any{
		"apiVersion": "v1",
		"kind":       "Secret",
		"metadata":   a.meta(a.Name),
		"type":       "Opaque",
		"data":       data,
	}
}

// secretsHash is the SHA-256 of the app's secrets, names and values.
func (a App) secretsHash() string {
	h := sha256.New()
	for _, name := range slices.Sorted(slices.Values(a.App.Secrets)) {
		fmt.Fprintf(h, "%s=%q\n", name, a.Secrets[name])
	}
	return hex.EncodeToString(h.Sum(nil))
}

// container is the objects of the container c: its Deployment, its
// Service if it has a port, and its Ingress if it has a host.
func (a App) container(c deployfile.Container) []any {
	labels := a.labels()
	labels[ContainerLabel] = c.Name
	meta := func() map[string]any {
		return map[string]any{"name": c.Name, "namespace": a.Name, "labels": labels}
	}
	container := map[string]any{"name": c.Name, "image": cmp.Or(c.Image, a.Images[c.Name])}
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
	if len(a.App.Secrets) > 0 {
		container["envFrom"] = []any{map[string]any{"secretRef": map[string]any{"name": a.Name}}}
		podMeta["annotations"] = map[string]string{SecretsAnnotation: a.secretsHash()}
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
	ingress["annotations"] = map[string]string{"cert-manager.io/cluster-issuer": a.Issuer}
	return append(docs, map[string]any{
		"apiVersion": "networking.k8s.io/v1",
		"kind":       "Ingress",
		"metadata":   ingress,
		"spec": map[string]any{
			"ingressClassName": "traefik",
			"tls":              []any{map[string]any{"hosts": []string{c.Host}, "secretName": TLSSecret(c.Name)}},
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

// readManifests are the manifests in dir and its subdirectories, in the
// order of their paths, with ImageVar replaced by image.
func readManifests(dir, image string, labels map[string]string) (string, error) {
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
		if strings.Contains(text, ImageVar) && image == "" {
			return "", fmt.Errorf("%s names %s, but the app has no build", p, ImageVar)
		}
		text = strings.ReplaceAll(text, ImageVar, image)
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
			if err := label(&doc, labels); err != nil {
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

// label adds labels to the metadata of doc, a YAML document, and keeps
// every other value as it was written.
func label(doc *yaml.Node, labels map[string]string) error {
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
	if name := value(meta, "name"); name == nil || name.Kind != yaml.ScalarNode || name.Value == "" {
		return fmt.Errorf("an object has no name")
	}
	if ns := value(meta, "namespace"); ns != nil && ns.Value != "" && ns.Value != labels[AppLabel] {
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
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	for _, k := range keys {
		v := &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: labels[k]}
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

// chart is the HelmChart that installs the chart in dir into the app's
// namespace, with the built image as its value image.
func (a App) chart(dir string) (any, error) {
	content, err := packChart(dir)
	if err != nil {
		return nil, err
	}
	spec := map[string]any{
		"chartContent":    content,
		"targetNamespace": a.Name,
	}
	if a.Image != "" {
		spec["valuesContent"] = "image: " + a.Image + "\n"
	}
	return map[string]any{
		"apiVersion": "helm.cattle.io/v1",
		"kind":       "HelmChart",
		"metadata":   map[string]any{"name": ChartName(a.Name), "namespace": ChartNamespace, "labels": a.labels()},
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
		if ok, _ := path.Match(p, rel); ok {
			return true
		}
		if ok, _ := path.Match(p, path.Base(rel)); ok {
			return true
		}
	}
	return false
}
