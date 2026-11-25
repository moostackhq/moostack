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
	"regexp"
	"slices"
	"strings"
	"unicode"

	"gopkg.in/yaml.v3"
	"k8s.io/apimachinery/pkg/api/validation/path"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/validation"

	"github.com/moostackhq/moostack/internal/model"
)

// imageVar is what manifests name a built image with: ${image:web} is
// the image built under the name web.
var imageVar = regexp.MustCompile(`\$\{image:([^}]*)\}`)

const (
	// claimKind is how the record names a PersistentVolumeClaim.
	claimKind = "PersistentVolumeClaim"
	// appLabel is the label that marks what is an app's with the app's
	// name.
	appLabel = "moostack.dev/app"
	// containerLabel is the label that marks a container's objects and
	// pods with its name.
	containerLabel = "moostack.dev/container"
	// secretsAnnotation is the pod annotation that holds the hash of the
	// secrets its container reads.
	secretsAnnotation = "moostack.dev/secrets"
	// preStart is the name of the init container that runs a
	// container's command before the start.
	preStart = "pre-start"
	// cronLabel is the label that marks the pods of a container's
	// scheduled command with the command's name.
	cronLabel = "moostack.dev/cron"
	// volumeRequest is the storage a volume's claim asks for.
	volumeRequest = "1Gi"
	// gracePeriod is the seconds a replica has to stop in, after its
	// stop delay.
	gracePeriod = 30
	// maxCronJob is the longest name a CronJob can have.
	maxCronJob = 52
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

// claimOf is the name of the PersistentVolumeClaim of the volume
// called volume of the container called container.
func claimOf(container, volume string) string { return container + "-" + volume }

// cronJobOf is the name of the CronJob of the scheduled command called
// cron of the container called container.
func cronJobOf(container, cron string) string { return container + "-" + cron }

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
// certificate's Secret would be named as the app's, a container with
// an environment value that isn't a variable's name or that names a
// secret the app doesn't have, a volume or a scheduled command whose
// name isn't a DNS label that starts with a letter, two whose objects
// would be named alike, a container named preStart that has a command
// before the start, a scheduled command whose CronJob's name would
// be longer than maxCronJob, and an app whose manifests can't be read,
// name an image the app doesn't have or checkApplied refuses, one whose
// chart can't be packed, and one with no containers, manifests or chart.
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
	var claims, cronJobs []string
	for i, c := range app.Containers {
		for _, v := range c.Volumes {
			if problems := validation.IsDNS1035Label(v.Name); len(problems) > 0 {
				return fmt.Errorf("containers: %s: volumes: %q: %s", c.Name, v.Name, strings.Join(problems, "; "))
			}
			if slices.Contains(claims, claimOf(c.Name, v.Name)) {
				return fmt.Errorf("containers: %s: volumes: %s: another volume is %s on the cluster too; rename one", c.Name, v.Name, claimOf(c.Name, v.Name))
			}
			claims = append(claims, claimOf(c.Name, v.Name))
		}
		for _, cron := range c.Cron {
			name := cronJobOf(c.Name, cron.Name)
			if problems := validation.IsDNS1035Label(cron.Name); len(problems) > 0 {
				return fmt.Errorf("containers: %s: cron: %q: %s", c.Name, cron.Name, strings.Join(problems, "; "))
			}
			if len(name) > maxCronJob {
				return fmt.Errorf("containers: %s: cron: %s: %s is longer than %d characters; shorten a name", c.Name, cron.Name, name, maxCronJob)
			}
			if slices.Contains(cronJobs, name) {
				return fmt.Errorf("containers: %s: cron: %s: another scheduled command is %s on the cluster too; rename one", c.Name, cron.Name, name)
			}
			cronJobs = append(cronJobs, name)
		}
		if c.PreStart != nil && c.Name == preStart {
			return fmt.Errorf("containers: %s: its command before the start is named so on the cluster; name the container otherwise", c.Name)
		}
		if slices.ContainsFunc(app.Containers[:i], func(other model.Container) bool { return other.Name == c.Name }) {
			return fmt.Errorf("containers: %s is the name of two containers", c.Name)
		}
		if problems := validation.IsDNS1035Label(c.Name); len(problems) > 0 {
			return fmt.Errorf("containers: %q: %s", c.Name, strings.Join(problems, "; "))
		}
		if c.Host != "" && tlsSecret(c.Name) == secretOf(app.Name) && len(app.Secrets) > 0 {
			return fmt.Errorf("containers: %s: host: its certificate's Secret would be named as the app's, %s; rename one", c.Name, app.Name)
		}
		for variable, v := range c.Env {
			if problems := validation.IsEnvVarName(variable); len(problems) > 0 {
				return fmt.Errorf("containers: %s: env: %q: %s", c.Name, variable, strings.Join(problems, "; "))
			}
			if _, ok := app.Secrets[v.Secret]; v.Secret != "" && !ok {
				return fmt.Errorf("containers: %s: env: %s: the app has no secret %s", c.Name, variable, v.Secret)
			}
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

// secretsHash is the SHA-256 of the secrets of app that c reads, names
// and values, and empty if c reads none.
func secretsHash(app model.App, c model.Container) string {
	var names []string
	for _, v := range c.Env {
		if v.Secret != "" && !slices.Contains(names, v.Secret) {
			names = append(names, v.Secret)
		}
	}
	if len(names) == 0 {
		return ""
	}
	slices.Sort(names)
	h := sha256.New()
	for _, name := range names {
		fmt.Fprintf(h, "%s=%q\n", name, app.Secrets[name])
	}
	return hex.EncodeToString(h.Sum(nil))
}

// env is c's environment values in the order of their variables, each
// secret read from the app's Secret.
func env(app model.App, c model.Container) []any {
	var out []any
	for _, variable := range slices.Sorted(maps.Keys(c.Env)) {
		v := c.Env[variable]
		if v.Secret == "" {
			out = append(out, map[string]any{"name": variable, "value": v.Value})
			continue
		}
		out = append(out, map[string]any{"name": variable, "valueFrom": map[string]any{
			"secretKeyRef": map[string]any{"name": secretOf(app.Name), "key": v.Secret},
		}})
	}
	return out
}

// containerObjects are the objects of the container c: the
// PersistentVolumeClaims of its volumes, its Deployment, the CronJobs
// of its scheduled commands, its Service if it has a port, and its
// Ingress if it has a host.
func containerObjects(app model.App, c model.Container) []any {
	labels := labels(app)
	labels[containerLabel] = c.Name
	named := func(name string) map[string]any {
		return map[string]any{"name": name, "namespace": namespaceOf(app.Name), "labels": labels}
	}
	meta := func() map[string]any { return named(c.Name) }
	var docs, mounts, volumes []any
	for _, v := range c.Volumes {
		docs = append(docs, map[string]any{
			"apiVersion": "v1",
			"kind":       "PersistentVolumeClaim",
			"metadata":   named(claimOf(c.Name, v.Name)),
			"spec": map[string]any{
				"accessModes": []string{"ReadWriteOnce"},
				"resources":   map[string]any{"requests": map[string]any{"storage": volumeRequest}},
			},
		})
		mounts = append(mounts, map[string]any{"name": v.Name, "mountPath": v.Path})
		volumes = append(volumes, map[string]any{"name": v.Name, "persistentVolumeClaim": map[string]any{"claimName": claimOf(c.Name, v.Name)}})
	}
	// runs is a container of c's image, with its environment values and
	// its volumes.
	runs := func(name string) map[string]any {
		out := map[string]any{"name": name, "image": c.Image}
		if e := env(app, c); len(e) > 0 {
			out["env"] = e
		}
		if len(mounts) > 0 {
			out["volumeMounts"] = mounts
		}
		return out
	}
	// command is a container that runs run.
	command := func(name string, run model.Command) map[string]any {
		out := runs(name)
		out["command"] = run.Command
		if run.User != nil {
			user := map[string]any{"runAsUser": run.User.ID}
			if run.User.Group != nil {
				user["runAsGroup"] = *run.User.Group
			}
			out["securityContext"] = user
		}
		return out
	}
	container := runs(c.Name)
	if len(c.Command) > 0 {
		container["command"] = c.Command
	}
	if c.Port != 0 {
		container["ports"] = []any{map[string]any{"containerPort": c.Port}}
	}
	if probe := readiness(c); probe != nil {
		probe["periodSeconds"] = 2
		container["readinessProbe"] = probe
	}
	pod := map[string]any{"containers": []any{container}}
	if c.StopDelay > 0 {
		container["lifecycle"] = map[string]any{"preStop": map[string]any{"sleep": map[string]any{"seconds": c.StopDelay}}}
		pod["terminationGracePeriodSeconds"] = c.StopDelay + gracePeriod
	}
	if c.PreStart != nil {
		pod["initContainers"] = []any{command(preStart, *c.PreStart)}
	}
	if len(volumes) > 0 {
		pod["volumes"] = volumes
	}
	podMeta := map[string]any{"labels": labels}
	if hash := secretsHash(app, c); hash != "" {
		podMeta["annotations"] = map[string]string{secretsAnnotation: hash}
	}
	strategy := map[string]any{"rollingUpdate": map[string]any{"maxUnavailable": 0, "maxSurge": 1}}
	if len(c.Volumes) > 0 && !c.Rolling {
		strategy = map[string]any{"type": "Recreate"}
	}
	docs = append(docs, map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata":   meta(),
		"spec": map[string]any{
			"replicas": c.Replicas,
			"selector": map[string]any{"matchLabels": labels},
			"strategy": strategy,
			"template": map[string]any{
				"metadata": podMeta,
				"spec":     pod,
			},
		},
	})
	for _, cron := range c.Cron {
		run := map[string]any{"restartPolicy": "Never", "containers": []any{command(cron.Name, cron.Command)}}
		if len(volumes) > 0 {
			run["volumes"] = volumes
		}
		ran := maps.Clone(labels)
		delete(ran, containerLabel)
		ran[cronLabel] = cron.Name
		docs = append(docs, map[string]any{
			"apiVersion": "batch/v1",
			"kind":       "CronJob",
			"metadata":   named(cronJobOf(c.Name, cron.Name)),
			"spec": map[string]any{
				"schedule":          cron.Schedule,
				"concurrencyPolicy": "Forbid",
				"jobTemplate": map[string]any{"spec": map[string]any{"template": map[string]any{
					"metadata": map[string]any{"labels": ran},
					"spec":     run,
				}}},
			},
		})
	}
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

// readiness is the check a replica of c passes before it takes
// requests: its health check, or else its port accepting a connection,
// and nil for a container with neither. A GET carries the container's
// host as its Host header.
func readiness(c model.Container) map[string]any {
	switch h := c.Healthcheck; {
	case h != nil && h.Path != "":
		get := map[string]any{"path": h.Path, "port": c.Port}
		if c.Host != "" {
			get["httpHeaders"] = []any{map[string]any{"name": "Host", "value": c.Host}}
		}
		return map[string]any{"httpGet": get}
	case h != nil:
		return map[string]any{"exec": map[string]any{"command": h.Command}}
	case c.Port != 0:
		return map[string]any{"tcpSocket": map[string]any{"port": c.Port}}
	}
	return nil
}

// readManifests are the manifests in app's directory of them and its
// subdirectories, in the order of their paths, with each imageVar
// replaced by the image it names and each object labelled as the app's.
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
		for _, m := range imageVar.FindAllStringSubmatch(text, -1) {
			if app.Images[m[1]] == "" {
				return "", fmt.Errorf("%s names %s, but the app builds no %s", p, m[0], m[1])
			}
		}
		text = imageVar.ReplaceAllStringFunc(text, func(v string) string {
			return app.Images[imageVar.FindStringSubmatch(v)[1]]
		})
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
// namespace, with the app's images as its value images, by name.
func chart(app model.App) (any, error) {
	content, err := packChart(app.Chart)
	if err != nil {
		return nil, err
	}
	spec := map[string]any{
		"chartContent":    content,
		"targetNamespace": namespaceOf(app.Name),
	}
	if len(app.Images) > 0 {
		values, err := yaml.Marshal(map[string]any{"images": app.Images})
		if err != nil {
			return nil, err
		}
		spec["valuesContent"] = string(values)
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
