package k3s_test

import (
	"context"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	"github.com/moostackhq/moostack/internal/model"
	"github.com/moostackhq/moostack/internal/orchestrator"
	"github.com/moostackhq/moostack/internal/orchestrator/k3s"
)

// The test cluster runs the controllers of Kubernetes and of k3s, and
// no kubelet, cert-manager or Traefik. The tests stand in for those:
// they give pods their status, and make the objects whose readiness a
// deploy waits for.

var (
	crds           = schema.GroupVersionResource{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"}
	clusterIssuers = schema.GroupVersionResource{Group: "cert-manager.io", Version: "v1", Resource: "clusterissuers"}
	certificates   = schema.GroupVersionResource{Group: "cert-manager.io", Version: "v1", Resource: "certificates"}
	helmCharts     = schema.GroupVersionResource{Group: "helm.cattle.io", Version: "v1", Resource: "helmcharts"}
)

// definition is a CustomResourceDefinition of a kind that holds
// anything.
func definition(group, kind, plural, scope string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apiextensions.k8s.io/v1",
		"kind":       "CustomResourceDefinition",
		"metadata":   map[string]any{"name": plural + "." + group},
		"spec": map[string]any{
			"group": group,
			"scope": scope,
			"names": map[string]any{"kind": kind, "plural": plural, "singular": strings.ToLower(kind), "listKind": kind + "List"},
			"versions": []any{map[string]any{
				"name": "v1", "served": true, "storage": true,
				"schema": map[string]any{"openAPIV3Schema": map[string]any{"type": "object", "x-kubernetes-preserve-unknown-fields": true}},
			}},
		},
	}}
}

// theIssuer is the cluster's issuer, ready.
func theIssuer() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "cert-manager.io/v1",
		"kind":       "ClusterIssuer",
		"metadata":   map[string]any{"name": "moostack"},
		"status":     map[string]any{"conditions": []any{map[string]any{"type": "Ready", "status": "True"}}},
	}}
}

// readyForApps makes the cluster ready for apps, as a deploy reads it:
// the issuer, cert-manager's webhook, Traefik on a node, and a pod of
// Traefik's ports.
func readyForApps(ctx context.Context) error {
	for _, d := range []*unstructured.Unstructured{
		definition("cert-manager.io", "ClusterIssuer", "clusterissuers", "Cluster"),
		definition("cert-manager.io", "Certificate", "certificates", "Namespaced"),
	} {
		if _, err := objects.Resource(crds).Create(ctx, d, metav1.CreateOptions{}); err != nil {
			return err
		}
	}
	err := retry(ctx, func() error {
		_, err := objects.Resource(clusterIssuers).Create(ctx, theIssuer(), metav1.CreateOptions{})
		if apierrors.IsAlreadyExists(err) {
			return nil
		}
		return err
	})
	if err != nil {
		return fmt.Errorf("creating the issuer: %w", err)
	}
	const manifests = `
apiVersion: v1
kind: Namespace
metadata: {name: cert-manager}
---
apiVersion: v1
kind: Node
metadata: {name: kubeletless}
---
apiVersion: apps/v1
kind: Deployment
metadata: {name: cert-manager-webhook, namespace: cert-manager}
spec:
  selector: {matchLabels: {app: webhook}}
  template:
    metadata: {labels: {app: webhook}}
    spec: {containers: [{name: webhook, image: example.test/webhook}]}
---
apiVersion: apps/v1
kind: DaemonSet
metadata: {name: traefik, namespace: kube-system}
spec:
  selector: {matchLabels: {app: traefik}}
  template:
    metadata: {labels: {app: traefik}}
    spec: {containers: [{name: traefik, image: example.test/traefik}]}
---
apiVersion: v1
kind: Pod
metadata: {name: svclb-traefik, namespace: kube-system, labels: {svccontroller.k3s.cattle.io/svcname: traefik}}
spec: {containers: [{name: lb, image: example.test/lb}]}
`
	if err := create(ctx, manifests); err != nil {
		return err
	}
	return retry(ctx, func() error {
		if err := servicesRun(ctx); err != nil {
			return err
		}
		webhook, err := admin.AppsV1().Deployments("cert-manager").Get(ctx, "cert-manager-webhook", metav1.GetOptions{})
		if err != nil {
			return err
		}
		traefik, err := admin.AppsV1().DaemonSets("kube-system").Get(ctx, "traefik", metav1.GetOptions{})
		if err != nil {
			return err
		}
		if webhook.Status.AvailableReplicas < 1 || traefik.Status.NumberAvailable < 1 {
			return errors.New("cert-manager's webhook and Traefik aren't available yet")
		}
		return nil
	})
}

// servicesRun reports the node kubeletless ready, and makes the pods of
// cert-manager's webhook, of Traefik and of its ports run and be ready.
func servicesRun(ctx context.Context) error {
	node, err := admin.CoreV1().Nodes().Get(ctx, "kubeletless", metav1.GetOptions{})
	if err != nil {
		return err
	}
	now := metav1.Now()
	node.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue, Reason: "KubeletReady", LastHeartbeatTime: now, LastTransitionTime: node.CreationTimestamp}}
	if _, err := admin.CoreV1().Nodes().UpdateStatus(ctx, node, metav1.UpdateOptions{}); err != nil && !apierrors.IsConflict(err) {
		return err
	}
	for _, pods := range [][2]string{
		{"cert-manager", "app=webhook"},
		{"kube-system", "app=traefik"},
		{"kube-system", "svccontroller.k3s.cattle.io/svcname=traefik"},
	} {
		if err := setPods(ctx, pods[0], pods[1], isReady); err != nil {
			return err
		}
	}
	return nil
}

// resources are the kinds the tests create and read, by kind.
var resources = map[string]schema.GroupVersionResource{
	"Namespace":   {Version: "v1", Resource: "namespaces"},
	"Node":        {Version: "v1", Resource: "nodes"},
	"Pod":         {Version: "v1", Resource: "pods"},
	"Secret":      {Version: "v1", Resource: "secrets"},
	"Service":     {Version: "v1", Resource: "services"},
	"ConfigMap":   {Version: "v1", Resource: "configmaps"},
	"Deployment":  {Group: "apps", Version: "v1", Resource: "deployments"},
	"StatefulSet": {Group: "apps", Version: "v1", Resource: "statefulsets"},
	"DaemonSet":   {Group: "apps", Version: "v1", Resource: "daemonsets"},
	"Ingress":     {Group: "networking.k8s.io", Version: "v1", Resource: "ingresses"},
	"HelmChart":   helmCharts,
	"Job":         {Group: "batch", Version: "v1", Resource: "jobs"},
	"Certificate": certificates,
}

// create creates the objects of manifests.
func create(ctx context.Context, manifests string) error {
	dec := yamlDecoder(manifests)
	for {
		var doc map[string]any
		if err := dec.Decode(&doc); errors.Is(err, io.EOF) {
			return nil
		} else if err != nil {
			return err
		}
		if len(doc) == 0 {
			continue
		}
		o := &unstructured.Unstructured{Object: doc}
		var err error
		if o.GetNamespace() == "" {
			_, err = objects.Resource(resources[o.GetKind()]).Create(ctx, o, metav1.CreateOptions{})
		} else {
			_, err = objects.Resource(resources[o.GetKind()]).Namespace(o.GetNamespace()).Create(ctx, o, metav1.CreateOptions{})
		}
		if err != nil {
			return fmt.Errorf("creating %s %s: %w", o.GetKind(), o.GetName(), err)
		}
	}
}

// isReady makes pod one that runs and is ready, and reports whether it
// changed it.
func isReady(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
			return false
		}
	}
	if pod.DeletionTimestamp != nil {
		return false
	}
	pod.Status.Phase = corev1.PodRunning
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now()}}
	pod.Status.ContainerStatuses = nil
	for _, c := range pod.Spec.Containers {
		pod.Status.ContainerStatuses = append(pod.Status.ContainerStatuses, corev1.ContainerStatus{
			Name: c.Name, Image: c.Image, ImageID: c.Image, Ready: true,
			State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.Now()}},
		})
	}
	return true
}

// has makes a function that gives a pod the phase, once.
func has(phase corev1.PodPhase) func(*corev1.Pod) bool {
	return func(pod *corev1.Pod) bool {
		if pod.Status.Phase == phase {
			return false
		}
		pod.Status.Phase = phase
		return true
	}
}

// setPods gives the pods of namespace that selector selects the status
// set makes, for those it changes.
func setPods(ctx context.Context, namespace, selector string, set func(*corev1.Pod) bool) error {
	pods, err := admin.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return err
	}
	for _, p := range pods.Items {
		if !set(&p) {
			continue
		}
		if _, err := admin.CoreV1().Pods(namespace).UpdateStatus(ctx, &p, metav1.UpdateOptions{}); err != nil && !apierrors.IsConflict(err) && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

// keep calls f ten times a second until the test ends or stop is
// called.
func keep(t *testing.T, f func(ctx context.Context) error) (stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		for ctx.Err() == nil {
			if err := f(ctx); err != nil && ctx.Err() == nil {
				t.Errorf("standing in for the cluster: %v", err)
				return
			}
			select {
			case <-ctx.Done():
			case <-time.After(100 * time.Millisecond):
			}
		}
	}()
	stop = sync.OnceFunc(func() {
		cancel()
		<-done
	})
	t.Cleanup(stop)
	return stop
}

// kubelet makes every pod of namespace run and be ready, as it comes,
// until the test ends or stop is called.
func kubelet(t *testing.T, namespace string) (stop func()) {
	return keep(t, func(ctx context.Context) error { return setPods(ctx, namespace, "", isReady) })
}

// helm gives the pods that install and uninstall the chart of the app
// called app the phase, as they come, until the test ends or stop is
// called.
func helm(t *testing.T, app string, phase corev1.PodPhase) (stop func()) {
	selector := "job-name in (helm-install-app-" + app + ",helm-delete-app-" + app + ")"
	return keep(t, func(ctx context.Context) error { return setPods(ctx, "kube-system", selector, has(phase)) })
}

// deployer is a backend that connects as the cluster's administrator.
func deployer(t *testing.T) *k3s.Backend {
	t.Helper()
	b, err := k3s.New(adminConfig)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// within is a context that ends after d, or with the test.
func within(t *testing.T, d time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

// cleared deletes the app's namespace and HelmChart when the test ends.
func cleared(t *testing.T, app string) {
	t.Cleanup(func() {
		ctx := context.Background()
		charts := objects.Resource(helmCharts).Namespace("kube-system")
		charts.Patch(ctx, "app-"+app, types.MergePatchType, []byte(`{"metadata":{"finalizers":null}}`), metav1.PatchOptions{})
		charts.Delete(ctx, "app-"+app, metav1.DeleteOptions{})
		admin.CoreV1().Namespaces().Delete(ctx, app, metav1.DeleteOptions{})
	})
}

// deploy deploys app with a kubelet for its pods, and returns the steps
// told.
func deploy(t *testing.T, b *k3s.Backend, app model.App) []orchestrator.Step {
	t.Helper()
	cleared(t, app.Name)
	kubelet(t, app.Name)
	var steps []orchestrator.Step
	if err := b.Deploy(within(t, time.Minute), app, func(s orchestrator.Step) { steps = append(steps, s) }); err != nil {
		t.Fatalf("Deploy: %v", err)
	}
	return steps
}

// shop is an app of two containers, named name, with a secret.
func shop(name string) model.App {
	return model.App{
		Name: name,
		Containers: []model.Container{
			{Name: "web", Image: "example.test/web:1", Port: 8080, Host: name + ".example.test", Replicas: 2},
			{Name: "worker", Image: "example.test/worker:1", Command: []string{"work"}, Replicas: 1},
		},
		Secrets: map[string]string{"GREETING": "hi"},
	}
}

// entries are how the record names the kinds an app's objects have.
var entries = map[string]string{
	"Secret":      "Secret",
	"Service":     "Service",
	"ConfigMap":   "ConfigMap",
	"Deployment":  "Deployment.apps",
	"StatefulSet": "StatefulSet.apps",
	"DaemonSet":   "DaemonSet.apps",
	"Ingress":     "Ingress.networking.k8s.io",
}

// applied are the objects of the namespace that carry the app's label,
// as the record names them, in order, without the record itself.
func applied(t *testing.T, namespace string) []string {
	t.Helper()
	var names []string
	for kind, entry := range entries {
		list, err := objects.Resource(resources[kind]).Namespace(namespace).List(context.Background(), metav1.ListOptions{LabelSelector: appLabel + "=" + namespace})
		if err != nil {
			t.Fatal(err)
		}
		for _, o := range list.Items {
			if kind == "ConfigMap" && o.GetName() == "moostack-objects" || o.GetDeletionTimestamp() != nil {
				continue
			}
			names = append(names, entry+"/"+o.GetName())
		}
	}
	slices.Sort(names)
	return names
}

// record is what the app's record lists, in order.
func record(t *testing.T, app string) []string {
	t.Helper()
	m, err := admin.CoreV1().ConfigMaps(app).Get(context.Background(), "moostack-objects", metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	if m.Labels[appLabel] != app {
		t.Errorf("the record of %s carries the labels %v, want the app's", app, m.Labels)
	}
	names := strings.Fields(m.Data["objects"])
	slices.Sort(names)
	return names
}

func TestADeployAppliesTheAppsObjectsAndRecordsThem(t *testing.T) {
	name := unique("applied")
	steps := deploy(t, deployer(t), shop(name))
	if !slices.Equal(steps, []orchestrator.Step{orchestrator.Applying, orchestrator.WaitingForReady}) {
		t.Errorf("the steps told were %v, want Applying, then WaitingForReady", steps)
	}
	want := []string{"Deployment.apps/web", "Deployment.apps/worker", "Ingress.networking.k8s.io/web", "Secret/" + name, "Service/web"}
	if got := applied(t, name); !slices.Equal(got, want) {
		t.Errorf("the cluster has %q, want %q", got, want)
	}
	if got := record(t, name); !slices.Equal(got, want) {
		t.Errorf("the record lists %q, want %q", got, want)
	}
	ctx := context.Background()
	ns, err := admin.CoreV1().Namespaces().Get(ctx, name, metav1.GetOptions{})
	if err != nil || ns.Labels[appLabel] != name {
		t.Errorf("the namespace: %v, labels %v, want the app's label", err, ns.Labels)
	}
	web, err := admin.AppsV1().Deployments(name).Get(ctx, "web", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if c := web.Spec.Template.Spec.Containers[0]; *web.Spec.Replicas != 2 || c.Image != "example.test/web:1" || c.Ports[0].ContainerPort != 8080 || c.EnvFrom[0].SecretRef.Name != name {
		t.Errorf("web's Deployment has %d replicas of %+v", *web.Spec.Replicas, c)
	}
	if !slices.ContainsFunc(web.ManagedFields, func(f metav1.ManagedFieldsEntry) bool {
		return f.Manager == "mooctl" && f.Operation == metav1.ManagedFieldsOperationApply
	}) {
		t.Errorf("web's Deployment wasn't applied server side as mooctl: %+v", web.ManagedFields)
	}
	secret, err := admin.CoreV1().Secrets(name).Get(ctx, name, metav1.GetOptions{})
	if err != nil || string(secret.Data["GREETING"]) != "hi" {
		t.Errorf("the Secret: %v, %q", err, secret.Data)
	}
	ingress, err := admin.NetworkingV1().Ingresses(name).Get(ctx, "web", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if ingress.Annotations["cert-manager.io/cluster-issuer"] != "moostack" || ingress.Spec.Rules[0].Host != name+".example.test" || ingress.Spec.TLS[0].SecretName != "web-tls" {
		t.Errorf("the Ingress: %+v, %+v", ingress.Annotations, ingress.Spec)
	}
}

func TestADeployDeletesWhatTheLastAppliedAndThisOneDoesnt(t *testing.T) {
	name := unique("trimmed")
	b := deployer(t)
	app := shop(name)
	deploy(t, b, app)
	app.Containers = app.Containers[1:]
	app.Secrets = nil
	deploy(t, b, app)
	want := []string{"Deployment.apps/worker"}
	if got := applied(t, name); !slices.Equal(got, want) {
		t.Errorf("the cluster has %q after a deploy of worker alone, want %q", got, want)
	}
	if got := record(t, name); !slices.Equal(got, want) {
		t.Errorf("the record lists %q, want %q", got, want)
	}
}

func TestADeployTakesOverWhatAnotherManagerSet(t *testing.T) {
	name := unique("forced")
	b := deployer(t)
	deploy(t, b, shop(name))
	scale := []byte(`{"apiVersion":"apps/v1","kind":"Deployment","spec":{"replicas":5}}`)
	if _, err := admin.AppsV1().Deployments(name).Patch(context.Background(), "web", types.ApplyPatchType, scale, metav1.PatchOptions{FieldManager: "other", Force: new(true)}); err != nil {
		t.Fatal(err)
	}
	deploy(t, b, shop(name))
	web, err := admin.AppsV1().Deployments(name).Get(context.Background(), "web", metav1.GetOptions{})
	if err != nil || *web.Spec.Replicas != 2 {
		t.Errorf("web has %d replicas after a deploy over another manager's 5, %v, want 2", *web.Spec.Replicas, err)
	}
}

func TestADeployDeletesAnObjectWhoseNameDiffersFromOneKeptOnlyInCaseOrHasASpace(t *testing.T) {
	name := unique("names")
	b := deployer(t)
	role := func(n string) string {
		return "apiVersion: rbac.authorization.k8s.io/v1\nkind: Role\nmetadata: {name: \"" + n + "\"}\nrules: []"
	}
	roles := func() []string {
		list, err := admin.RbacV1().Roles(name).List(context.Background(), metav1.ListOptions{})
		if err != nil {
			t.Fatal(err)
		}
		var names []string
		for _, r := range list.Items {
			names = append(names, r.Name)
		}
		slices.Sort(names)
		return names
	}
	deploy(t, b, manifestsOf(t, name, role("Reader"), role("reader"), role("read pods"), role("read")))
	if got, want := roles(), []string{"Reader", "read", "read pods", "reader"}; !slices.Equal(got, want) {
		t.Fatalf("the app's Roles are %q, want %q", got, want)
	}
	m, err := admin.CoreV1().ConfigMaps(name).Get(context.Background(), "moostack-objects", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := m.Data["objects"], "Role.rbac.authorization.k8s.io/Reader\nRole.rbac.authorization.k8s.io/reader\nRole.rbac.authorization.k8s.io/read pods\nRole.rbac.authorization.k8s.io/read"; got != want {
		t.Errorf("the record holds %q, want %q", got, want)
	}
	deploy(t, b, manifestsOf(t, name, role("Reader"), role("read")))
	if got, want := roles(), []string{"Reader", "read"}; !slices.Equal(got, want) {
		t.Errorf("the app's Roles after a deploy without two of them are %q, want %q", got, want)
	}
}

// manifestsOf is an app of its own manifests, named name.
func manifestsOf(t *testing.T, name string, manifests ...string) model.App {
	t.Helper()
	return model.App{Name: name, Manifests: files(t, t.TempDir(), map[string]string{"app.yaml": strings.Join(manifests, "\n---\n")})}
}

const (
	configMap  = "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings}\ndata: {a: b}"
	deployment = `apiVersion: apps/v1
kind: Deployment
metadata: {name: web}
spec:
  replicas: 2
  selector: {matchLabels: {app: web}}
  template:
    metadata: {labels: {app: web}}
    spec: {containers: [{name: web, image: example.test/web}]}`
	statefulSet = `apiVersion: apps/v1
kind: StatefulSet
metadata: {name: db}
spec:
  serviceName: db
  replicas: 2
  selector: {matchLabels: {app: db}}
  template:
    metadata: {labels: {app: db}}
    spec: {containers: [{name: db, image: example.test/db}]}`
	daemonSet = `apiVersion: apps/v1
kind: DaemonSet
metadata: {name: agent}
spec:
  selector: {matchLabels: {app: agent}}
  template:
    metadata: {labels: {app: agent}}
    spec: {containers: [{name: agent, image: example.test/agent}]}`
)

func TestADeployOfManifestsAppliesThemInTheAppsNamespace(t *testing.T) {
	name := unique("manifests")
	// An object that takes another namespace through a merge key.
	merged := "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: merged-" + name + ", <<: {namespace: kube-public}}"
	deploy(t, deployer(t), manifestsOf(t, name, configMap, deployment, merged))
	if _, err := admin.CoreV1().ConfigMaps("kube-public").Get(context.Background(), "merged-"+name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("the app's ConfigMap in another namespace: %v, want none there", err)
	}
	want := []string{"ConfigMap/merged-" + name, "ConfigMap/settings", "Deployment.apps/web"}
	if got := applied(t, name); !slices.Equal(got, want) {
		t.Errorf("the cluster has %q, want %q", got, want)
	}
	if got := record(t, name); !slices.Equal(got, want) {
		t.Errorf("the record lists %q, want %q", got, want)
	}
}

// absent fails the test if the cluster has a namespace called name.
func absent(t *testing.T, name string) {
	t.Helper()
	if _, err := admin.CoreV1().Namespaces().Get(context.Background(), name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("the cluster has the namespace %s: %v, want nothing applied", name, err)
	}
}

func TestADeployRefusesAKindThatIsntKeptInANamespaceWithNothingApplied(t *testing.T) {
	name := unique("unscoped")
	cleared(t, name)
	app := manifestsOf(t, name, configMap, "apiVersion: rbac.authorization.k8s.io/v1\nkind: ClusterRole\nmetadata: {name: "+name+"}")
	var steps []orchestrator.Step
	err := deployer(t).Deploy(within(t, time.Minute), app, func(s orchestrator.Step) { steps = append(steps, s) })
	if want := "clusterrole/" + name + " isn't in a namespace; an app's manifests hold only objects of its namespace"; err == nil || err.Error() != want {
		t.Errorf("Deploy: %v, want %q", err, want)
	}
	absent(t, name)
	if _, err := admin.RbacV1().ClusterRoles().Get(context.Background(), name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("the ClusterRole: %v, want none", err)
	}
	if slices.Contains(steps, orchestrator.WaitingForReady) {
		t.Errorf("the steps told were %v, want no wait for a refused app", steps)
	}
}

func TestADeployRefusesAKindTheClusterDoesntHaveWithNothingApplied(t *testing.T) {
	name := unique("unknown")
	cleared(t, name)
	app := manifestsOf(t, name, configMap, "apiVersion: example.test/v1\nkind: Widget\nmetadata: {name: w}")
	err := deployer(t).Deploy(within(t, time.Minute), app, func(orchestrator.Step) {})
	if err == nil || !strings.Contains(err.Error(), "widget/w: no matches for kind \"Widget\"") {
		t.Errorf("Deploy: %v, want the kind refused", err)
	}
	absent(t, name)
}

func TestADeployLeavesARecordedKindTheClusterNoLongerHasAndWhatIsntTheAppsToDelete(t *testing.T) {
	name := unique("stale")
	b := deployer(t)
	deploy(t, b, manifestsOf(t, name, configMap, deployment))
	ctx := context.Background()
	records := admin.CoreV1().ConfigMaps(name)
	m, err := records.Get(ctx, "moostack-objects", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// The app's objects as kubectl names them, a kind without a
	// definition, an entry without a name, and a kind that isn't kept in
	// a namespace.
	m.Data["objects"] = "configmap/settings\ndeployment/web\nWidget.example.test/gone\nDeployment.apps/\nNamespace/" + name
	if _, err := records.Update(ctx, m, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	deploy(t, b, manifestsOf(t, name, configMap, deployment))
	both := []string{"ConfigMap/settings", "Deployment.apps/web"}
	if got := applied(t, name); !slices.Equal(got, both) {
		t.Errorf("the cluster has %q after a deploy over a record that names the app's objects as kubectl does, want %q", got, both)
	}
	if got := record(t, name); !slices.Equal(got, both) {
		t.Errorf("the record lists %q, want %q", got, both)
	}
	m, err = records.Get(ctx, "moostack-objects", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	m.Data["objects"] = "configmap/settings\ndeployment/web"
	if _, err := records.Update(ctx, m, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	deploy(t, b, manifestsOf(t, name, deployment))
	want := []string{"Deployment.apps/web"}
	if got := applied(t, name); !slices.Equal(got, want) {
		t.Errorf("the cluster has %q, want %q", got, want)
	}
	if got := record(t, name); !slices.Equal(got, want) {
		t.Errorf("the record lists %q, want %q", got, want)
	}
	if ns, err := admin.CoreV1().Namespaces().Get(ctx, name, metav1.GetOptions{}); err != nil || ns.DeletionTimestamp != nil {
		t.Errorf("the app's namespace, which its record listed: %v, deleted at %v", err, ns.DeletionTimestamp)
	}
}

// blocked fails the test if a call that reports to done has returned
// within d.
func blocked(t *testing.T, done <-chan error, d time.Duration, why string) {
	t.Helper()
	select {
	case err := <-done:
		t.Fatalf("the call returned %v, %s", err, why)
	case <-time.After(d):
	}
}

// returned is what a call reports to done, within a minute.
func returned(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(time.Minute):
		t.Fatal("the call didn't return")
		return nil
	}
}

func TestADeployReturnsOnceEveryWorkloadRunsAsApplied(t *testing.T) {
	for kind, manifest := range map[string]string{"Deployment": deployment, "StatefulSet": statefulSet, "DaemonSet": daemonSet} {
		t.Run(kind, func(t *testing.T) {
			name := unique("waits")
			cleared(t, name)
			b := deployer(t)
			start := func(app model.App) <-chan error {
				done := make(chan error, 1)
				go func() { done <- b.Deploy(within(t, 2*time.Minute), app, func(orchestrator.Step) {}) }()
				return done
			}
			done := start(manifestsOf(t, name, manifest))
			blocked(t, done, 4*time.Second, "with none of its pods ready")
			stop := kubelet(t, name)
			if err := returned(t, done); err != nil {
				t.Fatalf("Deploy: %v", err)
			}
			stop()

			done = start(manifestsOf(t, name, strings.ReplaceAll(manifest, "image: example.test/", "image: example.test/new-")))
			blocked(t, done, 4*time.Second, "with the pods of the changed app not ready")
			kubelet(t, name)
			if err := returned(t, done); err != nil {
				t.Fatalf("Deploy of the changed app: %v", err)
			}
			pods, err := admin.CoreV1().Pods(name).List(context.Background(), metav1.ListOptions{})
			if err != nil {
				t.Fatal(err)
			}
			for _, p := range pods.Items {
				if p.DeletionTimestamp == nil && !strings.HasPrefix(p.Spec.Containers[0].Image, "example.test/new-") {
					t.Errorf("the pod %s runs %s once the changed app's deploy returned", p.Name, p.Spec.Containers[0].Image)
				}
			}
		})
	}
}

func TestADeployThatIsntReadyInTimeFailsWithTheAppsPodsAndLastEvents(t *testing.T) {
	name := unique("never")
	cleared(t, name)
	app := shop(name)
	app.Containers = app.Containers[:1]
	start := time.Now()
	err := deployer(t).Deploy(within(t, 6*time.Second), app, func(orchestrator.Step) {})
	if err == nil {
		t.Fatal("a deploy whose pods never get ready succeeded")
	}
	if took := time.Since(start); took < 5*time.Second || took > 30*time.Second {
		t.Errorf("the deploy failed after %s, with 6 seconds to get ready in", took)
	}
	lines := strings.Split(err.Error(), "\n")
	if want := name + " isn't ready: deployment web: 0 of 2 replicas are updated and available"; lines[0] != want {
		t.Errorf("the error starts with %q, want %q", lines[0], want)
	}
	pods, events := 0, 0
	for _, line := range lines[1:] {
		switch {
		case strings.HasPrefix(line, "pod web-") && strings.HasSuffix(line, " on no node: Pending"):
			pods++
		case strings.HasPrefix(line, "Normal deployment/web: Scaled up replica set"), strings.HasPrefix(line, "Normal replicaset/web-"):
			events++
		}
	}
	if pods != 2 || events < 2 {
		t.Errorf("the error lists %d pods and %d events of the app, want its 2 pods and its events:\n%s", pods, events, err)
	}
}

func TestTheLastEventsOfADeployThatIsntReadyAreItsNewestWhoeverToldThem(t *testing.T) {
	name := unique("events")
	cleared(t, name)
	app := shop(name)
	app.Containers = app.Containers[:1]
	b := deployer(t)
	first := b.Deploy(within(t, 8*time.Second), app, func(orchestrator.Step) {})
	if first == nil || !strings.Contains(first.Error(), "nodes are available") {
		t.Fatalf("Deploy: %v, want the scheduler's event of pods that no node takes", first)
	}
	old := metav1.NewTime(time.Now().Add(-30 * time.Minute))
	for i := range 16 {
		e := &corev1.Event{
			ObjectMeta:     metav1.ObjectMeta{Name: fmt.Sprintf("old-%d", i), Namespace: name},
			InvolvedObject: corev1.ObjectReference{Kind: "Pod", Name: "web-old", Namespace: name},
			Reason:         "Pulled", Message: "an event of half an hour ago", Type: "Normal",
			FirstTimestamp: old, LastTimestamp: old,
		}
		if _, err := admin.CoreV1().Events(name).Create(context.Background(), e, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	err := b.Deploy(within(t, 8*time.Second), app, func(orchestrator.Step) {})
	if err == nil || !strings.Contains(err.Error(), "nodes are available") {
		t.Errorf("Deploy with 16 older events in the namespace: %v, want the scheduler's event among the last", err)
	}
	if lines := strings.Split(err.Error(), "\n"); strings.Count(err.Error(), "an event of half an hour ago") > 14 || len(lines) > 1+2+15 {
		t.Errorf("the error holds more than the last 15 events:\n%v", err)
	}
}

func TestADeployRefusesAnAppWhoseNamespaceIsThereAndIsntAnApps(t *testing.T) {
	name := unique("theirs")
	createNamespace(t, name)
	cleared(t, name)
	err := deployer(t).Deploy(within(t, time.Minute), shop(name), func(orchestrator.Step) {})
	if want := "the cluster has a namespace " + name + " that isn't an app's; name the app otherwise"; err == nil || err.Error() != want {
		t.Errorf("Deploy: %v, want %q", err, want)
	}
	ns, err := admin.CoreV1().Namespaces().Get(context.Background(), name, metav1.GetOptions{})
	if err != nil || len(ns.Labels[appLabel]) != 0 {
		t.Errorf("the namespace after the refused deploy: %v, labelled as of the app %q, want it as it was", err, ns.Labels[appLabel])
	}
	if got := applied(t, name); len(got) != 0 {
		t.Errorf("the refused deploy applied %q", got)
	}
}

func TestADeployWhoseRolloutExceedsItsDeadlineFailsAtOnce(t *testing.T) {
	name := unique("deadline")
	cleared(t, name)
	app := manifestsOf(t, name, strings.Replace(deployment, "spec:\n", "spec:\n  progressDeadlineSeconds: 2\n", 1))
	start := time.Now()
	err := deployer(t).Deploy(within(t, 2*time.Minute), app, func(orchestrator.Step) {})
	if err == nil || !strings.Contains(err.Error(), name+" isn't ready: deployment web exceeded its progress deadline\npod web-") {
		t.Errorf("Deploy: %v, want the progress deadline exceeded, and the pods", err)
	}
	if took := time.Since(start); took > time.Minute {
		t.Errorf("the deploy failed after %s, want soon after the 2 seconds of the deadline", took)
	}
}

func TestAnAppsWorkloadThatIsntRolledIsRefusedToWaitFor(t *testing.T) {
	name := unique("ondelete")
	cleared(t, name)
	app := manifestsOf(t, name, strings.Replace(daemonSet, "spec:\n", "spec:\n  updateStrategy: {type: OnDelete}\n", 1))
	err := deployer(t).Deploy(within(t, time.Minute), app, func(orchestrator.Step) {})
	if want := name + " isn't ready: daemonset agent: a rollout is waited for only with the update strategy RollingUpdate"; err == nil || err.Error() != want {
		t.Errorf("Deploy: %v, want %q", err, want)
	}
}

// chartOf is an app of its own chart, named name, whose chart holds a
// ConfigMap with the value.
func chartOf(t *testing.T, name, value string) model.App {
	t.Helper()
	return model.App{Name: name, Image: "example.test/" + name + ":1", Chart: files(t, t.TempDir()+"/chart", map[string]string{
		"Chart.yaml":         "apiVersion: v2\nname: web\nversion: 0.1.0\n",
		"templates/cm.yaml":  "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: c}\ndata: {value: " + value + "}\n",
		"templates/NOTES.md": "x",
	})}
}

// installJob is the UID of the job that installs the chart of the app
// called app.
func installJob(t *testing.T, app string) string {
	t.Helper()
	job, err := admin.BatchV1().Jobs("kube-system").Get(context.Background(), "helm-install-app-"+app, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return string(job.UID)
}

func TestADeployOfAChartWaitsForItsInstallAndAnUnchangedChartForNoNewOne(t *testing.T) {
	name := unique("chart")
	cleared(t, name)
	b := deployer(t)
	start := func(app model.App) <-chan error {
		done := make(chan error, 1)
		go func() { done <- b.Deploy(within(t, 2*time.Minute), app, func(orchestrator.Step) {}) }()
		return done
	}
	app := chartOf(t, name, "one")
	done := start(app)
	blocked(t, done, 4*time.Second, "with the chart's install not ended")
	stop := helm(t, name, corev1.PodSucceeded)
	if err := returned(t, done); err != nil {
		t.Fatalf("Deploy: %v", err)
	}
	stop()
	chart, err := objects.Resource(helmCharts).Namespace("kube-system").Get(context.Background(), "app-"+name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	spec := chart.Object["spec"].(map[string]any)
	if chart.GetLabels()[appLabel] != name || spec["targetNamespace"] != name || spec["valuesContent"] != "image: example.test/"+name+":1\n" || spec["chartContent"] == "" {
		t.Errorf("the HelmChart: labels %v, spec %v", chart.GetLabels(), spec)
	}
	if got := record(t, name); len(got) != 0 {
		t.Errorf("the record of an app of a chart lists %q, want nothing", got)
	}
	first := installJob(t, name)

	// A deploy of the unchanged chart returns with no install ending.
	if err := returned(t, start(app)); err != nil {
		t.Fatalf("Deploy of the unchanged chart: %v", err)
	}
	if again := installJob(t, name); again != first {
		t.Errorf("the unchanged chart was installed again, by the job %s after %s", again, first)
	}

	done = start(chartOf(t, name, "two"))
	blocked(t, done, 4*time.Second, "with the changed chart's install not ended")
	helm(t, name, corev1.PodSucceeded)
	if err := returned(t, done); err != nil {
		t.Fatalf("Deploy of the changed chart: %v", err)
	}
	if changed := installJob(t, name); changed == first {
		t.Error("the changed chart's deploy returned with the install job of the chart before")
	}

	// As an app of containers, it has no chart.
	deploy(t, b, shop(name))
	if _, err := objects.Resource(helmCharts).Namespace("kube-system").Get(context.Background(), "app-"+name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("the HelmChart after a deploy of the app without its chart: %v, want it gone", err)
	}
}

func TestADeployOfAChartWhoseInstallFailsThreeTimesFails(t *testing.T) {
	name := unique("badchart")
	cleared(t, name)
	helm(t, name, corev1.PodFailed)
	err := deployer(t).Deploy(within(t, 3*time.Minute), chartOf(t, name, "one"), func(orchestrator.Step) {})
	if err == nil || !strings.HasPrefix(err.Error(), name+" isn't ready: its chart's install failed:\n") {
		t.Errorf("Deploy: %v, want the chart's install failed", err)
	}
	job, err := admin.BatchV1().Jobs("kube-system").Get(context.Background(), "helm-install-app-"+name, metav1.GetOptions{})
	if err != nil || job.Status.Failed != 3 {
		t.Errorf("the deploy failed with %d failed pods of the install, %v, want 3", job.Status.Failed, err)
	}
}

func TestADeployOfAChartThatIsntInstalledInTimeFails(t *testing.T) {
	name := unique("slowchart")
	cleared(t, name)
	err := deployer(t).Deploy(within(t, 5*time.Second), chartOf(t, name, "one"), func(orchestrator.Step) {})
	if want := name + " isn't ready: its chart isn't installed yet"; err == nil || err.Error() != want {
		t.Errorf("Deploy: %v, want %q", err, want)
	}
}

func TestADeployWaitsForTheClustersOwnServices(t *testing.T) {
	ctx := context.Background()
	issuers := objects.Resource(clusterIssuers)
	restore := func() {
		if _, err := issuers.Create(ctx, theIssuer(), metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			t.Errorf("restoring the issuer: %v", err)
		}
	}
	t.Cleanup(restore)
	if err := issuers.Delete(ctx, "moostack", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	notReady := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "cert-manager.io/v1", "kind": "ClusterIssuer", "metadata": map[string]any{"name": "moostack"},
		"status": map[string]any{"conditions": []any{map[string]any{"type": "Ready", "status": "False"}}},
	}}

	b := deployer(t)
	name := unique("early")
	cleared(t, name)
	for state, make := range map[string]func(){
		"absent":    func() {},
		"not ready": func() { issuers.Create(ctx, notReady, metav1.CreateOptions{}) },
	} {
		make()
		var steps []orchestrator.Step
		err := b.Deploy(within(t, 5*time.Second), shop(name), func(s orchestrator.Step) { steps = append(steps, s) })
		if want := "the cluster isn't ready for apps: still waiting for the issuer moostack"; err == nil || err.Error() != want {
			t.Errorf("Deploy with the issuer %s: %v, want %q", state, err, want)
		}
		if !slices.Equal(steps, []orchestrator.Step{orchestrator.WaitingForCluster}) {
			t.Errorf("the steps told with the issuer %s were %v, want WaitingForCluster alone", state, steps)
		}
		absent(t, name)
	}
	if err := issuers.Delete(ctx, "moostack", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}

	kubelet(t, name)
	var steps []orchestrator.Step
	done := make(chan error, 1)
	go func() {
		done <- b.Deploy(within(t, time.Minute), shop(name), func(s orchestrator.Step) { steps = append(steps, s) })
	}()
	blocked(t, done, 3*time.Second, "with no issuer")
	restore()
	if err := returned(t, done); err != nil {
		t.Fatalf("Deploy: %v", err)
	}
	if !slices.Equal(steps, []orchestrator.Step{orchestrator.WaitingForCluster, orchestrator.Applying, orchestrator.WaitingForReady}) {
		t.Errorf("the steps told were %v, want WaitingForCluster, Applying, WaitingForReady", steps)
	}
}

func TestADeployNamesEachOfTheClustersServicesThatIsntReady(t *testing.T) {
	ctx := context.Background()
	// A cluster of its own, with none of the services.
	config := rest.CopyConfig(adminConfig)
	config.Wrap(func(rt http.RoundTripper) http.RoundTripper {
		return roundTripper(func(r *http.Request) (*http.Response, error) {
			for _, own := range []string{"/clusterissuers/", "/namespaces/cert-manager/", "/namespaces/kube-system/daemonsets/"} {
				r.URL.Path = strings.Replace(r.URL.Path, own, own+"no-", 1)
			}
			if r.URL.Query().Get("labelSelector") == "svccontroller.k3s.cattle.io/svcname=traefik" {
				q := r.URL.Query()
				q.Set("labelSelector", "svccontroller.k3s.cattle.io/svcname=none")
				r.URL.RawQuery = q.Encode()
			}
			return rt.RoundTrip(r)
		})
	})
	b, err := k3s.New(config)
	if err != nil {
		t.Fatal(err)
	}
	name := unique("bare")
	deadline, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	err = b.Deploy(deadline, shop(name), func(orchestrator.Step) {})
	if want := "the cluster isn't ready for apps: still waiting for the issuer moostack, cert-manager's webhook, Traefik, Traefik's ports"; err == nil || err.Error() != want {
		t.Errorf("Deploy: %v, want %q", err, want)
	}
	absent(t, name)
}

// certificate is the Certificate of the container web of the app
// called app, with the condition.
func certificate(app, status, message string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "cert-manager.io/v1", "kind": "Certificate",
		"metadata": map[string]any{"name": "web-tls", "namespace": app},
		"status":   map[string]any{"conditions": []any{map[string]any{"type": "Ready", "status": status, "message": message}}},
	}}
}

func TestCertificateWaitsUntilTheHostsCertificateIsIssued(t *testing.T) {
	ctx := context.Background()
	name := unique("cert")
	b := deployer(t)
	deploy(t, b, shop(name))

	err := b.Certificate(within(t, 3*time.Second), name, "web")
	if want := "it isn't asked for yet"; err == nil || err.Error() != want {
		t.Errorf("Certificate with none asked for: %v, want %q", err, want)
	}
	certs := objects.Resource(certificates).Namespace(name)
	if _, err := certs.Create(ctx, certificate(name, "False", "the challenge is pending"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	err = b.Certificate(within(t, 3*time.Second), name, "web")
	if want := "the challenge is pending"; err == nil || err.Error() != want {
		t.Errorf("Certificate with one that isn't issued: %v, want %q", err, want)
	}

	done := make(chan error, 1)
	go func() { done <- b.Certificate(within(t, time.Minute), name, "web") }()
	blocked(t, done, 3*time.Second, "with the certificate not issued")
	issued := certificate(name, "True", "issued")
	current, err := certs.Get(ctx, "web-tls", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	issued.SetResourceVersion(current.GetResourceVersion())
	if _, err := certs.Update(ctx, issued, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := returned(t, done); err != nil {
		t.Errorf("Certificate once it is issued: %v", err)
	}
}

func TestCertificateWaitsForTheCertificateAsItIsNow(t *testing.T) {
	name := unique("oldcert")
	b := deployer(t)
	deploy(t, b, shop(name))
	c := certificate(name, "True", "issued for the host before")
	condition := c.Object["status"].(map[string]any)["conditions"].([]any)[0].(map[string]any)
	condition["observedGeneration"] = int64(0)
	certs := objects.Resource(certificates).Namespace(name)
	created, err := certs.Create(context.Background(), c, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if created.GetGeneration() < 1 {
		t.Fatalf("the Certificate's generation is %d, want one later than its condition's 0", created.GetGeneration())
	}
	if err := b.Certificate(within(t, 3*time.Second), name, "web"); err == nil || err.Error() != "issued for the host before" {
		t.Errorf("Certificate with a Ready condition of an earlier generation: %v, want it not issued", err)
	}
	// The update that sets the condition is a generation of its own.
	condition["observedGeneration"] = created.GetGeneration() + 1
	c.SetResourceVersion(created.GetResourceVersion())
	if _, err := certs.Update(context.Background(), c, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := b.Certificate(within(t, time.Minute), name, "web"); err != nil {
		t.Errorf("Certificate with a Ready condition of its generation: %v", err)
	}
}

func TestCertificateWhoseContextEndsAsItReadsTheAppFailsWithTheCause(t *testing.T) {
	name := unique("cutcert")
	deploy(t, deployer(t), shop(name))
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("interrupted")
	config := rest.CopyConfig(adminConfig)
	config.Wrap(func(rt http.RoundTripper) http.RoundTripper {
		return roundTripper(func(r *http.Request) (*http.Response, error) {
			cancel(cause)
			return rt.RoundTrip(r)
		})
	})
	b, err := k3s.New(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Certificate(ctx, name, "web"); err == nil || err.Error() != cause.Error() {
		t.Errorf("Certificate: %v, want the context's cause alone", err)
	}
}

func TestADeployWhoseWorkloadsArentReadInTimeSaysSo(t *testing.T) {
	name := unique("unread")
	cleared(t, name)
	config := rest.CopyConfig(adminConfig)
	config.Wrap(func(rt http.RoundTripper) http.RoundTripper {
		return roundTripper(func(r *http.Request) (*http.Response, error) {
			if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/namespaces/"+name+"/deployments") {
				<-r.Context().Done()
			}
			return rt.RoundTrip(r)
		})
	})
	b, err := k3s.New(config)
	if err != nil {
		t.Fatal(err)
	}
	err = b.Deploy(within(t, 5*time.Second), shop(name), func(orchestrator.Step) {})
	if err == nil || !strings.HasPrefix(err.Error(), name+" isn't ready: its workloads weren't read in time\npod web-") {
		t.Errorf("Deploy: %v, want that its workloads weren't read in time, and its pods", err)
	}
}

func TestCertificateOfAnAppTheClusterDoesntHaveIsErrNoApp(t *testing.T) {
	b := deployer(t)
	if err := b.Certificate(within(t, time.Minute), unique("absent"), "web"); !errors.Is(err, orchestrator.ErrNoApp) {
		t.Errorf("Certificate of an app never deployed: %v, want ErrNoApp", err)
	}
	// A namespace that isn't an app's.
	if err := b.Certificate(within(t, time.Minute), "kube-system", "web"); !errors.Is(err, orchestrator.ErrNoApp) {
		t.Errorf("Certificate of the cluster's own namespace: %v, want ErrNoApp", err)
	}
}

func TestACallOnAnEndedContextFailsAndADeployChangesNothing(t *testing.T) {
	b := deployer(t)
	there := unique("there")
	deploy(t, b, shop(there))
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("the operation lock is lost")
	cancel(cause)
	name := unique("ended")
	cleared(t, name)
	var steps []orchestrator.Step
	if err := b.Deploy(ctx, shop(name), func(s orchestrator.Step) { steps = append(steps, s) }); !errors.Is(err, cause) {
		t.Errorf("Deploy on an ended context: %v, want its cause", err)
	}
	if len(steps) != 0 {
		t.Errorf("a deploy on an ended context told the steps %v", steps)
	}
	absent(t, name)
	if err := b.Certificate(ctx, there, "web"); !errors.Is(err, cause) {
		t.Errorf("Certificate on an ended context: %v, want its cause", err)
	}
	if _, err := b.Nodes(ctx); !errors.Is(err, cause) {
		t.Errorf("Nodes on an ended context: %v, want its cause", err)
	}
}

func TestADeployWhoseContextEndsWhileItWaitsFailsWithTheCause(t *testing.T) {
	name := unique("interrupted")
	cleared(t, name)
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("interrupted")
	done := make(chan error, 1)
	go func() { done <- deployer(t).Deploy(ctx, shop(name), func(orchestrator.Step) {}) }()
	blocked(t, done, 3*time.Second, "with none of its pods ready")
	cancel(cause)
	if err := returned(t, done); !errors.Is(err, cause) || strings.Contains(err.Error(), "pod ") {
		t.Errorf("Deploy: %v, want the context's cause alone", err)
	}
}

func TestADeployWhoseContextEndsAsItAppliesFailsWithTheCause(t *testing.T) {
	name := unique("cut")
	cleared(t, name)
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("the operation lock is lost")
	err := deployer(t).Deploy(ctx, shop(name), func(s orchestrator.Step) {
		if s == orchestrator.Applying {
			cancel(cause)
		}
	})
	if err == nil || err.Error() != cause.Error() {
		t.Errorf("Deploy: %v, want the context's cause alone", err)
	}
}

func TestACallThroughAHostToAnAPIThatNeverAnswersFailsInTime(t *testing.T) {
	release := make(chan struct{})
	silent := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { <-release }))
	defer silent.Close()
	defer close(release)
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: silent.Certificate().Raw})
	config := clientcmdapi.NewConfig()
	config.Clusters["c"] = &clientcmdapi.Cluster{Server: "https://127.0.0.1:6443", CertificateAuthorityData: ca}
	config.AuthInfos["u"] = &clientcmdapi.AuthInfo{Token: "t"}
	config.Contexts["x"] = &clientcmdapi.Context{Cluster: "c", AuthInfo: "u"}
	config.CurrentContext = "x"
	text, err := clientcmd.Write(*config)
	if err != nil {
		t.Fatal(err)
	}
	b := k3s.Through(hostTo{&controlPlane{kubeconfig: string(text)}, silent.Listener.Addr().String()}, k3s.Cluster{})
	done := make(chan error, 1)
	start := time.Now()
	go func() {
		_, err := b.Nodes(context.Background())
		done <- err
	}()
	select {
	case err := <-done:
		if took := time.Since(start); err == nil || took < 50*time.Second {
			t.Errorf("Nodes: %v after %s, want a failure after the minute a request has", err, took)
		}
	case <-time.After(90 * time.Second):
		t.Error("a call to an API that never answers hasn't failed after 90 seconds")
	}
}

// hostTo is a control plane whose connections reach address.
type hostTo struct {
	*controlPlane
	address string
}

func (h hostTo) DialTCP(ctx context.Context, _ string) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(ctx, "tcp", h.address)
}

func TestNodesAreTheClustersNodesOrderedByName(t *testing.T) {
	ctx := context.Background()
	id := unique("node")
	make := func(name string, labels map[string]string, unschedulable bool, status corev1.NodeStatus) {
		t.Helper()
		n := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels}, Spec: corev1.NodeSpec{Unschedulable: unschedulable}}
		created, err := admin.CoreV1().Nodes().Create(ctx, n, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { admin.CoreV1().Nodes().Delete(ctx, name, metav1.DeleteOptions{}) })
		err = retry(within(t, time.Minute), func() error {
			created.Status = status
			_, err := admin.CoreV1().Nodes().UpdateStatus(ctx, created, metav1.UpdateOptions{})
			if apierrors.IsConflict(err) {
				if latest, err := admin.CoreV1().Nodes().Get(ctx, name, metav1.GetOptions{}); err == nil {
					created = latest
				}
			}
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	status := func(ready corev1.ConditionStatus, arch, address, memory string) corev1.NodeStatus {
		return corev1.NodeStatus{
			Capacity:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse(memory)},
			Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: ready}},
			Addresses:  []corev1.NodeAddress{{Type: corev1.NodeHostName, Address: "host"}, {Type: corev1.NodeInternalIP, Address: address}, {Type: corev1.NodeInternalIP, Address: "fd00::1"}},
			NodeInfo:   corev1.NodeSystemInfo{Architecture: arch, KubeletVersion: "v1.37.2+k3s1"},
		}
	}
	make(id+"-z", map[string]string{"node-role.kubernetes.io/control-plane": "true"}, false, status(corev1.ConditionTrue, "amd64", "10.0.0.2", "2021376Ki"))
	make(id+"-a", nil, true, status(corev1.ConditionFalse, "arm64", "10.0.0.3", "2014832Ki"))
	make(id+"-g", nil, false, status(corev1.ConditionUnknown, "amd64", "10.0.0.4", "2Gi"))
	make(id+"-m", map[string]string{"node-role.kubernetes.io/control-plane": "false"}, false, corev1.NodeStatus{})
	all, err := deployer(t).Nodes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var got []model.NodeState
	for _, n := range all {
		if strings.HasPrefix(n.Name, id) {
			got = append(got, n)
		}
	}
	want := []model.NodeState{
		{Name: id + "-a", Address: "10.0.0.3", Architecture: "arm64", Version: "v1.37.2+k3s1", MemoryMiB: 1967, Ready: "no, cordoned"},
		{Name: id + "-g", Address: "10.0.0.4", Architecture: "amd64", Version: "v1.37.2+k3s1", MemoryMiB: 2048, Ready: "no"},
		{Name: id + "-m", Ready: "?"},
		{Name: id + "-z", Address: "10.0.0.2", Architecture: "amd64", Version: "v1.37.2+k3s1", MemoryMiB: 1974, ControlPlane: true, Ready: "yes"},
	}
	if !slices.Equal(got, want) {
		t.Errorf("Nodes:\n got %+v\nwant %+v", got, want)
	}
}

// failing is a backend whose request number n to the API, counted from
// 1, fails, and how many requests it has made.
func failing(t *testing.T, n int) (*k3s.Backend, *int) {
	t.Helper()
	return losing(t, n, false)
}

// losing is failing, with the request that fails sent first and its
// answer lost, if answered.
func losing(t *testing.T, n int, answered bool) (*k3s.Backend, *int) {
	t.Helper()
	requests := new(int)
	config := rest.CopyConfig(adminConfig)
	config.Wrap(func(rt http.RoundTripper) http.RoundTripper {
		return roundTripper(func(r *http.Request) (*http.Response, error) {
			*requests++
			if *requests == n {
				if answered {
					if res, err := rt.RoundTrip(r); err == nil {
						io.Copy(io.Discard, res.Body)
						res.Body.Close()
					}
				}
				return nil, errors.New("the connection is lost")
			}
			return rt.RoundTrip(r)
		})
	})
	b, err := k3s.New(config)
	if err != nil {
		t.Fatal(err)
	}
	return b, requests
}

// recordHasWhatIsApplied fails the test if the cluster has an object of
// the app that its record doesn't list.
func recordHasWhatIsApplied(t *testing.T, app, when string) {
	t.Helper()
	listed := record(t, app)
	for _, o := range applied(t, app) {
		if !slices.Contains(listed, o) {
			t.Errorf("%s: the cluster has %s, and the record lists only %q", when, o, listed)
		}
	}
}

// objectsOf are the objects a deploy of app leaves, as the record names
// them.
func objectsOf(app model.App) []string {
	var names []string
	if len(app.Secrets) > 0 {
		names = append(names, "Secret/"+app.Name)
	}
	for _, c := range app.Containers {
		names = append(names, "Deployment.apps/"+c.Name)
		if c.Port != 0 {
			names = append(names, "Service/"+c.Name)
		}
		if c.Host != "" {
			names = append(names, "Ingress.networking.k8s.io/"+c.Name)
		}
	}
	slices.Sort(names)
	return names
}

func TestADeployThatFailsAtAnyRequestLosesNoObjectFromTheRecordAndADeployAgainFinishes(t *testing.T) {
	for name, answered := range map[string]bool{"the request never sent": false, "its answer lost": true} {
		t.Run(name, func(t *testing.T) { deployFailingAtEachRequest(t, answered) })
	}
}

func deployFailingAtEachRequest(t *testing.T, answered bool) {
	name := unique("crash")
	first := shop(name)
	deploy(t, deployer(t), first)
	second := model.App{Name: name, Containers: []model.Container{
		{Name: "api", Image: "example.test/api:1", Port: 9000, Host: "api." + name + ".example.test", Replicas: 1},
		{Name: "worker", Image: "example.test/worker:2", Replicas: 1},
	}}
	for n := 1; ; n++ {
		if n > 200 {
			t.Fatal("a deploy made more than 200 requests")
		}
		b, requests := losing(t, n, answered)
		err := b.Deploy(within(t, time.Minute), second, func(orchestrator.Step) {})
		when := fmt.Sprintf("after a deploy whose request %d failed", n)
		recordHasWhatIsApplied(t, name, when)
		if *requests < n {
			if err != nil {
				t.Fatalf("Deploy with no request failed: %v", err)
			}
			if n < 10 {
				t.Errorf("the deploy made %d requests, too few to have applied anything", *requests)
			}
			break
		}
		if err == nil {
			t.Errorf("%s: Deploy succeeded", when)
		}
		// The app is the first again, as a deploy that works leaves it.
		deploy(t, deployer(t), first)
		if got, want := applied(t, name), objectsOf(first); !slices.Equal(got, want) {
			t.Fatalf("%s and a deploy of the app before: the cluster has %q, want %q", when, got, want)
		}
	}
	if got, want := applied(t, name), objectsOf(second); !slices.Equal(got, want) {
		t.Errorf("the cluster has %q, want %q", got, want)
	}
	if got, want := record(t, name), objectsOf(second); !slices.Equal(got, want) {
		t.Errorf("the record lists %q, want %q", got, want)
	}
}

// TestSequencesOfDeploysMatchAModel runs generated sequences of deploys
// of an app with other containers each time, some failing at a request,
// and checks after each that the record lists every object of the app
// on the cluster, and after each that succeeds that the cluster has the
// objects of the app deployed and no other.
func TestSequencesOfDeploysMatchAModel(t *testing.T) {
	for seed := range uint64(4) {
		rng := rand.New(rand.NewPCG(seed, 4))
		name := unique("model")
		cleared(t, name)
		kubelet(t, name)
		var history []string
		for range 6 {
			app := model.App{Name: name}
			for _, c := range []model.Container{
				{Name: "web", Image: "example.test/web:1", Port: 8080, Host: name + ".example.test", Replicas: 1},
				{Name: "api", Image: "example.test/api:1", Port: 9000, Replicas: 1},
				{Name: "worker", Image: "example.test/worker:1", Replicas: 1},
			} {
				if rng.IntN(2) == 0 {
					c.Image = fmt.Sprintf("%s-%d", c.Image, rng.IntN(3))
					app.Containers = append(app.Containers, c)
				}
			}
			if len(app.Containers) == 0 {
				app.Containers = []model.Container{{Name: "worker", Image: "example.test/worker:1", Replicas: 1}}
			}
			if rng.IntN(2) == 0 {
				app.Secrets = map[string]string{"A": fmt.Sprint(rng.IntN(3))}
			}
			fail := 0
			if rng.IntN(2) == 0 {
				fail = 1 + rng.IntN(30)
			}
			b, requests := failing(t, fail)
			err := b.Deploy(within(t, time.Minute), app, func(orchestrator.Step) {})
			history = append(history, fmt.Sprintf("deploy %q failing request %d: %v", objectsOf(app), fail, err))
			when := fmt.Sprintf("seed %d, after %s", seed, strings.Join(history, "; "))
			if failed := fail != 0 && *requests >= fail; failed != (err != nil) {
				t.Fatalf("%s: Deploy returned %v after %d requests", when, err, *requests)
			}
			recordHasWhatIsApplied(t, name, when)
			if err != nil {
				continue
			}
			if got, want := applied(t, name), objectsOf(app); !slices.Equal(got, want) {
				t.Fatalf("%s: the cluster has %q, want %q", when, got, want)
			}
			if got, want := record(t, name), objectsOf(app); !slices.Equal(got, want) {
				t.Fatalf("%s: the record lists %q, want %q", when, got, want)
			}
		}
	}
}

// controlPlane is a host that answers as the test cluster's control
// plane does: its kubeconfig is read with cat, and every connection
// made through it reaches the API.
type controlPlane struct {
	// kubeconfig is what the host's kubeconfig file holds.
	kubeconfig string
	// broken makes every connection through the host fail.
	broken bool

	mu    sync.Mutex
	runs  []string
	dials []string
}

func (h *controlPlane) Run(_ context.Context, cmd string, stdin io.Reader) (string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.runs = append(h.runs, cmd)
	if cmd != "cat /etc/rancher/k3s/k3s.yaml" || stdin != nil {
		return "", fmt.Errorf("exit 127: %s: not found", cmd)
	}
	if h.kubeconfig == "" {
		return "", errors.New("exit 1: cat: /etc/rancher/k3s/k3s.yaml: No such file or directory")
	}
	return h.kubeconfig, nil
}

func (h *controlPlane) RunStoppable(ctx context.Context, cmd string) (string, error) {
	return h.Run(ctx, cmd, nil)
}

func (h *controlPlane) Upload(context.Context, string, io.Reader, fs.FileMode) error {
	return errors.New("the test's control plane takes no files")
}

func (h *controlPlane) DialTCP(ctx context.Context, addr string) (net.Conn, error) {
	h.mu.Lock()
	h.dials = append(h.dials, addr)
	broken := h.broken
	h.mu.Unlock()
	if broken {
		return nil, errors.New("the connection to the host is lost")
	}
	api, err := url.Parse(adminConfig.Host)
	if err != nil {
		return nil, err
	}
	return (&net.Dialer{}).DialContext(ctx, "tcp", api.Host)
}

func TestThroughAHostTheBackendReadsTheKubeconfigOnceAndDialsTheAPIThroughIt(t *testing.T) {
	host := &controlPlane{kubeconfig: string(kubeconfig)}
	b := k3s.Through(host, k3s.Cluster{})
	if err := b.Check(shop("shop")); err != nil || len(host.runs) != 0 {
		t.Errorf("Check: %v, after the commands %q on the host, want none", err, host.runs)
	}
	name := unique("through")
	deploy(t, b, shop(name))
	if err := b.Certificate(within(t, 2*time.Second), name, "web"); err == nil {
		t.Error("Certificate with none asked for succeeded")
	}
	if _, err := b.Nodes(within(t, time.Minute)); err != nil {
		t.Errorf("Nodes: %v", err)
	}
	if want := []string{"cat /etc/rancher/k3s/k3s.yaml"}; !slices.Equal(host.runs, want) {
		t.Errorf("the commands run on the host were %q, want %q", host.runs, want)
	}
	if len(host.dials) == 0 || slices.ContainsFunc(host.dials, func(addr string) bool { return addr != "127.0.0.1:6443" }) {
		t.Errorf("the addresses dialed through the host were %q, want the API's on the host, 127.0.0.1:6443", host.dials)
	}
	if got := applied(t, name); !slices.Equal(got, objectsOf(shop(name))) {
		t.Errorf("the cluster has %q, want %q", got, objectsOf(shop(name)))
	}
}

func TestAFailedRequestPrintsNothing(t *testing.T) {
	out, err := os.CreateTemp(t.TempDir(), "stderr")
	if err != nil {
		t.Fatal(err)
	}
	stderr := os.Stderr
	os.Stderr = out
	defer func() { os.Stderr = stderr }()
	name := unique("quiet")
	cleared(t, name)
	for n := 1; n <= 12; n++ {
		b, _ := failing(t, n)
		if err := b.Deploy(within(t, time.Minute), manifestsOf(t, name, configMap), func(orchestrator.Step) {}); err == nil {
			t.Errorf("a deploy whose request %d failed succeeded", n)
		}
	}
	os.Stderr = stderr
	printed, err := os.ReadFile(out.Name())
	if err != nil {
		t.Fatal(err)
	}
	if len(printed) != 0 {
		t.Errorf("the failed deploys printed:\n%s", printed)
	}
}

// TestAKubeconfigMakesTheBackendRunNothingAndReadNoFile gives the
// backend kubeconfigs that name a command to run for a credential, a
// file to read a token from, and files for the client's key and
// certificate, each of this machine.
func TestAKubeconfigMakesTheBackendRunNothingAndReadNoFile(t *testing.T) {
	dir := t.TempDir()
	ran := filepath.Join(dir, "ran")
	plugin := filepath.Join(dir, "plugin")
	credential := `{"apiVersion":"client.authentication.k8s.io/v1","kind":"ExecCredential","status":{"token":"t"}}`
	if err := os.WriteFile(plugin, []byte("#!/bin/sh\ntouch "+ran+"\necho '"+credential+"'\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	token := filepath.Join(dir, "token")
	if err := os.WriteFile(token, []byte(reader.BearerToken), 0o600); err != nil {
		t.Fatal(err)
	}
	own, err := clientcmd.Load(kubeconfig)
	if err != nil {
		t.Fatal(err)
	}
	user := own.AuthInfos[own.Contexts[own.CurrentContext].AuthInfo]
	keyFile, certFile := filepath.Join(dir, "key"), filepath.Join(dir, "cert")
	if err := os.WriteFile(keyFile, user.ClientKeyData, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(certFile, user.ClientCertificateData, 0o600); err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*clientcmdapi.AuthInfo){
		"a command to run": func(u *clientcmdapi.AuthInfo) {
			u.Exec = &clientcmdapi.ExecConfig{APIVersion: "client.authentication.k8s.io/v1", Command: plugin, InteractiveMode: clientcmdapi.NeverExecInteractiveMode}
		},
		"a file to read a token from": func(u *clientcmdapi.AuthInfo) { u.TokenFile = token },
		"files of the client's key and certificate": func(u *clientcmdapi.AuthInfo) {
			u.ClientKey, u.ClientCertificate = keyFile, certFile
		},
	} {
		config := own.DeepCopy()
		u := config.AuthInfos[config.Contexts[config.CurrentContext].AuthInfo]
		u.ClientKeyData, u.ClientCertificateData = nil, nil
		change(u)
		text, err := clientcmd.Write(*config)
		if err != nil {
			t.Fatal(err)
		}
		host := &controlPlane{kubeconfig: string(text)}
		// The token in the file is one that may list pods.
		_, err = k3s.Through(host, k3s.Cluster{}).Running(within(t, 30*time.Second))
		if err == nil {
			t.Errorf("%s: the backend listed pods with a credential the kubeconfig doesn't hold", name)
		}
		if _, err := os.Stat(ran); err == nil {
			t.Fatalf("%s: the backend ran the kubeconfig's command on this machine", name)
		}
	}
}

func TestCallsAtOnceThroughOneHostReadTheKubeconfigOnce(t *testing.T) {
	host := &controlPlane{kubeconfig: string(kubeconfig)}
	b := k3s.Through(host, k3s.Cluster{})
	var calls sync.WaitGroup
	for range 8 {
		calls.Go(func() {
			if _, err := b.Nodes(within(t, time.Minute)); err != nil {
				t.Errorf("Nodes: %v", err)
			}
		})
	}
	calls.Wait()
	host.mu.Lock()
	defer host.mu.Unlock()
	if len(host.runs) != 1 {
		t.Errorf("the kubeconfig was read %d times, want once", len(host.runs))
	}
}

func TestWithoutAHostEveryCallThatNeedsTheAPIFails(t *testing.T) {
	b := k3s.Through(nil, k3s.Cluster{})
	ctx := within(t, time.Minute)
	_, nodes := b.Nodes(ctx)
	_, running := b.Running(ctx)
	for method, err := range map[string]error{
		"Deploy":      b.Deploy(ctx, shop("shop"), func(orchestrator.Step) {}),
		"Certificate": b.Certificate(ctx, "shop", "web"),
		"Nodes":       nodes,
		"Running":     running,
	} {
		if want := "the cluster has no control plane to reach it through"; err == nil || err.Error() != want {
			t.Errorf("%s: %v, want %q", method, err, want)
		}
	}
}

// secrets are the values of the kubeconfig that give access to the
// cluster.
func secrets(t *testing.T) []string {
	t.Helper()
	var out []string
	for _, line := range strings.Split(string(kubeconfig), "\n") {
		key, value, _ := strings.Cut(strings.TrimSpace(line), ": ")
		switch key {
		case "client-key-data", "client-certificate-data", "token", "password":
			out = append(out, value)
		}
	}
	if len(out) < 2 {
		t.Fatalf("the kubeconfig holds %d secrets, want its client's key and certificate", len(out))
	}
	return out
}

func TestTheKubeconfigIsInNoErrorAndNoFile(t *testing.T) {
	home := t.TempDir()
	for _, v := range []string{"HOME", "TMPDIR", "XDG_CACHE_HOME", "XDG_CONFIG_HOME", "KUBECACHEDIR"} {
		t.Setenv(v, home)
	}
	t.Chdir(home)
	name := unique("leak")
	cleared(t, name)
	step := func(orchestrator.Step) {}
	short := func() context.Context { return within(t, 4*time.Second) }
	garbled := strings.Replace(string(kubeconfig), "client-key-data: ", "client-key-data: !!", 1)
	if garbled == string(kubeconfig) {
		t.Fatal("the kubeconfig has no client-key-data")
	}
	errs := map[string]error{
		"no kubeconfig on the host":       k3s.Through(&controlPlane{}, k3s.Cluster{}).Deploy(short(), shop(name), step),
		"a kubeconfig that can't be read": k3s.Through(&controlPlane{kubeconfig: garbled}, k3s.Cluster{}).Deploy(short(), shop(name), step),
		"a kubeconfig cut short":          k3s.Through(&controlPlane{kubeconfig: string(kubeconfig[:len(kubeconfig)/2])}, k3s.Cluster{}).Deploy(short(), shop(name), step),
		"a host that connects nowhere":    k3s.Through(&controlPlane{kubeconfig: string(kubeconfig), broken: true}, k3s.Cluster{}).Deploy(short(), shop(name), step),
	}
	b := k3s.Through(&controlPlane{kubeconfig: string(kubeconfig)}, k3s.Cluster{})
	errs["a kind that isn't kept in a namespace"] = b.Deploy(short(), manifestsOf(t, name, "apiVersion: v1\nkind: Namespace\nmetadata: {name: "+name+"}"), step)
	errs["an object the API refuses"] = b.Deploy(short(), manifestsOf(t, name, "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: c}\ndata: {a: [b]}"), step)
	errs["a rollout that isn't ready in time"] = b.Deploy(short(), shop(name), step)
	errs["a chart that isn't installed in time"] = b.Deploy(short(), chartOf(t, name, "one"), step)
	errs["a certificate that isn't issued in time"] = b.Certificate(short(), name, "web")
	lost := &controlPlane{kubeconfig: string(kubeconfig)}
	c := k3s.Through(lost, k3s.Cluster{})
	if _, err := c.Nodes(short()); err != nil {
		t.Fatal(err)
	}
	lost.mu.Lock()
	lost.kubeconfig, lost.broken = "", true
	lost.mu.Unlock()
	errs["a connection lost after it was made"] = c.Deploy(short(), shop(unique("lost")), step)

	for when, err := range errs {
		if err == nil {
			t.Errorf("%s: the call succeeded", when)
			continue
		}
		for _, secret := range secrets(t) {
			if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), secret[:24]) {
				t.Errorf("%s: the error holds a secret of the kubeconfig: %v", when, err)
			}
		}
	}
	for when, want := range map[string]string{
		"no kubeconfig on the host":       "reading the control plane's /etc/rancher/k3s/k3s.yaml: exit 1: cat: /etc/rancher/k3s/k3s.yaml: No such file or directory",
		"a kubeconfig that can't be read": "the control plane's /etc/rancher/k3s/k3s.yaml isn't a kubeconfig",
		"a kubeconfig cut short":          "the control plane's /etc/rancher/k3s/k3s.yaml isn't a kubeconfig",
	} {
		if err := errs[when]; err != nil && err.Error() != want {
			t.Errorf("%s: %v, want %q", when, err, want)
		}
	}
	var written []string
	fs.WalkDir(os.DirFS(home), ".", func(path string, d fs.DirEntry, err error) error {
		if path != "." {
			written = append(written, path)
		}
		return nil
	})
	if len(written) != 0 {
		t.Errorf("the calls wrote the files %q", written)
	}
}

// held makes a ConfigMap in the app's namespace that stays, though
// deleted, until the test ends or release is called.
func held(t *testing.T, app string) (release func()) {
	t.Helper()
	maps := admin.CoreV1().ConfigMaps(app)
	m := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "held", Finalizers: []string{"example.test/held"}}}
	if _, err := maps.Create(context.Background(), m, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	release = sync.OnceFunc(func() {
		maps.Patch(context.Background(), "held", types.MergePatchType, []byte(`{"metadata":{"finalizers":null}}`), metav1.PatchOptions{})
	})
	t.Cleanup(release)
	return release
}

// charts are the names of the HelmCharts of the app called app.
func charts(t *testing.T, app string) []string {
	t.Helper()
	list, err := objects.Resource(helmCharts).Namespace("kube-system").List(context.Background(), metav1.ListOptions{LabelSelector: appLabel + "=" + app})
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, c := range list.Items {
		names = append(names, c.GetName())
	}
	return names
}

// terminating reports whether the namespace called name is being
// deleted.
func terminating(t *testing.T, name string) bool {
	t.Helper()
	ns, err := admin.CoreV1().Namespaces().Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return ns.DeletionTimestamp != nil
}

// remove starts a removal of the app that has two minutes, and returns
// where it reports.
func remove(t *testing.T, b *k3s.Backend, app string) <-chan error {
	done := make(chan error, 1)
	go func() { done <- b.Remove(within(t, 2*time.Minute), app) }()
	return done
}

func TestARemovalOfAnAppOfContainersReturnsOnceNothingOfItIsLeft(t *testing.T) {
	b := deployer(t)
	name, other := unique("removed"), unique("kept")
	deploy(t, b, shop(name))
	deploy(t, b, shop(other))
	release := held(t, name)
	done := remove(t, b, name)
	blocked(t, done, 3*time.Second, "with an object of the app still there")
	if !terminating(t, name) {
		t.Error("the app's namespace isn't being deleted")
	}
	if _, ok := appNamed(t, b, name); !ok {
		t.Errorf("Apps lacks %s while it is being removed", name)
	}
	release()
	if err := returned(t, done); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	absent(t, name)
	if terminating(t, other) || !slices.Equal(applied(t, other), objectsOf(shop(other))) {
		t.Errorf("removing %s left of %s: %q", name, other, applied(t, other))
	}
}

func TestARemovalOfAnAppOfAChartRemovesItsChartAndThenItsNamespace(t *testing.T) {
	b := deployer(t)
	name := unique("unchart")
	cleared(t, name)
	stop := helm(t, name, corev1.PodSucceeded)
	if err := b.Deploy(within(t, 2*time.Minute), chartOf(t, name, "one"), func(orchestrator.Step) {}); err != nil {
		t.Fatalf("Deploy: %v", err)
	}
	stop()
	done := remove(t, b, name)
	blocked(t, done, 4*time.Second, "with the chart's uninstall not ended")
	if terminating(t, name) {
		t.Error("the app's namespace is being deleted with its chart still installed")
	}
	helm(t, name, corev1.PodSucceeded)
	if err := returned(t, done); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	absent(t, name)
	if left := charts(t, name); len(left) != 0 {
		t.Errorf("the removal left the HelmCharts %q", left)
	}
}

func TestARemovalOfAnAppOfManifestsLeavesNothing(t *testing.T) {
	b := deployer(t)
	name := unique("unmanifest")
	deploy(t, b, manifestsOf(t, name, deployment, "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings}\n"))
	if err := b.Remove(within(t, time.Minute), name); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	absent(t, name)
}

func TestRemoveOfAnAppTheClusterDoesntHaveIsErrNoAppAndRemovesNothing(t *testing.T) {
	b := deployer(t)
	if err := b.Remove(within(t, time.Minute), unique("absent")); !errors.Is(err, orchestrator.ErrNoApp) {
		t.Errorf("Remove of an app never deployed: %v, want ErrNoApp", err)
	}
	// A namespace that isn't an app's, one of another app's, and the
	// chart of an app whose namespace isn't its own.
	plain, other := unique("plain"), unique("other")
	cleared(t, plain)
	cleared(t, other)
	createNamespace(t, plain)
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: other, Labels: map[string]string{appLabel: "shop"}}}
	if _, err := admin.CoreV1().Namespaces().Create(context.Background(), ns, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	chart := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "helm.cattle.io/v1",
		"kind":       "HelmChart",
		"metadata":   map[string]any{"name": "app-" + plain, "namespace": "kube-system", "labels": map[string]any{appLabel: plain}},
		"spec":       map[string]any{"chart": "none", "targetNamespace": plain},
	}}
	if _, err := objects.Resource(helmCharts).Namespace("kube-system").Create(context.Background(), chart, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{plain, other, "kube-system"} {
		if err := b.Remove(within(t, time.Minute), name); !errors.Is(err, orchestrator.ErrNoApp) {
			t.Errorf("Remove of %s, a namespace that isn't the app's: %v, want ErrNoApp", name, err)
		}
		if terminating(t, name) {
			t.Errorf("the namespace %s, which isn't an app's, is being deleted", name)
		}
	}
	if left := charts(t, plain); len(left) != 1 {
		t.Errorf("the HelmCharts labelled as %s's are %q, want the one left", plain, left)
	}
}

func TestARemovalThatFailsAtAnyRequestAndIsRunAgainEndsWithTheAppGone(t *testing.T) {
	for kind, app := range map[string]func(t *testing.T, name string) model.App{
		"containers": func(_ *testing.T, name string) model.App { return shop(name) },
		"chart":      func(t *testing.T, name string) model.App { return chartOf(t, name, "one") },
	} {
		for how, answered := range map[string]bool{"the request never sent": false, "its answer lost": true} {
			t.Run(kind+", "+how, func(t *testing.T) {
				t.Parallel()
				for n := 1; ; n++ {
					if n > 50 {
						t.Fatal("a removal made more than 50 requests")
					}
					name := unique("crash")
					cleared(t, name)
					kubelet(t, name)
					helm(t, name, corev1.PodSucceeded)
					if err := deployer(t).Deploy(within(t, 2*time.Minute), app(t, name), func(orchestrator.Step) {}); err != nil {
						t.Fatalf("Deploy: %v", err)
					}
					b, requests := losing(t, n, answered)
					err := b.Remove(within(t, 2*time.Minute), name)
					if *requests < n {
						if err != nil {
							t.Fatalf("Remove with no request failed: %v", err)
						}
						if n < 4 {
							t.Errorf("the removal made %d requests, too few to have removed anything", *requests)
						}
						absent(t, name)
						return
					}
					if err == nil {
						t.Errorf("Remove whose request %d failed succeeded", n)
					}
					// Only a removal that got as far as the namespace may
					// find no app when it is run again.
					before := len(charts(t, name))
					err = deployer(t).Remove(within(t, 2*time.Minute), name)
					if err != nil && !(errors.Is(err, orchestrator.ErrNoApp) && before == 0) {
						t.Fatalf("Remove again after request %d failed: %v, with %d charts of the app left", n, err, before)
					}
					absent(t, name)
					if left := charts(t, name); len(left) != 0 {
						t.Errorf("after request %d failed and a removal again, the HelmCharts %q are left", n, left)
					}
				}
			})
		}
	}
}

func TestARemovalInterruptedWhileItWaitsFailsWithTheCauseAndRunAgainFinishes(t *testing.T) {
	b := deployer(t)
	name := unique("cutremoval")
	deploy(t, b, shop(name))
	release := held(t, name)
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("interrupted")
	done := make(chan error, 1)
	go func() { done <- b.Remove(ctx, name) }()
	blocked(t, done, 3*time.Second, "with an object of the app still there")
	cancel(cause)
	if err := returned(t, done); err == nil || err.Error() != cause.Error() {
		t.Errorf("Remove: %v, want the context's cause alone", err)
	}
	// The app is still there for the removal run again, which waits too.
	again := remove(t, b, name)
	blocked(t, again, 3*time.Second, "with an object of the app still there")
	release()
	if err := returned(t, again); err != nil {
		t.Errorf("Remove again: %v", err)
	}
	absent(t, name)
}

func TestARemovalThatIsntDoneInTimeSaysTheAppIsStillThere(t *testing.T) {
	b := deployer(t)
	name := unique("slowremoval")
	deploy(t, b, shop(name))
	held(t, name)
	err := b.Remove(within(t, 4*time.Second), name)
	if want := "removing " + name + ": it is still there"; err == nil || err.Error() != want {
		t.Errorf("Remove: %v, want %q", err, want)
	}
}

func TestARemovalOnAnEndedContextFailsAndRemovesNothing(t *testing.T) {
	b := deployer(t)
	name := unique("kept")
	deploy(t, b, shop(name))
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("the operation lock is lost")
	cancel(cause)
	if err := b.Remove(ctx, name); !errors.Is(err, cause) {
		t.Errorf("Remove on an ended context: %v, want its cause", err)
	}
	if terminating(t, name) {
		t.Error("a removal on an ended context deleted the app's namespace")
	}
}

// appNamed is the app called name among the cluster's apps.
func appNamed(t *testing.T, b *k3s.Backend, name string) (model.AppState, bool) {
	t.Helper()
	apps, err := b.Apps(within(t, time.Minute))
	if err != nil {
		t.Fatalf("Apps: %v", err)
	}
	if !slices.IsSortedFunc(apps, func(x, y model.AppState) int { return strings.Compare(x.Name, y.Name) }) {
		t.Errorf("Apps isn't ordered by name: %+v", apps)
	}
	i := slices.IndexFunc(apps, func(a model.AppState) bool { return a.Name == name })
	if i < 0 {
		return model.AppState{}, false
	}
	return apps[i], true
}

func TestAppsAreTheClustersAppsWithTheirReplicasImagesAndHosts(t *testing.T) {
	b := deployer(t)
	name := unique("state")
	cleared(t, name)
	if _, ok := appNamed(t, b, name); ok {
		t.Fatalf("Apps has %s before its deploy", name)
	}
	// With no pod ready, the deploy doesn't end.
	app := shop(name)
	app.Containers = append(app.Containers, model.Container{Name: "cron", Image: "example.test/worker:1", Replicas: 1})
	if err := b.Deploy(within(t, 3*time.Second), app, func(orchestrator.Step) {}); err == nil {
		t.Fatal("Deploy succeeded with no pod ready")
	}
	want := model.AppState{
		Name: name, Ready: 0, Want: 4,
		Images: []string{"example.test/worker:1", "example.test/web:1"},
		Hosts:  []string{name + ".example.test"},
	}
	if got, _ := appNamed(t, b, name); !reflect.DeepEqual(got, want) {
		t.Errorf("the app with no pod ready is\n%+v, want\n%+v", got, want)
	}
	deploy(t, b, app)
	want.Ready = 4
	if got, _ := appNamed(t, b, name); !reflect.DeepEqual(got, want) {
		t.Errorf("the app deployed is\n%+v, want\n%+v", got, want)
	}
	// What isn't an app's is none: the cluster's own namespaces.
	for _, own := range []string{"kube-system", "cert-manager", "default"} {
		if _, ok := appNamed(t, b, own); ok {
			t.Errorf("Apps has %s, a namespace of the cluster's own", own)
		}
	}
	if err := b.Remove(within(t, time.Minute), name); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if _, ok := appNamed(t, b, name); ok {
		t.Errorf("Apps has %s after its removal", name)
	}
}

func TestAnAppOfManifestsHasTheReplicasOfItsStatefulSetsAndDaemonSetsToo(t *testing.T) {
	b := deployer(t)
	name := unique("sets")
	deploy(t, b, manifestsOf(t, name, deployment, statefulSet, daemonSet))
	got, ok := appNamed(t, b, name)
	if !ok {
		t.Fatalf("Apps lacks %s", name)
	}
	// Two replicas of the Deployment, two of the StatefulSet, and the
	// DaemonSet's on the cluster's one node.
	want := model.AppState{Name: name, Ready: 5, Want: 5, Images: []string{"example.test/web", "example.test/db", "example.test/agent"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("the app is\n%+v, want\n%+v", got, want)
	}
}

func TestAppsAndRunningOnAnEndedContextFailWithItsCause(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("interrupted")
	cancel(cause)
	if _, err := deployer(t).Apps(ctx); err == nil || err.Error() != cause.Error() {
		t.Errorf("Apps on an ended context: %v, want its cause alone", err)
	}
	if _, err := deployer(t).Running(ctx); err == nil || err.Error() != cause.Error() {
		t.Errorf("Running on an ended context: %v, want its cause alone", err)
	}
}

func TestOnlyANamespaceNamedAsItsLabelSaysIsAnApp(t *testing.T) {
	b := deployer(t)
	name, other := unique("named"), unique("mislabelled")
	deploy(t, b, shop(name))
	cleared(t, other)
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: other, Labels: map[string]string{appLabel: name}}}
	if _, err := admin.CoreV1().Namespaces().Create(context.Background(), ns, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	apps, err := b.Apps(within(t, time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	var got []model.AppState
	for _, a := range apps {
		if a.Name == name || a.Name == other {
			got = append(got, a)
		}
	}
	if len(got) != 1 || got[0].Name != name || got[0].Want != 3 {
		t.Errorf("of %s and the namespace %s labelled as it, Apps has %+v, want the app alone, once", name, other, got)
	}
	// Each app Apps lists, Remove finds.
	if err := b.Remove(within(t, time.Minute), name); err != nil {
		t.Errorf("Remove of an app Apps lists: %v", err)
	}
}

func TestAnAppsStateLeavesOutInitContainersRulesWithoutAHostAndCountsWhatIsntReady(t *testing.T) {
	b := deployer(t)
	name := unique("parts")
	cleared(t, name)
	app := manifestsOf(t, name, `apiVersion: apps/v1
kind: Deployment
metadata: {name: web}
spec:
  replicas: 0
  selector: {matchLabels: {app: web}}
  template:
    metadata: {labels: {app: web}}
    spec:
      initContainers: [{name: migrate, image: example.test/migrate}]
      containers: [{name: web, image: example.test/web}]`, daemonSet, `apiVersion: networking.k8s.io/v1
kind: Ingress
metadata: {name: web}
spec:
  rules:
    - host: `+name+`.example.test
      http: {paths: [{path: /, pathType: Prefix, backend: {service: {name: web, port: {number: 80}}}}]}
    - http: {paths: [{path: /any, pathType: Prefix, backend: {service: {name: web, port: {number: 80}}}}]}`)
	// With no pod ready, the deploy doesn't end.
	if err := b.Deploy(within(t, 3*time.Second), app, func(orchestrator.Step) {}); err == nil {
		t.Fatal("Deploy succeeded with no pod ready")
	}
	want := model.AppState{Name: name, Ready: 0, Want: 1, Images: []string{"example.test/web", "example.test/agent"}, Hosts: []string{name + ".example.test"}}
	if got, _ := appNamed(t, b, name); !reflect.DeepEqual(got, want) {
		t.Errorf("the app is\n%+v, want\n%+v", got, want)
	}
}

// cutting is a backend whose context cancel ends with cause at its
// request number n to the API, counted from 1.
func cutting(t *testing.T, n int, cancel context.CancelCauseFunc, cause error) *k3s.Backend {
	t.Helper()
	requests := 0
	config := rest.CopyConfig(adminConfig)
	config.Wrap(func(rt http.RoundTripper) http.RoundTripper {
		return roundTripper(func(r *http.Request) (*http.Response, error) {
			if requests++; requests == n {
				cancel(cause)
			}
			return rt.RoundTrip(r)
		})
	})
	b, err := k3s.New(config)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestAReadOfTheClustersStateWhoseContextEndsAtARequestFailsWithTheCause(t *testing.T) {
	cause := errors.New("the operation lock is lost")
	for name, read := range map[string]func(b *k3s.Backend, ctx context.Context) error{
		"Apps":    func(b *k3s.Backend, ctx context.Context) error { _, err := b.Apps(ctx); return err },
		"Nodes":   func(b *k3s.Backend, ctx context.Context) error { _, err := b.Nodes(ctx); return err },
		"Running": func(b *k3s.Backend, ctx context.Context) error { _, err := b.Running(ctx); return err },
	} {
		for n := 1; n <= 2; n++ {
			if name != "Apps" && n > 1 {
				continue
			}
			ctx, cancel := context.WithCancelCause(context.Background())
			if err := read(cutting(t, n, cancel, cause), ctx); err == nil || err.Error() != cause.Error() {
				t.Errorf("%s whose context ended at its request %d: %v, want the cause alone", name, n, err)
			}
		}
	}
}

func TestARemovalWhoseChartIsntGoneInTimeSaysSoAndLeavesTheNamespace(t *testing.T) {
	b := deployer(t)
	name := unique("heldchart")
	cleared(t, name)
	stop := helm(t, name, corev1.PodSucceeded)
	if err := b.Deploy(within(t, 2*time.Minute), chartOf(t, name, "one"), func(orchestrator.Step) {}); err != nil {
		t.Fatalf("Deploy: %v", err)
	}
	stop()
	err := b.Remove(within(t, 4*time.Second), name)
	if want := "removing " + name + "'s chart: it is still there"; err == nil || err.Error() != want {
		t.Errorf("Remove: %v, want %q", err, want)
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("interrupted")
	done := make(chan error, 1)
	go func() { done <- b.Remove(ctx, name) }()
	blocked(t, done, 3*time.Second, "with the chart's uninstall not ended")
	cancel(cause)
	if err := returned(t, done); err == nil || err.Error() != cause.Error() {
		t.Errorf("Remove interrupted while it waits for the chart: %v, want the cause alone", err)
	}
	if terminating(t, name) {
		t.Error("the app's namespace is being deleted with its chart still installed")
	}
}

func TestAnOperationThatCantReadTheAppSaysItLookedForIt(t *testing.T) {
	name := unique("unread")
	deploy(t, deployer(t), shop(name))
	for operation, call := range map[string]func(b *k3s.Backend, ctx context.Context) error{
		"Remove":      func(b *k3s.Backend, ctx context.Context) error { return b.Remove(ctx, name) },
		"Certificate": func(b *k3s.Backend, ctx context.Context) error { return b.Certificate(ctx, name, "web") },
		"Logs": func(b *k3s.Backend, ctx context.Context) error {
			return b.Logs(ctx, name, orchestrator.LogOptions{Tail: 1}, io.Discard)
		},
	} {
		b, _ := failing(t, 1)
		err := call(b, within(t, time.Minute))
		if err == nil || !strings.HasPrefix(err.Error(), "looking for "+name+": ") || !strings.HasSuffix(err.Error(), "the connection is lost") {
			t.Errorf("%s whose first request failed: %v, want it to say it looked for the app, and why", operation, err)
		}
	}
	if terminating(t, name) {
		t.Error("an operation that couldn't read the app removed it")
	}
}

func TestADeployOfAnAppBeingRemovedIsRefusedUntilItIsGone(t *testing.T) {
	b := deployer(t)
	refused := func(t *testing.T, app model.App) {
		t.Helper()
		var steps []orchestrator.Step
		err := b.Deploy(within(t, time.Minute), app, func(s orchestrator.Step) { steps = append(steps, s) })
		if want := app.Name + " is being removed; remove it again, then deploy it"; err == nil || err.Error() != want {
			t.Errorf("Deploy of an app being removed: %v, want %q", err, want)
		}
		if slices.Contains(steps, orchestrator.WaitingForReady) {
			t.Errorf("the refused deploy told the steps %v", steps)
		}
	}
	t.Run("containers", func(t *testing.T) {
		name := unique("leaving")
		deploy(t, b, shop(name))
		release := held(t, name)
		if err := b.Remove(within(t, 3*time.Second), name); err == nil {
			t.Fatal("Remove succeeded with an object of the app held")
		}
		before := record(t, name)
		changed := shop(name)
		changed.Containers = changed.Containers[:1]
		refused(t, changed)
		if got := record(t, name); !slices.Equal(got, before) {
			t.Errorf("the refused deploy changed the record from %q to %q", before, got)
		}
		release()
		if err := b.Remove(within(t, time.Minute), name); err != nil && !errors.Is(err, orchestrator.ErrNoApp) {
			t.Fatalf("Remove again: %v", err)
		}
		deploy(t, b, changed)
		if terminating(t, name) {
			t.Error("the app deployed after its removal is being removed")
		}
	})
	t.Run("its chart alone", func(t *testing.T) {
		name := unique("leavingchart")
		cleared(t, name)
		stop := helm(t, name, corev1.PodSucceeded)
		app := chartOf(t, name, "one")
		if err := b.Deploy(within(t, 2*time.Minute), app, func(orchestrator.Step) {}); err != nil {
			t.Fatalf("Deploy: %v", err)
		}
		kept := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "kept"}}
		if _, err := admin.CoreV1().ConfigMaps(name).Create(context.Background(), kept, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
		first := installJob(t, name)
		stop()
		// A deploy of the app without its chart deletes the chart, and
		// is cut off while the chart is uninstalled.
		err := b.Deploy(within(t, 3*time.Second), shop(name), func(orchestrator.Step) {})
		if want := "removing " + name + "'s chart: it is still there"; err == nil || err.Error() != want {
			t.Fatalf("Deploy of the app without its chart, with the uninstall not ended: %v, want %q", err, want)
		}
		err = b.Deploy(within(t, 3*time.Second), app, func(orchestrator.Step) {})
		if want := name + "'s chart of before is still being uninstalled"; err == nil || err.Error() != want {
			t.Errorf("Deploy of the chart with the one before not uninstalled in time: %v, want %q", err, want)
		}
		done := make(chan error, 1)
		go func() { done <- b.Deploy(within(t, 2*time.Minute), app, func(orchestrator.Step) {}) }()
		blocked(t, done, 3*time.Second, "with the chart of before still being uninstalled")
		helm(t, name, corev1.PodSucceeded)
		if err := returned(t, done); err != nil {
			t.Fatalf("Deploy of the chart once the one before is uninstalled: %v", err)
		}
		chart, err := objects.Resource(helmCharts).Namespace("kube-system").Get(context.Background(), "app-"+name, metav1.GetOptions{})
		if err != nil || chart.GetDeletionTimestamp() != nil {
			t.Errorf("the HelmChart after the deploy: %v, deleted at %v, want a new one", err, chart.GetDeletionTimestamp())
		}
		if again := installJob(t, name); again == first {
			t.Error("the deploy returned on the install job of the chart before")
		}
		if _, err := admin.CoreV1().ConfigMaps(name).Get(context.Background(), "kept", metav1.GetOptions{}); err != nil || terminating(t, name) {
			t.Errorf("what the namespace held is gone, or the namespace is being deleted: %v", err)
		}
	})
}
