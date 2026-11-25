package k3s

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/dynamic"

	"github.com/moostackhq/moostack/internal/model"
	"github.com/moostackhq/moostack/internal/orchestrator"
)

var (
	certificates = schema.GroupVersionResource{Group: "cert-manager.io", Version: "v1", Resource: "certificates"}
	helmCharts   = schema.GroupVersionResource{Group: "helm.cattle.io", Version: "v1", Resource: "helmcharts"}
)

const (
	// fieldManager is the name objects are applied under.
	fieldManager = "mooctl"
	// rolloutTimeout bounds waiting for an app's replicas, for its
	// chart's install, for the removal of its chart and for the removal
	// of its namespace.
	rolloutTimeout = 5 * time.Minute
	// certificateTimeout bounds waiting for a container's certificate.
	certificateTimeout = 2 * time.Minute
	// chartFailures is how many failed pods of a chart's install job
	// fail the install.
	chartFailures = 3
)

// Deploy refuses app as Check does. It waits for the cluster's own
// services, telling WaitingForCluster if they aren't ready. Then it
// applies app's objects server side: its namespace and HelmChart, then
// the objects of its namespace, which it writes to the record before
// it applies them. It deletes the HelmChart of an app without a chart,
// and the recorded objects app no longer has, but for its
// PersistentVolumeClaims, which it leaves. It refuses, with nothing
// applied, an object of app's manifests whose kind isn't kept in a
// namespace or that the cluster doesn't have, an app whose namespace
// is there and isn't an app's, and one whose namespace is being
// deleted. Before it reads or applies the chart of an app that has
// one, it waits until no HelmChart of the app is being deleted. Last it
// waits until the chart is installed and every
// Deployment, StatefulSet and DaemonSet of the namespace runs as
// applied, for rolloutTimeout or until ctx's deadline, and then fails
// with the namespace's pods and last events. If ctx ends otherwise, it
// fails with ctx's cause.
func (b *Backend) Deploy(ctx context.Context, app model.App, step func(orchestrator.Step)) error {
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	return causeIfEnded(ctx, b.deploy(ctx, app, step))
}

// causeIfEnded is err, or ctx's cause if err is the error of a call
// that failed once ctx had ended other than by its deadline.
func causeIfEnded(ctx context.Context, err error) error {
	if err != nil && ctx.Err() != nil && !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return context.Cause(ctx)
	}
	return err
}

func (b *Backend) deploy(ctx context.Context, app model.App, step func(orchestrator.Step)) error {
	objs, err := objectsOf(app)
	if err != nil {
		return err
	}
	cluster, err := parse(objs.cluster)
	if err != nil {
		return err
	}
	own, err := parse(objs.own)
	if err != nil {
		return err
	}
	a, err := b.connect(ctx)
	if err != nil {
		return err
	}
	missing, err := a.missingServices(ctx)
	if err != nil {
		return err
	}
	if len(missing) > 0 {
		step(orchestrator.WaitingForCluster)
		if err := a.waitServices(ctx); err != nil {
			return err
		}
	}
	var before chartState
	if app.Chart != "" {
		if err := a.waitUninstalled(ctx, app.Name); err != nil {
			return err
		}
		if before, err = a.chartState(ctx, app.Name); err != nil {
			return fmt.Errorf("reading %s's chart: %w", app.Name, err)
		}
	}
	step(orchestrator.Applying)
	if err := a.apply(ctx, app, cluster, own); err != nil {
		return err
	}
	step(orchestrator.WaitingForReady)
	if app.Chart != "" {
		if err := a.waitChart(ctx, app.Name, before); err != nil {
			return fmt.Errorf("%s isn't ready: %w", app.Name, err)
		}
	}
	if err := a.waitRollout(ctx, namespaceOf(app.Name)); err != nil {
		return fmt.Errorf("%s isn't ready: %w", app.Name, err)
	}
	return nil
}

// parse is the objects of manifests.
func parse(manifests string) ([]*unstructured.Unstructured, error) {
	var objs []*unstructured.Unstructured
	dec := yaml.NewYAMLOrJSONDecoder(strings.NewReader(manifests), 4096)
	for {
		var doc map[string]any
		err := dec.Decode(&doc)
		if errors.Is(err, io.EOF) {
			return objs, nil
		}
		if err != nil {
			return nil, err
		}
		if len(doc) == 0 {
			continue
		}
		objs = append(objs, &unstructured.Unstructured{Object: doc})
	}
}

// kindAndName is o's kind in lower case and its name, as deployment/web.
func kindAndName(o *unstructured.Unstructured) string {
	return strings.ToLower(o.GetKind()) + "/" + o.GetName()
}

// recorded is o's entry in the record: its kind, its API group if it
// has one, and its name, as Deployment.apps/web.
func recorded(o *unstructured.Unstructured) string {
	return o.GroupVersionKind().GroupKind().String() + "/" + o.GetName()
}

// apply applies app's objects: cluster, each as it is, then own in the
// app's namespace, and deletes what the app no longer has.
func (a *api) apply(ctx context.Context, app model.App, cluster, own []*unstructured.Unstructured) error {
	ns := namespaceOf(app.Name)
	var planned []string
	for _, o := range own {
		mapping, err := a.mapping(o.GroupVersionKind().GroupKind(), o.GroupVersionKind().Version)
		if err != nil {
			return fmt.Errorf("%s: %w", kindAndName(o), err)
		}
		if mapping.Scope.Name() != meta.RESTScopeNameNamespace {
			return fmt.Errorf("%s isn't in a namespace; an app's manifests hold only objects of its namespace", kindAndName(o))
		}
		o.SetNamespace(ns)
		planned = append(planned, recorded(o))
	}
	existing, err := a.client.CoreV1().Namespaces().Get(ctx, ns, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("reading %s's namespace: %w", app.Name, err)
	}
	if err == nil && existing.Labels[appLabel] != app.Name {
		return fmt.Errorf("the cluster has a namespace %s that isn't an app's; name the app otherwise", ns)
	}
	if err == nil && existing.DeletionTimestamp != nil {
		return fmt.Errorf("%s is being removed; remove it again, then deploy it", app.Name)
	}
	for _, o := range cluster {
		if err := a.applyObject(ctx, o); err != nil {
			return fmt.Errorf("applying %s's objects: %w", app.Name, err)
		}
	}
	if app.Chart == "" {
		if err := a.deleteCharts(ctx, app.Name); err != nil {
			return fmt.Errorf("removing %s's chart: %w", app.Name, err)
		}
	}
	before, err := a.record(ctx, app.Name)
	if err != nil {
		return err
	}
	all := slices.Clone(before)
	for _, p := range planned {
		if !has(all, p) {
			all = append(all, p)
		}
	}
	if err := a.writeRecord(ctx, app.Name, all); err != nil {
		return err
	}
	for _, o := range own {
		if err := a.applyObject(ctx, o); err != nil {
			return fmt.Errorf("applying %s's objects: %w", app.Name, err)
		}
	}
	for _, entry := range before {
		if has(planned, entry) || strings.HasPrefix(entry, claimKind+"/") {
			continue
		}
		if err := a.deleteRecorded(ctx, ns, entry); err != nil {
			return fmt.Errorf("deleting %s's %s, which it no longer has: %w", app.Name, entry, err)
		}
	}
	return a.writeRecord(ctx, app.Name, planned)
}

// has reports whether entries hold entry, its kind in whatever case.
func has(entries []string, entry string) bool {
	kind, name, _ := strings.Cut(entry, "/")
	return slices.ContainsFunc(entries, func(e string) bool {
		k, n, _ := strings.Cut(e, "/")
		return n == name && strings.EqualFold(k, kind)
	})
}

// applyObject applies o, server side, taking over the fields another
// manager set.
func (a *api) applyObject(ctx context.Context, o *unstructured.Unstructured) error {
	gvk := o.GroupVersionKind()
	mapping, err := a.mapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		return fmt.Errorf("%s: %w", kindAndName(o), err)
	}
	var r dynamic.ResourceInterface = a.dynamic.Resource(mapping.Resource)
	if mapping.Scope.Name() == meta.RESTScopeNameNamespace {
		r = a.dynamic.Resource(mapping.Resource).Namespace(o.GetNamespace())
	}
	if _, err := r.Apply(ctx, o.GetName(), o, metav1.ApplyOptions{FieldManager: fieldManager, Force: true}); err != nil {
		return fmt.Errorf("%s: %w", kindAndName(o), err)
	}
	return nil
}

// waitUninstalled waits until no HelmChart of the app called app is
// being deleted, for rolloutTimeout or until ctx's deadline.
func (a *api) waitUninstalled(ctx context.Context, app string) error {
	charts := a.dynamic.Resource(helmCharts).Namespace(chartNamespace)
	err := poll(ctx, rolloutTimeout, time.Second, func(ctx context.Context) (bool, error) {
		list, err := charts.List(ctx, metav1.ListOptions{LabelSelector: appLabel + "=" + app})
		if err != nil {
			return false, err
		}
		return !slices.ContainsFunc(list.Items, func(c unstructured.Unstructured) bool { return c.GetDeletionTimestamp() != nil }), nil
	})
	if errors.Is(err, errNotInTime) {
		return fmt.Errorf("%s's chart of before is still being uninstalled", app)
	}
	if err != nil {
		return fmt.Errorf("reading %s's chart: %w", app, err)
	}
	return nil
}

// deleteCharts deletes the HelmCharts of the app called app, and
// returns once they are gone.
func (a *api) deleteCharts(ctx context.Context, app string) error {
	charts := a.dynamic.Resource(helmCharts).Namespace(chartNamespace)
	selector := metav1.ListOptions{LabelSelector: appLabel + "=" + app}
	deleted := false
	err := poll(ctx, rolloutTimeout, time.Second, func(ctx context.Context) (bool, error) {
		left, err := charts.List(ctx, selector)
		if err != nil || len(left.Items) == 0 {
			return err == nil, err
		}
		if deleted {
			return false, nil
		}
		for _, c := range left.Items {
			if err := charts.Delete(ctx, c.GetName(), metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
				return false, err
			}
		}
		deleted = true
		return false, nil
	})
	if errors.Is(err, errNotInTime) {
		return errors.New("it is still there")
	}
	return err
}

// record is the objects the last deploy of the app called app applied
// in its namespace, as recorded entries, one a line. It leaves out an
// entry without a name, and one of a kind that isn't kept in a
// namespace.
func (a *api) record(ctx context.Context, app string) ([]string, error) {
	m, err := a.client.CoreV1().ConfigMaps(namespaceOf(app)).Get(ctx, recordName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading what %s's last deploy applied: %w", app, err)
	}
	var entries []string
	for _, entry := range strings.Split(m.Data["objects"], "\n") {
		kind, name, _ := strings.Cut(entry, "/")
		if name == "" {
			continue
		}
		mapping, err := a.mapping(schema.ParseGroupKind(kind))
		if err == nil && mapping.Scope.Name() != meta.RESTScopeNameNamespace {
			continue
		}
		if err != nil && !meta.IsNoMatchError(err) {
			return nil, fmt.Errorf("reading what %s's last deploy applied: %w", app, err)
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func (a *api) writeRecord(ctx context.Context, app string, entries []string) error {
	m := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": recordName, "namespace": namespaceOf(app), "labels": map[string]any{appLabel: app}},
		"data":       map[string]any{"objects": strings.Join(entries, "\n")},
	}}
	if err := a.applyObject(ctx, m); err != nil {
		return fmt.Errorf("recording %s's objects: %w", app, err)
	}
	return nil
}

// deleteRecorded deletes the object of the record's entry in namespace.
// An object that isn't there, and a kind the cluster doesn't have, are
// no error.
func (a *api) deleteRecorded(ctx context.Context, namespace, entry string) error {
	kind, name, _ := strings.Cut(entry, "/")
	mapping, err := a.mapping(schema.ParseGroupKind(kind))
	if meta.IsNoMatchError(err) {
		return nil
	}
	if err != nil {
		return err
	}
	err = a.dynamic.Resource(mapping.Resource).Namespace(namespace).Delete(ctx, name, metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

// waitRollout waits until every Deployment, StatefulSet and DaemonSet
// of namespace runs as it was last applied. When the time is up, its
// error names one that doesn't, and lists the namespace's pods and last
// events.
func (a *api) waitRollout(ctx context.Context, namespace string) error {
	waiting := "its workloads weren't read in time"
	err := poll(ctx, rolloutTimeout, time.Second, func(ctx context.Context) (bool, error) {
		w, err := a.rollingOut(ctx, namespace)
		if err != nil {
			return false, err
		}
		waiting = w
		return w == "", nil
	})
	if errors.Is(err, errNotInTime) {
		return fmt.Errorf("%s\n%s", waiting, a.diagnostics(ctx, namespace))
	}
	return err
}

// rollingOut names an object of namespace that doesn't yet run as it
// was last applied, with how far it is, or is empty.
func (a *api) rollingOut(ctx context.Context, namespace string) (string, error) {
	deployments, err := a.client.AppsV1().Deployments(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", err
	}
	for _, d := range deployments.Items {
		waiting, err := deploymentRollingOut(d)
		if err != nil {
			return "", fmt.Errorf("%w\n%s", err, a.diagnostics(ctx, namespace))
		}
		if waiting != "" {
			return waiting, nil
		}
	}
	sets, err := a.client.AppsV1().StatefulSets(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", err
	}
	for _, s := range sets.Items {
		if waiting, err := statefulSetRollingOut(s); err != nil || waiting != "" {
			return waiting, err
		}
	}
	daemons, err := a.client.AppsV1().DaemonSets(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", err
	}
	for _, d := range daemons.Items {
		if waiting, err := daemonSetRollingOut(d); err != nil || waiting != "" {
			return waiting, err
		}
	}
	return "", nil
}

// deploymentRollingOut says how far d is from running as it was last
// applied, or nothing once it does, by the conditions of kubectl
// rollout status: its change seen, every replica it wants updated, no
// replica of before left, and every updated one available. It fails
// for one past its progress deadline.
func deploymentRollingOut(d appsv1.Deployment) (string, error) {
	s := d.Status
	if d.Generation > s.ObservedGeneration {
		return fmt.Sprintf("deployment %s: its change isn't seen yet", d.Name), nil
	}
	for _, c := range s.Conditions {
		if c.Type == appsv1.DeploymentProgressing && c.Reason == "ProgressDeadlineExceeded" {
			return "", fmt.Errorf("deployment %s exceeded its progress deadline", d.Name)
		}
	}
	want := int32(1)
	if d.Spec.Replicas != nil {
		want = *d.Spec.Replicas
	}
	if s.UpdatedReplicas < want || s.AvailableReplicas < s.UpdatedReplicas {
		return fmt.Sprintf("deployment %s: %d of %d replicas are updated and available", d.Name, min(s.UpdatedReplicas, s.AvailableReplicas), want), nil
	}
	if s.Replicas > s.UpdatedReplicas {
		return fmt.Sprintf("deployment %s: %d replicas of before are still there", d.Name, s.Replicas-s.UpdatedReplicas), nil
	}
	return "", nil
}

// statefulSetRollingOut says how far s is from running as it was last
// applied, or nothing once it does, by the conditions of kubectl
// rollout status: its change seen, every replica it wants ready, and
// every replica updated, or those from its partition on. It fails for
// one whose update strategy isn't RollingUpdate.
func statefulSetRollingOut(s appsv1.StatefulSet) (string, error) {
	if s.Spec.UpdateStrategy.Type != appsv1.RollingUpdateStatefulSetStrategyType {
		return "", fmt.Errorf("statefulset %s: a rollout is waited for only with the update strategy RollingUpdate", s.Name)
	}
	st := s.Status
	if st.ObservedGeneration == 0 || s.Generation > st.ObservedGeneration {
		return fmt.Sprintf("statefulset %s: its change isn't seen yet", s.Name), nil
	}
	want := int32(1)
	if s.Spec.Replicas != nil {
		want = *s.Spec.Replicas
	}
	if st.ReadyReplicas < want {
		return fmt.Sprintf("statefulset %s: %d of %d replicas are ready", s.Name, st.ReadyReplicas, want), nil
	}
	if r := s.Spec.UpdateStrategy.RollingUpdate; r != nil && r.Partition != nil {
		if st.UpdatedReplicas < want-*r.Partition {
			return fmt.Sprintf("statefulset %s: %d of %d replicas are updated", s.Name, st.UpdatedReplicas, want-*r.Partition), nil
		}
		return "", nil
	}
	if st.UpdateRevision != st.CurrentRevision {
		return fmt.Sprintf("statefulset %s: %d replicas are updated", s.Name, st.UpdatedReplicas), nil
	}
	return "", nil
}

// daemonSetRollingOut says how far d is from running as it was last
// applied, or nothing once it does, by the conditions of kubectl
// rollout status: its change seen, and a replica updated and available
// on every node it is to run on. It fails for one whose update
// strategy isn't RollingUpdate.
func daemonSetRollingOut(d appsv1.DaemonSet) (string, error) {
	if d.Spec.UpdateStrategy.Type != appsv1.RollingUpdateDaemonSetStrategyType {
		return "", fmt.Errorf("daemonset %s: a rollout is waited for only with the update strategy RollingUpdate", d.Name)
	}
	st := d.Status
	if d.Generation > st.ObservedGeneration {
		return fmt.Sprintf("daemonset %s: its change isn't seen yet", d.Name), nil
	}
	if st.UpdatedNumberScheduled < st.DesiredNumberScheduled || st.NumberAvailable < st.DesiredNumberScheduled {
		return fmt.Sprintf("daemonset %s: %d of %d replicas are updated and available", d.Name, min(st.UpdatedNumberScheduled, st.NumberAvailable), st.DesiredNumberScheduled), nil
	}
	return "", nil
}

// diagnostics are namespace's pods, each with its node and its phase or
// what its containers wait for, and its last 15 events. It reads them
// though ctx has ended.
func (a *api) diagnostics(ctx context.Context, namespace string) string {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	var b strings.Builder
	pods, err := a.client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return "its pods can't be listed: " + err.Error()
	}
	for _, p := range pods.Items {
		state := string(p.Status.Phase)
		for _, s := range slices.Concat(p.Status.InitContainerStatuses, p.Status.ContainerStatuses) {
			if s.State.Waiting != nil && s.State.Waiting.Reason != "" {
				state = s.State.Waiting.Reason
			}
		}
		fmt.Fprintf(&b, "pod %s on %s: %s\n", p.Name, cmp.Or(p.Spec.NodeName, "no node"), state)
	}
	events, err := a.client.CoreV1().Events(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return b.String() + "its events can't be listed: " + err.Error()
	}
	slices.SortStableFunc(events.Items, func(x, y corev1.Event) int { return eventTime(x).Compare(eventTime(y)) })
	for _, e := range events.Items[max(0, len(events.Items)-15):] {
		fmt.Fprintf(&b, "%s %s/%s: %s\n", e.Type, strings.ToLower(e.InvolvedObject.Kind), e.InvolvedObject.Name, e.Message)
	}
	return strings.TrimSuffix(b.String(), "\n")
}

// eventTime is when e last happened: the latest of its last timestamp,
// its event time and, of one of a series, the series' last.
func eventTime(e corev1.Event) time.Time {
	latest := e.LastTimestamp.Time
	if e.EventTime.After(latest) {
		latest = e.EventTime.Time
	}
	if e.Series != nil && e.Series.LastObservedTime.After(latest) {
		latest = e.Series.LastObservedTime.Time
	}
	return latest
}

// chartState is what a deploy reads of an app's chart before applying
// it: its HelmChart's generation and its install job's UID, each zero
// if there is none.
type chartState struct {
	generation int64
	job        string
}

func (a *api) chartState(ctx context.Context, app string) (chartState, error) {
	var s chartState
	c, err := a.dynamic.Resource(helmCharts).Namespace(chartNamespace).Get(ctx, chartName(app), metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return s, err
	}
	if err == nil {
		s.generation = c.GetGeneration()
	}
	job, err := a.client.BatchV1().Jobs(chartNamespace).Get(ctx, chartJob(app), metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return s, err
	}
	if err == nil {
		s.job = string(job.UID)
	}
	return s, nil
}

// waitChart waits until the Helm controller has installed the HelmChart
// of the app called app as it is now: until its install job, the one of
// before if the HelmChart's generation is unchanged, or else a new one,
// has succeeded. chartFailures failed pods of that job fail it, with
// the job's last log lines.
func (a *api) waitChart(ctx context.Context, app string, before chartState) error {
	now, err := a.chartState(ctx, app)
	if err != nil {
		return err
	}
	changed := now.generation != before.generation
	err = poll(ctx, rolloutTimeout, time.Second, func(ctx context.Context) (bool, error) {
		job, err := a.client.BatchV1().Jobs(chartNamespace).Get(ctx, chartJob(app), metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		if changed && string(job.UID) == before.job {
			return false, nil
		}
		if job.Status.Succeeded >= 1 {
			return true, nil
		}
		if job.Status.Failed >= chartFailures {
			return false, fmt.Errorf("its chart's install failed:\n%s", a.jobLog(ctx, job.Name))
		}
		return false, nil
	})
	if errors.Is(err, errNotInTime) {
		return errors.New("its chart isn't installed yet")
	}
	return err
}

// jobLog is the last 20 log lines of the newest pod of the job called
// name in chartNamespace, or why they can't be read.
func (a *api) jobLog(ctx context.Context, name string) string {
	pods, err := a.client.CoreV1().Pods(chartNamespace).List(ctx, metav1.ListOptions{LabelSelector: "job-name=" + name})
	if err != nil {
		return "its log can't be read: " + err.Error()
	}
	if len(pods.Items) == 0 {
		return "it has no log"
	}
	newest := slices.MaxFunc(pods.Items, func(x, y corev1.Pod) int { return x.CreationTimestamp.Compare(y.CreationTimestamp.Time) })
	tail := int64(20)
	log, err := a.client.CoreV1().Pods(chartNamespace).GetLogs(newest.Name, &corev1.PodLogOptions{TailLines: &tail}).DoRaw(ctx)
	if err != nil {
		return "its log can't be read: " + err.Error()
	}
	return strings.TrimSpace(string(log))
}

// Certificate waits until the Certificate of the container's host is
// issued as it is now, by a Ready condition that isn't of an earlier
// generation of it, for certificateTimeout or until ctx's deadline, and then
// fails with what cert-manager says of it.
func (b *Backend) Certificate(ctx context.Context, app, container string) error {
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	return causeIfEnded(ctx, b.certificate(ctx, app, container))
}

func (b *Backend) certificate(ctx context.Context, app, container string) error {
	a, err := b.connect(ctx)
	if err != nil {
		return err
	}
	ns, err := a.namespace(ctx, app)
	if err != nil {
		return err
	}
	said := "it isn't asked for yet"
	err = poll(ctx, certificateTimeout, time.Second, func(ctx context.Context) (bool, error) {
		cert, err := a.dynamic.Resource(certificates).Namespace(ns).Get(ctx, tlsSecret(container), metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		var messages []string
		conditions, _, _ := unstructured.NestedSlice(cert.Object, "status", "conditions")
		for _, c := range conditions {
			m, ok := c.(map[string]any)
			if !ok {
				continue
			}
			seen, told := m["observedGeneration"].(int64)
			if m["type"] == "Ready" && m["status"] == "True" && (!told || seen >= cert.GetGeneration()) {
				return true, nil
			}
			if m["message"] != nil {
				messages = append(messages, fmt.Sprint(m["message"]))
			}
		}
		if len(messages) > 0 {
			said = strings.Join(messages, "; ")
		}
		return false, nil
	})
	if errors.Is(err, errNotInTime) {
		return errors.New(said)
	}
	return err
}

// namespace is the namespace of the app called app: the one
// namespaceOf names, if it carries appLabel with the app's name. It
// fails with ErrNoApp if the cluster has none such.
func (a *api) namespace(ctx context.Context, app string) (string, error) {
	ns, err := a.client.CoreV1().Namespaces().Get(ctx, namespaceOf(app), metav1.GetOptions{})
	if apierrors.IsNotFound(err) || err == nil && ns.Labels[appLabel] != app {
		return "", orchestrator.ErrNoApp
	}
	if err != nil {
		return "", fmt.Errorf("looking for %s: %w", app, err)
	}
	return ns.Name, nil
}

// Remove deletes the app's HelmCharts, which uninstalls its chart, and
// once they are gone its namespace, with everything in it. It returns
// once the namespace is gone. Each of the two has rolloutTimeout, or
// until ctx's deadline. If ctx ends otherwise, it fails with ctx's
// cause.
func (b *Backend) Remove(ctx context.Context, app string) error {
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	return causeIfEnded(ctx, b.remove(ctx, app))
}

func (b *Backend) remove(ctx context.Context, app string) error {
	a, err := b.connect(ctx)
	if err != nil {
		return err
	}
	ns, err := a.namespace(ctx, app)
	if err != nil {
		return err
	}
	if err := a.deleteCharts(ctx, app); err != nil {
		return fmt.Errorf("removing %s's chart: %w", app, err)
	}
	namespaces := a.client.CoreV1().Namespaces()
	if err := namespaces.Delete(ctx, ns, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("removing %s: %w", app, err)
	}
	err = poll(ctx, rolloutTimeout, time.Second, func(ctx context.Context) (bool, error) {
		_, err := namespaces.Get(ctx, ns, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	})
	if errors.Is(err, errNotInTime) {
		return fmt.Errorf("removing %s: it is still there", app)
	}
	if err != nil {
		return fmt.Errorf("removing %s: %w", app, err)
	}
	return nil
}
