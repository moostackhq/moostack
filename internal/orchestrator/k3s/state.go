package k3s

import (
	"cmp"
	"context"
	"fmt"
	"slices"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"github.com/moostackhq/moostack/internal/model"
)

// Apps are the namespaces that are an app's, as api.namespace finds
// one: each named as its appLabel says. An app wants the replicas of
// the Deployments and StatefulSets of its namespace, and a pod of each
// of its DaemonSets on every node the DaemonSet is to run on; those of
// them that are ready are its ready ones. Its images are those of the
// containers these ask for, init containers left out, and its hosts
// those of the rules of its namespace's Ingresses. If ctx ends
// otherwise than by its deadline, it fails with ctx's cause.
func (b *Backend) Apps(ctx context.Context) ([]model.AppState, error) {
	if ctx.Err() != nil {
		return nil, context.Cause(ctx)
	}
	apps, err := b.apps(ctx)
	return apps, causeIfEnded(ctx, err)
}

func (b *Backend) apps(ctx context.Context) ([]model.AppState, error) {
	a, err := b.connect(ctx)
	if err != nil {
		return nil, err
	}
	all := metav1.ListOptions{}
	namespaces, err := a.client.CoreV1().Namespaces().List(ctx, metav1.ListOptions{LabelSelector: appLabel})
	if err != nil {
		return nil, fmt.Errorf("listing the apps: %w", err)
	}
	deployments, err := a.client.AppsV1().Deployments(metav1.NamespaceAll).List(ctx, all)
	if err != nil {
		return nil, fmt.Errorf("listing the apps' deployments: %w", err)
	}
	statefulSets, err := a.client.AppsV1().StatefulSets(metav1.NamespaceAll).List(ctx, all)
	if err != nil {
		return nil, fmt.Errorf("listing the apps' statefulsets: %w", err)
	}
	daemonSets, err := a.client.AppsV1().DaemonSets(metav1.NamespaceAll).List(ctx, all)
	if err != nil {
		return nil, fmt.Errorf("listing the apps' daemonsets: %w", err)
	}
	ingresses, err := a.client.NetworkingV1().Ingresses(metav1.NamespaceAll).List(ctx, all)
	if err != nil {
		return nil, fmt.Errorf("listing the apps' ingresses: %w", err)
	}
	var apps []model.AppState
	for _, ns := range namespaces.Items {
		s := model.AppState{Name: ns.Labels[appLabel]}
		if namespaceOf(s.Name) != ns.Name {
			continue
		}
		runs := func(namespace string, want, ready int32, pod corev1.PodSpec) {
			if namespace != ns.Name {
				return
			}
			s.Want += int(want)
			s.Ready += int(ready)
			for _, c := range pod.Containers {
				if !slices.Contains(s.Images, c.Image) {
					s.Images = append(s.Images, c.Image)
				}
			}
		}
		for _, d := range deployments.Items {
			runs(d.Namespace, ptr.Deref(d.Spec.Replicas, 1), d.Status.ReadyReplicas, d.Spec.Template.Spec)
		}
		for _, d := range statefulSets.Items {
			runs(d.Namespace, ptr.Deref(d.Spec.Replicas, 1), d.Status.ReadyReplicas, d.Spec.Template.Spec)
		}
		for _, d := range daemonSets.Items {
			runs(d.Namespace, d.Status.DesiredNumberScheduled, d.Status.NumberReady, d.Spec.Template.Spec)
		}
		for _, i := range ingresses.Items {
			if i.Namespace != ns.Name {
				continue
			}
			for _, r := range i.Spec.Rules {
				if r.Host != "" {
					s.Hosts = append(s.Hosts, r.Host)
				}
			}
		}
		apps = append(apps, s)
	}
	slices.SortFunc(apps, func(x, y model.AppState) int { return cmp.Compare(x.Name, y.Name) })
	return apps, nil
}

// Running returns the containers of every pod that are running, init
// and ephemeral ones as well. A container's instance is its pod, its ID
// the pod's namespace and name and its own name, its app
// the one appLabel on the pod names, and its image the one the pod asks
// for. It reads the pods a page at a time and keeps only the
// containers. If ctx ends otherwise than by its deadline, it fails with
// ctx's cause.
func (b *Backend) Running(ctx context.Context) ([]model.Running, error) {
	running, err := b.running(ctx)
	return running, causeIfEnded(ctx, err)
}

func (b *Backend) running(ctx context.Context) ([]model.Running, error) {
	a, err := b.connect(ctx)
	if err != nil {
		return nil, err
	}
	var running []model.Running
	opts := metav1.ListOptions{Limit: pageSize}
	for {
		pods, err := a.client.CoreV1().Pods(metav1.NamespaceAll).List(ctx, opts)
		if err != nil {
			return nil, fmt.Errorf("listing pods: %w", err)
		}
		for _, pod := range pods.Items {
			running = append(running, runningIn(pod)...)
		}
		if pods.Continue == "" {
			break
		}
		opts.Continue = pods.Continue
	}
	slices.SortFunc(running, func(a, b model.Running) int {
		return cmp.Or(cmp.Compare(a.App, b.App), cmp.Compare(a.Instance, b.Instance), cmp.Compare(a.Container, b.Container))
	})
	return running, nil
}

func runningIn(pod corev1.Pod) []model.Running {
	images := map[string]string{}
	for _, c := range pod.Spec.InitContainers {
		images[c.Name] = c.Image
	}
	for _, c := range pod.Spec.Containers {
		images[c.Name] = c.Image
	}
	for _, c := range pod.Spec.EphemeralContainers {
		images[c.Name] = c.Image
	}
	var running []model.Running
	for _, s := range slices.Concat(pod.Status.InitContainerStatuses, pod.Status.ContainerStatuses, pod.Status.EphemeralContainerStatuses) {
		if s.State.Running == nil {
			continue
		}
		running = append(running, model.Running{
			App:       pod.Labels[appLabel],
			Instance:  pod.Name,
			Container: s.Name,
			Image:     images[s.Name],
			ID:        pod.Namespace + "/" + pod.Name + "/" + s.Name,
			Started:   s.State.Running.StartedAt.Time,
		})
	}
	return running
}

// pageSize is how many pods one call to the API asks for.
const pageSize = 500

// Nodes gives a node the address that is its InternalIP, the version of
// its kubelet, and its memory as its capacity. A node is a control
// plane by its node-role.kubernetes.io/control-plane label. Ready is
// "yes", "no", or "?" for a node without a Ready condition, followed by
// ", cordoned" for a node that takes no new pods. If ctx ends otherwise
// than by its deadline, it fails with ctx's cause.
func (b *Backend) Nodes(ctx context.Context) ([]model.NodeState, error) {
	if ctx.Err() != nil {
		return nil, context.Cause(ctx)
	}
	nodes, err := b.nodes(ctx)
	return nodes, causeIfEnded(ctx, err)
}

func (b *Backend) nodes(ctx context.Context) ([]model.NodeState, error) {
	a, err := b.connect(ctx)
	if err != nil {
		return nil, err
	}
	list, err := a.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("listing nodes: %w", err)
	}
	var nodes []model.NodeState
	for _, n := range list.Items {
		s := model.NodeState{
			Name:         n.Name,
			Architecture: n.Status.NodeInfo.Architecture,
			Version:      n.Status.NodeInfo.KubeletVersion,
			MemoryMiB:    int(n.Status.Capacity.Memory().Value() >> 20),
			ControlPlane: n.Labels["node-role.kubernetes.io/control-plane"] == "true",
			Ready:        "?",
		}
		for _, addr := range n.Status.Addresses {
			if addr.Type == corev1.NodeInternalIP {
				s.Address = addr.Address
				break
			}
		}
		for _, c := range n.Status.Conditions {
			if c.Type == corev1.NodeReady {
				s.Ready = "no"
				if c.Status == corev1.ConditionTrue {
					s.Ready = "yes"
				}
			}
		}
		if n.Spec.Unschedulable {
			s.Ready += ", cordoned"
		}
		nodes = append(nodes, s)
	}
	slices.SortFunc(nodes, func(x, y model.NodeState) int { return cmp.Compare(x.Name, y.Name) })
	return nodes, nil
}
