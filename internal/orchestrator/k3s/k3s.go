// Package k3s is the orchestrator backend for k3s, over the Kubernetes
// API.
package k3s

import (
	"cmp"
	"context"
	"fmt"
	"slices"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/moostackhq/moostack/internal/orchestrator"
)

type Backend struct {
	client kubernetes.Interface
}

// New returns a Backend that connects with config.
func New(config *rest.Config) (*Backend, error) {
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return &Backend{client: client}, nil
}

// InCluster returns a Backend that connects as the service account of
// the pod the process runs in.
func InCluster() (*Backend, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	return New(config)
}

// Running returns the containers of every pod that are running, init
// and ephemeral ones as well. A container's image is the one its pod
// asks for. It reads the pods a page at a time and keeps only the
// containers.
func (b *Backend) Running(ctx context.Context) ([]orchestrator.Container, error) {
	var running []orchestrator.Container
	opts := metav1.ListOptions{Limit: pageSize}
	for {
		pods, err := b.client.CoreV1().Pods(metav1.NamespaceAll).List(ctx, opts)
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
	slices.SortFunc(running, func(a, b orchestrator.Container) int {
		return cmp.Or(cmp.Compare(a.Namespace, b.Namespace), cmp.Compare(a.Pod, b.Pod), cmp.Compare(a.Name, b.Name))
	})
	return running, nil
}

func runningIn(pod corev1.Pod) []orchestrator.Container {
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
	var running []orchestrator.Container
	for _, s := range slices.Concat(pod.Status.InitContainerStatuses, pod.Status.ContainerStatuses, pod.Status.EphemeralContainerStatuses) {
		if s.State.Running == nil {
			continue
		}
		running = append(running, orchestrator.Container{
			Namespace: pod.Namespace,
			Pod:       pod.Name,
			Name:      s.Name,
			Image:     images[s.Name],
		})
	}
	return running
}

// pageSize is how many pods one call to the API asks for.
const pageSize = 500

var _ orchestrator.Orchestrator = (*Backend)(nil)
