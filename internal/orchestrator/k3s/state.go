package k3s

import (
	"cmp"
	"context"
	"fmt"
	"slices"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/moostackhq/moostack/internal/model"
)

// Running returns the containers of every pod that are running, init
// and ephemeral ones as well. A container's replica is its pod, its app
// the one appLabel on the pod names, and its image the one the pod asks
// for. It reads the pods a page at a time and keeps only the
// containers.
func (b *Backend) Running(ctx context.Context) ([]model.Running, error) {
	var running []model.Running
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
	slices.SortFunc(running, func(a, b model.Running) int {
		return cmp.Or(cmp.Compare(a.App, b.App), cmp.Compare(a.Replica, b.Replica), cmp.Compare(a.Container, b.Container))
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
			Replica:   pod.Name,
			Container: s.Name,
			Image:     images[s.Name],
		})
	}
	return running
}

// pageSize is how many pods one call to the API asks for.
const pageSize = 500
