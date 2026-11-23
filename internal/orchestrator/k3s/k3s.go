// Package k3s is the orchestrator backend for k3s, over the Kubernetes
// API.
package k3s

import (
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

func (b *Backend) Can() orchestrator.Capabilities {
	return orchestrator.Capabilities{KubernetesObjects: true}
}

var _ orchestrator.Orchestrator = (*Backend)(nil)
