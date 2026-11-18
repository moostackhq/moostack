// Package orchestrator is the port to what runs a cluster's containers.
package orchestrator

import "context"

type Container struct {
	Namespace string
	Pod       string
	Name      string
	Image     string
}

type Orchestrator interface {
	// Running returns the cluster's running containers, ordered by
	// namespace, then pod, then name.
	Running(ctx context.Context) ([]Container, error)
}
