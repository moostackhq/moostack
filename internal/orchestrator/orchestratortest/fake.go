// Package orchestratortest holds a fake Orchestrator and the tests that
// every Orchestrator passes.
package orchestratortest

import (
	"cmp"
	"context"
	"slices"

	"github.com/moostackhq/moostack/internal/orchestrator"
)

type Fake struct {
	Containers []orchestrator.Container
	Err        error
}

func (f *Fake) Running(context.Context) ([]orchestrator.Container, error) {
	if f.Err != nil {
		return nil, f.Err
	}
	return f.Containers, nil
}

type Pod struct {
	Namespace, Name string
	Containers      []PodContainer
}

type PodContainer struct {
	Name, Image string
	Status      Status
}

type Status int

const (
	Waiting Status = iota
	Running
	Terminated
)

// NewFake returns a Fake whose answer is what pods have running.
func NewFake(pods []Pod) *Fake {
	f := &Fake{}
	for _, p := range pods {
		for _, c := range p.Containers {
			if c.Status == Running {
				f.Containers = append(f.Containers, orchestrator.Container{Namespace: p.Namespace, Pod: p.Name, Name: c.Name, Image: c.Image})
			}
		}
	}
	slices.SortFunc(f.Containers, func(a, b orchestrator.Container) int {
		return cmp.Or(cmp.Compare(a.Namespace, b.Namespace), cmp.Compare(a.Pod, b.Pod), cmp.Compare(a.Name, b.Name))
	})
	return f
}
