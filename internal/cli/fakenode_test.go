package cli_test

import (
	"context"
	"fmt"
	"slices"
	"sync"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/cluster"
	"github.com/moostackhq/moostack/internal/nodeclient"
	"github.com/moostackhq/moostack/internal/orchestrate"
)

// fakeNode answers for a node in place of its node API.
type fakeNode struct {
	// err, if set, is why the node can't be reached.
	err error
	// status is what the node reports.
	status nodeclient.Status
	// dialing, if set, is called when mooctl starts to reach the node,
	// and mooctl reaches it once dialing returns.
	dialing func(ctx context.Context)

	mu sync.Mutex
	// recorded are the generations recorded on the node, in order.
	recorded []int64
}

// healthyNode is a node in direct mode that has applied generation.
func healthyNode(generation int64) *fakeNode {
	return &fakeNode{status: nodeclient.Status{Mode: nodev1.Mode_MODE_DIRECT, Generation: generation}}
}

// generations are the generations recorded on the node so far.
func (n *fakeNode) generations() []int64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return slices.Clone(n.recorded)
}

func (n *fakeNode) Status(context.Context) nodeclient.Status { return n.status }

func (n *fakeNode) RecordGeneration(_ context.Context, g int64, _ []string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.recorded = append(n.recorded, g)
	return nil
}

func (n *fakeNode) Close() error { return nil }

// dialNode is what reaches the operator's fake nodes, or nil to reach
// nodes over SSH if the operator has none.
func (o *operator) dialNode() func(context.Context, cluster.Node) (orchestrate.Node, error) {
	if o.nodes == nil {
		return nil
	}
	return func(ctx context.Context, n cluster.Node) (orchestrate.Node, error) {
		f, ok := o.nodes[n.Name]
		if !ok {
			return nil, fmt.Errorf("the test has no node %s", n.Name)
		}
		if f.dialing != nil {
			f.dialing(ctx)
		}
		if f.err != nil {
			return nil, f.err
		}
		return f, nil
	}
}
