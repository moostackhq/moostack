//go:build linux

package agent_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/hostlayout"
	"github.com/moostackhq/moostack/internal/nodetest"
)

func TestTheAgentKeepsTheGenerationItRecordsAcrossARestart(t *testing.T) {
	n := nodetest.Start(t)
	if got := n.EventuallyHealthy(t).Generation; got != 0 {
		t.Errorf("a new node reports generation %d, want 0", got)
	}
	record(t, n, 3, "node-1")
	record(t, n, 3, "node-1", "node-2")
	n.Restart(t, "agent")
	reported := n.EventuallyHealthy(t)
	if reported.Generation != 3 || !slices.Equal(reported.Nodes, []string{"node-1", "node-2"}) {
		t.Errorf("after a restart, the node reports generation %d of %v, want 3 of [node-1 node-2]", reported.Generation, reported.Nodes)
	}
}

func TestTheAgentSaysWhenItCantTellItsGenerationAndARecordingRepairsIt(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	record(t, n, 5, "node-1")
	n.Exec(t, "root", "sh", "-c", "echo garbage > "+hostlayout.GenerationFile)
	// The agent still answers, with its mode and components.
	reported := n.EventuallyHealthy(t)
	if reported.Mode != nodev1.Mode_MODE_DIRECT || len(reported.Components) == 0 {
		t.Errorf("with its generation file corrupt, the agent reports %v", reported)
	}
	if reported.Generation != 0 || reported.GenerationUnknown == "" {
		t.Errorf("with its generation file corrupt, the agent reports generation %d and the problem %q, want 0 and a problem", reported.Generation, reported.GenerationUnknown)
	}
	// It can't tell the generation it applied, so it accepts any.
	record(t, n, 2, "node-1")
	if reported := n.EventuallyHealthy(t); reported.Generation != 2 || reported.GenerationUnknown != "" {
		t.Errorf("after recording generation 2, the agent reports generation %d and the problem %q", reported.Generation, reported.GenerationUnknown)
	}
}

func TestTheAgentRefusesALowerGeneration(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	record(t, n, 5, "node-1")
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, err := n.Agent.RecordGeneration(ctx, &nodev1.RecordGenerationRequest{Generation: 4})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("recording generation 4 after 5: %v, want FailedPrecondition", err)
	}
	if got := n.EventuallyHealthy(t).Generation; got != 5 {
		t.Errorf("after the refusal, the node reports generation %d, want 5", got)
	}
}

// record records generation g of a cluster directory of nodes on n,
// which must succeed.
func record(t *testing.T, n *nodetest.Node, g int64, nodes ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if _, err := n.Agent.RecordGeneration(ctx, &nodev1.RecordGenerationRequest{Generation: g, Nodes: nodes}); err != nil {
		t.Fatalf("recording generation %d: %v", g, err)
	}
}
