package updater

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
)

// healthyAgent serves an agent's status, healthy at its version.
type healthyAgent struct {
	nodev1.UnimplementedAgentServer
}

func (healthyAgent) Status(context.Context, *nodev1.StatusRequest) (*nodev1.StatusResponse, error) {
	return &nodev1.StatusResponse{Components: []*nodev1.Component{{Name: "agent", Version: "1.0.0", Health: nodev1.Health_HEALTH_HEALTHY}}}, nil
}

// A rollback waits for the agent it switched back to on the connection
// that waited for the new one, which didn't serve: it sees the old agent
// within a second of it serving, however long the new one was down,
// rather than once the connection's backoff ends.
func TestTheAgentsHealthIsSeenSoonAfterALongOutage(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "agent.sock")
	healthy, err := agentHealthy(socket)
	if err != nil {
		t.Fatal(err)
	}
	for end := time.Now().Add(20 * time.Second); time.Now().Before(end); time.Sleep(250 * time.Millisecond) {
		healthy(t.Context(), "1.0.0")
	}
	l, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer()
	nodev1.RegisterAgentServer(s, healthyAgent{})
	go s.Serve(l)
	defer s.Stop()
	start := time.Now()
	for healthy(t.Context(), "1.0.0") != nil {
		if time.Since(start) > 1500*time.Millisecond {
			t.Fatalf("the agent, serving since %s ago, isn't seen healthy: %v", time.Since(start).Round(time.Millisecond), healthy(t.Context(), "1.0.0"))
		}
		time.Sleep(250 * time.Millisecond)
	}
}
