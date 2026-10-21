//go:build linux

package updater

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/abslot"
)

// UpdateAgent receives the new agent and swaps to it, through the
// agent's A/B slots.
func (s *updaterServer) UpdateAgent(stream grpc.ClientStreamingServer[nodev1.UpdateAgentRequest, nodev1.UpdateAgentResponse]) error {
	err := s.agent.Update(stream.Context(), func() (*abslot.Update, []byte, error) {
		req, err := stream.Recv()
		if err != nil {
			return nil, nil, err
		}
		if u := req.Update; u != nil {
			return &abslot.Update{Version: u.Version, SHA256: u.Sha256, HealthTimeout: u.HealthTimeout.AsDuration()}, req.Chunk, nil
		}
		return nil, req.Chunk, nil
	})
	if err != nil {
		return err
	}
	return stream.SendAndClose(&nodev1.UpdateAgentResponse{})
}

// agentHealthy fails unless the agent on socket serves its status, at
// version unless that is empty, with every component it reports
// healthy. It keeps one connection, which reconnects as the agent
// restarts.
func agentHealthy(socket string) (func(ctx context.Context, version string) error, error) {
	conn, err := grpc.NewClient("unix://"+socket, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	agent := nodev1.NewAgentClient(conn)
	return func(ctx context.Context, version string) error {
		res, err := agent.Status(ctx, &nodev1.StatusRequest{})
		if err != nil {
			return fmt.Errorf("the agent doesn't answer: %w", err)
		}
		for _, c := range res.Components {
			if c.Name == "agent" && version != "" && c.Version != version {
				return fmt.Errorf("the agent runs version %s, not %s", c.Version, version)
			}
			if c.Health != nodev1.Health_HEALTH_HEALTHY {
				return fmt.Errorf("%s is unhealthy: %s", c.Name, c.Problem)
			}
		}
		if !slices.ContainsFunc(res.Components, func(c *nodev1.Component) bool { return c.Name == "agent" }) {
			return errors.New("the agent doesn't report itself")
		}
		return nil
	}, nil
}
