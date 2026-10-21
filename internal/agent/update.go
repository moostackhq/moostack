//go:build linux

package agent

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/abslot"
)

// UpdateUpdater receives the new updater and swaps to it, through the
// updater's A/B slots.
func (s *agentServer) UpdateUpdater(stream grpc.ClientStreamingServer[nodev1.UpdateUpdaterRequest, nodev1.UpdateUpdaterResponse]) error {
	err := s.updater.Update(stream.Context(), func() (*abslot.Update, []byte, error) {
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
	return stream.SendAndClose(&nodev1.UpdateUpdaterResponse{})
}

// updaterHealthy fails unless the updater on socket serves its status,
// at version unless that is empty, healthy. It keeps one connection,
// which reconnects as the updater restarts.
func updaterHealthy(socket string) (func(ctx context.Context, version string) error, error) {
	conn, err := grpc.NewClient("unix://"+socket, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	updater := nodev1.NewUpdaterClient(conn)
	return func(ctx context.Context, version string) error {
		res, err := updater.Status(ctx, &nodev1.UpdaterStatusRequest{})
		if err != nil {
			return fmt.Errorf("the updater doesn't answer: %w", err)
		}
		switch u := res.Updater; {
		case u == nil:
			return errors.New("the updater doesn't report itself")
		case version != "" && u.Version != version:
			return fmt.Errorf("the updater runs version %s, not %s", u.Version, version)
		case u.Health != nodev1.Health_HEALTH_HEALTHY:
			return fmt.Errorf("the updater is unhealthy: %s", u.Problem)
		}
		return nil
	}, nil
}

// recoverRetry is how long the agent waits to switch the updater back
// again, after it failed to.
const recoverRetry = 5 * time.Second

// recoverUpdater switches the updater back if a swap of it is recorded,
// as when the agent stopped before it saw the new one healthy, trying
// again until it can or ctx ends. The agent serves meanwhile, since a
// broken updater mustn't keep it from serving.
func recoverUpdater(ctx context.Context, updater *abslot.Component) {
	for {
		err := updater.Recover(ctx)
		if err == nil {
			return
		}
		log.Printf("%v; trying again in %s", err, recoverRetry)
		select {
		case <-ctx.Done():
			return
		case <-time.After(recoverRetry):
		}
	}
}
