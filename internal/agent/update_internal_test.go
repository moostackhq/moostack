package agent

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
)

// healthyUpdater serves an updater's status, healthy at its version.
type healthyUpdater struct {
	nodev1.UnimplementedUpdaterServer
}

func (healthyUpdater) Status(context.Context, *nodev1.UpdaterStatusRequest) (*nodev1.UpdaterStatusResponse, error) {
	return &nodev1.UpdaterStatusResponse{Updater: &nodev1.Component{Name: "updater", Version: "1.0.0", Health: nodev1.Health_HEALTH_HEALTHY}}, nil
}

// A rollback waits for the updater it switched back to on the connection
// that waited for the new one, which didn't serve: it sees the old
// updater within a second of it serving, however long the new one was
// down, rather than once the connection's backoff ends.
func TestTheUpdatersHealthIsSeenSoonAfterALongOutage(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "updater.sock")
	healthy, err := updaterHealthy(socket)
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
	nodev1.RegisterUpdaterServer(s, healthyUpdater{})
	go s.Serve(l)
	defer s.Stop()
	start := time.Now()
	for healthy(t.Context(), "1.0.0") != nil {
		if time.Since(start) > 1500*time.Millisecond {
			t.Fatalf("the updater, serving since %s ago, isn't seen healthy: %v", time.Since(start).Round(time.Millisecond), healthy(t.Context(), "1.0.0"))
		}
		time.Sleep(250 * time.Millisecond)
	}
}
