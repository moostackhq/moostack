//go:build linux

// Package updater is the node component that will replace the agent
// with a new version. For now it serves its own status, on its own
// socket, so that it answers whatever state the agent is in.
package updater

import (
	"context"
	"errors"

	"google.golang.org/grpc"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/identity"
	"github.com/moostackhq/moostack/internal/rootsocket"
)

// Config says where the updater serves, and where it finds the node's
// identity.
type Config struct {
	// Version is the updater's own version.
	Version string
	// Socket is where to serve the updater's node API, to root only.
	Socket string
	// Identity is the node's identity, which the updater shares with the
	// agent.
	Identity identity.Files
}

// Serve serves the updater's node API on cfg.Socket until ctx ends. It
// won't serve without a valid node identity.
func Serve(ctx context.Context, cfg Config) error {
	if _, err := identity.Load(cfg.Identity); err != nil {
		return err
	}
	l, err := rootsocket.Listen(cfg.Socket)
	if err != nil {
		return err
	}
	server := grpc.NewServer()
	nodev1.RegisterUpdaterServer(server, &updaterServer{version: cfg.Version})
	stop := context.AfterFunc(ctx, server.GracefulStop)
	defer stop()
	if err := server.Serve(l); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		return err
	}
	return nil
}

type updaterServer struct {
	nodev1.UnimplementedUpdaterServer
	version string
}

func (s *updaterServer) Status(context.Context, *nodev1.UpdaterStatusRequest) (*nodev1.UpdaterStatusResponse, error) {
	return &nodev1.UpdaterStatusResponse{
		Updater: &nodev1.Component{Name: "updater", Version: s.version, Health: nodev1.Health_HEALTH_HEALTHY},
	}, nil
}
