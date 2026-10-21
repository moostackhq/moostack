//go:build linux

// Package updater is the node component that replaces the agent with a
// new version, through an A/B swap that it rolls back if the new agent
// isn't healthy. It serves its own node API, on its own socket, so that
// it answers whatever state the agent is in.
package updater

import (
	"context"
	"errors"

	"google.golang.org/grpc"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/abslot"
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
	// Agent is the agent's binary, a symbolic link to the active one of
	// AgentSlots, relative, in the same directory.
	Agent string
	// AgentSlots are the agent's A/B slots: the one the agent runs from,
	// and the one an update writes the new agent into.
	AgentSlots [2]string
	// AgentSwapFile records a swap of the agent that hasn't been seen
	// healthy. Its directory must exist.
	AgentSwapFile string
	// AgentSocket is where the agent serves the node API.
	AgentSocket string
	// RestartAgent restarts the agent, from the binary Agent links to.
	RestartAgent func(ctx context.Context) error
}

// Serve serves the updater's node API on cfg.Socket until ctx ends, and
// then finishes an update of the agent that runs. It won't serve without
// a valid node identity. If a swap of the agent is recorded, as when the
// updater crashed in one, it first switches the agent back, within the
// health timeout the swap's update had, and fails if it can't, so that
// systemd starts it again to try once more.
func Serve(ctx context.Context, cfg Config) error {
	if _, err := identity.Load(cfg.Identity); err != nil {
		return err
	}
	healthy, err := agentHealthy(cfg.AgentSocket)
	if err != nil {
		return err
	}
	s := &updaterServer{cfg: cfg, agent: &abslot.Component{
		Name:     "agent",
		Swapper:  "the updater",
		Link:     cfg.Agent,
		Slots:    cfg.AgentSlots,
		SwapFile: cfg.AgentSwapFile,
		Restart:  cfg.RestartAgent,
		Healthy:  healthy,
	}}
	if err := s.agent.Recover(ctx); err != nil {
		return err
	}
	l, err := rootsocket.Listen(cfg.Socket)
	if err != nil {
		return err
	}
	server := grpc.NewServer()
	nodev1.RegisterUpdaterServer(server, s)
	stop := context.AfterFunc(ctx, server.GracefulStop)
	defer stop()
	if err := server.Serve(l); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		return err
	}
	return nil
}

type updaterServer struct {
	nodev1.UnimplementedUpdaterServer
	cfg   Config
	agent *abslot.Component
}

func (s *updaterServer) Status(context.Context, *nodev1.UpdaterStatusRequest) (*nodev1.UpdaterStatusResponse, error) {
	return &nodev1.UpdaterStatusResponse{
		Updater: &nodev1.Component{Name: "updater", Version: s.cfg.Version, Health: nodev1.Health_HEALTH_HEALTHY},
	}, nil
}
