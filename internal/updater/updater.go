//go:build linux

// Package updater is the node component that replaces the agent with a
// new version, through an A/B swap that it rolls back if the new agent
// isn't healthy. It serves its own node API, on its own socket, so that
// it answers whatever state the agent is in.
package updater

import (
	"context"
	"errors"
	"log"
	"net"
	"os"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"

	controlplanev1 "github.com/moostackhq/moostack/api/controlplane/v1"
	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/abslot"
	"github.com/moostackhq/moostack/internal/buildid"
	"github.com/moostackhq/moostack/internal/controlplane"
	"github.com/moostackhq/moostack/internal/hostlayout"
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
	// NodeFile is the node's hostlayout.NodeConfig.
	NodeFile string
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
// a valid node identity. On a node in control plane mode, it also dials
// the control plane, and serves it the updater's node API over that
// tunnel, while cfg.Socket serves only status, until the node file says
// the node is detached. If a swap of the agent is recorded, as when the
// updater crashed in one, it first switches the agent back, within the
// health timeout the swap's update had, and fails if it can't, so that
// systemd starts it again to try once more.
func Serve(ctx context.Context, cfg Config) error {
	id, err := identity.Load(cfg.Identity)
	if err != nil {
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
	node, err := hostlayout.ReadNodeConfig(cfg.NodeFile)
	if err != nil {
		return err
	}
	l, err := rootsocket.Listen(cfg.Socket)
	if err != nil {
		return err
	}
	s.controlPlane.Store(node.Mode == hostlayout.ModeControlPlane)
	server := grpc.NewServer(controlplane.Lockout(s.controlPlane.Load, "status", nodev1.Updater_Status_FullMethodName)...)
	nodev1.RegisterUpdaterServer(server, s)
	servers := map[*grpc.Server]net.Listener{server: l}
	if s.controlPlane.Load() {
		tunnel := grpc.NewServer(controlplane.ServerOptions()...)
		nodev1.RegisterUpdaterServer(tunnel, s)
		listener := controlplane.Listen(controlplane.Tunnel{Address: node.ControlPlane, Identity: id, Protocol: controlplanev1.ProtocolUpdater, Log: log.Printf})
		servers[tunnel] = listener
		go s.followDetach(ctx, listener)
	}
	// The build is hashed from the start, in the background, so that
	// status says it by the time an operation asks.
	s.build()
	return serveAll(ctx, servers)
}

// detachPoll is how often the updater of a node in control plane mode
// reads the node file, to see a detach.
const detachPoll = time.Second

// followDetach reads the node file every detachPoll until it says the
// node is in direct mode, as once the agent has detached it, and then
// disconnects tunnel and lifts the local socket's lockout, or until ctx
// ends. A node file it can't read leaves the mode as it is.
func (s *updaterServer) followDetach(ctx context.Context, tunnel *controlplane.Listener) {
	t := time.NewTicker(detachPoll)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if node, err := hostlayout.ReadNodeConfig(s.cfg.NodeFile); err == nil && node.Mode == hostlayout.ModeDirect {
			log.Printf("the node was detached from its control plane: serving in direct mode")
			tunnel.Disconnect()
			s.controlPlane.Store(false)
			return
		}
	}
}

// serveAll serves each server on its listener until ctx ends or one of
// them fails, and then stops them all gracefully, so that an update of
// the agent that runs finishes.
func serveAll(ctx context.Context, servers map[*grpc.Server]net.Listener) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	errs := make(chan error, len(servers))
	for s, l := range servers {
		go func() {
			err := s.Serve(l)
			if errors.Is(err, grpc.ErrServerStopped) {
				err = nil
			}
			errs <- err
			cancel()
		}()
	}
	<-ctx.Done()
	for s := range servers {
		s.GracefulStop()
	}
	var err error
	for range servers {
		err = errors.Join(err, <-errs)
	}
	return err
}

type updaterServer struct {
	nodev1.UnimplementedUpdaterServer
	cfg Config
	// controlPlane says whether the node is in control plane mode, rather
	// than direct mode, until a detach.
	controlPlane atomic.Bool
	agent        *abslot.Component
	// builds tells the build of the updater's own file.
	builds buildid.Cache
}

// build is the build of the file the updater runs from, by the name a
// release gives it, empty until it is hashed, which status doesn't wait
// for, or if it can't tell.
func (s *updaterServer) build() string {
	path, err := os.Executable()
	if err != nil {
		return ""
	}
	return s.builds.Peek(map[string]string{"updater": path})
}

func (s *updaterServer) Status(context.Context, *nodev1.UpdaterStatusRequest) (*nodev1.UpdaterStatusResponse, error) {
	mode := nodev1.Mode_MODE_DIRECT
	if s.controlPlane.Load() {
		mode = nodev1.Mode_MODE_CONTROL_PLANE
	}
	return &nodev1.UpdaterStatusResponse{
		Updater: &nodev1.Component{Name: "updater", Version: s.cfg.Version, Build: s.build(), Health: nodev1.Health_HEALTH_HEALTHY},
		Mode:    mode,
	}, nil
}
