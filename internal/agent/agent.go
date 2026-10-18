//go:build linux

// Package agent is the node component that serves the node API.
package agent

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	versionv1 "github.com/containerd/containerd/api/services/version/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/hostlayout"
)

// Config says where the agent finds what it serves.
type Config struct {
	// Version is the agent's own version.
	Version string
	// Socket is where to serve the node API, to root only.
	Socket string
	// NodeFile is the node's hostlayout.NodeConfig.
	NodeFile string
	// ContainerdSocket is where containerd serves its API.
	ContainerdSocket string
}

// containerdTimeout bounds asking containerd for its version.
const containerdTimeout = 2 * time.Second

// Serve serves the node API on cfg.Socket until ctx ends. It replaces a
// socket left by an earlier agent.
func Serve(ctx context.Context, cfg Config) error {
	data, err := os.ReadFile(cfg.NodeFile)
	if err != nil {
		return err
	}
	node, err := hostlayout.ParseNodeConfig(data)
	if err != nil {
		return fmt.Errorf("%s: %w", cfg.NodeFile, err)
	}
	containerd, err := grpc.NewClient("unix://"+cfg.ContainerdSocket, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer containerd.Close()

	l, err := listen(cfg.Socket)
	if err != nil {
		return err
	}
	server := grpc.NewServer()
	nodev1.RegisterAgentServer(server, &agentServer{version: cfg.Version, mode: modes[node.Mode], containerd: versionv1.NewVersionClient(containerd)})
	stop := context.AfterFunc(ctx, server.GracefulStop)
	defer stop()
	if err := server.Serve(l); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		return err
	}
	return nil
}

// listen listens on the unix socket at path, which only root can connect
// to: the socket and its directory are root's alone.
func listen(path string) (net.Listener, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return nil, err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	// The socket is created with the umask's mode, so no one else can
	// connect between bind and chmod.
	old := syscall.Umask(0o177)
	l, err := net.Listen("unix", path)
	syscall.Umask(old)
	return l, err
}

// modes are the node API's modes by the node file's names for them.
var modes = map[string]nodev1.Mode{hostlayout.ModeDirect: nodev1.Mode_MODE_DIRECT}

type agentServer struct {
	nodev1.UnimplementedAgentServer
	version    string
	mode       nodev1.Mode
	containerd versionv1.VersionClient
}

func (s *agentServer) Status(ctx context.Context, _ *nodev1.StatusRequest) (*nodev1.StatusResponse, error) {
	return &nodev1.StatusResponse{
		Mode: s.mode,
		Components: []*nodev1.Component{
			{Name: "agent", Version: s.version, Health: nodev1.Health_HEALTH_HEALTHY},
			s.containerdStatus(ctx),
		},
	}, nil
}

// containerdStatus is containerd's version, if it answers, without the
// leading v, as a release pins it.
func (s *agentServer) containerdStatus(ctx context.Context) *nodev1.Component {
	ctx, cancel := context.WithTimeout(ctx, containerdTimeout)
	defer cancel()
	c := &nodev1.Component{Name: "containerd"}
	v, err := s.containerd.Version(ctx, &emptypb.Empty{})
	if err != nil {
		c.Health = nodev1.Health_HEALTH_UNHEALTHY
		c.Problem = err.Error()
		return c
	}
	c.Version = strings.TrimPrefix(v.Version, "v")
	c.Health = nodev1.Health_HEALTH_HEALTHY
	return c
}
