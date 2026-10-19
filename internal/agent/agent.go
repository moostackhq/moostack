//go:build linux

// Package agent is the node component that serves the node API, and
// listens for the node's peers with mTLS.
package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"time"

	versionv1 "github.com/containerd/containerd/api/services/version/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/hostlayout"
	"github.com/moostackhq/moostack/internal/identity"
	"github.com/moostackhq/moostack/internal/rootsocket"
)

// Config says where the agent finds what it serves.
type Config struct {
	// Version is the agent's own version.
	Version string
	// Socket is where to serve the node API, to root only.
	Socket string
	// NodeFile is the node's hostlayout.NodeConfig.
	NodeFile string
	// GenerationFile holds the generation the node last applied. Its
	// directory must exist.
	GenerationFile string
	// LockFile keeps the operation lock across restarts. Its directory
	// must exist.
	LockFile string
	// ContainerdSocket is where containerd serves its API.
	ContainerdSocket string
	// Identity is the node's identity, which it proves to its peers.
	Identity identity.Files
	// PeerPort is the port to listen for peers on, at the node's private
	// address.
	PeerPort uint16
}

// StopTimeout bounds how long the agent waits, once told to stop, for
// open calls to finish, such as a peer watching its health. Then it
// closes them.
const StopTimeout = 5 * time.Second

// containerdTimeout bounds asking containerd for its version.
const containerdTimeout = 2 * time.Second

// Serve serves the node API on cfg.Socket, and listens for peers at the
// node's private address, until ctx ends. Peers connect with mTLS, and
// only those with an identity from the cluster CA are accepted. It won't
// serve without a valid identity of its own.
func Serve(ctx context.Context, cfg Config) error {
	data, err := os.ReadFile(cfg.NodeFile)
	if err != nil {
		return err
	}
	node, err := hostlayout.ParseNodeConfig(data)
	if err != nil {
		return fmt.Errorf("%s: %w", cfg.NodeFile, err)
	}
	id, err := identity.Load(cfg.Identity)
	if err != nil {
		return err
	}
	containerd, err := grpc.NewClient("unix://"+cfg.ContainerdSocket, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer containerd.Close()

	peerListener, err := net.Listen("tcp", netip.AddrPortFrom(node.PrivateAddress, cfg.PeerPort).String())
	if err != nil {
		return fmt.Errorf("listening for peers: %w", err)
	}
	defer peerListener.Close()
	l, err := rootsocket.Listen(cfg.Socket)
	if err != nil {
		return err
	}
	server := grpc.NewServer()
	nodev1.RegisterAgentServer(server, &agentServer{version: cfg.Version, mode: modes[node.Mode], containerd: versionv1.NewVersionClient(containerd), generationFile: cfg.GenerationFile, lock: loadLock(cfg.LockFile)})
	peers := grpc.NewServer(grpc.Creds(credentials.NewTLS(id.ServerTLS())))
	healthpb.RegisterHealthServer(peers, health.NewServer())
	return serveAll(ctx, map[*grpc.Server]net.Listener{server: l, peers: peerListener})
}

// serveAll serves each server on its listener until ctx ends or one of
// them fails, and then stops them all, giving open calls StopTimeout to
// finish.
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
	force := time.AfterFunc(StopTimeout, func() {
		for s := range servers {
			s.Stop()
		}
	})
	defer force.Stop()
	for s := range servers {
		s.GracefulStop()
	}
	var err error
	for range servers {
		err = errors.Join(err, <-errs)
	}
	return err
}

// modes are the node API's modes by the node file's names for them.
var modes = map[string]nodev1.Mode{hostlayout.ModeDirect: nodev1.Mode_MODE_DIRECT}

type agentServer struct {
	nodev1.UnimplementedAgentServer
	version        string
	mode           nodev1.Mode
	containerd     versionv1.VersionClient
	generationFile string
	lock           *operationLock
}

func (s *agentServer) Status(ctx context.Context, _ *nodev1.StatusRequest) (*nodev1.StatusResponse, error) {
	res := &nodev1.StatusResponse{
		Mode: s.mode,
		Components: []*nodev1.Component{
			{Name: "agent", Version: s.version, Health: nodev1.Health_HEALTH_HEALTHY},
			s.containerdStatus(ctx),
		},
		Lock: s.lockStatus(),
	}
	if applied, err := s.applied(); err != nil {
		res.GenerationUnknown = err.Error()
	} else {
		res.Generation, res.Nodes = applied.Generation, applied.Nodes
	}
	return res, nil
}

func (s *agentServer) RecordGeneration(_ context.Context, req *nodev1.RecordGenerationRequest) (*nodev1.RecordGenerationResponse, error) {
	// The lock's mutex stays held until the generation is written, so
	// that the lease can't be broken or run out between the check and the
	// write. It orders recordings too.
	s.lock.mu.Lock()
	defer s.lock.mu.Unlock()
	if h := s.lock.current(); h == nil || req.Lease == "" || h.Lease != req.Lease {
		return nil, status.Error(codes.FailedPrecondition, "the operation's lease doesn't hold the node's operation lock")
	}
	// A node that can't tell the generation it applied accepts any, so
	// that a recording repairs it.
	if recorded, err := s.applied(); err == nil && req.Generation < recorded.Generation {
		return nil, status.Errorf(codes.FailedPrecondition, "the node has applied generation %d, later than %d", recorded.Generation, req.Generation)
	}
	data, err := json.Marshal(appliedGeneration{Generation: req.Generation, Nodes: req.Nodes})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "recording the generation: %v", err)
	}
	if err := writeFile(s.generationFile, append(data, '\n')); err != nil {
		return nil, status.Errorf(codes.Internal, "recording the generation: %v", err)
	}
	return &nodev1.RecordGenerationResponse{}, nil
}

// appliedGeneration is what the generation file holds: the generation of
// the cluster directory that the node last applied, and the names of the
// directory's nodes at that generation.
type appliedGeneration struct {
	Generation int64    `json:"generation"`
	Nodes      []string `json:"nodes"`
}

// applied is what the node last applied: generation 0 of no nodes if it
// has recorded none. It fails if the file can't be read, naming it.
func (s *agentServer) applied() (appliedGeneration, error) {
	var a appliedGeneration
	data, err := os.ReadFile(s.generationFile)
	if errors.Is(err, fs.ErrNotExist) {
		return a, nil
	}
	if err == nil {
		err = json.Unmarshal(data, &a)
	}
	if err != nil {
		return appliedGeneration{}, fmt.Errorf("reading %s: %w", s.generationFile, err)
	}
	return a, nil
}

// writeFile replaces the file at path with data, which a crash leaves
// either whole or not written at all. Once it returns, the new file
// survives a crash: its directory is synced after the rename.
func writeFile(path string, data []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	_, err = tmp.Write(data)
	if err == nil {
		err = tmp.Sync()
	}
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(tmp.Name(), path)
	}
	if err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

// syncDir syncs the directory at path, so that the renames and removals
// in it survive a crash.
func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	err = dir.Sync()
	if closeErr := dir.Close(); err == nil {
		err = closeErr
	}
	return err
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
