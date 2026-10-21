//go:build linux

// Package agent is the node component that serves the node API, runs
// the cluster services and carries calls to them, and listens for the
// node's peers with mTLS.
package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	servicev1 "github.com/moostackhq/moostack/api/service/v1"
	"github.com/moostackhq/moostack/internal/abslot"
	"github.com/moostackhq/moostack/internal/gateway"
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
	// Runc is runc's binary, whose version the agent reports. containerd
	// finds it on the PATH its unit gives it.
	Runc string
	// ServicesFile keeps the cluster services the node runs. Its
	// directory must exist.
	ServicesFile string
	// ServicesDir holds each cluster service's socket directory, and
	// ServiceLogDir their output.
	ServicesDir, ServiceLogDir string
	// Identity is the node's identity, which it proves to its peers.
	Identity identity.Files
	// RemovedIdentitiesFile keeps the identities removed from the
	// cluster, which the node refuses as peers. Its directory must exist.
	RemovedIdentitiesFile string
	// PeerPort is the port to listen for peers on, at the node's private
	// address.
	PeerPort uint16
	// Updater is the updater's binary, a symbolic link to the active one
	// of UpdaterSlots, relative, in the same directory.
	Updater string
	// UpdaterSlots are the updater's A/B slots.
	UpdaterSlots [2]string
	// UpdaterSwapFile records a swap of the updater that hasn't been seen
	// healthy. Its directory must exist.
	UpdaterSwapFile string
	// UpdaterSocket is where the updater serves its node API.
	UpdaterSocket string
	// RestartUpdater restarts the updater, from the binary Updater links
	// to.
	RestartUpdater func(ctx context.Context) error
	// ContainerdBinDir holds containerd's binaries, ContainerdFiles by
	// name, which an update replaces.
	ContainerdBinDir string
	ContainerdFiles  []string
	// RestartContainerd restarts containerd, leaving the containers'
	// shims running.
	RestartContainerd func(ctx context.Context) error
}

// StopTimeout bounds how long the agent waits, once told to stop, for
// open calls to finish, such as a peer watching its health. Then it
// closes them.
const StopTimeout = 5 * time.Second

// containerdTimeout bounds asking containerd for its version.
const containerdTimeout = 2 * time.Second

// Serve serves the node API on cfg.Socket, and listens for peers at the
// node's private address, until ctx ends. Its graceful stop waits for
// an update of the updater that runs, past StopTimeout, since the update
// goes on once its call ends. If a swap of the updater is recorded, as
// when the agent crashed in one, it switches the updater back meanwhile.
// Peers connect with mTLS, and only those with an identity from the
// cluster CA that the node doesn't hold as removed are accepted. It
// won't serve without a valid identity of its own, or with removed
// identities it can't read. Calls on cfg.Socket that aren't the node
// API's go to the cluster services, as an operator's, and the services
// that aren't running are started.
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
	removed, err := loadRemoved(cfg.RemovedIdentitiesFile)
	if err != nil {
		return err
	}
	client, err := containerd.New(cfg.ContainerdSocket, containerd.WithTimeout(containerdTimeout))
	if err != nil {
		return err
	}
	defer client.Close()
	gw := gateway.New()
	defer gw.Close()
	services, err := loadServices(ctx, client, gw, cfg.ServicesFile, cfg.ServicesDir, cfg.ServiceLogDir)
	if err != nil {
		return err
	}

	peerListener, err := net.Listen("tcp", netip.AddrPortFrom(node.PrivateAddress, cfg.PeerPort).String())
	if err != nil {
		return fmt.Errorf("listening for peers: %w", err)
	}
	defer peerListener.Close()
	l, err := rootsocket.Listen(cfg.Socket)
	if err != nil {
		return err
	}
	healthy, err := updaterHealthy(cfg.UpdaterSocket)
	if err != nil {
		return err
	}
	updater := &abslot.Component{
		Name:     "updater",
		Swapper:  "the agent",
		Link:     cfg.Updater,
		Slots:    cfg.UpdaterSlots,
		SwapFile: cfg.UpdaterSwapFile,
		Restart:  cfg.RestartUpdater,
		Healthy:  healthy,
	}
	server := grpc.NewServer(gw.ServerOptions(servicev1.CallerKind_CALLER_KIND_OPERATOR)...)
	agent := &agentServer{version: cfg.Version, mode: modes[node.Mode], containerd: client, shim: filepath.Join(cfg.ContainerdBinDir, hostlayout.ContainerdShim), runc: cfg.Runc, services: services, generationFile: cfg.GenerationFile, lock: loadLock(cfg.LockFile), removed: removed, updater: updater}
	agent.containerdBinaries = &containerdBinaries{dir: cfg.ContainerdBinDir, files: cfg.ContainerdFiles, restart: cfg.RestartContainerd, wait: agent.waitContainerd}
	nodev1.RegisterAgentServer(server, agent)
	peers := grpc.NewServer(grpc.Creds(newPeerCredentials(id, removed)))
	healthpb.RegisterHealthServer(peers, health.NewServer())
	go services.StartAll(ctx)
	go recoverUpdater(ctx, updater)
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
	version    string
	mode       nodev1.Mode
	containerd *containerd.Client
	// shim is containerd's shim, which must be at containerd's version.
	shim               string
	runc               string
	services           *services
	generationFile     string
	lock               *operationLock
	removed            *removedIdentities
	updater            *abslot.Component
	containerdBinaries *containerdBinaries
}

func (s *agentServer) Status(ctx context.Context, _ *nodev1.StatusRequest) (*nodev1.StatusResponse, error) {
	res := &nodev1.StatusResponse{
		Mode: s.mode,
		Components: []*nodev1.Component{
			{Name: "agent", Version: s.version, Health: nodev1.Health_HEALTH_HEALTHY},
			s.containerdStatus(ctx),
			s.runcStatus(ctx),
		},
		Lock:     s.lockStatus(),
		Services: s.services.Status(ctx),
	}
	if applied, err := s.applied(); err != nil {
		res.GenerationUnknown = err.Error()
	} else {
		res.Generation, res.Nodes = applied.Generation, applied.Nodes
	}
	return res, nil
}

func (s *agentServer) RecordGeneration(_ context.Context, req *nodev1.RecordGenerationRequest) (*nodev1.RecordGenerationResponse, error) {
	err := s.lock.fenced(req.Lease, func() error {
		// A node that can't tell the generation it applied accepts any, so
		// that a recording repairs it.
		if recorded, err := s.applied(); err == nil && req.Generation < recorded.Generation {
			return status.Errorf(codes.FailedPrecondition, "the node has applied generation %d, later than %d", recorded.Generation, req.Generation)
		}
		data, err := json.Marshal(appliedGeneration{Generation: req.Generation, Nodes: req.Nodes})
		if err == nil {
			err = writeFile(s.generationFile, append(data, '\n'))
		}
		if err != nil {
			return status.Errorf(codes.Internal, "recording the generation: %v", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
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
// leading v, as a release pins it. It is healthy if its shim is at the
// same version: binaries that are part old and part new, as when the
// host restarted while an update put them in place, aren't.
func (s *agentServer) containerdStatus(ctx context.Context) *nodev1.Component {
	ctx, cancel := context.WithTimeout(ctx, containerdTimeout)
	defer cancel()
	c := &nodev1.Component{Name: "containerd", Health: nodev1.Health_HEALTH_UNHEALTHY}
	v, err := s.containerd.Version(ctx)
	if err != nil {
		c.Problem = err.Error()
		return c
	}
	c.Version = strings.TrimPrefix(v.Version, "v")
	shim, err := s.shimVersion(ctx)
	switch {
	case err != nil:
		c.Problem = err.Error()
	case shim != c.Version:
		c.Problem = fmt.Sprintf("its shim, %s, is at %s", hostlayout.ContainerdShim, shim)
	default:
		c.Health = nodev1.Health_HEALTH_HEALTHY
	}
	return c
}

// shimVersion is the version of containerd's shim, as it says when run,
// without the leading v.
func (s *agentServer) shimVersion(ctx context.Context) (string, error) {
	out, err := exec.CommandContext(ctx, s.shim, "-v").Output()
	if err != nil {
		return "", fmt.Errorf("running %s -v: %v", s.shim, err)
	}
	for _, line := range strings.Split(string(out), "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), "Version:"); ok {
			return strings.TrimPrefix(strings.TrimSpace(v), "v"), nil
		}
	}
	return "", fmt.Errorf("%s -v doesn't say its version", s.shim)
}

// runcTimeout bounds asking runc for its version.
const runcTimeout = 2 * time.Second

// runcStatus is runc's version, as it says when run, healthy if it runs.
func (s *agentServer) runcStatus(ctx context.Context) *nodev1.Component {
	ctx, cancel := context.WithTimeout(ctx, runcTimeout)
	defer cancel()
	c := &nodev1.Component{Name: "runc", Health: nodev1.Health_HEALTH_UNHEALTHY}
	out, err := exec.CommandContext(ctx, s.runc, "--version").Output()
	if err != nil {
		c.Problem = fmt.Sprintf("running %s --version: %v", s.runc, err)
		return c
	}
	first, _, _ := strings.Cut(string(out), "\n")
	version, ok := strings.CutPrefix(first, "runc version ")
	if !ok {
		c.Problem = fmt.Sprintf("%s --version says %q", s.runc, first)
		return c
	}
	c.Version, c.Health = version, nodev1.Health_HEALTH_HEALTHY
	return c
}

func (s *agentServer) ImportImage(stream nodev1.Agent_ImportImageServer) error {
	r, w := io.Pipe()
	go func() {
		for {
			req, err := stream.Recv()
			if err == io.EOF {
				w.Close()
				return
			}
			if err != nil {
				w.CloseWithError(err)
				return
			}
			if _, err := w.Write(req.Chunk); err != nil {
				return
			}
		}
	}()
	images, err := s.services.Import(stream.Context(), r)
	// An import that stopped early leaves the rest of the stream unread.
	r.CloseWithError(errors.New("the import ended"))
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "importing the image: %v", err)
	}
	return stream.SendAndClose(&nodev1.ImportImageResponse{Images: images})
}

func (s *agentServer) RunService(ctx context.Context, req *nodev1.RunServiceRequest) (*nodev1.RunServiceResponse, error) {
	if req.Service == nil {
		return nil, status.Error(codes.InvalidArgument, "no service")
	}
	if err := s.services.Run(ctx, req.Service); err != nil {
		if _, ok := status.FromError(err); ok {
			return nil, err
		}
		return nil, status.Errorf(codes.Internal, "running %s: %v", req.Service.Name, err)
	}
	return &nodev1.RunServiceResponse{}, nil
}
