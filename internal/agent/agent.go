//go:build linux

// Package agent is the node component that serves the node API, runs
// containers, the cluster services among them, carries calls to their
// APIs, and listens for the node's peers with mTLS. On a node that has
// joined a control plane, it serves the node API to the control plane
// over a tunnel it dials, and its local socket serves only status and
// break-glass detach.
package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	controlplanev1 "github.com/moostackhq/moostack/api/controlplane/v1"
	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	servicev1 "github.com/moostackhq/moostack/api/service/v1"
	"github.com/moostackhq/moostack/internal/abslot"
	"github.com/moostackhq/moostack/internal/buildid"
	"github.com/moostackhq/moostack/internal/controlplane"
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
	// ContainersFile keeps the containers the node runs, the cluster
	// services among them. Its directory must exist.
	ContainersFile string
	// ContainersDir holds the socket directory of each container that
	// serves an API, and ContainerLogDir the containers' output.
	ContainersDir, ContainerLogDir string
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
// that aren't running are started. On a node in control plane mode, it
// also dials the control plane, and serves it the node API and the
// cluster services over that tunnel, while cfg.Socket serves only status
// and break-glass detach.
func Serve(ctx context.Context, cfg Config) error {
	node, err := hostlayout.ReadNodeConfig(cfg.NodeFile)
	if err != nil {
		return err
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
	containers, err := loadContainers(ctx, client, gw, cfg)
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
	agent := &agentServer{node: node, nodeFile: cfg.NodeFile, containerd: client, shim: filepath.Join(cfg.ContainerdBinDir, hostlayout.ContainerdShim), runc: cfg.Runc, containers: containers, generationFile: cfg.GenerationFile, lock: loadLock(cfg.LockFile), removed: removed, updater: updater, version: cfg.Version}
	agent.controlPlane.Store(node.Mode == hostlayout.ModeControlPlane)
	agent.containerdBinaries = &containerdBinaries{dir: cfg.ContainerdBinDir, files: cfg.ContainerdFiles, restart: cfg.RestartContainerd, wait: agent.waitContainerd}
	// The builds are hashed from the start, in the background, so that
	// status says them by the time an operation asks.
	agent.build(map[string]string{"agent": executable()})
	agent.build(agent.containerdFiles())
	agent.build(map[string]string{"runc": agent.runc})
	server := grpc.NewServer(append(gw.ServerOptions(servicev1.CallerKind_CALLER_KIND_OPERATOR),
		controlplane.Lockout(agent.joined, "status and break-glass detach", nodev1.Agent_Status_FullMethodName, nodev1.Agent_Detach_FullMethodName)...)...)
	nodev1.RegisterAgentServer(server, agent)
	peers := grpc.NewServer(grpc.Creds(newPeerCredentials(id, removed)))
	healthpb.RegisterHealthServer(peers, health.NewServer())
	servers := map[*grpc.Server]net.Listener{server: l, peers: peerListener}
	if node.Mode == hostlayout.ModeControlPlane {
		agent.tunnel = controlplane.Listen(controlplane.Tunnel{Address: node.ControlPlane, Identity: id, Protocol: controlplanev1.ProtocolAgent, Log: log.Printf})
		tunnel := grpc.NewServer(slices.Concat(gw.ServerOptions(servicev1.CallerKind_CALLER_KIND_CONTROL_PLANE), controlplane.ServerOptions(),
			[]grpc.ServerOption{grpc.ChainUnaryInterceptor(refuseDetach)})...)
		nodev1.RegisterAgentServer(tunnel, agent)
		servers[tunnel] = agent.tunnel
	}
	go containers.StartAll(ctx)
	go containers.KeepDeletingUnused(ctx, unusedRetry, changeTimeout)
	go recoverUpdater(ctx, updater)
	return serveAll(ctx, servers)
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

type agentServer struct {
	nodev1.UnimplementedAgentServer
	version string
	// node is what the node file said when the agent started, and
	// nodeFile the file.
	node     hostlayout.NodeConfig
	nodeFile string
	// controlPlane says whether the node is in control plane mode, rather
	// than direct mode, until a detach.
	controlPlane atomic.Bool
	// tunnel is the tunnel to the control plane, nil on a node that
	// started in direct mode.
	tunnel *controlplane.Listener
	// detaching is held by a detach.
	detaching  sync.Mutex
	containerd *containerd.Client
	// shim is containerd's shim, which must be at containerd's version.
	shim               string
	runc               string
	containers         *containers
	generationFile     string
	lock               *operationLock
	removed            *removedIdentities
	updater            *abslot.Component
	containerdBinaries *containerdBinaries
	// builds tells the builds of the node components' files.
	builds buildid.Cache
}

func (s *agentServer) Status(ctx context.Context, _ *nodev1.StatusRequest) (*nodev1.StatusResponse, error) {
	res := &nodev1.StatusResponse{
		Mode: s.mode(),
		Components: []*nodev1.Component{
			{Name: "agent", Version: s.version, Build: s.build(map[string]string{"agent": executable()}), Health: nodev1.Health_HEALTH_HEALTHY},
			s.containerdStatus(ctx),
			s.runcStatus(ctx),
		},
		Lock:       s.lockStatus(),
		Containers: s.containers.Report(ctx),
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

// writeFile replaces the file at path with data, readable by its owner
// alone, which a crash leaves either whole or not written at all. Once it
// returns, the new file survives a crash: its directory is synced after
// the rename.
func writeFile(path string, data []byte) error {
	return writeFileMode(path, data, 0o600)
}

// writeFileMode is writeFile, with the file's mode perm.
func writeFileMode(path string, data []byte, perm os.FileMode) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	err = tmp.Chmod(perm)
	if err == nil {
		_, err = tmp.Write(data)
	}
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
	c := &nodev1.Component{Name: "containerd", Build: s.build(s.containerdFiles()), Health: nodev1.Health_HEALTH_UNHEALTHY}
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

// build is the build of the files at the paths, by the names a release
// gives them, empty until they are hashed, which status doesn't wait
// for, or if they can't be read.
func (s *agentServer) build(paths map[string]string) string {
	return s.builds.Peek(paths)
}

// containerdFiles are containerd's files, by name, at their paths.
func (s *agentServer) containerdFiles() map[string]string {
	files := map[string]string{}
	for _, f := range s.containerdBinaries.files {
		files[f] = filepath.Join(s.containerdBinaries.dir, f)
	}
	return files
}

// executable is the file this program runs from, empty if it can't
// tell.
func executable() string {
	path, err := os.Executable()
	if err != nil {
		return ""
	}
	return path
}

// runcTimeout bounds asking runc for its version.
const runcTimeout = 2 * time.Second

// runcStatus is runc's version, as it says when run, healthy if it runs.
func (s *agentServer) runcStatus(ctx context.Context) *nodev1.Component {
	ctx, cancel := context.WithTimeout(ctx, runcTimeout)
	defer cancel()
	c := &nodev1.Component{Name: "runc", Build: s.build(map[string]string{"runc": s.runc}), Health: nodev1.Health_HEALTH_UNHEALTHY}
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
	// The first request carries the lease; a stream without one has none.
	first, err := stream.Recv()
	if err != nil && err != io.EOF {
		return err
	}
	fence := s.fence(first.GetLease())
	// Refused at once, the archive isn't read.
	if err := fence(); err != nil {
		return err
	}
	// The archive is read whole before the import waits for its turn, so
	// that a client that stalls holds up no change of the containers.
	archive, err := spool(filepath.Dir(s.containers.file), first, stream)
	if err != nil {
		return err
	}
	defer archive.Close()
	var refused error
	images, err := s.containers.Import(stream.Context(), archive, func() error { refused = fence(); return refused })
	if refused != nil {
		return refused
	}
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "importing the image: %v", err)
	}
	return stream.SendAndClose(&nodev1.ImportImageResponse{Images: images})
}

// spool writes the chunks of first and of the rest of stream to a file
// in dir, which no other process can open, since it is unlinked at once,
// so that nothing is left of it once it is closed or the agent stops.
// It returns the file, read from its start.
func spool(dir string, first *nodev1.ImportImageRequest, stream nodev1.Agent_ImportImageServer) (*os.File, error) {
	f, err := os.CreateTemp(dir, ".import-")
	if err != nil {
		return nil, status.Errorf(codes.Internal, "keeping the image's archive: %v", err)
	}
	if err := os.Remove(f.Name()); err != nil {
		f.Close()
		return nil, status.Errorf(codes.Internal, "keeping the image's archive: %v", err)
	}
	for req := first; req != nil; {
		if _, err := f.Write(req.Chunk); err != nil {
			f.Close()
			return nil, status.Errorf(codes.Internal, "keeping the image's archive: %v", err)
		}
		if req, err = stream.Recv(); err == io.EOF {
			break
		} else if err != nil {
			f.Close()
			return nil, err
		}
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		f.Close()
		return nil, status.Errorf(codes.Internal, "keeping the image's archive: %v", err)
	}
	return f, nil
}

func (s *agentServer) PullImage(ctx context.Context, req *nodev1.PullImageRequest) (*nodev1.PullImageResponse, error) {
	image, digest, err := s.containers.Pull(ctx, req.Reference, s.fence(req.Lease))
	if err != nil {
		return nil, asStatus(err, "pulling "+req.Reference)
	}
	return &nodev1.PullImageResponse{Image: image, Digest: digest}, nil
}

func (s *agentServer) Run(ctx context.Context, req *nodev1.RunRequest) (*nodev1.RunResponse, error) {
	if req.Container == nil {
		return nil, status.Error(codes.InvalidArgument, "no container")
	}
	c := containerOf(req.Container)
	// The run goes on if the call ends, as when mooctl is stopped, since
	// it may have removed the container's old one already.
	ctx = context.WithoutCancel(ctx)
	started, err := s.containers.Run(ctx, c, s.fence(req.Lease))
	if err != nil {
		return nil, asStatus(err, "running "+c.Name)
	}
	return &nodev1.RunResponse{Started: started}, nil
}

func (s *agentServer) Remove(ctx context.Context, req *nodev1.RemoveRequest) (*nodev1.RemoveResponse, error) {
	// The removal goes on if the call ends, as a run does.
	ctx = context.WithoutCancel(ctx)
	removed, err := s.containers.Remove(ctx, req.Name, s.fence(req.Lease))
	if err != nil {
		return nil, asStatus(err, "removing "+req.Name)
	}
	return &nodev1.RemoveResponse{Removed: removed}, nil
}

// asStatus is err as a call answers it: as it is if it has a code, and
// as INTERNAL, saying what failed, if it has none.
func asStatus(err error, what string) error {
	if _, ok := status.FromError(err); ok {
		return err
	}
	return status.Errorf(codes.Internal, "%s: %v", what, err)
}

// fence fails with FAILED_PRECONDITION unless lease holds the operation
// lock when it is called.
func (s *agentServer) fence(lease string) func() error {
	return func() error { return s.lock.holds(lease) }
}
