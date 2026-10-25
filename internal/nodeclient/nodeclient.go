// Package nodeclient reaches a node's node API over SSH.
package nodeclient

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/hostlayout"
	"github.com/moostackhq/moostack/internal/release"
	"github.com/moostackhq/moostack/internal/rootsocket"
	"github.com/moostackhq/moostack/internal/sshclient"
)

// Relay is the command that connects to a unix socket on a node through
// sudo, for sshclient.Options.Relay. The relay is that of the program
// that serves the socket, so that a broken agent doesn't keep mooctl from
// the updater. It panics for a socket no program serves, which mooctl
// never dials.
func Relay(socket string) string {
	program, ok := hostlayout.Server(socket)
	if !ok {
		panic("nodeclient: no program serves " + socket)
	}
	return program + " relay " + sshclient.ShellQuote(socket)
}

// Node is a connection to a node's agent and updater over c, which SSH
// authenticates. Close it when done.
type Node struct {
	Agent                  nodev1.AgentClient
	Updater                nodev1.UpdaterClient
	agentConn, updaterConn *grpc.ClientConn
}

// Dial connects to the agent's and the updater's sockets on the node
// over c. It connects to each on first use, and again after a dropped
// connection.
func Dial(c *sshclient.Client) (*Node, error) {
	agentConn, err := dialUnix(c, hostlayout.AgentSocket)
	if err != nil {
		return nil, err
	}
	updaterConn, err := dialUnix(c, hostlayout.UpdaterSocket)
	if err != nil {
		agentConn.Close()
		return nil, err
	}
	return &Node{
		Agent:       nodev1.NewAgentClient(agentConn),
		Updater:     nodev1.NewUpdaterClient(updaterConn),
		agentConn:   agentConn,
		updaterConn: updaterConn,
	}, nil
}

// dialUnix is a gRPC connection to socket on the node over c.
func dialUnix(c *sshclient.Client, socket string) (*grpc.ClientConn, error) {
	return dial(socket, func(ctx context.Context) (net.Conn, error) { return c.DialUnix(ctx, socket) })
}

// dial is a connection to socket, called name, through d.
func dial(name string, d func(ctx context.Context) (net.Conn, error)) (*grpc.ClientConn, error) {
	return grpc.NewClient("passthrough:///"+name,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return d(ctx) }),
		grpc.WithChainUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			return quotedStatus(invoker(ctx, method, req, reply, cc, opts...))
		}),
		grpc.WithChainStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			// A stream that fails to open fails here, before the node says
			// anything; what it answers comes with the stream's messages.
			s, err := streamer(ctx, desc, cc, method, opts...)
			if err != nil {
				return nil, err
			}
			return quotedStream{s}, nil
		}),
		rootsocket.Reconnect)
}

// quotedStatus is err, with the message of the status a node answered
// with made Printable, since mooctl repeats it to the operator.
func quotedStatus(err error) error {
	if err == nil {
		return nil
	}
	s, ok := status.FromError(err)
	if !ok || Printable(s.Message()) == s.Message() {
		return err
	}
	p := s.Proto()
	p.Message = Printable(p.Message)
	return status.ErrorProto(p)
}

// quotedStream is a stream whose errors are made Printable.
type quotedStream struct{ grpc.ClientStream }

func (s quotedStream) SendMsg(m any) error { return quotedStatus(s.ClientStream.SendMsg(m)) }

func (s quotedStream) RecvMsg(m any) error { return quotedStatus(s.ClientStream.RecvMsg(m)) }

// Close closes the connections.
func (n *Node) Close() error {
	return errors.Join(n.agentConn.Close(), n.updaterConn.Close())
}

// Status is what a node's agent and updater report together.
type Status struct {
	// AgentErr says why the agent didn't answer, nil if it did.
	AgentErr error
	// UpdaterErr says why the updater didn't answer, nil if it did.
	UpdaterErr error
	// Mode is unspecified if the agent didn't answer.
	Mode nodev1.Mode
	// UpdaterMode is the mode the updater serves in, unspecified if it
	// didn't answer or is of a release before it said.
	UpdaterMode nodev1.Mode
	// Generation is the generation of the cluster directory that the node
	// last applied, 0 if none, and Nodes are the names of that
	// directory's nodes. Both are unknown, and zero, if the agent didn't
	// answer or GenerationUnknown says why the node can't tell.
	Generation int64
	Nodes      []string
	// GenerationUnknown says why the node can't tell the generation it
	// last applied, empty if it can.
	GenerationUnknown string
	// Lock is the node's operation lock, nil if no lease holds it or the
	// agent didn't answer.
	Lock *Lock
	// Components are the agent's components and the updater. The agent
	// or the updater that didn't answer is unhealthy, with why as its
	// problem, and the components only the agent reports are missing.
	Components []*nodev1.Component
	// Containers are the containers the node keeps, the cluster services
	// among them, none if the agent didn't answer.
	Containers []*nodev1.ContainerStatus
}

// Status asks the agent and the updater for their status, both at once,
// so that one that hangs doesn't keep the other from answering.
func (n *Node) Status(ctx context.Context) Status {
	var (
		wg                   sync.WaitGroup
		agent                *nodev1.StatusResponse
		updater              *nodev1.UpdaterStatusResponse
		agentErr, updaterErr error
	)
	wg.Go(func() { agent, agentErr = n.Agent.Status(ctx, &nodev1.StatusRequest{}) })
	wg.Go(func() { updater, updaterErr = n.Updater.Status(ctx, &nodev1.UpdaterStatusRequest{}) })
	wg.Wait()
	if updaterErr == nil && updater.Updater == nil {
		updaterErr = errors.New("it reported nothing about itself")
	}
	var s Status
	if agentErr != nil {
		s.AgentErr = agentErr
		s.Components = append(s.Components, notAnswering("agent", agentErr))
	} else {
		s.Mode = agent.Mode
		s.Generation, s.Nodes, s.GenerationUnknown = agent.Generation, agent.Nodes, agent.GenerationUnknown
		s.Lock = lockOf(agent.Lock)
		s.Components = append(s.Components, agent.Components...)
		s.Containers = agent.Containers
	}
	if updaterErr != nil {
		s.UpdaterErr = updaterErr
		s.Components = append(s.Components, notAnswering("updater", updaterErr))
	} else {
		s.Components = append(s.Components, updater.Updater)
		s.UpdaterMode = updater.Mode
	}
	s.GenerationUnknown = Printable(s.GenerationUnknown)
	for i, name := range s.Nodes {
		s.Nodes[i] = Printable(name)
	}
	for _, c := range s.Components {
		c.Name, c.Version, c.Build, c.Problem = Printable(c.Name), Printable(c.Version), Printable(c.Build), Printable(c.Problem)
	}
	for _, c := range s.Containers {
		c.Name, c.Image, c.Digest, c.Problem = Printable(c.Name), Printable(c.Image), Printable(c.Digest), Printable(c.Problem)
	}
	return s
}

// Services are the cluster services among the node's containers, as
// release.MayBeService tells them, each as a component named for its
// service, at the version of its image, as healthy as its API: one whose
// image the node can't tell at a version unknown.
func (s Status) Services() []*nodev1.Component {
	var services []*nodev1.Component
	for _, c := range s.Containers {
		if release.MayBeService(c.Name, c.Image) {
			name, _ := release.ServiceName(c.Name)
			// One whose image the node can't tell has no version.
			version, _ := release.ServiceVersion(c.Name, c.Image)
			services = append(services, &nodev1.Component{Name: name, Version: version, Build: c.Digest, Health: c.Health, Problem: c.Problem})
		}
	}
	return services
}

// Printable is s, a text that a node reported, quoted if it holds a rune
// that isn't printable, such as a line break or an escape, or isn't
// valid UTF-8, whose stray bytes some terminals take as controls, so that
// whatever repeats it to the operator shows it as one piece of text,
// which can't add lines of its own or move the terminal's cursor. A text
// that is printable is as it was.
func Printable(s string) string {
	if !utf8.ValidString(s) || strings.ContainsFunc(s, func(r rune) bool { return !strconv.IsPrint(r) }) {
		return strconv.Quote(s)
	}
	return s
}

// KnownGeneration is the generation the node last applied, and whether
// it is known: whether the agent answered and could tell.
func (s Status) KnownGeneration() (int64, bool) {
	if s.AgentErr != nil || s.GenerationUnknown != "" {
		return 0, false
	}
	return s.Generation, true
}

// RecordGeneration has the agent record generation, of a cluster
// directory of the nodes called nodes, as the one the node has applied,
// for the operation whose lease holds the node's operation lock.
func (n *Node) RecordGeneration(ctx context.Context, generation int64, nodes []string, lease string) error {
	_, err := n.Agent.RecordGeneration(ctx, &nodev1.RecordGenerationRequest{Generation: generation, Nodes: nodes, Lease: lease})
	return err
}

// RemoveIdentities has the agent add identities, by their fingerprints,
// to those the node holds as removed and refuses as peers, for the
// operation whose lease holds the node's operation lock.
func (n *Node) RemoveIdentities(ctx context.Context, identities []string, lease string) error {
	_, err := n.Agent.RemoveIdentities(ctx, &nodev1.RemoveIdentitiesRequest{Identities: identities, Lease: lease})
	return err
}

// Lock is the operation lock, as one operation's lease holds it.
type Lock struct {
	// Lease identifies the operation's hold on the lock.
	Lease string
	// Holder is who runs the operation, and Machine where.
	Holder, Machine string
	// Operation is the command the operation runs, such as "cluster
	// update", empty if the node didn't say.
	Operation string
	// Since is when the node granted the lease, and Until when it runs
	// out on the node unless renewed.
	Since, Until time.Time
}

// lockOf is l, nil if l is.
func lockOf(l *nodev1.Lock) *Lock {
	if l == nil {
		return nil
	}
	return &Lock{Lease: l.Lease, Holder: Printable(l.Holder), Machine: Printable(l.Machine), Operation: Printable(l.Operation), Since: l.Since.AsTime(), Until: l.Until.AsTime()}
}

// AcquireLock asks the agent for the node's operation lock for lock's
// lease, or to renew it, for ttl. It returns the lock as it now is: held
// by lock's lease if granted, or by another.
func (n *Node) AcquireLock(ctx context.Context, lock Lock, ttl time.Duration) (Lock, error) {
	res, err := n.Agent.AcquireLock(ctx, &nodev1.AcquireLockRequest{
		Lock: &nodev1.Lock{Lease: lock.Lease, Holder: lock.Holder, Machine: lock.Machine, Operation: lock.Operation},
		Ttl:  durationpb.New(ttl),
	})
	if err != nil {
		return Lock{}, err
	}
	if res.Lock == nil {
		return Lock{}, errors.New("the agent reported no lock")
	}
	return *lockOf(res.Lock), nil
}

// RenewLock asks the agent to renew the node's operation lock for lease,
// for ttl, only if lease still holds it. It returns the lock as it now
// is, nil if free.
func (n *Node) RenewLock(ctx context.Context, lease string, ttl time.Duration) (*Lock, error) {
	res, err := n.Agent.RenewLock(ctx, &nodev1.RenewLockRequest{Lease: lease, Ttl: durationpb.New(ttl)})
	if err != nil {
		return nil, err
	}
	return lockOf(res.Lock), nil
}

// ReleaseLock has the agent release the node's operation lock, if lease
// holds it.
func (n *Node) ReleaseLock(ctx context.Context, lease string) error {
	_, err := n.Agent.ReleaseLock(ctx, &nodev1.ReleaseLockRequest{Lease: lease})
	return err
}

// BreakLock has the agent release the node's operation lock, whichever
// lease holds it, and returns the lock it broke, nil if none.
func (n *Node) BreakLock(ctx context.Context) (*Lock, error) {
	res, err := n.Agent.BreakLock(ctx, &nodev1.BreakLockRequest{})
	if err != nil {
		return nil, err
	}
	return lockOf(res.Broken), nil
}

// notAnswering is the program that didn't answer with err.
func notAnswering(name string, err error) *nodev1.Component {
	return &nodev1.Component{Name: name, Health: nodev1.Health_HEALTH_UNHEALTHY, Problem: fmt.Sprintf("the %s doesn't answer: %v", name, err)}
}

// ModeName is mode as mooctl shows it, such as "control plane": "-" when
// the mode isn't known.
func ModeName(mode nodev1.Mode) string {
	if mode == nodev1.Mode_MODE_UNSPECIFIED {
		return "-"
	}
	return strings.ReplaceAll(strings.ToLower(strings.TrimPrefix(mode.String(), "MODE_")), "_", " ")
}

// HealthName is health as mooctl shows it: "unknown" when the health
// isn't known.
func HealthName(h nodev1.Health) string {
	if h == nodev1.Health_HEALTH_UNSPECIFIED {
		return "unknown"
	}
	return strings.ToLower(strings.TrimPrefix(h.String(), "HEALTH_"))
}

// importChunk is how much of an image archive each request carries.
const importChunk = 1 << 20

// ImportImage has agent import the OCI image layout archive data into
// the node's containerd, for the operation whose lease holds the node's
// operation lock, and returns the names of the images imported.
func ImportImage(ctx context.Context, agent nodev1.AgentClient, data []byte, lease string) ([]string, error) {
	stream, err := agent.ImportImage(ctx)
	if err != nil {
		return nil, err
	}
	// The first request carries the lease, even for an empty archive.
	req := &nodev1.ImportImageRequest{Lease: lease}
	for {
		n := min(len(data), importChunk)
		req.Chunk = data[:n]
		if err := stream.Send(req); err != nil {
			// The agent ended the call; CloseAndRecv says why.
			break
		}
		data = data[n:]
		if len(data) == 0 {
			break
		}
		req = &nodev1.ImportImageRequest{}
	}
	res, err := stream.CloseAndRecv()
	if err != nil {
		return nil, err
	}
	images := make([]string, len(res.Images))
	for i, image := range res.Images {
		images[i] = Printable(image)
	}
	return images, nil
}

// ImportImage has the agent import the OCI image layout archive into
// the node's containerd, for the operation whose lease holds the node's
// operation lock, and returns the names of the images imported.
func (n *Node) ImportImage(ctx context.Context, archive []byte, lease string) ([]string, error) {
	return ImportImage(ctx, n.Agent, archive, lease)
}

// Remove has the agent stop and delete the container called name, for
// the operation whose lease holds the node's operation lock.
func (n *Node) Remove(ctx context.Context, name, lease string) error {
	_, err := n.Agent.Remove(ctx, &nodev1.RemoveRequest{Name: name, Lease: lease})
	return err
}

// Run has the agent run c from its image, replacing the container of its
// name, for the operation whose lease holds the node's operation lock.
func (n *Node) Run(ctx context.Context, c *nodev1.Container, lease string) error {
	_, err := n.Agent.Run(ctx, &nodev1.RunRequest{Container: c, Lease: lease})
	return err
}

// UpdateAgent has the updater replace the agent with binary, the agent at
// version, which must become healthy within healthTimeout.
func (n *Node) UpdateAgent(ctx context.Context, version string, binary []byte, healthTimeout time.Duration) error {
	return UpdateAgent(ctx, n.Updater, version, binary, healthTimeout)
}

// updateChunk is how much of a binary each request of an update
// carries.
const updateChunk = 1 << 20

// sendChunks sends data with send, updateChunk at a time, and at least
// once. It stops at the first send that fails, since the call ended,
// which the call's answer says why.
func sendChunks(data []byte, send func(chunk []byte) error) {
	for {
		n := min(len(data), updateChunk)
		if send(data[:n]) != nil {
			return
		}
		data = data[n:]
		if len(data) == 0 {
			return
		}
	}
}

// UpdateAgent has updater replace the agent with binary, the agent at
// version, which must become healthy within healthTimeout. It fails
// saying so if the updater is from a release that can't.
func UpdateAgent(ctx context.Context, updater nodev1.UpdaterClient, version string, binary []byte, healthTimeout time.Duration) error {
	stream, err := updater.UpdateAgent(ctx)
	if err != nil {
		return err
	}
	update := &nodev1.AgentUpdate{Version: version, Sha256: sha256Hex(binary), HealthTimeout: durationpb.New(healthTimeout)}
	sendChunks(binary, func(chunk []byte) error {
		req := &nodev1.UpdateAgentRequest{Update: update, Chunk: chunk}
		update = nil
		return stream.Send(req)
	})
	_, err = stream.CloseAndRecv()
	// An updater from a release before UpdateAgent doesn't serve it.
	if status.Code(err) == codes.Unimplemented {
		return fmt.Errorf("the node's updater is from a release that can't update the agent: %w", err)
	}
	return err
}

// UpdateUpdater has the agent replace the updater with binary, the
// updater at version, which must become healthy within healthTimeout.
func (n *Node) UpdateUpdater(ctx context.Context, version string, binary []byte, healthTimeout time.Duration) error {
	return UpdateUpdater(ctx, n.Agent, version, binary, healthTimeout)
}

// UpdateUpdater has agent replace the updater with binary, the updater
// at version, which must become healthy within healthTimeout.
func UpdateUpdater(ctx context.Context, agent nodev1.AgentClient, version string, binary []byte, healthTimeout time.Duration) error {
	stream, err := agent.UpdateUpdater(ctx)
	if err != nil {
		return err
	}
	update := &nodev1.UpdaterUpdate{Version: version, Sha256: sha256Hex(binary), HealthTimeout: durationpb.New(healthTimeout)}
	sendChunks(binary, func(chunk []byte) error {
		req := &nodev1.UpdateUpdaterRequest{Update: update, Chunk: chunk}
		update = nil
		return stream.Send(req)
	})
	_, err = stream.CloseAndRecv()
	return err
}

// UpdateContainerd has the agent replace containerd's binaries with
// files, by name, containerd at version, which must answer within
// healthTimeout.
func (n *Node) UpdateContainerd(ctx context.Context, version string, files map[string][]byte, healthTimeout time.Duration) error {
	return UpdateContainerd(ctx, n.Agent, version, files, healthTimeout)
}

// UpdateContainerd has agent replace containerd's binaries with files,
// by name, containerd at version, which must answer within
// healthTimeout.
func UpdateContainerd(ctx context.Context, agent nodev1.AgentClient, version string, files map[string][]byte, healthTimeout time.Duration) error {
	stream, err := agent.UpdateContainerd(ctx)
	if err != nil {
		return err
	}
	update := &nodev1.ContainerdUpdate{Version: version, HealthTimeout: durationpb.New(healthTimeout)}
	names := slices.Sorted(maps.Keys(files))
	for _, name := range names {
		update.Files = append(update.Files, &nodev1.ContainerdFile{Name: name, Sha256: sha256Hex(files[name])})
	}
	sent := true
	for _, name := range names {
		sendChunks(files[name], func(chunk []byte) error {
			req := &nodev1.UpdateContainerdRequest{Update: update, File: name, Chunk: chunk}
			update = nil
			err := stream.Send(req)
			sent = err == nil
			return err
		})
		if !sent {
			break
		}
	}
	_, err = stream.CloseAndRecv()
	return err
}

// sha256Hex is data's SHA-256, hex.
func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
