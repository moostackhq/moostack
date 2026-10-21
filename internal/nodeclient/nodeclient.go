// Package nodeclient reaches a node's node API over SSH.
package nodeclient

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/hostlayout"
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
	return grpc.NewClient("passthrough:///"+socket,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return c.DialUnix(ctx, socket)
		}))
}

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
	// Services are the cluster services the node runs, none if the agent
	// didn't answer.
	Services []*nodev1.Component
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
		s.Services = agent.Services
	}
	if updaterErr != nil {
		s.UpdaterErr = updaterErr
		s.Components = append(s.Components, notAnswering("updater", updaterErr))
	} else {
		s.Components = append(s.Components, updater.Updater)
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
	// Since is when the node granted the lease, and Until when it runs
	// out on the node unless renewed.
	Since, Until time.Time
}

// lockOf is l, nil if l is.
func lockOf(l *nodev1.Lock) *Lock {
	if l == nil {
		return nil
	}
	return &Lock{Lease: l.Lease, Holder: l.Holder, Machine: l.Machine, Since: l.Since.AsTime(), Until: l.Until.AsTime()}
}

// AcquireLock asks the agent for the node's operation lock for lock's
// lease, or to renew it, for ttl. It returns the lock as it now is: held
// by lock's lease if granted, or by another.
func (n *Node) AcquireLock(ctx context.Context, lock Lock, ttl time.Duration) (Lock, error) {
	res, err := n.Agent.AcquireLock(ctx, &nodev1.AcquireLockRequest{
		Lock: &nodev1.Lock{Lease: lock.Lease, Holder: lock.Holder, Machine: lock.Machine},
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
// the node's containerd, and returns the names of the images imported.
func ImportImage(ctx context.Context, agent nodev1.AgentClient, data []byte) ([]string, error) {
	stream, err := agent.ImportImage(ctx)
	if err != nil {
		return nil, err
	}
	for len(data) > 0 {
		n := min(len(data), importChunk)
		if err := stream.Send(&nodev1.ImportImageRequest{Chunk: data[:n]}); err != nil {
			// The agent ended the call; CloseAndRecv says why.
			break
		}
		data = data[n:]
	}
	res, err := stream.CloseAndRecv()
	if err != nil {
		return nil, err
	}
	return res.Images, nil
}

// UpdateAgent has the updater replace the agent with binary, the agent at
// version, which must become healthy within healthTimeout.
func (n *Node) UpdateAgent(ctx context.Context, version string, binary []byte, healthTimeout time.Duration) error {
	return UpdateAgent(ctx, n.Updater, version, binary, healthTimeout)
}

// updateChunk is how much of an agent's binary each request carries.
const updateChunk = 1 << 20

// UpdateAgent has updater replace the agent with binary, the agent at
// version, which must become healthy within healthTimeout. It fails
// saying so if the updater is from a release that can't.
func UpdateAgent(ctx context.Context, updater nodev1.UpdaterClient, version string, binary []byte, healthTimeout time.Duration) error {
	stream, err := updater.UpdateAgent(ctx)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(binary)
	req := &nodev1.UpdateAgentRequest{Update: &nodev1.AgentUpdate{
		Version:       version,
		Sha256:        hex.EncodeToString(sum[:]),
		HealthTimeout: durationpb.New(healthTimeout),
	}}
	for {
		n := min(len(binary), updateChunk)
		req.Chunk = binary[:n]
		if err := stream.Send(req); err != nil {
			// The updater ended the call; CloseAndRecv says why.
			break
		}
		binary = binary[n:]
		if len(binary) == 0 {
			break
		}
		req = &nodev1.UpdateAgentRequest{}
	}
	_, err = stream.CloseAndRecv()
	// An updater from a release before UpdateAgent doesn't serve it.
	if status.Code(err) == codes.Unimplemented {
		return fmt.Errorf("the node's updater is from a release that can't update the agent: %w", err)
	}
	return err
}
