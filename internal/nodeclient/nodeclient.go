// Package nodeclient reaches a node's node API over SSH.
package nodeclient

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

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
	// Mode is unspecified if the agent didn't answer.
	Mode nodev1.Mode
	// Components are the agent's components and the updater. The agent
	// or the updater that didn't answer is unhealthy, with why as its
	// problem, and the components only the agent reports are missing.
	Components []*nodev1.Component
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
		s.Components = append(s.Components, notAnswering("agent", agentErr))
	} else {
		s.Mode = agent.Mode
		s.Components = append(s.Components, agent.Components...)
	}
	if updaterErr != nil {
		s.Components = append(s.Components, notAnswering("updater", updaterErr))
	} else {
		s.Components = append(s.Components, updater.Updater)
	}
	return s
}

// notAnswering is the program that didn't answer with err.
func notAnswering(name string, err error) *nodev1.Component {
	return &nodev1.Component{Name: name, Health: nodev1.Health_HEALTH_UNHEALTHY, Problem: fmt.Sprintf("the %s doesn't answer: %v", name, err)}
}

// ModeName is mode as mooctl shows it: "-" when the mode isn't known.
func ModeName(mode nodev1.Mode) string {
	if mode == nodev1.Mode_MODE_UNSPECIFIED {
		return "-"
	}
	return strings.ToLower(strings.TrimPrefix(mode.String(), "MODE_"))
}

// HealthName is health as mooctl shows it: "unknown" when the health
// isn't known.
func HealthName(h nodev1.Health) string {
	if h == nodev1.Health_HEALTH_UNSPECIFIED {
		return "unknown"
	}
	return strings.ToLower(strings.TrimPrefix(h.String(), "HEALTH_"))
}
