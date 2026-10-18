// Package nodeclient reaches a node's node API over SSH.
package nodeclient

import (
	"context"
	"net"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/hostlayout"
	"github.com/moostackhq/moostack/internal/sshclient"
)

// Relay is the command that connects to a unix socket on a node through
// sudo, for sshclient.Options.Relay: the agent's relay.
func Relay(socket string) string { return hostlayout.Agent + " relay " + socket }

// Agent is a connection to a node's agent over c, which SSH
// authenticates. Close it when done.
type Agent struct {
	nodev1.AgentClient
	conn *grpc.ClientConn
}

// DialAgent connects to the agent's socket on the node over c. It
// connects on first use, and again after a dropped connection.
func DialAgent(c *sshclient.Client) (*Agent, error) {
	conn, err := grpc.NewClient("passthrough:///agent",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return c.DialUnix(ctx, hostlayout.AgentSocket)
		}))
	if err != nil {
		return nil, err
	}
	return &Agent{AgentClient: nodev1.NewAgentClient(conn), conn: conn}, nil
}

// Close closes the connection.
func (a *Agent) Close() error { return a.conn.Close() }

// ModeName is mode as mooctl shows it.
func ModeName(mode nodev1.Mode) string {
	return strings.ToLower(strings.TrimPrefix(mode.String(), "MODE_"))
}

// HealthName is health as mooctl shows it.
func HealthName(h nodev1.Health) string {
	return strings.ToLower(strings.TrimPrefix(h.String(), "HEALTH_"))
}
