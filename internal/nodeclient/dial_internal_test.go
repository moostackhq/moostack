package nodeclient

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
)

// renewingAgent grants every renewal.
type renewingAgent struct {
	nodev1.UnimplementedAgentServer
}

func (renewingAgent) RenewLock(context.Context, *nodev1.RenewLockRequest) (*nodev1.RenewLockResponse, error) {
	return &nodev1.RenewLockResponse{Lock: &nodev1.Lock{Lease: "ours"}}, nil
}

// mooctl renews the lease on an agent that an update restarted until it
// answers, for a third of a lease time: its connection to the agent
// reaches the agent within about a second of it serving, however long
// it was down, rather than once the connection's backoff ends.
func TestTheAgentIsReachedSoonAfterALongOutage(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "agent.sock")
	conn, err := dial(socket, func(ctx context.Context) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	agent := nodev1.NewAgentClient(conn)
	renew := func() error {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()
		_, err := agent.RenewLock(ctx, &nodev1.RenewLockRequest{Lease: "ours"})
		return err
	}
	for end := time.Now().Add(20 * time.Second); time.Now().Before(end); time.Sleep(250 * time.Millisecond) {
		renew()
	}
	l, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer()
	nodev1.RegisterAgentServer(s, renewingAgent{})
	go s.Serve(l)
	defer s.Stop()
	start := time.Now()
	for err := renew(); err != nil; err = renew() {
		if time.Since(start) > 1500*time.Millisecond {
			t.Fatalf("the agent, serving since %s ago, isn't reached: %v", time.Since(start).Round(time.Millisecond), err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}
