package rootsocket_test

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/rootsocket"
)

// reportingAgent answers status with a membership of members, each of
// the longest name, and an address of address bytes.
type reportingAgent struct {
	nodev1.UnimplementedAgentServer
	members, address int
}

func (a reportingAgent) Status(context.Context, *nodev1.StatusRequest) (*nodev1.StatusResponse, error) {
	m := &nodev1.Membership{}
	for i := range a.members {
		m.Members = append(m.Members, &nodev1.Member{Node: fmt.Sprintf("%0255d", i), Address: strings.Repeat("a", a.address)})
	}
	return &nodev1.StatusResponse{Membership: m}, nil
}

// askStatus asks an agent answering as a does, over Dial.
func askStatus(t *testing.T, a reportingAgent) (*nodev1.StatusResponse, error) {
	t.Helper()
	socket := filepath.Join(t.TempDir(), "agent.sock")
	l, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer()
	t.Cleanup(s.Stop)
	nodev1.RegisterAgentServer(s, a)
	go s.Serve(l)
	conn, err := rootsocket.Dial(socket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	return nodev1.NewAgentClient(conn).Status(ctx, &nodev1.StatusRequest{})
}

// The agent and the updater read a status with a full membership view,
// the most members with the longest names and addresses.
func TestAStatusWithAFullMembershipViewIsRead(t *testing.T) {
	res, err := askStatus(t, reportingAgent{members: 10000, address: 255})
	if err != nil {
		t.Fatal(err)
	}
	if got := len(res.Membership.Members); got != 10000 {
		t.Errorf("the status has %d members", got)
	}
}

func TestAnAnswerOverTheMostIsRefused(t *testing.T) {
	_, err := askStatus(t, reportingAgent{members: 1, address: 33 << 20})
	if status.Code(err) != codes.ResourceExhausted {
		t.Errorf("an answer over the most: %v, want ResourceExhausted", err)
	}
}
