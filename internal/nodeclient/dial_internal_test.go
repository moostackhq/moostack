package nodeclient

import (
	"context"
	"net"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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

// forgingAgent answers with errors and image names that would add a line
// of their own to what mooctl prints.
type forgingAgent struct {
	nodev1.UnimplementedAgentServer
	images []string
}

const forged = "refused\nnode9: connecting to root@node9.invalid: connection refused\x1b[2A"

func (forgingAgent) RecordGeneration(context.Context, *nodev1.RecordGenerationRequest) (*nodev1.RecordGenerationResponse, error) {
	return nil, status.Error(codes.FailedPrecondition, forged)
}

func (a forgingAgent) ImportImage(stream grpc.ClientStreamingServer[nodev1.ImportImageRequest, nodev1.ImportImageResponse]) error {
	for {
		if _, err := stream.Recv(); err != nil {
			break
		}
	}
	if a.images == nil {
		return status.Error(codes.InvalidArgument, forged)
	}
	return stream.SendAndClose(&nodev1.ImportImageResponse{Images: a.images})
}

func TestTheNodeAPIsErrorsAndImageNamesComeQuoted(t *testing.T) {
	for _, images := range [][]string{nil, {"docker.io/library/ok:1", forged}} {
		socket := filepath.Join(t.TempDir(), "agent.sock")
		l, err := net.Listen("unix", socket)
		if err != nil {
			t.Fatal(err)
		}
		s := grpc.NewServer()
		nodev1.RegisterAgentServer(s, forgingAgent{images: images})
		go s.Serve(l)
		conn, err := dial(socket, func(ctx context.Context) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socket)
		})
		if err != nil {
			t.Fatal(err)
		}
		n := &Node{Agent: nodev1.NewAgentClient(conn)}
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		err = n.RecordGeneration(ctx, 1, nil, "lease")
		if status.Code(err) != codes.FailedPrecondition || status.Convert(err).Message() != strconv.Quote(forged) {
			t.Errorf("RecordGeneration's error is %v, want its message quoted, with its code", err)
		}
		got, err := n.ImportImage(ctx, strings.NewReader("archive"), "lease")
		if images == nil {
			if status.Code(err) != codes.InvalidArgument || status.Convert(err).Message() != strconv.Quote(forged) {
				t.Errorf("ImportImage's error is %v, want its message quoted, with its code", err)
			}
		} else if err != nil || !slices.Equal(got, []string{"docker.io/library/ok:1", strconv.Quote(forged)}) {
			t.Errorf("ImportImage says %q, %v, want the second name quoted", got, err)
		}
		cancel()
		conn.Close()
		s.Stop()
	}
}
