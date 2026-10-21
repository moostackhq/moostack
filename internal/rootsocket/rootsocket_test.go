//go:build linux

package rootsocket_test

import (
	"context"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/moostackhq/moostack/internal/rootsocket"
)

// slowListener answers each connection's handshake only after delay, as a
// program on a starved node does.
type slowListener struct {
	net.Listener
	delay time.Duration
}

func (l slowListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &slowConn{Conn: c, delay: l.delay}, nil
}

// slowConn delays its first write, the server's side of the handshake.
type slowConn struct {
	net.Conn
	once  sync.Once
	delay time.Duration
}

func (c *slowConn) Write(b []byte) (int, error) {
	c.once.Do(func() { time.Sleep(c.delay) })
	return c.Conn.Write(b)
}

// A program that serves, but answers the handshake slower than the
// connection tries again, is still reached.
func TestDialReachesAProgramThatAnswersItsHandshakeSlowly(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "program.sock")
	l, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer()
	healthpb.RegisterHealthServer(s, health.NewServer())
	go s.Serve(slowListener{l, 1500 * time.Millisecond})
	defer s.Stop()
	conn, err := rootsocket.Dial(socket)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := healthpb.NewHealthClient(conn)
	start := time.Now()
	for {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		_, err := client.Check(ctx, &healthpb.HealthCheckRequest{})
		cancel()
		if err == nil {
			return
		}
		if time.Since(start) > 10*time.Second {
			t.Fatalf("a program with a 1.5 s handshake isn't reached in 10 s: %v", err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}
