package sshclient_test

import (
	"context"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/moostackhq/moostack/internal/sshclient"
)

// startGRPCServer runs testdata/grpcserver in d as root, serving health
// checks on a root-only socket, and returns the socket.
func startGRPCServer(t *testing.T, d *sshd) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "grpcserver")
	build := exec.Command("go", "build", "-o", bin, "./testdata/grpcserver")
	build.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS=linux", "GOARCH="+runtime.GOARCH)
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building grpcserver: %v\n%s", err, out)
	}
	if out, err := exec.Command(d.runtime, "cp", bin, d.name+":/usr/local/bin/grpcserver").CombinedOutput(); err != nil {
		t.Fatalf("copying grpcserver: %v\n%s", err, out)
	}
	sock := "/run/node.sock"
	if out, err := exec.Command(d.runtime, "exec", "-d", d.name, "grpcserver", sock).CombinedOutput(); err != nil {
		t.Fatalf("starting grpcserver: %v\n%s", err, out)
	}
	eventually(t, 10*time.Second, func() error {
		if exec.Command(d.runtime, "exec", d.name, "test", "-S", sock).Run() != nil {
			return errors.New("grpcserver didn't create its socket")
		}
		return nil
	})
	return sock
}

func TestGRPCThroughAForwardedSocket(t *testing.T) {
	signer, file, _ := newKey(t)
	d := startSSHD(t, signer.PublicKey())
	sock := startGRPCServer(t, d)

	for _, user := range []string{"root", "op"} {
		t.Run("as "+user, func(t *testing.T) {
			h := newHome(t, nodeConfig(d, user))
			h.write(t, ".ssh/key", file)
			opts := h.options()
			opts.Relay = func(socket string) string { return "socat STDIO UNIX-CONNECT:" + socket }
			ctx := testContext(t)
			c, err := sshclient.Dial(ctx, d.node(), opts)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()

			conn, err := grpc.NewClient("passthrough:///node",
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
					return c.DialUnix(ctx, sock)
				}))
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			for range 2 {
				res, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
				if err != nil {
					t.Fatal(err)
				}
				if res.Status != healthpb.HealthCheckResponse_SERVING {
					t.Errorf("status = %v, want SERVING", res.Status)
				}
			}
		})
	}
}

func TestRelayedDialUnixGivesUpWhenTheRelayDoesntStart(t *testing.T) {
	signer, file, _ := newKey(t)
	s := startServer(t, signer.PublicKey())
	s.pretendNotRoot(t)
	h := newHome(t, strings.Replace(s.config("  IdentityFile {home}/.ssh/key\n"), "User root", "User op", 1))
	h.write(t, ".ssh/key", file)
	opts := h.options()
	opts.Relay = func(string) string { return "cat" }
	c, err := sshclient.Dial(testContext(t), s.node(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	// The session opens, but the request to run the relay gets no answer.
	s.stallExec.Store(true)
	ctx, cancel := context.WithTimeout(testContext(t), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		conn, err := c.DialUnix(ctx, "/run/node.sock")
		if conn != nil {
			conn.Close()
		}
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("err = %v, want the context's deadline", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("DialUnix didn't give up when its context ended")
	}
}
