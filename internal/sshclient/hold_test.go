package sshclient_test

import (
	"context"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/moostackhq/moostack/internal/k3s"
	"github.com/moostackhq/moostack/internal/sshclient"
)

func TestTheOperationLockIsHeldUntilClosedThroughSudo(t *testing.T) {
	d, h := startNode(t, "op")
	ctx := testContext(t)
	dial := func() *sshclient.Client {
		c, err := sshclient.Dial(ctx, d.node(), h.options())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		return c
	}
	first, second := dial(), dial()

	held, err := first.Hold(ctx, k3s.LockScript, k3s.LockReady)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.Hold(ctx, k3s.LockScript, k3s.LockReady); err == nil || !strings.Contains(err.Error(), "another mooctl operation") {
		t.Fatalf("a second Hold = %v, want it refused", err)
	}
	if err := held.Close(); err != nil {
		t.Errorf("Close = %v", err)
	}
	again, err := second.Hold(ctx, k3s.LockScript, k3s.LockReady)
	if err != nil {
		t.Fatalf("Hold after Close = %v", err)
	}

	// A dropped connection releases it.
	second.Close()
	select {
	case <-again.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("Done isn't closed once the connection is")
	}
	third := dial()
	deadline := time.Now().Add(10 * time.Second)
	for {
		l, err := third.Hold(ctx, k3s.LockScript, k3s.LockReady)
		if err == nil {
			l.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the lock outlived its connection: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func TestHoldFailsWithStderrWhenTheCommandExits(t *testing.T) {
	d, h := startNode(t, "root")
	ctx := testContext(t)
	c, err := sshclient.Dial(ctx, d.node(), h.options())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Hold(ctx, "echo nope >&2; exit 1", "ready"); err == nil || !strings.Contains(err.Error(), "nope") {
		t.Errorf("Hold = %v", err)
	}
}

func TestDialTCPReachesAPortAsTheNodeSeesIt(t *testing.T) {
	d, h := startNode(t, "op")
	ctx := testContext(t)
	c, err := sshclient.Dial(ctx, d.node(), h.options())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	conn, err := c.DialTCP(ctx, "127.0.0.1:22")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	banner := make([]byte, 4)
	if _, err := io.ReadFull(conn, banner); err != nil || string(banner) != "SSH-" {
		t.Errorf("read %q, %v; want sshd's banner", banner, err)
	}
}

func TestStreamWritesOutputAsItComesAndEndsWithItsContext(t *testing.T) {
	d, h := startNode(t, "op")
	c, err := sshclient.Dial(testContext(t), d.node(), h.options())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	var out, errOut strings.Builder
	code, err := c.Stream(testContext(t), "echo one; echo two >&2; exit 3", nil, &out, &errOut)
	if err != nil || code != 3 || out.String() != "one\n" || errOut.String() != "two\n" {
		t.Errorf("Stream = %d, %v; stdout %q, stderr %q", code, err, out.String(), errOut.String())
	}

	// A command that never ends, as logs --follow, ends with the context.
	r, w := io.Pipe()
	ctx, cancel := context.WithCancel(testContext(t))
	done := make(chan error, 1)
	go func() {
		_, err := c.Stream(ctx, "while true; do echo tick; sleep 0.2; done", nil, w, io.Discard)
		w.Close()
		done <- err
	}()
	line := make([]byte, 5)
	if _, err := io.ReadFull(r, line); err != nil || string(line) != "tick\n" {
		t.Fatalf("read %q, %v; want a line before the command ends", line, err)
	}
	cancel()
	go io.Copy(io.Discard, r)
	select {
	case err := <-done:
		if err == nil {
			t.Error("Stream returned no error once its context ended")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Stream outlived its context")
	}
	if out, err := c.Run(testContext(t), "pgrep -f '[e]cho tick' || true", nil); err != nil || strings.TrimSpace(string(out.Stdout)) != "" {
		t.Errorf("the command still runs: %q, %v", out.Stdout, err)
	}
}

// silentProxy relays TCP to target until silence is set, after which it
// swallows everything both ways without closing either side, as a path
// that went silent does.
type silentProxy struct {
	ln      net.Listener
	silence atomic.Bool
}

func newSilentProxy(t *testing.T, target string) *silentProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &silentProxy{ln: ln}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			s, err := net.Dial("tcp", target)
			if err != nil {
				c.Close()
				continue
			}
			pipe := func(dst, src net.Conn) {
				buf := make([]byte, 32*1024)
				for {
					n, err := src.Read(buf)
					if n > 0 && !p.silence.Load() {
						dst.Write(buf[:n])
					}
					if err != nil {
						return
					}
				}
			}
			go pipe(s, c)
			go pipe(c, s)
		}
	}()
	return p
}

func (p *silentProxy) port() string {
	return p.ln.Addr().(*net.TCPAddr).AddrPort().String()[len("127.0.0.1:"):]
}

func TestTheLockIsReleasedWhenItsConnectionGoesSilent(t *testing.T) {
	signer, file, _ := newKey(t)
	d := startSSHD(t, signer.PublicKey())
	proxy := newSilentProxy(t, "127.0.0.1:"+d.port)
	direct := newHome(t, nodeConfig(d, "root"))
	direct.write(t, ".ssh/key", file)
	viaProxy := newHome(t, strings.Replace(nodeConfig(d, "root"), "Port "+d.port, "Port "+proxy.port(), 1))
	viaProxy.write(t, ".ssh/key", file)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()

	a, err := sshclient.Dial(ctx, d.node(), viaProxy.options())
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	held, err := a.Hold(ctx, k3s.LockScript, k3s.LockReady)
	if err != nil {
		t.Fatal(err)
	}
	// A heartbeat keeps a quiet but healthy holder's lock.
	time.Sleep(50 * time.Second)
	b, err := sshclient.Dial(ctx, d.node(), direct.options())
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if _, err := b.Hold(ctx, k3s.LockScript, k3s.LockReady); err == nil {
		t.Fatal("a lock held for 50 s over a healthy connection was taken from it")
	}

	start := time.Now()
	proxy.silence.Store(true)
	select {
	case <-held.Done():
	case <-time.After(60 * time.Second):
		t.Fatal("Done isn't closed a minute after the connection went silent")
	}
	closed := make(chan error, 1)
	go func() { closed <- held.Close() }()
	select {
	case <-closed:
	case <-time.After(15 * time.Second):
		t.Error("Close didn't return on a silent connection")
	}
	for {
		l, err := b.Hold(ctx, k3s.LockScript, k3s.LockReady)
		if err == nil {
			l.Close()
			break
		}
		if time.Since(start) > 70*time.Second {
			t.Fatalf("the lock is still held %v after its connection went silent: %v", time.Since(start), err)
		}
		time.Sleep(time.Second)
	}
}

func TestRunStoppableEndsTheCommandWithItsContext(t *testing.T) {
	for _, user := range []string{"root", "op"} {
		t.Run(user, func(t *testing.T) {
			d, h := startNode(t, user)
			c, err := sshclient.Dial(testContext(t), d.node(), h.options())
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			res, err := c.RunStoppable(testContext(t), "echo out; echo err >&2; exit 4")
			if err != nil || res.ExitCode != 4 || string(res.Stdout) != "out\n" || string(res.Stderr) != "err\n" {
				t.Errorf("RunStoppable = %+v, %v", res, err)
			}
			ctx, cancel := context.WithTimeout(testContext(t), time.Second)
			defer cancel()
			if _, err := c.RunStoppable(ctx, "sleep 3; touch /tmp/after-cancel"); err == nil {
				t.Fatal("RunStoppable outlived its context")
			}
			time.Sleep(4 * time.Second)
			if out := d.exec(t, "sh", "-c", "test -e /tmp/after-cancel && echo there || echo gone"); strings.TrimSpace(out) != "gone" {
				t.Error("the command went on after its context ended")
			}
		})
	}
}
