package sshclient_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/moostackhq/moostack/internal/sshclient"
)

// inProcessPool is a pool for one in-process server, which it reaches as
// root under the SSH config's "node".
func inProcessPool(t *testing.T) (*server, *sshclient.Pool, sshclient.Node) {
	t.Helper()
	signer, file, _ := newKey(t)
	s := startServer(t, signer.PublicKey())
	h := newHome(t, s.config("  IdentityFile {home}/.ssh/key\n"))
	h.write(t, ".ssh/key", file)
	p := sshclient.NewPool(h.options())
	t.Cleanup(func() { p.Close() })
	return s, p, s.node()
}

// run runs echo ok on n through p.
func run(t *testing.T, p *sshclient.Pool, n sshclient.Node) error {
	t.Helper()
	ctx := testContext(t)
	c, err := p.Client(ctx, n)
	if err != nil {
		return err
	}
	res, err := c.Run(ctx, "echo ok", nil)
	if err == nil && string(res.Stdout) != "ok\n" {
		t.Errorf("stdout = %q, want ok", res.Stdout)
	}
	return err
}

func TestPoolKeepsOneConnectionPerNode(t *testing.T) {
	s, p, n := inProcessPool(t)
	for range 3 {
		if err := run(t, p, n); err != nil {
			t.Fatal(err)
		}
	}
	if got := s.acceptedConns(); got != 1 {
		t.Errorf("the server accepted %d connections, want 1", got)
	}
}

func TestPoolReconnectsADroppedConnection(t *testing.T) {
	s, p, n := inProcessPool(t)
	if err := run(t, p, n); err != nil {
		t.Fatal(err)
	}
	s.drop(false)
	if err := run(t, p, n); err != nil {
		t.Fatalf("after a drop: %v", err)
	}
	if got := s.acceptedConns(); got != 2 {
		t.Errorf("the server accepted %d connections, want 2", got)
	}
}

func TestPoolReconnectsOnlyOnce(t *testing.T) {
	s, p, n := inProcessPool(t)
	if err := run(t, p, n); err != nil {
		t.Fatal(err)
	}
	s.drop(true)
	if err := run(t, p, n); err == nil {
		t.Fatal("Run succeeded with every connection dropped")
	}
	if got := s.acceptedConns(); got != 2 {
		t.Errorf("the server accepted %d connections, want 2", got)
	}
}

func TestPoolCloseClosesItsConnections(t *testing.T) {
	_, p, n := inProcessPool(t)
	ctx := testContext(t)
	c, err := p.Client(ctx, n)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Run(ctx, "true", nil); err == nil {
		t.Error("Run succeeded after the pool closed")
	}
	if _, err := p.Client(ctx, n); err == nil {
		t.Error("Client succeeded after the pool closed")
	}
}

func TestPoolReconnectsWhenSSHDDropsTheConnection(t *testing.T) {
	d, h := startNode(t, "op")
	p := sshclient.NewPool(h.options())
	defer p.Close()
	n := d.node()
	if err := run(t, p, n); err != nil {
		t.Fatal(err)
	}
	// Each connection has its own sshd-session process.
	d.exec(t, "pkill", "sshd-session")
	if err := run(t, p, n); err != nil {
		t.Fatalf("after a drop: %v", err)
	}
}

func TestPoolConnectsAgainWhenTheCallerItWaitedForGivesUp(t *testing.T) {
	s, p, n := inProcessPool(t)
	s.frozen.Store(true)
	ctx, cancel := context.WithCancel(testContext(t))
	first := make(chan error)
	go func() {
		_, err := p.Client(ctx, n)
		first <- err
	}()
	// Wait until the first caller is connecting, so the second one waits
	// for it.
	for s.arrived.Load() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	second := make(chan error)
	go func() {
		_, err := p.Client(testContext(t), n)
		second <- err
	}()
	time.Sleep(100 * time.Millisecond)
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Errorf("the first caller got %v, want context.Canceled", err)
	}
	s.frozen.Store(false)
	if err := <-second; err != nil {
		t.Errorf("the second caller got %v, want a connection", err)
	}
}

func TestPoolReconnectsAfterAConnectionGoesSilent(t *testing.T) {
	s, p, n := inProcessPool(t)
	if err := run(t, p, n); err != nil {
		t.Fatal(err)
	}
	s.freezeOpen()
	ctx, cancel := context.WithTimeout(testContext(t), time.Second)
	defer cancel()
	c, err := p.Client(ctx, n)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Run(ctx, "true", nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want the context's deadline", err)
	}
	if err := run(t, p, n); err != nil {
		t.Fatalf("after the silent connection: %v", err)
	}
	if got := s.acceptedConns(); got != 2 {
		t.Errorf("the server accepted %d connections, want 2", got)
	}
}

func TestPoolCloseDoesntWaitForAConnectionBeingMade(t *testing.T) {
	s, p, n := inProcessPool(t)
	s.frozen.Store(true)
	got := make(chan error)
	go func() {
		_, err := p.Client(testContext(t), n)
		got <- err
	}()
	for s.arrived.Load() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	closed := make(chan error)
	go func() { closed <- p.Close() }()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close waited for the connection being made")
	}
	s.frozen.Store(false)
	if err := <-got; err == nil {
		t.Error("the connection being made was handed out after Close")
	}
}

func TestCallersWithShortDeadlinesGetPastASilentConnection(t *testing.T) {
	s, p, n := inProcessPool(t)
	if err := run(t, p, n); err != nil {
		t.Fatal(err)
	}
	s.freezeOpen()
	c, err := p.Client(testContext(t), n)
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, 30*time.Second, func() error {
		ctx, cancel := context.WithTimeout(testContext(t), 500*time.Millisecond)
		defer cancel()
		_, err := c.Run(ctx, "true", nil)
		return err
	})
	if got := s.acceptedConns(); got != 2 {
		t.Errorf("the server accepted %d connections, want 2", got)
	}
}

func TestABusyConnectionIsntTakenForASilentOne(t *testing.T) {
	s, p, n := inProcessPool(t)
	ctx := testContext(t)
	c, err := p.Client(ctx, n)
	if err != nil {
		t.Fatal(err)
	}
	// The connection is busy carrying output, and its keepalives are
	// answered late, behind that output.
	busy := make(chan error, 1)
	go func() {
		_, err := c.Run(ctx, "i=0; while [ $i -lt 250 ]; do echo busy; sleep 0.1; i=$((i+1)); done", nil)
		busy <- err
	}()
	time.Sleep(200 * time.Millisecond)
	// Longer than a probe waits without receiving anything.
	s.keepaliveDelay.Store(int64(20 * time.Second))
	// A call that gives up starts a probe.
	short, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	c.Run(short, "sleep 1", nil)
	if err := <-busy; err != nil {
		t.Errorf("the busy command failed: %v", err)
	}
	if got := s.acceptedConns(); got != 1 {
		t.Errorf("the server accepted %d connections, want 1", got)
	}
}
