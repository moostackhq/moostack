package sshclient_test

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunReturnsWhenTheCommandExitsWithoutReadingABlockedStdin(t *testing.T) {
	_, p, n := inProcessPool(t)
	ctx := testContext(t)
	c, err := p.Client(ctx, n)
	if err != nil {
		t.Fatal(err)
	}
	// Nothing is ever written, so every read blocks.
	stdin, w := io.Pipe()
	defer w.Close()
	type result struct {
		code int
		err  error
	}
	done := make(chan result, 1)
	go func() {
		res, err := c.Run(context.Background(), "exit 1", stdin)
		done <- result{res.ExitCode, err}
	}()
	select {
	case r := <-done:
		if r.err != nil || r.code != 1 {
			t.Errorf("got exit code %d and %v, want 1 and no error", r.code, r.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run didn't return after the command exited")
	}
}

func TestNoReadOfStdinStartsAfterRunGivesUp(t *testing.T) {
	_, p, n := inProcessPool(t)
	c, err := p.Client(testContext(t), n)
	if err != nil {
		t.Fatal(err)
	}
	stdin := &heldReader{release: make(chan struct{})}
	ctx, cancel := context.WithTimeout(testContext(t), 300*time.Millisecond)
	defer cancel()
	if _, err := c.Run(ctx, "sleep 5", stdin); err == nil {
		t.Fatal("Run outlived its context")
	}
	close(stdin.release)
	time.Sleep(300 * time.Millisecond)
	if got := stdin.reads.Load(); got != 1 {
		t.Errorf("%d reads of stdin started, want only the one under way when Run gave up", got)
	}
}

// heldReader holds its first read until release is closed, then gives
// data, and counts the reads that started.
type heldReader struct {
	release chan struct{}
	reads   atomic.Int64
}

func (r *heldReader) Read(p []byte) (int, error) {
	r.reads.Add(1)
	<-r.release
	clear(p)
	return len(p), nil
}

func TestRunGivesUpOnASilentConnection(t *testing.T) {
	s, p, n := inProcessPool(t)
	if err := run(t, p, n); err != nil {
		t.Fatal(err)
	}
	s.frozen.Store(true)
	defer s.frozen.Store(false)
	ctx, cancel := context.WithTimeout(testContext(t), time.Second)
	defer cancel()
	c, err := p.Client(ctx, n)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if _, err := c.Run(ctx, "true", nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want the context's deadline", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("Run took %v to give up", elapsed)
	}
}
