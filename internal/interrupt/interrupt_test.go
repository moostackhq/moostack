//go:build unix

package interrupt_test

import (
	"context"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/moostackhq/moostack/internal/interrupt"
)

// exits records the exit codes a Guard asks for.
type exits chan int

// exit records code.
func (e exits) exit(code int) { e <- code }

// wantExit fails unless the guard exits with code.
func (e exits) wantExit(t *testing.T, code int) {
	t.Helper()
	select {
	case got := <-e:
		if got != code {
			t.Errorf("exited with %d, want %d", got, code)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("didn't exit, want %d", code)
	}
}

// wantNoExit fails if the guard exits.
func (e exits) wantNoExit(t *testing.T) {
	t.Helper()
	select {
	case got := <-e:
		t.Errorf("exited with %d", got)
	case <-time.After(200 * time.Millisecond):
	}
}

// signal sends sig to the test process.
func signal(t *testing.T, sig syscall.Signal) {
	t.Helper()
	if err := syscall.Kill(os.Getpid(), sig); err != nil {
		t.Fatal(err)
	}
}

func TestASignalCleansUpAndExits(t *testing.T) {
	for _, sig := range []syscall.Signal{syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP} {
		t.Run(sig.String(), func(t *testing.T) {
			e := make(exits, 1)
			cleaned := make(chan bool, 1)
			g := interrupt.Catch(func() { cleaned <- true }, e.exit)
			defer g.Stop()

			signal(t, sig)

			e.wantExit(t, 128+int(sig))
			if len(cleaned) != 1 {
				t.Error("the cleanup didn't run before the exit")
			}
		})
	}
}

func TestPassLeavesSIGINTToTheProgram(t *testing.T) {
	e := make(exits, 1)
	g := interrupt.Catch(func() {}, e.exit)
	defer g.Stop()

	g.Pass(func() error {
		signal(t, syscall.SIGINT)
		time.Sleep(100 * time.Millisecond)
		return nil
	})

	e.wantNoExit(t)
}

func TestPassStillEndsOnSIGTERM(t *testing.T) {
	e := make(exits, 1)
	g := interrupt.Catch(func() {}, e.exit)
	defer g.Stop()

	g.Pass(func() error {
		signal(t, syscall.SIGTERM)
		time.Sleep(100 * time.Millisecond)
		return nil
	})

	e.wantExit(t, 128+int(syscall.SIGTERM))
}

func TestASecondSignalExitsWhileTheCleanupHangs(t *testing.T) {
	e := make(exits, 1)
	hang := make(chan struct{})
	defer close(hang)
	started := make(chan struct{})
	g := interrupt.Catch(func() {
		close(started)
		<-hang
	}, e.exit)
	defer g.Stop()

	signal(t, syscall.SIGTERM)
	<-started
	signal(t, syscall.SIGTERM)

	e.wantExit(t, 128+int(syscall.SIGTERM))
}

func TestContextEndsOnASignalWithoutExiting(t *testing.T) {
	for _, sig := range []syscall.Signal{syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP} {
		e := make(exits, 1)
		ctx, stop := interrupt.Context(context.Background(), e.exit)
		signal(t, sig)
		select {
		case <-ctx.Done():
		case <-time.After(5 * time.Second):
			t.Errorf("%v didn't end the context", sig)
		}
		e.wantNoExit(t)
		stop()
	}
}

func TestContextExitsOnASecondSignal(t *testing.T) {
	e := make(exits, 1)
	ctx, stop := interrupt.Context(context.Background(), e.exit)
	defer stop()
	signal(t, syscall.SIGINT)
	<-ctx.Done()
	signal(t, syscall.SIGTERM)
	e.wantExit(t, 128+int(syscall.SIGTERM))
}

func TestContextAfterStopLeavesSignalsAlone(t *testing.T) {
	e := make(exits, 1)
	ctx, stop := interrupt.Context(context.Background(), e.exit)
	stop()
	if ctx.Err() == nil {
		t.Error("stop didn't end the context")
	}
	// A later Context sees the signal, not the stopped one.
	later := make(exits, 1)
	ctx2, stop2 := interrupt.Context(context.Background(), later.exit)
	defer stop2()
	signal(t, syscall.SIGINT)
	<-ctx2.Done()
	e.wantNoExit(t)
}
