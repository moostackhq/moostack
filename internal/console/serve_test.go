package console_test

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/moostackhq/moostack/internal/console"
	"github.com/moostackhq/moostack/internal/orchestrator"
)

// memListener is a listener whose connections are made in memory. The
// accepts whose number is in fail fail, and it records when each accept
// was called.
type memListener struct {
	conns  chan net.Conn
	closed chan struct{}
	once   sync.Once

	mu      sync.Mutex
	fail    map[int]bool
	accepts []time.Time
}

func newMemListener() *memListener {
	return &memListener{conns: make(chan net.Conn), closed: make(chan struct{})}
}

func (l *memListener) Accept() (net.Conn, error) {
	l.mu.Lock()
	n := len(l.accepts)
	l.accepts = append(l.accepts, time.Now())
	fail := l.fail[n]
	l.mu.Unlock()
	if fail {
		return nil, errors.New("accept: too many open files")
	}
	select {
	case conn := <-l.conns:
		return conn, nil
	case <-l.closed:
		return nil, &net.OpError{Op: "accept", Net: "unix", Addr: l.Addr(), Err: net.ErrClosed}
	}
}

func (l *memListener) Close() error {
	l.once.Do(func() { close(l.closed) })
	return nil
}

func (l *memListener) Addr() net.Addr { return &net.UnixAddr{Name: "memory", Net: "unix"} }

func (l *memListener) dial(t *testing.T) net.Conn {
	t.Helper()
	client, server := net.Pipe()
	select {
	case l.conns <- server:
	case <-l.closed:
		t.Fatal("the listener is closed")
	}
	return client
}

func (l *memListener) isClosed() bool {
	select {
	case <-l.closed:
		return true
	default:
		return false
	}
}

// serveInMemory serves c on listeners in memory and returns them, with
// a function that stops c without waiting and the channel that gets
// what Serve returns.
func serveInMemory(c *console.Console) (web, tokenSocket *memListener, stop func(), done chan error) {
	web, tokenSocket = newMemListener(), newMemListener()
	ctx, cancel := context.WithCancel(context.Background())
	done = make(chan error, 1)
	go func() { done <- c.Serve(ctx, web, tokenSocket) }()
	return web, tokenSocket, cancel, done
}

// untilClosed returns how long from now the other end takes to close
// conn.
func untilClosed(conn net.Conn) time.Duration {
	start := time.Now()
	io.Copy(io.Discard, conn)
	return time.Since(start)
}

// gated is a cluster that answers once release is closed.
type gated struct {
	orchestrator.Orchestrator
	release chan struct{}
}

func (g gated) Running(ctx context.Context) ([]orchestrator.Container, error) {
	select {
	case <-g.release:
		return g.Orchestrator.Running(ctx)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestAStopLetsARequestInFlightEnd(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		c := console.New(gated{cluster, release})
		session := newSession(t, c)
		web, _, stop, done := serveInMemory(c)

		conn := web.dial(t)
		defer conn.Close()
		req, err := http.NewRequest(http.MethodGet, "http://console.test/console/", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.AddCookie(session)
		req.Close = true
		if err := req.Write(conn); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()

		start := time.Now()
		stop()
		synctest.Wait()
		close(release)
		res, err := http.ReadResponse(bufio.NewReader(conn), req)
		if err != nil {
			t.Fatalf("the request in flight got no answer: %v", err)
		}
		body, _ := io.ReadAll(res.Body)
		if res.StatusCode != http.StatusOK || !strings.Contains(string(body), "coredns") {
			t.Errorf("the request in flight: status %d:\n%s", res.StatusCode, body)
		}
		if err := <-done; err != nil {
			t.Errorf("Serve returned %v", err)
		}
		if took := time.Since(start); took >= time.Second {
			t.Errorf("the stop took %v, want no wait once the request ended", took)
		}
		time.Sleep(time.Second)
	})
}

func TestAStopCutsARequestStillOpenAfter20SecondsAndIsNoError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		web, tokenSocket, stop, done := serveInMemory(console.New(cluster))
		conn := web.dial(t)
		defer conn.Close()
		io.WriteString(conn, "POST /console/login HTTP/1.1\r\nHost: console.test\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: 100\r\n\r\ntoken=")
		synctest.Wait()

		start := time.Now()
		stop()
		if err := <-done; err != nil {
			t.Errorf("Serve returned %v", err)
		}
		if took := time.Since(start); took < 20*time.Second || took >= 21*time.Second {
			t.Errorf("the stop took %v, want 20s", took)
		}
		if took := untilClosed(conn); took >= time.Second {
			t.Errorf("the request was still open %v after the stop", took)
		}
		if !tokenSocket.isClosed() {
			t.Error("the token socket is still open")
		}
		time.Sleep(time.Second)
	})
}

func TestASlowOrIdleClientIsCutOff(t *testing.T) {
	for what, c := range map[string]struct {
		sent string
		want time.Duration
	}{
		"a head that doesn't end": {"GET /console/login HTTP/1.1\r\nHost:", 10 * time.Second},
		"a body that doesn't end": {"POST /console/login HTTP/1.1\r\nHost: console.test\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: 100\r\n\r\ntoken=", 30 * time.Second},
	} {
		synctest.Test(t, func(t *testing.T) {
			web, _, stop, done := serveInMemory(console.New(cluster))
			conn := web.dial(t)
			defer conn.Close()
			io.WriteString(conn, c.sent)
			if took := untilClosed(conn); took < c.want || took >= c.want+time.Second {
				t.Errorf("%s was cut off after %v, want %v", what, took, c.want)
			}
			stop()
			<-done
			time.Sleep(time.Second)
		})
	}
	synctest.Test(t, func(t *testing.T) {
		web, _, stop, done := serveInMemory(console.New(cluster))
		conn := web.dial(t)
		defer conn.Close()
		io.WriteString(conn, "GET /console/login HTTP/1.1\r\nHost: console.test\r\n\r\n")
		reader := bufio.NewReader(conn)
		res, err := http.ReadResponse(reader, nil)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, res.Body)
		start := time.Now()
		io.Copy(io.Discard, reader)
		if took := time.Since(start); took < 2*time.Minute || took >= 2*time.Minute+time.Second {
			t.Errorf("an idle connection was closed after %v, want 2m", took)
		}
		stop()
		<-done
		time.Sleep(time.Second)
	})
}

func TestServeEndsWhenItsWebListenerFails(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		web, tokenSocket := newMemListener(), newMemListener()
		web.fail = map[int]bool{0: true}
		err := console.New(cluster).Serve(context.Background(), web, tokenSocket)
		if err == nil || !strings.Contains(err.Error(), "too many open files") {
			t.Errorf("Serve returned %v, want the web listener's error", err)
		}
		if !tokenSocket.isClosed() {
			t.Error("the token socket is still open")
		}
	})
}

func TestFailedAcceptsOnTheSocketWaitLongerEachTimeAndAreLogged(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		written := recordOutput(t)
		web, tokenSocket := newMemListener(), newMemListener()
		tokenSocket.fail = map[int]bool{}
		for i := range 11 {
			tokenSocket.fail[i] = true
		}
		tokenSocket.fail[12] = true
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- console.New(cluster).Serve(ctx, web, tokenSocket) }()

		conn := tokenSocket.dial(t)
		token, _ := io.ReadAll(conn)
		if len(token) != 27 {
			t.Errorf("the socket answered %q after its failed accepts", token)
		}
		time.Sleep(time.Second)
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Serve returned %v", err)
		}

		tokenSocket.mu.Lock()
		defer tokenSocket.mu.Unlock()
		var waits []time.Duration
		for i := 1; i < len(tokenSocket.accepts); i++ {
			waits = append(waits, tokenSocket.accepts[i].Sub(tokenSocket.accepts[i-1]))
		}
		ms := time.Millisecond
		want := []time.Duration{5 * ms, 10 * ms, 20 * ms, 40 * ms, 80 * ms, 160 * ms, 320 * ms, 640 * ms, time.Second, time.Second, time.Second, 0, 5 * ms}
		if len(waits) < len(want) {
			t.Fatalf("the waits between accepts were %v, want %v", waits, want)
		}
		for i, w := range want {
			if waits[i] != w {
				t.Errorf("the wait after accept %d was %v, want %v (all: %v)", i, waits[i], w, waits[:len(want)])
			}
		}
		if got := strings.Count(written(), "accepting on the token socket"); got != 12 {
			t.Errorf("%d failed accepts were logged, want 12:\n%s", got, written())
		}
	})
}
