package sshclient

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/crypto/ssh"
)

// connection is one connection to a node.
type connection struct {
	*ssh.Client
	route *route
	// netConn is the network connection under the SSH connection, which
	// knows when it last received anything.
	netConn *watchedConn
	// dropped is closed when the connection ends.
	dropped chan struct{}
	// probing says a keepalive is checking whether the connection has
	// gone silent.
	probing   atomic.Bool
	closeOnce sync.Once
	closeErr  error
}

// close closes the connection and its jump hosts' connections, once, as a
// probe, a reconnect and Close may each close it.
func (c *connection) close() error {
	c.closeOnce.Do(func() {
		c.closeErr = c.Client.Close()
		c.route.close()
	})
	return c.closeErr
}

// watchedConn is a network connection that records when it last received
// anything, so that a probe can tell a busy connection from a silent one.
type watchedConn struct {
	net.Conn
	// at is when it last received anything, in Unix nanoseconds.
	at atomic.Int64
	// heard, if set, is called once, when it first receives anything.
	heard     func()
	heardOnce sync.Once
}

func (r *watchedConn) Read(p []byte) (int, error) {
	n, err := r.Conn.Read(p)
	if n > 0 {
		r.at.Store(time.Now().UnixNano())
		if r.heard != nil {
			r.heardOnce.Do(r.heard)
		}
	}
	return n, err
}

// since reports whether the connection received anything after t.
func (r *watchedConn) since(t time.Time) bool { return r.at.Load() > t.UnixNano() }

// errClosed is the error from a Client that has been closed.
var errClosed = errors.New("the SSH connection is closed")

// newSession opens a session on the node, and returns the connection it
// is on.
func (c *Client) newSession(ctx context.Context) (*ssh.Session, *connection, error) {
	return open(ctx, c, func(conn *ssh.Client) (*ssh.Session, error) { return conn.NewSession() })
}

// open opens a session or channel with f, giving up when ctx ends, and
// returns the connection it is on. If the connection has dropped, it
// reconnects once and calls f again. Nothing has run on the node when it
// fails.
func open[T io.Closer](ctx context.Context, c *Client, f func(*ssh.Client) (T, error)) (T, *connection, error) {
	var zero T
	if err := ctx.Err(); err != nil {
		return zero, nil, err
	}
	conn, err := c.current()
	if err != nil {
		return zero, nil, err
	}
	v, err := openOn(ctx, conn, f)
	if err == nil || ctx.Err() != nil || conn.answers(ctx) {
		return v, conn, err
	}
	conn, err = c.reconnect(ctx, conn)
	if err != nil {
		return zero, nil, err
	}
	v, err = openOn(ctx, conn, f)
	return v, conn, err
}

// openOn calls f on conn within ctx, and probes conn if ctx ends first.
func openOn[T io.Closer](ctx context.Context, conn *connection, f func(*ssh.Client) (T, error)) (T, error) {
	v, err := withContext(ctx, func() (T, error) { return f(conn.Client) })
	if err != nil && ctx.Err() != nil {
		conn.probe()
	}
	return v, err
}

// withContext calls f, giving up when ctx ends, so that a connection that
// has gone silent can't block. What f returns after that is closed.
func withContext[T io.Closer](ctx context.Context, f func() (T, error)) (T, error) {
	type result struct {
		v   T
		err error
	}
	done := make(chan result, 1)
	go func() {
		v, err := f()
		done <- result{v, err}
	}()
	select {
	case r := <-done:
		return r.v, r.err
	case <-ctx.Done():
		go func() {
			if r := <-done; r.err == nil {
				r.v.Close()
			}
		}()
		var zero T
		return zero, ctx.Err()
	}
}

// probeTimeout is how long a connection may go without receiving
// anything, while a keepalive is waiting for an answer, before it counts
// as gone silent. During an upload, the node's only traffic is a window
// adjustment for about every 96KB it takes in, so this leaves room for
// uploads down to about 6KB/s.
const probeTimeout = 15 * time.Second

// answers reports whether the connection answers a keepalive. A dropped
// connection doesn't, nor does one that receives nothing at all for
// probeTimeout while the keepalive waits; a busy one, whose answer is
// queued behind other data, does. If ctx ends first, it counts as
// answering, since then there is no time to reconnect anyway, and a probe
// carries on in the background.
func (c *connection) answers(ctx context.Context) bool {
	select {
	case <-c.dropped:
		return false
	default:
	}
	done := make(chan error, 1)
	go func() {
		_, _, err := c.SendRequest("keepalive@openssh.com", true, nil)
		done <- err
	}()
	ticker := time.NewTicker(probeTimeout)
	defer ticker.Stop()
	for windowStart := time.Now(); ; {
		select {
		case err := <-done:
			return err == nil
		case <-ticker.C:
			if !c.netConn.since(windowStart) {
				return false
			}
			windowStart = time.Now()
		case <-ctx.Done():
			c.probe()
			return true
		}
	}
}

// probe checks in the background whether the connection has gone silent,
// after a call gave up waiting on it, and closes it if so. The next call
// then finds it dropped and reconnects, without first waiting out the
// probe itself.
func (c *connection) probe() {
	if !c.probing.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer c.probing.Store(false)
		if !c.answers(context.Background()) {
			c.close()
		}
	}()
}

// current is the client's connection now, or errClosed once the client
// has been closed.
func (c *Client) current() (*connection, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, errClosed
	}
	return c.conn, nil
}

// reconnect replaces old, which has dropped or gone silent, unless
// another call has already replaced it.
func (c *Client) reconnect(ctx context.Context, old *connection) (*connection, error) {
	current, err := c.current()
	if err != nil {
		return nil, err
	}
	if current != old {
		return current, nil
	}
	conn, err := c.dial(ctx)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	switch {
	case c.closed:
		conn.close()
		return nil, errClosed
	case c.conn != old:
		// Another call reconnected first.
		conn.close()
		return c.conn, nil
	}
	old.close()
	c.conn = conn
	return conn, nil
}
