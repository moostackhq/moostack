package sshclient

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/crypto/ssh"
)

// HeartbeatInterval is how often a held command is sent a heartbeat: a
// line on its stdin, and a keepalive on its connection.
const HeartbeatInterval = 15 * time.Second

// releaseTimeout bounds how long Close waits for a held command to end
// once its stdin is closed, and again once its session is.
const releaseTimeout = 10 * time.Second

// readyTimeout bounds how long Hold waits for the command to be ready.
var readyTimeout = time.Minute

// Held is a command that runs on the node for as long as it is held.
type Held struct {
	session *ssh.Session
	conn    *connection
	stdin   io.WriteCloser
	done    chan struct{}
	err     error
	once    sync.Once
	stop    chan struct{}
}

// Hold runs cmd as root and waits until it writes ready as a line of its
// stdout, for readyTimeout at most. Until Close, it writes an empty line
// to the command's stdin every HeartbeatInterval, and closes the
// connection if the connection stops answering keepalives, which ends
// the command. A command that exits before writing ready fails Hold with
// its stderr.
func (c *Client) Hold(ctx context.Context, cmd, ready string) (_ *Held, err error) {
	readyCtx, cancel := context.WithTimeout(ctx, readyTimeout)
	defer cancel()
	defer func() {
		if err != nil && ctx.Err() == nil && readyCtx.Err() != nil {
			err = errors.New("the node didn't answer")
		}
	}()
	session, conn, err := c.newSession(readyCtx)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			session.Close()
		}
	}()
	stdin, err := session.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := session.StdoutPipe()
	if err != nil {
		return nil, err
	}
	var stderr bytes.Buffer
	session.Stderr = &stderr
	if _, err = withContext(readyCtx, func() (*ssh.Session, error) { return session, session.Start(c.asRoot(cmd)) }); err != nil {
		if readyCtx.Err() != nil {
			conn.probe()
		}
		return nil, err
	}
	isReady := make(chan bool, 1)
	go func() {
		s := bufio.NewScanner(stdout)
		for s.Scan() {
			if strings.TrimSpace(s.Text()) == ready {
				isReady <- true
				io.Copy(io.Discard, stdout)
				return
			}
		}
		isReady <- false
	}()
	select {
	case ok := <-isReady:
		if !ok {
			waitBounded(session, releaseTimeout)
			msg := strings.TrimSpace(stderr.String())
			if msg == "" {
				msg = "the command ended before it was ready"
			}
			return nil, errors.New(msg)
		}
	case <-readyCtx.Done():
		conn.close()
		return nil, readyCtx.Err()
	}
	h := &Held{session: session, conn: conn, stdin: stdin, done: make(chan struct{}), stop: make(chan struct{})}
	go func() {
		h.err = session.Wait()
		close(h.done)
	}()
	go h.heartbeat(conn)
	return h, nil
}

// heartbeat writes a line to the command's stdin every
// HeartbeatInterval, until Close or the command's end, and closes conn
// once a write fails or conn stops answering keepalives. One keepalive
// is checked at a time; a slow answer doesn't hold up the lines.
func (h *Held) heartbeat(conn *connection) {
	t := time.NewTicker(HeartbeatInterval)
	defer t.Stop()
	var checking atomic.Bool
	for {
		select {
		case <-t.C:
		case <-h.stop:
			return
		case <-h.done:
			return
		}
		if _, err := h.stdin.Write([]byte("\n")); err != nil {
			conn.close()
			return
		}
		if checking.CompareAndSwap(false, true) {
			go func() {
				defer checking.Store(false)
				if !conn.answers(context.Background()) {
					conn.close()
				}
			}()
		}
	}
}

// waitBounded waits for session's command to end, closing the session
// if it hasn't within d.
func waitBounded(session *ssh.Session, d time.Duration) {
	done := make(chan struct{})
	go func() {
		session.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(d):
		session.Close()
	}
}

// Done is closed once the held command has ended, as when the
// connection dropped.
func (h *Held) Done() <-chan struct{} { return h.done }

// Close ends the held command by closing its stdin, and waits for it:
// for releaseTimeout before it closes the session, and releaseTimeout
// more before it closes the connection.
func (h *Held) Close() error {
	h.once.Do(func() {
		close(h.stop)
		h.stdin.Close()
	})
	select {
	case <-h.done:
	case <-time.After(releaseTimeout):
		h.session.Close()
		select {
		case <-h.done:
		case <-time.After(releaseTimeout):
			h.conn.close()
			<-h.done
		}
	}
	var exit *ssh.ExitError
	if h.err != nil && !errors.As(h.err, &exit) && !errors.Is(h.err, io.EOF) {
		return h.err
	}
	return nil
}
