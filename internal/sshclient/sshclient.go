// Package sshclient connects to nodes over SSH with the operator's own
// credentials and SSH config.
package sshclient

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/moostackhq/moostack/internal/sshconfig"
)

// Options says how to find the operator's SSH config and credentials.
type Options struct {
	// SSH names the config files and the local user.
	SSH sshconfig.Options
	// Getenv looks up environment variables, such as SSH_AUTH_SOCK. Nil
	// means os.Getenv.
	Getenv func(string) string
	// Relay is the command that connects its stdin and stdout to a unix
	// socket on the node, for DialUnix through sudo. It is given the
	// socket's path quoted for the shell. Nil means DialUnix works only as
	// root.
	Relay func(socket string) string
}

func (o Options) getenv(name string) string {
	if o.Getenv == nil {
		return os.Getenv(name)
	}
	return o.Getenv(name)
}

// Node is a node to connect to.
type Node struct {
	// Target is the SSH target, host or user@host, resolved through the
	// SSH config.
	Target string
	// HostKeys are the host keys to trust. No other key is trusted.
	HostKeys []ssh.PublicKey
}

// Client is a connection to a node. Its commands run as root, directly or
// through passwordless sudo. If the connection drops, the next call
// reconnects once.
type Client struct {
	// dial makes a new connection to the node.
	dial func(ctx context.Context) (*connection, error)
	// sudo says commands need sudo to run as root.
	sudo  bool
	relay func(socket string) string

	mu     sync.Mutex
	conn   *connection
	closed bool
}

// Dial connects to n.
func Dial(ctx context.Context, n Node, opts Options) (*Client, error) {
	s, err := sshconfig.Resolve(n.Target, opts.SSH)
	if err != nil {
		return nil, err
	}
	c := &Client{relay: opts.Relay, dial: func(ctx context.Context) (*connection, error) {
		conn, err := dialNode(ctx, n, s, opts)
		if err != nil {
			return nil, fmt.Errorf("connecting to %s: %w", n.Target, err)
		}
		return conn, nil
	}}
	if c.conn, err = c.dial(ctx); err != nil {
		return nil, err
	}
	if s.User != "root" {
		if err := c.useSudo(ctx); err != nil {
			c.Close()
			return nil, fmt.Errorf("connecting to %s as %s: %w", n.Target, s.User, err)
		}
	}
	return c, nil
}

// dialNode connects to n, whose settings are s.
func dialNode(ctx context.Context, n Node, s sshconfig.Settings, opts Options) (*connection, error) {
	r, err := routeTo(ctx, s, opts, 0)
	if err != nil {
		return nil, err
	}
	client, netConn, err := connectAsOperator(ctx, r, s, opts, hostKeyCheck{pinned(n.Target, n.HostKeys), hostKeyAlgorithms(n.HostKeys)})
	var negotiation *ssh.AlgorithmNegotiationError
	if errors.As(err, &negotiation) && negotiation.What == "host key" {
		// The host has no key of a pinned type, so fetch the one it has.
		if key, fetchErr := fetchHostKey(ctx, r, s); fetchErr == nil {
			err = &HostKeyMismatchError{Target: n.Target, Got: key, Pinned: n.HostKeys}
		}
	}
	if err != nil {
		r.close()
		return nil, err
	}
	c := &connection{Client: client, route: r, netConn: netConn, dropped: make(chan struct{})}
	go func() {
		client.Wait()
		close(c.dropped)
	}()
	return c, nil
}

// connectAsOperator connects over r with the settings s, authenticating
// as the operator and checking the host key with check.
func connectAsOperator(ctx context.Context, r *route, s sshconfig.Settings, opts Options, check hostKeyCheck) (*ssh.Client, *watchedConn, error) {
	a := newAuth(ctx, s, opts)
	defer a.close()
	return connect(ctx, r, s, &ssh.ClientConfig{
		User:              s.User,
		Auth:              a.methods(),
		HostKeyCallback:   check.callback,
		HostKeyAlgorithms: check.algorithms,
	})
}

// connect makes an SSH connection over r with the settings s, giving up
// when ctx ends. As in OpenSSH, ConnectTimeout bounds connecting and
// waiting for the server's banner, but not the key exchange or
// authentication, where ssh-agent may wait for its user. Over jump hosts,
// each hop's own ConnectTimeout bounds reaching it; OpenSSH counts
// reaching the jump hosts against the node's ConnectTimeout too.
func connect(ctx context.Context, r *route, s sshconfig.Settings, config *ssh.ClientConfig) (*ssh.Client, *watchedConn, error) {
	addr := net.JoinHostPort(s.HostName, strconv.Itoa(s.Port))
	dialCtx := ctx
	var deadline time.Time
	if s.ConnectTimeout > 0 {
		deadline = time.Now().Add(s.ConnectTimeout)
		var cancel context.CancelFunc
		dialCtx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}
	// Over a jump host, this is a channel of the jump host's connection.
	raw, err := r.dial(dialCtx, addr)
	if err != nil {
		return nil, nil, err
	}
	netConn := &watchedConn{Conn: raw}
	var bannerTimedOut atomic.Bool
	if !deadline.IsZero() {
		banner := time.AfterFunc(time.Until(deadline), func() {
			bannerTimedOut.Store(true)
			netConn.Close()
		})
		defer banner.Stop()
		netConn.heard = func() { banner.Stop() }
	}
	stop := context.AfterFunc(ctx, func() { netConn.Close() })
	conn, chans, reqs, err := ssh.NewClientConn(netConn, addr, config)
	if !stop() {
		err = errors.Join(ctx.Err(), err)
	}
	if err != nil && bannerTimedOut.Load() {
		err = errors.New("timed out waiting for the server's banner")
	}
	if err != nil {
		netConn.Close()
		return nil, nil, err
	}
	return ssh.NewClient(conn, chans, reqs), netConn, nil
}

// useSudo checks whether the user is root, by ID, and if not that it can
// use passwordless sudo.
func (c *Client) useSudo(ctx context.Context) error {
	res, err := c.Run(ctx, "id -u", nil)
	if err != nil {
		return err
	}
	if strings.TrimSpace(string(res.Stdout)) == "0" {
		return nil
	}
	c.sudo = true
	res, err = c.Run(ctx, "true", nil)
	if err != nil {
		return err
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("the user isn't root and can't use passwordless sudo: %s", bytes.TrimSpace(res.Stderr))
	}
	return nil
}

// Close closes the connection, and those to its jump hosts.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	return c.conn.close()
}

// Result is what a command wrote and how it exited.
type Result struct {
	Stdout, Stderr []byte
	ExitCode       int
}

// Run runs cmd with the shell, with stdin as its input if it isn't nil. A
// command that exits non-zero isn't an error. No read of stdin starts once
// Run returns, though one already under way may finish. If ctx ends
// first, Run closes the session and returns, and the command may keep
// running.
func (c *Client) Run(ctx context.Context, cmd string, stdin io.Reader) (Result, error) {
	session, conn, err := c.newSession(ctx)
	if err != nil {
		return Result{}, err
	}
	defer session.Close()
	if stdin != nil {
		// x/crypto copies stdin in a goroutine of its own, which can start
		// another read after Run gives up on ctx.
		gate := &readGate{r: stdin}
		defer gate.close()
		session.Stdin = gate
	}
	type outcome struct {
		res Result
		err error
	}
	done := make(chan outcome, 1)
	go func() {
		var stdout, stderr bytes.Buffer
		session.Stdout = &stdout
		session.Stderr = &stderr
		err := session.Run(c.asRoot(cmd))
		res := Result{Stdout: stdout.Bytes(), Stderr: stderr.Bytes()}
		var exit *ssh.ExitError
		if errors.As(err, &exit) {
			res.ExitCode, err = exit.ExitStatus(), nil
		}
		done <- outcome{res, err}
	}()
	select {
	case o := <-done:
		return o.res, o.err
	case <-ctx.Done():
		conn.probe()
		return Result{}, ctx.Err()
	}
}

// readGate reads from r until it is closed. Closing it doesn't wait for a
// read under way, which the caller's reader may block in for good, and a
// read that checked just before the close still goes ahead.
type readGate struct {
	r      io.Reader
	closed atomic.Bool
}

func (g *readGate) Read(p []byte) (int, error) {
	if g.closed.Load() {
		return 0, io.EOF
	}
	return g.r.Read(p)
}

// close stops any more reads from r starting.
func (g *readGate) close() { g.closed.Store(true) }

// asRoot is cmd run as root.
func (c *Client) asRoot(cmd string) string {
	if c.sudo {
		return "sudo -n -- sh -c " + shellQuote(cmd)
	}
	return cmd
}
