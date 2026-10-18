package sshclient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"golang.org/x/crypto/ssh"
)

// DialUnix connects to the unix socket at socket on the node, for a gRPC
// client's dialer. As root, sshd connects to the socket itself. Through
// sudo, the node runs Options.Relay as root, since sshd would connect as
// the user, who can't open a root-only socket. A relayed connection has
// no deadlines.
func (c *Client) DialUnix(ctx context.Context, socket string) (net.Conn, error) {
	if !c.sudo {
		conn, _, err := open(ctx, c, func(client *ssh.Client) (net.Conn, error) {
			return client.Dial("unix", socket)
		})
		if err != nil {
			return nil, fmt.Errorf("forwarding to %s: %w", socket, err)
		}
		return conn, nil
	}
	if c.relay == nil {
		return nil, fmt.Errorf("forwarding to %s: the user isn't root and no relay is set", socket)
	}
	conn, err := c.runRelay(ctx, c.relay(socket), socket)
	if err != nil {
		return nil, fmt.Errorf("forwarding to %s: %w", socket, err)
	}
	return conn, nil
}

// runRelay starts cmd as root and returns its stdin and stdout as a
// connection.
func (c *Client) runRelay(ctx context.Context, cmd, socket string) (_ net.Conn, err error) {
	session, conn, err := c.newSession(ctx)
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
	// Start waits for the node to answer, which a silent connection never
	// does.
	if _, err = withContext(ctx, func() (*ssh.Session, error) { return session, session.Start(c.asRoot(cmd)) }); err != nil {
		if ctx.Err() != nil {
			conn.probe()
		}
		return nil, err
	}
	return &relayConn{session: session, stdin: stdin, stdout: stdout, addr: &net.UnixAddr{Name: socket, Net: "unix"}}, nil
}

// relayConn is a connection through a relay command's stdin and stdout.
// It has no deadlines.
type relayConn struct {
	session *ssh.Session
	stdin   io.WriteCloser
	stdout  io.Reader
	addr    net.Addr
}

func (r *relayConn) Read(p []byte) (int, error)  { return r.stdout.Read(p) }
func (r *relayConn) Write(p []byte) (int, error) { return r.stdin.Write(p) }

func (r *relayConn) Close() error {
	return errors.Join(r.stdin.Close(), r.session.Close())
}

func (r *relayConn) LocalAddr() net.Addr  { return &net.UnixAddr{Name: "@", Net: "unix"} }
func (r *relayConn) RemoteAddr() net.Addr { return r.addr }

func (r *relayConn) SetDeadline(time.Time) error      { return errNoDeadlines }
func (r *relayConn) SetReadDeadline(time.Time) error  { return errNoDeadlines }
func (r *relayConn) SetWriteDeadline(time.Time) error { return errNoDeadlines }

var errNoDeadlines = errors.New("a relayed connection has no deadlines")
