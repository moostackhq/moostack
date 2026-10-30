package sshclient

import (
	"context"
	"errors"
	"io"
	"time"

	"golang.org/x/crypto/ssh"
)

// Stream runs cmd as root with stdin, if not nil, as its input, and its
// output going to stdout and stderr as it comes, and returns its exit
// code. If ctx ends first, Stream closes the session, which ends the
// command, and returns ctx's error.
func (c *Client) Stream(ctx context.Context, cmd string, stdin io.Reader, stdout, stderr io.Writer) (int, error) {
	session, conn, err := c.newSession(ctx)
	if err != nil {
		return 0, err
	}
	defer session.Close()
	if stdin != nil {
		gate := &readGate{r: stdin}
		defer gate.close()
		session.Stdin = gate
	}
	session.Stdout, session.Stderr = stdout, stderr
	if _, err := withContext(ctx, func() (*ssh.Session, error) { return session, session.Start(c.asRoot(cmd)) }); err != nil {
		if ctx.Err() != nil {
			conn.probe()
		}
		return 0, err
	}
	done := make(chan error, 1)
	go func() { done <- session.Wait() }()
	select {
	case err := <-done:
		var exit *ssh.ExitError
		if errors.As(err, &exit) {
			return exit.ExitStatus(), nil
		}
		return 0, err
	case <-ctx.Done():
		session.Signal(ssh.SIGTERM)
		session.Close()
		return 0, ctx.Err()
	}
}

// RunStoppable is Run, with cmd run in a process group of its own on the
// node, which is killed once ctx ends, the connection drops, or no
// heartbeat line has reached the node for 45 seconds. A heartbeat line
// goes every HeartbeatInterval.
func (c *Client) RunStoppable(ctx context.Context, cmd string) (Result, error) {
	r, w := io.Pipe()
	done := make(chan struct{})
	defer close(done)
	go func() {
		defer w.Close()
		t := time.NewTicker(HeartbeatInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case <-t.C:
				if _, err := w.Write([]byte("\n")); err != nil {
					return
				}
			}
		}
	}()
	return c.Run(ctx, stoppable(cmd), r)
}

// stoppable is a shell script that runs cmd in a session of its own and
// kills that session's processes once the script's stdin ends or no line
// has come on it for 45 seconds.
func stoppable(cmd string) string {
	return `exec 3<&0
setsid sh -c ` + ShellQuote(cmd) + ` </dev/null 3<&- &
c=$!
(exec >/dev/null 2>&1; while timeout 45 sh -c 'IFS= read -r _' <&3; do :; done; kill -TERM -- "-$c") &
w=$!
wait "$c"
s=$?
kill "$w" 2>/dev/null
exit "$s"`
}
