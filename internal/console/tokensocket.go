package console

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/moostackhq/moostack/internal/filelock"
)

// The token socket is a socket file on which the console answers each
// connection with a new login token and a newline.

// ListenTokenSocket listens on the token socket at path, an absolute
// path, which only its owner may connect to. It replaces a socket that
// nothing listens on, and refuses a file that is no socket, and a
// socket that a console answers on or that can't be dialled. It holds
// a lock on the file path.lock meanwhile, so that of consoles starting
// at once one gets the socket.
func ListenTokenSocket(path string) (net.Listener, error) {
	if !filepath.IsAbs(path) {
		return nil, fmt.Errorf("the token socket's path %q isn't absolute", path)
	}
	unlock, err := filelock.Lock(path + ".lock")
	if err != nil {
		return nil, err
	}
	defer unlock()
	if info, err := os.Lstat(path); err == nil && info.Mode().Type() != os.ModeSocket {
		return nil, fmt.Errorf("%s is no socket", path)
	}
	conn, err := net.DialTimeout("unix", path, time.Second)
	if err == nil {
		conn.Close()
		return nil, fmt.Errorf("a console is listening at %s already", path)
	}
	if !errors.Is(err, syscall.ECONNREFUSED) && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("the token socket at %s is in use: %w", path, err)
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	old := syscall.Umask(0o177)
	defer syscall.Umask(old)
	return net.Listen("unix", path)
}

// RequestToken asks the console that listens on the token socket at
// path, an absolute path, for a login token. It gives up 5 seconds
// after it was called, or when ctx ends.
func RequestToken(ctx context.Context, path string) (string, error) {
	if !filepath.IsAbs(path) {
		return "", fmt.Errorf("the token socket's path %q isn't absolute", path)
	}
	deadline := time.Now().Add(5 * time.Second)
	dialer := net.Dialer{Deadline: deadline}
	conn, err := dialer.DialContext(ctx, "unix", path)
	if err != nil {
		return "", fmt.Errorf("no console is listening at %s: %w", path, err)
	}
	defer conn.Close()
	defer context.AfterFunc(ctx, func() { conn.Close() })()
	conn.SetDeadline(deadline)
	answer, err := io.ReadAll(io.LimitReader(conn, 64))
	if ctx.Err() != nil {
		return "", ctx.Err()
	}
	if err != nil {
		return "", fmt.Errorf("reading the token from %s: %w", path, err)
	}
	token := strings.TrimSuffix(string(answer), "\n")
	if !isToken(token) {
		return "", fmt.Errorf("the console at %s gave no token", path)
	}
	return token, nil
}

// serveTokens answers the connections to l until l is closed. After a
// failed accept it waits, from 5 milliseconds up to a second, and goes
// on.
func (c *Console) serveTokens(l net.Listener) {
	var wait time.Duration
	for {
		conn, err := l.Accept()
		if errors.Is(err, net.ErrClosed) {
			return
		}
		if err != nil {
			slog.Error("accepting on the token socket", "err", err)
			wait = min(max(2*wait, 5*time.Millisecond), time.Second)
			time.Sleep(wait)
			continue
		}
		wait = 0
		conn.SetDeadline(time.Now().Add(5 * time.Second))
		io.WriteString(conn, c.tokens.issue()+"\n")
		conn.Close()
	}
}
