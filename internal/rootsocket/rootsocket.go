//go:build linux

// Package rootsocket is the unix sockets that the agent and the updater
// serve the node API on, which only root can connect to, and the relay
// that lets mooctl reach them through sudo.
package rootsocket

import (
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"syscall"
)

// Listen listens on the unix socket at path, which only root can connect
// to: the socket and its directory are root's alone. It replaces a socket
// left at path by an earlier program.
func Listen(path string) (net.Listener, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return nil, err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	// The socket is created with the umask's mode, so no one else can
	// connect between bind and chmod.
	old := syscall.Umask(0o177)
	l, err := net.Listen("unix", path)
	syscall.Umask(old)
	return l, err
}

// Relay connects in and out to the unix socket at socket, so that mooctl
// can reach a root-only socket through sudo. When in ends, it closes the
// socket for writing, and it returns once the socket's side closes.
func Relay(socket string, in io.Reader, out io.Writer) error {
	conn, err := net.Dial("unix", socket)
	if err != nil {
		return err
	}
	defer conn.Close()
	go func() {
		io.Copy(conn, in)
		conn.(*net.UnixConn).CloseWrite()
	}()
	_, err = io.Copy(out, conn)
	return err
}
