//go:build linux

package agent

import (
	"io"
	"net"
)

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
