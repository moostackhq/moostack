package sshclient

import (
	"context"
	"fmt"
	"net"

	"golang.org/x/crypto/ssh"
)

// DialTCP connects to addr as the node sees it, through the SSH
// connection.
func (c *Client) DialTCP(ctx context.Context, addr string) (net.Conn, error) {
	conn, _, err := open(ctx, c, func(client *ssh.Client) (net.Conn, error) {
		return client.Dial("tcp", addr)
	})
	if err != nil {
		return nil, fmt.Errorf("forwarding to %s: %w", addr, err)
	}
	return conn, nil
}
