package sshclient_test

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/moostackhq/moostack/internal/sshclient"
)

// This file runs an SSH server in the test process. It runs exec requests
// with the local shell, so a client logs in as "root" to skip sudo.

// server is an in-process SSH server.
type server struct {
	listener net.Listener
	port     string
	hostKey  ssh.Signer

	mu sync.Mutex
	// offered are the public keys clients offered, in order.
	offered []ssh.PublicKey
	// conns are the connections open now.
	conns []ssh.Conn
	// accepted counts the connections accepted so far.
	accepted int
	// dropNew says to close each new connection once it is accepted.
	dropNew bool
	// arrived counts the TCP connections that arrived, handshake or not.
	arrived atomic.Int32
	// frozen, while set, stops the server reading from any connection, as
	// if the network had gone silent.
	frozen atomic.Bool
	// keepaliveDelay, while above zero, is how long the server waits before
	// answering a keepalive, as one queued behind a busy connection's data
	// would be.
	keepaliveDelay atomic.Int64
	// authDelay is how long the server takes to answer each key offered.
	authDelay atomic.Int64
	// userCA, if set, is the only key the server accepts: as the authority
	// of a user certificate.
	userCA atomic.Pointer[ssh.PublicKey]
	// ran are the commands clients asked to run, in order.
	ran []string
	// stallExec, while set, makes the server never answer a request to
	// run a command.
	stallExec atomic.Bool
	// fakeBin, if set, is a directory of commands that shadow the real
	// ones.
	fakeBin atomic.Pointer[string]
	// tcpConns are the TCP connections that arrived.
	tcpConns []*freezableConn
}

// startServer starts a server that accepts authorized, and stops it when
// the test ends.
func startServer(t *testing.T, authorized ...ssh.PublicKey) *server {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	hostKey, err := ssh.NewSignerFromKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	s := &server{listener: l, port: strconv.Itoa(l.Addr().(*net.TCPAddr).Port), hostKey: hostKey}
	config := &ssh.ServerConfig{
		// Let a client offer every key it has.
		MaxAuthTries: -1,
		PublicKeyCallback: func(meta ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
			time.Sleep(time.Duration(s.authDelay.Load()))
			s.mu.Lock()
			s.offered = append(s.offered, key)
			s.mu.Unlock()
			if ca := s.userCA.Load(); ca != nil {
				checker := ssh.CertChecker{IsUserAuthority: func(auth ssh.PublicKey) bool {
					return bytes.Equal(auth.Marshal(), (*ca).Marshal())
				}}
				return checker.Authenticate(meta, key)
			}
			for _, a := range authorized {
				if bytes.Equal(a.Marshal(), key.Marshal()) {
					return nil, nil
				}
			}
			return nil, errUnauthorized
		},
	}
	config.AddHostKey(hostKey)
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			s.arrived.Add(1)
			fc := &freezableConn{Conn: conn, frozen: &s.frozen}
			s.mu.Lock()
			s.tcpConns = append(s.tcpConns, fc)
			s.mu.Unlock()
			go s.serve(fc, config)
		}
	}()
	return s
}

var errUnauthorized = errors.New("unauthorized")

func (s *server) serve(c net.Conn, config *ssh.ServerConfig) {
	conn, chans, reqs, err := ssh.NewServerConn(c, config)
	if err != nil {
		c.Close()
		return
	}
	s.mu.Lock()
	s.conns = append(s.conns, conn)
	s.accepted++
	dropNew := s.dropNew
	s.mu.Unlock()
	if dropNew {
		conn.Close()
	}
	go s.answerGlobalRequests(reqs)
	for nc := range chans {
		if nc.ChannelType() != "session" {
			nc.Reject(ssh.UnknownChannelType, "only sessions")
			continue
		}
		ch, reqs, err := nc.Accept()
		if err != nil {
			continue
		}
		go s.serveSession(ch, reqs)
	}
}

// serveSession runs the session's exec request with the local shell.
func (s *server) serveSession(ch ssh.Channel, reqs <-chan *ssh.Request) {
	defer ch.Close()
	for req := range reqs {
		if req.Type != "exec" || len(req.Payload) < 4 {
			req.Reply(false, nil)
			continue
		}
		s.mu.Lock()
		s.ran = append(s.ran, string(req.Payload[4:]))
		s.mu.Unlock()
		if s.stallExec.Load() {
			// Never answer, until the client gives up on the session.
			for range reqs {
			}
			return
		}
		req.Reply(true, nil)
		cmd := exec.Command("sh", "-c", string(req.Payload[4:]))
		if dir := s.fakeBin.Load(); dir != nil {
			cmd.Env = append(os.Environ(), "PATH="+*dir+":"+os.Getenv("PATH"))
		}
		cmd.Stdout, cmd.Stderr = ch, ch.Stderr()
		// As sshd does, don't wait for the input the command never reads.
		stdin, err := cmd.StdinPipe()
		if err != nil {
			return
		}
		go func() {
			io.Copy(stdin, ch)
			stdin.Close()
		}()
		status := uint32(0)
		if err := cmd.Run(); err != nil {
			status = 1
			if exit, ok := err.(*exec.ExitError); ok {
				status = uint32(exit.ExitCode())
			}
		}
		ch.SendRequest("exit-status", false, binary.BigEndian.AppendUint32(nil, status))
		return
	}
}

// offeredKeys returns the keys clients offered so far.
func (s *server) offeredKeys() []ssh.PublicKey {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]ssh.PublicKey(nil), s.offered...)
}

// answerGlobalRequests turns down every global request, which is how
// OpenSSH answers a keepalive, after keepaliveDelay.
func (s *server) answerGlobalRequests(reqs <-chan *ssh.Request) {
	for req := range reqs {
		go func() {
			time.Sleep(time.Duration(s.keepaliveDelay.Load()))
			req.Reply(false, nil)
		}()
	}
}

// pretendNotRoot makes the server's commands see a user who isn't root
// but has passwordless sudo: id -u prints 1000, and sudo runs its command.
func (s *server) pretendNotRoot(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	for name, script := range map[string]string{
		"id":   "#!/bin/sh\necho 1000\n",
		"sudo": "#!/bin/sh\nwhile [ \"$1\" != -- ]; do shift; done\nshift\nexec \"$@\"\n",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(script), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	s.fakeBin.Store(&dir)
}

// freezeOpen stops the server reading from the connections open now, for
// good, as if the network to them had gone silent. New connections work.
func (s *server) freezeOpen() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.tcpConns {
		c.frozenAlone.Store(true)
	}
}

// freezableConn reads nothing while frozen or frozenAlone is set.
type freezableConn struct {
	net.Conn
	frozen *atomic.Bool
	// frozenAlone freezes this connection alone.
	frozenAlone atomic.Bool
}

func (c *freezableConn) Read(p []byte) (int, error) {
	for c.frozen.Load() || c.frozenAlone.Load() {
		time.Sleep(10 * time.Millisecond)
	}
	return c.Conn.Read(p)
}

// ranCommands returns the commands clients asked to run so far.
func (s *server) ranCommands() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.ran...)
}

// acceptedConns counts the connections accepted so far.
func (s *server) acceptedConns() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.accepted
}

// drop closes every connection the server has accepted, and if dropNew
// is set, every one it accepts from now on.
func (s *server) drop(dropNew bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dropNew = dropNew
	for _, c := range s.conns {
		c.Close()
	}
	s.conns = nil
}

// config is an SSH config that reaches s as root under the name "node",
// followed by extra.
func (s *server) config(extra string) string {
	return `
Host node
  HostName 127.0.0.1
  Port ` + s.port + `
  User root
` + extra
}

// node is the node s is, reached as the SSH config's "node".
func (s *server) node() sshclient.Node {
	return sshclient.Node{Target: "node", HostKeys: []ssh.PublicKey{s.hostKey.PublicKey()}}
}
