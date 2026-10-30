package sshclient_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"

	"github.com/moostackhq/moostack/internal/sshclient"
	"github.com/moostackhq/moostack/internal/sshconfig"
)

// This file sets up the operator's side: keys, an agent, and a home
// directory with an SSH config.

// newKey makes an ed25519 key and returns its signer, its private key in
// OpenSSH's file format, and the private key itself.
func newKey(t *testing.T) (ssh.Signer, []byte, ed25519.PrivateKey) {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := ssh.NewSignerFromKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	block, err := ssh.MarshalPrivateKey(priv, "")
	if err != nil {
		t.Fatal(err)
	}
	return signer, pem.EncodeToMemory(block), priv
}

// home is a local user's home directory, with an SSH config.
type home struct {
	dir string
	env map[string]string
}

// newHome makes a home directory for the local user "op" whose
// ~/.ssh/config is config. "{home}" in config is replaced by the directory.
func newHome(t *testing.T, config string) *home {
	t.Helper()
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, ".ssh"), 0o700); err != nil {
		t.Fatal(err)
	}
	h := &home{dir: dir, env: map[string]string{}}
	h.write(t, ".ssh/config", []byte(strings.ReplaceAll(config, "{home}", dir)))
	return h
}

// write writes a file under the home directory, readable only by its
// owner.
func (h *home) write(t *testing.T, name string, data []byte) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(h.dir, name), data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func (h *home) options() sshclient.Options {
	return sshclient.Options{
		SSH: sshconfig.Options{
			UserConfig: filepath.Join(h.dir, ".ssh", "config"),
			LocalUser:  "op",
			Home:       h.dir,
		},
		Getenv: func(name string) string { return h.env[name] },
	}
}

// testContext returns a context that ends with the test, or after a
// minute.
func testContext(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	t.Cleanup(cancel)
	return ctx
}

// startAgent serves an ssh-agent holding keys and returns its socket.
func startAgent(t *testing.T, keys ...ed25519.PrivateKey) string {
	t.Helper()
	return startAgentWith(t, addedKeys(keys...)...)
}

// startAgentWith serves an ssh-agent holding keys and returns its socket.
func startAgentWith(t *testing.T, keys ...agent.AddedKey) string {
	t.Helper()
	return serveAgent(t, newKeyring(t, keys...))
}

// addedKeys are keys as ssh-agent holds them.
func addedKeys(keys ...ed25519.PrivateKey) []agent.AddedKey {
	var added []agent.AddedKey
	for _, k := range keys {
		added = append(added, agent.AddedKey{PrivateKey: k})
	}
	return added
}

// newKeyring is an ssh-agent keyring holding keys.
func newKeyring(t *testing.T, keys ...agent.AddedKey) agent.Agent {
	t.Helper()
	keyring := agent.NewKeyring()
	for _, k := range keys {
		if err := keyring.Add(k); err != nil {
			t.Fatal(err)
		}
	}
	return keyring
}

// serveAgent serves keyring as an ssh-agent and returns its socket.
func serveAgent(t *testing.T, keyring agent.Agent) string {
	t.Helper()
	l, sock := listenUnix(t)
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				agent.ServeAgent(keyring, conn)
			}()
		}
	}()
	return sock
}

// listenUnix listens on a new unix socket, until the test ends, and
// returns the listener and the socket's path.
func listenUnix(t *testing.T) (net.Listener, string) {
	t.Helper()
	// t.TempDir's paths can be too long for a unix socket.
	dir, err := os.MkdirTemp("", "sock")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	sock := filepath.Join(dir, "sock")
	l, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	return l, sock
}

// eventually calls try until it succeeds, and fails the test with its last
// error if it hasn't within timeout.
func eventually(t *testing.T, timeout time.Duration, try func() error) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		err := try()
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
