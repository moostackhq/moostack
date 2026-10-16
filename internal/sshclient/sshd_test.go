package sshclient_test

import (
	"fmt"
	"io"
	"net"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/moostackhq/moostack/internal/sshclient"
)

// This file runs an OpenSSH sshd in a container, from testdata/sshd. Root,
// op (passwordless sudo) and nosudo (no sudo) accept the same key.

const image = "moostack-sshclient-test"

var (
	containerOnce sync.Once
	// containerCmd is docker or podman.
	containerCmd string
	containerErr error
)

// containerRuntime returns docker or podman, whichever works, and builds
// the image once. It skips the test if neither works.
func containerRuntime(t *testing.T) string {
	t.Helper()
	containerOnce.Do(func() {
		for _, name := range []string{"docker", "podman"} {
			if exec.Command(name, "info").Run() == nil {
				containerCmd = name
				break
			}
		}
		if containerCmd == "" {
			return
		}
		out, err := exec.Command(containerCmd, "build", "-q", "-t", image, "testdata/sshd").CombinedOutput()
		if err != nil {
			containerErr = fmt.Errorf("building %s: %v\n%s", image, err, out)
		}
	})
	if containerCmd == "" {
		t.Skip("neither docker nor podman works")
	}
	if containerErr != nil {
		t.Fatal(containerErr)
	}
	return containerCmd
}

// sshd is a running container.
type sshd struct {
	runtime, name string
	// port is the port sshd is published on at 127.0.0.1.
	port string
	// hostKey is the container's ed25519 host key, read from its file.
	hostKey ssh.PublicKey
}

// startSSHD starts a container that accepts authorizedKey, and removes it
// when the test ends.
func startSSHD(t *testing.T, authorizedKey ssh.PublicKey) *sshd {
	t.Helper()
	rt := containerRuntime(t)
	out, err := exec.Command(rt, "run", "-d", "--rm", "-p", "127.0.0.1::22",
		"-e", "AUTHORIZED_KEY="+strings.TrimSpace(string(ssh.MarshalAuthorizedKey(authorizedKey))), image).CombinedOutput()
	if err != nil {
		t.Fatalf("starting sshd: %v\n%s", err, out)
	}
	d := &sshd{runtime: rt, name: strings.TrimSpace(string(out))}
	t.Cleanup(func() { exec.Command(rt, "rm", "-f", d.name).Run() })

	out, err = exec.Command(rt, "port", d.name, "22/tcp").Output()
	if err != nil {
		t.Fatalf("reading sshd's port: %v", err)
	}
	addr := strings.Fields(string(out))[0]
	d.port = addr[strings.LastIndex(addr, ":")+1:]

	pub, _, _, _, err := ssh.ParseAuthorizedKey([]byte(d.exec(t, "cat", "/etc/ssh/ssh_host_ed25519_key.pub")))
	if err != nil {
		t.Fatal(err)
	}
	d.hostKey = pub
	// Wait until sshd answers with its banner.
	eventually(t, 30*time.Second, func() error {
		if !d.listening() {
			return fmt.Errorf("sshd didn't start; logs:\n%s", d.logs())
		}
		return nil
	})
	return d
}

// listening reports whether sshd sends its banner on the published port.
func (d *sshd) listening() bool {
	conn, err := net.DialTimeout("tcp", "127.0.0.1:"+d.port, time.Second)
	if err != nil {
		return false
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(time.Second))
	banner := make([]byte, 4)
	_, err = io.ReadFull(conn, banner)
	return err == nil && string(banner) == "SSH-"
}

// exec runs a command in the container as root, and fails the test if it
// fails.
func (d *sshd) exec(t *testing.T, args ...string) string {
	t.Helper()
	out, err := exec.Command(d.runtime, append([]string{"exec", d.name}, args...)...).CombinedOutput()
	if err != nil {
		t.Fatalf("%s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}

func (d *sshd) logs() string {
	out, _ := exec.Command(d.runtime, "logs", d.name).CombinedOutput()
	return string(out)
}

// node is the node d is, reached as the SSH config's "node".
func (d *sshd) node() sshclient.Node {
	return sshclient.Node{Target: "node", HostKeys: []ssh.PublicKey{d.hostKey}}
}

// nodeConfig is an SSH config that reaches d as user with the key in
// ~/.ssh/key, under the name "node".
func nodeConfig(d *sshd, user string) string {
	return `
Host node
  HostName 127.0.0.1
  Port ` + d.port + `
  User ` + user + `
  IdentityFile {home}/.ssh/key
  IdentitiesOnly yes
`
}

// startNode starts a container and a home directory whose SSH config
// reaches it as user, with a key it accepts.
func startNode(t *testing.T, user string) (*sshd, *home) {
	t.Helper()
	signer, file, _ := newKey(t)
	d := startSSHD(t, signer.PublicKey())
	h := newHome(t, nodeConfig(d, user))
	h.write(t, ".ssh/key", file)
	return d, h
}
