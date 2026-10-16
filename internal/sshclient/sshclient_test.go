package sshclient_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"errors"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/moostackhq/moostack/internal/sshclient"
)

func TestRunAsRootWithIdentityFile(t *testing.T) {
	d, h := startNode(t, "root")
	ctx := testContext(t)

	c, err := sshclient.Dial(ctx, d.node(), h.options())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	res, err := c.Run(ctx, "id -un; echo err >&2; exit 3", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(res.Stdout), "root\n"; got != want {
		t.Errorf("stdout = %q, want %q", got, want)
	}
	if got, want := string(res.Stderr), "err\n"; got != want {
		t.Errorf("stderr = %q, want %q", got, want)
	}
	if res.ExitCode != 3 {
		t.Errorf("exit code = %d, want 3", res.ExitCode)
	}
}

func TestHostKeyMismatchShowsBothFingerprints(t *testing.T) {
	d, h := startNode(t, "root")
	other, _, _ := newKey(t)

	_, err := sshclient.Dial(testContext(t), sshclient.Node{Target: "node", HostKeys: []ssh.PublicKey{other.PublicKey()}}, h.options())
	var mismatch *sshclient.HostKeyMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("err = %v, want a HostKeyMismatchError", err)
	}
	for _, fp := range []string{ssh.FingerprintSHA256(d.hostKey), ssh.FingerprintSHA256(other.PublicKey())} {
		if !strings.Contains(err.Error(), fp) {
			t.Errorf("error %q doesn't show %s", err, fp)
		}
	}
}

func TestFetchHostKeyNeedsNoCredentials(t *testing.T) {
	signer, _, _ := newKey(t)
	d := startSSHD(t, signer.PublicKey())
	h := newHome(t, nodeConfig(d, "root"))

	key, err := sshclient.FetchHostKey(testContext(t), "node", h.options())
	if err != nil {
		t.Fatal(err)
	}
	if got, want := ssh.FingerprintSHA256(key), ssh.FingerprintSHA256(d.hostKey); got != want {
		t.Errorf("fetched %s %s, want the ed25519 key %s", key.Type(), got, want)
	}
}

func TestPinnedKeyOfATypeTheHostLacksIsAMismatch(t *testing.T) {
	d, h := startNode(t, "root")
	// The container has a P-256 ECDSA host key, not a P-384 one.
	priv, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	p384, err := ssh.NewPublicKey(&priv.PublicKey)
	if err != nil {
		t.Fatal(err)
	}

	_, err = sshclient.Dial(testContext(t), sshclient.Node{Target: "node", HostKeys: []ssh.PublicKey{p384}}, h.options())
	var mismatch *sshclient.HostKeyMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("err = %v, want a HostKeyMismatchError", err)
	}
	for _, fp := range []string{ssh.FingerprintSHA256(d.hostKey), ssh.FingerprintSHA256(p384)} {
		if !strings.Contains(err.Error(), fp) {
			t.Errorf("error %q doesn't show %s", err, fp)
		}
	}
}

func TestRunThroughPasswordlessSudo(t *testing.T) {
	d, h := startNode(t, "op")
	ctx := testContext(t)

	c, err := sshclient.Dial(ctx, d.node(), h.options())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	res, err := c.Run(ctx, `id -un; cat; printf '%s\n' "it's quoted"`, strings.NewReader("input\n"))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(res.Stdout), "root\ninput\nit's quoted\n"; got != want || res.ExitCode != 0 {
		t.Errorf("stdout = %q, exit code %d, want %q and 0; stderr %q", got, res.ExitCode, want, res.Stderr)
	}
}

func TestDialFailsForAUserWithoutSudo(t *testing.T) {
	d, h := startNode(t, "nosudo")

	_, err := sshclient.Dial(testContext(t), d.node(), h.options())
	if err == nil || !strings.Contains(err.Error(), "passwordless sudo") {
		t.Fatalf("err = %v, want one about passwordless sudo", err)
	}
}

func TestAgentAuth(t *testing.T) {
	signer, _, priv := newKey(t)
	d := startSSHD(t, signer.PublicKey())
	h := newHome(t, `
Host node
  HostName 127.0.0.1
  Port `+d.port+`
  User root
`)
	h.env["SSH_AUTH_SOCK"] = startAgent(t, priv)
	ctx := testContext(t)

	c, err := sshclient.Dial(ctx, d.node(), h.options())
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
}

// TestProxyJump reaches the container's sshd at 127.0.0.2:22, an address
// only the container itself can reach, through the container as a jump
// host.
func TestProxyJump(t *testing.T) {
	signer, file, _ := newKey(t)
	d := startSSHD(t, signer.PublicKey())
	cases := []struct{ name, config string }{
		{"a jump host from the config", `
Host node
  HostName 127.0.0.2
  User op
  ProxyJump jump
Host jump
  HostName 127.0.0.1
  Port ` + d.port + `
  User root
`},
		{"two jump hosts with users and ports", `
Host node
  HostName 127.0.0.2
  User op
  ProxyJump root@127.0.0.1:` + d.port + `,nosudo@127.0.0.2
`},
		{"quoted jump hosts, as the shell reads them in OpenSSH", `
Host node
  HostName 127.0.0.2
  User op
  ProxyJump "jump",'nosudo'@127.0.0.2
Host jump
  HostName 127.0.0.1
  Port ` + d.port + `
  User root
`},
		{"a jump host's own ProxyJump", `
Host node
  HostName 127.0.0.2
  User op
  ProxyJump inner
Host inner
  HostName 127.0.0.2
  User nosudo
  ProxyJump outer
Host outer
  HostName 127.0.0.1
  Port ` + d.port + `
  User root
`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := newHome(t, c.config+`
Host *
  IdentityFile {home}/.ssh/key
  IdentitiesOnly yes
`)
			h.write(t, ".ssh/key", file)
			ctx := testContext(t)
			cl, err := sshclient.Dial(ctx, d.node(), h.options())
			if err != nil {
				t.Fatal(err)
			}
			defer cl.Close()
			res, err := cl.Run(ctx, "id -un", nil)
			if err != nil {
				t.Fatal(err)
			}
			if got := string(res.Stdout); got != "root\n" {
				t.Errorf("stdout = %q, want root", got)
			}
		})
	}
}

func TestProxyJumpThatTheShellWouldExpandIsRefused(t *testing.T) {
	for _, hop := range []string{
		`$JUMP`, "`jump`", `"$JUMP"`, `jump;true`, `*`,
		// OpenSSH runs the hops before the last through the shell once
		// more, so these would expand there.
		`'$JUMP',last`, `'"jump"',last`, `'a\b',last`, `[2001:db8::1]:22,last`,
		// OpenSSH expands % in the command it runs.
		`'a%%b'`,
	} {
		t.Run(hop, func(t *testing.T) {
			h := newHome(t, "Host node\n  ProxyJump "+hop+"\n")
			_, err := sshclient.Dial(testContext(t), sshclient.Node{Target: "node"}, h.options())
			if err == nil || !strings.Contains(err.Error(), "isn't supported") {
				t.Errorf("err = %v, want one saying it isn't supported", err)
			}
		})
	}
}

// TestPinnedRSAHostCertificate checks that a pinned RSA host certificate
// is asked for with its SHA-2 algorithms.
func TestPinnedRSAHostCertificate(t *testing.T) {
	d, h := startNode(t, "root")
	// The host presents its RSA key with a certificate, which OpenSSH 10
	// does only with SHA-2 signatures. sshd is PID 1.
	d.exec(t, "sh", "-c", `ssh-keygen -q -t ed25519 -N '' -f /etc/ssh/host_ca &&
ssh-keygen -q -s /etc/ssh/host_ca -I node -h -n 127.0.0.1 /etc/ssh/ssh_host_rsa_key.pub &&
echo 'HostCertificate /etc/ssh/ssh_host_rsa_key-cert.pub' >> /etc/ssh/sshd_config &&
kill -HUP 1`)
	pub, _, _, _, err := ssh.ParseAuthorizedKey([]byte(d.exec(t, "cat", "/etc/ssh/ssh_host_rsa_key-cert.pub")))
	if err != nil {
		t.Fatal(err)
	}
	// sshd reloads its config in the background, so try until it presents
	// the certificate.
	eventually(t, 10*time.Second, func() error {
		c, err := sshclient.Dial(testContext(t), sshclient.Node{Target: "node", HostKeys: []ssh.PublicKey{pub}}, h.options())
		if err == nil {
			c.Close()
		}
		return err
	})
}
