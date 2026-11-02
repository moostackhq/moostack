package sshclient_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"encoding/pem"
	"net"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"

	"github.com/moostackhq/moostack/internal/sshclient"
)

// TestIdentitySources checks where keys come from: identity files,
// passphrase-protected or public, and the ssh-agent that IdentityAgent
// names. TestKeyOrderMatchesOpenSSH checks the order they are offered in.
// Keys are named a, b and none, for IdentityFile none. The agent socket
// is in SSH_AUTH_SOCK unless env moves it, and "{agent}" in config is
// replaced by it.
func TestIdentitySources(t *testing.T) {
	cases := []struct {
		name      string
		agentKeys []string
		// files are the identity files ~/.ssh/<name>, written with a
		// passphrase if the name is in protected.
		files     []string
		protected []string
		// public are files that hold only the public key.
		public     []string
		config     string
		env        string
		authorized string
		// offered are the keys offered, in order.
		offered []string
		fails   bool
		// inSSHDir runs the case in ~/.ssh, where relative identity files
		// are.
		inSSHDir bool
	}{
		{
			name:      "a passphrase-protected identity file is used from the agent",
			agentKeys: []string{"a", "b"}, files: []string{"b"}, protected: []string{"b"},
			config:     "  IdentityFile {home}/.ssh/b\n  IdentitiesOnly yes\n",
			authorized: "b", offered: []string{"b"},
		},
		{
			name:      "an identity file may be a public key the agent holds",
			agentKeys: []string{"a", "b"}, files: []string{"b"}, public: []string{"b"},
			config:     "  IdentityFile {home}/.ssh/b\n  IdentitiesOnly yes\n",
			authorized: "b", offered: []string{"b"},
		},
		{
			name:  "IdentityFile none is no file, even if one is called none",
			files: []string{"none"}, inSSHDir: true,
			config:     "  IdentityFile none\n  IdentitiesOnly yes\n",
			authorized: "none", fails: true,
		},
		{
			name:      "IdentityAgent none ignores the agent",
			agentKeys: []string{"a"},
			config:    "  IdentityAgent none\n", authorized: "a", fails: true,
		},
		{
			name:      "IdentityAgent $NAME reads the socket from NAME",
			agentKeys: []string{"a"}, env: "MY_AGENT",
			config: "  IdentityAgent $MY_AGENT\n", authorized: "a", offered: []string{"a"},
		},
		{
			name:      "IdentityAgent may be a path",
			agentKeys: []string{"a"}, env: "UNUSED",
			config: "  IdentityAgent {agent}\n", authorized: "a", offered: []string{"a"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			keys, pubs := namedKeys(t, "a", "b", "none")
			var agentKeys []ed25519.PrivateKey
			for _, name := range c.agentKeys {
				agentKeys = append(agentKeys, keys[name])
			}
			sock := startAgent(t, agentKeys...)
			s := startServer(t, pubs[c.authorized])
			h := newHome(t, s.config(strings.ReplaceAll(c.config, "{agent}", sock)))
			h.env["SSH_AUTH_SOCK"] = sock
			if c.env != "" {
				delete(h.env, "SSH_AUTH_SOCK")
				h.env[c.env] = sock
			}
			for _, name := range c.files {
				passphrase := ""
				if slices.Contains(c.protected, name) {
					passphrase = "secret"
				}
				if slices.Contains(c.public, name) {
					h.write(t, ".ssh/"+name, ssh.MarshalAuthorizedKey(pubs[name]))
					continue
				}
				h.write(t, ".ssh/"+name, privateKeyFile(t, keys[name], passphrase))
			}

			if c.inSSHDir {
				t.Chdir(filepath.Join(h.dir, ".ssh"))
			}
			cl, err := sshclient.Dial(testContext(t), s.node(), h.options())
			if c.fails && err == nil {
				t.Error("Dial succeeded, want it to fail")
			}
			if !c.fails && err != nil {
				t.Errorf("Dial: %v", err)
			}
			if cl != nil {
				cl.Close()
			}
			var got, want []string
			for _, k := range s.offeredKeys() {
				got = append(got, ssh.FingerprintSHA256(k))
			}
			for _, name := range c.offered {
				want = append(want, ssh.FingerprintSHA256(pubs[name]))
			}
			if strings.Join(got, " ") != strings.Join(want, " ") {
				t.Errorf("offered %v, want %v (a is %s, b is %s)", got, want,
					ssh.FingerprintSHA256(pubs["a"]), ssh.FingerprintSHA256(pubs["b"]))
			}
		})
	}
}

// privateKeyFile is key in OpenSSH's file format, encrypted with
// passphrase unless it is empty.
func privateKeyFile(t *testing.T, key ed25519.PrivateKey, passphrase string) []byte {
	t.Helper()
	var block *pem.Block
	var err error
	if passphrase == "" {
		block, err = ssh.MarshalPrivateKey(key, "")
	} else {
		block, err = ssh.MarshalPrivateKeyWithPassphrase(key, "", []byte(passphrase))
	}
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(block)
}

func TestDialGivesUpOnAStalledAgent(t *testing.T) {
	for _, c := range []struct {
		name string
		// ctxTimeout, if set, ends Dial's context; otherwise ssh-agent's
		// own timeout has to.
		ctxTimeout bool
	}{
		{name: "when the context ends", ctxTimeout: true},
		{name: "when the request to ssh-agent times out"},
	} {
		t.Run(c.name, func(t *testing.T) {
			signer, _, _ := newKey(t)
			s := startServer(t, signer.PublicKey())
			h := newHome(t, s.config(""))
			// The agent accepts connections and never answers.
			l, sock := listenUnix(t)
			holdConnections(t, l)
			h.env["SSH_AUTH_SOCK"] = sock
			ctx := testContext(t)
			if c.ctxTimeout {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, time.Second)
				defer cancel()
			} else {
				sshclient.SetAgentTimeout(t, time.Second)
			}
			done := make(chan error, 1)
			go func() {
				_, err := sshclient.Dial(ctx, s.node(), h.options())
				done <- err
			}()
			select {
			case err := <-done:
				if err == nil {
					t.Error("Dial succeeded through a stalled agent")
				}
			case <-time.After(10 * time.Second):
				t.Fatal("Dial didn't give up on the stalled agent")
			}
		})
	}
}

// TestACertificateFileAuthenticates checks that an identity file's
// -cert.pub is offered with its key, to a server that accepts only
// certificates.
func TestACertificateFileAuthenticates(t *testing.T) {
	ca, _, _ := newKey(t)
	signer, file, _ := newKey(t)
	cert := &ssh.Certificate{
		Key: signer.PublicKey(), CertType: ssh.UserCert, KeyId: "op",
		ValidPrincipals: []string{"root"}, ValidBefore: ssh.CertTimeInfinity,
	}
	if err := cert.SignCert(rand.Reader, ca); err != nil {
		t.Fatal(err)
	}
	s := startServer(t)
	caKey := ca.PublicKey()
	s.userCA.Store(&caKey)
	h := newHome(t, s.config("  IdentityFile {home}/.ssh/key\n  IdentitiesOnly yes\n"))
	h.write(t, ".ssh/key", file)
	h.write(t, ".ssh/key-cert.pub", ssh.MarshalAuthorizedKey(cert))
	c, err := sshclient.Dial(testContext(t), s.node(), h.options())
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
}

// TestKeyOrderMatchesOpenSSH checks the order keys and certificates are
// offered in against what OpenSSH 9.9 and 10.3 offer, as ssh -v lists
// them, for the same identity files and ssh-agent, and against what
// OpenSSH's pubkey_prepare and sign_and_send_pubkey do. Keys are a to d,
// and "x-cert" is a user certificate for x. The server accepts nothing,
// so every key that can be signed for is offered.
func TestKeyOrderMatchesOpenSSH(t *testing.T) {
	type file struct {
		// name is the identity file's name in ~/.ssh.
		name string
		// key is the key it holds, privately unless public is set, or
		// empty for no file. A private key is passphrase-protected if
		// protected is set.
		key       string
		public    bool
		protected bool
		// holdsCert, if set, is a certificate the file holds instead.
		holdsCert string
		// pub is the key in name.pub, if any.
		pub string
		// cert is the certificate in name-cert.pub, if any. "hostcert"
		// is a host certificate for the key.
		cert string
	}
	cases := []struct {
		name           string
		files          []file
		agent          []string
		identitiesOnly bool
		offered        []string
	}{
		{
			name:    "identities ssh-agent holds come first, in its order, then its other keys, then the rest",
			files:   []file{{name: "a", key: "a"}, {name: "b", key: "b", cert: "b-cert"}, {name: "c", key: "c"}},
			agent:   []string{"b", "b-cert", "c", "d"},
			offered: []string{"b", "b-cert", "c", "d", "a"},
		},
		{
			name:           "a certificate on disk comes straight after its own key",
			files:          []file{{name: "a", key: "a", cert: "a-cert"}, {name: "b", key: "b", cert: "b-cert"}},
			identitiesOnly: true,
			offered:        []string{"a", "a-cert", "b", "b-cert"},
		},
		{
			name: "an identity ssh-agent holds leaves its certificate on disk with the rest",
			files: []file{
				{name: "a", key: "a", cert: "a-cert"}, {name: "b", key: "b", cert: "b-cert"}, {name: "c", key: "c", cert: "c-cert"},
			},
			agent:   []string{"b", "c", "d"},
			offered: []string{"b", "c", "d", "a", "a-cert", "b-cert", "c-cert"},
		},
		{
			name: "IdentitiesOnly leaves out ssh-agent's other keys",
			files: []file{
				{name: "a", key: "a", cert: "a-cert"}, {name: "b", key: "b", cert: "b-cert"}, {name: "c", key: "c", cert: "c-cert"},
			},
			agent: []string{"b", "c", "d"}, identitiesOnly: true,
			offered: []string{"b", "c", "a", "a-cert", "b-cert", "c-cert"},
		},
		{
			name:    "a certificate ssh-agent holds without a -cert.pub is one of its other keys",
			files:   []file{{name: "c", key: "c", public: true}},
			agent:   []string{"c", "c-cert"},
			offered: []string{"c", "c-cert"},
		},
		{
			name:  "so IdentitiesOnly leaves it out",
			files: []file{{name: "c", key: "c", public: true}},
			agent: []string{"c", "c-cert"}, identitiesOnly: true,
			offered: []string{"c"},
		},
		{
			name:    "a -cert.pub without its key is signed by ssh-agent",
			files:   []file{{name: "e", cert: "c-cert"}},
			agent:   []string{"c"},
			offered: []string{"c", "c-cert"},
		},
		{
			// OpenSSH offers the certificate, then can't sign it, as
			// ssh-agent's key isn't one of its identities; leaving it out
			// ends the same way.
			name:  "under IdentitiesOnly, a -cert.pub without its key isn't usable",
			files: []file{{name: "e", cert: "c-cert"}},
			agent: []string{"c"}, identitiesOnly: true,
			offered: nil,
		},
		{
			name:  "a certificate is signed by ssh-agent's key for an identity that is only a public key",
			files: []file{{name: "pc", key: "c", public: true, cert: "c-cert"}},
			agent: []string{"c"}, identitiesOnly: true,
			offered: []string{"c", "c-cert"},
		},
		{
			name:  "a certificate is signed by ssh-agent's key for a passphrase-protected identity",
			files: []file{{name: "enc", key: "c", protected: true, cert: "c-cert"}},
			agent: []string{"c"}, identitiesOnly: true,
			offered: []string{"c", "c-cert"},
		},
		{
			name:  "a certificate is signed by another identity file's key",
			files: []file{{name: "e", cert: "c-cert"}, {name: "cplain", key: "c"}}, identitiesOnly: true,
			offered: []string{"c-cert", "c"},
		},
		{
			name:    "an identity file may itself be a certificate",
			files:   []file{{name: "cc", holdsCert: "c-cert"}},
			agent:   []string{"c"},
			offered: []string{"c", "c-cert"},
		},
		{
			// OpenSSH offers b, then can't sign it, and never uses a.
			name:  "a private key that isn't the identity's isn't used",
			files: []file{{name: "m", key: "a", pub: "b"}}, identitiesOnly: true,
			offered: nil,
		},
		{
			name:  "an identity's public key comes from its .pub before its private key",
			files: []file{{name: "m", key: "a", pub: "b"}},
			agent: []string{"b"}, identitiesOnly: true,
			offered: []string{"b"},
		},
		{
			name:  "a certificate both on disk and in ssh-agent is offered once",
			files: []file{{name: "c", key: "c", cert: "c-cert"}},
			agent: []string{"c", "c-cert"}, identitiesOnly: true,
			offered: []string{"c", "c-cert"},
		},
		{
			name:  "a host certificate is ignored",
			files: []file{{name: "c", key: "c", cert: "hostcert"}},
			agent: []string{"c"}, identitiesOnly: true,
			offered: []string{"c"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ca, _, _ := newKey(t)
			keys, pubs := namedKeys(t, "a", "b", "c", "d")
			certs := map[string]*ssh.Certificate{}
			certFor := func(name string) *ssh.Certificate {
				if cert, ok := certs[name]; ok {
					return cert
				}
				cert := &ssh.Certificate{Key: pubs[strings.TrimSuffix(name, "-cert")], CertType: ssh.UserCert, ValidBefore: ssh.CertTimeInfinity}
				if name == "hostcert" {
					cert.Key, cert.CertType = pubs["c"], ssh.HostCert
				}
				if err := cert.SignCert(rand.Reader, ca); err != nil {
					t.Fatal(err)
				}
				certs[name] = cert
				return cert
			}
			var added []agent.AddedKey
			for _, name := range c.agent {
				if strings.HasSuffix(name, "-cert") {
					added = append(added, agent.AddedKey{PrivateKey: keys[strings.TrimSuffix(name, "-cert")], Certificate: certFor(name)})
				} else {
					added = append(added, agent.AddedKey{PrivateKey: keys[name]})
				}
			}
			config := ""
			for _, f := range c.files {
				config += "  IdentityFile {home}/.ssh/" + f.name + "\n"
			}
			if c.identitiesOnly {
				config += "  IdentitiesOnly yes\n"
			}
			s := startServer(t)
			h := newHome(t, s.config(config))
			h.env["SSH_AUTH_SOCK"] = startAgentWith(t, added...)
			for _, f := range c.files {
				switch {
				case f.holdsCert != "":
					h.write(t, ".ssh/"+f.name, ssh.MarshalAuthorizedKey(certFor(f.holdsCert)))
				case f.key != "" && f.public:
					h.write(t, ".ssh/"+f.name, ssh.MarshalAuthorizedKey(pubs[f.key]))
				case f.key != "" && f.protected:
					h.write(t, ".ssh/"+f.name, privateKeyFile(t, keys[f.key], "secret"))
				case f.key != "":
					h.write(t, ".ssh/"+f.name, privateKeyFile(t, keys[f.key], ""))
				}
				if f.pub != "" {
					h.write(t, ".ssh/"+f.name+".pub", ssh.MarshalAuthorizedKey(pubs[f.pub]))
				}
				if f.cert != "" {
					h.write(t, ".ssh/"+f.name+"-cert.pub", ssh.MarshalAuthorizedKey(certFor(f.cert)))
				}
			}

			if _, err := sshclient.Dial(testContext(t), s.node(), h.options()); err == nil {
				t.Fatal("Dial succeeded with a server that accepts nothing")
			}
			names := map[string]string{}
			for name, pub := range pubs {
				names[ssh.FingerprintSHA256(pub)] = name
			}
			for name, cert := range certs {
				names[ssh.FingerprintSHA256(cert)] = name
			}
			var got []string
			for _, k := range s.offeredKeys() {
				got = append(got, names[ssh.FingerprintSHA256(k)])
			}
			if !slices.Equal(got, c.offered) {
				t.Errorf("offered %v, want %v", got, c.offered)
			}
		})
	}
}

// TestConnectTimeoutDoesntCoverAuthentication checks that ConnectTimeout
// bounds connecting, as in OpenSSH, and not ssh-agent's signing, which can
// wait for its user to approve.
func TestConnectTimeoutDoesntCoverAuthentication(t *testing.T) {
	signer, _, priv := newKey(t)
	s := startServer(t, signer.PublicKey())
	h := newHome(t, s.config("  ConnectTimeout 1\n"))
	h.env["SSH_AUTH_SOCK"] = startSlowAgent(t, 2*time.Second, priv)
	c, err := sshclient.Dial(testContext(t), s.node(), h.options())
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
}

// TestConnectTimeoutBoundsTheBanner checks that ConnectTimeout gives up on
// a host that accepts the connection and never says anything.
func TestConnectTimeoutBoundsTheBanner(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	holdConnections(t, l)
	h := newHome(t, "Host node\n  HostName 127.0.0.1\n  Port "+strconv.Itoa(l.Addr().(*net.TCPAddr).Port)+"\n  User root\n  ConnectTimeout 1\n")
	start := time.Now()
	if _, err := sshclient.Dial(testContext(t), sshclient.Node{Target: "node"}, h.options()); err == nil {
		t.Fatal("Dial succeeded against a host that never answers")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("Dial took %v to give up", elapsed)
	}
}

// TestShortRSAKeysAreLeftOut checks that RSA keys under 1024 bits are
// left out, as OpenSSH's RequiredRSASize leaves them out, from ssh-agent
// and from identity files.
func TestShortRSAKeysAreLeftOut(t *testing.T) {
	// Go refuses keys this short unless told otherwise.
	t.Setenv("GODEBUG", "rsa1024min=0")
	short, err := rsa.GenerateKey(rand.Reader, 768)
	if err != nil {
		t.Fatal(err)
	}
	keys, pubs := namedKeys(t, "a")
	s := startServer(t)
	h := newHome(t, s.config("  IdentityFile {home}/.ssh/short\n"))
	block, err := ssh.MarshalPrivateKey(short, "")
	if err != nil {
		t.Fatal(err)
	}
	h.write(t, ".ssh/short", pem.EncodeToMemory(block))
	h.env["SSH_AUTH_SOCK"] = startAgentWith(t, agent.AddedKey{PrivateKey: short}, agent.AddedKey{PrivateKey: keys["a"]})
	if _, err := sshclient.Dial(testContext(t), s.node(), h.options()); err == nil {
		t.Fatal("Dial succeeded with a server that accepts nothing")
	}
	var got []string
	for _, k := range s.offeredKeys() {
		got = append(got, ssh.FingerprintSHA256(k))
	}
	if want := []string{ssh.FingerprintSHA256(pubs["a"])}; !slices.Equal(got, want) {
		t.Errorf("offered %v, want only a, %v", got, want)
	}
}

// TestAgentTimeoutCountsOnlyWhileARequestWaits checks that ssh-agent's
// timeout bounds each request, not the time between requests, which here
// the server spends checking the key ssh-agent listed before it is asked
// to sign.
func TestAgentTimeoutCountsOnlyWhileARequestWaits(t *testing.T) {
	sshclient.SetAgentTimeout(t, 300*time.Millisecond)
	signer, _, priv := newKey(t)
	s := startServer(t, signer.PublicKey())
	s.authDelay.Store(int64(time.Second))
	h := newHome(t, s.config(""))
	h.env["SSH_AUTH_SOCK"] = startAgent(t, priv)
	c, err := sshclient.Dial(testContext(t), s.node(), h.options())
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
}
