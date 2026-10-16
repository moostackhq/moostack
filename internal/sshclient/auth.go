package sshclient

import (
	"context"
	"crypto/rsa"
	"errors"
	"net"
	"os"
	"slices"
	"strings"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"

	"github.com/moostackhq/moostack/internal/sshconfig"
)

// auth is how to authenticate as the operator. Private keys are only ever
// read into memory.
type auth struct {
	// settings are the settings to authenticate with.
	settings sshconfig.Settings
	// sshAgent is the connection to ssh-agent, if there is one.
	sshAgent net.Conn
	// stop stops closing sshAgent when the context ends.
	stop func() bool
}

// newAuth connects to the ssh-agent the settings name, if any. One that
// can't be reached is skipped, as OpenSSH skips it. The connection closes
// when ctx ends, and each request to ssh-agent has agentTimeout to be
// answered, so that an agent that stops answering can't block.
func newAuth(ctx context.Context, s sshconfig.Settings, opts Options) *auth {
	a := &auth{settings: s}
	if sock := agentSocket(s.IdentityAgent, opts); sock != "" {
		var d net.Dialer
		if conn, err := d.DialContext(ctx, "unix", sock); err == nil {
			a.sshAgent = &agentConn{Conn: conn}
			a.stop = context.AfterFunc(ctx, func() { conn.Close() })
		}
	}
	return a
}

// agentSocket is the ssh-agent socket that IdentityAgent names, or empty
// for none.
func agentSocket(identityAgent string, opts Options) string {
	switch {
	case identityAgent == "", identityAgent == "SSH_AUTH_SOCK":
		return opts.getenv("SSH_AUTH_SOCK")
	case identityAgent == "none":
		return ""
	case strings.HasPrefix(identityAgent, "$"):
		return opts.getenv(identityAgent[1:])
	}
	return identityAgent
}

// agentTimeout is how long ssh-agent has to answer a request. It is long,
// as ssh-agent may wait for its user to approve a signature.
var agentTimeout = 2 * time.Minute

// agentConn is a connection to ssh-agent that gives each request
// agentTimeout to be answered. x/crypto keeps a read waiting between
// requests, so the deadline is cleared once an answer arrives, and only a
// request waiting for its answer can time out.
type agentConn struct {
	net.Conn
}

func (c *agentConn) Write(p []byte) (int, error) {
	c.Conn.SetDeadline(time.Now().Add(agentTimeout))
	return c.Conn.Write(p)
}

func (c *agentConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 {
		c.Conn.SetDeadline(time.Time{})
	}
	return n, err
}

// close closes the connection to ssh-agent.
func (a *auth) close() {
	if a.sshAgent != nil {
		a.stop()
		a.sshAgent.Close()
	}
}

// methods are the auth methods for the settings, offering the keys
// signers gives.
func (a *auth) methods() []ssh.AuthMethod {
	return []ssh.AuthMethod{ssh.PublicKeysCallback(func() ([]ssh.Signer, error) {
		var held []ssh.Signer
		if a.sshAgent != nil {
			held, _ = agent.NewClient(a.sshAgent).Signers()
		}
		return a.signers(held), nil
	})}
}

// signers are the keys to offer, given the keys ssh-agent holds. They
// follow OpenSSH's pubkey_prepare and sign_and_send_pubkey: first, in
// ssh-agent's order, the identities ssh-agent holds; then ssh-agent's
// other keys, unless IdentitiesOnly is set; last, in order, the other
// identities. An identity nothing can sign for isn't offered. x/crypto
// can't sign with FIDO keys, so those work only through ssh-agent.
func (a *auth) signers(held []ssh.Signer) []ssh.Signer {
	unheld := a.identities()
	var ordered, others []*identity
	for _, h := range held {
		if tooShort(h.PublicKey()) {
			continue
		}
		i := slices.IndexFunc(unheld, func(id *identity) bool { return sameKey(id.pub, h.PublicKey()) })
		switch {
		case i >= 0:
			unheld[i].held = h
			ordered = append(ordered, unheld[i])
			unheld = slices.Delete(unheld, i, i+1)
		case !a.settings.IdentitiesOnly:
			others = append(others, &identity{pub: h.PublicKey(), held: h})
		}
	}
	ordered = append(append(ordered, others...), unheld...)
	var signers []ssh.Signer
	for _, id := range ordered {
		if signer := id.signer(ordered); signer != nil {
			signers = append(signers, signer)
		}
	}
	return signers
}

// identity is a key or user certificate to offer.
type identity struct {
	// pub is the key or certificate.
	pub ssh.PublicKey
	// file is the identity file whose private key signs for it, if any.
	file string
	// held is ssh-agent's signer for pub, if ssh-agent holds it.
	held ssh.Signer
}

// cert is the identity's certificate, or nil if it is a key.
func (id *identity) cert() *ssh.Certificate {
	cert, _ := id.pub.(*ssh.Certificate)
	return cert
}

// key is the identity's key: itself, or the key its certificate
// certifies.
func (id *identity) key() ssh.PublicKey {
	if cert := id.cert(); cert != nil {
		return cert.Key
	}
	return id.pub
}

// signer signs for the identity, or is nil if nothing can. As in OpenSSH,
// a certificate is signed by the first identity in ordered with its key,
// else as the identity itself is.
func (id *identity) signer(ordered []*identity) ssh.Signer {
	if cert := id.cert(); cert != nil {
		i := slices.IndexFunc(ordered, func(other *identity) bool { return other.cert() == nil && sameKey(other.pub, cert.Key) })
		if i >= 0 {
			if key := ordered[i].ownSigner(); key != nil {
				return certSigner(cert, key)
			}
		}
	}
	return id.ownSigner()
}

// ownSigner signs for the identity with ssh-agent, if it holds the
// identity, else with the private key in its file, if that is the
// identity's and no passphrase protects it.
func (id *identity) ownSigner() ssh.Signer {
	if id.held != nil {
		return id.held
	}
	private, _ := loadPrivateKey(id.file)
	if private == nil || !sameKey(private.PublicKey(), id.key()) {
		return nil
	}
	if cert := id.cert(); cert != nil {
		return certSigner(cert, private)
	}
	return private
}

// certSigner signs for cert with key, or is nil if key isn't cert's.
func certSigner(cert *ssh.Certificate, key ssh.Signer) ssh.Signer {
	signer, err := ssh.NewCertSigner(cert, key)
	if err != nil {
		return nil
	}
	return signer
}

// identities are what the identity files name, as OpenSSH's
// load_public_identity_files lists them: for each file in order, its key,
// then the certificate in its -cert.pub. Host certificates and short RSA
// keys are left out, and IdentityFile none names nothing.
func (a *auth) identities() []*identity {
	var ids []*identity
	add := func(pub ssh.PublicKey, file string) {
		if pub == nil || tooShort(pub) {
			return
		}
		id := &identity{pub: pub, file: file}
		if cert := id.cert(); cert != nil && cert.CertType != ssh.UserCert {
			return
		}
		ids = append(ids, id)
	}
	for _, f := range a.settings.IdentityFiles {
		if strings.EqualFold(f, "none") {
			continue
		}
		add(loadPublicKey(f), f)
		if cert, ok := loadPublicKey(f + "-cert").(*ssh.Certificate); ok {
			add(cert, f)
		}
	}
	return ids
}

// loadPublicKey loads the public key for the identity file f as OpenSSH's
// sshkey_load_public does: from f, if f is a public key, else from f.pub,
// else from the private key in f. x/crypto, unlike OpenSSH, also finds
// the public key in an unprotected PEM private key.
func loadPublicKey(f string) ssh.PublicKey {
	if pub := readPublicKey(f); pub != nil {
		return pub
	}
	if pub := readPublicKey(f + ".pub"); pub != nil {
		return pub
	}
	private, pub := loadPrivateKey(f)
	if private != nil {
		return private.PublicKey()
	}
	return pub
}

// loadPrivateKey loads the private key in f, or nil if there is none. A
// private key that a passphrase protects gives nil, and the public key,
// if the file holds it.
func loadPrivateKey(f string) (ssh.Signer, ssh.PublicKey) {
	if f == "" {
		return nil, nil
	}
	data, err := os.ReadFile(f)
	if err != nil {
		return nil, nil
	}
	signer, err := ssh.ParsePrivateKey(data)
	if err == nil {
		return signer, nil
	}
	var missing *ssh.PassphraseMissingError
	if errors.As(err, &missing) {
		return nil, missing.PublicKey
	}
	return nil, nil
}

// minRSABits is the shortest RSA key offered, OpenSSH's default
// RequiredRSASize. Go doesn't sign with shorter ones either.
const minRSABits = 1024

// tooShort reports whether key, or the key a certificate certifies, is an
// RSA key shorter than minRSABits.
func tooShort(key ssh.PublicKey) bool {
	// ssh-agent's keys are raw blobs, so parse the key again.
	parsed, err := ssh.ParsePublicKey(key.Marshal())
	if err != nil {
		return false
	}
	if cert, ok := parsed.(*ssh.Certificate); ok {
		parsed = cert.Key
	}
	crypto, ok := parsed.(ssh.CryptoPublicKey)
	if !ok {
		return false
	}
	rsaKey, ok := crypto.CryptoPublicKey().(*rsa.PublicKey)
	return ok && rsaKey.N.BitLen() < minRSABits
}

// readPublicKey reads the public key in file, or nil if there isn't one.
func readPublicKey(file string) ssh.PublicKey {
	data, err := os.ReadFile(file)
	if err != nil {
		return nil
	}
	pub, _, _, _, err := ssh.ParseAuthorizedKey(data)
	if err != nil {
		return nil
	}
	return pub
}
