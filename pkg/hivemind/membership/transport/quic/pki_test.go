package quic_test

// The tests' cluster CA and node certificates: the node's name as common
// name and only DNS name, and TLS configurations that refuse removed
// identities by fingerprint.

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"math/big"
	"sync"
	"testing"
	"time"

	. "github.com/moostackhq/moostack/pkg/hivemind/membership/transport/quic"
)

// cluster is a cluster CA and the identities it removed.
type cluster struct {
	t    testing.TB
	ca   *x509.Certificate
	key  ed25519.PrivateKey
	Pool *x509.CertPool

	mu      sync.Mutex
	removed map[[32]byte]bool
}

// newCluster makes a cluster CA.
func newCluster(t testing.TB) *cluster {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "cluster CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, key)
	if err != nil {
		t.Fatal(err)
	}
	ca, _ := x509.ParseCertificate(der)
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	return &cluster{t: t, ca: ca, key: key, Pool: pool, removed: map[[32]byte]bool{}}
}

// ident is one node's certificate.
type ident struct {
	Name string
	Cert tls.Certificate
	c    *cluster
}

// Node issues name a certificate valid for an hour.
func (c *cluster) Node(name string) ident {
	return c.Issue(name, time.Now().Add(time.Hour))
}

// Issue issues name a certificate valid until notAfter.
func (c *cluster) Issue(name string, notAfter time.Time) ident {
	c.t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		c.t.Fatal(err)
	}
	serial, _ := rand.Int(rand.Reader, big.NewInt(1<<62))
	tmpl := &x509.Certificate{
		SerialNumber: serial, Subject: pkix.Name{CommonName: name}, DNSNames: []string{name},
		NotBefore: time.Now().Add(-2 * time.Hour), NotAfter: notAfter,
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, c.ca, pub, c.key)
	if err != nil {
		c.t.Fatal(err)
	}
	return ident{Name: name, Cert: tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, c: c}
}

// Remove refuses id from now on.
func (c *cluster) Remove(id ident) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.removed[sha256.Sum256(id.Cert.Certificate[0])] = true
}

// IsRemoved says whether cert is a removed identity's.
func (c *cluster) IsRemoved(cert *x509.Certificate) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.removed[sha256.Sum256(cert.Raw)]
}

func (c *cluster) refuseRemoved(cs tls.ConnectionState) error {
	if len(cs.PeerCertificates) > 0 && c.IsRemoved(cs.PeerCertificates[0]) {
		return errors.New("removed identity")
	}
	return nil
}

// Config is the transport configuration of the node id, at addr.
func (id ident) Config(addr string) Config {
	c := id.c
	return Config{
		Addr: addr,
		Server: &tls.Config{
			MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{id.Cert},
			ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: c.Pool,
			VerifyConnection: c.refuseRemoved,
		},
		Client: func(peer string) *tls.Config {
			return &tls.Config{
				MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{id.Cert},
				RootCAs: c.Pool, ServerName: peer,
				VerifyConnection: c.refuseRemoved,
			}
		},
		PeerID:   func(cert *x509.Certificate) (string, error) { return cert.Subject.CommonName, nil },
		ResetKey: sha256.Sum256([]byte("reset " + id.Name)),
	}
}
