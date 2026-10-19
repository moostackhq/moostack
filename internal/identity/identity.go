// Package identity is the cluster CA and the node identities it issues:
// an ed25519 key and an X.509 certificate for each node, with which the
// node's agent and updater prove who they are.
//
// Certificates never expire. A node's identity is revoked by removing
// the node, not by letting it run out.
package identity

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// notAfter is the end of every certificate's validity: RFC 5280's date
// for a certificate with no well-defined expiration.
var notAfter = time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)

// CA is a cluster CA: its certificate and its key.
type CA struct {
	cert    *x509.Certificate
	certPEM []byte
	key     ed25519.PrivateKey
}

// NewCA creates the CA of the cluster called cluster.
func NewCA(cluster string) (*CA, error) {
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	serial, err := newSerial()
	if err != nil {
		return nil, err
	}
	template := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "Moostack cluster CA " + cluster},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLenZero:        true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, pub, key)
	if err != nil {
		return nil, err
	}
	return ParseCA(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), mustKeyPEM(key))
}

// ParseCA reads a CA from its certificate and its key, both PEM. It
// refuses a certificate that isn't a CA's, or a key that isn't its key.
func ParseCA(certPEM, keyPEM []byte) (*CA, error) {
	cert, err := parseCert(certPEM)
	if err != nil {
		return nil, fmt.Errorf("the CA certificate: %w", err)
	}
	if !cert.IsCA || cert.KeyUsage&x509.KeyUsageCertSign == 0 {
		return nil, errors.New("the CA certificate isn't a CA's")
	}
	key, err := parseKey(keyPEM)
	if err != nil {
		return nil, fmt.Errorf("the CA key: %w", err)
	}
	if !key.Public().(ed25519.PublicKey).Equal(cert.PublicKey) {
		return nil, errors.New("the CA key isn't the CA certificate's key")
	}
	return &CA{cert: cert, certPEM: certPEM, key: key}, nil
}

// CertPEM is the CA's certificate, PEM.
func (ca *CA) CertPEM() []byte { return ca.certPEM }

// KeyPEM is the CA's key, PEM.
func (ca *CA) KeyPEM() []byte { return mustKeyPEM(ca.key) }

// Issue issues the node called node its identity: a certificate for the
// key of requestPEM, a PEM certificate request made by NewKey, signed by
// ca. It returns the certificate, PEM.
func (ca *CA) Issue(requestPEM []byte, node string) ([]byte, error) {
	if node == "" {
		return nil, errors.New("no node name")
	}
	der, err := decodeOne(requestPEM, "CERTIFICATE REQUEST")
	if err != nil {
		return nil, fmt.Errorf("the node's certificate request: %w", err)
	}
	req, err := x509.ParseCertificateRequest(der)
	if err != nil {
		return nil, fmt.Errorf("the node's certificate request: %w", err)
	}
	if err := req.CheckSignature(); err != nil {
		return nil, fmt.Errorf("the node's certificate request: %w", err)
	}
	pub, ok := req.PublicKey.(ed25519.PublicKey)
	if !ok {
		return nil, fmt.Errorf("the node's certificate request is for a %T, not an ed25519 key", req.PublicKey)
	}
	serial, err := newSerial()
	if err != nil {
		return nil, err
	}
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: node},
		DNSNames:     []string{node},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err = x509.CreateCertificate(rand.Reader, template, ca.cert, pub, ca.key)
	if err != nil {
		return nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), nil
}

// NewKey creates a node's key at path, readable by its owner alone, and
// returns a certificate request for it, PEM, for the CA to Issue. It
// refuses to replace a key that is there. The key never leaves the node:
// only the request does.
func NewKey(path string) ([]byte, error) {
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, err
	}
	_, err = f.Write(mustKeyPEM(key))
	if err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		os.Remove(path)
		return nil, err
	}
	der, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, key)
	if err != nil {
		return nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: der}), nil
}

// Files are where a node keeps its identity.
type Files struct {
	// Key is the node's key, which NewKey creates.
	Key string
	// Cert is the node's certificate, which the CA issued.
	Cert string
	// CA is the cluster CA's certificate.
	CA string
}

// Identity is a node's identity, loaded to prove who it is to its peers
// and to check theirs.
type Identity struct {
	// Node is the node's name.
	Node string
	cert tls.Certificate
	cas  *x509.CertPool
}

// Load loads the identity in files. It refuses a certificate that isn't
// for the key, or that the CA didn't issue.
func Load(files Files) (*Identity, error) {
	read := func(path string) ([]byte, error) {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("the node's identity: %w", err)
		}
		return data, nil
	}
	keyPEM, err := read(files.Key)
	if err != nil {
		return nil, err
	}
	certPEM, err := read(files.Cert)
	if err != nil {
		return nil, err
	}
	caPEM, err := read(files.CA)
	if err != nil {
		return nil, err
	}
	key, err := parseKey(keyPEM)
	if err != nil {
		return nil, fmt.Errorf("the node's identity: %s: %w", files.Key, err)
	}
	cert, err := parseCert(certPEM)
	if err != nil {
		return nil, fmt.Errorf("the node's identity: %s: %w", files.Cert, err)
	}
	ca, err := parseCert(caPEM)
	if err != nil {
		return nil, fmt.Errorf("the node's identity: %s: %w", files.CA, err)
	}
	if !key.Public().(ed25519.PublicKey).Equal(cert.PublicKey) {
		return nil, fmt.Errorf("the node's identity: %s isn't the certificate of the key in %s", files.Cert, files.Key)
	}
	cas := x509.NewCertPool()
	cas.AddCert(ca)
	_, err = cert.Verify(x509.VerifyOptions{
		Roots:     cas,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	})
	if err != nil {
		return nil, fmt.Errorf("the node's identity: %s wasn't issued by the cluster CA in %s: %w", files.Cert, files.CA, err)
	}
	if cert.Subject.CommonName == "" {
		return nil, fmt.Errorf("the node's identity: %s names no node", files.Cert)
	}
	return &Identity{
		Node: cert.Subject.CommonName,
		cert: tls.Certificate{Certificate: [][]byte{cert.Raw}, PrivateKey: key, Leaf: cert},
		cas:  cas,
	}, nil
}

// ServerTLS is the TLS config to serve peers with: it proves this node's
// identity, and accepts only a peer with an identity from the cluster CA.
func (id *Identity) ServerTLS() *tls.Config {
	return &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{id.cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    id.cas,
	}
}

// ClientTLS is the TLS config to call the node called peer with: it
// proves this node's identity, and accepts only peer's identity from the
// cluster CA.
func (id *Identity) ClientTLS(peer string) *tls.Config {
	return &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{id.cert},
		RootCAs:      id.cas,
		ServerName:   peer,
	}
}

// decodeOne is the DER of data, which must be one PEM block of type
// blockType.
func decodeOne(data []byte, blockType string) ([]byte, error) {
	block, rest := pem.Decode(data)
	if block == nil || block.Type != blockType {
		return nil, fmt.Errorf("no PEM %s", strings.ToLower(blockType))
	}
	if len(bytes.TrimSpace(rest)) > 0 {
		return nil, errors.New("more than one PEM block")
	}
	return block.Bytes, nil
}

// parseCert reads one PEM certificate.
func parseCert(data []byte) (*x509.Certificate, error) {
	der, err := decodeOne(data, "CERTIFICATE")
	if err != nil {
		return nil, err
	}
	return x509.ParseCertificate(der)
}

// parseKey reads one PEM ed25519 private key, PKCS #8.
func parseKey(data []byte) (ed25519.PrivateKey, error) {
	der, err := decodeOne(data, "PRIVATE KEY")
	if err != nil {
		return nil, err
	}
	key, err := x509.ParsePKCS8PrivateKey(der)
	if err != nil {
		return nil, err
	}
	ed, ok := key.(ed25519.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("a %T, not an ed25519 key", key)
	}
	return ed, nil
}

// mustKeyPEM is key as PEM, PKCS #8, which can't fail for an ed25519 key.
func mustKeyPEM(key ed25519.PrivateKey) []byte {
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		panic(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
}

// newSerial is a random 128-bit serial number.
func newSerial() (*big.Int, error) {
	return rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
}
