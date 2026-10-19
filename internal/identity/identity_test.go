package identity_test

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/moostackhq/moostack/internal/identity"
)

func TestIssueRefusesBadRequests(t *testing.T) {
	ca := newCA(t)
	request, _ := newKey(t)
	block, _ := pem.Decode(request)
	badSignature := bytes.Clone(block.Bytes)
	// An ed25519 signature is the request's last 64 bytes.
	badSignature[len(badSignature)-1] ^= 1
	for name, c := range map[string]struct {
		request []byte
		node    string
		want    string
	}{
		"no node name":                 {request, "", "no node name"},
		"no PEM":                       {[]byte("not PEM\n"), "node1", "no PEM certificate request"},
		"another PEM type":             {ca.CertPEM(), "node1", "no PEM certificate request"},
		"two requests":                 {append(bytes.Clone(request), request...), "node1", "more than one PEM block"},
		"a request that doesn't parse": {requestPEM([]byte("not DER")), "node1", "asn1: structure error"},
		"a bad signature":              {requestPEM(badSignature), "node1", "verification failure"},
		"a key that isn't ed25519":     {requestPEM(ecdsaRequest(t)), "node1", "not an ed25519 key"},
	} {
		t.Run(name, func(t *testing.T) {
			cert, err := ca.Issue(c.request, c.node)
			if err == nil {
				t.Fatalf("Issue signed it:\n%s", cert)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("Issue refused it with %q, want an error saying %q", err, c.want)
			}
		})
	}
}

func TestIssueIssuesTheNodeAnIdentityForItsKey(t *testing.T) {
	ca := newCA(t)
	request, key := newKey(t)
	certPEM, err := ca.Issue(request, "node1")
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode(certPEM)
	if block == nil || block.Type != "CERTIFICATE" {
		t.Fatalf("Issue returned no PEM certificate:\n%s", certPEM)
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(ca.CertPEM())
	// Verify accepts any one of the usages it is given, so each is
	// verified on its own: a node serves its peers and calls them.
	for _, usage := range []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth} {
		_, err = cert.Verify(x509.VerifyOptions{Roots: roots, DNSName: "node1", KeyUsages: []x509.ExtKeyUsage{usage}})
		if err != nil {
			t.Errorf("the certificate doesn't verify as node1's against the CA, for usage %d: %v", usage, err)
		}
	}
	if cert.Subject.CommonName != "node1" {
		t.Errorf("the certificate names %q, want node1", cert.Subject.CommonName)
	}
	if !key.Public().(ed25519.PublicKey).Equal(cert.PublicKey) {
		t.Error("the certificate isn't for the node's key")
	}
}

// newCA is a new cluster CA.
func newCA(t *testing.T) *identity.CA {
	t.Helper()
	ca, err := identity.NewCA("prod")
	if err != nil {
		t.Fatal(err)
	}
	return ca
}

// newKey makes a node's key with NewKey, and returns its certificate
// request and the key.
func newKey(t *testing.T) ([]byte, ed25519.PrivateKey) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "node.key")
	request, err := identity.NewKey(path)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode(data)
	if block == nil {
		t.Fatalf("the key isn't PEM:\n%s", data)
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	return request, key.(ed25519.PrivateKey)
}

// ecdsaRequest is the DER of a certificate request for a new ECDSA key.
func ecdsaRequest(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, key)
	if err != nil {
		t.Fatal(err)
	}
	return der
}

// requestPEM is der as a PEM certificate request.
func requestPEM(der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: der})
}
