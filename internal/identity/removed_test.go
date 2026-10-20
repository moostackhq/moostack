package identity_test

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/moostackhq/moostack/internal/identity"
)

func TestTheServerRefusesAPeerWhoseIdentityIsRemoved(t *testing.T) {
	ca := newCA(t)
	server, removed, kept := issue(t, ca, "node1"), issue(t, ca, "node2"), issue(t, ca, "node3")
	isRemoved := func(fp string) bool { return fp == removed.Fingerprint() }

	serverErr, _ := handshake(t, server.ServerTLS(isRemoved), removed.ClientTLS("node1", nil))
	if serverErr == nil || !strings.Contains(serverErr.Error(), "node2") || !strings.Contains(serverErr.Error(), "removed") {
		t.Errorf("the server accepted node2, whose identity is removed: %v", serverErr)
	}
	if serverErr, clientErr := handshake(t, server.ServerTLS(isRemoved), kept.ClientTLS("node1", nil)); serverErr != nil || clientErr != nil {
		t.Errorf("the server refused node3, whose identity isn't removed: %v, %v", serverErr, clientErr)
	}
}

func TestTheClientRefusesAPeerWhoseIdentityIsRemoved(t *testing.T) {
	ca := newCA(t)
	removed, client := issue(t, ca, "node1"), issue(t, ca, "node2")
	isRemoved := func(fp string) bool { return fp == removed.Fingerprint() }
	_, clientErr := handshake(t, removed.ServerTLS(nil), client.ClientTLS("node1", isRemoved))
	if clientErr == nil || !strings.Contains(clientErr.Error(), "node1") || !strings.Contains(clientErr.Error(), "removed") {
		t.Errorf("the client accepted node1, whose identity is removed: %v", clientErr)
	}
}

func TestAFingerprintIsTheCertificatesSHA256(t *testing.T) {
	ca := newCA(t)
	request, _ := newKey(t)
	certPEM, err := ca.Issue(request, "node1")
	if err != nil {
		t.Fatal(err)
	}
	fp, err := identity.Fingerprint(certPEM)
	if err != nil {
		t.Fatal(err)
	}
	// It is what openssl x509 -fingerprint -sha256 prints, lowercase
	// and without its colons: the SHA-256 of the certificate's DER.
	block, _ := pem.Decode(certPEM)
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if want := "sha256:" + hexSHA256(block.Bytes); fp != want {
		t.Errorf("the fingerprint is %s, want %s", fp, want)
	}
	if got := identity.CertFingerprint(cert); got != fp {
		t.Errorf("CertFingerprint is %s, Fingerprint %s", got, fp)
	}
	if _, err := identity.Fingerprint([]byte("not PEM")); err == nil {
		t.Error("Fingerprint took something that isn't a PEM certificate")
	}
}

// issue is the identity ca issues the node called name.
func issue(t *testing.T, ca *identity.CA, name string) *identity.Identity {
	t.Helper()
	dir := t.TempDir()
	files := identity.Files{Key: filepath.Join(dir, "node.key"), Cert: filepath.Join(dir, "node.crt"), CA: filepath.Join(dir, "ca.crt")}
	request, err := identity.NewKey(files.Key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := ca.Issue(request, name)
	if err != nil {
		t.Fatal(err)
	}
	for path, data := range map[string][]byte{files.Cert: cert, files.CA: ca.CertPEM()} {
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	id, err := identity.Load(files)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// handshake runs a TLS handshake between server and client, and has the
// client read once, which is when it learns that a TLS 1.3 server
// refused its certificate. It returns each side's error.
func handshake(t *testing.T, server, client *tls.Config) (serverErr, clientErr error) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	done := make(chan error, 1)
	go func() {
		s, err := l.Accept()
		if err != nil {
			done <- err
			return
		}
		conn := tls.Server(s, server)
		err = conn.Handshake()
		if err == nil {
			_, err = conn.Write([]byte("x"))
		}
		conn.Close()
		done <- err
	}()
	c, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	conn := tls.Client(c, client)
	clientErr = conn.Handshake()
	if clientErr == nil {
		_, clientErr = conn.Read(make([]byte, 1))
	}
	conn.Close()
	return <-done, clientErr
}

// hexSHA256 is the SHA-256 of data, hex.
func hexSHA256(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
