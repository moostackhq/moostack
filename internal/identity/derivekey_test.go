package identity_test

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/moostackhq/moostack/internal/identity"
)

// Each identity is loaded twice from its files, as a node's agent loads
// it once a run: both loads derive the same key for a label, which no
// other identity or label derives and which isn't the private key.
func TestDeriveKeyIsTheSameForAKeyAndLabelAndDiffersOtherwise(t *testing.T) {
	ca := newCA(t)
	seen := map[[32]byte]string{}
	for i := range 20 {
		files := issueFiles(t, ca, "node1")
		first, second := load(t, files), load(t, files)
		for _, label := range []string{"a label", "another label"} {
			k := first.DeriveKey(label)
			if k != second.DeriveKey(label) {
				t.Fatalf("identity %d: two loads derive different keys for %q", i, label)
			}
			if k == ([32]byte{}) {
				t.Fatalf("identity %d: the key for %q is zero", i, label)
			}
			if bytes.Contains(readKey(t, files.Key), k[:16]) {
				t.Fatalf("identity %d: the key for %q is part of the private key", i, label)
			}
			at := fmt.Sprintf("identity %d, label %q", i, label)
			if was, ok := seen[k]; ok {
				t.Fatalf("%s and %s derive the same key", was, at)
			}
			seen[k] = at
		}
	}
}

// issueFiles writes an identity ca issues the node called name, and
// returns its files.
func issueFiles(t *testing.T, ca *identity.CA, name string) identity.Files {
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
	return files
}

// load is the identity in files.
func load(t *testing.T, files identity.Files) *identity.Identity {
	t.Helper()
	id, err := identity.Load(files)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// readKey is the ed25519 private key at path, seed and public key.
func readKey(t *testing.T, path string) ed25519.PrivateKey {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode(data)
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	return key.(ed25519.PrivateKey)
}

// The key the private key whose seed is the bytes 0 to 31 derives for "a
// label": HKDF-SHA256 of the seed, with no salt and the label as info.
func TestDeriveKeyIsHKDFOfTheSeed(t *testing.T) {
	ca := newCA(t)
	key := ed25519.NewKeyFromSeed([]byte{
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
		16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
	})
	der, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := ca.Issue(requestPEM(der), "node1")
	if err != nil {
		t.Fatal(err)
	}
	pkcs8, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	id, err := identity.Parse(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: pkcs8}), cert, ca.CertPEM())
	if err != nil {
		t.Fatal(err)
	}
	k := id.DeriveKey("a label")
	if got, want := hex.EncodeToString(k[:]), "7ec3d0c6448676ba9bb7955497a22613579552e3d90028d6733a41ad38001e4f"; got != want {
		t.Errorf("the key derived for %q is %s, want %s", "a label", got, want)
	}
}
