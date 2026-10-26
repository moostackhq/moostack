//go:build linux

package nodetest

import (
	"archive/tar"
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"io"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

// RegistryHost is the name the nodes reach a Registry by.
const RegistryHost = "registry.test"

// TrustStore is the file of a node's system trust store, which Go's TLS
// reads the certificates it trusts from on Linux.
const TrustStore = "/etc/ssl/certs/ca-certificates.crt"

// Registry is a registry of images, as a public one serves them, in a
// container of its own, which the nodes it was started for reach as
// RegistryHost, with TLS, trusting it through their system trust
// store, as they trust a public registry. It is nodetest's registry
// program.
type Registry struct {
	// Name is the container's.
	Name string
	// dir holds what it serves: the blobs, and each repository's tags and
	// fault.
	dir string
}

// registryProgram is the registry's binary in binaries', in a directory
// of its own, so that Start doesn't install it on the nodes.
const registryProgram = "tools/registry"

// StartRegistry starts a registry, and has each of nodes reach it as
// RegistryHost and trust the CA of its certificate through the node's
// system trust store. Start it before the nodes' agents first pull.
func StartRegistry(t *testing.T, nodes ...*Node) *Registry {
	t.Helper()
	r := &Registry{dir: t.TempDir()}
	certs := t.TempDir()
	ca := writeCertificates(t, certs)
	if err := os.MkdirAll(filepath.Join(r.dir, "blobs", "sha256"), 0o755); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command("docker", "run", "-d", "--rm",
		"-v", filepath.Join(binaries(t), filepath.Dir(registryProgram))+":/mnt/bin:ro",
		"-v", r.dir+":/srv:ro",
		"-v", certs+":/certs:ro",
		"debian:13-slim", "/mnt/bin/"+filepath.Base(registryProgram), "-dir", "/srv", "-cert", "/certs/cert.pem", "-key", "/certs/key.pem").Output()
	if err != nil {
		t.Fatalf("starting the registry: %v\n%s", err, stderr(err))
	}
	r.Name = strings.TrimSpace(string(out))
	t.Cleanup(func() {
		if t.Failed() {
			// Read without failing, so that the container goes either way.
			out, _ := exec.Command("docker", "logs", r.Name).CombinedOutput()
			t.Logf("the registry's requests:\n%s", out)
		}
		exec.Command("docker", "rm", "-f", r.Name).Run()
	})
	out, err = exec.Command("docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", r.Name).Output()
	if err != nil {
		t.Fatalf("finding the registry's address: %v\n%s", err, stderr(err))
	}
	address := strings.TrimSpace(string(out))
	for _, n := range nodes {
		n.Exec(t, "root", "sh", "-c", "set -e\n"+
			"echo '"+address+" "+RegistryHost+"' >>/etc/hosts\n"+
			"mkdir -p "+filepath.Dir(TrustStore)+"\n"+
			"cat >>"+TrustStore+" <<'EOF'\n"+string(ca)+"EOF\n")
	}
	Eventually(t, 10*time.Second, func() error {
		_, err := exec.Command("docker", "exec", r.Name, "true").Output()
		return err
	})
	return r
}

// writeCertificates writes a certificate for RegistryHost, cert.pem, and
// its key, key.pem, to dir, issued by a CA of their own, and returns the
// CA's certificate, PEM.
func writeCertificates(t *testing.T, dir string) []byte {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	caTemplate := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "nodetest registry CA"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(24 * time.Hour), IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.CreateCertificate(rand.Reader, &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: RegistryHost},
		DNSNames: []string{RegistryHost}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(24 * time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}, caCert, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(dir, "cert.pem"), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	writeFile(t, filepath.Join(dir, "key.pem"), pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))
	// The container reads them as another user than the test's.
	for _, f := range []string{"cert.pem", "key.pem"} {
		if err := os.Chmod(filepath.Join(dir, f), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
}

// Push has the registry serve img as repo's tag, and returns its
// reference, as a node pulls it, with the digest of its manifest.
func (r *Registry) Push(t *testing.T, repo, tag string, img Image) (ref, digest string) {
	t.Helper()
	m := r.pushBlobs(t, img)
	r.tag(t, repo, tag, m.MediaType, m.Digest.String())
	return RegistryHost + "/" + repo + ":" + tag, m.Digest.String()
}

// PushIndex has the registry serve an index of imgs, each for its
// architecture, as repo's tag, and returns its reference.
func (r *Registry) PushIndex(t *testing.T, repo, tag string, imgs ...Image) string {
	t.Helper()
	index := ocispec.Index{MediaType: ocispec.MediaTypeImageIndex}
	index.SchemaVersion = 2
	for _, img := range imgs {
		m := r.pushBlobs(t, img)
		m.Annotations = nil
		m.Platform = &ocispec.Platform{OS: "linux", Architecture: img.architecture()}
		index.Manifests = append(index.Manifests, m)
	}
	data, err := json.Marshal(index)
	if err != nil {
		t.Fatal(err)
	}
	r.tag(t, repo, tag, ocispec.MediaTypeImageIndex, r.writeBlob(t, data))
	return RegistryHost + "/" + repo + ":" + tag
}

// Fault has the registry answer every request for repo as fault says:
// "404", "401" or "429", as the registry program says. It returns the
// reference of a tag of repo.
func (r *Registry) Fault(t *testing.T, repo, fault string) string {
	t.Helper()
	r.write(t, filepath.Join(repo, "fault"), []byte(fault+"\n"))
	return RegistryHost + "/" + repo + ":1"
}

// Requests are the requests the registry answered so far, each as
// "<method> <path> <status>".
func (r *Registry) Requests(t *testing.T) []string {
	t.Helper()
	out, err := exec.Command("docker", "logs", r.Name).Output()
	if err != nil {
		t.Fatalf("reading the registry's requests: %v\n%s", err, stderr(err))
	}
	return strings.Split(strings.TrimSpace(string(out)), "\n")
}

// pushBlobs writes the blobs of img's archive, and returns the descriptor
// of its manifest.
func (r *Registry) pushBlobs(t *testing.T, img Image) ocispec.Descriptor {
	t.Helper()
	tr := tar.NewReader(bytes.NewReader(img.Archive(t)))
	var index ocispec.Index
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(tr)
		if err != nil {
			t.Fatal(err)
		}
		switch {
		case h.Name == "index.json":
			if err := json.Unmarshal(data, &index); err != nil {
				t.Fatal(err)
			}
		case strings.HasPrefix(h.Name, "blobs/sha256/"):
			r.writeBlob(t, data)
		}
	}
	if len(index.Manifests) != 1 {
		t.Fatalf("the image %s has %d manifests, not one", img.Name, len(index.Manifests))
	}
	return index.Manifests[0]
}

// writeBlob writes data as a blob, and returns its digest.
func (r *Registry) writeBlob(t *testing.T, data []byte) string {
	t.Helper()
	sum := sha256.Sum256(data)
	r.write(t, filepath.Join("blobs", "sha256", hex.EncodeToString(sum[:])), data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// tag has repo's tag name the manifest or index of mediaType at digest.
func (r *Registry) tag(t *testing.T, repo, tag, mediaType, digest string) {
	t.Helper()
	r.write(t, filepath.Join(repo, "tags", tag), []byte(mediaType+" "+digest+"\n"))
}

// write writes data to the file at path in what the registry serves,
// making its directories, each readable by the container's user, as the
// test's temporary directory isn't.
func (r *Registry) write(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(filepath.Join(r.dir, path)), 0o755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(r.dir, path), data)
	if out, err := exec.Command("chmod", "-R", "a+rX", r.dir).CombinedOutput(); err != nil {
		t.Fatalf("making the registry's files readable: %v\n%s", err, out)
	}
}
