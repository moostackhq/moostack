package controlplane_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	controlplanev1 "github.com/moostackhq/moostack/api/controlplane/v1"
	"github.com/moostackhq/moostack/internal/controlplane"
	"github.com/moostackhq/moostack/internal/identity"
)

// An answer whose certificate peers can't authenticate the node by is
// refused: one for client authentication alone, though the contract
// says both, one naming the node in its common name alone, which TLS
// doesn't check a peer's name against, and one for another name.
func TestJoinRefusesACertificatePeersCantAuthenticate(t *testing.T) {
	for _, c := range []struct {
		name string
		edit func(*x509.Certificate)
	}{
		{"for a client alone", func(c *x509.Certificate) { c.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth} }},
		{"naming the node in its common name alone", func(c *x509.Certificate) { c.DNSNames = nil }},
		{"for another name", func(c *x509.Certificate) { c.DNSNames = []string{"node-8"} }},
	} {
		t.Run(c.name, func(t *testing.T) {
			cp := newControlPlane(t)
			cp.serveJoin(t, func(req *controlplanev1.JoinRequest) (*controlplanev1.JoinResponse, error) {
				return issuedAs(t, cp, req, c.edit), nil
			}, controlplanev1.ProtocolJoin)
			if joined, _, err := join(t, cp.token(t, "s")); err == nil {
				t.Errorf("Join took a certificate %s, as node %s", c.name, joined.Node)
			}
		})
	}
}

// issuedAs is an answer to req with a certificate from cp's CA for the
// node node-7, of both usages and naming it in its DNS names, as edit
// changes it.
func issuedAs(t *testing.T, cp *fakeControlPlane, req *controlplanev1.JoinRequest, edit func(*x509.Certificate)) *controlplanev1.JoinResponse {
	t.Helper()
	caCert, err := x509.ParseCertificate(mustDER(t, cp.ca.CertPEM()))
	if err != nil {
		t.Fatal(err)
	}
	caKey, err := x509.ParsePKCS8PrivateKey(mustDER(t, cp.ca.KeyPEM()))
	if err != nil {
		t.Fatal(err)
	}
	csr, err := x509.ParseCertificateRequest(mustDER(t, req.CertificateRequest))
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(7), Subject: pkix.Name{CommonName: "node-7"}, DNSNames: []string{"node-7"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	edit(template)
	der, err := x509.CreateCertificate(rand.Reader, template, caCert, csr.PublicKey, caKey.(ed25519.PrivateKey))
	if err != nil {
		t.Fatal(err)
	}
	return &controlplanev1.JoinResponse{Certificate: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), Ca: cp.ca.CertPEM(), Cluster: "test"}
}

func mustDER(t *testing.T, p []byte) []byte {
	b, _ := pem.Decode(p)
	if b == nil {
		t.Fatal("no PEM")
	}
	return b.Bytes
}

// A control plane that takes the request and never answers: Join ends
// at its context's deadline.
func TestJoinEndsAtItsDeadlineWhenTheControlPlaneNeverAnswers(t *testing.T) {
	cp := newControlPlane(t)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	cp.serveJoin(t, func(*controlplanev1.JoinRequest) (*controlplanev1.JoinResponse, error) {
		<-release
		return nil, status.Error(codes.Canceled, "x")
	}, controlplanev1.ProtocolJoin)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	start := time.Now()
	_, err := controlplane.Join(ctx, cp.token(t, "s"), filepath.Join(t.TempDir(), "id", "node.key"))
	if err == nil || time.Since(start) > 4*time.Second {
		t.Errorf("Join: %v after %s", err, time.Since(start))
	}
	// The control plane may have issued the identity.
	if err == nil || !strings.Contains(err.Error(), "spending the join token") {
		t.Errorf("Join cut off waiting for the answer: %v, want it to say the token may be spent", err)
	}
}

// What a Join says of the token: spent once the control plane answered
// with an identity the node refuses; that it may be spent once the
// request may have reached the control plane, whose answer the node
// didn't get or understand; and neither when the node never reached the
// control plane, or the control plane refused the request.
func TestJoinSaysWhetherTheTokenIsSpent(t *testing.T) {
	other := newCA(t, "other")
	answer := func(f func(cp *fakeControlPlane, req *controlplanev1.JoinRequest) (*controlplanev1.JoinResponse, error)) func(*testing.T, *fakeControlPlane) {
		return func(t *testing.T, cp *fakeControlPlane) {
			cp.serveJoin(t, func(req *controlplanev1.JoinRequest) (*controlplanev1.JoinResponse, error) { return f(cp, req) }, controlplanev1.ProtocolJoin)
		}
	}
	for _, c := range []struct {
		name  string
		serve func(*testing.T, *fakeControlPlane)
		// says is what Join says of the token: "spent", "may" or "".
		says string
	}{
		{"an answer with another CA", answer(func(_ *fakeControlPlane, req *controlplanev1.JoinRequest) (*controlplanev1.JoinResponse, error) {
			return issuing(t, other, nil)(req)
		}), "spent"},
		{"an identity for another key", answer(func(cp *fakeControlPlane, _ *controlplanev1.JoinRequest) (*controlplanev1.JoinResponse, error) {
			request, err := identity.NewKey(filepath.Join(t.TempDir(), "other.key"))
			if err != nil {
				return nil, err
			}
			cert, err := cp.ca.Issue(request, "node-7")
			return &controlplanev1.JoinResponse{Certificate: cert, Ca: cp.ca.CertPEM(), Cluster: "test"}, err
		}), "spent"},
		{"an answer naming no cluster", answer(func(cp *fakeControlPlane, req *controlplanev1.JoinRequest) (*controlplanev1.JoinResponse, error) {
			res, err := issuing(t, cp.ca, nil)(req)
			if res != nil {
				res.Cluster = ""
			}
			return res, err
		}), "spent"},
		{"a refused secret", answer(func(*fakeControlPlane, *controlplanev1.JoinRequest) (*controlplanev1.JoinResponse, error) {
			return nil, status.Error(codes.PermissionDenied, "the secret was used already")
		}), ""},
		{"an answer that isn't one", answer(func(*fakeControlPlane, *controlplanev1.JoinRequest) (*controlplanev1.JoinResponse, error) {
			return nil, status.Error(codes.Internal, "the control plane failed after issuing it")
		}), "may"},
		{"a connection dropped once the request came", func(t *testing.T, cp *fakeControlPlane) {
			go dropAfterRequest(cp, true)
		}, "may"},
		{"a connection dropped before HTTP/2 started", func(t *testing.T, cp *fakeControlPlane) {
			go dropAfterRequest(cp, false)
		}, ""},
		{"a request it can't read", refusing(codes.InvalidArgument), ""},
		{"a node it doesn't know", refusing(codes.Unauthenticated), ""},
		{"no Join served", refusing(codes.Unimplemented), ""},
		{"an exhausted resource", refusing(codes.ResourceExhausted), "may"},
		{"a control plane the node doesn't trust", func(t *testing.T, cp *fakeControlPlane) {
			cp.cert = serverCert(t, other, "localhost", false)
			answer(func(cp *fakeControlPlane, req *controlplanev1.JoinRequest) (*controlplanev1.JoinResponse, error) {
				return issuing(t, cp.ca, nil)(req)
			})(t, cp)
		}, ""},
		{"no control plane", func(t *testing.T, cp *fakeControlPlane) { cp.l.Close() }, ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			cp := newControlPlane(t)
			tok := cp.token(t, "s")
			c.serve(t, cp)
			_, _, err := join(t, tok)
			if err == nil {
				t.Fatal("joined")
			}
			says := ""
			switch {
			case strings.Contains(err.Error(), "spent the join token"):
				says = "spent"
			case strings.Contains(err.Error(), "may have issued the node its identity all the same, spending the join token"):
				says = "may"
			}
			if says != c.says {
				t.Errorf("Join: %v, want it to say of the token %q, not %q", err, c.says, says)
			}
		})
	}
}

// refusing serves Join refusing every request with code.
func refusing(code codes.Code) func(*testing.T, *fakeControlPlane) {
	return func(t *testing.T, cp *fakeControlPlane) {
		cp.serveJoin(t, func(*controlplanev1.JoinRequest) (*controlplanev1.JoinResponse, error) {
			return nil, status.Error(code, "refused")
		}, controlplanev1.ProtocolJoin)
	}
}

// dropAfterRequest takes the next connection to cp, and closes it once
// the request has come whole, starting HTTP/2 as a server, or, unless
// started, as soon as the client's preface has come, before any request.
func dropAfterRequest(cp *fakeControlPlane, started bool) {
	raw, err := cp.l.Accept()
	if err != nil {
		return
	}
	conn := tls.Server(raw, &tls.Config{Certificates: []tls.Certificate{cp.cert}, NextProtos: []string{controlplanev1.ProtocolJoin}})
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := io.ReadFull(conn, make([]byte, len(http2.ClientPreface))); err != nil || !started {
		return
	}
	fr := http2.NewFramer(conn, conn)
	fr.ReadMetaHeaders = hpack.NewDecoder(4096, nil)
	fr.WriteSettings()
	for {
		f, err := fr.ReadFrame()
		if err != nil {
			return
		}
		switch f := f.(type) {
		case *http2.SettingsFrame:
			if !f.IsAck() {
				fr.WriteSettingsAck()
			}
		case *http2.DataFrame:
			if f.StreamEnded() {
				return
			}
		}
	}
}

// A Join that fails before it makes the key leaves a key that is there.
func TestAJoinThatFailsEarlyLeavesTheKeyThere(t *testing.T) {
	key := filepath.Join(t.TempDir(), "node.key")
	if err := os.WriteFile(key, []byte("a key"), 0o600); err != nil {
		t.Fatal(err)
	}
	tok := &controlplanev1.JoinToken{Address: "no-port", CaFingerprint: "sha256:" + strings.Repeat("ab", 32), Secret: "s"}
	if _, err := controlplane.Join(t.Context(), tok, key); err == nil {
		t.Fatal("joined a control plane at no-port")
	}
	if data, err := os.ReadFile(key); err != nil || string(data) != "a key" {
		t.Errorf("the key after a Join that failed early: %q, %v", data, err)
	}
}

// A Join that fails removes the key it made, so that it can run again.
func TestAFailedJoinCanRunAgain(t *testing.T) {
	cp := newControlPlane(t)
	cp.serveJoin(t, func(*controlplanev1.JoinRequest) (*controlplanev1.JoinResponse, error) {
		return nil, status.Error(codes.Unavailable, "control plane restarting")
	}, controlplanev1.ProtocolJoin)
	key := filepath.Join(t.TempDir(), "id", "node.key")
	ctx := t.Context()
	if _, err := controlplane.Join(ctx, cp.token(t, "s"), key); err == nil || !strings.Contains(err.Error(), "control plane restarting") {
		t.Fatalf("the first Join: %v, want the control plane's refusal", err)
	}
	if _, err := os.Stat(key); err == nil {
		t.Error("the failed Join left its key")
	}
	if _, err := controlplane.Join(ctx, cp.token(t, "s"), key); err == nil || !strings.Contains(err.Error(), "control plane restarting") {
		t.Errorf("the second Join: %v, want it to reach the control plane again", err)
	}
	// A key that is there already is left alone.
	if err := os.WriteFile(key, []byte("another key"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := controlplane.Join(ctx, cp.token(t, "s"), key); err == nil {
		t.Error("Join replaced a key that was there")
	}
	if data, err := os.ReadFile(key); err != nil || string(data) != "another key" {
		t.Errorf("Join changed the key that was there: %q, %v", data, err)
	}
}

// identity.Parse's messages, as Join passes them on, read plainly.
func TestParseSaysWhatIsWrong(t *testing.T) {
	ca := newCA(t, "a")
	dir := t.TempDir()
	r1, _ := identity.NewKey(filepath.Join(dir, "k1"))
	identity.NewKey(filepath.Join(dir, "k2"))
	cert, _ := ca.Issue(r1, "n")
	k2, _ := os.ReadFile(filepath.Join(dir, "k2"))
	if _, err := identity.Parse(k2, cert, ca.CertPEM()); err == nil || err.Error() != "the node's identity: the certificate isn't for the key" {
		t.Errorf("a certificate for another key: %v", err)
	}
	r2, _ := identity.NewKey(filepath.Join(dir, "k3"))
	cert2, _ := newCA(t, "c").Issue(r2, "n")
	k3, _ := os.ReadFile(filepath.Join(dir, "k3"))
	if _, err := identity.Parse(k3, cert2, ca.CertPEM()); err == nil || !strings.HasPrefix(err.Error(), "the node's identity: the certificate wasn't issued by the cluster CA, the CA certificate, ") {
		t.Errorf("a certificate from another CA: %v", err)
	}
}
