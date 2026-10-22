package controlplane_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	controlplanev1 "github.com/moostackhq/moostack/api/controlplane/v1"
	"github.com/moostackhq/moostack/internal/controlplane"
	"github.com/moostackhq/moostack/internal/identity"
)

// fakeControlPlane is a control plane on the test's machine, at
// localhost, with a certificate from its CA that it presents with the
// CA's.
type fakeControlPlane struct {
	ca      *identity.CA
	address string
	// cert is the control plane's certificate, with the CA's after it.
	cert tls.Certificate
	l    net.Listener
}

// newCA is a CA, failing t if it can't be made.
func newCA(t *testing.T, name string) *identity.CA {
	t.Helper()
	ca, err := identity.NewCA(name)
	if err != nil {
		t.Fatal(err)
	}
	return ca
}

// serverCert is a certificate that ca issues for host, with ca's own
// after it unless alone.
func serverCert(t *testing.T, ca *identity.CA, host string, alone bool) tls.Certificate {
	t.Helper()
	keyFile := filepath.Join(t.TempDir(), "server.key")
	request, err := identity.NewKey(keyFile)
	if err != nil {
		t.Fatal(err)
	}
	certPEM, err := ca.Issue(request, host)
	if err != nil {
		t.Fatal(err)
	}
	keyPEM, err := os.ReadFile(keyFile)
	if err != nil {
		t.Fatal(err)
	}
	if !alone {
		certPEM = append(certPEM, ca.CertPEM()...)
	}
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	return cert
}

// newControlPlane listens at localhost, with a certificate for localhost
// from a CA of its own, presented with the CA's.
func newControlPlane(t *testing.T) *fakeControlPlane {
	t.Helper()
	ca := newCA(t, "cp")
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	_, port, _ := net.SplitHostPort(l.Addr().String())
	return &fakeControlPlane{ca: ca, address: "localhost:" + port, cert: serverCert(t, ca, "localhost", false), l: l}
}

// token is a join token of the control plane, with the fingerprint of
// its CA.
func (cp *fakeControlPlane) token(t *testing.T, secret string) *controlplanev1.JoinToken {
	t.Helper()
	fp, err := identity.Fingerprint(cp.ca.CertPEM())
	if err != nil {
		t.Fatal(err)
	}
	return &controlplanev1.JoinToken{Address: cp.address, CaFingerprint: fp, Secret: secret}
}

// serveJoin has the control plane serve Join with join, under the
// protocol names protos.
func (cp *fakeControlPlane) serveJoin(t *testing.T, join func(*controlplanev1.JoinRequest) (*controlplanev1.JoinResponse, error), protos ...string) {
	t.Helper()
	s := grpc.NewServer()
	controlplanev1.RegisterControlPlaneServer(s, joinServer{join: join})
	go s.Serve(tls.NewListener(cp.l, &tls.Config{Certificates: []tls.Certificate{cp.cert}, NextProtos: protos}))
	t.Cleanup(s.Stop)
}

type joinServer struct {
	controlplanev1.UnimplementedControlPlaneServer
	join func(*controlplanev1.JoinRequest) (*controlplanev1.JoinResponse, error)
}

func (s joinServer) Join(_ context.Context, req *controlplanev1.JoinRequest) (*controlplanev1.JoinResponse, error) {
	return s.join(req)
}

// issuing answers a join by issuing the node node-7 of cluster qa its
// identity from ca, and records the request in got.
func issuing(t *testing.T, ca *identity.CA, got **controlplanev1.JoinRequest) func(*controlplanev1.JoinRequest) (*controlplanev1.JoinResponse, error) {
	return func(req *controlplanev1.JoinRequest) (*controlplanev1.JoinResponse, error) {
		if got != nil {
			*got = req
		}
		cert, err := ca.Issue(req.CertificateRequest, "node-7")
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return &controlplanev1.JoinResponse{Certificate: cert, Ca: ca.CertPEM(), Cluster: "test"}, nil
	}
}

func join(t *testing.T, tok *controlplanev1.JoinToken) (*controlplane.Joined, string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	key := filepath.Join(t.TempDir(), "identity", "node.key")
	joined, err := controlplane.Join(ctx, tok, key)
	return joined, key, err
}

func TestJoinGetsTheNodesIdentityFromTheControlPlane(t *testing.T) {
	cp := newControlPlane(t)
	var req *controlplanev1.JoinRequest
	cp.serveJoin(t, issuing(t, cp.ca, &req), controlplanev1.ProtocolJoin)
	joined, key, err := join(t, cp.token(t, "s3cret"))
	if err != nil {
		t.Fatal(err)
	}
	if req == nil || req.Secret != "s3cret" {
		t.Fatalf("the control plane got the request %v, want the token's secret", req)
	}
	if joined.Node != "node-7" || joined.Cluster != "test" {
		t.Errorf("joined as node %q of cluster %q, want node-7 of test", joined.Node, joined.Cluster)
	}
	if string(joined.CA) != string(cp.ca.CertPEM()) {
		t.Error("the joined node's CA isn't the control plane's")
	}
	// The certificate is the identity of the key Join made, from the CA.
	dir := t.TempDir()
	files := identity.Files{Key: key, Cert: filepath.Join(dir, "node.crt"), CA: filepath.Join(dir, "ca.crt")}
	os.WriteFile(files.Cert, joined.Certificate, 0o644)
	os.WriteFile(files.CA, joined.CA, 0o644)
	id, err := identity.Load(files)
	if err != nil {
		t.Fatalf("the joined identity doesn't load: %v", err)
	}
	if id.Node != "node-7" {
		t.Errorf("the identity names %s, want node-7", id.Node)
	}
	// A peer authenticates the node by it, as TLS checks a peer's name.
	if err := handshake(id, node(t, cp.ca), "node-7"); err != nil {
		t.Errorf("a peer calling node-7 doesn't authenticate the joined identity: %v", err)
	}
	if info, err := os.Stat(key); err != nil || info.Mode().Perm() != 0o600 {
		t.Errorf("the node's key: %v, %v, want it readable by its owner alone", info, err)
	}
}

func TestJoinRefusesAControlPlaneItCantTrust(t *testing.T) {
	other := newCA(t, "other")
	otherCert := func(t *testing.T, cp *fakeControlPlane) {
		// A certificate for localhost from another CA, which the
		// control plane presents with that CA's.
		cp.cert = serverCert(t, other, "localhost", false)
	}
	for _, c := range []struct {
		name string
		edit func(t *testing.T, cp *fakeControlPlane)
		// answer answers the join, issuing by default.
		answer func(t *testing.T, cp *fakeControlPlane) func(*controlplanev1.JoinRequest) (*controlplanev1.JoinResponse, error)
		protos []string
		want   string
	}{
		{name: "another CA", edit: otherCert, want: "CA"},
		{name: "no CA presented", edit: func(t *testing.T, cp *fakeControlPlane) { cp.cert = serverCert(t, cp.ca, "localhost", true) }, want: "CA"},
		{name: "another CA's certificate with the token's CA after it", edit: func(t *testing.T, cp *fakeControlPlane) {
			// The token's CA is public, so an impostor can present it
			// too: only the certificate it issued is trusted.
			otherCert(t, cp)
			block, _ := pem.Decode(cp.ca.CertPEM())
			cp.cert.Certificate = append(cp.cert.Certificate, block.Bytes)
		}, want: "isn't the join token's CA's"},
		{name: "a certificate for another host", edit: func(t *testing.T, cp *fakeControlPlane) { cp.cert = serverCert(t, cp.ca, "cp.example.com", false) }, want: "localhost"},
		{name: "no protocol name", protos: []string{}, want: controlplanev1.ProtocolJoin},
		{name: "an answer with another CA", answer: func(t *testing.T, cp *fakeControlPlane) func(*controlplanev1.JoinRequest) (*controlplanev1.JoinResponse, error) {
			issue := issuing(t, other, nil)
			return issue
		}, want: "CA"},
		{name: "a certificate for another key", answer: func(t *testing.T, cp *fakeControlPlane) func(*controlplanev1.JoinRequest) (*controlplanev1.JoinResponse, error) {
			return func(*controlplanev1.JoinRequest) (*controlplanev1.JoinResponse, error) {
				request, err := identity.NewKey(filepath.Join(t.TempDir(), "other.key"))
				if err != nil {
					return nil, err
				}
				cert, err := cp.ca.Issue(request, "node-7")
				return &controlplanev1.JoinResponse{Certificate: cert, Ca: cp.ca.CertPEM(), Cluster: "test"}, err
			}
		}, want: "key"},
		{name: "a refused secret", answer: func(t *testing.T, cp *fakeControlPlane) func(*controlplanev1.JoinRequest) (*controlplanev1.JoinResponse, error) {
			return func(*controlplanev1.JoinRequest) (*controlplanev1.JoinResponse, error) {
				return nil, status.Error(codes.PermissionDenied, "the secret was used already")
			}
		}, want: "the secret was used already"},
		{name: "no cluster", answer: func(t *testing.T, cp *fakeControlPlane) func(*controlplanev1.JoinRequest) (*controlplanev1.JoinResponse, error) {
			issue := issuing(t, cp.ca, nil)
			return func(req *controlplanev1.JoinRequest) (*controlplanev1.JoinResponse, error) {
				res, err := issue(req)
				if res != nil {
					res.Cluster = ""
				}
				return res, err
			}
		}, want: "cluster"},
	} {
		t.Run(c.name, func(t *testing.T) {
			cp := newControlPlane(t)
			tok := cp.token(t, "s3cret")
			if c.edit != nil {
				c.edit(t, cp)
			}
			// The control plane records the request when it answers by
			// default, as it does in each case of a control plane the node
			// must not trust.
			var req *controlplanev1.JoinRequest
			answer := issuing(t, cp.ca, &req)
			if c.answer != nil {
				answer = c.answer(t, cp)
			}
			protos := []string{controlplanev1.ProtocolJoin}
			if c.protos != nil {
				protos = c.protos
			}
			cp.serveJoin(t, answer, protos...)
			joined, _, err := join(t, tok)
			if err == nil {
				t.Fatalf("joined %+v", joined)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("Join: %v, want an error about %s", err, c.want)
			}
			if req != nil {
				t.Errorf("a control plane the node didn't trust got its request, with the secret %q", req.Secret)
			}
		})
	}
}

// node is a node's identity from ca, called node-7.
func node(t *testing.T, ca *identity.CA) *identity.Identity {
	t.Helper()
	dir := t.TempDir()
	files := identity.Files{Key: filepath.Join(dir, "node.key"), Cert: filepath.Join(dir, "node.crt"), CA: filepath.Join(dir, "ca.crt")}
	request, err := identity.NewKey(files.Key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := ca.Issue(request, "node-7")
	if err != nil {
		t.Fatal(err)
	}
	os.WriteFile(files.Cert, cert, 0o644)
	os.WriteFile(files.CA, ca.CertPEM(), 0o644)
	id, err := identity.Load(files)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// tunnel is a connection the control plane accepted from a node's
// tunnel, with the node it proved and a client of what the node serves.
type tunnel struct {
	node   string
	conn   *silenceableConn
	health healthpb.HealthClient
}

// accept accepts the next tunnel, under protocol, or without one if it
// is empty, within timeout.
func (cp *fakeControlPlane) accept(t *testing.T, protocol string, timeout time.Duration) (*tunnel, error) {
	t.Helper()
	cas := x509.NewCertPool()
	cas.AppendCertsFromPEM(cp.ca.CertPEM())
	type accepted struct {
		tun *tunnel
		err error
	}
	done := make(chan accepted, 1)
	go func() {
		raw, err := cp.l.Accept()
		if err != nil {
			done <- accepted{err: err}
			return
		}
		cfg := &tls.Config{Certificates: []tls.Certificate{cp.cert}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: cas}
		if protocol != "" {
			cfg.NextProtos = []string{protocol}
		}
		conn := tls.Server(raw, cfg)
		if err := conn.Handshake(); err != nil {
			raw.Close()
			done <- accepted{err: err}
			return
		}
		state := conn.ConnectionState()
		sc := &silenceableConn{Conn: conn, closed: make(chan struct{})}
		t.Cleanup(func() { sc.Close() })
		client, err := grpc.NewClient("passthrough:///node", grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return sc, nil }))
		if err != nil {
			done <- accepted{err: err}
			return
		}
		t.Cleanup(func() { client.Close() })
		done <- accepted{tun: &tunnel{node: state.PeerCertificates[0].Subject.CommonName, conn: sc, health: healthpb.NewHealthClient(client)}}
	}()
	select {
	case a := <-done:
		return a.tun, a.err
	case <-time.After(timeout):
		return nil, errors.New("no tunnel")
	}
}

// check calls the node's health service over the tunnel.
func (tun *tunnel) check(t *testing.T) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, err := tun.health.Check(ctx, &healthpb.HealthCheckRequest{})
	return err
}

// silenceableConn is a connection that, once silenced, reads nothing
// more and drops what is written, as a connection to a control plane
// gone behind a partition.
type silenceableConn struct {
	net.Conn
	silent    atomic.Bool
	closeOnce sync.Once
	closed    chan struct{}
}

func (c *silenceableConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	if c.silent.Load() {
		<-c.closed
		return 0, net.ErrClosed
	}
	return n, err
}

func (c *silenceableConn) Write(b []byte) (int, error) {
	if c.silent.Load() {
		return len(b), nil
	}
	return c.Conn.Write(b)
}

func (c *silenceableConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

// serveTunnel serves a health service to the control plane at cp, over
// a tunnel of protocol as the node id, until the test ends, and returns
// the tunnel's listener and what it logs.
func serveTunnel(t *testing.T, cp *fakeControlPlane, id *identity.Identity, protocol string) (*controlplane.Listener, *logs) {
	t.Helper()
	logs := &logs{}
	l := controlplane.Listen(controlplane.Tunnel{Address: cp.address, Identity: id, Protocol: protocol, Log: logs.add})
	s := grpc.NewServer(controlplane.ServerOptions()...)
	healthpb.RegisterHealthServer(s, health.NewServer())
	go s.Serve(l)
	t.Cleanup(s.Stop)
	return l, logs
}

type logs struct {
	mu    sync.Mutex
	lines []string
}

func (l *logs) add(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, strings.TrimSpace(fmt.Sprintf(format, args...)))
}

func (l *logs) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Join(l.lines, "\n")
}

func TestATunnelServesTheControlPlaneAndComesBackOnceDropped(t *testing.T) {
	cp := newControlPlane(t)
	serveTunnel(t, cp, node(t, cp.ca), controlplanev1.ProtocolAgent)
	tun, err := cp.accept(t, controlplanev1.ProtocolAgent, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if tun.node != "node-7" {
		t.Errorf("the tunnel proved %s, want node-7", tun.node)
	}
	if err := tun.check(t); err != nil {
		t.Fatalf("calling the node over the tunnel: %v", err)
	}
	// The control plane drops the connection, and the node dials again.
	tun.conn.Close()
	tun, err = cp.accept(t, controlplanev1.ProtocolAgent, 10*time.Second)
	if err != nil {
		t.Fatalf("after the control plane dropped the tunnel: %v", err)
	}
	if err := tun.check(t); err != nil {
		t.Fatalf("calling the node over the tunnel dialled again: %v", err)
	}
}

func TestATunnelGoneSilentIsDialledAgain(t *testing.T) {
	controlplane.SetKeepalive(t, time.Second, time.Second)
	cp := newControlPlane(t)
	_, logs := serveTunnel(t, cp, node(t, cp.ca), controlplanev1.ProtocolUpdater)
	tun, err := cp.accept(t, controlplanev1.ProtocolUpdater, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := tun.check(t); err != nil {
		t.Fatal(err)
	}
	tun.conn.silent.Store(true)
	// The node's keepalive finds the connection silent within about 2 s.
	if _, err := cp.accept(t, controlplanev1.ProtocolUpdater, 15*time.Second); err != nil {
		t.Fatalf("after the tunnel went silent: %v", err)
	}
	if !strings.Contains(logs.String(), "stops answering its pings or breaks the protocol") {
		t.Errorf("the node logged\n%s\nwant why it closed the silent connection", logs)
	}
}

func TestATunnelRefusesAControlPlaneItCantTrust(t *testing.T) {
	for _, c := range []struct {
		name string
		cert func(t *testing.T, cp *fakeControlPlane) tls.Certificate
	}{
		{"another CA", func(t *testing.T, cp *fakeControlPlane) tls.Certificate {
			return serverCert(t, newCA(t, "other"), "localhost", false)
		}},
		{"a certificate for another host", func(t *testing.T, cp *fakeControlPlane) tls.Certificate {
			return serverCert(t, cp.ca, "cp.example.com", false)
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			cp := newControlPlane(t)
			id := node(t, cp.ca)
			cp.cert = c.cert(t, cp)
			_, logs := serveTunnel(t, cp, id, controlplanev1.ProtocolAgent)
			// The control plane's handshake fails, as the node refuses its
			// certificate.
			if tun, err := cp.accept(t, controlplanev1.ProtocolAgent, 10*time.Second); err == nil {
				if err := tun.check(t); err == nil {
					t.Fatal("the node served the control plane")
				}
			}
			deadline := time.Now().Add(5 * time.Second)
			for !strings.Contains(logs.String(), "certificate") && time.Now().Before(deadline) {
				time.Sleep(100 * time.Millisecond)
			}
			if !strings.Contains(logs.String(), "certificate") {
				t.Errorf("the node logged\n%s\nwant why it refused the control plane's certificate", logs)
			}
		})
	}
}

func TestATunnelRefusesAControlPlaneThatDoesntTakeItsProtocol(t *testing.T) {
	cp := newControlPlane(t)
	_, logs := serveTunnel(t, cp, node(t, cp.ca), controlplanev1.ProtocolAgent)
	if tun, err := cp.accept(t, "", 10*time.Second); err == nil {
		if err := tun.check(t); err == nil {
			t.Fatal("the node served a control plane that took no protocol")
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(logs.String(), controlplanev1.ProtocolAgent) && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	if !strings.Contains(logs.String(), controlplanev1.ProtocolAgent) {
		t.Errorf("the node logged\n%s\nwant why it refused the control plane", logs)
	}
}

func TestADisconnectedTunnelIsntDialledAgain(t *testing.T) {
	cp := newControlPlane(t)
	l, _ := serveTunnel(t, cp, node(t, cp.ca), controlplanev1.ProtocolAgent)
	tun, err := cp.accept(t, controlplanev1.ProtocolAgent, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	l.Disconnect()
	if err := tun.check(t); err == nil {
		t.Error("the node still served the tunnel it disconnected")
	}
	if _, err := cp.accept(t, controlplanev1.ProtocolAgent, 3*time.Second); err == nil {
		t.Error("the node dialled the control plane again after it disconnected")
	}
}

// handshake has client call server, by the name name, with the node
// identities' mutual TLS, and says how the handshake failed, if it did.
func handshake(server, client *identity.Identity, name string) error {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	errs := make(chan error, 1)
	go func() { errs <- tls.Server(a, server.ServerTLS(nil)).Handshake() }()
	err := tls.Client(b, client.ClientTLS(name, nil)).Handshake()
	if err != nil {
		a.Close()
		<-errs
		return err
	}
	return <-errs
}
