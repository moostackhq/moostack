//go:build linux

package nodetest

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	controlplanev1 "github.com/moostackhq/moostack/api/controlplane/v1"
	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/identity"
)

// TunnelHost is the name a node's container reaches the test's machine
// by, where a TunnelPeer listens.
const TunnelHost = "tunnel-peer.test"

// TunnelPeer is the far end of a joined node's tunnels, on the test's
// machine: it answers one join, issuing the node its identity from a CA
// of its own, accepts the agent's and the updater's tunnels, and calls
// the node API back over them. It is a transport helper, not a control
// plane.
type TunnelPeer struct {
	// CA issues the node's identity and the peer's certificate.
	CA *identity.CA
	// Address is where the node dials the peer.
	Address string
	secret  string
	cert    tls.Certificate
	cas     *x509.CertPool
	joins   *connListener
	l       net.Listener

	mu sync.Mutex
	// tunnels are the tunnel accepted last, by protocol name.
	tunnels map[string]*Tunnel
	// changed is closed, and replaced, when a tunnel is accepted.
	changed chan struct{}
	// joined says whether the secret has been used.
	joined bool
	// all are every tunnel accepted, which Stop closes.
	all []*Tunnel
}

// Tunnel is a tunnel the peer accepted.
type Tunnel struct {
	// Node is the node whose identity the tunnel proved.
	Node string
	// Conn calls what the node serves over the tunnel.
	Conn *grpc.ClientConn
	raw  net.Conn
}

// Close drops the tunnel, as a control plane gone away does.
func (tun *Tunnel) Close() { tun.raw.Close() }

// NewTunnelPeer starts a tunnel peer, and stops it when the test ends.
func NewTunnelPeer(t *testing.T) *TunnelPeer {
	t.Helper()
	ca, err := identity.NewCA("tunnel-peer")
	if err != nil {
		t.Fatal(err)
	}
	// The node's container reaches the peer through its network's
	// gateway, so the peer listens on every address.
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	p := &TunnelPeer{
		l:       l,
		CA:      ca,
		Address: net.JoinHostPort(TunnelHost, strconv.Itoa(l.Addr().(*net.TCPAddr).Port)),
		secret:  "the tunnel peer's secret",
		cas:     x509.NewCertPool(),
		joins:   &connListener{conns: make(chan net.Conn), done: make(chan struct{})},
		tunnels: map[string]*Tunnel{},
		changed: make(chan struct{}),
	}
	p.cas.AppendCertsFromPEM(ca.CertPEM())
	p.cert = peerCert(t, ca)
	joins := grpc.NewServer()
	controlplanev1.RegisterControlPlaneServer(joins, joinServer{p: p})
	go joins.Serve(p.joins)
	t.Cleanup(joins.Stop)
	t.Cleanup(p.Stop)
	go p.accept(l)
	return p
}

// Stop stops the peer, as a control plane gone for good: it closes its
// listener and every tunnel.
func (p *TunnelPeer) Stop() {
	p.l.Close()
	p.mu.Lock()
	all := p.all
	p.all = nil
	p.mu.Unlock()
	for _, tun := range all {
		tun.Conn.Close()
		tun.raw.Close()
	}
}

// peerCert is the peer's certificate for TunnelHost, from ca, with ca's
// after it.
func peerCert(t *testing.T, ca *identity.CA) tls.Certificate {
	t.Helper()
	keyFile := filepath.Join(t.TempDir(), "peer.key")
	request, err := identity.NewKey(keyFile)
	if err != nil {
		t.Fatal(err)
	}
	certPEM, err := ca.Issue(request, TunnelHost)
	if err != nil {
		t.Fatal(err)
	}
	keyPEM, err := os.ReadFile(keyFile)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := tls.X509KeyPair(append(certPEM, ca.CertPEM()...), keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	return cert
}

// Token is a join token of the peer.
func (p *TunnelPeer) Token(t *testing.T) string {
	t.Helper()
	fp, err := identity.Fingerprint(p.CA.CertPEM())
	if err != nil {
		t.Fatal(err)
	}
	token, err := controlplanev1.EncodeToken(&controlplanev1.JoinToken{Address: p.Address, CaFingerprint: fp, Secret: p.secret})
	if err != nil {
		t.Fatal(err)
	}
	return token
}

// accept handshakes each connection to l, and hands it on by its
// protocol name, until l closes.
func (p *TunnelPeer) accept(l net.Listener) {
	for {
		raw, err := l.Accept()
		if err != nil {
			p.joins.Close()
			return
		}
		go func() {
			conn := tls.Server(raw, &tls.Config{
				MinVersion:   tls.VersionTLS13,
				Certificates: []tls.Certificate{p.cert},
				NextProtos:   []string{controlplanev1.ProtocolJoin, controlplanev1.ProtocolAgent, controlplanev1.ProtocolUpdater},
				ClientAuth:   tls.VerifyClientCertIfGiven,
				ClientCAs:    p.cas,
			})
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := conn.HandshakeContext(ctx); err != nil {
				raw.Close()
				return
			}
			state := conn.ConnectionState()
			if state.NegotiatedProtocol == controlplanev1.ProtocolJoin {
				select {
				case p.joins.conns <- conn:
				case <-p.joins.done:
					conn.Close()
				}
				return
			}
			if len(state.PeerCertificates) == 0 {
				conn.Close()
				return
			}
			client, err := grpc.NewClient("passthrough:///node", grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return conn, nil }))
			if err != nil {
				conn.Close()
				return
			}
			// The peer starts HTTP/2 at once, as the protocol asks of a
			// control plane.
			client.Connect()
			tun := &Tunnel{Node: state.PeerCertificates[0].Subject.CommonName, Conn: client, raw: conn}
			p.mu.Lock()
			p.all = append(p.all, tun)
			p.tunnels[state.NegotiatedProtocol] = tun
			close(p.changed)
			p.changed = make(chan struct{})
			p.mu.Unlock()
		}()
	}
}

// Tunnel is the tunnel of protocol that the peer accepted last, or
// within timeout the first one. It fails the test if there is none.
func (p *TunnelPeer) Tunnel(t *testing.T, protocol string, timeout time.Duration) *Tunnel {
	t.Helper()
	tun, err := p.NextTunnel(protocol, nil, timeout)
	if err != nil {
		t.Fatal(err)
	}
	return tun
}

// NextTunnel waits up to timeout for a tunnel of protocol other than
// last, and returns it.
func (p *TunnelPeer) NextTunnel(protocol string, last *Tunnel, timeout time.Duration) (*Tunnel, error) {
	deadline := time.After(timeout)
	for {
		p.mu.Lock()
		tun, changed := p.tunnels[protocol], p.changed
		p.mu.Unlock()
		if tun != nil && tun != last {
			return tun, nil
		}
		select {
		case <-changed:
		case <-deadline:
			return nil, fmt.Errorf("no new tunnel of %s within %s", protocol, timeout)
		}
	}
}

// Agent and Updater are clients of the node API over the tunnels the
// peer accepted last from the agent and the updater, within a minute.
func (p *TunnelPeer) Agent(t *testing.T) nodev1.AgentClient {
	t.Helper()
	return nodev1.NewAgentClient(p.Tunnel(t, controlplanev1.ProtocolAgent, time.Minute).Conn)
}

func (p *TunnelPeer) Updater(t *testing.T) nodev1.UpdaterClient {
	t.Helper()
	return nodev1.NewUpdaterClient(p.Tunnel(t, controlplanev1.ProtocolUpdater, time.Minute).Conn)
}

type joinServer struct {
	controlplanev1.UnimplementedControlPlaneServer
	p *TunnelPeer
}

// JoinedNode is the name the peer issues the node that joins it, which
// isn't the name a node of nodetest has otherwise.
const JoinedNode = "joined-1"

// Join issues the node JoinedNode of the cluster test its identity, once.
func (s joinServer) Join(_ context.Context, req *controlplanev1.JoinRequest) (*controlplanev1.JoinResponse, error) {
	s.p.mu.Lock()
	defer s.p.mu.Unlock()
	if req.Secret != s.p.secret || s.p.joined {
		return nil, status.Error(codes.PermissionDenied, "the secret isn't the tunnel peer's, or was used already")
	}
	cert, err := s.p.CA.Issue(req.CertificateRequest, JoinedNode)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	s.p.joined = true
	return &controlplanev1.JoinResponse{Certificate: cert, Ca: s.p.CA.CertPEM(), Cluster: "test"}, nil
}

// connListener hands a server the connections sent on conns, until it
// is closed, closing done.
type connListener struct {
	conns chan net.Conn
	done  chan struct{}
	once  sync.Once
}

func (l *connListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-l.done:
		return nil, errors.New("the tunnel peer stopped")
	}
}

func (l *connListener) Close() error {
	l.once.Do(func() { close(l.done) })
	return nil
}

func (l *connListener) Addr() net.Addr { return &net.TCPAddr{} }
