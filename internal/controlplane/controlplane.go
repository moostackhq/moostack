// Package controlplane is a node's side of a control plane: joining one,
// and the tunnels over which the agent and the updater each serve their
// node API to it. The node always dials out, so that the control plane
// reaches nodes behind NAT.
package controlplane

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	controlplanev1 "github.com/moostackhq/moostack/api/controlplane/v1"
	"github.com/moostackhq/moostack/internal/identity"
)

// Joined is what a node got from the control plane it joined, as agent
// join prints it, in JSON.
type Joined struct {
	// Node is the node's name, which its certificate names, and Cluster
	// its cluster's.
	Node    string `json:"node"`
	Cluster string `json:"cluster"`
	// Certificate is the node's identity, and CA the CA's certificate,
	// both PEM.
	Certificate []byte `json:"certificate"`
	CA          []byte `json:"ca"`
}

// dialTimeout bounds dialling the control plane and its TLS handshake.
const dialTimeout = 10 * time.Second

// Join joins the control plane that tok names: it makes the node's key
// at keyPath, which it refuses to replace, and has the control plane
// issue the node its identity, with tok's secret. It trusts only a
// control plane that presents the CA tok names, with a certificate from
// it for tok's host, and takes only an identity from that CA for the
// key. If it fails, it removes the key it made, so that it can run
// again, and says whether the control plane may have spent the token.
func Join(ctx context.Context, tok *controlplanev1.JoinToken, keyPath string) (*Joined, error) {
	host, _, err := net.SplitHostPort(tok.Address)
	if err != nil {
		return nil, err
	}
	request, err := identity.NewKey(keyPath)
	if err != nil {
		return nil, fmt.Errorf("making the node's key: %w", err)
	}
	joined, err := join(ctx, tok, host, keyPath, request)
	if err != nil {
		os.Remove(keyPath)
	}
	return joined, err
}

// join has the control plane of tok, at host, issue the node its
// identity for the key at keyPath, whose certificate request is request.
func join(ctx context.Context, tok *controlplanev1.JoinToken, host, keyPath string, request []byte) (*Joined, error) {
	keyPEM, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, err
	}
	cfg := &tls.Config{
		MinVersion: tls.VersionTLS13,
		NextProtos: []string{controlplanev1.ProtocolJoin},
		// The CA is the one the token names, among those the control
		// plane presents, checked below.
		InsecureSkipVerify: true,
		VerifyConnection: func(cs tls.ConnectionState) error {
			if err := negotiated(cs, controlplanev1.ProtocolJoin); err != nil {
				return err
			}
			return verifyByFingerprint(cs, host, tok.CaFingerprint)
		},
	}
	// reached says whether the control plane has sent the node anything,
	// which gRPC waits for before it sends the request, so that the
	// request may have reached it.
	var reached atomic.Bool
	conn, err := grpc.NewClient("passthrough:///"+tok.Address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			c, err := dial(ctx, tok.Address, cfg)
			if err != nil {
				return nil, err
			}
			return readNotifier{Conn: c, read: &reached}, nil
		}))
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	res, err := controlplanev1.NewControlPlaneClient(conn).Join(ctx, &controlplanev1.JoinRequest{Secret: tok.Secret, CertificateRequest: request})
	switch code := status.Code(err); {
	case err == nil:
	case !reached.Load():
		return nil, fmt.Errorf("reaching the control plane at %s: %s", tok.Address, status.Convert(err).Message())
	case code == codes.PermissionDenied || code == codes.InvalidArgument || code == codes.Unauthenticated || code == codes.Unimplemented:
		// The control plane refused the request, or serves no Join.
		return nil, fmt.Errorf("the control plane at %s didn't issue the node its identity: %s", tok.Address, status.Convert(err).Message())
	default:
		// The request may have reached the control plane, which may have
		// issued the identity, its answer lost or not understood.
		return nil, fmt.Errorf("the control plane at %s didn't answer as the node expects: %s; it may have issued the node its identity all the same, spending the join token", tok.Address, status.Convert(err).Message())
	}
	// The control plane issued an identity, so the token is spent,
	// whatever the node makes of the answer.
	const spent = "; the control plane spent the join token on it, so ask it for a new one, and to forget the identity"
	if fp, err := identity.Fingerprint(res.Ca); err != nil || fp != tok.CaFingerprint {
		return nil, fmt.Errorf("the control plane at %s answered with a CA other than the join token's%s", tok.Address, spent)
	}
	id, err := identity.Parse(keyPEM, res.Certificate, res.Ca)
	if err != nil {
		return nil, fmt.Errorf("the control plane at %s answered with an identity that isn't the node key's from its CA: %w%s", tok.Address, err, spent)
	}
	if res.Cluster == "" {
		return nil, fmt.Errorf("the control plane at %s didn't name the node's cluster%s", tok.Address, spent)
	}
	return &Joined{Node: id.Node, Cluster: res.Cluster, Certificate: res.Certificate, CA: res.Ca}, nil
}

// readNotifier is a connection that sets read once anything is read
// from it.
type readNotifier struct {
	net.Conn
	read *atomic.Bool
}

func (c readNotifier) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	if n > 0 {
		c.read.Store(true)
	}
	return n, err
}

// verifyByFingerprint checks that the server of cs presented, after its
// certificate, the CA of fingerprint, which issued its certificate for
// host.
func verifyByFingerprint(cs tls.ConnectionState, host, fingerprint string) error {
	if len(cs.PeerCertificates) == 0 {
		return errors.New("the control plane presented no certificate")
	}
	roots := x509.NewCertPool()
	found := false
	for _, c := range cs.PeerCertificates[1:] {
		if identity.CertFingerprint(c) == fingerprint {
			roots.AddCert(c)
			found = true
		}
	}
	if !found {
		return fmt.Errorf("the control plane didn't present the join token's CA, %s", fingerprint)
	}
	_, err := cs.PeerCertificates[0].Verify(x509.VerifyOptions{Roots: roots, DNSName: host})
	if err != nil {
		return fmt.Errorf("the control plane's certificate isn't the join token's CA's for %s: %w", host, err)
	}
	return nil
}

// negotiated checks that the server of cs took protocol.
func negotiated(cs tls.ConnectionState, protocol string) error {
	if cs.NegotiatedProtocol != protocol {
		return fmt.Errorf("the control plane didn't take the protocol %s", protocol)
	}
	return nil
}

// dial is a TLS connection to address with cfg, its handshake done.
func dial(ctx context.Context, address string, cfg *tls.Config) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	d := tls.Dialer{Config: cfg}
	return d.DialContext(ctx, "tcp", address)
}

// Tunnel is where a node's program dials the control plane, and as
// what.
type Tunnel struct {
	// Address is the control plane's, as host:port.
	Address string
	// Identity is the node's, which the program proves, and whose
	// cluster CA must have issued the control plane's certificate for
	// Address's host.
	Identity *identity.Identity
	// Protocol is the program's protocol name: ProtocolAgent or
	// ProtocolUpdater.
	Protocol string
	// Log logs what happens to the connection.
	Log func(format string, args ...any)
}

// How a tunnel finds a connection gone silent: the program pings the
// control plane once the connection has been idle for keepaliveTime, and
// closes it if the answer doesn't come within keepaliveTimeout.
var (
	keepaliveTime    = 20 * time.Second
	keepaliveTimeout = 10 * time.Second
)

// ServerOptions are the options of a server that serves a tunnel's
// Listener: keepalives that find a connection gone silent, and that let
// the control plane ping as often as every 5 s, and a bound on the
// control plane starting HTTP/2.
func ServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.ConnectionTimeout(dialTimeout),
		grpc.KeepaliveParams(keepalive.ServerParameters{Time: keepaliveTime, Timeout: keepaliveTimeout}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 5 * time.Second, PermitWithoutStream: true}),
	}
}

// The delays between dialling the control plane again: the first, which
// doubles each time, and the longest.
const (
	firstRedial = 500 * time.Millisecond
	maxRedial   = 10 * time.Second
)

// Listener is a tunnel as a listener of a gRPC server: it dials the
// control plane, and accepts the connection, then dials again once it
// closes, with backoff, until it is closed or disconnected.
type Listener struct {
	t      Tunnel
	ctx    context.Context
	cancel context.CancelFunc
	// disconnected is closed by Disconnect.
	disconnected chan struct{}
	disconnect   sync.Once

	mu sync.Mutex
	// conn is the connection accepted last, nil before the first.
	conn *conn
	// delay is how long to wait before dialling again.
	delay time.Duration
}

// Listen is t's listener. Close it when done.
func Listen(t Tunnel) *Listener {
	ctx, cancel := context.WithCancel(context.Background())
	return &Listener{t: t, ctx: ctx, cancel: cancel, disconnected: make(chan struct{}), delay: firstRedial}
}

// Accept dials the control plane once the connection it accepted last
// has closed, and again after each failure, and returns the connection.
// Once l is disconnected, it waits for l to close.
func (l *Listener) Accept() (net.Conn, error) {
	l.mu.Lock()
	last := l.conn
	l.mu.Unlock()
	if last != nil {
		select {
		case <-last.closed:
		case <-l.ctx.Done():
			return nil, net.ErrClosed
		}
		select {
		case <-l.disconnected:
		default:
			l.t.Log("the connection to the control plane at %s closed: %v; dialling it again", l.t.Address, last.cause())
		}
		// A connection that lasted resets the backoff.
		if time.Since(last.opened) > maxRedial {
			l.delay = firstRedial
		}
		if !l.wait() {
			return nil, net.ErrClosed
		}
	}
	host, _, err := net.SplitHostPort(l.t.Address)
	if err != nil {
		return nil, err
	}
	for {
		select {
		case <-l.disconnected:
			<-l.ctx.Done()
			return nil, net.ErrClosed
		default:
		}
		cfg := l.t.Identity.ClientTLS(host, nil)
		cfg.NextProtos = []string{l.t.Protocol}
		cfg.VerifyConnection = func(cs tls.ConnectionState) error { return negotiated(cs, l.t.Protocol) }
		raw, err := dial(l.ctx, l.t.Address, cfg)
		if err == nil {
			c := &conn{Conn: raw, closed: make(chan struct{}), opened: time.Now()}
			l.mu.Lock()
			l.conn = c
			l.mu.Unlock()
			select {
			case <-l.disconnected:
				// Disconnected while dialling.
				c.Close()
				continue
			default:
			}
			l.t.Log("connected to the control plane at %s", l.t.Address)
			return c, nil
		}
		if l.ctx.Err() != nil {
			return nil, net.ErrClosed
		}
		l.t.Log("dialling the control plane at %s: %v; trying again in %s", l.t.Address, err, l.delay.Round(time.Millisecond))
		if !l.wait() {
			return nil, net.ErrClosed
		}
	}
}

// wait waits out the backoff, which then grows, and says whether l is
// still open.
func (l *Listener) wait() bool {
	d := l.delay
	l.delay = min(2*l.delay, maxRedial)
	// Jitter spreads out the nodes that a control plane restart dropped.
	d = d/2 + rand.N(d/2+1)
	select {
	case <-time.After(d):
		return true
	case <-l.disconnected:
		return true
	case <-l.ctx.Done():
		return false
	}
}

// Disconnect closes the connection to the control plane, if one is
// open, and dials it no more.
func (l *Listener) Disconnect() {
	l.disconnect.Do(func() { close(l.disconnected) })
	l.mu.Lock()
	c := l.conn
	l.mu.Unlock()
	if c != nil {
		c.Close()
	}
}

// Close stops l dialling, leaving the connection it accepted to the
// server.
func (l *Listener) Close() error {
	l.cancel()
	return nil
}

func (l *Listener) Addr() net.Addr { return tunnelAddr(l.t.Address) }

// tunnelAddr is the control plane's address, as a tunnel's.
type tunnelAddr string

func (a tunnelAddr) Network() string { return "tcp" }
func (a tunnelAddr) String() string  { return string(a) }

// conn is a connection to the control plane, which says when it
// closes, and why.
type conn struct {
	net.Conn
	opened time.Time
	once   sync.Once
	closed chan struct{}

	mu sync.Mutex
	// readErr is the first error reading the connection, such as the
	// control plane's alert refusing the node's certificate.
	readErr error
	// closedFirst says the node closed the connection before reading it
	// failed, as when its keepalive found the control plane silent.
	closedFirst bool
}

func (c *conn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	if err != nil {
		c.mu.Lock()
		if c.readErr == nil {
			c.readErr = err
		}
		c.mu.Unlock()
	}
	return n, err
}

// cause is why the connection closed, as far as reading it says.
func (c *conn) cause() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch {
	case c.closedFirst:
		return errors.New("the node closed it, as when the control plane stops answering its pings or breaks the protocol")
	case errors.Is(c.readErr, os.ErrDeadlineExceeded):
		return errors.New("the control plane didn't start HTTP/2 in time")
	}
	return c.readErr
}

func (c *conn) Close() error {
	c.mu.Lock()
	if c.readErr == nil {
		c.closedFirst = true
	}
	c.mu.Unlock()
	c.once.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

// Lockout is the options of a server on a node's local socket that, while
// locked says the node is in control plane mode, refuses every call but
// those of the methods allowed, by their full names, with
// FAILED_PRECONDITION, so that the node is never operated in both modes.
// It refuses the calls the server hands to an unknown service handler
// too. serves says what the socket serves meanwhile, as in "status".
func Lockout(locked func() bool, serves string, allowed ...string) []grpc.ServerOption {
	check := func(method string) error {
		if !locked() || slices.Contains(allowed, method) {
			return nil
		}
		return status.Errorf(codes.FailedPrecondition, "the node is in control plane mode, so its local socket serves only %s: a control plane operates it. To operate it over SSH again, detach it from the control plane for good with agent detach, run as root on the node", serves)
	}
	return []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			if err := check(info.FullMethod); err != nil {
				return nil, err
			}
			return handler(ctx, req)
		}),
		grpc.ChainStreamInterceptor(func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			if err := check(info.FullMethod); err != nil {
				return err
			}
			return handler(srv, ss)
		}),
	}
}
