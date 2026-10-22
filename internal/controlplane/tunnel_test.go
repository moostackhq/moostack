package controlplane_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	controlplanev1 "github.com/moostackhq/moostack/api/controlplane/v1"
	"github.com/moostackhq/moostack/internal/controlplane"
)

// A control plane that refuses the node's identity, as one it removed,
// has the node log why.
func TestATunnelLogsWhyTheControlPlaneRefusedIt(t *testing.T) {
	cp := newControlPlane(t)
	id := node(t, cp.ca)
	_, logs := serveTunnel(t, cp, id, controlplanev1.ProtocolAgent)
	// The control plane refuses the node's identity, as one it removed.
	cas := x509.NewCertPool()
	cas.AppendCertsFromPEM(newCA(t, "other").CertPEM())
	go func() {
		for {
			raw, err := cp.l.Accept()
			if err != nil {
				return
			}
			conn := tls.Server(raw, &tls.Config{Certificates: []tls.Certificate{cp.cert}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: cas, NextProtos: []string{controlplanev1.ProtocolAgent}})
			conn.Handshake()
			conn.Close()
		}
	}()
	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(logs.String(), "certificate") && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	if !strings.Contains(logs.String(), "certificate") {
		t.Errorf("the node logged\n%s\nwant that the control plane refused its certificate", logs)
	}
}

// A tunnel disconnected while it dials closes the connection the dial
// then makes, and dials no more.
func TestATunnelDisconnectedWhileDiallingServesNothing(t *testing.T) {
	cp := newControlPlane(t)
	l, _ := serveTunnel(t, cp, node(t, cp.ca), controlplanev1.ProtocolAgent)
	cas := x509.NewCertPool()
	cas.AppendCertsFromPEM(cp.ca.CertPEM())
	raw, err := cp.l.Accept()
	if err != nil {
		t.Fatal(err)
	}
	// Hold the handshake while the node disconnects.
	l.Disconnect()
	conn := tls.Server(raw, &tls.Config{Certificates: []tls.Certificate{cp.cert}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: cas, NextProtos: []string{controlplanev1.ProtocolAgent}})
	if err := conn.Handshake(); err != nil {
		t.Fatal(err)
	}
	client, err := grpc.NewClient("passthrough:///node", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return conn, nil }))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if _, err := healthpb.NewHealthClient(client).Check(ctx, &healthpb.HealthCheckRequest{}); err == nil {
		t.Error("the node served the connection it dialled while disconnecting")
	}
	if _, err := cp.accept(t, controlplanev1.ProtocolAgent, 3*time.Second); err == nil {
		t.Error("dialled again after disconnecting")
	}
}

// countingCP accepts TCP connections and counts them, doing nothing else
// or, with handshake, the TLS handshake but no HTTP/2.
func countingCP(t *testing.T, cp *fakeControlPlane, handshake bool) *atomic.Int32 {
	var n atomic.Int32
	cas := x509.NewCertPool()
	cas.AppendCertsFromPEM(cp.ca.CertPEM())
	go func() {
		for {
			raw, err := cp.l.Accept()
			if err != nil {
				return
			}
			n.Add(1)
			t.Cleanup(func() { raw.Close() })
			if handshake {
				conn := tls.Server(raw, &tls.Config{Certificates: []tls.Certificate{cp.cert}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: cas, NextProtos: []string{controlplanev1.ProtocolAgent}})
				go conn.Handshake()
			}
		}
	}()
	return &n
}

// A control plane that accepts the connection and never answers the TLS
// handshake is dialled again once the dial times out.
func TestATunnelWhoseHandshakeHangsIsDialledAgain(t *testing.T) {
	cp := newControlPlane(t)
	n := countingCP(t, cp, false)
	serveTunnel(t, cp, node(t, cp.ca), controlplanev1.ProtocolAgent)
	time.Sleep(13 * time.Second)
	if n.Load() < 2 {
		t.Errorf("%d dials in 13 s against a hung handshake, want a second one after the 10 s dial timeout", n.Load())
	}
}

// A control plane that completes TLS but never starts HTTP/2 is dialled
// again, as the protocol says.
func TestATunnelWithoutHTTP2IsDialledAgain(t *testing.T) {
	cp := newControlPlane(t)
	n := countingCP(t, cp, true)
	_, logs := serveTunnel(t, cp, node(t, cp.ca), controlplanev1.ProtocolAgent)
	time.Sleep(13 * time.Second)
	if n.Load() < 2 {
		t.Errorf("%d dials in 13 s against a control plane that never starts HTTP/2, want a second", n.Load())
	}
	if !strings.Contains(logs.String(), "didn't start HTTP/2 in time") {
		t.Errorf("the node logged\n%s\nwant why it closed the connection", logs)
	}
}

// Dials to a control plane that refuses them back off, doubling from
// half a second, each wait jittered down to half of it.
func TestATunnelBacksOffDoubling(t *testing.T) {
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	// The control plane takes each connection and closes it at once.
	var mu sync.Mutex
	var dials []time.Time
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			dials = append(dials, time.Now())
			mu.Unlock()
			c.Close()
		}
	}()
	logs := &logs{}
	tl := controlplane.Listen(controlplane.Tunnel{Address: l.Addr().String(), Identity: node(t, newCA(t, "cp")), Protocol: controlplanev1.ProtocolAgent, Log: logs.add})
	defer tl.Close()
	go tl.Accept()
	time.Sleep(9 * time.Second)
	re := regexp.MustCompile(`trying again in (\S+)`)
	var logged []time.Duration
	for _, m := range re.FindAllStringSubmatch(logs.String(), -1) {
		d, _ := time.ParseDuration(m[1])
		logged = append(logged, d)
	}
	want := []time.Duration{500 * time.Millisecond, time.Second, 2 * time.Second, 4 * time.Second}
	if len(logged) < len(want) || !slices.Equal(logged[:len(want)], want) {
		t.Errorf("the node logged waits of %v between dials, want %v first", logged, want)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(dials) < len(want)+1 {
		t.Fatalf("%d dials in 9 s, want at least %d", len(dials), len(want)+1)
	}
	jittered := false
	for i, w := range want {
		gap := dials[i+1].Sub(dials[i])
		if gap < w/2-50*time.Millisecond || gap > w+500*time.Millisecond {
			t.Errorf("dial %d came %s after the one before, want between %s and %s", i+2, gap, w/2, w)
		}
		if gap < w-25*time.Millisecond {
			jittered = true
		}
	}
	// Each wait is drawn from w/2 to w, so the odds of four within 25 ms of
	// their whole backoff are about one in half a million, more under a
	// load that slows the dials.
	if !jittered {
		t.Error("every wait was the whole of its backoff, unjittered")
	}
}
