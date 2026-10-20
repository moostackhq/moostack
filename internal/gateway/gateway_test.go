package gateway_test

import (
	"bytes"
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	servicev1 "github.com/moostackhq/moostack/api/service/v1"
	"github.com/moostackhq/moostack/internal/gateway"
)

// These tests call a gateway's server as a caller of the operator kind,
// with services that record what they receive.

const (
	operator = servicev1.CallerKind_CALLER_KIND_OPERATOR
	peer     = servicev1.CallerKind_CALLER_KIND_PEER_NODE
)

// everyone is every caller kind.
var everyone = []servicev1.CallerKind{operator, servicev1.CallerKind_CALLER_KIND_CONTROL_PLANE, peer, servicev1.CallerKind_CALLER_KIND_LOCAL_SERVICE}

// methods are what the test services describe: Echo for everyone, Ping
// for peers alone, and Stream and Fail for everyone. Their Quiet method
// is undescribed.
var methods = map[string][]servicev1.CallerKind{
	"Health":   everyone,
	"Describe": everyone,
	"Echo":     everyone,
	"Ping":     {peer},
	"Stream":   everyone,
	"Fail":     everyone,
	"Cancel":   everyone,
}

func TestACallReachesTheServiceThatServesItsAPI(t *testing.T) {
	g := gateway.New()
	defer g.Close()
	a := startService(t, g, "test.a.v1.A", methods)
	b := startService(t, g, "test.b.v1.B", methods)
	conn := startGateway(t, g, operator)

	// The payload isn't a valid message of any kind, so only a gateway
	// that passes it on untouched gets it through.
	payload := []byte{0xff, 0x00, 0xfe, 'x'}
	got, err := call(t, conn, "/test.b.v1.B/Echo", payload)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("the answer is %x, want %x", got, payload)
	}
	if calls := b.received(); len(calls) != 1 || calls[0].method != "/test.b.v1.B/Echo" || !bytes.Equal(calls[0].payloads[0], payload) {
		t.Errorf("B received %+v", calls)
	}
	if calls := a.received(); len(calls) != 0 {
		t.Errorf("A received %+v", calls)
	}
}

func TestTheServiceIsToldTheCallerKindAndNotWhatTheCallerClaims(t *testing.T) {
	g := gateway.New()
	defer g.Close()
	s := startService(t, g, "test.a.v1.A", methods)
	conn := startGateway(t, g, operator)
	ctx := metadata.AppendToOutgoingContext(t.Context(),
		servicev1.CallerKindKey, peer.String(),
		servicev1.CallerNodeKey, "node-9",
		"moostack-caller-other", "x",
		"x-request-id", "42")
	if _, err := callContext(ctx, conn, "/test.a.v1.A/Echo", []byte("hi")); err != nil {
		t.Fatal(err)
	}
	calls := s.received()
	if len(calls) != 1 {
		t.Fatalf("the service received %d calls", len(calls))
	}
	md := calls[0].md
	if got := md.Get(servicev1.CallerKindKey); len(got) != 1 || got[0] != "CALLER_KIND_OPERATOR" {
		t.Errorf("the service was told the caller kind %q", got)
	}
	for _, key := range []string{servicev1.CallerNodeKey, "moostack-caller-other"} {
		if got := md.Get(key); len(got) != 0 {
			t.Errorf("the service was told %s %q, which only the caller claimed", key, got)
		}
	}
	if got := md.Get("x-request-id"); len(got) != 1 || got[0] != "42" {
		t.Errorf("the caller's own metadata reached the service as %q", got)
	}
}

func TestAMethodTheCallerKindIsntAllowedIsRefusedBeforeTheService(t *testing.T) {
	g := gateway.New()
	defer g.Close()
	s := startService(t, g, "test.a.v1.A", methods)
	conn := startGateway(t, g, operator)
	_, err := call(t, conn, "/test.a.v1.A/Ping", []byte("hi"))
	if status.Code(err) != codes.PermissionDenied || !strings.Contains(err.Error(), "CALLER_KIND_OPERATOR") {
		t.Errorf("an operator calling Ping: %v, want permission denied naming the caller kind", err)
	}
	if calls := s.received(); len(calls) != 0 {
		t.Errorf("the service received %+v", calls)
	}
}

func TestAMethodTheServiceDoesntDescribeIsRefused(t *testing.T) {
	g := gateway.New()
	defer g.Close()
	s := startService(t, g, "test.a.v1.A", methods)
	conn := startGateway(t, g, operator)
	if _, err := call(t, conn, "/test.a.v1.A/Quiet", []byte("hi")); status.Code(err) != codes.PermissionDenied {
		t.Errorf("calling an undescribed method: %v, want permission denied", err)
	}
	if calls := s.received(); len(calls) != 0 {
		t.Errorf("the service received %+v", calls)
	}
}

func TestACallOfAnAPIWithoutARouteIsUnimplemented(t *testing.T) {
	g := gateway.New()
	defer g.Close()
	startService(t, g, "test.a.v1.A", methods)
	conn := startGateway(t, g, operator)
	if _, err := call(t, conn, "/test.z.v1.Z/Echo", []byte("hi")); status.Code(err) != codes.Unimplemented {
		t.Errorf("calling an API no service serves: %v, want unimplemented", err)
	}
}

func TestAServiceThatIsntRunningAnswersUnavailableAtOnce(t *testing.T) {
	g := gateway.New()
	defer g.Close()
	if err := g.Set("test.a.v1.A", filepath.Join(shortTempDir(t), "missing.sock")); err != nil {
		t.Fatal(err)
	}
	s := startService(t, g, "test.b.v1.B", methods)
	conn := startGateway(t, g, operator)
	if _, err := call(t, conn, "/test.b.v1.B/Echo", nil); err != nil {
		t.Fatal(err)
	}
	s.stop()
	for _, api := range []string{"test.a.v1.A", "test.b.v1.B"} {
		start := time.Now()
		_, err := call(t, conn, "/"+api+"/Echo", nil)
		if status.Code(err) != codes.Unavailable {
			t.Errorf("calling %s, whose service isn't running: %v, want unavailable", api, err)
		}
		if took := time.Since(start); took > 2*time.Second {
			t.Errorf("calling %s, whose service isn't running, took %s", api, took)
		}
	}
}

func TestAServiceThatComesBackIsReachedAgainWithinASecond(t *testing.T) {
	g := gateway.New()
	defer g.Close()
	s := startService(t, g, "test.a.v1.A", methods)
	conn := startGateway(t, g, operator)
	if _, err := call(t, conn, "/test.a.v1.A/Echo", nil); err != nil {
		t.Fatal(err)
	}
	s.stop()
	// The gateway keeps trying to reconnect while the service is gone.
	for range 25 {
		call(t, conn, "/test.a.v1.A/Echo", nil)
		time.Sleep(200 * time.Millisecond)
	}
	s.restart(t)
	deadline := time.Now().Add(1500 * time.Millisecond)
	for {
		_, err := call(t, conn, "/test.a.v1.A/Echo", nil)
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the service came back, but calls still fail: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestSetForgetsWhatTheServiceDescribed(t *testing.T) {
	g := gateway.New()
	defer g.Close()
	startService(t, g, "test.a.v1.A", methods)
	conn := startGateway(t, g, operator)
	if _, err := call(t, conn, "/test.a.v1.A/Ping", nil); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("Ping before the new service: %v", err)
	}
	// A newer service lets operators ping, and replaces the route.
	newer := map[string][]servicev1.CallerKind{"Describe": everyone, "Ping": everyone}
	startService(t, g, "test.a.v1.A", newer)
	if _, err := call(t, conn, "/test.a.v1.A/Ping", nil); err != nil {
		t.Errorf("Ping once the service allows operators: %v", err)
	}
}

func TestACallToAHungServiceDoesntWaitForAnotherCallsDescription(t *testing.T) {
	g := gateway.New()
	defer g.Close()
	s := startService(t, g, "test.a.v1.A", methods)
	s.mu.Lock()
	s.hang, s.described = true, make(chan struct{}, 2)
	s.mu.Unlock()
	conn := startGateway(t, g, operator)
	go call(t, conn, "/test.a.v1.A/Echo", nil)
	<-s.described
	// The service is asked again for this call, and gives up as late as
	// the first call does, not after it.
	start := time.Now()
	if _, err := call(t, conn, "/test.a.v1.A/Echo", nil); err == nil {
		t.Error("a call to a service that doesn't describe itself succeeded")
	}
	if took := time.Since(start); took > 7*time.Second {
		t.Errorf("a call to a hung service took %s, waiting for another call's description", took)
	}
}

func TestARemovedRouteIsUnimplemented(t *testing.T) {
	g := gateway.New()
	defer g.Close()
	s := startService(t, g, "test.a.v1.A", methods)
	conn := startGateway(t, g, operator)
	g.Remove("test.a.v1.A")
	if _, err := call(t, conn, "/test.a.v1.A/Echo", nil); status.Code(err) != codes.Unimplemented {
		t.Errorf("calling a removed route: %v, want unimplemented", err)
	}
	if calls := s.received(); len(calls) != 0 {
		t.Errorf("the service received %+v", calls)
	}
}

func TestSettingAnUnchangedRouteKeepsItsCalls(t *testing.T) {
	g := gateway.New()
	defer g.Close()
	s := startService(t, g, "test.a.v1.A", methods)
	conn := startGateway(t, g, operator)
	stream, err := conn.NewStream(t.Context(), &grpc.StreamDesc{ClientStreams: true, ServerStreams: true}, "/test.a.v1.A/Stream", grpc.ForceCodecV2(rawCodec{}))
	if err != nil {
		t.Fatal(err)
	}
	for i, p := range []string{"before", "after"} {
		if i == 1 {
			if err := g.Set("test.a.v1.A", s.socket); err != nil {
				t.Fatal(err)
			}
		}
		if err := stream.SendMsg(&frame{[]byte(p)}); err != nil {
			t.Fatalf("sending %s setting the route again: %v", p, err)
		}
		var got frame
		if err := stream.RecvMsg(&got); err != nil {
			t.Fatalf("receiving %s setting the route again: %v", p, err)
		}
	}
}

func TestAServicesOwnCanceledReachesTheCallerAsItIs(t *testing.T) {
	g := gateway.New()
	defer g.Close()
	startService(t, g, "test.a.v1.A", methods)
	conn := startGateway(t, g, operator)
	_, err := call(t, conn, "/test.a.v1.A/Cancel", nil)
	if s, _ := status.FromError(err); s.Code() != codes.Canceled || s.Message() != "the service gave up" {
		t.Errorf("a call the service cancels: %v, want the service's canceled", err)
	}
}

func TestACallThatSettingAnotherSocketCutsOffAnswersUnavailable(t *testing.T) {
	g := gateway.New()
	defer g.Close()
	startService(t, g, "test.a.v1.A", methods)
	conn := startGateway(t, g, operator)
	stream, err := conn.NewStream(t.Context(), &grpc.StreamDesc{ClientStreams: true, ServerStreams: true}, "/test.a.v1.A/Stream", grpc.ForceCodecV2(rawCodec{}))
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.SendMsg(&frame{[]byte("one")}); err != nil {
		t.Fatal(err)
	}
	if err := stream.RecvMsg(&frame{}); err != nil {
		t.Fatal(err)
	}
	startService(t, g, "test.a.v1.A", methods)
	if err := stream.RecvMsg(&frame{}); status.Code(err) != codes.Unavailable {
		t.Errorf("a call whose route was set to another socket: %v, want unavailable", err)
	}
}

func TestACallWaitingForADescriptionThatAHoldCutsOffAnswersUnavailable(t *testing.T) {
	g := gateway.New()
	defer g.Close()
	s := startService(t, g, "test.a.v1.A", methods)
	s.mu.Lock()
	s.hang, s.described = true, make(chan struct{}, 1)
	s.mu.Unlock()
	conn := startGateway(t, g, operator)
	done := make(chan error, 1)
	go func() {
		_, err := call(t, conn, "/test.a.v1.A/Echo", nil)
		done <- err
	}()
	<-s.described
	g.Hold("test.a.v1.A")
	select {
	case err := <-done:
		if status.Code(err) != codes.Unavailable {
			t.Errorf("a call waiting for a description that a hold cut off: %v, want unavailable", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("a call waiting for a description goes on after a hold")
	}
}

func TestACallThatAHoldCutsOffAnswersUnavailable(t *testing.T) {
	g := gateway.New()
	defer g.Close()
	startService(t, g, "test.a.v1.A", methods)
	conn := startGateway(t, g, operator)
	stream, err := conn.NewStream(t.Context(), &grpc.StreamDesc{ClientStreams: true, ServerStreams: true}, "/test.a.v1.A/Stream", grpc.ForceCodecV2(rawCodec{}))
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.SendMsg(&frame{[]byte("one")}); err != nil {
		t.Fatal(err)
	}
	if err := stream.RecvMsg(&frame{}); err != nil {
		t.Fatal(err)
	}
	g.Hold("test.a.v1.A")
	if err := stream.RecvMsg(&frame{}); status.Code(err) != codes.Unavailable {
		t.Errorf("a call whose route was held: %v, want unavailable", err)
	}
}

func TestAHeldRouteAnswersUnavailableAndForgetsWhatTheServiceDescribed(t *testing.T) {
	g := gateway.New()
	defer g.Close()
	s := startService(t, g, "test.a.v1.A", methods)
	conn := startGateway(t, g, operator)
	if _, err := call(t, conn, "/test.a.v1.A/Ping", nil); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("Ping before the service is replaced: %v", err)
	}
	g.Hold("test.a.v1.A")
	start := time.Now()
	if _, err := call(t, conn, "/test.a.v1.A/Echo", nil); status.Code(err) != codes.Unavailable {
		t.Errorf("calling a held route: %v, want unavailable", err)
	}
	if took := time.Since(start); took > time.Second {
		t.Errorf("calling a held route took %s", took)
	}
	if err := g.Health(t.Context(), "test.a.v1.A"); err == nil {
		t.Error("a held route is healthy")
	}
	// The service is replaced, on the same socket, by one that lets
	// operators ping, and the route set again.
	s.stop()
	s.methods = map[string][]servicev1.CallerKind{"Describe": everyone, "Ping": everyone}
	s.restart(t)
	if err := g.Set("test.a.v1.A", s.socket); err != nil {
		t.Fatal(err)
	}
	if _, err := call(t, conn, "/test.a.v1.A/Ping", nil); err != nil {
		t.Errorf("Ping once the replacement allows operators: %v", err)
	}
	if calls := s.received(); len(calls) != 1 || calls[0].method != "/test.a.v1.A/Ping" {
		t.Errorf("the service received %+v", calls)
	}
}

func TestTheServicesErrorsAndTrailersReachTheCaller(t *testing.T) {
	g := gateway.New()
	defer g.Close()
	startService(t, g, "test.a.v1.A", methods)
	conn := startGateway(t, g, operator)
	var trailer metadata.MD
	err := conn.Invoke(t.Context(), "/test.a.v1.A/Fail", &frame{}, &frame{}, grpc.ForceCodecV2(rawCodec{}), grpc.Trailer(&trailer))
	if s, _ := status.FromError(err); s.Code() != codes.NotFound || s.Message() != "no such thing" {
		t.Errorf("a failing call: %v, want the service's not found", err)
	}
	if got := trailer.Get("x-why"); len(got) != 1 || got[0] != "testing" {
		t.Errorf("the trailer is %v", trailer)
	}
}

func TestAStreamPassesThroughBothWays(t *testing.T) {
	g := gateway.New()
	defer g.Close()
	s := startService(t, g, "test.a.v1.A", methods)
	conn := startGateway(t, g, operator)
	stream, err := conn.NewStream(t.Context(), &grpc.StreamDesc{ClientStreams: true, ServerStreams: true}, "/test.a.v1.A/Stream", grpc.ForceCodecV2(rawCodec{}))
	if err != nil {
		t.Fatal(err)
	}
	sent := [][]byte{[]byte("one"), []byte("two"), []byte("three")}
	for _, p := range sent {
		if err := stream.SendMsg(&frame{p}); err != nil {
			t.Fatal(err)
		}
		var got frame
		if err := stream.RecvMsg(&got); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got.data, p) {
			t.Errorf("the stream answered %q to %q", got.data, p)
		}
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if err := stream.RecvMsg(&frame{}); err != io.EOF {
		t.Errorf("the stream ended with %v, want EOF", err)
	}
	if calls := s.received(); len(calls) != 1 || len(calls[0].payloads) != 3 {
		t.Errorf("the service received %+v", calls)
	}
}

func TestHealthAsksTheService(t *testing.T) {
	g := gateway.New()
	defer g.Close()
	s := startService(t, g, "test.a.v1.A", methods)
	if err := g.Health(t.Context(), "test.a.v1.A"); err != nil {
		t.Errorf("a healthy service: %v", err)
	}
	s.setProblem("out of disk")
	if err := g.Health(t.Context(), "test.a.v1.A"); err == nil || !strings.Contains(err.Error(), "out of disk") {
		t.Errorf("a service with a problem: %v, want it", err)
	}
	s.stop()
	if err := g.Health(t.Context(), "test.a.v1.A"); status.Code(err) != codes.Unavailable {
		t.Errorf("a service that isn't running: %v, want unavailable", err)
	}
	if err := g.Health(t.Context(), "test.z.v1.Z"); err == nil {
		t.Error("an API without a route is healthy")
	}
}

// service is a cluster service for the tests, which records the calls
// it receives, apart from Health and Describe, and answers each message
// of a call with the same bytes.
type service struct {
	api     string
	socket  string
	methods map[string][]servicev1.CallerKind
	server  *grpc.Server

	mu      sync.Mutex
	calls   []received
	problem string
	// hang, when set, makes Describe tell described and then never
	// answer.
	hang      bool
	described chan struct{}
}

type received struct {
	method   string
	md       metadata.MD
	payloads [][]byte
}

// startService starts a service of api, describing methods, and routes
// g's calls of api to it.
func startService(t *testing.T, g *gateway.Gateway, api string, methods map[string][]servicev1.CallerKind) *service {
	t.Helper()
	s := &service{api: api, methods: methods, socket: filepath.Join(shortTempDir(t), "api.sock")}
	s.restart(t)
	if err := g.Set(api, s.socket); err != nil {
		t.Fatal(err)
	}
	return s
}

// restart starts serving on the service's socket, as a service run again
// does.
func (s *service) restart(t *testing.T) {
	t.Helper()
	os.Remove(s.socket)
	l, err := net.Listen("unix", s.socket)
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.ForceServerCodecV2(rawCodec{}), grpc.UnknownServiceHandler(s.handle))
	go server.Serve(l)
	t.Cleanup(server.Stop)
	s.mu.Lock()
	s.server = server
	s.mu.Unlock()
}

func (s *service) handle(_ any, stream grpc.ServerStream) error {
	method, _ := grpc.MethodFromServerStream(stream)
	switch strings.TrimPrefix(method, "/"+s.api+"/") {
	case "Describe":
		s.mu.Lock()
		hang := s.hang
		s.mu.Unlock()
		if hang {
			s.described <- struct{}{}
			<-stream.Context().Done()
			return stream.Context().Err()
		}
		res := &servicev1.DescribeResponse{}
		for name, callers := range s.methods {
			res.Methods = append(res.Methods, &servicev1.Method{Name: name, Callers: callers})
		}
		return answer(stream, res)
	case "Health":
		s.mu.Lock()
		defer s.mu.Unlock()
		return answer(stream, &servicev1.HealthResponse{Problem: s.problem})
	case "Fail":
		stream.SetTrailer(metadata.Pairs("x-why", "testing"))
		return status.Error(codes.NotFound, "no such thing")
	case "Cancel":
		return status.Error(codes.Canceled, "the service gave up")
	}
	md, _ := metadata.FromIncomingContext(stream.Context())
	r := received{method: method, md: md}
	for {
		var f frame
		err := stream.RecvMsg(&f)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		r.payloads = append(r.payloads, f.data)
		if err := stream.SendMsg(&f); err != nil {
			return err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, r)
	return nil
}

// answer receives a call's request and answers it with res.
func answer(stream grpc.ServerStream, res proto.Message) error {
	if err := stream.RecvMsg(&frame{}); err != nil {
		return err
	}
	data, err := proto.Marshal(res)
	if err != nil {
		return err
	}
	return stream.SendMsg(&frame{data})
}

func (s *service) received() []received {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]received(nil), s.calls...)
}

func (s *service) setProblem(p string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.problem = p
}

func (s *service) stop() {
	s.mu.Lock()
	server := s.server
	s.mu.Unlock()
	server.Stop()
}

// startGateway serves g's calls from callers of kind, and returns a
// connection to it.
func startGateway(t *testing.T, g *gateway.Gateway, kind servicev1.CallerKind) *grpc.ClientConn {
	t.Helper()
	socket := filepath.Join(shortTempDir(t), "gateway.sock")
	l, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(g.ServerOptions(kind)...)
	go server.Serve(l)
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("unix://"+socket, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

// call makes a unary call of method with payload, and returns the
// answer's bytes.
func call(t *testing.T, conn *grpc.ClientConn, method string, payload []byte) ([]byte, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	return callContext(ctx, conn, method, payload)
}

func callContext(ctx context.Context, conn *grpc.ClientConn, method string, payload []byte) ([]byte, error) {
	var out frame
	err := conn.Invoke(ctx, method, &frame{payload}, &out, grpc.ForceCodecV2(rawCodec{}))
	return out.data, err
}

// shortTempDir is a temporary directory with a path short enough for a
// unix socket in it, which t.TempDir's may not be.
func shortTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "gw")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

// frame is a message as the bytes on the wire.
type frame struct{ data []byte }

// rawCodec sends and receives frames as they are.
type rawCodec struct{}

func (rawCodec) Marshal(v any) (mem.BufferSlice, error) {
	return mem.BufferSlice{mem.SliceBuffer(v.(*frame).data)}, nil
}

func (rawCodec) Unmarshal(data mem.BufferSlice, v any) error {
	v.(*frame).data = data.Materialize()
	return nil
}

func (rawCodec) Name() string { return "proto" }

var _ encoding.CodecV2 = rawCodec{}
