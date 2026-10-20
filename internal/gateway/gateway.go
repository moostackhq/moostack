// Package gateway is how the agent carries calls to the cluster services
// without understanding them. It routes each call by the gRPC service
// name of the API it calls, to the service that serves that API on a unix
// socket, and passes the call's messages through as they are. It tells
// the service the caller kind in the call's metadata, and refuses a call
// whose caller kind the service doesn't allow for the method, so the
// service never sees it.
package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/encoding/proto"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	servicev1 "github.com/moostackhq/moostack/api/service/v1"
)

// Gateway routes calls to the cluster services. Close it when done.
type Gateway struct {
	mu     sync.Mutex
	routes map[string]*route
}

// route is the service that serves an API, as a connection to its
// socket, with what it described.
type route struct {
	api    string
	socket string
	// conn is nil while the route is held.
	conn *grpc.ClientConn

	mu sync.Mutex
	// methods are the callers allowed for each method, by its name, nil
	// until the service has described them.
	methods map[string][]servicev1.CallerKind
}

// New is a gateway without routes.
func New() *Gateway {
	return &Gateway{routes: map[string]*route{}}
}

// Set routes calls of the API whose gRPC service name is api to the
// service serving it on socket. A route of api to socket already is
// kept, with its calls. Any other route of api is replaced, and the
// gateway asks the service again which callers each method allows. The
// caller must not call Set, Hold or Remove of api at the same time.
func (g *Gateway) Set(api, socket string) error {
	if r := g.route(api); r != nil && !r.held() && r.socket == socket {
		return nil
	}
	conn, err := grpc.NewClient("passthrough:///"+api,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", socket)
		}),
		// A service that comes back is reached again within a second.
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff:           backoff.Config{BaseDelay: 100 * time.Millisecond, Multiplier: 1.6, MaxDelay: time.Second},
			MinConnectTimeout: time.Second,
		}))
	if err != nil {
		return err
	}
	g.mu.Lock()
	old := g.routes[api]
	g.routes[api] = &route{api: api, socket: socket, conn: conn}
	g.mu.Unlock()
	old.close()
	return nil
}

// Hold makes calls of api answer 'unavailable' until the route is set
// again, as while its service is replaced, and forgets what the service
// described.
func (g *Gateway) Hold(api string) {
	g.mu.Lock()
	r := g.routes[api]
	g.routes[api] = &route{api: api}
	g.mu.Unlock()
	r.close()
}

// Remove removes the route of api, if there is one.
func (g *Gateway) Remove(api string) {
	g.mu.Lock()
	r := g.routes[api]
	delete(g.routes, api)
	g.mu.Unlock()
	r.close()
}

// Close closes the connections to the services.
func (g *Gateway) Close() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	var err error
	for _, r := range g.routes {
		err = errors.Join(err, r.close())
	}
	g.routes = map[string]*route{}
	return err
}

// held is whether calls of r's API answer 'unavailable' until the
// route is set again.
func (r *route) held() bool {
	return r.conn == nil
}

// close closes r's connection, if r is a route that has one.
func (r *route) close() error {
	if r == nil || r.held() {
		return nil
	}
	return r.conn.Close()
}

// route is the route of api, nil if there is none.
func (g *Gateway) route(api string) *route {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.routes[api]
}

// ServerOptions are the options of a server whose callers are of kind,
// for the gateway to handle the calls the server doesn't serve itself.
// The server's own services are unaffected.
func (g *Gateway) ServerOptions(kind servicev1.CallerKind) []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.ForceServerCodecV2(codec{}),
		grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error {
			return g.handle(stream, kind)
		}),
	}
}

// handle carries the call on stream, from a caller of kind, to the
// service that serves its API.
func (g *Gateway) handle(stream grpc.ServerStream, kind servicev1.CallerKind) error {
	full, ok := grpc.MethodFromServerStream(stream)
	if !ok {
		return status.Error(codes.Internal, "the call names no method")
	}
	api, method, ok := strings.Cut(strings.TrimPrefix(full, "/"), "/")
	if !ok {
		return status.Errorf(codes.Unimplemented, "%s isn't a method", full)
	}
	r := g.route(api)
	if r == nil {
		return status.Errorf(codes.Unimplemented, "no cluster service serves %s", api)
	}
	if r.held() {
		return status.Errorf(codes.Unavailable, "the service serving %s isn't running", api)
	}
	callers, err := r.callers(stream.Context(), method)
	if err != nil {
		return r.cutOff(err)
	}
	if !slices.Contains(callers, kind) {
		return status.Errorf(codes.PermissionDenied, "a caller of kind %s may not call %s", kind, full)
	}
	return r.cutOff(r.proxy(stream, full, kind))
}

// cutOff is err, the end of a call on r, but 'unavailable' if the call
// was canceled because r's connection closed, as a Hold or Set does.
func (r *route) cutOff(err error) error {
	if status.Code(err) == codes.Canceled && r.conn.GetState() == connectivity.Shutdown {
		return status.Errorf(codes.Unavailable, "the service's route changed during the call: %s", status.Convert(err).Message())
	}
	return err
}

// describeTimeout bounds asking a service which callers its methods
// allow.
const describeTimeout = 5 * time.Second

// callers are the caller kinds that the service allows to call method,
// none if it doesn't describe the method. It asks the service the first
// time. Calls that come while it asks ask too, rather than wait for an
// answer that a hung service never gives.
func (r *route) callers(ctx context.Context, method string) ([]servicev1.CallerKind, error) {
	r.mu.Lock()
	methods := r.methods
	r.mu.Unlock()
	if methods == nil {
		ctx, cancel := context.WithTimeout(ctx, describeTimeout)
		defer cancel()
		var res servicev1.DescribeResponse
		if err := r.conn.Invoke(ctx, "/"+r.api+"/Describe", &servicev1.DescribeRequest{}, &res); err != nil {
			s := status.Convert(err)
			return nil, status.Errorf(s.Code(), "the service serving %s doesn't describe its methods: %s", r.api, s.Message())
		}
		methods = map[string][]servicev1.CallerKind{}
		for _, m := range res.Methods {
			methods[m.Name] = m.Callers
		}
		r.mu.Lock()
		r.methods = methods
		r.mu.Unlock()
	}
	return methods[method], nil
}

// proxy passes the call on in, of the method full, to the service, as
// from a caller of kind, and its answer back.
func (r *route) proxy(in grpc.ServerStream, full string, kind servicev1.CallerKind) error {
	ctx, cancel := context.WithCancel(in.Context())
	defer cancel()
	md, _ := metadata.FromIncomingContext(ctx)
	md = md.Copy()
	for key := range md {
		if strings.HasPrefix(key, servicev1.CallerKeyPrefix) || strings.HasPrefix(key, ":") {
			delete(md, key)
		}
	}
	md.Set(servicev1.CallerKindKey, kind.String())
	out, err := r.conn.NewStream(metadata.NewOutgoingContext(ctx, md),
		&grpc.StreamDesc{ClientStreams: true, ServerStreams: true}, full, grpc.ForceCodecV2(codec{}))
	if err != nil {
		return err
	}
	// The caller's messages go on until the caller is done, or the
	// service ends the call, whose status comes back below.
	go func() {
		for {
			var f frame
			if err := in.RecvMsg(&f); err != nil {
				if err == io.EOF {
					out.CloseSend()
				}
				return
			}
			if err := out.SendMsg(&f); err != nil {
				return
			}
		}
	}()
	header, err := out.Header()
	if err != nil {
		return err
	}
	if err := in.SendHeader(header); err != nil {
		return err
	}
	for {
		var f frame
		if err := out.RecvMsg(&f); err != nil {
			in.SetTrailer(out.Trailer())
			if err == io.EOF {
				return nil
			}
			return err
		}
		if err := in.SendMsg(&f); err != nil {
			return err
		}
	}
}

// healthTimeout bounds asking a service whether it is healthy.
const healthTimeout = 2 * time.Second

// Health asks the service that serves api whether it is healthy, and
// fails with why not: the service's problem, or why it doesn't answer.
func (g *Gateway) Health(ctx context.Context, api string) error {
	r := g.route(api)
	if r == nil {
		return fmt.Errorf("no cluster service serves %s", api)
	}
	if r.held() {
		return status.Errorf(codes.Unavailable, "the service serving %s isn't running", api)
	}
	ctx, cancel := context.WithTimeout(ctx, healthTimeout)
	defer cancel()
	var res servicev1.HealthResponse
	if err := r.conn.Invoke(ctx, "/"+api+"/Health", &servicev1.HealthRequest{}, &res); err != nil {
		return fmt.Errorf("the service doesn't answer: %w", err)
	}
	if res.Problem != "" {
		return errors.New(res.Problem)
	}
	return nil
}

// frame is a message of a call that the gateway carries, as the bytes
// on the wire.
type frame struct{ data mem.BufferSlice }

// codec passes frames through as they are, and handles every other
// message as protobuf, so that a server's own services work as before.
type codec struct{}

// protoCodec is gRPC's protobuf codec, which codec hands every message
// but a frame to.
var protoCodec = encoding.GetCodecV2(proto.Name)

func (codec) Marshal(v any) (mem.BufferSlice, error) {
	if f, ok := v.(*frame); ok {
		// The reference that Unmarshal took goes on to gRPC, which frees
		// it once sent.
		return f.data, nil
	}
	return protoCodec.Marshal(v)
}

func (codec) Unmarshal(data mem.BufferSlice, v any) error {
	if f, ok := v.(*frame); ok {
		// gRPC frees data once Unmarshal returns, so the frame takes a
		// reference of its own, which Marshal hands on.
		data.Ref()
		f.data = data
		return nil
	}
	return protoCodec.Unmarshal(data, v)
}

func (codec) Name() string { return proto.Name }
