package controlplane_test

import (
	"context"
	"net"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/moostackhq/moostack/internal/controlplane"
)

// The lockout refuses what isn't allowed, the calls to an unknown
// service handler included, while locked, and nothing once unlocked.
func TestLockoutRefusesAllButTheAllowedWhileLocked(t *testing.T) {
	var locked atomic.Bool
	locked.Store(true)
	var reached atomic.Int32
	s := grpc.NewServer(append([]grpc.ServerOption{grpc.UnknownServiceHandler(func(any, grpc.ServerStream) error {
		reached.Add(1)
		return status.Error(codes.Unimplemented, "reached")
	})}, controlplane.Lockout(locked.Load, "status", "/grpc.health.v1.Health/Check")...)...)
	healthpb.RegisterHealthServer(s, healthServer{})
	l, _ := net.Listen("tcp", "localhost:0")
	go s.Serve(l)
	defer s.Stop()
	c, _ := grpc.NewClient(l.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer c.Close()
	h := healthpb.NewHealthClient(c)
	ctx := t.Context()
	if _, err := h.Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Errorf("allowed call: %v", err)
	}
	if _, err := h.List(ctx, &healthpb.HealthListRequest{}); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("unary not allowed: %v", err)
	}
	err := c.Invoke(ctx, "/moostack.service.v1.Foo/Bar", &healthpb.HealthCheckRequest{}, &healthpb.HealthCheckResponse{})
	if status.Code(err) != codes.FailedPrecondition || reached.Load() != 0 {
		t.Errorf("unknown service: %v, reached %d", err, reached.Load())
	}
	locked.Store(false)
	if _, err := h.List(ctx, &healthpb.HealthListRequest{}); err != nil && status.Code(err) == codes.FailedPrecondition {
		t.Errorf("unlocked: %v", err)
	}
}

type healthServer struct {
	healthpb.UnimplementedHealthServer
}

func (healthServer) Check(context.Context, *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	return &healthpb.HealthCheckResponse{}, nil
}

func (healthServer) List(context.Context, *healthpb.HealthListRequest) (*healthpb.HealthListResponse, error) {
	return &healthpb.HealthListResponse{}, nil
}
