//go:build linux

package agent

import (
	"context"
	"fmt"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/hostlayout"
)

// joined says whether the node is in control plane mode, so that its
// local socket is locked out.
func (s *agentServer) joined() bool {
	return s.controlPlane.Load()
}

// mode is the node's mode, as the node API says it.
func (s *agentServer) mode() nodev1.Mode {
	if s.joined() {
		return nodev1.Mode_MODE_CONTROL_PLANE
	}
	return nodev1.Mode_MODE_DIRECT
}

// refuseDetach refuses Detach over the control plane's tunnel: only root
// on the node may detach it.
func refuseDetach(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if info.FullMethod == nodev1.Agent_Detach_FullMethodName {
		return nil, status.Error(codes.PermissionDenied, "only root on the node may detach it, with agent detach")
	}
	return handler(ctx, req)
}

func (s *agentServer) Detach(ctx context.Context, _ *nodev1.DetachRequest) (*nodev1.DetachResponse, error) {
	s.detaching.Lock()
	defer s.detaching.Unlock()
	if !s.joined() {
		return nil, status.Error(codes.FailedPrecondition, "the node is in direct mode already")
	}
	warn("break-glass detach: the node leaves the control plane at %s, and is operated in direct mode from now on", s.node.ControlPlane)
	node := s.node
	node.Mode, node.ControlPlane = hostlayout.ModeDirect, ""
	data, err := node.Marshal()
	if err == nil {
		// The node file is everyone's to read, as node add writes it.
		err = writeFileMode(s.nodeFile, data, 0o644)
	}
	if err != nil {
		warn("break-glass detach failed, so the node stays in control plane mode: writing %s: %v", s.nodeFile, err)
		return nil, status.Errorf(codes.Internal, "writing %s: %v", s.nodeFile, err)
	}
	s.node = node
	s.controlPlane.Store(false)
	if s.tunnel != nil {
		s.tunnel.Disconnect()
	}
	// The updater sees the node file, and follows.
	warn("break-glass detach: the node is in direct mode")
	return &nodev1.DetachResponse{}, nil
}

// warn logs a line that the journal logs at warning level, by its
// priority prefix.
func warn(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "<4>"+format+"\n", args...)
}
