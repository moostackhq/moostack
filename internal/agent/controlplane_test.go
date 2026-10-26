//go:build linux

package agent_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"

	controlplanev1 "github.com/moostackhq/moostack/api/controlplane/v1"
	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/hostlayout"
	"github.com/moostackhq/moostack/internal/nodeclient"
	"github.com/moostackhq/moostack/internal/nodetest"
)

// These tests join a node to a tunnel peer on the test's machine, which
// calls the node API back over the agent's and the updater's tunnels.

// eventuallyHealthy waits for agent to report every component healthy,
// and returns that status.
func eventuallyHealthy(t *testing.T, agent nodev1.AgentClient) *nodev1.StatusResponse {
	t.Helper()
	var res *nodev1.StatusResponse
	nodetest.Eventually(t, time.Minute, func() error {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		var err error
		if res, err = agent.Status(ctx, &nodev1.StatusRequest{}); err != nil {
			return err
		}
		for _, c := range append(slices.Clone(res.Components), nodetest.Services(res)...) {
			if c.Health != nodev1.Health_HEALTH_HEALTHY {
				return status.Errorf(codes.Unavailable, "%s is %s: %s", c.Name, c.Health, c.Problem)
			}
		}
		return nil
	})
	return res
}

// components are the names and versions of what status reports, the
// cluster services included.
func components(res *nodev1.StatusResponse) []string {
	var got []string
	for _, c := range append(slices.Clone(res.Components), nodetest.Services(res)...) {
		got = append(got, c.Name+" "+c.Version+" "+c.Health.String())
	}
	slices.Sort(got)
	return got
}

func TestAJoinedNodeServesTheNodeAPIOverItsTunnels(t *testing.T) {
	peer := nodetest.NewTunnelPeer(t)
	n := nodetest.StartJoined(t, peer)
	agent := peer.Agent(t)
	for _, protocol := range []string{controlplanev1.ProtocolAgent, controlplanev1.ProtocolUpdater} {
		if tun := peer.Tunnel(t, protocol, time.Minute); tun.Node != nodetest.JoinedNode {
			t.Errorf("the %s tunnel proved %s, want the node it joined as, %s", protocol, tun.Node, nodetest.JoinedNode)
		}
	}
	remote := eventuallyHealthy(t, agent)
	if remote.Mode != nodev1.Mode_MODE_CONTROL_PLANE {
		t.Errorf("over the tunnel, status says mode %s, want control plane", remote.Mode)
	}
	// The local socket answers status alike.
	local, err := n.Status(t)
	if err != nil {
		t.Fatalf("status on the local socket: %v", err)
	}
	if local.Mode != remote.Mode || !slices.Equal(components(local), components(remote)) {
		t.Errorf("status on the local socket is %s %v, over the tunnel %s %v", local.Mode, components(local), remote.Mode, components(remote))
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	updater, err := peer.Updater(t).Status(ctx, &nodev1.UpdaterStatusRequest{})
	if err != nil || updater.Updater.GetVersion() != nodetest.Version || updater.Updater.GetHealth() != nodev1.Health_HEALTH_HEALTHY {
		t.Errorf("the updater's status over its tunnel: %v, %v, want it healthy at %s", updater, err, nodetest.Version)
	}

	// The lock and the generation, as mooctl uses them on a direct node.
	lock := &nodev1.Lock{Lease: "cp", Holder: "the tunnel peer", Machine: "test"}
	got, err := agent.AcquireLock(ctx, &nodev1.AcquireLockRequest{Lock: lock, Ttl: durationpb.New(time.Minute)})
	if err != nil || got.Lock.GetLease() != "cp" {
		t.Fatalf("acquiring the lock over the tunnel: %v, %v", got, err)
	}
	if _, err := agent.RecordGeneration(ctx, &nodev1.RecordGenerationRequest{Generation: 3, Nodes: []string{"node-1"}, Lease: "other"}); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("recording a generation over the tunnel without the lock: %v, want failed precondition", err)
	}
	if _, err := agent.RecordGeneration(ctx, &nodev1.RecordGenerationRequest{Generation: 3, Nodes: []string{"node-1"}, Lease: "cp"}); err != nil {
		t.Fatalf("recording a generation over the tunnel: %v", err)
	}
	if _, err := agent.ReleaseLock(ctx, &nodev1.ReleaseLockRequest{Lease: "cp"}); err != nil {
		t.Fatalf("releasing the lock over the tunnel: %v", err)
	}
	res, err := agent.Status(ctx, &nodev1.StatusRequest{})
	if err != nil || res.Generation != 3 || res.Lock != nil {
		t.Errorf("after the lock and the generation over the tunnel, status is %v, %v, want generation 3 and the lock free", res, err)
	}

	// The control plane runs a cluster service, and calls it. An import
	// without the lease is refused over the tunnel too.
	if _, err := nodeclient.ImportImage(ctx, agent, nodetest.ImageArchive(t, "dns"), "other"); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("importing dns's image over the tunnel without the lock: %v, want failed precondition", err)
	}
	images, err := importOver(ctx, agent, nodetest.ImageArchive(t, "dns"))
	if err != nil {
		t.Fatalf("importing dns's image over the tunnel: %v", err)
	}
	if !slices.Contains(images, nodetest.Service("dns").Image) {
		t.Fatalf("importing dns's image over the tunnel imported %v", images)
	}
	if _, err := runOver(ctx, agent, nodetest.Service("dns")); err != nil {
		t.Fatalf("running dns over the tunnel: %v", err)
	}
	eventuallyHealthy(t, agent)
	conn := peer.Tunnel(t, controlplanev1.ProtocolAgent, time.Minute).Conn
	echo, err := call(ctx, conn, nodetest.Service("dns").Api, "Echo", []byte("from the control plane"))
	if err != nil {
		t.Fatalf("Echo on dns over the tunnel: %v", err)
	}
	if string(echo.Payload) != "from the control plane" || echo.CallerKind != "CALLER_KIND_CONTROL_PLANE" {
		t.Errorf("Echo on dns over the tunnel answered %q from caller %q, want it from the control plane", echo.Payload, echo.CallerKind)
	}
}

// lockedOut checks that a call of each of desc's methods on conn, but
// those in allowed, fails with FAILED_PRECONDITION, saying the node is
// in control plane mode.
func lockedOut(t *testing.T, conn *grpc.ClientConn, desc grpc.ServiceDesc, allowed ...string) {
	t.Helper()
	for _, m := range desc.Methods {
		if slices.Contains(allowed, m.MethodName) {
			continue
		}
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		err := conn.Invoke(ctx, "/"+desc.ServiceName+"/"+m.MethodName, &emptypb.Empty{}, &emptypb.Empty{})
		cancel()
		wantLockedOut(t, desc.ServiceName+"/"+m.MethodName, err)
	}
	for _, s := range desc.Streams {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		stream, err := conn.NewStream(ctx, &grpc.StreamDesc{ClientStreams: s.ClientStreams, ServerStreams: s.ServerStreams}, "/"+desc.ServiceName+"/"+s.StreamName)
		if err == nil {
			stream.CloseSend()
			err = stream.RecvMsg(&emptypb.Empty{})
		}
		cancel()
		wantLockedOut(t, desc.ServiceName+"/"+s.StreamName, err)
	}
}

func wantLockedOut(t *testing.T, what string, err error) {
	t.Helper()
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "control plane mode") || !strings.Contains(err.Error(), "agent detach") {
		t.Errorf("%s on the local socket of a joined node: %v, want failed precondition for control plane mode", what, err)
	}
}

func TestAJoinedNodesLocalSocketsAnswerStatusAlone(t *testing.T) {
	peer := nodetest.NewTunnelPeer(t)
	n := nodetest.StartJoined(t, peer)
	agent := peer.Agent(t)
	eventuallyHealthy(t, agent)
	// A cluster service runs, run by the control plane.
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	if _, err := importOver(ctx, agent, nodetest.ImageArchive(t, "dns")); err != nil {
		t.Fatal(err)
	}
	if _, err := runOver(ctx, agent, nodetest.Service("dns")); err != nil {
		t.Fatal(err)
	}
	eventuallyHealthy(t, agent)

	if res := n.EventuallyHealthy(t); res.Mode != nodev1.Mode_MODE_CONTROL_PLANE {
		t.Errorf("status on the local socket says mode %s, want control plane", res.Mode)
	}
	if res, err := n.UpdaterStatus(t); err != nil || res.Updater.GetVersion() != nodetest.Version {
		t.Errorf("the updater's status on its local socket: %v, %v", res, err)
	}
	lockedOut(t, n.AgentConn(t), nodev1.Agent_ServiceDesc, "Status", "Detach")
	lockedOut(t, n.UpdaterConn(t), nodev1.Updater_ServiceDesc, "Status")
	_, err := call(t.Context(), n.AgentConn(t), nodetest.Service("dns").Api, "Echo", []byte("from an operator"))
	wantLockedOut(t, "Echo on dns", err)
	// agent join refuses a bad token, and a node that has a key already.
	for _, c := range []struct{ token, want string }{
		{"nonsense", "moostack-join:"},
		{"", "moostack-join:"},
		{peer.Token(t), "exists"},
	} {
		out, err := n.Output("root", "sh", "-c", "printf '%s\\n' \""+c.token+"\" | "+hostlayout.Agent+" join")
		if err == nil || !strings.Contains(out, c.want) {
			t.Errorf("agent join with %q on a joined node: %v\n%s\nwant it refused, saying %s", c.token, err, out, c.want)
		}
	}
	// Nothing changed: the lock is free and no generation recorded.
	if res, err := n.Status(t); err != nil || res.Lock != nil || res.Generation != 0 {
		t.Errorf("after the refused calls, status is %v, %v, want the lock free and no generation", res, err)
	}
}

// tunnelStatus calls status over tun, of protocol, within 10 s.
func tunnelStatus(t *testing.T, protocol string, tun *nodetest.Tunnel) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	var err error
	if protocol == controlplanev1.ProtocolAgent {
		_, err = nodev1.NewAgentClient(tun.Conn).Status(ctx, &nodev1.StatusRequest{})
	} else {
		_, err = nodev1.NewUpdaterClient(tun.Conn).Status(ctx, &nodev1.UpdaterStatusRequest{})
	}
	return err
}

func TestAJoinedNodesTunnelsStayUpIdleAndComeBackOnceDropped(t *testing.T) {
	peer := nodetest.NewTunnelPeer(t)
	nodetest.StartJoined(t, peer)
	eventuallyHealthy(t, peer.Agent(t))
	protocols := []string{controlplanev1.ProtocolAgent, controlplanev1.ProtocolUpdater}
	tunnels := map[string]*nodetest.Tunnel{}
	for _, protocol := range protocols {
		tunnels[protocol] = peer.Tunnel(t, protocol, time.Minute)
	}
	// Idle, past the bound on starting HTTP/2, the tunnels stay up.
	time.Sleep(15 * time.Second)
	for _, protocol := range protocols {
		if next, err := peer.NextTunnel(protocol, tunnels[protocol], time.Second); err == nil {
			t.Errorf("the idle %s tunnel was dialled again, as %v", protocol, next)
		}
		if err := tunnelStatus(t, protocol, tunnels[protocol]); err != nil {
			t.Errorf("status over the idle %s tunnel: %v", protocol, err)
		}
	}
	for _, protocol := range protocols {
		tun := tunnels[protocol]
		tun.Close()
		if err := tunnelStatus(t, protocol, tun); err == nil {
			t.Errorf("the dropped %s tunnel still answers", protocol)
		}
		next, err := peer.NextTunnel(protocol, tun, 30*time.Second)
		if err != nil {
			t.Fatalf("after the peer dropped the %s tunnel: %v", protocol, err)
		}
		if err := tunnelStatus(t, protocol, next); err != nil {
			t.Errorf("status over the %s tunnel dialled again: %v", protocol, err)
		}
	}
}

func TestUpdatesOverTheTunnelsKeepAJoinedNodeInControlPlaneMode(t *testing.T) {
	peer := nodetest.NewTunnelPeer(t)
	n := nodetest.StartJoined(t, peer)
	eventuallyHealthy(t, peer.Agent(t))
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()

	// The control plane updates the agent through the updater.
	agentTunnel := peer.Tunnel(t, controlplanev1.ProtocolAgent, time.Minute)
	if err := nodeclient.UpdateAgent(ctx, peer.Updater(t), nodetest.NextVersion, nodetest.NextAgent(t), time.Minute); err != nil {
		t.Fatalf("updating the agent over the updater's tunnel: %v", err)
	}
	next, err := peer.NextTunnel(controlplanev1.ProtocolAgent, agentTunnel, time.Minute)
	if err != nil {
		t.Fatalf("the updated agent: %v", err)
	}
	res := eventuallyHealthy(t, nodev1.NewAgentClient(next.Conn))
	if res.Mode != nodev1.Mode_MODE_CONTROL_PLANE || res.Components[0].GetVersion() != nodetest.NextVersion {
		t.Errorf("over its new tunnel, the updated agent says mode %s and %v, want control plane mode at %s", res.Mode, components(res), nodetest.NextVersion)
	}

	// And the updater through the agent.
	updaterTunnel := peer.Tunnel(t, controlplanev1.ProtocolUpdater, time.Minute)
	if err := nodeclient.UpdateUpdater(ctx, nodev1.NewAgentClient(next.Conn), nodetest.NextVersion, nodetest.NextUpdater(t), time.Minute); err != nil {
		t.Fatalf("updating the updater over the agent's tunnel: %v", err)
	}
	nextUpdater, err := peer.NextTunnel(controlplanev1.ProtocolUpdater, updaterTunnel, time.Minute)
	if err != nil {
		t.Fatalf("the updated updater: %v", err)
	}
	ures, err := nodev1.NewUpdaterClient(nextUpdater.Conn).Status(ctx, &nodev1.UpdaterStatusRequest{})
	if err != nil || ures.Mode != nodev1.Mode_MODE_CONTROL_PLANE || ures.Updater.GetVersion() != nodetest.NextVersion {
		t.Errorf("over its new tunnel, the updated updater says %v, %v, want control plane mode at %s", ures, err, nodetest.NextVersion)
	}
	// The local sockets are still locked out.
	lockedOut(t, n.AgentConn(t), nodev1.Agent_ServiceDesc, "Status", "Detach")
	lockedOut(t, n.UpdaterConn(t), nodev1.Updater_ServiceDesc, "Status")
}

// warningOrWorse matches a line of a program's stderr that the journal
// logs at warning level or a more severe one.
var warningOrWorse = regexp.MustCompile(`(?m)^<[0-4]>.*detach`)

// onTerminal runs args as root in the node with a terminal, answering
// answer to what it asks.
func onTerminal(n *nodetest.Node, answer string, args ...string) (string, error) {
	return n.Output("root", "sh", "-c", "printf '%s\\n' \""+answer+"\" | script -qec '"+strings.Join(args, " ")+"' /dev/null")
}

// checkDirect checks, when, that the node is in direct mode, its agent's
// and its updater's local sockets taking mutating calls, and that
// neither dials the peer again in 5 s, after the tunnels last.
func checkDirect(t *testing.T, n *nodetest.Node, peer *nodetest.TunnelPeer, last map[string]*nodetest.Tunnel, when string) {
	t.Helper()
	res := n.EventuallyHealthy(t)
	if res.Mode != nodev1.Mode_MODE_DIRECT {
		t.Errorf("%s, status says mode %s, want direct", when, res.Mode)
	}
	lease := "op-" + strings.ReplaceAll(when, " ", "-")
	if got := acquire(t, n, lease, time.Minute); got.GetLease() != lease {
		t.Errorf("%s, acquiring the lock on the local socket: %v", when, got)
	}
	releaseLock(t, n, lease)
	// The updater follows the node file.
	nodetest.Eventually(t, 30*time.Second, func() error {
		res, err := n.UpdaterStatus(t)
		if err != nil {
			return err
		}
		if res.Mode != nodev1.Mode_MODE_DIRECT {
			return fmt.Errorf("%s, the updater says mode %s, want direct", when, res.Mode)
		}
		return nil
	})
	// It takes an update of the agent again: one without a binary gets
	// past the lockout to the update itself.
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	stream, err := n.Updater.UpdateAgent(ctx)
	if err == nil {
		_, err = stream.CloseAndRecv()
	}
	if err == nil || status.Code(err) == codes.Unavailable || strings.Contains(err.Error(), "control plane mode") {
		t.Errorf("%s, an update of the agent without a binary on the updater's socket: %v, want it past the lockout and refused", when, err)
	}
	for protocol, tun := range last {
		if _, err := peer.NextTunnel(protocol, tun, 5*time.Second); err == nil {
			t.Errorf("%s, the %s dialled the tunnel peer again", when, protocol)
		}
		if err := tunnelStatus(t, protocol, tun); err == nil {
			t.Errorf("%s, the %s still serves its old tunnel", when, protocol)
		}
	}
}

// tunnels are the tunnels the peer accepted last, by protocol.
func tunnels(t *testing.T, peer *nodetest.TunnelPeer) map[string]*nodetest.Tunnel {
	t.Helper()
	return map[string]*nodetest.Tunnel{
		controlplanev1.ProtocolAgent:   peer.Tunnel(t, controlplanev1.ProtocolAgent, time.Minute),
		controlplanev1.ProtocolUpdater: peer.Tunnel(t, controlplanev1.ProtocolUpdater, time.Minute),
	}
}

func TestBreakGlassDetachReturnsAJoinedNodeToDirectMode(t *testing.T) {
	peer := nodetest.NewTunnelPeer(t)
	n := nodetest.StartJoined(t, peer)
	agent := peer.Agent(t)
	eventuallyHealthy(t, agent)
	last := tunnels(t, peer)

	// The control plane can't detach the node.
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	if _, err := agent.Detach(ctx, &nodev1.DetachRequest{}); status.Code(err) != codes.PermissionDenied {
		t.Errorf("detaching over the tunnel: %v, want permission denied", err)
	}
	// Nor can a user other than root on the node, who isn't asked.
	if out, err := n.Output("nobody", hostlayout.Agent, "detach", "--yes"); err == nil || !strings.Contains(out, "permission denied") || !strings.Contains(out, "as root") {
		t.Errorf("agent detach as nobody: %v\n%s\nwant permission denied, and to run it as root", err, out)
	}
	if out, err := n.Output("nobody", "script", "-qec", hostlayout.Agent+" detach", "/dev/null"); err == nil || !strings.Contains(out, "as root") || strings.Contains(out, "for good?") {
		t.Errorf("agent detach as nobody on a terminal: %v\n%s\nwant it refused before the question", err, out)
	}
	// Nor root without confirming it.
	if out, err := n.Output("root", hostlayout.Agent, "detach"); err == nil || !strings.Contains(out, "--yes") {
		t.Errorf("agent detach without a terminal or --yes: %v\n%s\nwant it refused, naming --yes", err, out)
	}
	out, err := onTerminal(n, "n", hostlayout.Agent, "detach")
	if err == nil || !strings.Contains(out, "for good?") || !strings.Contains(out, peer.Address) || !strings.Contains(out, "not detached") {
		t.Errorf("agent detach answered no on a terminal: %v\n%s\nwant the question, naming %s, and no detach", err, out, peer.Address)
	}
	// The answer is the terminal's, not stdin's.
	out, err = onTerminal(n, "n", "echo y |", hostlayout.Agent, "detach")
	if err == nil || !strings.Contains(out, "not detached") {
		t.Errorf("agent detach with yes on stdin, answered no on the terminal: %v\n%s\nwant no detach", err, out)
	}
	// Ending the terminal's input answers no.
	if out, err := n.Output("root", "sh", "-c", "script -qec '"+hostlayout.Agent+" detach' /dev/null </dev/null"); err == nil || !strings.Contains(out, "not detached") {
		t.Errorf("agent detach with the terminal's input ended: %v\n%s\nwant no detach", err, out)
	}
	if res, err := n.Status(t); err != nil || res.Mode != nodev1.Mode_MODE_CONTROL_PLANE {
		t.Fatalf("after the refused detaches, status is %v, %v, want control plane mode", res, err)
	}
	if log, _ := n.Output("root", "cat", "/var/log/agent.log"); warningOrWorse.MatchString(log) {
		t.Errorf("the agent logged a detach before one:\n%s", log)
	}

	// Root on the node detaches it, confirming on the terminal. Once agent
	// detach says so, the updater has followed: its log says so at once.
	out, err = onTerminal(n, "y", hostlayout.Agent, "detach", "&& grep -c was.detached.from.its.control.plane /var/log/updater.log")
	if err != nil {
		t.Fatalf("agent detach as root, answered yes: %v\n%s", err, out)
	}
	if !strings.HasSuffix(strings.TrimSpace(out), "\n1") {
		t.Errorf("agent detach printed\n%s\nwant the updater to have followed by when it returned", out)
	}
	if !strings.Contains(out, "direct mode") {
		t.Errorf("agent detach printed %q, want it to say the node is in direct mode", out)
	}
	// Once agent detach says so, the updater serves in direct mode too.
	if res, err := n.UpdaterStatus(t); err != nil || res.Mode != nodev1.Mode_MODE_DIRECT {
		t.Errorf("the updater's status once agent detach returned: %v, %v, want direct mode", res, err)
	}
	if log, _ := n.Output("root", "cat", "/var/log/agent.log"); !warningOrWorse.MatchString(log) {
		t.Errorf("the agent logged\n%s\nwant the detach at warning level or worse", log)
	}
	checkDirect(t, n, peer, last, "once detached")
	if node, err := n.Output("root", "stat", "-c", "%a", hostlayout.NodeFile); err != nil || strings.TrimSpace(node) != "644" {
		t.Errorf("the detached node's file has mode %q, %v, want 644, as node add writes it", node, err)
	}
	if node, err := n.Output("root", "cat", hostlayout.NodeFile); err != nil || !strings.Contains(node, "mode: direct\n") || strings.Contains(node, "control_plane") {
		t.Errorf("the detached node's file is %q, %v, want direct mode without a control plane", node, err)
	}

	// The node stays detached across a restart of the agent and of the
	// updater.
	n.Restart(t, "agent")
	n.Restart(t, "updater")
	checkDirect(t, n, peer, last, "after the agent and the updater restarted")

	// A node in direct mode isn't detached again.
	if out, err := n.Output("root", hostlayout.Agent, "detach", "--yes"); err == nil || !strings.Contains(out, "direct mode already") {
		t.Errorf("agent detach on a node in direct mode: %v\n%s\nwant it refused", err, out)
	}
	// Asked on a terminal, it is refused before the question.
	if out, err := onTerminal(n, "y", hostlayout.Agent, "detach"); err == nil || !strings.Contains(out, "direct mode already") || strings.Contains(out, "for good?") {
		t.Errorf("agent detach on a terminal, on a node in direct mode: %v\n%s\nwant it refused before the question", err, out)
	}
}

// A detach cut off once the node file says direct mode, as by a crash
// of the agent, ends with the node in direct mode all the same: the
// agent and the updater both follow the node file.
func TestADetachCutOffAfterTheNodeFileEndsInDirectMode(t *testing.T) {
	peer := nodetest.NewTunnelPeer(t)
	n := nodetest.StartJoined(t, peer)
	eventuallyHealthy(t, peer.Agent(t))
	last := tunnels(t, peer)
	node, err := n.Output("root", "cat", hostlayout.NodeFile)
	if err != nil {
		t.Fatal(err)
	}
	direct := regexp.MustCompile(`(?m)^control_plane: .*\n`).ReplaceAllString(strings.Replace(node, "mode: control_plane", "mode: direct", 1), "")
	n.WriteConfig(t, hostlayout.NodeFile, []byte(direct))
	n.Crash(t, "agent")
	checkDirect(t, n, peer, last, "after the cut-off detach")
}

// The control plane being gone for good is what a detach is for.
func TestDetachWorksWithTheControlPlaneGone(t *testing.T) {
	peer := nodetest.NewTunnelPeer(t)
	n := nodetest.StartJoined(t, peer)
	eventuallyHealthy(t, peer.Agent(t))
	last := tunnels(t, peer)
	peer.Stop()
	for protocol, tun := range last {
		if err := tunnelStatus(t, protocol, tun); err == nil {
			t.Fatalf("the %s tunnel still answers once the peer stopped", protocol)
		}
	}
	if out, err := n.Output("root", hostlayout.Agent, "detach", "--yes"); err != nil {
		t.Fatalf("agent detach --yes with the control plane gone: %v\n%s", err, out)
	}
	checkDirect(t, n, peer, nil, "detached with the control plane gone")
}

func TestDetachingANodeInDirectModeIsRefused(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if _, err := n.Agent.Detach(ctx, &nodev1.DetachRequest{}); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("detaching a node in direct mode: %v, want failed precondition", err)
	}
	if log, _ := n.Output("root", "cat", "/var/log/agent.log"); warningOrWorse.MatchString(log) {
		t.Errorf("the agent logged a detach it refused:\n%s", log)
	}
}

// A detach while the control plane updates the agent through the
// updater ends at once, and the node in direct mode, whatever becomes of
// the update.
func TestADetachDuringAnUpdateOfTheAgentEndsInDirectMode(t *testing.T) {
	peer := nodetest.NewTunnelPeer(t)
	n := nodetest.StartJoined(t, peer)
	eventuallyHealthy(t, peer.Agent(t))
	last := tunnels(t, peer)
	bin := nodetest.NextAgent(t)
	sum := sha256.Sum256(bin)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	stream, err := peer.Updater(t).UpdateAgent(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Half the binary is sent, and the update waits for the rest.
	half := len(bin) / 2
	if err := stream.Send(&nodev1.UpdateAgentRequest{Update: &nodev1.AgentUpdate{Version: nodetest.NextVersion, Sha256: hex.EncodeToString(sum[:]), HealthTimeout: durationpb.New(time.Minute)}, Chunk: bin[:half]}); err != nil {
		t.Fatal(err)
	}
	// The detach starts, and the rest of the binary comes while it runs,
	// so that the update would restart the agent during it.
	type result struct {
		out  string
		err  error
		took time.Duration
	}
	done := make(chan result, 1)
	go func() {
		start := time.Now()
		out, err := n.Output("root", hostlayout.Agent, "detach", "--yes")
		done <- result{out, err, time.Since(start)}
	}()
	time.Sleep(500 * time.Millisecond)
	stream.Send(&nodev1.UpdateAgentRequest{Chunk: bin[half:]})
	stream.CloseAndRecv()
	if r := <-done; r.err != nil || r.took > 5*time.Second {
		t.Errorf("agent detach during an update of the agent: %v after %s\n%s", r.err, r.took, r.out)
	}
	checkDirect(t, n, peer, last, "detached during an update")
}

// The control plane runs and removes containers over its tunnel, under
// the operation lock, as mooctl does on the local socket, and can't pull
// without it.
func TestTheControlPlaneRunsAndRemovesContainersOverTheTunnel(t *testing.T) {
	peer := nodetest.NewTunnelPeer(t)
	nodetest.StartJoined(t, peer)
	agent := peer.Agent(t)
	eventuallyHealthy(t, agent)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	if _, err := agent.AcquireLock(ctx, &nodev1.AcquireLockRequest{Lock: &nodev1.Lock{Lease: "cp", Holder: "control plane"}, Ttl: durationpb.New(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	if _, err := nodeclient.ImportImage(ctx, agent, nodetest.ImageArchive(t, "dns"), "cp"); err != nil {
		t.Fatal(err)
	}
	dns := nodetest.Service("dns")
	nobody := "65534:65534"
	c := &nodev1.Container{Name: "service-dns", Image: dns.Image, User: &nobody, ReadOnlyRoot: true, Capabilities: &nodev1.Capabilities{}, Api: dns.Api}
	for _, lease := range []string{"", "other"} {
		if _, err := agent.PullImage(ctx, &nodev1.PullImageRequest{Reference: "registry.test/web:1", Lease: lease}); status.Code(err) != codes.FailedPrecondition {
			t.Errorf("pulling an image over the tunnel for the lease %q, which doesn't hold the lock: %v, want failed precondition", lease, err)
		}
	}
	if _, err := agent.Run(ctx, &nodev1.RunRequest{Container: c, Lease: "other"}); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("running a container over the tunnel without the lock: %v, want failed precondition", err)
	}
	if _, err := agent.Run(ctx, &nodev1.RunRequest{Container: c, Lease: "cp"}); err != nil {
		t.Fatalf("running a container over the tunnel: %v", err)
	}
	conn := peer.Tunnel(t, controlplanev1.ProtocolAgent, time.Minute).Conn
	nodetest.Eventually(t, 30*time.Second, func() error {
		_, err := call(ctx, conn, dns.Api, "Echo", nil)
		return err
	})
	if _, err := agent.Remove(ctx, &nodev1.RemoveRequest{Name: "service-dns", Lease: "other"}); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("removing a container over the tunnel without the lock: %v, want failed precondition", err)
	}
	if _, err := agent.Remove(ctx, &nodev1.RemoveRequest{Name: "service-dns", Lease: "cp"}); err != nil {
		t.Fatalf("removing a container over the tunnel: %v", err)
	}
	res, err := agent.Status(ctx, &nodev1.StatusRequest{})
	if err != nil || len(res.Containers) != 0 {
		t.Errorf("status over the tunnel after the removal: %v, %v, want no containers", res.GetContainers(), err)
	}
}

// importOver has agent import the image archive data, holding the
// operation lock for the call, as runOver runs a container.
func importOver(ctx context.Context, agent nodev1.AgentClient, data []byte) ([]string, error) {
	if _, err := agent.AcquireLock(ctx, &nodev1.AcquireLockRequest{Lock: &nodev1.Lock{Lease: "cp", Holder: "control plane"}, Ttl: durationpb.New(time.Minute)}); err != nil {
		return nil, err
	}
	images, err := nodeclient.ImportImage(ctx, agent, data, "cp")
	if err != nil {
		return nil, err
	}
	_, err = agent.ReleaseLock(ctx, &nodev1.ReleaseLockRequest{Lease: "cp"})
	return images, err
}

// runOver has the agent run c over the tunnel agent, holding the
// operation lock for a lease of the control plane's while it does, as the
// control plane does.
func runOver(ctx context.Context, agent nodev1.AgentClient, c *nodev1.Container) (*nodev1.RunResponse, error) {
	if _, err := agent.AcquireLock(ctx, &nodev1.AcquireLockRequest{Lock: &nodev1.Lock{Lease: "cp", Holder: "control plane"}, Ttl: durationpb.New(time.Minute)}); err != nil {
		return nil, err
	}
	res, err := agent.Run(ctx, &nodev1.RunRequest{Container: c, Lease: "cp"})
	if err != nil {
		return nil, err
	}
	_, err = agent.ReleaseLock(ctx, &nodev1.ReleaseLockRequest{Lease: "cp"})
	return res, err
}
