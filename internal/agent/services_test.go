//go:build linux

package agent_test

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/containerd/containerd/v2/core/leases"
	"github.com/containerd/containerd/v2/defaults"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/errdefs"
	"github.com/opencontainers/image-spec/identity"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	servicev1 "github.com/moostackhq/moostack/api/service/v1"
	"github.com/moostackhq/moostack/internal/hostlayout"
	"github.com/moostackhq/moostack/internal/nodetest"
	"github.com/moostackhq/moostack/internal/release"
)

// These tests run the cluster services as the agent runs them, in the
// node's containerd, and call them through the agent's socket, as an
// operator does.

func TestTheClusterServicesRunAndReportHealthy(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	status := n.StartServices(t)
	got := map[string]string{}
	for _, s := range status.Services {
		got[s.Name] = s.Version + " " + s.Health.String()
	}
	want := map[string]string{}
	for _, name := range []string{"registry", "dns", "proxy", "telemetry"} {
		want[name] = nodetest.Version + " HEALTH_HEALTHY"
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("the services are %v, want %v", got, want)
	}
}

func TestAnOperatorsCallReachesEachServiceByItsAPI(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.StartServices(t)
	conn := n.AgentConn(t)
	for _, s := range release.ClusterServices {
		payload := []byte("to " + s.Name)
		// What the caller claims about itself never reaches the service.
		ctx := metadata.AppendToOutgoingContext(t.Context(),
			servicev1.CallerKindKey, "CALLER_KIND_PEER_NODE",
			servicev1.CallerNodeKey, "node-9")
		res, err := call(ctx, conn, s.API, "Echo", payload)
		if err != nil {
			t.Errorf("Echo on %s: %v", s.Name, err)
			continue
		}
		if !bytes.Equal(res.Payload, payload) || res.CallerKind != "CALLER_KIND_OPERATOR" || res.CallerNode != "" {
			t.Errorf("Echo on %s answered %q from caller %q of node %q, want %q from an operator of no node", s.Name, res.Payload, res.CallerKind, res.CallerNode, payload)
		}
	}
}

func TestAMethodAnOperatorIsntAllowedIsRefusedAndTheServiceNeverSeesIt(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.StartServices(t)
	conn := n.AgentConn(t)
	api := nodetest.Service("dns").Api
	if res, err := call(t.Context(), conn, api, "Echo", nil); err != nil || res.Calls != 1 {
		t.Fatalf("the first Echo: %v, %v", res, err)
	}
	if _, err := call(t.Context(), conn, api, "Ping", nil); status.Code(err) != codes.PermissionDenied {
		t.Errorf("an operator's Ping: %v, want permission denied", err)
	}
	// The service counts every Echo and Ping it receives.
	if res, err := call(t.Context(), conn, api, "Echo", nil); err != nil || res.Calls != 2 {
		t.Errorf("the Echo after a refused Ping: %v, %v, want the service's second call", res, err)
	}
}

func TestAStoppedServiceAnswersUnavailableAtOnce(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.StartServices(t)
	conn := n.AgentConn(t)
	api := nodetest.Service("dns").Api
	if _, err := call(t.Context(), conn, api, "Echo", nil); err != nil {
		t.Fatal(err)
	}
	killService(t, n, "dns")
	start := time.Now()
	_, err := call(t.Context(), conn, api, "Echo", nil)
	if status.Code(err) != codes.Unavailable {
		t.Errorf("Echo on a stopped service: %v, want unavailable", err)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Errorf("Echo on a stopped service took %s", took)
	}
	s, err := n.Status(t)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range s.Services {
		switch {
		case c.Name == "dns" && (c.Health != nodev1.Health_HEALTH_UNHEALTHY || c.Problem == ""):
			t.Errorf("the stopped dns is %s with problem %q", c.Health, c.Problem)
		case c.Name != "dns" && c.Health != nodev1.Health_HEALTH_HEALTHY:
			t.Errorf("%s is %s while dns is stopped: %s", c.Name, c.Health, c.Problem)
		}
	}
}

func TestACallOfAnAPIThatNoServiceServesIsUnimplemented(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.StartServices(t)
	if _, err := call(t.Context(), n.AgentConn(t), "moostack.nothing.v1.Nothing", "Echo", nil); status.Code(err) != codes.Unimplemented {
		t.Errorf("a call of an API no service serves: %v, want unimplemented", err)
	}
}

func TestTheServicesKeepServingAcrossAnAgentRestart(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.StartServices(t)
	api := nodetest.Service("registry").Api
	if _, err := call(t.Context(), n.AgentConn(t), api, "Echo", nil); err != nil {
		t.Fatal(err)
	}
	n.Restart(t, "agent")
	n.EventuallyHealthy(t)
	// The same process answers: it counts the call before the restart.
	if res, err := call(t.Context(), n.AgentConn(t), api, "Echo", nil); err != nil || res.Calls != 2 {
		t.Errorf("Echo after the agent restarted: %v, %v, want the service's second call", res, err)
	}
}

func TestTheAgentStartsTheServicesThatArentRunningWhenItStarts(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.StartServices(t)
	// As after the node rebooted: the services are gone, and the agent
	// starts again.
	n.Kill(t, "agent")
	for _, s := range release.ClusterServices {
		killService(t, n, s.Name)
	}
	n.Restart(t, "agent")
	status := n.EventuallyHealthy(t)
	if len(status.Services) != len(release.ClusterServices) {
		t.Errorf("the agent reports %d services after it started again", len(status.Services))
	}
	for _, s := range release.ClusterServices {
		if res, err := call(t.Context(), n.AgentConn(t), s.API, "Echo", nil); err != nil || res.Calls != 1 {
			t.Errorf("Echo on %s after the agent started it again: %v, %v", s.Name, res, err)
		}
	}
}

// The agent starts a service from the container it has, so that one
// whose image is gone, as after someone cleaned the node's images up by
// hand, runs on.
func TestTheAgentStartsAServiceWhoseImageIsGoneFromItsContainer(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.StartServices(t)
	n.Kill(t, "agent")
	killService(t, n, "dns")
	n.DeleteImage(t, nodetest.Service("dns").Image)
	n.Restart(t, "agent")
	api := nodetest.Service("dns").Api
	nodetest.Eventually(t, 30*time.Second, func() error {
		if _, err := call(t.Context(), n.AgentConn(t), api, "Echo", nil); err != nil {
			s, _ := n.Status(t)
			return fmt.Errorf("dns doesn't answer: %v; status: %v", err, s.GetServices())
		}
		return nil
	})
	if _, err := n.Agent.RunService(t.Context(), &nodev1.RunServiceRequest{Service: nodetest.Service("dns")}); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("running dns again: %v, want it refused, its image gone", err)
	}
}

// At start, the agent keeps a service whose container is as stored, even
// one that doesn't answer: only running it again replaces it.
func TestTheAgentKeepsAServiceAsStoredWhenItStartsAnsweringOrNot(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.StartServices(t)
	pid := servicePID(t, n, "dns")
	n.Exec(t, "root", "sh", "-c", "kill -STOP "+pid)
	defer n.Exec(t, "root", "sh", "-c", "kill -CONT "+pid)
	n.Kill(t, "agent")
	n.Restart(t, "agent")
	nodetest.Eventually(t, 30*time.Second, func() error {
		s, err := n.Status(t)
		if err != nil {
			return err
		}
		for _, c := range s.Services {
			if c.Name == "dns" && c.Health == nodev1.Health_HEALTH_UNHEALTHY {
				return nil
			}
		}
		return fmt.Errorf("dns isn't unhealthy yet: %v", s.Services)
	})
	// The agent has started every service once by the time it answers,
	// and tries again for one that fails every 2 s.
	time.Sleep(3 * time.Second)
	if _, err := n.Output("root", "test", "-e", "/proc/"+pid); err != nil {
		t.Errorf("the agent replaced dns, which doesn't answer, when it started: pid %s is gone", pid)
	}
}

func TestAStartRetryDoesntUndoARunInBetween(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.StartServices(t)
	restartWantingAServiceItCantStart(t, n, "proxy", nil)
	n.RunService(t, "proxy")
	// Long enough for the agent to try starting the services again.
	time.Sleep(5 * time.Second)
	status, err := n.Status(t)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range status.Services {
		if c.Name == "proxy" && (c.Health != nodev1.Health_HEALTH_HEALTHY || c.Version != nodetest.Version) {
			t.Errorf("proxy is %s at %s after it was run again: %s", c.Health, c.Version, c.Problem)
		}
	}
	if _, err := call(t.Context(), n.AgentConn(t), nodetest.Service("proxy").Api, "Echo", nil); err != nil {
		t.Errorf("Echo on proxy after it was run again: %v", err)
	}
}

func TestAServiceThatCantBeReplacedRunsOnAndStatusSaysSo(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.StartServices(t)
	before := servicePID(t, n, "dns")
	dns := restartWantingAServiceItCantStart(t, n, "dns", nil)
	if dns.Version != nodetest.Version || dns.Health != nodev1.Health_HEALTH_UNHEALTHY {
		t.Errorf("dns is %s at %s, want unhealthy at the version that runs, %s", dns.Health, dns.Version, nodetest.Version)
	}
	if after := servicePID(t, n, "dns"); after != before {
		t.Errorf("dns ran as pid %s, then as %s, although its new image is missing", before, after)
	}
	if _, err := call(t.Context(), n.AgentConn(t), nodetest.Service("dns").Api, "Echo", nil); err != nil {
		t.Errorf("Echo on the dns that runs on: %v", err)
	}
}

func TestAServiceThatCantBeReplacedUnderAnotherAPIServesTheOldOneOn(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.StartServices(t)
	restartWantingAServiceItCantStart(t, n, "dns", toOther)
	if _, err := call(t.Context(), n.AgentConn(t), nodetest.Service("dns").Api, "Echo", nil); err != nil {
		t.Errorf("Echo on the API the dns that runs on serves: %v", err)
	}
	if _, err := call(t.Context(), n.AgentConn(t), otherAPI, "Echo", nil); status.Code(err) != codes.Unavailable {
		t.Errorf("a call of the API that no running service serves yet: %v, want unavailable", err)
	}
}

// otherAPI is an API that no cluster service serves.
const otherAPI = "moostack.other.v1.Other"

// toOther moves a service to otherAPI.
func toOther(s *nodev1.Service) { s.Api = otherAPI }

func TestAServiceThatStartsLaterUnderAnotherAPIIsRoutedForItAlone(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.StartServices(t)
	restartWantingAServiceItCantStart(t, n, "dns", toOther)
	wanted := nodetest.Service("dns")
	wanted.Version, wanted.Image = "9.9.9", release.ImageRef("dns", "9.9.9")
	toOther(wanted)
	n.ImportFixtureAs(t, wanted)
	// The agent tries again within a few seconds.
	nodetest.Eventually(t, 30*time.Second, func() error {
		if _, err := call(t.Context(), n.AgentConn(t), otherAPI, "Echo", nil); err != nil {
			return err
		}
		_, err := call(t.Context(), n.AgentConn(t), nodetest.Service("dns").Api, "Echo", nil)
		if !noRoute(err) {
			return fmt.Errorf("a call of the API dns served before: %v, want no route", err)
		}
		return nil
	})
}

func TestAnAPIHeldForAServiceRunAgainUnderItsOldOneIsRoutedNoMore(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.StartServices(t)
	restartWantingAServiceItCantStart(t, n, "dns", toOther)
	n.RunService(t, "dns")
	if _, err := call(t.Context(), n.AgentConn(t), otherAPI, "Echo", nil); !noRoute(err) {
		t.Errorf("a call of the API dns no longer wants: %v, want no route", err)
	}
}

// noRoute is whether err is the agent's refusal of a call of an API
// that it routes for no service.
func noRoute(err error) bool {
	s := status.Convert(err)
	return s.Code() == codes.Unimplemented && strings.Contains(s.Message(), "no cluster service serves")
}

func TestAnAPIHeldAtAgentStartIsRoutedNoMoreOnceItsServiceMovesToAnother(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.StartServices(t)
	n.Kill(t, "containerd")
	n.Kill(t, "agent")
	n.Restart(t, "agent")
	nodetest.Eventually(t, 30*time.Second, func() error {
		_, err := n.Status(t)
		return err
	})
	n.Restart(t, "containerd")
	nodetest.Eventually(t, 30*time.Second, func() error {
		st, err := n.Status(t)
		if err != nil {
			return err
		}
		for _, c := range st.Components {
			if c.Name == "containerd" && c.Health != nodev1.Health_HEALTH_HEALTHY {
				return fmt.Errorf("containerd is %s: %s", c.Health, c.Problem)
			}
		}
		return nil
	})
	moved := nodetest.Service("dns")
	moved.Version, moved.Image = "moved", release.ImageRef("dns", "moved")
	toOther(moved)
	n.ImportFixtureAs(t, moved)
	if _, err := n.Agent.RunService(t.Context(), &nodev1.RunServiceRequest{Service: moved}); err != nil {
		t.Fatal(err)
	}
	if _, err := call(t.Context(), n.AgentConn(t), nodetest.Service("dns").Api, "Echo", nil); !noRoute(err) {
		t.Errorf("a call of the API dns served before the agent started: %v, want no route", err)
	}
}

func TestRunningAServiceOfAnAPIThatAnotherStillServesIsRefused(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.StartServices(t)
	// dns is stored under otherAPI, but still runs and serves its old API.
	restartWantingAServiceItCantStart(t, n, "dns", toOther)
	n.ImportFixtures(t)
	clash := nodetest.Fixture()
	clash.Name = "clash"
	_, err := n.Agent.RunService(t.Context(), &nodev1.RunServiceRequest{Service: clash})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("running a service of the API the old dns still serves: %v, want a failed precondition", err)
	}
}

// A rerun of cluster update runs a service at its target that is
// unhealthy: the agent replaces it, rather than keeping a container whose
// task runs but doesn't answer.
func TestRunningAnUnhealthyServiceAgainReplacesIt(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.StartServices(t)
	pid := servicePID(t, n, "dns")
	n.Exec(t, "root", "sh", "-c", "kill -STOP "+pid)
	nodetest.Eventually(t, 30*time.Second, func() error {
		s, err := n.Status(t)
		if err != nil {
			return err
		}
		for _, c := range s.Services {
			if c.Name == "dns" && c.Health == nodev1.Health_HEALTH_UNHEALTHY {
				return nil
			}
		}
		return fmt.Errorf("dns isn't unhealthy yet: %v", s.Services)
	})
	if _, err := n.Agent.RunService(t.Context(), &nodev1.RunServiceRequest{Service: nodetest.Service("dns")}); err != nil {
		t.Fatalf("running the unhealthy dns again: %v", err)
	}
	if res, err := call(t.Context(), n.AgentConn(t), nodetest.Service("dns").Api, "Echo", nil); err != nil || res.Calls != 1 {
		t.Errorf("Echo on dns after it ran again: %v, %v, want a new dns's first call", res, err)
	}
	if _, err := n.Output("root", "test", "-e", "/proc/"+pid); err == nil {
		t.Errorf("the unhealthy dns runs on as pid %s", pid)
	}
}

func TestRunningAServiceAgainUnchangedKeepsItsCalls(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.ImportFixtures(t)
	fixture := nodetest.Fixture()
	if _, err := n.Agent.RunService(t.Context(), &nodev1.RunServiceRequest{Service: fixture}); err != nil {
		t.Fatal(err)
	}
	stream, err := n.AgentConn(t).NewStream(t.Context(), &grpc.StreamDesc{ClientStreams: true, ServerStreams: true}, "/"+fixture.Api+"/Stream", grpc.ForceCodecV2(rawCodec{}))
	if err != nil {
		t.Fatal(err)
	}
	for i := range 2 {
		if i == 1 {
			if _, err := n.Agent.RunService(t.Context(), &nodev1.RunServiceRequest{Service: fixture}); err != nil {
				t.Fatal(err)
			}
		}
		if err := stream.SendMsg(&frame{}); err != nil {
			t.Fatalf("sending on the call, run %d: %v", i+1, err)
		}
		if err := stream.RecvMsg(&frame{}); err != nil {
			t.Fatalf("receiving on the call, run %d: %v", i+1, err)
		}
	}
}

// restartWantingAServiceItCantStart restarts the agent with the
// service called name stored at version 9.9.9, whose image the node
// lacks, and changed by edit too unless it is nil, so that the agent
// keeps trying to start that one, and returns the service as the agent
// reports it once it says why it can't.
func restartWantingAServiceItCantStart(t *testing.T, n *nodetest.Node, name string, edit func(*nodev1.Service)) *nodev1.Component {
	t.Helper()
	n.Kill(t, "agent")
	var stored []string
	for _, s := range release.ClusterServices {
		svc := nodetest.Service(s.Name)
		if s.Name == name {
			svc.Version, svc.Image = "9.9.9", release.ImageRef(name, "9.9.9")
			if edit != nil {
				edit(svc)
			}
		}
		stored = append(stored, fmt.Sprintf(`{"name":%q,"version":%q,"api":%q,"image":%q}`, svc.Name, svc.Version, svc.Api, svc.Image))
	}
	n.Exec(t, "root", "sh", "-c", "echo '["+strings.Join(stored, ",")+"]' > "+hostlayout.ServicesFile)
	n.Restart(t, "agent")
	var reported *nodev1.Component
	nodetest.Eventually(t, time.Minute, func() error {
		status, err := n.Status(t)
		if err != nil {
			return err
		}
		for _, c := range status.Services {
			if c.Name == name && strings.Contains(c.Problem, "starting it") {
				reported = c
				return nil
			}
		}
		return fmt.Errorf("%s isn't failing to start: %v", name, status.Services)
	})
	return reported
}

func TestImportingAndRunningAServiceAgainChangesNothing(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.StartServices(t)
	api := nodetest.Service("proxy").Api
	if _, err := call(t.Context(), n.AgentConn(t), api, "Echo", nil); err != nil {
		t.Fatal(err)
	}
	n.ImportImage(t, "proxy")
	n.RunService(t, "proxy")
	if res, err := call(t.Context(), n.AgentConn(t), api, "Echo", nil); err != nil || res.Calls != 2 {
		t.Errorf("Echo after running proxy again: %v, %v, want the same service's second call", res, err)
	}
}

func TestRunningAServiceWhoseImageTheNodeHasntImportedIsRefused(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	_, err := n.Agent.RunService(t.Context(), &nodev1.RunServiceRequest{Service: nodetest.Service("dns")})
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "no image") {
		t.Errorf("running dns without its image: %v, want a failed precondition naming the image", err)
	}
}

func TestRunningAServiceOfAnotherServicesAPIIsRefused(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.StartServices(t)
	clash := nodetest.Service("dns")
	clash.Name = "dns-twin"
	_, err := n.Agent.RunService(t.Context(), &nodev1.RunServiceRequest{Service: clash})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("running a second service of dns's API: %v, want a failed precondition", err)
	}
}

func TestRunningAServiceAtAnotherVersionOrFromAnotherImageReplacesIt(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.StartServices(t)
	for _, c := range []struct {
		what string
		edit func(*nodev1.Service)
	}{
		{"another version", func(s *nodev1.Service) { s.Version = "9.9.9" }},
		{"another image", func(s *nodev1.Service) { s.Image = nodetest.Fixture().Image }},
	} {
		n.RunService(t, "dns")
		// Running dns deletes its other images, as the fixture's.
		n.ImportFixtures(t)
		before := servicePID(t, n, "dns")
		svc := nodetest.Service("dns")
		c.edit(svc)
		if _, err := n.Agent.RunService(t.Context(), &nodev1.RunServiceRequest{Service: svc}); err != nil {
			t.Fatalf("running dns from %s: %v", c.what, err)
		}
		if _, err := n.Output("root", "test", "-e", "/proc/"+before); err == nil {
			t.Errorf("dns runs on as pid %s when run from %s, rather than being replaced", before, c.what)
		}
	}
}

func TestRunningAServiceUnderAnotherAPIReplacesItAndRoutesTheOldAPINoMore(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.StartServices(t)
	before := servicePID(t, n, "dns")
	moved := nodetest.Service("dns")
	old := moved.Api
	moved.Api = otherAPI
	if _, err := n.Agent.RunService(t.Context(), &nodev1.RunServiceRequest{Service: moved}); err != nil {
		t.Fatal(err)
	}
	if _, err := call(t.Context(), n.AgentConn(t), old, "Echo", nil); status.Code(err) != codes.Unimplemented {
		t.Errorf("a call of the API dns served before: %v, want unimplemented", err)
	}
	if after := servicePID(t, n, "dns"); after == before {
		t.Errorf("dns runs on as pid %s under another API, rather than being replaced", after)
	}
}

func TestAServiceRunAgainIsAskedAnewWhatItsMethodsAllow(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.StartServices(t)
	api := nodetest.Service("dns").Api
	if _, err := call(t.Context(), n.AgentConn(t), api, "Ping", nil); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("an operator's Ping on dns: %v, want permission denied", err)
	}
	// The fixture serves the same API on the same socket, and lets
	// operators ping.
	n.ImportFixtures(t)
	if _, err := n.Agent.RunService(t.Context(), &nodev1.RunServiceRequest{Service: nodetest.Fixture()}); err != nil {
		t.Fatal(err)
	}
	if _, err := call(t.Context(), n.AgentConn(t), api, "Ping", nil); err != nil {
		t.Errorf("an operator's Ping once the service that allows it runs: %v", err)
	}
}

func TestAServicesOutputGoesToItsLog(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.ImportFixtures(t)
	if _, err := n.Agent.RunService(t.Context(), &nodev1.RunServiceRequest{Service: nodetest.Fixture()}); err != nil {
		t.Fatal(err)
	}
	want := "serving " + nodetest.Fixture().Api + "\n"
	nodetest.Eventually(t, 10*time.Second, func() error {
		log, err := n.Output("root", "cat", hostlayout.ServiceLogDir+"/dns.log")
		if err != nil {
			return err
		}
		if !strings.Contains(log, want) {
			return fmt.Errorf("dns's log is %q, want it to have %q", log, want)
		}
		return nil
	})
}

func TestAServiceThatFailsToStartAnswersUnavailable(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.ImportFixtures(t)
	if _, err := n.Agent.RunService(t.Context(), &nodev1.RunServiceRequest{Service: nodetest.Broken()}); err == nil {
		t.Fatal("running a service without a program succeeded")
	}
	if _, err := call(t.Context(), n.AgentConn(t), nodetest.Broken().Api, "Echo", nil); status.Code(err) != codes.Unavailable {
		t.Errorf("a call of a service that failed to start: %v, want unavailable", err)
	}
}

func TestTheServicesRunAsNobodyWithoutCapabilitiesOrANetwork(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.StartServices(t)
	pid := servicePID(t, n, "telemetry")
	status, err := n.Output("root", "cat", "/proc/"+pid+"/status")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"Uid:\t65534\t65534\t65534\t65534", "Gid:\t65534\t65534\t65534\t65534", "CapEff:\t0000000000000000", "CapPrm:\t0000000000000000", "CapBnd:\t0000000000000000", "NoNewPrivs:\t1"} {
		if !strings.Contains(status, want+"\n") {
			t.Errorf("the service's status has no %q:\n%s", want, status)
		}
	}
	dev, err := n.Output("root", "cat", "/proc/"+pid+"/net/dev")
	if err != nil {
		t.Fatal(err)
	}
	var interfaces []string
	for _, line := range strings.Split(dev, "\n")[2:] {
		if name, _, ok := strings.Cut(strings.TrimSpace(line), ":"); ok {
			interfaces = append(interfaces, name)
		}
	}
	if !slices.Equal(interfaces, []string{"lo"}) {
		t.Errorf("the service has the network interfaces %v, want only lo", interfaces)
	}
	mounts, err := n.Output("root", "cat", "/proc/"+pid+"/mountinfo")
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(mounts, "\n") {
		if f := strings.Fields(line); len(f) > 5 && f[4] == "/" && !strings.HasPrefix(f[5], "ro") {
			t.Errorf("the service's root is mounted %s, want read-only", f[5])
		}
	}
}

func TestOnlyRootReachesAServicesSocket(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.StartServices(t)
	socket := hostlayout.ServicesDir + "/dns/" + servicev1.APISocket
	if out, err := n.Output("root", "test", "-S", socket); err != nil {
		t.Fatalf("the service's socket isn't at %s: %v %s", socket, err, out)
	}
	// nobody is the services' own user too.
	for _, user := range []string{"65534", "1000"} {
		out, err := n.Output(user, hostlayout.Agent, "relay", socket)
		if err == nil || !strings.Contains(out, "permission denied") {
			t.Errorf("a relay to the dns socket as %s: %v, %s; want permission denied", user, err, out)
		}
	}
}

// echoResponse is a service's EchoResponse. Each service's API declares
// its own Echo messages, all with the same fields, which echo encodes and
// decodes by their numbers.
type echoResponse struct {
	Payload                []byte
	CallerKind, CallerNode string
	Calls                  int64
}

// call calls method, Echo or Ping, of api with payload through conn.
func call(ctx context.Context, conn *grpc.ClientConn, api, method string, payload []byte) (*echoResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req := protowire.AppendBytes(protowire.AppendTag(nil, 1, protowire.BytesType), payload)
	var out frame
	if err := conn.Invoke(ctx, "/"+api+"/"+method, &frame{req}, &out, grpc.ForceCodecV2(rawCodec{})); err != nil {
		return nil, err
	}
	var res echoResponse
	for b := out.data; len(b) > 0; {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		b = b[n:]
		var v []byte
		var err error
		switch {
		case num == 1 && typ == protowire.BytesType:
			res.Payload, b, err = consumeBytes(b)
		case num == 2 && typ == protowire.BytesType:
			v, b, err = consumeBytes(b)
			res.CallerKind = string(v)
		case num == 3 && typ == protowire.BytesType:
			v, b, err = consumeBytes(b)
			res.CallerNode = string(v)
		case num == 4 && typ == protowire.VarintType:
			calls, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return nil, protowire.ParseError(n)
			}
			res.Calls, b = int64(calls), b[n:]
		default:
			return nil, fmt.Errorf("an EchoResponse with field %d of type %d", num, typ)
		}
		if err != nil {
			return nil, err
		}
	}
	return &res, nil
}

// consumeBytes is the bytes field value at the start of b, and the rest
// of b after it.
func consumeBytes(b []byte) (v, rest []byte, err error) {
	v, n := protowire.ConsumeBytes(b)
	if n < 0 {
		return nil, nil, protowire.ParseError(n)
	}
	return v, b[n:], nil
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

// servicePID is the pid of the cluster service called name, in the node.
func servicePID(t *testing.T, n *nodetest.Node, name string) string {
	t.Helper()
	pids := pidsOf(t, n, name)
	if len(pids) != 1 {
		t.Fatalf("%s has the pids %v", name, pids)
	}
	return pids[0]
}

// pidsOf are the pids of the processes called name in the node.
func pidsOf(t *testing.T, n *nodetest.Node, name string) []string {
	t.Helper()
	out, err := n.Output("root", "sh", "-c", `for p in /proc/[0-9]*; do [ "$(cat $p/comm 2>/dev/null)" = "$1" ] && echo ${p#/proc/}; done; true`, "-", name)
	if err != nil {
		t.Fatalf("finding %s: %v %s", name, err, out)
	}
	return strings.Fields(out)
}

// killService kills the cluster service called name, and waits until its
// process is gone.
func killService(t *testing.T, n *nodetest.Node, name string) {
	t.Helper()
	n.Exec(t, "root", "sh", "-c", "kill -KILL "+servicePID(t, n, name))
	nodetest.Eventually(t, 10*time.Second, func() error {
		if pids := pidsOf(t, n, name); len(pids) > 0 {
			return fmt.Errorf("%s still runs as %v", name, pids)
		}
		return nil
	})
}

// A call may end while the agent replaces a service, as when mooctl is
// stopped: the agent goes on until the new service runs, rather than
// leaving the node without it.
func TestAServiceRunGoesOnWhenItsCallEnds(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.StartServices(t)
	n.ImportFixtures(t)
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if _, err := n.Agent.RunService(ctx, &nodev1.RunServiceRequest{Service: nodetest.Fixture()}); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("running dns from the fixture within 50ms: %v, want the call to end first", err)
	}
	nodetest.Eventually(t, time.Minute, func() error {
		s, err := n.Status(t)
		if err != nil {
			return err
		}
		for _, c := range s.Services {
			if c.Name == "dns" && c.Version == "fixture" && c.Health == nodev1.Health_HEALTH_HEALTHY {
				return nil
			}
		}
		return fmt.Errorf("dns isn't running from the fixture, healthy: %v", s.Services)
	})
}

// A node keeps no image of a service that a run from another image
// replaced, so that its disk doesn't grow with each release.
func TestRunningAServiceFromAnotherImageDeletesTheImageItReplaced(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.StartServices(t)
	n.ImportFixtures(t)
	if _, err := n.Agent.RunService(t.Context(), &nodev1.RunServiceRequest{Service: nodetest.Fixture()}); err != nil {
		t.Fatal(err)
	}
	_, err := n.Agent.RunService(t.Context(), &nodev1.RunServiceRequest{Service: nodetest.Service("dns")})
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "the node has no image "+nodetest.Service("dns").Image) {
		t.Errorf("running dns from the image the fixture replaced: %v, want it refused, the image gone", err)
	}
	// The other services' images stay.
	n.RunService(t, "proxy")
}

// An update stopped between importing a service's image and running it,
// or whose run failed, leaves the image unused: the service's next run
// deletes it, with the image it replaced.
func TestRunningAServiceDeletesItsImagesThatNeverRan(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.StartServices(t)
	stopped := nodetest.Service("dns")
	stopped.Version, stopped.Image = "stopped", release.ImageRef("dns", "stopped")
	n.ImportFixtureAs(t, stopped)
	n.ImportFixtures(t)
	if _, err := n.Agent.RunService(t.Context(), &nodev1.RunServiceRequest{Service: nodetest.Fixture()}); err != nil {
		t.Fatal(err)
	}
	for _, gone := range []*nodev1.Service{stopped, nodetest.Service("dns")} {
		_, err := n.Agent.RunService(t.Context(), &nodev1.RunServiceRequest{Service: gone})
		if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "the node has no image "+gone.Image) {
			t.Errorf("running dns from %s: %v, want it refused, the image gone", gone.Image, err)
		}
	}
	// The images of other services stay, the broken one's that never ran
	// too.
	n.RunService(t, "proxy")
	if _, err := n.Agent.RunService(t.Context(), &nodev1.RunServiceRequest{Service: nodetest.Broken()}); err != nil && strings.Contains(err.Error(), "the node has no image") {
		t.Errorf("running broken: %v, want its image kept", err)
	}
}

// A run goes on on the node after mooctl stops waiting for it. The next
// update's import, of another tag or of the image that run replaced, is
// kept: the run's deletion of the service's other images is over before
// the import.
func TestARunThatGoesOnKeepsTheImageTheNextUpdateImports(t *testing.T) {
	for name, next := range map[string]func(n *nodetest.Node) *nodev1.Service{
		"another tag": func(n *nodetest.Node) *nodev1.Service {
			svc := nodetest.Service("dns")
			svc.Version, svc.Image = "next", release.ImageRef("dns", "next")
			n.ImportFixtureAs(t, svc)
			return svc
		},
		"the image it replaced": func(n *nodetest.Node) *nodev1.Service {
			n.ImportImage(t, "dns")
			return nodetest.Service("dns")
		},
	} {
		t.Run(name, func(t *testing.T) {
			n := nodetest.Start(t)
			n.EventuallyHealthy(t)
			n.StartServices(t)
			slow := nodetest.Service("dns")
			slow.Version, slow.Image = "slow", release.ImageRef("dns", "slow")
			n.ImportSlowFixtureAs(t, slow)
			ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
			defer cancel()
			if _, err := n.Agent.RunService(ctx, &nodev1.RunServiceRequest{Service: slow}); status.Code(err) != codes.DeadlineExceeded {
				t.Fatalf("running dns from the slow image within 50ms: %v, want the call to end first", err)
			}
			svc := next(n)
			ctx, cancel = context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			if _, err := n.Agent.RunService(ctx, &nodev1.RunServiceRequest{Service: svc}); err != nil {
				t.Errorf("running dns from %s, imported while the stopped run went on: %v, want it run", svc.Image, err)
			}
		})
	}
}

// A service's run keeps the images of its repository that another service
// runs, and those of another repository whose name starts as its does.
func TestRunningAServiceKeepsTheImagesOtherServicesRun(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.StartServices(t)
	shared := &nodev1.Service{Name: "dnsb", Version: "shared", Api: otherAPI, Image: release.ImageRef("dns", "shared")}
	n.ImportFixtureAs(t, shared)
	if _, err := n.Agent.RunService(t.Context(), &nodev1.RunServiceRequest{Service: shared}); err != nil {
		t.Fatal(err)
	}
	prefixed := &nodev1.Service{Name: "dnsx", Version: "1", Api: "moostack.dnsx.v1.DNSX", Image: "moostack.local/dnsx:1"}
	n.ImportFixtureAs(t, prefixed)
	n.ImportFixtures(t)
	if _, err := n.Agent.RunService(t.Context(), &nodev1.RunServiceRequest{Service: nodetest.Fixture()}); err != nil {
		t.Fatal(err)
	}
	for _, kept := range []*nodev1.Service{shared, prefixed} {
		if _, err := n.Agent.RunService(t.Context(), &nodev1.RunServiceRequest{Service: kept}); err != nil && strings.Contains(err.Error(), "the node has no image") {
			t.Errorf("running %s from %s after dns ran: %v, want its image kept", kept.Name, kept.Image, err)
		}
	}
}

// A release built again under the same version names its images as the
// build before did: running a service from the new build replaces the
// one that runs from the old.
func TestRunningAServiceFromARebuildOfItsImageReplacesIt(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.StartServices(t)
	n.ImportFixtures(t)
	fixture := nodetest.Fixture()
	if _, err := n.Agent.RunService(t.Context(), &nodev1.RunServiceRequest{Service: fixture}); err != nil {
		t.Fatal(err)
	}
	before := servicePID(t, n, "fixtureservice")
	n.ImportFixtureRebuiltAs(t, fixture)
	if _, err := n.Agent.RunService(t.Context(), &nodev1.RunServiceRequest{Service: fixture}); err != nil {
		t.Fatal(err)
	}
	if _, err := n.Output("root", "test", "-e", "/proc/"+before); err == nil {
		t.Errorf("dns runs on as pid %s from the build it ran from, rather than from the rebuild", before)
	}
}

// A rebuild of a service's image that changes only its config has the
// old build's layer: running the service from it keeps that layer's
// snapshot, whenever containerd collects garbage meanwhile.
func TestRunningAServiceFromARebuildOfItsConfigAloneSurvivesGarbageCollection(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.StartServices(t)
	n.ImportFixtures(t)
	fixture := nodetest.Fixture()
	if _, err := n.Agent.RunService(t.Context(), &nodev1.RunServiceRequest{Service: fixture}); err != nil {
		t.Fatal(err)
	}
	digest, chain := imageOf(t, n, fixture.Image)
	stop := n.CollectGarbage(t)
	defer stop()
	for build := 1; build <= 5; build++ {
		before := servicePID(t, n, "fixtureservice")
		n.ImportFixtureReconfiguredAs(t, fixture, build)
		rebuilt, rebuiltChain := imageOf(t, n, fixture.Image)
		if rebuilt == digest || rebuiltChain != chain {
			t.Fatalf("rebuild %d is %s on the layers %s, want another image on the layers %s of %s", build, rebuilt, rebuiltChain, chain, digest)
		}
		digest = rebuilt
		if _, err := n.Agent.RunService(t.Context(), &nodev1.RunServiceRequest{Service: fixture}); err != nil {
			t.Fatalf("running dns from rebuild %d: %v", build, err)
		}
		if _, err := n.Output("root", "test", "-e", "/proc/"+before); err == nil {
			t.Fatalf("dns runs on as pid %s, rather than from rebuild %d", before, build)
		}
	}
}

// The snapshot of a layer that a service's rebuilds shared goes once the
// service runs from other layers, so that a node keeps no layer per
// rebuild.
func TestTheLayerOfAServicesRebuildsGoesOnceItRunsFromAnother(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.StartServices(t)
	n.ImportFixtures(t)
	fixture := nodetest.Fixture()
	if _, err := n.Agent.RunService(t.Context(), &nodev1.RunServiceRequest{Service: fixture}); err != nil {
		t.Fatal(err)
	}
	_, chain := imageOf(t, n, fixture.Image)
	n.ImportFixtureReconfiguredAs(t, fixture, 1)
	if _, err := n.Agent.RunService(t.Context(), &nodev1.RunServiceRequest{Service: fixture}); err != nil {
		t.Fatal(err)
	}
	n.ImportFixtureRebuiltAs(t, fixture)
	if _, err := n.Agent.RunService(t.Context(), &nodev1.RunServiceRequest{Service: fixture}); err != nil {
		t.Fatal(err)
	}
	stop := n.CollectGarbage(t)
	defer stop()
	client := n.Containerd(t)
	ctx := namespaces.WithNamespace(t.Context(), hostlayout.ContainerdNamespace)
	nodetest.Eventually(t, 10*time.Second, func() error {
		if _, err := client.SnapshotService(defaults.DefaultSnapshotter).Stat(ctx, chain); !errdefs.IsNotFound(err) {
			return fmt.Errorf("the snapshot %s of the rebuilds' layer: %v, want it gone", chain, err)
		}
		return nil
	})
}

// An agent killed while it unpacks a service's image, and started again,
// runs the service from it when asked again.
func TestAServiceRunsAfterTheAgentIsKilledWhileItUnpacks(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.StartServices(t)
	slow := nodetest.Service("dns")
	slow.Version, slow.Image = "slow", release.ImageRef("dns", "slow")
	n.ImportSlowFixtureAs(t, slow)
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if _, err := n.Agent.RunService(ctx, &nodev1.RunServiceRequest{Service: slow}); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("running dns from the slow image within 50ms: %v, want the call to end first", err)
	}
	n.Crash(t, "agent")
	n.EventuallyHealthy(t)
	nsCtx := namespaces.WithNamespace(t.Context(), hostlayout.ContainerdNamespace)
	img, err := n.Containerd(t).GetImage(nsCtx, slow.Image)
	if err != nil {
		t.Fatal(err)
	}
	if unpacked, err := img.IsUnpacked(nsCtx, defaults.DefaultSnapshotter); err != nil || unpacked {
		t.Fatalf("the slow image unpacked: %v, %v, want the agent killed before it was", unpacked, err)
	}
	ctx, cancel = context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	if _, err := n.Agent.RunService(ctx, &nodev1.RunServiceRequest{Service: slow}); err != nil {
		t.Fatalf("running dns from the slow image after the agent was killed unpacking it: %v", err)
	}
	if status := n.EventuallyHealthy(t); !slices.ContainsFunc(status.Services, func(c *nodev1.Component) bool { return c.Name == "dns" && c.Version == slow.Version }) {
		t.Errorf("the agent reports %v, want dns at %s", status.Services, slow.Version)
	}
}

// A run whose call names a containerd lease, as containerd's own
// clients name theirs, holds what it makes the container on in the
// agent's lease all the same, and adds nothing to the one named.
func TestARunHoldsItsSnapshotsInItsOwnLeaseWhateverLeaseItsCallNames(t *testing.T) {
	for name, existing := range map[string]bool{"one that doesn't exist": false, "one that exists": true} {
		t.Run(name, func(t *testing.T) {
			n := nodetest.Start(t)
			n.EventuallyHealthy(t)
			n.StartServices(t)
			n.ImportFixtures(t)
			client := n.Containerd(t)
			nsCtx := namespaces.WithNamespace(t.Context(), hostlayout.ContainerdNamespace)
			named := leases.Lease{ID: "none"}
			if existing {
				var err error
				if named, err = client.LeasesService().Create(nsCtx, leases.WithRandomID()); err != nil {
					t.Fatal(err)
				}
			}
			ctx := metadata.AppendToOutgoingContext(t.Context(), "containerd-lease", named.ID)
			if _, err := n.Agent.RunService(ctx, &nodev1.RunServiceRequest{Service: nodetest.Fixture()}); err != nil {
				t.Errorf("running dns with a call that names the lease %s: %v", named.ID, err)
			}
			if existing {
				if held, err := client.LeasesService().ListResources(nsCtx, named); err != nil || len(held) != 0 {
					t.Errorf("the lease the call named holds %v, %v, want nothing", held, err)
				}
			}
		})
	}
}

// imageOf is the digest of the image ref in n's containerd, and the
// chain of its layers.
func imageOf(t *testing.T, n *nodetest.Node, ref string) (digest, chain string) {
	t.Helper()
	ctx := namespaces.WithNamespace(t.Context(), hostlayout.ContainerdNamespace)
	img, err := n.Containerd(t).GetImage(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	diffIDs, err := img.RootFS(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return img.Target().Digest.String(), identity.ChainID(diffIDs).String()
}
