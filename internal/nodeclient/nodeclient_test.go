package nodeclient_test

import (
	"context"
	"testing"

	"google.golang.org/grpc"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/nodeclient"
)

func TestRelayRunsTheRelayOfTheProgramThatServesTheSocket(t *testing.T) {
	for socket, want := range map[string]string{
		"/run/moostack/agent.sock":   "/opt/moostack/bin/agent relay '/run/moostack/agent.sock'",
		"/run/moostack/updater.sock": "/opt/moostack/bin/updater relay '/run/moostack/updater.sock'",
	} {
		if got := nodeclient.Relay(socket); got != want {
			t.Errorf("Relay(%q) is %q, want %q", socket, got, want)
		}
	}
}

func TestRelayOfASocketNoProgramServesPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("Relay of a socket no program serves didn't panic")
		}
	}()
	nodeclient.Relay("/run/other.sock")
}

func TestModeNameSpellsControlPlaneModeWithASpace(t *testing.T) {
	for mode, want := range map[nodev1.Mode]string{
		nodev1.Mode_MODE_UNSPECIFIED:   "-",
		nodev1.Mode_MODE_DIRECT:        "direct",
		nodev1.Mode_MODE_CONTROL_PLANE: "control plane",
	} {
		if got := nodeclient.ModeName(mode); got != want {
			t.Errorf("ModeName(%s) is %q, want %q", mode, got, want)
		}
	}
}

// statusOf is an agent that answers status with what it was given.
type statusOf struct {
	nodev1.AgentClient
	agent *nodev1.StatusResponse
}

func (s statusOf) Status(context.Context, *nodev1.StatusRequest, ...grpc.CallOption) (*nodev1.StatusResponse, error) {
	return s.agent, nil
}

// updaterStatusOf is an updater that answers status with res.
type updaterStatusOf struct {
	nodev1.UpdaterClient
	res *nodev1.UpdaterStatusResponse
}

func (s updaterStatusOf) Status(context.Context, *nodev1.UpdaterStatusRequest, ...grpc.CallOption) (*nodev1.UpdaterStatusResponse, error) {
	return s.res, nil
}

func TestStatusSaysTheModesOfTheAgentAndTheUpdater(t *testing.T) {
	for _, c := range []struct{ agent, updater nodev1.Mode }{
		{nodev1.Mode_MODE_DIRECT, nodev1.Mode_MODE_CONTROL_PLANE},
		{nodev1.Mode_MODE_CONTROL_PLANE, nodev1.Mode_MODE_DIRECT},
		{nodev1.Mode_MODE_DIRECT, nodev1.Mode_MODE_UNSPECIFIED},
	} {
		n := &nodeclient.Node{
			Agent:   statusOf{agent: &nodev1.StatusResponse{Mode: c.agent}},
			Updater: updaterStatusOf{res: &nodev1.UpdaterStatusResponse{Updater: &nodev1.Component{Name: "updater"}, Mode: c.updater}},
		}
		s := n.Status(t.Context())
		if s.Mode != c.agent || s.UpdaterMode != c.updater {
			t.Errorf("with the agent in %s and the updater in %s, Status says %s and %s", c.agent, c.updater, s.Mode, s.UpdaterMode)
		}
	}
}
