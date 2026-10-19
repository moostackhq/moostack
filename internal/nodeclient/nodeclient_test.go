package nodeclient_test

import (
	"testing"

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
