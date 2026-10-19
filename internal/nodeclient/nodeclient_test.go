package nodeclient_test

import (
	"testing"

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
