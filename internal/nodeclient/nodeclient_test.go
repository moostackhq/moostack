package nodeclient_test

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"

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

func TestStatusQuotesWhatTheNodeReportsThatIsntPrintable(t *testing.T) {
	for _, bad := range []string{"1.0.0\nnode9  unreachable", "1.0.0\r", "\x1b[2Aok", "1.0.0‮", "a b", "a\tb"} {
		quoted := strconv.Quote(bad)
		n := &nodeclient.Node{
			Agent: statusOf{agent: &nodev1.StatusResponse{
				Mode:              nodev1.Mode_MODE_DIRECT,
				Nodes:             []string{"node1", bad},
				GenerationUnknown: bad,
				Lock:              &nodev1.Lock{Lease: "l", Holder: bad, Machine: bad, Operation: bad},
				Components:        []*nodev1.Component{{Name: "agent", Version: bad, Build: bad, Problem: bad}},
				Services:          []*nodev1.Component{{Name: bad, Version: "1.0.0", Problem: "it is stuck"}},
			}},
			Updater: updaterStatusOf{res: &nodev1.UpdaterStatusResponse{Updater: &nodev1.Component{Name: "updater", Version: bad}}},
		}
		s := n.Status(t.Context())
		agent, updater, service := s.Components[0], s.Components[1], s.Services[0]
		for what, got := range map[string]string{
			"the node's name": s.Nodes[1], "why the generation is unknown": s.GenerationUnknown,
			"the lock's holder": s.Lock.Holder, "the lock's machine": s.Lock.Machine, "the lock's operation": s.Lock.Operation,
			"the agent's version": agent.Version, "the agent's build": agent.Build, "the agent's problem": agent.Problem,
			"the updater's version": updater.Version, "the service's name": service.Name,
		} {
			if got != quoted {
				t.Errorf("%q as %s: Status says %q, want %q", bad, what, got, quoted)
			}
		}
		// What is printable is left as it is.
		if s.Nodes[0] != "node1" || service.Version != "1.0.0" || service.Problem != "it is stuck" || s.Lock.Lease != "l" {
			t.Errorf("Status changed printable text: %q, %q, %q, %q", s.Nodes[0], service.Version, service.Problem, s.Lock.Lease)
		}
	}
}

// Printable leaves printable text, valid UTF-8, as it is, and quotes any
// other, so that what it returns is valid UTF-8 and printable, and
// unquotes to what it was given.
func FuzzPrintable(f *testing.F) {
	for _, s := range []string{"", "1.0.0", "1.0.0\nnode9", "\x1b[2A", "ü", "\u202e", "\xff", "\x9b", " "} {
		f.Add(s)
	}
	// printable is the text a terminal shows as it is.
	printable := func(s string) bool {
		return utf8.ValidString(s) && !strings.ContainsFunc(s, func(r rune) bool { return !strconv.IsPrint(r) })
	}
	f.Fuzz(func(t *testing.T, s string) {
		got := nodeclient.Printable(s)
		if !printable(got) {
			t.Fatalf("Printable(%q) = %q, which isn't printable", s, got)
		}
		if printable(s) {
			if got != s {
				t.Fatalf("Printable(%q) = %q, want it as it was", s, got)
			}
			return
		}
		if back, err := strconv.Unquote(got); err != nil || back != s {
			t.Fatalf("Printable(%q) = %q, which unquotes to %q, %v", s, got, back, err)
		}
	})
}
