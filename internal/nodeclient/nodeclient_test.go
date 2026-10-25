package nodeclient_test

import (
	"context"
	"slices"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

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
				Containers:        []*nodev1.ContainerStatus{{Name: bad, Image: bad, Digest: bad, Problem: bad}},
			}},
			Updater: updaterStatusOf{res: &nodev1.UpdaterStatusResponse{Updater: &nodev1.Component{Name: "updater", Version: bad}}},
		}
		s := n.Status(t.Context())
		agent, updater, container := s.Components[0], s.Components[1], s.Containers[0]
		for what, got := range map[string]string{
			"the node's name": s.Nodes[1], "why the generation is unknown": s.GenerationUnknown,
			"the lock's holder": s.Lock.Holder, "the lock's machine": s.Lock.Machine, "the lock's operation": s.Lock.Operation,
			"the agent's version": agent.Version, "the agent's build": agent.Build, "the agent's problem": agent.Problem,
			"the updater's version": updater.Version,
			"the container's name":  container.Name, "the container's image": container.Image,
			"the container's digest": container.Digest, "the container's problem": container.Problem,
		} {
			if got != quoted {
				t.Errorf("%q as %s: Status says %q, want %q", bad, what, got, quoted)
			}
		}
		// What is printable is left as it is.
		if s.Nodes[0] != "node1" || s.Lock.Lease != "l" {
			t.Errorf("Status changed printable text: %q, %q", s.Nodes[0], s.Lock.Lease)
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

// The cluster services are the containers named for a service that run
// the release's image of that service, each named for its service, at
// the version of that image, as healthy as the container's API, and
// those so named whose image the node can't tell, at no version; no
// other container is one: not one under the service's name alone, as
// releases before the service- names ran it.
func TestStatusTellsTheClusterServicesAmongTheContainers(t *testing.T) {
	n := &nodeclient.Node{
		Agent: statusOf{agent: &nodev1.StatusResponse{Containers: []*nodev1.ContainerStatus{
			{Name: "service-dns", Image: "moostack.local/dns:1.2.0", Digest: "sha256:dns", Health: nodev1.Health_HEALTH_UNHEALTHY, Problem: "it is stuck"},
			{Name: "service-proxy", Image: "moostack.local/dns:1.2.0"},
			{Name: "service-registry", Image: "moostack.local/registry:"},
			{Name: "service-telemetry", Problem: "its image can't be read"},
			{Name: "dns", Image: "moostack.local/dns:1.1.0"},
			{Name: "app-web", Image: "docker.io/library/nginx:1.27"},
		}}},
		Updater: updaterStatusOf{res: &nodev1.UpdaterStatusResponse{Updater: &nodev1.Component{Name: "updater"}}},
	}
	got := n.Status(t.Context()).Services()
	want := []*nodev1.Component{
		{Name: "dns", Version: "1.2.0", Build: "sha256:dns", Health: nodev1.Health_HEALTH_UNHEALTHY, Problem: "it is stuck"},
		{Name: "telemetry", Problem: "its image can't be read"},
	}
	if !slices.EqualFunc(got, want, func(a, b *nodev1.Component) bool { return proto.Equal(a, b) }) {
		t.Errorf("Status says the services are %v, want %v", got, want)
	}
}
