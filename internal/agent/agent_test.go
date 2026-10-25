//go:build linux

package agent_test

import (
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	servicev1 "github.com/moostackhq/moostack/api/service/v1"
	"github.com/moostackhq/moostack/internal/hostlayout"
	"github.com/moostackhq/moostack/internal/nodetest"
	"github.com/moostackhq/moostack/internal/release"
)

// These tests run the node's components in a privileged container, as a
// node runs them, and call the node API through the agent's relay.

func TestStatusReportsTheNodesModeAndComponents(t *testing.T) {
	n := nodetest.Start(t)
	status := n.EventuallyHealthy(t)
	if status.Mode != nodev1.Mode_MODE_DIRECT {
		t.Errorf("mode is %s, want direct", status.Mode)
	}
	got := map[string]string{}
	for _, c := range status.Components {
		got[c.Name] = c.Version + " " + c.Health.String()
	}
	want := map[string]string{
		"agent":      nodetest.Version + " HEALTH_HEALTHY",
		"containerd": release.ContainerdVersion + " HEALTH_HEALTHY",
		"runc":       release.RuncVersion + " HEALTH_HEALTHY",
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("components are %v, want %v", got, want)
	}
}

func TestStatusReportsAStoppedContainerdUnhealthy(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.Kill(t, "containerd")
	var containerd *nodev1.Component
	nodetest.Eventually(t, 30*time.Second, func() error {
		status, err := n.Status(t)
		if err != nil {
			return err
		}
		for _, c := range status.Components {
			if c.Name == "containerd" {
				containerd = c
			}
			if c.Name == "agent" && c.Health != nodev1.Health_HEALTH_HEALTHY {
				t.Errorf("the agent is %s with containerd stopped", c.Health)
			}
		}
		if containerd == nil || containerd.Health != nodev1.Health_HEALTH_UNHEALTHY {
			return fmt.Errorf("containerd is %v", containerd)
		}
		return nil
	})
	if containerd.Problem == "" {
		t.Error("an unhealthy containerd has no problem given")
	}
}

func TestStatusReportsARuncThatDoesntRunUnhealthy(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.Exec(t, "root", "mount", "--bind", "/dev/null", hostlayout.Runc)
	status, err := n.Status(t)
	if err != nil {
		t.Fatal(err)
	}
	i := slices.IndexFunc(status.Components, func(c *nodev1.Component) bool { return c.Name == "runc" })
	if i < 0 {
		t.Fatalf("the agent doesn't report runc: %v", status.Components)
	}
	if c := status.Components[i]; c.Health != nodev1.Health_HEALTH_UNHEALTHY || c.Problem == "" {
		t.Errorf("runc that doesn't run is %s: %q", c.Health, c.Problem)
	}
}

func TestTheAgentStartsWithContainerdStoppedAndSaysSo(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.StartServices(t)
	n.Kill(t, "containerd")
	n.Kill(t, "agent")
	n.Restart(t, "agent")
	nodetest.Eventually(t, 30*time.Second, func() error {
		st, err := n.Status(t)
		if err != nil {
			return err
		}
		for _, c := range append(slices.Clone(st.Components), nodetest.Services(st)...) {
			if c.Name == "containerd" || slices.ContainsFunc(release.ClusterServices, func(s release.ClusterService) bool { return s.Name == c.Name }) {
				if c.Health != nodev1.Health_HEALTH_UNHEALTHY || c.Problem == "" {
					return fmt.Errorf("%s is %s: %q, want unhealthy saying why", c.Name, c.Health, c.Problem)
				}
			}
		}
		if len(nodetest.Services(st)) != len(release.ClusterServices) {
			return fmt.Errorf("the agent reports the services %v", nodetest.Services(st))
		}
		return nil
	})
	health := "/" + nodetest.Service("dns").Api + "/Health"
	err := n.AgentConn(t).Invoke(t.Context(), health, &servicev1.HealthRequest{}, &servicev1.HealthResponse{})
	if status.Code(err) != codes.Unavailable {
		t.Errorf("a call of dns while containerd is down: %v, want unavailable", err)
	}
	if _, err := runService(t.Context(), t, n, nodetest.Service("dns")); status.Code(err) != codes.Unavailable {
		t.Errorf("running dns while containerd is down: %v, want unavailable", err)
	}
	// Once containerd is back, the agent starts the services and routes
	// them again by itself.
	n.Restart(t, "containerd")
	n.EventuallyHealthy(t)
}

func TestContainerdKeepsToMoostacksDirectories(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	// Running containers is what puts shims' sockets and runc's state
	// on the host.
	n.StartServices(t)
	for _, dir := range []string{"/run/containerd", "/var/lib/containerd", "/opt/containerd", "/etc/containerd", "/var/run/nri"} {
		if out, err := n.Output("root", "test", "-e", dir); err == nil {
			t.Errorf("containerd made %s", dir)
		} else if _, ok := err.(*exec.ExitError); !ok {
			t.Fatalf("checking for %s: %v\n%s", dir, err, out)
		}
	}
}

func TestOnlyRootCanConnectToTheSocket(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	out, err := n.Output("65534", hostlayout.Agent, "relay", hostlayout.AgentSocket)
	if err == nil || !strings.Contains(out, "permission denied") {
		t.Errorf("a relay as nobody: %v, %s; want permission denied", err, out)
	}
	// The socket is root's alone, not only its directory.
	mode, err := n.Output("root", "stat", "-c", "%a %U", hostlayout.AgentSocket)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(mode); got != "600 root" {
		t.Errorf("the socket is %s, want 600 root", got)
	}
}

func TestMain(m *testing.M) {
	code := m.Run()
	nodetest.Cleanup()
	os.Exit(code)
}
