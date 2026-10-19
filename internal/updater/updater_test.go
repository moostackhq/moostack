//go:build linux

package updater_test

import (
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/hostlayout"
	"github.com/moostackhq/moostack/internal/nodetest"
)

// These tests run the node's components in a privileged container, as a
// node runs them, and call the node API through the updater's relay.

func TestStatusReportsTheUpdaterHealthyWithTheAgentStopped(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.Kill(t, "agent")
	nodetest.Eventually(t, 30*time.Second, func() error {
		_, err := n.Status(t)
		if err == nil {
			return errors.New("the agent still answers after it was killed")
		}
		return nil
	})
	status, err := n.UpdaterStatus(t)
	if err != nil {
		t.Fatal(err)
	}
	u := status.Updater
	if u.GetName() != "updater" || u.GetVersion() != nodetest.Version || u.GetHealth() != nodev1.Health_HEALTH_HEALTHY {
		t.Errorf("with the agent stopped, the updater reports %v, want updater %s healthy", u, nodetest.Version)
	}
}

func TestOnlyRootCanConnectToTheUpdatersSocket(t *testing.T) {
	n := nodetest.Start(t)
	nodetest.Eventually(t, 30*time.Second, func() error {
		_, err := n.UpdaterStatus(t)
		return err
	})
	out, err := n.Output("65534", hostlayout.Updater, "relay", hostlayout.UpdaterSocket)
	if err == nil || !strings.Contains(out, "permission denied") {
		t.Errorf("a relay as nobody: %v, %s; want permission denied", err, out)
	}
	mode, err := n.Output("root", "stat", "-c", "%a %U", hostlayout.UpdaterSocket)
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

func TestTheUpdaterWontServeWithoutAValidIdentity(t *testing.T) {
	nodetest.CheckRefusesInvalidIdentity(t, hostlayout.Updater)
}
