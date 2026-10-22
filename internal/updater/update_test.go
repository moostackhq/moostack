//go:build linux

package updater_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/hostlayout"
	"github.com/moostackhq/moostack/internal/nodeclient"
	"github.com/moostackhq/moostack/internal/nodetest"
)

// Broken agents, as shell scripts.
var (
	// exitsAtOnce never serves.
	exitsAtOnce = []byte("#!/bin/sh\nexit 1\n")
	// neverServes runs, but never listens.
	neverServes = []byte("#!/bin/sh\nexec sleep infinity\n")
)

// startsSlowly is NextAgent, which starts only after a few seconds.
var startsSlowly = []byte("#!/bin/sh\nsleep 3\nexec " + nodetest.NextAgentPath + " \"$@\"\n")

func TestUpdateAgentSwapsToTheNewAgentAndBack(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	if err := update(t, n, nodetest.NextAgent(t), nodetest.NextVersion, 30*time.Second); err != nil {
		t.Fatalf("updating the agent to %s: %v", nodetest.NextVersion, err)
	}
	wantAgent(t, n, nodetest.NextVersion, hostlayout.AgentSlots[1])
	// The A/B slot it swapped from keeps the agent it replaced.
	if got, want := sum(t, n, hostlayout.AgentSlots[0]), sha(nodetest.Agent(t)); got != want {
		t.Errorf("%s's SHA-256 is %s, want the replaced agent's, %s", hostlayout.AgentSlots[0], got, want)
	}
	// A second update swaps to the other A/B slot again.
	if err := update(t, n, nodetest.Agent(t), nodetest.Version, 30*time.Second); err != nil {
		t.Fatalf("updating the agent back to %s: %v", nodetest.Version, err)
	}
	wantAgent(t, n, nodetest.Version, hostlayout.AgentSlots[0])
	wantNoSwapRecord(t, n)
}

// A node that a release before the agent's A/B slots installed has the
// agent as a plain file, which the first update moves into its first A/B
// slot.
func TestUpdateAgentMovesAPlainAgentIntoItsFirstABSlot(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.Exec(t, "root", "mv", hostlayout.AgentSlots[0], hostlayout.Agent)
	if err := update(t, n, nodetest.NextAgent(t), nodetest.NextVersion, 30*time.Second); err != nil {
		t.Fatalf("updating a plain agent: %v", err)
	}
	wantAgent(t, n, nodetest.NextVersion, hostlayout.AgentSlots[1])
	if got, want := sum(t, n, hostlayout.AgentSlots[0]), sha(nodetest.Agent(t)); got != want {
		t.Errorf("%s's SHA-256 is %s, want the plain agent's, %s", hostlayout.AgentSlots[0], got, want)
	}
}

func TestABrokenAgentIsRolledBack(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	for _, c := range []struct {
		name    string
		binary  []byte
		version string
		// want is in the error, which says why the new agent wasn't
		// healthy.
		want string
	}{
		{"an agent that exits at once", exitsAtOnce, nodetest.NextVersion, "doesn't answer"},
		{"an agent that never serves", neverServes, nodetest.NextVersion, "doesn't answer"},
		{"an agent that reports another version", nodetest.Agent(t), "9.9.9-test", "runs version " + nodetest.Version + ", not 9.9.9-test"},
	} {
		t.Run(c.name, func(t *testing.T) {
			err := update(t, n, c.binary, c.version, 5*time.Second)
			if status.Code(err) != codes.Aborted || !strings.Contains(err.Error(), "rolled back") || !strings.Contains(err.Error(), c.want) {
				t.Errorf("updating to %s: %v, want ABORTED saying it rolled back, and %q", c.name, err, c.want)
			}
			wantAgent(t, n, nodetest.Version, hostlayout.AgentSlots[0])
			wantNoSwapRecord(t, n)
		})
	}
}

func TestANewAgentReportingAComponentUnhealthyIsRolledBack(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.Kill(t, "containerd")
	err := update(t, n, nodetest.NextAgent(t), nodetest.NextVersion, 5*time.Second)
	// The agent it replaced reports containerd unhealthy too, which the
	// error says.
	if status.Code(err) != codes.Internal || !strings.Contains(err.Error(), "containerd is unhealthy") || !strings.Contains(err.Error(), "doesn't become healthy either") {
		t.Errorf("updating with containerd down: %v, want INTERNAL saying containerd is unhealthy, before and after rolling back", err)
	}
	if got := link(t, n); got != filepath.Base(hostlayout.AgentSlots[0]) {
		t.Errorf("%s links to %s, want it switched back to %s", hostlayout.Agent, got, filepath.Base(hostlayout.AgentSlots[0]))
	}
	s, err := n.Status(t)
	if err != nil {
		t.Fatal(err)
	}
	if a := s.Components[0]; a.Version != nodetest.Version {
		t.Errorf("the node runs the agent at %s, want the one it replaced, %s", a.Version, nodetest.Version)
	}
	wantNoSwapRecord(t, n)
}

func TestAnUpdateThatDoesntMatchItsChecksumChangesNothing(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	pid := agentPID(t, n)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	stream, err := n.Updater.UpdateAgent(ctx)
	if err != nil {
		t.Fatal(err)
	}
	err = stream.Send(&nodev1.UpdateAgentRequest{
		Update: &nodev1.AgentUpdate{Version: nodetest.NextVersion, Sha256: sha(nodetest.Agent(t)), HealthTimeout: durationpb.New(30 * time.Second)},
		Chunk:  nodetest.NextAgent(t)[:1<<20],
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.CloseAndRecv(); status.Code(err) != codes.InvalidArgument {
		t.Errorf("an update whose binary doesn't match its checksum: %v, want INVALID_ARGUMENT", err)
	}
	if got := agentPID(t, n); got != pid {
		t.Errorf("the agent's pid is %s, want %s: it was restarted", got, pid)
	}
	wantAgent(t, n, nodetest.Version, hostlayout.AgentSlots[0])
	if out, err := n.Output("root", "sh", "-c", "ls "+hostlayout.BinDir); err != nil || strings.Contains(out, filepath.Base(hostlayout.AgentSlots[1])) {
		t.Errorf("the bin directory holds %s, %v; want nothing in the inactive slot", out, err)
	}
}

func TestUpdateAgentRefusesAnUpdateWithoutItsAgent(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	for _, c := range []struct {
		name   string
		update *nodev1.AgentUpdate
	}{
		{"no update", nil},
		{"no version", &nodev1.AgentUpdate{Sha256: sha(exitsAtOnce), HealthTimeout: durationpb.New(time.Second)}},
		{"no checksum", &nodev1.AgentUpdate{Version: "1", HealthTimeout: durationpb.New(time.Second)}},
		{"no health timeout", &nodev1.AgentUpdate{Version: "1", Sha256: sha(exitsAtOnce)}},
	} {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		stream, err := n.Updater.UpdateAgent(ctx)
		if err != nil {
			t.Fatal(err)
		}
		stream.Send(&nodev1.UpdateAgentRequest{Update: c.update, Chunk: exitsAtOnce})
		if _, err := stream.CloseAndRecv(); status.Code(err) != codes.InvalidArgument {
			t.Errorf("an update with %s: %v, want INVALID_ARGUMENT", c.name, err)
		}
		cancel()
	}
	wantAgent(t, n, nodetest.Version, hostlayout.AgentSlots[0])
}

func TestASecondUpdateWhileOneRunsIsRefused(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	first := make(chan error, 1)
	go func() { first <- update(t, n, neverServes, nodetest.NextVersion, 10*time.Second) }()
	waitForLink(t, n, hostlayout.AgentSlots[1])
	if err := update(t, n, nodetest.NextAgent(t), nodetest.NextVersion, 10*time.Second); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("a second update while one runs: %v, want FAILED_PRECONDITION", err)
	}
	if err := <-first; status.Code(err) != codes.Aborted {
		t.Errorf("the first update: %v, want ABORTED", err)
	}
	wantAgent(t, n, nodetest.Version, hostlayout.AgentSlots[0])
}

func TestAnUpdaterThatCrashesMidSwapSwitchesBackWhenItStarts(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	done := make(chan error, 1)
	go func() { done <- update(t, n, neverServes, nodetest.NextVersion, 5*time.Minute) }()
	waitForLink(t, n, hostlayout.AgentSlots[1])
	n.Crash(t, "updater")
	if err := <-done; err == nil {
		t.Error("the update succeeded, though the updater crashed before the new agent was healthy")
	}
	nodetest.Eventually(t, 30*time.Second, func() error {
		s, err := n.Status(t)
		if err != nil {
			return err
		}
		if v := s.Components[0].Version; v != nodetest.Version {
			return errors.New("the agent runs " + v)
		}
		return nil
	})
	waitForNoSwapRecord(t, n)
	wantAgent(t, n, nodetest.Version, hostlayout.AgentSlots[0])
}

// The updater switches the agent back before it reads the node file, so
// that a node file it can't read doesn't leave the agent swapped.
func TestAnUpdaterThatCrashesMidSwapSwitchesBackWithANodeFileItCantRead(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	done := make(chan error, 1)
	go func() { done <- update(t, n, neverServes, nodetest.NextVersion, 5*time.Minute) }()
	waitForLink(t, n, hostlayout.AgentSlots[1])
	n.WriteConfig(t, hostlayout.NodeFile, []byte("not a node file\n"))
	n.Crash(t, "updater")
	<-done
	// The agent, restarted on the node file it can't read either, doesn't
	// serve, but runs from the slot it ran from before.
	waitForNoSwapRecord(t, n)
	if got := link(t, n); got != filepath.Base(hostlayout.AgentSlots[0]) {
		t.Errorf("%s links to %s, want %s", hostlayout.Agent, got, filepath.Base(hostlayout.AgentSlots[0]))
	}
}

func TestARollbackWhoseRestartFailsKeepsTheSwapAndRefusesUpdatesUntilTheNextStart(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	done := make(chan error, 1)
	go func() { done <- update(t, n, neverServes, nodetest.NextVersion, 20*time.Second) }()
	// Once the agent that never serves runs, the rollback's restart
	// fails.
	nodetest.Eventually(t, 30*time.Second, func() error {
		if _, err := n.Status(t); err == nil {
			return errors.New("the agent still answers")
		}
		return nil
	})
	n.BreakRestarts(t, nodetest.RestartsFail)
	if err := <-done; status.Code(err) != codes.Internal || !strings.Contains(err.Error(), "the restart failed") {
		t.Errorf("an update whose rollback can't restart the agent: %v, want INTERNAL saying the restart failed", err)
	}
	if out, err := n.Output("root", "test", "-e", hostlayout.AgentSwapFile); err != nil {
		t.Errorf("the swap's record is gone, though the agent wasn't switched back: %v %s", err, out)
	}
	// Until then, an update would overwrite an A/B slot the record
	// relies on.
	n.BreakRestarts(t, nodetest.RestartsWork)
	slots := [2]string{sum(t, n, hostlayout.AgentSlots[0]), sum(t, n, hostlayout.AgentSlots[1])}
	if err := update(t, n, nodetest.NextAgent(t), nodetest.NextVersion, 20*time.Second); status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "when it next starts") {
		t.Errorf("an update while a swap is recorded: %v, want FAILED_PRECONDITION saying the updater switches back when it next starts", err)
	}
	for i, slot := range hostlayout.AgentSlots {
		if got := sum(t, n, slot); got != slots[i] {
			t.Errorf("an update refused while a swap is recorded changed %s", slot)
		}
	}
	// The next updater to start switches the agent back.
	n.Crash(t, "updater")
	waitForNoSwapRecord(t, n)
	wantAgent(t, n, nodetest.Version, hostlayout.AgentSlots[0])
}

func TestAnUpdateWhoseRestartsHangEndsWithinItsTimeouts(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	n.BreakRestarts(t, nodetest.RestartsHang)
	start := time.Now()
	err := update(t, n, nodetest.NextAgent(t), nodetest.NextVersion, 3*time.Second)
	// The swap and the rollback have the health timeout each, their
	// restarts included.
	if took := time.Since(start); took > 15*time.Second {
		t.Errorf("an update whose restarts hang took %s, want it to end within its two health timeouts", took)
	}
	if status.Code(err) != codes.Internal {
		t.Errorf("an update whose restarts hang: %v, want INTERNAL", err)
	}
	// An updater that starts with the swap recorded has the update's
	// health timeout to switch back, and exits if it can't, so that
	// systemd starts it again.
	start = time.Now()
	n.Crash(t, "updater")
	nodetest.Eventually(t, 15*time.Second, func() error {
		// Nothing reaps it, so it is gone once it is a zombie.
		if _, err := n.Output("root", "sh", "-c", `pid=$(cat /run/updater.pid); [ -e /proc/$pid ] && [ "$(cut -d' ' -f3 /proc/$pid/stat)" != Z ]`); err == nil {
			return errors.New("the updater still runs")
		}
		return nil
	})
	if took := time.Since(start); took > 8*time.Second {
		t.Errorf("an updater whose switch back hangs took %s to exit, want it within the update's health timeout", took)
	}
	n.BreakRestarts(t, nodetest.RestartsWork)
	n.Crash(t, "updater")
	waitForNoSwapRecord(t, n)
	wantAgent(t, n, nodetest.Version, hostlayout.AgentSlots[0])
}

func TestAHealthyUpdateWhoseRecordStaysSaysTheUpdaterSwitchesBack(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	done := make(chan error, 1)
	go func() {
		done <- nodeclient.UpdateAgent(t.Context(), n.Updater, nodetest.NextVersion, startsSlowly, 30*time.Second)
	}()
	waitForLink(t, n, hostlayout.AgentSlots[1])
	// A directory that isn't empty, in the record's place, can't be
	// removed.
	n.Exec(t, "root", "sh", "-c", "rm "+hostlayout.AgentSwapFile+" && mkdir "+hostlayout.AgentSwapFile+" && touch "+hostlayout.AgentSwapFile+"/x")
	if err := <-done; status.Code(err) != codes.Internal || !strings.Contains(err.Error(), "switches back to the agent it replaced when it next starts") {
		t.Errorf("an update whose record can't be removed: %v, want INTERNAL saying the updater switches back when it next starts", err)
	}
}

func TestAnUpdaterStoppedMidSwapFinishesItFirst(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	done := make(chan error, 1)
	go func() {
		done <- nodeclient.UpdateAgent(t.Context(), n.Updater, nodetest.NextVersion, startsSlowly, 30*time.Second)
	}()
	waitForLink(t, n, hostlayout.AgentSlots[1])
	// Restart stops the updater as systemd does, with SIGTERM, and waits
	// for it to exit.
	n.Restart(t, "updater")
	if err := <-done; err != nil {
		t.Errorf("the update the stopping updater finished: %v", err)
	}
	wantAgent(t, n, nodetest.NextVersion, hostlayout.AgentSlots[1])
	wantNoSwapRecord(t, n)
}

func TestAnUpdateGoesOnOnceTheUpdaterHasTheBinary(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- nodeclient.UpdateAgent(ctx, n.Updater, nodetest.NextVersion, startsSlowly, 30*time.Second)
	}()
	waitForLink(t, n, hostlayout.AgentSlots[1])
	cancel()
	if err := <-done; status.Code(err) != codes.Canceled {
		t.Errorf("the update whose call ended: %v, want it cancelled", err)
	}
	waitForNoSwapRecord(t, n)
	wantAgent(t, n, nodetest.NextVersion, hostlayout.AgentSlots[1])
}

// update has the node's updater replace its agent with binary, the agent
// at version.
func update(t *testing.T, n *nodetest.Node, binary []byte, version string, healthTimeout time.Duration) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	return nodeclient.UpdateAgent(ctx, n.Updater, version, binary, healthTimeout)
}

// wantAgent fails unless the node's agent reports version, healthy, with
// every component healthy, and runs from slot.
func wantAgent(t *testing.T, n *nodetest.Node, version, slot string) {
	t.Helper()
	s := n.EventuallyHealthy(t)
	if a := s.Components[0]; a.Name != "agent" || a.Version != version {
		t.Errorf("the node reports %v, want the agent at %s", a, version)
	}
	if got := link(t, n); got != filepath.Base(slot) {
		t.Errorf("%s links to %s, want %s", hostlayout.Agent, got, filepath.Base(slot))
	}
}

// wantNoSwapRecord fails if the updater still records a swap.
func wantNoSwapRecord(t *testing.T, n *nodetest.Node) {
	t.Helper()
	if out, err := n.Output("root", "test", "-e", hostlayout.AgentSwapFile); err == nil {
		t.Errorf("%s is still there once the update ended %s", hostlayout.AgentSwapFile, out)
	}
}

// waitForNoSwapRecord waits until the updater no longer records a swap.
func waitForNoSwapRecord(t *testing.T, n *nodetest.Node) {
	t.Helper()
	nodetest.Eventually(t, 30*time.Second, func() error {
		if _, err := n.Output("root", "test", "-e", hostlayout.AgentSwapFile); err == nil {
			return errors.New(hostlayout.AgentSwapFile + " is still there")
		}
		return nil
	})
}

// waitForLink waits until the agent's link points to slot.
func waitForLink(t *testing.T, n *nodetest.Node, slot string) {
	t.Helper()
	nodetest.Eventually(t, 30*time.Second, func() error {
		if got := link(t, n); got != filepath.Base(slot) {
			return errors.New(hostlayout.Agent + " links to " + got)
		}
		return nil
	})
}

// link is what the agent's link points to.
func link(t *testing.T, n *nodetest.Node) string {
	t.Helper()
	out, err := n.Output("root", "readlink", hostlayout.Agent)
	if err != nil {
		t.Fatalf("readlink %s: %v\n%s", hostlayout.Agent, err, out)
	}
	return strings.TrimSpace(out)
}

// sum is the SHA-256 of the file at path in the node, hex.
func sum(t *testing.T, n *nodetest.Node, path string) string {
	t.Helper()
	out, err := n.Output("root", "sha256sum", path)
	if err != nil {
		t.Fatalf("sha256sum %s: %v\n%s", path, err, out)
	}
	return strings.Fields(out)[0]
}

// sha is the SHA-256 of data, hex.
func sha(data []byte) string {
	s := sha256.Sum256(data)
	return hex.EncodeToString(s[:])
}

// agentPID is the pid of the node's agent.
func agentPID(t *testing.T, n *nodetest.Node) string {
	t.Helper()
	out, err := n.Output("root", "cat", "/run/agent.pid")
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(out)
}
