//go:build linux

package updater

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
)

// The agent runs from one of its two A/B slots, which its link points to.
// An update writes the new agent into the other A/B slot, records the swap,
// switches the link to that A/B slot and restarts the agent. Once the new
// agent is healthy, it removes the record. Otherwise it switches the link
// back and restarts the agent it replaced. An updater that starts with
// the record there switches back too, since no one saw the new agent
// healthy. The record names the A/B slot it switches back to, and the
// update's health timeout, which switching back has too.

// UpdateAgent receives the new agent and swaps to it. It holds the
// update lock for the whole update, so that two never interleave, and
// refuses one while a swap is recorded.
func (s *updaterServer) UpdateAgent(stream grpc.ClientStreamingServer[nodev1.UpdateAgentRequest, nodev1.UpdateAgentResponse]) error {
	if !s.updating.TryLock() {
		return status.Error(codes.FailedPrecondition, "another update of the agent is running")
	}
	defer s.updating.Unlock()
	// A swap whose rollback failed leaves its record, which names the A/B
	// slot of the agent it replaced. An update would overwrite one of the
	// slots and the record.
	if _, err := os.Stat(s.cfg.AgentSwapFile); err == nil {
		return status.Error(codes.FailedPrecondition, "a swap that didn't finish is recorded, so the updater switches back to the agent it replaced when it next starts")
	} else if !errors.Is(err, os.ErrNotExist) {
		return status.Errorf(codes.Internal, "the swap's record: %v", err)
	}
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	update := first.Update
	if err := checkUpdate(update); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	active, err := s.activeSlot()
	if err != nil {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	next := s.cfg.AgentSlots[0]
	if active == next {
		next = s.cfg.AgentSlots[1]
	}
	if err := receive(stream, first.Chunk, next, update.Sha256); err != nil {
		return err
	}
	// The update goes on even if the call ends: it never leaves the node
	// halfway.
	ctx := context.WithoutCancel(stream.Context())
	if err := s.swap(ctx, active, next, update.Version, update.HealthTimeout.AsDuration()); err != nil {
		return err
	}
	return stream.SendAndClose(&nodev1.UpdateAgentResponse{})
}

// checkUpdate fails unless update says which agent the binary is.
func checkUpdate(update *nodev1.AgentUpdate) error {
	switch {
	case update == nil:
		return errors.New("the first request doesn't say which agent it is")
	case update.Version == "":
		return errors.New("the update has no version")
	case len(update.Sha256) != sha256.Size*2:
		return errors.New("the update has no SHA-256")
	case update.HealthTimeout.AsDuration() <= 0:
		return errors.New("the update has no health timeout")
	}
	return nil
}

// activeSlot is the A/B slot the agent's link points to.
func (s *updaterServer) activeSlot() (string, error) {
	target, err := os.Readlink(s.cfg.Agent)
	if err != nil {
		return "", fmt.Errorf("the agent's link: %w", err)
	}
	slot := filepath.Join(filepath.Dir(s.cfg.Agent), target)
	if !slices.Contains(s.cfg.AgentSlots[:], slot) {
		return "", fmt.Errorf("%s links to %s, which isn't one of the agent's A/B slots", s.cfg.Agent, target)
	}
	return slot, nil
}

// receive writes the binary that stream carries, after first, into slot,
// replacing what it held, if the binary's SHA-256 is want. A binary that
// doesn't match leaves slot as it was.
func receive(stream grpc.ClientStreamingServer[nodev1.UpdateAgentRequest, nodev1.UpdateAgentResponse], first []byte, slot, want string) (err error) {
	tmp := slot + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o755)
	if err != nil {
		return err
	}
	defer func() {
		if f != nil {
			f.Close()
		}
		if err != nil {
			os.Remove(tmp)
		}
	}()
	h := sha256.New()
	w := io.MultiWriter(f, h)
	chunk := first
	for {
		if _, err := w.Write(chunk); err != nil {
			return err
		}
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		chunk = req.Chunk
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != strings.ToLower(want) {
		return status.Errorf(codes.InvalidArgument, "the binary's SHA-256 is %s, not %s", got, want)
	}
	if err := f.Sync(); err != nil {
		return err
	}
	err = f.Close()
	f = nil
	if err != nil {
		return err
	}
	if err := os.Rename(tmp, slot); err != nil {
		return err
	}
	return syncDir(filepath.Dir(slot))
}

// swap switches the agent from the A/B slot active to next, restarts it, and
// waits for the agent at version to become healthy, all within timeout.
// If it doesn't, it switches back, within timeout again.
func (s *updaterServer) swap(ctx context.Context, active, next, version string, timeout time.Duration) error {
	if err := writeFile(s.cfg.AgentSwapFile, []byte(filepath.Base(active)+"\n"+timeout.String()+"\n")); err != nil {
		return fmt.Errorf("recording the swap: %w", err)
	}
	if err := s.link(next); err != nil {
		return s.rollBack(ctx, active, timeout, err)
	}
	swapCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := s.cfg.RestartAgent(swapCtx); err != nil {
		return s.rollBack(ctx, active, timeout, fmt.Errorf("restarting the new agent: %w", err))
	}
	if err := s.waitHealthy(swapCtx, version); err != nil {
		return s.rollBack(ctx, active, timeout, fmt.Errorf("the new agent didn't become healthy within %s: %w", timeout, err))
	}
	if err := s.removeSwapRecord(); err != nil {
		return status.Errorf(codes.Internal, "the new agent is healthy, but the swap's record stays, so the updater switches back to the agent it replaced when it next starts: %v", err)
	}
	return nil
}

// rollBack switches the agent back to the A/B slot active after the swap
// failed with cause, restarts it, and waits for it to answer, all within
// timeout. It fails with ABORTED, saying why the swap failed, or, if
// switching back fails too, with INTERNAL, saying both. The swap's record
// stays until the agent has restarted from active, so that an updater
// that starts again switches back if this one couldn't.
func (s *updaterServer) rollBack(ctx context.Context, active string, timeout time.Duration, cause error) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := s.restore(ctx, active); err != nil {
		return status.Errorf(codes.Internal, "%v; rolling back to the agent it replaced failed too: %v", cause, err)
	}
	if err := s.waitHealthy(ctx, ""); err != nil {
		return status.Errorf(codes.Internal, "%v; rolled back to the agent it replaced, which doesn't become healthy either: %v", cause, err)
	}
	return status.Errorf(codes.Aborted, "%v; rolled back to the agent it replaced", cause)
}

// restore switches the agent back to the A/B slot active, restarts it, and
// then removes the swap's record.
func (s *updaterServer) restore(ctx context.Context, active string) error {
	if err := s.link(active); err != nil {
		return err
	}
	if err := s.cfg.RestartAgent(ctx); err != nil {
		return fmt.Errorf("restarting the agent: %w", err)
	}
	return s.removeSwapRecord()
}

// removeSwapRecord removes the swap's record, if it is there.
func (s *updaterServer) removeSwapRecord() error {
	if err := os.Remove(s.cfg.AgentSwapFile); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return syncDir(filepath.Dir(s.cfg.AgentSwapFile))
}

// recoverSwap switches the agent back if a swap is recorded, as when
// the updater stopped before it saw the new agent healthy, within the
// health timeout the swap's update had.
func (s *updaterServer) recoverSwap(ctx context.Context) error {
	data, err := os.ReadFile(s.cfg.AgentSwapFile)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	fields := strings.Fields(string(data))
	if len(fields) != 2 {
		return fmt.Errorf("%s records a swap as %q, not as an A/B slot and a health timeout", s.cfg.AgentSwapFile, data)
	}
	slot := filepath.Join(filepath.Dir(s.cfg.Agent), fields[0])
	if !slices.Contains(s.cfg.AgentSlots[:], slot) {
		return fmt.Errorf("%s records a swap from %q, which isn't one of the agent's A/B slots", s.cfg.AgentSwapFile, fields[0])
	}
	timeout, err := time.ParseDuration(fields[1])
	if err != nil || timeout <= 0 {
		return fmt.Errorf("%s records a swap whose health timeout is %q, not a duration", s.cfg.AgentSwapFile, fields[1])
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := s.restore(ctx, slot); err != nil {
		return fmt.Errorf("switching back the agent from a swap that didn't finish: %w", err)
	}
	return nil
}

// link points the agent's link to slot, replacing it at once.
func (s *updaterServer) link(slot string) error {
	tmp := s.cfg.Agent + ".link"
	if err := os.Remove(tmp); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.Symlink(filepath.Base(slot), tmp); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.cfg.Agent); err != nil {
		return err
	}
	return syncDir(filepath.Dir(s.cfg.Agent))
}

// waitHealthy waits until ctx ends for the agent to serve its status,
// at version unless that is empty, with every component it reports
// healthy.
func (s *updaterServer) waitHealthy(ctx context.Context, version string) error {
	conn, err := grpc.NewClient("unix://"+s.cfg.AgentSocket, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	agent := nodev1.NewAgentClient(conn)
	for {
		err := healthy(ctx, agent, version)
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(250 * time.Millisecond):
		}
	}
}

// healthy fails unless agent serves its status, at version unless that
// is empty, with every component it reports healthy.
func healthy(ctx context.Context, agent nodev1.AgentClient, version string) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	res, err := agent.Status(ctx, &nodev1.StatusRequest{})
	if err != nil {
		return fmt.Errorf("the agent doesn't answer: %w", err)
	}
	for _, c := range res.Components {
		if c.Name == "agent" && version != "" && c.Version != version {
			return fmt.Errorf("the agent runs version %s, not %s", c.Version, version)
		}
		if c.Health != nodev1.Health_HEALTH_HEALTHY {
			return fmt.Errorf("%s is unhealthy: %s", c.Name, c.Problem)
		}
	}
	if !slices.ContainsFunc(res.Components, func(c *nodev1.Component) bool { return c.Name == "agent" }) {
		return errors.New("the agent doesn't report itself")
	}
	return nil
}

// writeFile writes data to path through a rename, synced, so that a
// crash leaves the old file or the new one.
func writeFile(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp, path)
	}
	if err != nil {
		os.Remove(tmp)
		return err
	}
	return syncDir(filepath.Dir(path))
}

// syncDir syncs the directory dir, so that a rename in it lasts.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
