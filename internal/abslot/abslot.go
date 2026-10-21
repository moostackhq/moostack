//go:build linux

// Package abslot replaces a node component's binary through its A/B
// slots: the component runs from one of them, which its link points to.
// An update writes the new binary into the other A/B slot, records the
// swap, switches the link to that A/B slot and restarts the component.
// Once the new one is healthy, it removes the record. Otherwise it
// switches the link back and restarts the one it replaced. A program
// that starts with the record there switches back too, since no one saw
// the new one healthy. The record names the A/B slot it switches back
// to, and the update's health timeout, which switching back has too.
//
// The updater swaps the agent this way, and the agent the updater, so
// that neither replaces itself.
package abslot

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
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Component is a node component that runs from one of two A/B slots.
type Component struct {
	// Name is the component's, such as agent, and Swapper the one that
	// swaps it, such as the updater, as errors say them.
	Name, Swapper string
	// Link is the component's binary, a symbolic link to the active one
	// of Slots, relative, in the same directory.
	Link string
	// Slots are the component's A/B slots: the one it runs from, and the
	// one an update writes the new binary into.
	Slots [2]string
	// SwapFile records a swap that hasn't been seen healthy. Its
	// directory must exist.
	SwapFile string
	// Restart restarts the component, from the binary Link links to.
	Restart func(ctx context.Context) error
	// Healthy fails unless the component answers healthy, at version
	// unless that is empty.
	Healthy func(ctx context.Context, version string) error

	// updating is held while an update runs, or a switch back.
	updating sync.Mutex
}

// Update is the binary an update installs.
type Update struct {
	// Version is the version the new binary reports.
	Version string
	// SHA256 is the binary's SHA-256, hex.
	SHA256 string
	// HealthTimeout is how long the new binary has to become healthy.
	HealthTimeout time.Duration
}

// Recv returns an update's next request: the first says which binary it
// is, the others don't, and each carries the binary's next bytes. It
// returns io.EOF once there are no more.
type Recv func() (*Update, []byte, error)

// Update receives the new binary from recv and swaps to it, answering
// once it is healthy. If it doesn't become healthy within the update's
// health timeout, it switches back, restarts the binary it replaced, and
// fails with ABORTED, or with INTERNAL if the one it replaced doesn't
// restart or become healthy within the health timeout either. It fails
// with INTERNAL too if the new binary is healthy but the swap's record
// stays. It refuses a binary that doesn't match the update's checksum
// with INVALID_ARGUMENT, and, with FAILED_PRECONDITION, an update while
// another runs or while a swap that didn't finish is recorded, changing
// nothing. Once it has the whole binary, the update goes on even if ctx
// ends.
func (c *Component) Update(ctx context.Context, recv Recv) error {
	if !c.updating.TryLock() {
		return status.Errorf(codes.FailedPrecondition, "another update of the %s is running", c.Name)
	}
	defer c.updating.Unlock()
	// A swap whose rollback failed leaves its record, which names the A/B
	// slot of the binary it replaced. An update would overwrite one of
	// the A/B slots and the record.
	if _, err := os.Stat(c.SwapFile); err == nil {
		return status.Errorf(codes.FailedPrecondition, "a swap that didn't finish is recorded, so %s switches back to the %s it replaced when it next starts", c.Swapper, c.Name)
	} else if !errors.Is(err, os.ErrNotExist) {
		return status.Errorf(codes.Internal, "the swap's record: %v", err)
	}
	update, first, err := recv()
	if err != nil {
		return err
	}
	if err := c.check(update); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if err := c.adopt(); err != nil {
		return status.Errorf(codes.Internal, "moving the %s into its A/B slots: %v", c.Name, err)
	}
	active, err := c.activeSlot()
	if err != nil {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	next := c.Slots[0]
	if active == next {
		next = c.Slots[1]
	}
	if err := receive(recv, first, next, update.SHA256); err != nil {
		return err
	}
	// The update goes on even if the call ends: it never leaves the node
	// halfway.
	return c.swap(context.WithoutCancel(ctx), active, next, update.Version, update.HealthTimeout)
}

// check fails unless update says which binary it is.
func (c *Component) check(update *Update) error {
	switch {
	case update == nil:
		return fmt.Errorf("the first request doesn't say which %s it is", c.Name)
	case update.Version == "":
		return errors.New("the update has no version")
	case len(update.SHA256) != sha256.Size*2:
		return errors.New("the update has no SHA-256")
	case update.HealthTimeout <= 0:
		return errors.New("the update has no health timeout")
	}
	return nil
}

// adopt moves a plain binary in the link's place, as a release before
// the A/B slots installed it, into the first A/B slot, and links to it.
// The binary is hard linked first, so that it is at one path or both
// until the link replaces it, and a crash in between leaves it running
// from either. A first A/B slot already there is left from such a crash.
func (c *Component) adopt() error {
	fi, err := os.Lstat(c.Link)
	if err != nil {
		return err
	}
	if !fi.Mode().IsRegular() {
		return nil
	}
	if err := os.Remove(c.Slots[0]); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.Link(c.Link, c.Slots[0]); err != nil {
		return err
	}
	return c.link(c.Slots[0])
}

// activeSlot is the A/B slot the link points to.
func (c *Component) activeSlot() (string, error) {
	target, err := os.Readlink(c.Link)
	if err != nil {
		return "", fmt.Errorf("the %s's link: %w", c.Name, err)
	}
	slot := filepath.Join(filepath.Dir(c.Link), target)
	if !slices.Contains(c.Slots[:], slot) {
		return "", fmt.Errorf("%s links to %s, which isn't one of the %s's A/B slots", c.Link, target, c.Name)
	}
	return slot, nil
}

// receive writes the binary that recv carries, after first, into slot,
// replacing what it held, if the binary's SHA-256 is want. A binary that
// doesn't match leaves slot as it was.
func receive(recv Recv, first []byte, slot, want string) (err error) {
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
		_, chunk, err = recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
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

// swap switches the component from the A/B slot active to next, restarts
// it, and waits for it to become healthy at version, all within timeout.
// If it doesn't, it switches back, within timeout again.
func (c *Component) swap(ctx context.Context, active, next, version string, timeout time.Duration) error {
	if err := writeFile(c.SwapFile, []byte(filepath.Base(active)+"\n"+timeout.String()+"\n")); err != nil {
		return fmt.Errorf("recording the swap: %w", err)
	}
	if err := c.link(next); err != nil {
		return c.rollBack(ctx, active, timeout, err)
	}
	swapCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := c.Restart(swapCtx); err != nil {
		return c.rollBack(ctx, active, timeout, fmt.Errorf("restarting the new %s: %w", c.Name, err))
	}
	if err := c.waitHealthy(swapCtx, version); err != nil {
		return c.rollBack(ctx, active, timeout, fmt.Errorf("the new %s didn't become healthy within %s: %w", c.Name, timeout, err))
	}
	if err := c.removeSwapRecord(); err != nil {
		return status.Errorf(codes.Internal, "the new %s is healthy, but the swap's record stays, so %s switches back to the %[1]s it replaced when it next starts: %v", c.Name, c.Swapper, err)
	}
	return nil
}

// rollBack switches the component back to the A/B slot active after the
// swap failed with cause, restarts it, and waits for it to answer, all
// within timeout. It fails with ABORTED, saying why the swap failed, or,
// if switching back fails too, with INTERNAL, saying both. The swap's
// record stays until the component has restarted from active, so that a
// swapper that starts again switches back if this one couldn't.
func (c *Component) rollBack(ctx context.Context, active string, timeout time.Duration, cause error) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := c.restore(ctx, active); err != nil {
		return status.Errorf(codes.Internal, "%v; rolling back to the %s it replaced failed too: %v", cause, c.Name, err)
	}
	if err := c.waitHealthy(ctx, ""); err != nil {
		return status.Errorf(codes.Internal, "%v; rolled back to the %s it replaced, which doesn't become healthy either: %v", cause, c.Name, err)
	}
	return status.Errorf(codes.Aborted, "%v; rolled back to the %s it replaced", cause, c.Name)
}

// restore switches the component back to the A/B slot active, restarts
// it, and then removes the swap's record.
func (c *Component) restore(ctx context.Context, active string) error {
	if err := c.link(active); err != nil {
		return err
	}
	if err := c.Restart(ctx); err != nil {
		return fmt.Errorf("restarting the %s: %w", c.Name, err)
	}
	return c.removeSwapRecord()
}

// removeSwapRecord removes the swap's record, if it is there.
func (c *Component) removeSwapRecord() error {
	if err := os.Remove(c.SwapFile); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return syncDir(filepath.Dir(c.SwapFile))
}

// Recover switches the component back if a swap is recorded, as when its
// swapper stopped before it saw the new one healthy, within the health
// timeout the swap's update had. An update waits for it.
func (c *Component) Recover(ctx context.Context) error {
	c.updating.Lock()
	defer c.updating.Unlock()
	data, err := os.ReadFile(c.SwapFile)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	fields := strings.Fields(string(data))
	if len(fields) != 2 {
		return fmt.Errorf("%s records a swap as %q, not as an A/B slot and a health timeout", c.SwapFile, data)
	}
	slot := filepath.Join(filepath.Dir(c.Link), fields[0])
	if !slices.Contains(c.Slots[:], slot) {
		return fmt.Errorf("%s records a swap from %q, which isn't one of the %s's A/B slots", c.SwapFile, fields[0], c.Name)
	}
	timeout, err := time.ParseDuration(fields[1])
	if err != nil || timeout <= 0 {
		return fmt.Errorf("%s records a swap whose health timeout is %q, not a duration", c.SwapFile, fields[1])
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := c.restore(ctx, slot); err != nil {
		return fmt.Errorf("switching back the %s from a swap that didn't finish: %w", c.Name, err)
	}
	return nil
}

// link points the component's link to slot, replacing it at once.
func (c *Component) link(slot string) error {
	tmp := c.Link + ".link"
	if err := os.Remove(tmp); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.Symlink(filepath.Base(slot), tmp); err != nil {
		return err
	}
	if err := os.Rename(tmp, c.Link); err != nil {
		return err
	}
	return syncDir(filepath.Dir(c.Link))
}

// waitHealthy waits until ctx ends for the component to be healthy, at
// version unless that is empty.
func (c *Component) waitHealthy(ctx context.Context, version string) error {
	for {
		err := c.healthy(ctx, version)
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

// healthy is Healthy, which has a few seconds to answer.
func (c *Component) healthy(ctx context.Context, version string) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return c.Healthy(ctx, version)
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
