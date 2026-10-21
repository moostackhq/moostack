//go:build linux

package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
)

// containerdBinaries are containerd's binaries, which an update replaces
// in place: each is renamed over the old one while containerd runs, and
// containerd is then restarted. The old ones are kept next to them, as
// <name>.prev, until the new containerd answers, and put back if it
// doesn't. An agent that stops before the restart leaves containerd
// running, though its binaries may be part old and part new until an
// update puts them in place again.
type containerdBinaries struct {
	// dir holds files, by name.
	dir   string
	files []string
	// restart restarts containerd, leaving the containers' shims
	// running.
	restart func(ctx context.Context) error
	// wait waits until containerd answers healthy, at version unless
	// that is empty, or ctx ends.
	wait func(ctx context.Context, version string) error
	// updating is held while an update of containerd runs.
	updating sync.Mutex
}

// UpdateContainerd receives containerd's new binaries, puts them in
// place and restarts containerd, and puts the old ones back if the new
// containerd doesn't answer.
func (s *agentServer) UpdateContainerd(stream grpc.ClientStreamingServer[nodev1.UpdateContainerdRequest, nodev1.UpdateContainerdResponse]) error {
	b := s.containerdBinaries
	if !b.updating.TryLock() {
		return status.Error(codes.FailedPrecondition, "another update of containerd is running")
	}
	defer b.updating.Unlock()
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	update := first.Update
	if err := b.check(update); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	received, err := b.receive(stream, first, update.Files)
	defer received.remove()
	if err != nil {
		return err
	}
	// The update goes on even if the call ends.
	ctx := context.WithoutCancel(stream.Context())
	if err := b.swap(ctx, received, update.Version, update.HealthTimeout.AsDuration()); err != nil {
		return err
	}
	return stream.SendAndClose(&nodev1.UpdateContainerdResponse{})
}

// swap puts received in place of containerd's binaries, restarts
// containerd and waits for it to answer at version, all within timeout.
// If it doesn't, it puts the old binaries back and restarts containerd,
// within timeout again, and fails with ABORTED, or with INTERNAL if
// containerd doesn't answer then either.
func (b *containerdBinaries) swap(ctx context.Context, received newBinaries, version string, timeout time.Duration) error {
	if err := received.install(); err != nil {
		return b.rollBack(ctx, received, timeout, fmt.Errorf("putting containerd's binaries in place: %w", err))
	}
	swapCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := b.restart(swapCtx); err != nil {
		return b.rollBack(ctx, received, timeout, fmt.Errorf("restarting the new containerd: %w", err))
	}
	if err := b.wait(swapCtx, version); err != nil {
		return b.rollBack(ctx, received, timeout, fmt.Errorf("the new containerd didn't answer at %s within %s: %w", version, timeout, err))
	}
	received.dropPrevious()
	return nil
}

// rollBack puts the binaries that received replaced back, after the
// swap failed with cause, restarts containerd and waits for it to
// answer, all within timeout. It fails with ABORTED, saying why the swap
// failed, or, if putting them back fails too, with INTERNAL, saying
// both.
func (b *containerdBinaries) rollBack(ctx context.Context, received newBinaries, timeout time.Duration, cause error) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := received.restore(); err != nil {
		return status.Errorf(codes.Internal, "%v; rolling back to the containerd it replaced failed too: %v", cause, err)
	}
	if err := b.restart(ctx); err != nil {
		return status.Errorf(codes.Internal, "%v; rolling back to the containerd it replaced failed too: restarting containerd: %v", cause, err)
	}
	if err := b.wait(ctx, ""); err != nil {
		return status.Errorf(codes.Internal, "%v; rolled back to the containerd it replaced, which doesn't answer either: %v", cause, err)
	}
	return status.Errorf(codes.Aborted, "%v; rolled back to the containerd it replaced", cause)
}

// check fails unless update says which containerd it is, with each of
// containerd's binaries once, and no other file.
func (b *containerdBinaries) check(update *nodev1.ContainerdUpdate) error {
	switch {
	case update == nil:
		return errors.New("the first request doesn't say which containerd it is")
	case update.Version == "":
		return errors.New("the update has no version")
	case update.HealthTimeout.AsDuration() <= 0:
		return errors.New("the update has no health timeout")
	}
	var names []string
	for _, f := range update.Files {
		switch {
		case !slices.Contains(b.files, f.Name):
			return fmt.Errorf("%q isn't one of containerd's binaries, %s", f.Name, strings.Join(b.files, ", "))
		case slices.Contains(names, f.Name):
			return fmt.Errorf("the update has %s twice", f.Name)
		case len(f.Sha256) != sha256.Size*2:
			return fmt.Errorf("the update has no SHA-256 for %s", f.Name)
		}
		names = append(names, f.Name)
	}
	for _, name := range b.files {
		if !slices.Contains(names, name) {
			return fmt.Errorf("the update has no %s", name)
		}
	}
	return nil
}

// newBinary is one of containerd's new binaries, as it is received next
// to the binary it replaces.
type newBinary struct {
	file *os.File
	hash hash.Hash
	// tmp is where it is received, path the binary it replaces, and prev
	// where the one it replaces is kept once it is installed, if there
	// was one.
	tmp, path, prev string
	sha256          string
	installed, kept bool
}

// newBinaries are containerd's new binaries, by name.
type newBinaries map[string]*newBinary

// receive writes the binaries that stream carries, from first on, next
// to the ones they replace, and checks each against its SHA-256 in
// files. It fails with INVALID_ARGUMENT for a chunk of another file, or
// a binary that doesn't match. Whether it fails or not, the caller
// removes what it returns once done with it.
func (b *containerdBinaries) receive(stream grpc.ClientStreamingServer[nodev1.UpdateContainerdRequest, nodev1.UpdateContainerdResponse], first *nodev1.UpdateContainerdRequest, files []*nodev1.ContainerdFile) (newBinaries, error) {
	received := newBinaries{}
	for _, f := range files {
		path := filepath.Join(b.dir, f.Name)
		tmp := path + ".new"
		file, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o755)
		if err != nil {
			return received, err
		}
		received[f.Name] = &newBinary{file: file, hash: sha256.New(), tmp: tmp, path: path, prev: path + ".prev", sha256: strings.ToLower(f.Sha256)}
	}
	req := first
	for {
		if len(req.Chunk) > 0 {
			nb, ok := received[req.File]
			if !ok {
				return received, status.Errorf(codes.InvalidArgument, "a chunk of %q, which isn't one of the update's files", req.File)
			}
			if _, err := io.MultiWriter(nb.file, nb.hash).Write(req.Chunk); err != nil {
				return received, err
			}
		}
		var err error
		req, err = stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return received, err
		}
	}
	for name, nb := range received {
		if got := hex.EncodeToString(nb.hash.Sum(nil)); got != nb.sha256 {
			return received, status.Errorf(codes.InvalidArgument, "%s's SHA-256 is %s, not %s", name, got, nb.sha256)
		}
		if err := nb.file.Sync(); err != nil {
			return received, err
		}
	}
	return received, nil
}

// install renames each binary over the one it replaces, which a running
// program keeps running from, keeping the old one as its prev, and syncs
// their directory.
func (r newBinaries) install() error {
	var dir string
	for _, nb := range r {
		if err := nb.file.Close(); err != nil {
			return err
		}
		// A prev there is left from an update that stopped.
		if err := os.Remove(nb.prev); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		// A binary that is missing has nothing to keep, and a rollback
		// removes the new one.
		err := os.Link(nb.path, nb.prev)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		nb.kept = err == nil
		if err := os.Rename(nb.tmp, nb.path); err != nil {
			return err
		}
		nb.installed = true
		dir = filepath.Dir(nb.path)
	}
	return syncDir(dir)
}

// restore puts back the binaries that install replaced, removes those
// it put where none was, and syncs their directory.
func (r newBinaries) restore() error {
	var dir string
	for _, nb := range r {
		if !nb.installed {
			continue
		}
		var err error
		if nb.kept {
			err = os.Rename(nb.prev, nb.path)
		} else {
			err = os.Remove(nb.path)
		}
		if err != nil {
			return err
		}
		nb.installed = false
		dir = filepath.Dir(nb.path)
	}
	if dir == "" {
		return nil
	}
	return syncDir(dir)
}

// dropPrevious removes the binaries that install replaced, once the new
// ones are known to work.
func (r newBinaries) dropPrevious() {
	for _, nb := range r {
		os.Remove(nb.prev)
	}
}

// remove closes and removes the binaries that weren't installed.
func (r newBinaries) remove() {
	for _, nb := range r {
		nb.file.Close()
		os.Remove(nb.tmp)
	}
}

// waitContainerd waits until containerd answers healthy, at version
// unless that is empty, or ctx ends.
func (s *agentServer) waitContainerd(ctx context.Context, version string) error {
	for {
		c := s.containerdStatus(ctx)
		var err error
		switch {
		case c.Health != nodev1.Health_HEALTH_HEALTHY:
			err = fmt.Errorf("containerd is unhealthy: %s", c.Problem)
		case version != "" && c.Version != version:
			err = fmt.Errorf("containerd runs version %s, not %s", c.Version, version)
		default:
			return nil
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(250 * time.Millisecond):
		}
	}
}
