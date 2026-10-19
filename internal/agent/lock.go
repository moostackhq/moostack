//go:build linux

package agent

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
)

// operationLock is the node's operation lock: which lease holds it, and
// until when. It keeps the lease on disk, so that a restarted agent
// holds the lock for the lease for a whole ttl from when it starts,
// which is never sooner than the lease would have run out, unless the
// lease ran out long before the restart.
type operationLock struct {
	file string
	mu   sync.Mutex
	// held is the lease that holds the lock, nil if none, and expires
	// when it stops holding it unless renewed.
	held    *heldLease
	expires time.Time
}

// heldLease is a lease that holds the lock, as the lock file keeps it.
type heldLease struct {
	Lease   string        `json:"lease"`
	Holder  string        `json:"holder"`
	Machine string        `json:"machine"`
	Since   time.Time     `json:"since"`
	TTL     time.Duration `json:"ttl"`
	// Until is when the lease runs out unless renewed, on the wall clock.
	Until time.Time `json:"until"`
}

// proto is h as the node API reports it, running out at until, nil if h
// is.
func (h *heldLease) proto(until time.Time) *nodev1.Lock {
	if h == nil {
		return nil
	}
	return &nodev1.Lock{Lease: h.Lease, Holder: h.Holder, Machine: h.Machine, Since: timestamppb.New(h.Since), Until: timestamppb.New(until)}
}

// loadLock is the lock kept in file, held from now for its lease's ttl,
// unless the lease ran out a whole ttl ago or more, on the wall clock:
// a wall clock stepped by less than a ttl then can't free a live lease
// early. A file that can't be read leaves the lock free: the file is
// replaced whole or not at all, so only someone editing it by hand
// breaks it.
func loadLock(file string) *operationLock {
	l := &operationLock{file: file}
	data, err := os.ReadFile(file)
	if err != nil {
		return l
	}
	var h heldLease
	if json.Unmarshal(data, &h) == nil && h.Lease != "" && time.Now().Before(h.Until.Add(h.TTL)) {
		l.held, l.expires = &h, time.Now().Add(h.TTL)
	}
	return l
}

// current is the lease that holds the lock now, nil if none. l.mu must
// be held.
func (l *operationLock) current() *heldLease {
	if l.held != nil && !time.Now().Before(l.expires) {
		l.held = nil
	}
	return l.held
}

// save keeps h, or that no lease holds the lock if h is nil, on disk.
// l.mu must be held.
func (l *operationLock) save(h *heldLease) error {
	if h == nil {
		err := os.Remove(l.file)
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		return syncDir(filepath.Dir(l.file))
	}
	data, err := json.Marshal(h)
	if err != nil {
		return err
	}
	return writeFile(l.file, append(data, '\n'))
}

func (s *agentServer) AcquireLock(_ context.Context, req *nodev1.AcquireLockRequest) (*nodev1.AcquireLockResponse, error) {
	want, ttl := req.GetLock(), req.GetTtl().AsDuration()
	if want.GetLease() == "" || ttl <= 0 {
		return nil, status.Error(codes.InvalidArgument, "acquiring the lock takes a lease and a positive ttl")
	}
	l := s.lock
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	h := l.current()
	if h != nil && h.Lease != want.GetLease() {
		return &nodev1.AcquireLockResponse{Lock: h.proto(l.expires)}, nil
	}
	next := &heldLease{Lease: want.GetLease(), Holder: want.GetHolder(), Machine: want.GetMachine(), Since: now, TTL: ttl, Until: now.Add(ttl)}
	if h != nil {
		next.Since = h.Since
	}
	if err := l.save(next); err != nil {
		return nil, status.Errorf(codes.Internal, "keeping the lock: %v", err)
	}
	l.held, l.expires = next, now.Add(ttl)
	return &nodev1.AcquireLockResponse{Lock: next.proto(l.expires)}, nil
}

func (s *agentServer) RenewLock(_ context.Context, req *nodev1.RenewLockRequest) (*nodev1.RenewLockResponse, error) {
	ttl := req.GetTtl().AsDuration()
	if ttl <= 0 {
		return nil, status.Error(codes.InvalidArgument, "renewing the lock takes a positive ttl")
	}
	l := s.lock
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	h := l.current()
	if h == nil || h.Lease != req.GetLease() {
		return &nodev1.RenewLockResponse{Lock: h.proto(l.expires)}, nil
	}
	next := *h
	next.TTL, next.Until = ttl, now.Add(ttl)
	if err := l.save(&next); err != nil {
		return nil, status.Errorf(codes.Internal, "keeping the lock: %v", err)
	}
	l.held, l.expires = &next, now.Add(ttl)
	return &nodev1.RenewLockResponse{Lock: next.proto(l.expires)}, nil
}

func (s *agentServer) ReleaseLock(_ context.Context, req *nodev1.ReleaseLockRequest) (*nodev1.ReleaseLockResponse, error) {
	l := s.lock
	l.mu.Lock()
	defer l.mu.Unlock()
	if h := l.current(); h == nil || h.Lease != req.GetLease() {
		return &nodev1.ReleaseLockResponse{}, nil
	}
	if err := l.save(nil); err != nil {
		return nil, status.Errorf(codes.Internal, "releasing the lock: %v", err)
	}
	l.held = nil
	return &nodev1.ReleaseLockResponse{}, nil
}

func (s *agentServer) BreakLock(context.Context, *nodev1.BreakLockRequest) (*nodev1.BreakLockResponse, error) {
	l := s.lock
	l.mu.Lock()
	defer l.mu.Unlock()
	h := l.current()
	if err := l.save(nil); err != nil {
		return nil, status.Errorf(codes.Internal, "breaking the lock: %v", err)
	}
	l.held = nil
	return &nodev1.BreakLockResponse{Broken: h.proto(l.expires)}, nil
}

// lockStatus is the lock as Status reports it.
func (s *agentServer) lockStatus() *nodev1.Lock {
	s.lock.mu.Lock()
	defer s.lock.mu.Unlock()
	return s.lock.current().proto(s.lock.expires)
}
