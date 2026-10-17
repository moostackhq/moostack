package filelock_test

import (
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/moostackhq/moostack/internal/filelock"
)

func TestLockIsExclusive(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lock")
	var holders, most atomic.Int32
	done := make(chan error)
	for range 10 {
		go func() {
			unlock, err := filelock.Lock(path)
			if err != nil {
				done <- err
				return
			}
			n := holders.Add(1)
			for {
				m := most.Load()
				if n <= m || most.CompareAndSwap(m, n) {
					break
				}
			}
			time.Sleep(5 * time.Millisecond)
			holders.Add(-1)
			unlock()
			done <- nil
		}()
	}
	for range 10 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if most.Load() != 1 {
		t.Errorf("%d held the lock at once, want 1", most.Load())
	}
}

func TestLockCreatesTheFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lock")

	unlock, err := filelock.Lock(path)

	if err != nil {
		t.Fatal(err)
	}
	unlock()
	unlock, err = filelock.Lock(path)
	if err != nil {
		t.Fatalf("relocking: %v", err)
	}
	unlock()
}
