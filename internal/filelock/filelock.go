// Package filelock takes exclusive locks on files, across processes and
// within one, on every system that has file locks.
package filelock

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// processLocks serialise locks within this process, which a file lock
// alone doesn't do everywhere: fcntl locks belong to the process. There
// is one per lock file this process has used.
var processLocks sync.Map

// Lock takes an exclusive lock on the file at path, creating it if it
// doesn't exist, and returns the function that releases it. It waits
// while another holder has the lock, in this process or another. The
// system releases the lock if the process dies, so a lock is never left
// behind. The file must never be removed while in use, since a new file
// at path would be a lock of its own.
func Lock(path string) (unlock func(), err error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	mu, _ := processLocks.LoadOrStore(abs, &sync.Mutex{})
	mu.(*sync.Mutex).Lock()
	// Everyone who can use the store must be able to open its lock.
	f, err := os.OpenFile(abs, os.O_RDWR|os.O_CREATE, 0o666)
	if err == nil {
		if err = lockExclusive(f); err != nil {
			f.Close()
		}
	}
	if err != nil {
		mu.(*sync.Mutex).Unlock()
		return nil, fmt.Errorf("locking %s: %w", path, err)
	}
	return func() {
		// Closing the file releases the lock.
		f.Close()
		mu.(*sync.Mutex).Unlock()
	}, nil
}
