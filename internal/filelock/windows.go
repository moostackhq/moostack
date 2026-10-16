//go:build windows

package filelock

import (
	"os"

	"golang.org/x/sys/windows"
)

// lockExclusive waits for an exclusive lock on f, with LockFileEx.
func lockExclusive(f *os.File) error {
	return windows.LockFileEx(windows.Handle(f.Fd()), windows.LOCKFILE_EXCLUSIVE_LOCK, 0, 1, 0, &windows.Overlapped{})
}
