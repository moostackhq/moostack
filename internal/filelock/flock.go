//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package filelock

import (
	"os"

	"golang.org/x/sys/unix"
)

// lockExclusive waits for an exclusive lock on f, with flock.
func lockExclusive(f *os.File) error {
	return unix.Flock(int(f.Fd()), unix.LOCK_EX)
}
