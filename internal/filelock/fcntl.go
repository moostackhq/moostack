//go:build aix || illumos || solaris

package filelock

import (
	"os"

	"golang.org/x/sys/unix"
)

// lockExclusive waits for an exclusive lock on f, with fcntl, as these
// systems have no flock. An fcntl lock belongs to the process, which
// Lock's process lock makes up for.
func lockExclusive(f *os.File) error {
	return unix.FcntlFlock(f.Fd(), unix.F_SETLKW, &unix.Flock_t{Type: unix.F_WRLCK})
}
