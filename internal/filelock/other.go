//go:build !(darwin || dragonfly || freebsd || linux || netbsd || openbsd || aix || illumos || solaris || windows)

package filelock

import (
	"errors"
	"os"
)

// lockExclusive fails: this system has no file lock to rely on.
func lockExclusive(*os.File) error {
	return errors.New("this system has no file locks")
}
