//go:build !windows

package terminal

import "os"

// openTerminal opens /dev/tty, which is read from and written to.
func openTerminal() (in, out *os.File, err error) {
	f, err := os.OpenFile("/dev/tty", os.O_RDWR, 0)
	return f, f, err
}
