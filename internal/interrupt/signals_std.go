//go:build unix || windows

package interrupt

import (
	"os"
	"syscall"
)

// stopSignals are the signals that end a command.
var stopSignals = []os.Signal{syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP}

// exitCode is the exit code after sig, as a shell gives a process that
// sig killed.
func exitCode(sig os.Signal) int {
	return 128 + int(sig.(syscall.Signal))
}
