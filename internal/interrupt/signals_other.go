//go:build !(unix || windows)

package interrupt

import "os"

// stopSignals are the signals that end a command: only an interrupt on
// this system.
var stopSignals = []os.Signal{os.Interrupt}

// exitCode is the exit code after an interrupt, as a shell gives a
// process that SIGINT killed.
func exitCode(os.Signal) int { return 130 }
