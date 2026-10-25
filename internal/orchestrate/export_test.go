package orchestrate

import "time"

// SetUpdateTimeout sets updateTimeout to d for a test, and returns what
// puts it back.
func SetUpdateTimeout(d time.Duration) (restore func()) {
	old := updateTimeout
	updateTimeout = d
	return func() { updateTimeout = old }
}
