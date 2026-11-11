package buildx

import "time"

// SetStopDelay has a build stopped killed after d, for the test.
func SetStopDelay(t interface{ Cleanup(func()) }, d time.Duration) {
	old := stopDelay
	stopDelay = d
	t.Cleanup(func() { stopDelay = old })
}
