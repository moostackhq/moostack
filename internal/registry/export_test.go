package registry

import "time"

// SetTimeouts sets the dial and response timeouts until the test ends.
func SetTimeouts(dial, response time.Duration) (restore func()) {
	d, r := dialTimeout, responseTimeout
	dialTimeout, responseTimeout = dial, response
	return func() { dialTimeout, responseTimeout = d, r }
}
