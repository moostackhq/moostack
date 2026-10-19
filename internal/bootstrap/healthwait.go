//go:build !dev

package bootstrap

import "time"

// healthWait is how long Install waits for the node components to become
// healthy: healthTimeout.
func healthWait() (time.Duration, error) { return healthTimeout, nil }
