//go:build dev

package bootstrap

import (
	"fmt"
	"os"
	"time"
)

// healthWait is how long Install waits for the node components to become
// healthy. A dev build takes it from MOOSTACK_DEV_HEALTH_TIMEOUT, a Go
// duration, if that is set, so that tests of a node that never becomes
// healthy don't wait out healthTimeout.
func healthWait() (time.Duration, error) {
	s := os.Getenv("MOOSTACK_DEV_HEALTH_TIMEOUT")
	if s == "" {
		return healthTimeout, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("MOOSTACK_DEV_HEALTH_TIMEOUT=%q: want a positive duration, such as 10s", s)
	}
	return d, nil
}
