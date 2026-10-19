//go:build dev

package cli

import (
	"fmt"
	"os"
	"time"

	"github.com/moostackhq/moostack/internal/orchestrate"
)

// leaseTTL is how long a node holds the operation lock after an
// operation last renewed it. A dev build takes it from
// MOOSTACK_DEV_LEASE_TTL, a Go duration, if that is set, so that a lease
// left by a mooctl that died can be seen to run out without waiting out
// orchestrate.DefaultLeaseTTL.
func leaseTTL() (time.Duration, error) {
	s := os.Getenv("MOOSTACK_DEV_LEASE_TTL")
	if s == "" {
		return orchestrate.DefaultLeaseTTL, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("MOOSTACK_DEV_LEASE_TTL=%q: want a positive duration, such as 10s", s)
	}
	return d, nil
}
