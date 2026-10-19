//go:build !dev

package cli

import (
	"time"

	"github.com/moostackhq/moostack/internal/orchestrate"
)

// leaseTTL is how long a node holds the operation lock after an
// operation last renewed it: orchestrate.DefaultLeaseTTL.
func leaseTTL() (time.Duration, error) { return orchestrate.DefaultLeaseTTL, nil }
