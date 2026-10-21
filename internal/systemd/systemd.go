//go:build linux

// Package systemd restarts the node components' units.
package systemd

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
)

// Restart restarts unit. It resets the unit's failed state first, since
// a program that kept exiting has its unit refuse to start again for a
// while, as when rolling back from a broken one.
func Restart(ctx context.Context, unit string) error {
	for _, args := range [][]string{{"reset-failed", unit}, {"restart", unit}} {
		out, err := exec.CommandContext(ctx, "systemctl", args...).CombinedOutput()
		if err != nil {
			return fmt.Errorf("systemctl %s: %w: %s", strings.Join(args, " "), err, bytes.TrimSpace(out))
		}
	}
	return nil
}
