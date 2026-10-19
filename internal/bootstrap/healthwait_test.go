//go:build !dev

package bootstrap

import (
	"testing"
	"time"
)

func TestOnlyADevBuildReadsTheHealthTimeoutFromTheEnvironment(t *testing.T) {
	t.Setenv("MOOSTACK_DEV_HEALTH_TIMEOUT", "1s")
	if got, err := healthWait(); err != nil || got != time.Minute {
		t.Errorf("healthWait is %s, %v, want a minute in a build without the dev tag", got, err)
	}
}
