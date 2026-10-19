//go:build dev

package bootstrap

import (
	"testing"
	"time"
)

func TestADevBuildReadsTheHealthTimeoutFromTheEnvironment(t *testing.T) {
	t.Setenv("MOOSTACK_DEV_HEALTH_TIMEOUT", "10s")
	if got, err := healthWait(); err != nil || got != 10*time.Second {
		t.Errorf("healthWait is %s, %v, want 10s", got, err)
	}
}

func TestADevBuildWithoutTheVariableWaitsAMinute(t *testing.T) {
	t.Setenv("MOOSTACK_DEV_HEALTH_TIMEOUT", "")
	if got, err := healthWait(); err != nil || got != time.Minute {
		t.Errorf("healthWait is %s, %v, want a minute", got, err)
	}
}

func TestADevBuildRefusesABadHealthTimeout(t *testing.T) {
	for _, s := range []string{"ten", "0s", "-1s"} {
		t.Setenv("MOOSTACK_DEV_HEALTH_TIMEOUT", s)
		if got, err := healthWait(); err == nil {
			t.Errorf("MOOSTACK_DEV_HEALTH_TIMEOUT=%s: healthWait is %s, want an error", s, got)
		}
	}
}
