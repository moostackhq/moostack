package controlplane

import (
	"testing"
	"time"
)

// SetKeepalive sets how long a tunnel waits, idle, before it pings the
// control plane, and how long for its answer, until t ends.
func SetKeepalive(t *testing.T, interval, timeout time.Duration) {
	oldInterval, oldTimeout := keepaliveTime, keepaliveTimeout
	keepaliveTime, keepaliveTimeout = interval, timeout
	t.Cleanup(func() { keepaliveTime, keepaliveTimeout = oldInterval, oldTimeout })
}
