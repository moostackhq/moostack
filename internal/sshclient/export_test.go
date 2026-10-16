package sshclient

import (
	"testing"
	"time"
)

// SetAgentTimeout sets how long ssh-agent has to answer a request, until
// the test ends.
func SetAgentTimeout(t *testing.T, d time.Duration) {
	old := agentTimeout
	agentTimeout = d
	t.Cleanup(func() { agentTimeout = old })
}
