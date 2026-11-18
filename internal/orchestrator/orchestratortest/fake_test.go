package orchestratortest_test

import (
	"testing"

	"github.com/moostackhq/moostack/internal/orchestrator"
	"github.com/moostackhq/moostack/internal/orchestrator/orchestratortest"
)

func TestTheFakeKeepsThePortsPromises(t *testing.T) {
	orchestratortest.Contract(t, func(t *testing.T, pods []orchestratortest.Pod) orchestrator.Orchestrator {
		return orchestratortest.NewFake(pods)
	})
}
