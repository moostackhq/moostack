package orchestratortest

import (
	"context"
	"crypto/rand"
	"slices"
	"strings"
	"testing"

	"github.com/moostackhq/moostack/internal/orchestrator"
)

// Contract tests that an Orchestrator keeps what the port promises.
// setup adds pods to the cluster, creating their namespaces, and returns
// the cluster's Orchestrator. Each call of Contract uses namespaces of
// its own.
func Contract(t *testing.T, setup func(t *testing.T, pods []Pod) orchestrator.Orchestrator) {
	id := strings.ToLower(rand.Text()[:8])
	ns := func(name string) string { return name + "-" + id }

	// running is the Orchestrator's answer, cut to the namespaces of pods.
	running := func(t *testing.T, pods []Pod) []orchestrator.Container {
		t.Helper()
		all, err := setup(t, pods).Running(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		var own []orchestrator.Container
		for _, c := range all {
			if slices.ContainsFunc(pods, func(p Pod) bool { return p.Namespace == c.Namespace }) {
				own = append(own, c)
			}
		}
		return own
	}
	check := func(t *testing.T, got, want []orchestrator.Container) {
		t.Helper()
		if !slices.Equal(got, want) {
			t.Errorf("Running:\n got %+v\nwant %+v", got, want)
		}
	}

	t.Run("a running container is listed with its namespace, pod, name and image", func(t *testing.T) {
		got := running(t, []Pod{{Namespace: ns("contract-one"), Name: "web-1", Containers: []PodContainer{
			{Name: "web", Image: "example.test/web:1", Status: Running},
		}}})
		check(t, got, []orchestrator.Container{
			{Namespace: ns("contract-one"), Pod: "web-1", Name: "web", Image: "example.test/web:1"},
		})
	})

	t.Run("waiting and terminated containers are left out", func(t *testing.T) {
		got := running(t, []Pod{
			{Namespace: ns("contract-states"), Name: "mixed", Containers: []PodContainer{
				{Name: "up", Image: "example.test/up:1", Status: Running},
				{Name: "pulling", Image: "example.test/pulling:1", Status: Waiting},
				{Name: "done", Image: "example.test/done:1", Status: Terminated},
			}},
			{Namespace: ns("contract-states"), Name: "pending", Containers: []PodContainer{
				{Name: "pulling", Image: "example.test/pulling:1", Status: Waiting},
			}},
			{Namespace: ns("contract-idle"), Name: "finished", Containers: []PodContainer{
				{Name: "job", Image: "example.test/job:1", Status: Terminated},
			}},
		})
		check(t, got, []orchestrator.Container{
			{Namespace: ns("contract-states"), Pod: "mixed", Name: "up", Image: "example.test/up:1"},
		})
	})

	t.Run("containers are ordered by namespace, then pod, then name", func(t *testing.T) {
		got := running(t, []Pod{
			{Namespace: ns("contract-z"), Name: "a", Containers: []PodContainer{
				{Name: "a", Image: "example.test/a:1", Status: Running},
			}},
			{Namespace: ns("contract-m"), Name: "b", Containers: []PodContainer{
				{Name: "z", Image: "example.test/z:1", Status: Running},
				{Name: "a", Image: "example.test/a:1", Status: Running},
			}},
			{Namespace: ns("contract-m"), Name: "a", Containers: []PodContainer{
				{Name: "m", Image: "example.test/m:1", Status: Running},
			}},
			{Namespace: ns("contract-a"), Name: "z", Containers: []PodContainer{
				{Name: "c", Image: "example.test/c:1", Status: Running},
			}},
		})
		check(t, got, []orchestrator.Container{
			{Namespace: ns("contract-a"), Pod: "z", Name: "c", Image: "example.test/c:1"},
			{Namespace: ns("contract-m"), Pod: "a", Name: "m", Image: "example.test/m:1"},
			{Namespace: ns("contract-m"), Pod: "b", Name: "a", Image: "example.test/a:1"},
			{Namespace: ns("contract-m"), Pod: "b", Name: "z", Image: "example.test/z:1"},
			{Namespace: ns("contract-z"), Pod: "a", Name: "a", Image: "example.test/a:1"},
		})
	})
}
