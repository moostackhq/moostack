package console

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/moostackhq/moostack/internal/orchestrator"
)

// readTimeout is how long reading the cluster for a page may take.
const readTimeout = 20 * time.Second

type containersPage struct {
	Groups  []containerGroup
	Problem string
}

type containerGroup struct {
	Namespace  string
	Containers []orchestrator.Container
}

func (c *Console) handleContainers(w http.ResponseWriter, r *http.Request) {
	if !c.hasSession(r) {
		http.Redirect(w, r, "/console/login", http.StatusSeeOther)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), readTimeout)
	defer cancel()
	containers, err := c.orchestrator.Running(ctx)
	if r.Context().Err() != nil {
		return
	}
	if err != nil {
		slog.ErrorContext(ctx, "reading the cluster", "err", err)
		render(w, http.StatusBadGateway, "containers.html", containersPage{Problem: "The cluster can't be read."})
		return
	}
	render(w, http.StatusOK, "containers.html", containersPage{Groups: groupByNamespace(containers)})
}

// groupByNamespace splits containers, which are ordered by namespace,
// into one group per namespace.
func groupByNamespace(containers []orchestrator.Container) []containerGroup {
	var groups []containerGroup
	for _, c := range containers {
		if len(groups) == 0 || groups[len(groups)-1].Namespace != c.Namespace {
			groups = append(groups, containerGroup{Namespace: c.Namespace})
		}
		last := &groups[len(groups)-1]
		last.Containers = append(last.Containers, c)
	}
	return groups
}
