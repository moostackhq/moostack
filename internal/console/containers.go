package console

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/moostackhq/moostack/internal/model"
)

// readTimeout is how long reading the cluster for a page may take.
const readTimeout = 20 * time.Second

type containersPage struct {
	Groups  []containerGroup
	Problem string
}

// containerGroup is the running containers of App, or the cluster's
// own if App is empty.
type containerGroup struct {
	App        string
	Containers []model.Running
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
	render(w, http.StatusOK, "containers.html", containersPage{Groups: groupByApp(containers)})
}

// groupByApp splits containers, which are ordered by app, into one
// group per app, and one of the cluster's own after them.
func groupByApp(containers []model.Running) []containerGroup {
	var groups []containerGroup
	for _, c := range containers {
		if len(groups) == 0 || groups[len(groups)-1].App != c.App {
			groups = append(groups, containerGroup{App: c.App})
		}
		last := &groups[len(groups)-1]
		last.Containers = append(last.Containers, c)
	}
	if len(groups) > 0 && groups[0].App == "" {
		groups = append(groups[1:], groups[0])
	}
	return groups
}
