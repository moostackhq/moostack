package console

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"slices"

	"github.com/moostackhq/moostack/internal/model"
	"github.com/moostackhq/moostack/internal/orchestrator"
)

// logLines is how many of a container's last lines its page shows, and
// logBytes how many bytes of them at most.
const (
	logLines = 100
	logBytes = 1 << 20
)

type containerPage struct {
	Container model.Running
	Logs      string
	Problem   string
}

func (c *Console) handleContainer(w http.ResponseWriter, r *http.Request) {
	if !c.hasSession(r) {
		http.Redirect(w, r, "/console/login", http.StatusSeeOther)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), readTimeout)
	defer cancel()
	id := r.URL.Query().Get("id")
	containers, err := c.orchestrator.Running(ctx)
	if r.Context().Err() != nil {
		return
	}
	if err != nil {
		slog.ErrorContext(ctx, "reading the cluster", "err", err)
		render(w, http.StatusBadGateway, "containers.html", containersPage{Problem: "The cluster can't be read."})
		return
	}
	i := slices.IndexFunc(containers, func(running model.Running) bool { return running.ID == id })
	if i < 0 {
		render(w, http.StatusNotFound, "containers.html", containersPage{Problem: "No such running container."})
		return
	}
	page := containerPage{Container: containers[i]}
	logs := &capped{left: logBytes}
	err = c.orchestrator.ContainerLogs(ctx, id, orchestrator.LogOptions{Tail: logLines}, logs)
	if r.Context().Err() != nil {
		return
	}
	switch {
	case errors.Is(err, orchestrator.ErrNoContainer):
		render(w, http.StatusNotFound, "containers.html", containersPage{Problem: "No such running container."})
		return
	case err != nil && !errors.Is(err, errCapped):
		slog.ErrorContext(ctx, "reading a container's logs", "container", id, "err", err)
		page.Problem = "The logs can't be read."
		render(w, http.StatusBadGateway, "container.html", page)
		return
	}
	page.Logs = logs.buf.String()
	render(w, http.StatusOK, "container.html", page)
}

// capped keeps the first left bytes written to it, and fails the write
// that has more with errCapped.
type capped struct {
	buf  bytes.Buffer
	left int
}

var errCapped = errors.New("the logs are longer than the page shows")

func (c *capped) Write(p []byte) (int, error) {
	if len(p) > c.left {
		n, _ := c.buf.Write(p[:c.left])
		c.left = 0
		return n, errCapped
	}
	c.left -= len(p)
	return c.buf.Write(p)
}
