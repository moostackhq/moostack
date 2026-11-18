// Package console is the console's HTTP server.
package console

import (
	"bytes"
	"context"
	"embed"
	"html/template"
	"log/slog"
	"net/http"
	"time"

	"github.com/moostackhq/moostack/internal/orchestrator"
)

//go:embed templates static
var files embed.FS

// readTimeout is how long reading the cluster for a page may take.
const readTimeout = 20 * time.Second

var templates = template.Must(template.ParseFS(files, "templates/*.html"))

type containersPage struct {
	Groups  []containerGroup
	Problem string
}

// containerGroup is the containers of one namespace.
type containerGroup struct {
	Namespace  string
	Containers []orchestrator.Container
}

// New returns the console's handler. Every route is under /console.
func New(o orchestrator.Orchestrator) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /console", http.RedirectHandler("/console/", http.StatusPermanentRedirect))
	mux.HandleFunc("GET /console/{$}", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), readTimeout)
		defer cancel()
		containers, err := o.Running(ctx)
		if err != nil {
			slog.ErrorContext(ctx, "reading the cluster", "err", err)
			render(w, http.StatusBadGateway, "containers.html", containersPage{Problem: "The cluster can't be read."})
			return
		}
		render(w, http.StatusOK, "containers.html", containersPage{Groups: groupByNamespace(containers)})
	})
	mux.HandleFunc("GET /console/static/{file}", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFileFS(w, r, files, "static/"+r.PathValue("file"))
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Security-Policy", "default-src 'self'; frame-ancestors 'none'")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		mux.ServeHTTP(w, r)
	})
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

func render(w http.ResponseWriter, code int, name string, data any) {
	var buf bytes.Buffer
	if err := templates.ExecuteTemplate(&buf, name, data); err != nil {
		slog.Error("rendering "+name, "err", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	w.Write(buf.Bytes())
}
