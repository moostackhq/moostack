// Package console is the console's HTTP server.
package console

import (
	"bytes"
	"context"
	"embed"
	"html/template"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/moostackhq/moostack/internal/orchestrator"
)

//go:embed templates static
var files embed.FS

var templates = template.Must(template.ParseFS(files, "templates/*.html"))

// Every route of Console is under /console.
type Console struct {
	orchestrator orchestrator.Orchestrator
	tokens       *tokens
	sessions     *sessions
	mux          *http.ServeMux
}

func New(o orchestrator.Orchestrator) *Console {
	c := &Console{orchestrator: o, tokens: newTokens(), sessions: newSessions(), mux: http.NewServeMux()}
	c.mux.Handle("GET /console", http.RedirectHandler("/console/", http.StatusPermanentRedirect))
	c.mux.HandleFunc("GET /console/{$}", c.handleContainers)
	c.mux.HandleFunc("GET /console/login", c.handleLoginPage)
	c.mux.HandleFunc("POST /console/login", c.handleLogin)
	c.mux.HandleFunc("POST /console/logout", c.handleLogout)
	c.mux.HandleFunc("GET /console/static/{file}", c.handleStatic)
	return c
}

func (c *Console) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Security-Policy", "default-src 'self'; frame-ancestors 'none'")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	c.mux.ServeHTTP(w, r)
}

// Serve serves the console on web, and its login tokens on
// tokenSocket, until ctx ends or web fails. It then closes both: the
// requests in flight get 20 seconds to end, and those still open after
// that are cut.
func (c *Console) Serve(ctx context.Context, web, tokenSocket net.Listener) error {
	pages := &http.Server{
		Handler:           c,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      time.Minute,
		IdleTimeout:       2 * time.Minute,
	}
	go c.serveTokens(tokenSocket)
	ended := make(chan error, 1)
	go func() { ended <- pages.Serve(web) }()
	var err error
	select {
	case err = <-ended:
	case <-ctx.Done():
	}

	tokenSocket.Close()
	shutdown, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if pages.Shutdown(shutdown) != nil {
		pages.Close()
	}
	return err
}

func (c *Console) handleStatic(w http.ResponseWriter, r *http.Request) {
	http.ServeFileFS(w, r, files, "static/"+r.PathValue("file"))
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
