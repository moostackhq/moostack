// Command console serves a cluster's console from inside the cluster.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/moostackhq/moostack/internal/console"
	"github.com/moostackhq/moostack/internal/orchestrator/k3s"
)

func main() {
	listen := flag.String("listen", ":8080", "the address to serve on")
	flag.Parse()
	if err := serve(*listen); err != nil {
		fmt.Fprintf(os.Stderr, "console: %v\n", err)
		os.Exit(1)
	}
}

// serve runs until SIGTERM or an interrupt, then lets the requests in
// flight end.
func serve(listen string) error {
	backend, err := k3s.InCluster()
	if err != nil {
		return err
	}
	srv := &http.Server{
		Addr:              listen,
		Handler:           console.New(backend),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      time.Minute,
		IdleTimeout:       2 * time.Minute,
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	stopped := make(chan error, 1)
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		stopped <- srv.Shutdown(shutdown)
	}()
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return <-stopped
}
