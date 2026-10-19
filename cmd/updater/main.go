//go:build linux

// Command updater is the node component that will replace the agent
// with a new version. For now it serves its own status.
//
//	updater serve          serve the updater's node API on its socket
//	updater relay SOCKET   connect stdin and stdout to a unix socket
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/moostackhq/moostack/internal/buildinfo"
	"github.com/moostackhq/moostack/internal/hostlayout"
	"github.com/moostackhq/moostack/internal/rootsocket"
	"github.com/moostackhq/moostack/internal/updater"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "updater: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	switch {
	case len(args) == 1 && args[0] == "serve":
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		return updater.Serve(ctx, updater.Config{
			Version:  buildinfo.Version,
			Socket:   hostlayout.UpdaterSocket,
			Identity: hostlayout.Identity,
		})
	case len(args) == 2 && args[0] == "relay":
		return rootsocket.Relay(args[1], os.Stdin, os.Stdout)
	}
	return errors.New(usage)
}

const usage = `usage:
  updater serve          serve the updater's node API on its socket
  updater relay SOCKET   connect stdin and stdout to a unix socket`
