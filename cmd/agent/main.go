//go:build linux

// Command agent is the node component that serves the node API.
//
//	agent serve          serve the node API on the agent's socket
//	agent relay SOCKET   connect stdin and stdout to a unix socket
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/moostackhq/moostack/internal/agent"
	"github.com/moostackhq/moostack/internal/buildinfo"
	"github.com/moostackhq/moostack/internal/hostlayout"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "agent: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	switch {
	case len(args) == 1 && args[0] == "serve":
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		return agent.Serve(ctx, agent.Config{
			Version:          buildinfo.Version,
			Socket:           hostlayout.AgentSocket,
			NodeFile:         hostlayout.NodeFile,
			ContainerdSocket: hostlayout.ContainerdSocket,
		})
	case len(args) == 2 && args[0] == "relay":
		return agent.Relay(args[1], os.Stdin, os.Stdout)
	}
	return errors.New(usage)
}

const usage = `usage:
  agent serve          serve the node API on the agent's socket
  agent relay SOCKET   connect stdin and stdout to a unix socket`
