//go:build linux

// Command updater is the node component that replaces the agent with a
// new version, and serves its own status.
//
//	updater serve          serve the updater's node API on its socket
//	updater relay SOCKET   connect stdin and stdout to a unix socket
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strings"
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
			Version:       buildinfo.Version,
			Socket:        hostlayout.UpdaterSocket,
			Identity:      hostlayout.Identity,
			Agent:         hostlayout.Agent,
			AgentSlots:    hostlayout.AgentSlots,
			AgentSwapFile: hostlayout.AgentSwapFile,
			AgentSocket:   hostlayout.AgentSocket,
			RestartAgent:  restartAgent,
		})
	case len(args) == 2 && args[0] == "relay":
		return rootsocket.Relay(args[1], os.Stdin, os.Stdout)
	}
	return errors.New(usage)
}

// restartAgent restarts the agent's unit. It resets the unit's failed
// state first, since an agent that kept exiting has the unit refuse to
// start again for a while, as when rolling back from a broken one.
func restartAgent(ctx context.Context) error {
	for _, args := range [][]string{{"reset-failed", hostlayout.AgentUnit}, {"restart", hostlayout.AgentUnit}} {
		out, err := exec.CommandContext(ctx, "systemctl", args...).CombinedOutput()
		if err != nil {
			return fmt.Errorf("systemctl %s: %w: %s", strings.Join(args, " "), err, bytes.TrimSpace(out))
		}
	}
	return nil
}

const usage = `usage:
  updater serve          serve the updater's node API on its socket
  updater relay SOCKET   connect stdin and stdout to a unix socket`
