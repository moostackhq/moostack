//go:build linux

// Command agent is the node component that serves the node API.
//
//	agent serve              serve the node API on the agent's socket, and listen for peers
//	agent relay SOCKET       connect stdin and stdout to a unix socket
//	agent request-identity   make the node's key, and print a request for its identity
//	agent join               join the control plane of the join token on stdin, and print the identity it issued
//	agent detach [--yes]     break-glass detach: return a node in control plane mode to direct mode, for good
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"

	controlplanev1 "github.com/moostackhq/moostack/api/controlplane/v1"
	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/agent"
	"github.com/moostackhq/moostack/internal/buildinfo"
	"github.com/moostackhq/moostack/internal/controlplane"
	"github.com/moostackhq/moostack/internal/hostlayout"
	"github.com/moostackhq/moostack/internal/identity"
	"github.com/moostackhq/moostack/internal/rootsocket"
	"github.com/moostackhq/moostack/internal/systemd"
	"github.com/moostackhq/moostack/internal/terminal"
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
			Version:               buildinfo.Version,
			Socket:                hostlayout.AgentSocket,
			NodeFile:              hostlayout.NodeFile,
			GenerationFile:        hostlayout.GenerationFile,
			LockFile:              hostlayout.LockFile,
			ContainerdSocket:      hostlayout.ContainerdSocket,
			Runc:                  hostlayout.Runc,
			ContainersFile:        hostlayout.ContainersFile,
			ContainersDir:         hostlayout.ContainersDir,
			ContainerLogDir:       hostlayout.ContainerLogDir,
			Identity:              hostlayout.Identity,
			RemovedIdentitiesFile: hostlayout.RemovedIdentitiesFile,
			PeerPort:              hostlayout.PeerPort,
			Updater:               hostlayout.Updater,
			UpdaterSlots:          hostlayout.UpdaterSlots,
			UpdaterSwapFile:       hostlayout.UpdaterSwapFile,
			UpdaterSocket:         hostlayout.UpdaterSocket,
			RestartUpdater:        func(ctx context.Context) error { return systemd.Restart(ctx, hostlayout.UpdaterUnit) },
			ContainerdBinDir:      hostlayout.BinDir,
			ContainerdFiles:       hostlayout.ContainerdFiles,
			RestartContainerd:     func(ctx context.Context) error { return systemd.Restart(ctx, hostlayout.ContainerdUnit) },
		})
	case len(args) == 2 && args[0] == "relay":
		return rootsocket.Relay(args[1], os.Stdin, os.Stdout)
	case len(args) == 1 && args[0] == "request-identity":
		request, err := identity.NewKey(hostlayout.Identity.Key)
		if err != nil {
			return err
		}
		_, err = os.Stdout.Write(request)
		return err
	case len(args) == 1 && args[0] == "join":
		return join()
	case len(args) == 1 && args[0] == "detach":
		return detach(false)
	case len(args) == 2 && args[0] == "detach" && args[1] == "--yes":
		return detach(true)
	}
	return errors.New(usage)
}

// joinTimeout bounds joining the control plane.
const joinTimeout = time.Minute

// join joins the control plane of the join token on stdin, making the
// node's key, and prints what it issued, as JSON.
func join() error {
	line, err := bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil && err != io.EOF {
		return err
	}
	tok, err := controlplanev1.ParseToken(strings.TrimSpace(line))
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), joinTimeout)
	defer cancel()
	joined, err := controlplane.Join(ctx, tok, hostlayout.Identity.Key)
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(joined)
}

// detachTimeout bounds a detach.
const detachTimeout = time.Minute

// detach has the agent detach the node, on its root-only socket, once
// the operator confirms on the terminal, unless yes says they have.
func detach(yes bool) error {
	if !yes {
		// What would refuse the detach refuses it before the question.
		if os.Geteuid() != 0 {
			return errors.New("detaching the node: run agent detach as root")
		}
		where := "its control plane"
		if node, err := hostlayout.ReadNodeConfig(hostlayout.NodeFile); err == nil && node.Mode == hostlayout.ModeDirect {
			return errors.New("detaching the node: the node is in direct mode already")
		} else if err == nil {
			where = "the control plane at " + node.ControlPlane
		}
		t, closer := terminal.Open(nil)
		if t == nil {
			return errors.New("agent detach can't be undone, and there is no terminal to confirm on: pass --yes to detach the node anyway")
		}
		defer closer.Close()
		fmt.Fprintf(t, "Detach this node from %s for good? The control plane can't reach it any more, and it can't join again without being reinstalled. [y/N] ", where)
		answer, err := t.ReadLine()
		if err == io.EOF {
			fmt.Fprintln(t)
			answer, err = "", nil
		}
		if err != nil {
			return err
		}
		if a := strings.ToLower(strings.TrimSpace(answer)); a != "y" && a != "yes" {
			return errors.New("not detached")
		}
	}
	conn, err := rootsocket.Dial(hostlayout.AgentSocket)
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), detachTimeout)
	defer cancel()
	if _, err := nodev1.NewAgentClient(conn).Detach(ctx, &nodev1.DetachRequest{}); err != nil {
		msg := status.Convert(err).Message()
		if os.Geteuid() != 0 {
			msg += "; run agent detach as root"
		}
		return fmt.Errorf("detaching the node: %s", msg)
	}
	// The updater follows once it reads the node file.
	if err := waitUpdaterDirect(ctx); err != nil {
		return fmt.Errorf("the agent is detached, but %v: restart the updater with systemctl restart %s", err, hostlayout.UpdaterUnit)
	}
	fmt.Println("Detached the node from its control plane: it is in direct mode, and the control plane can't reach it any more.")
	return nil
}

// updaterFollowTimeout bounds waiting for the updater to follow a
// detach, which it does within a second of reading the node file.
const updaterFollowTimeout = 15 * time.Second

// waitUpdaterDirect waits until the updater says it serves in direct
// mode, or doesn't say, as one of a release before it said.
func waitUpdaterDirect(ctx context.Context) error {
	conn, err := rootsocket.Dial(hostlayout.UpdaterSocket)
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(ctx, updaterFollowTimeout)
	defer cancel()
	updater := nodev1.NewUpdaterClient(conn)
	for {
		res, err := updater.Status(ctx, &nodev1.UpdaterStatusRequest{}, grpc.WaitForReady(true))
		if err == nil && res.Mode != nodev1.Mode_MODE_CONTROL_PLANE {
			return nil
		}
		select {
		case <-ctx.Done():
			if err != nil {
				return fmt.Errorf("the updater doesn't answer: %s", status.Convert(err).Message())
			}
			return fmt.Errorf("the updater still serves the control plane after %s", updaterFollowTimeout)
		case <-time.After(200 * time.Millisecond):
		}
	}
}

const usage = `usage:
  agent serve              serve the node API on the agent's socket, and listen for peers
  agent relay SOCKET       connect stdin and stdout to a unix socket
  agent request-identity   make the node's key, and print a request for its identity
  agent join               join the control plane of the join token on stdin, and print the identity it issued
  agent detach [--yes]     break-glass detach: return a node in control plane mode to direct mode, for good`
