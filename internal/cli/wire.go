package cli

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/netip"
	"strings"

	"github.com/moostackhq/moostack/internal/clusterdir"
	"github.com/moostackhq/moostack/internal/orchestrator"
	"github.com/moostackhq/moostack/internal/orchestrator/k3s"
	"github.com/moostackhq/moostack/internal/registry"
	"github.com/moostackhq/moostack/internal/sshclient"
)

// orchestratorThrough is the cluster's orchestrator, which reaches the
// cluster through cp, the connection to its control plane, or reaches
// none if cp is nil.
func orchestratorThrough(env Env, cp remote) orchestrator.Orchestrator {
	return orchestratorOf(env, clusterdir.Cluster{}, netip.Addr{}, cp)
}

// orchestratorOf is orchestratorThrough for the cluster c, whose
// control plane's private address is at, invalid if it has none: its
// orchestrator makes hosts c's nodes.
func orchestratorOf(env Env, c clusterdir.Cluster, at netip.Addr, cp remote) orchestrator.Orchestrator {
	var host orchestrator.Host
	if cp != nil {
		host = orchestratorHost{cp}
	}
	if env.Orchestrator != nil {
		return env.Orchestrator(host)
	}
	return k3s.Through(host, k3s.Cluster{Version: c.K3s, Email: c.Email, Registry: registry.Name, RegistryPort: registry.Port, ControlPlane: at})
}

// orchestratorHost is a remote as an orchestrator acts on it. A command
// that exits non-zero fails with its exit code and its stderr, and with
// nothing of its stdout.
type orchestratorHost struct{ r remote }

func (h orchestratorHost) Run(ctx context.Context, cmd string, stdin io.Reader) (string, error) {
	res, err := h.r.Run(ctx, cmd, stdin)
	return hostResult(ctx, res, err)
}

func (h orchestratorHost) RunStoppable(ctx context.Context, cmd string) (string, error) {
	res, err := h.r.RunStoppable(ctx, cmd)
	return hostResult(ctx, res, err)
}

func hostResult(ctx context.Context, res sshclient.Result, err error) (string, error) {
	if err == nil && res.ExitCode != 0 {
		if msg := strings.TrimSpace(string(res.Stderr)); msg != "" {
			return "", fmt.Errorf("exit %d: %s", res.ExitCode, msg)
		}
		return "", fmt.Errorf("exit %d", res.ExitCode)
	}
	return result(ctx, res, err)
}

func (h orchestratorHost) Upload(ctx context.Context, file string, r io.Reader, mode fs.FileMode) error {
	return h.r.Upload(ctx, file, r, mode, "root")
}

func (h orchestratorHost) DialTCP(ctx context.Context, addr string) (net.Conn, error) {
	return h.r.DialTCP(ctx, addr)
}
