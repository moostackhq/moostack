package cli

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"net"
	"path/filepath"
	"strings"

	"golang.org/x/crypto/ssh"

	"github.com/moostackhq/moostack/internal/clusterdir"
	"github.com/moostackhq/moostack/internal/sshclient"
	"github.com/moostackhq/moostack/internal/sshconfig"
)

// remote is a host mooctl runs commands on, as root.
type remote interface {
	Run(ctx context.Context, cmd string, stdin io.Reader) (sshclient.Result, error)
	RunStoppable(ctx context.Context, cmd string) (sshclient.Result, error)
	Upload(ctx context.Context, file string, r io.Reader, mode fs.FileMode, owner string) error
	DialTCP(ctx context.Context, addr string) (net.Conn, error)
	// Lock takes the cluster's operation lock on the host.
	Lock(ctx context.Context) (held, error)
	Close() error
}

type sshRemote struct{ *sshclient.Client }

func (r sshRemote) Lock(ctx context.Context) (held, error) {
	h, err := r.Hold(ctx, LockScript, LockReady)
	if err != nil {
		return nil, err
	}
	return h, nil
}

// connect connects to target with env.Connect, or over SSH.
func connect(ctx context.Context, env Env, target string, hostKey ssh.PublicKey) (remote, ssh.PublicKey, error) {
	if env.Connect != nil {
		return env.Connect(ctx, target, hostKey)
	}
	opts := sshclient.Options{
		SSH: sshconfig.Options{
			UserConfig:   filepath.Join(env.Home, ".ssh", "config"),
			SystemConfig: "/etc/ssh/ssh_config",
			LocalUser:    env.LocalUser,
			Home:         env.Home,
		},
	}
	if hostKey == nil {
		key, err := sshclient.FetchHostKey(ctx, target, opts)
		if err != nil {
			return nil, nil, fmt.Errorf("%s: %w", target, err)
		}
		hostKey = key
	}
	c, err := sshclient.Dial(ctx, sshclient.Node{Target: target, HostKeys: []ssh.PublicKey{hostKey}}, opts)
	if err != nil {
		return nil, nil, err
	}
	return sshRemote{c}, hostKey, nil
}

// dialNode connects to n, trusting only its pinned host key.
func dialNode(ctx context.Context, env Env, n clusterdir.Node) (remote, error) {
	r, _, err := connect(ctx, env, n.Target, n.HostKey)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", n.Name, err)
	}
	return r, nil
}

// run runs script as root on r, and fails with its stderr, or else its
// stdout, unless it exits 0. stdin, if not empty, is its input.
func run(ctx context.Context, r remote, script, stdin string) (string, error) {
	var in io.Reader
	if stdin != "" {
		in = strings.NewReader(stdin)
	}
	res, err := r.Run(ctx, script, in)
	return result(ctx, res, err)
}

// runStoppable is run with no input, with the script ended on the host
// once ctx ends.
func runStoppable(ctx context.Context, r remote, script string) (string, error) {
	res, err := r.RunStoppable(ctx, script)
	return result(ctx, res, err)
}

// result is a command's stdout, or why it failed: ctx's cause, the
// connection's error, or its stderr, or else its stdout, when it exited
// non-zero.
func result(ctx context.Context, res sshclient.Result, err error) (string, error) {
	if err != nil {
		if ctx.Err() != nil {
			return "", context.Cause(ctx)
		}
		return "", err
	}
	if res.ExitCode != 0 {
		msg := strings.TrimSpace(string(res.Stderr))
		if msg == "" {
			msg = strings.TrimSpace(string(res.Stdout))
		}
		return "", fmt.Errorf("exit %d: %s", res.ExitCode, msg)
	}
	return string(res.Stdout), nil
}

// upload writes content to path on r, creating its directory.
func upload(ctx context.Context, r remote, path, content string, mode fs.FileMode) error {
	if _, err := run(ctx, r, "mkdir -p "+sshclient.ShellQuote(filepath.Dir(path)), ""); err != nil {
		return err
	}
	return r.Upload(ctx, path, strings.NewReader(content), mode, "root")
}
