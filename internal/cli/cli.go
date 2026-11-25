// Package cli is mooctl's command line.
package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/user"
	"path/filepath"
	"strings"

	"github.com/urfave/cli/v3"
	"golang.org/x/crypto/ssh"

	"github.com/moostackhq/moostack/internal/clusterdir"
	"github.com/moostackhq/moostack/internal/deployfile"
	"github.com/moostackhq/moostack/internal/gitrepo"
	"github.com/moostackhq/moostack/internal/orchestrator"
	"github.com/moostackhq/moostack/internal/secretstore"
	"github.com/moostackhq/moostack/internal/terminal"
)

// K3sVersion is the k3s version a new cluster pins.
const K3sVersion = "v1.37.2+k3s1"

// commit is the commit mooctl was built from. The release build sets
// it; any other build leaves it empty.
var commit string

// Env is what mooctl's commands take from the machine they run on.
type Env struct {
	// Home is the operator's home directory.
	Home string
	// LocalUser is the operator's user name on this machine, the SSH user
	// when neither the target nor the SSH config names one.
	LocalUser string
	// Stdout is where results are printed.
	Stdout io.Writer
	// Stderr is where progress, warnings and problems are printed.
	Stderr io.Writer
	// Terminal opens the operator's terminal and returns it with a
	// Closer to close it with. Both are nil when there is no terminal.
	Terminal func(exit func(code int)) (terminal.Terminal, io.Closer)
	// Exit ends mooctl.
	Exit func(code int)
	// Docker and Railpack are the commands deploy builds with, docker and
	// railpack on the PATH if empty.
	Docker, Railpack string
	// Go is the command a development build is compiled with, go on the
	// PATH if empty.
	Go string
	// Browser opens url in the operator's browser.
	Browser func(url string) error
	// Root is the directory of cluster directories; clusterdir.Root if
	// empty.
	Root string
	// Release is the URL of the release the console is downloaded from,
	// MOOCTL_RELEASE_URL in the environment; release.URL of commit if
	// empty.
	Release string
	// Connect connects to a node by its SSH target, trusting only
	// hostKey, or, if hostKey is nil, the key the node offers, which it
	// returns. Nil means over SSH, with the operator's SSH config and
	// ssh-agent.
	Connect func(ctx context.Context, target string, hostKey ssh.PublicKey) (remote, ssh.PublicKey, error)
	// Orchestrator builds the cluster's orchestrator, which acts through
	// host, the cluster's control plane, or has none if host is nil. Nil
	// means the one orchestratorThrough builds.
	Orchestrator func(host orchestrator.Host) orchestrator.Orchestrator
}

// OSEnv is the Env of the running process.
func OSEnv() (Env, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return Env{}, err
	}
	u, err := user.Current()
	if err != nil {
		return Env{}, err
	}
	return Env{
		Home:      home,
		LocalUser: u.Username,
		Stdout:    os.Stdout,
		Stderr:    os.Stderr,
		Terminal:  terminal.Open,
		Exit:      os.Exit,
		Release:   os.Getenv("MOOCTL_RELEASE_URL"),
		Browser:   openBrowser,
	}, nil
}

// New is the mooctl command.
func New(env Env) *cli.Command {
	return &cli.Command{
		Name:      "mooctl",
		Usage:     "operate a Moostack cluster",
		Writer:    env.Stdout,
		ErrWriter: env.Stderr,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "cluster",
				Aliases: []string{"c"},
				Usage:   "the cluster ref, name or prefix/name; without it, the cluster of the deploy.yaml in the current directory, or else the default cluster",
			},
			&cli.StringFlag{
				Name:    "dir",
				Aliases: []string{"C"},
				Usage:   "the cluster directory, instead of the one the cluster ref names",
				Sources: cli.EnvVars("MOOCTL_DIR"),
			},
			&cli.StringFlag{
				Name:  "ssh-key",
				Usage: "the private key file to read the secret store with, instead of finding one in ~/.ssh",
			},
		},
		Commands: append([]*cli.Command{
			clusterCommand(env),
			secretCommand(env),
			deployCommand(env),
			logsCommand(env),
			consoleCommand(env),
		}, devCommands(env)...),
	}
}

// devCommands are the commands for developing Moostack, which a build
// with the release tag has none of.
var devCommands = func(Env) []*cli.Command { return nil }

// wantArgs refuses a command that wasn't given n arguments.
func wantArgs(cmd *cli.Command, n int) error {
	if cmd.NArg() != n {
		return fmt.Errorf("%s takes %s, not %d; see --help", cmd.FullName(), plural(n, "argument"), cmd.NArg())
	}
	return nil
}

// plural is n things, as in "1 argument" or "2 arguments".
func plural(n int, thing string) string {
	if n == 1 {
		return "1 " + thing
	}
	return fmt.Sprintf("%d %ss", n, thing)
}

// root is the directory of cluster directories.
func root(env Env) (string, error) {
	if env.Root != "" {
		return env.Root, nil
	}
	return clusterdir.Root()
}

// refDir is the cluster directory of the ref s: the one -C or
// MOOCTL_DIR names, or else the ref's under root.
func refDir(env Env, cmd *cli.Command, s string) (clusterdir.Ref, string, error) {
	ref, err := clusterdir.ParseRef(s)
	if err != nil {
		return clusterdir.Ref{}, "", err
	}
	d, err := dirFlag(cmd)
	if err != nil {
		return clusterdir.Ref{}, "", err
	}
	if d != "" {
		return ref, d, nil
	}
	r, err := root(env)
	if err != nil {
		return clusterdir.Ref{}, "", err
	}
	return ref, ref.Dir(r), nil
}

// dirFlag is the cluster directory -C or MOOCTL_DIR names, empty if
// neither is set, and refuses either set to nothing.
func dirFlag(cmd *cli.Command) (string, error) {
	d := cmd.String("dir")
	if d != "" {
		return d, nil
	}
	if v, ok := os.LookupEnv("MOOCTL_DIR"); ok && v == "" {
		return "", errors.New("MOOCTL_DIR is set to nothing; unset it, or give a cluster directory")
	}
	if cmd.IsSet("dir") {
		return "", errors.New("-C is empty; give a cluster directory")
	}
	return "", nil
}

// clusterPath is the cluster directory a command acts on: the one -C or
// MOOCTL_DIR names, or else that of the ref -c gives, or else that of
// the cluster the deploy file in the current directory names, or else
// that of the default cluster.
func clusterPath(env Env, cmd *cli.Command) (string, error) {
	d, err := dirFlag(cmd)
	if err != nil {
		return "", err
	}
	if d != "" {
		return d, nil
	}
	ref := cmd.String("cluster")
	if cmd.IsSet("cluster") && ref == "" {
		return "", errors.New("-c is empty; give a cluster ref")
	}
	if ref == "" {
		cluster, _, err := deployfile.ReadRef(".")
		switch {
		case err == nil:
			ref = cluster
		case !errors.Is(err, fs.ErrNotExist):
			return "", err
		}
	}
	if ref == "" {
		def, err := defaultCluster(env)
		if err != nil {
			return "", err
		}
		if def == "" {
			return "", errors.New("which cluster? pass -c <cluster ref>, set a default with mooctl cluster use <cluster ref>, or run mooctl in an app's directory, whose deploy.yaml names its cluster")
		}
		ref = def
	}
	_, dir, err := refDir(env, cmd, ref)
	return dir, err
}

// defaultFile is the file that holds the default cluster's ref, beside the
// directory of cluster directories.
func defaultFile(env Env) (string, error) {
	r, err := root(env)
	if err != nil {
		return "", err
	}
	return filepath.Join(filepath.Dir(r), "default-cluster"), nil
}

// defaultCluster is the default cluster's ref, empty if none is set.
func defaultCluster(env Env) (string, error) {
	path, err := defaultFile(env)
	if err != nil {
		return "", err
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	ref := strings.TrimSpace(string(data))
	if _, err := clusterdir.ParseRef(ref); err != nil {
		return "", fmt.Errorf("%s: %w", path, err)
	}
	return ref, nil
}

// setDefaultCluster makes ref the default cluster.
func setDefaultCluster(env Env, ref clusterdir.Ref) error {
	path, err := defaultFile(env)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return clusterdir.WriteFile(path, []byte(ref.String()+"\n"))
}

// openCluster opens the cluster directory the command acts on.
func openCluster(env Env, cmd *cli.Command) (*clusterdir.Dir, error) {
	path, err := clusterPath(env, cmd)
	if err != nil {
		return nil, err
	}
	d, err := clusterdir.Open(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("there is no cluster directory at %s; create one with mooctl cluster init, or get one with mooctl cluster clone", path)
	}
	return d, err
}

// updateFromRemote fetches the cluster directory at dir from its remote,
// if it has one, and fast-forwards to it.
func updateFromRemote(ctx context.Context, dir string) error {
	repo, err := gitrepo.Open(ctx, dir)
	if err != nil {
		return err
	}
	remote, err := repo.HasRemote(ctx)
	if err != nil || !remote {
		return err
	}
	if err := repo.Update(ctx); err != nil {
		if errors.Is(err, gitrepo.ErrDiverged) {
			return fmt.Errorf("%w; merge the remote's commits into %s with git pull --no-rebase, then run again", err, dir)
		}
		return err
	}
	return nil
}

// openTerminal opens the operator's terminal, and returns it with the
// function to close it with. The terminal is nil when there is none.
func openTerminal(env Env) (terminal.Terminal, func()) {
	if env.Terminal == nil {
		return nil, func() {}
	}
	t, closer := env.Terminal(env.Exit)
	if t == nil {
		return nil, func() {}
	}
	return t, func() { closer.Close() }
}

// keyOptions are the options to choose or find the operator's key for
// the secret store with, on the terminal t.
func keyOptions(env Env, cmd *cli.Command, t terminal.Terminal) secretstore.KeyOptions {
	return secretstore.KeyOptions{Home: env.Home, SSHKey: cmd.String("ssh-key"), Terminal: t, Out: env.Stderr}
}

// say prints a line of progress.
func say(env Env, format string, args ...any) {
	fmt.Fprintf(env.Stderr, format+"\n", args...)
}
