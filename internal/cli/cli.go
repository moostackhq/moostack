// Package cli is mooctl's command line.
package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/user"
	"path/filepath"
	"strings"

	"github.com/urfave/cli/v3"
	"golang.org/x/crypto/ssh"

	"github.com/moostackhq/moostack/internal/buildinfo"
	"github.com/moostackhq/moostack/internal/cluster"
	"github.com/moostackhq/moostack/internal/nodeclient"
	"github.com/moostackhq/moostack/internal/orchestrate"
	"github.com/moostackhq/moostack/internal/release"
	"github.com/moostackhq/moostack/internal/secretstore"
	"github.com/moostackhq/moostack/internal/sshclient"
	"github.com/moostackhq/moostack/internal/sshconfig"
	"github.com/moostackhq/moostack/internal/terminal"
)

// Env is what mooctl's commands take from the machine they run on.
type Env struct {
	// Version is the Moostack release this mooctl belongs to, empty if it
	// wasn't built by the release build.
	Version string
	// Home is the operator's home directory.
	Home string
	// LocalUser is the operator's user name on this machine, the SSH user
	// when neither the target nor the SSH config names one, and who holds
	// the operation lock for their operations.
	LocalUser string
	// Machine is this machine's name, where the operation lock says its
	// holder runs mooctl.
	Machine string
	// Stdout is where results are printed.
	Stdout io.Writer
	// Stderr is where warnings and problems are printed.
	Stderr io.Writer
	// Terminal opens the operator's terminal and returns it with a
	// Closer to close it with. Both are nil when there is no terminal.
	// A signal at a password prompt ends mooctl with exit.
	Terminal func(exit func(code int)) (terminal.Terminal, io.Closer)
	// Exit ends mooctl. It is called when a signal stops a command, once
	// what the command held is cleaned up.
	Exit func(code int)
	// DialNode connects to a node's node API. Nil means over SSH, with
	// the operator's SSH config and ssh-agent.
	DialNode func(ctx context.Context, n cluster.Node) (orchestrate.Node, error)
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
	machine, err := os.Hostname()
	if err != nil {
		return Env{}, err
	}
	return Env{
		Machine:   machine,
		Version:   buildinfo.Version,
		Home:      home,
		LocalUser: u.Username,
		Stdout:    os.Stdout,
		Stderr:    os.Stderr,
		Terminal:  terminal.Open,
		Exit:      os.Exit,
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
				Name:    "dir",
				Aliases: []string{"C"},
				Usage:   "the cluster directory",
				Value:   ".",
			},
			&cli.StringFlag{
				Name:    "release-source",
				Usage:   "the directory of Moostack releases to install from, such as the release build's dist/releases",
				Sources: cli.EnvVars("MOOCTL_RELEASE_SOURCE"),
			},
			&cli.StringFlag{
				Name:  "ssh-key",
				Usage: "the private key file to read the secret store with, instead of finding one in ~/.ssh",
			},
		},
		Commands: []*cli.Command{
			clusterCommand(env),
			nodeCommand(env),
			secretCommand(env),
		},
	}
}

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

// sshOptions are the options mooctl connects to nodes with: the
// operator's SSH config and ssh-agent, and the agent's and the updater's
// relays to reach their sockets through sudo.
func sshOptions(env Env) sshclient.Options {
	return sshclient.Options{
		SSH: sshconfig.Options{
			UserConfig:   filepath.Join(env.Home, ".ssh", "config"),
			SystemConfig: "/etc/ssh/ssh_config",
			LocalUser:    env.LocalUser,
			Home:         env.Home,
		},
		Relay: nodeclient.Relay,
	}
}

// nodesToDial are nodes as orchestrate takes them: by name, with what
// connects to each one's node API, env.DialNode or else SSH.
func nodesToDial(env Env, nodes []cluster.Node) orchestrate.Nodes {
	names := make([]string, len(nodes))
	byName := map[string]cluster.Node{}
	for i, n := range nodes {
		names[i] = n.Name
		byName[n.Name] = n
	}
	dial := env.DialNode
	if dial == nil {
		dial = func(ctx context.Context, n cluster.Node) (orchestrate.Node, error) { return dialSSH(ctx, env, n) }
	}
	return orchestrate.Nodes{
		Names: names,
		Dial:  func(ctx context.Context, name string) (orchestrate.Node, error) { return dial(ctx, byName[name]) },
	}
}

// dialSSH connects to n's node API over SSH.
func dialSSH(ctx context.Context, env Env, n cluster.Node) (orchestrate.Node, error) {
	c, err := sshclient.Dial(ctx, sshclient.Node{Target: n.User + "@" + n.Address, HostKeys: []ssh.PublicKey{n.HostKey}}, sshOptions(env))
	if err != nil {
		return nil, err
	}
	node, err := nodeclient.Dial(c)
	if err != nil {
		c.Close()
		return nil, err
	}
	return sshNode{node, c}, nil
}

// sshNode is a node's node API over its SSH connection, which Close
// closes too.
type sshNode struct {
	*nodeclient.Node
	ssh *sshclient.Client
}

// Close closes the SSH connection first. Closing the node API's
// connections waits for what they are reading, which only a closed SSH
// connection ends when the node has stopped answering.
func (n sshNode) Close() error { return errors.Join(n.ssh.Close(), n.Node.Close()) }

// forceFlag is --force, for every command that runs the preflight.
var forceFlag = &cli.BoolFlag{
	Name:  "force",
	Usage: "apply this cluster directory even if a node has applied a later generation of it, or can't tell which generation it applied",
}

// preflight runs the preflight on the nodes of d, for an operation that
// writes d, and takes the operation lock for the operator on this
// machine. The operation runs in the returned cluster's context, which
// ends if it loses the lock.
func preflight(ctx context.Context, env Env, cmd *cli.Command, d *cluster.Dir) (*orchestrate.Cluster, error) {
	nodes, err := d.Nodes()
	if err != nil {
		return nil, err
	}
	ttl, err := leaseTTL()
	if err != nil {
		return nil, err
	}
	toDial := nodesToDial(env, nodes)
	toDial.LeaseTTL = ttl
	c, err := orchestrate.Preflight(ctx, toDial, d.Cluster.Generation, cmd.Bool("force"), orchestrate.Holder{Who: env.LocalUser, Machine: env.Machine})
	if errors.As(err, new(*orchestrate.LockHeldError)) {
		return nil, fmt.Errorf("%w\nAnother operation holds the operation lock. Ask its holder, or wait for it to end. If their mooctl has died, the lock expires on its own, or break it now with mooctl cluster unlock.", err)
	}
	if perr := (*orchestrate.PreflightError)(nil); errors.As(err, &perr) && perr.Forceable() {
		var hints []string
		if errors.As(err, new(*orchestrate.LaterGenerationError)) {
			hints = append(hints, "Pull the latest cluster directory, or pass --force to apply this one anyway.")
		}
		if errors.As(err, new(*orchestrate.UnknownGenerationError)) {
			hints = append(hints, "If this cluster directory is the latest, pass --force to apply this cluster directory anyway, and have every node record its generation.")
		}
		return nil, fmt.Errorf("%w\n%s", err, strings.Join(hints, "\n"))
	}
	if err != nil {
		return nil, err
	}
	if len(c.Forced) > 0 {
		fmt.Fprintln(env.Stderr, "--force applies this cluster directory although:")
		for _, f := range c.Forced {
			fmt.Fprintf(env.Stderr, "  %v\n", f)
		}
	}
	if len(c.Unlisted) > 0 {
		fmt.Fprintf(env.Stderr, "Generation %d lists %s, which this cluster directory doesn't, so mooctl can't reach them, and they keep the generation they have applied.\n",
			c.Latest, strings.Join(c.Unlisted, ", "))
	}
	return c, nil
}

// remindToCommit reminds the operator to commit the cluster directory,
// after an operation that changed it.
func remindToCommit(env Env) {
	fmt.Fprintln(env.Stdout, "The cluster directory changed: commit it, so that your teammates see the change.")
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

// openRelease opens the cluster's pinned release in the release source.
func openRelease(cmd *cli.Command, d *cluster.Dir) (*release.Release, error) {
	source := cmd.String("release-source")
	if source == "" {
		return nil, errors.New("no release source; pass --release-source or set MOOCTL_RELEASE_SOURCE to a directory of releases")
	}
	return release.Open(source, d.Cluster.Release)
}
