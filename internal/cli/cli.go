// Package cli is mooctl's command line.
package cli

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"os/user"
	"path/filepath"
	"slices"
	"strings"
	"time"

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
	// wasn't built by the release build, and Previous the release before
	// it, empty if there is none.
	Version, Previous string
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
	// HTTPClient downloads releases from a release source at an https
	// URL. Nil means http.DefaultClient.
	HTTPClient *http.Client
	// Cache is the directory of releases downloaded from such a source.
	Cache string
	// StartCheck is how long deploy waits once an app's container started
	// before it checks that the container still runs. Zero means
	// orchestrate.DefaultStartCheck.
	StartCheck time.Duration
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
	cache, err := os.UserCacheDir()
	if err != nil {
		return Env{}, err
	}
	return Env{
		Machine:   machine,
		Version:   buildinfo.Version,
		Previous:  buildinfo.Previous,
		Cache:     filepath.Join(cache, "mooctl", "releases"),
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
				Usage:   "the cluster directory; without it, the current directory if it has a cluster.yml, or else $MOOCTL_DIR",
				Value:   ".",
			},
			&cli.StringFlag{
				Name:    "release-source",
				Usage:   "the Moostack releases to install from: a directory, such as the release build's dist/releases, or an https URL laid out as one, which mooctl downloads each release from once",
				Sources: cli.EnvVars("MOOCTL_RELEASE_SOURCE"),
			},
			&cli.StringFlag{
				Name:  "ssh-key",
				Usage: "the private key file to read the secret store with, instead of finding one in ~/.ssh",
			},
		},
		Commands: []*cli.Command{
			clusterCommand(env),
			deployCommand(env),
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

// clusterDir is the cluster directory that cmd names with -C, or else
// the current directory if it has a cluster file, or else the one
// MOOCTL_DIR names: the current directory if none does, or if the one
// that does is empty. With MOOCTL_DIR set and no -C, it says on stderr,
// once, which directory it is, as an exported MOOCTL_DIR may be
// forgotten.
func clusterDir(cmd *cli.Command) string {
	if cmd.IsSet("dir") {
		return cmp.Or(cmd.String("dir"), ".")
	}
	env := os.Getenv("MOOCTL_DIR")
	if env == "" {
		return "."
	}
	dir, note := env, "Cluster directory: "+env+", from MOOCTL_DIR"
	if here, err := os.Stat(cluster.FileName); err == nil {
		if there, err := os.Stat(filepath.Join(env, cluster.FileName)); err != nil || !os.SameFile(here, there) {
			dir, note = ".", "Cluster directory: the current directory, which has a "+cluster.FileName+", not MOOCTL_DIR's "+env
		}
	}
	root := cmd.Root()
	if root.Metadata == nil {
		root.Metadata = map[string]any{}
	}
	if root.Metadata[dirToldKey] == nil {
		root.Metadata[dirToldKey] = true
		fmt.Fprintln(root.ErrWriter, note)
	}
	return dir
}

// dirToldKey marks, in the root command's metadata, that clusterDir said
// which cluster directory it is.
const dirToldKey = "clusterDirTold"

// openCluster opens the cluster directory clusterDir names, and says to
// name it if it has no cluster file.
func openCluster(cmd *cli.Command) (*cluster.Dir, error) {
	d, err := cluster.Open(clusterDir(cmd))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("%w\nPass -C <cluster dir>, or set MOOCTL_DIR to the cluster directory.", err)
	}
	return d, err
}

// forceFlag is --force, for every command that runs the preflight.
var forceFlag = &cli.BoolFlag{
	Name:  "force",
	Usage: "go on from this cluster directory even if a node has applied a later generation of it, or can't tell which generation it applied",
}

// preflight runs the preflight on the nodes of d, apart from the node
// called skip if one is, for an operation on them: it refuses a
// node on a release this mooctl doesn't operate, and one that check, if
// set, fails, takes the operation lock for the operator on this machine
// and for cmd, and pushes d's removed identities to every node. The
// operation runs in the returned cluster's context, which ends if it
// loses the lock.
func preflight(ctx context.Context, env Env, cmd *cli.Command, d *cluster.Dir, skip string, check func(s nodeclient.Status, all []nodeclient.Status) error) (*orchestrate.Cluster, error) {
	nodes, err := d.Nodes()
	if err != nil {
		return nil, err
	}
	nodes = slices.DeleteFunc(nodes, func(n cluster.Node) bool { return n.Name == skip })
	removed, err := d.RemovedIdentities()
	if err != nil {
		return nil, err
	}
	ttl, err := leaseTTL()
	if err != nil {
		return nil, err
	}
	toDial := nodesToDial(env, nodes)
	toDial.LeaseTTL = ttl
	toDial.StartCheck = env.StartCheck
	toDial.Removed = cluster.Fingerprints(removed)
	toDial.Check = func(s nodeclient.Status, all []nodeclient.Status) error {
		if err := operated(env, s); err != nil {
			return err
		}
		if check != nil {
			return check(s, all)
		}
		return nil
	}
	operation := strings.TrimPrefix(cmd.FullName(), cmd.Root().Name+" ")
	c, err := orchestrate.Preflight(ctx, toDial, d.Cluster.Generation, cmd.Bool("force"), orchestrate.Holder{Who: env.LocalUser, Machine: env.Machine, Operation: operation})
	if errors.As(err, new(*orchestrate.LockHeldError)) {
		return nil, fmt.Errorf("%w\nAnother operation holds the operation lock. Ask its holder, or wait for it to end. If their mooctl has died, the lock expires on its own, or break it now with mooctl cluster unlock.", err)
	}
	if perr := (*orchestrate.PreflightError)(nil); errors.As(err, &perr) && perr.Forceable() {
		var hints []string
		if errors.As(err, new(*orchestrate.LaterGenerationError)) {
			hints = append(hints, "Pull the latest cluster directory, or pass --force to go on from this one anyway.")
		}
		if errors.As(err, new(*orchestrate.UnknownGenerationError)) {
			hints = append(hints, "If this cluster directory is the latest, pass --force to go on from it anyway; the next operation that writes it has every node record its generation.")
		}
		return nil, fmt.Errorf("%w\n%s", err, strings.Join(hints, "\n"))
	}
	if err != nil {
		return nil, err
	}
	if len(c.Forced) > 0 {
		fmt.Fprintln(env.Stderr, "--force goes on from this cluster directory although:")
		for _, f := range c.Forced {
			fmt.Fprintf(env.Stderr, "  %v\n", f)
		}
	}
	// The node skipped is in the cluster directory, but not among those
	// reached.
	c.Unlisted = slices.DeleteFunc(c.Unlisted, func(n string) bool { return n == skip })
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

// openRelease opens the cluster's pinned release in the release source:
// a directory, or an https URL, which it downloads the release from into
// env.Cache, unless it is there already.
func openRelease(ctx context.Context, env Env, cmd *cli.Command, d *cluster.Dir) (*release.Release, error) {
	source := cmd.String("release-source")
	if source == "" {
		return nil, errors.New("no release source; pass --release-source or set MOOCTL_RELEASE_SOURCE to a directory of releases or an https URL")
	}
	if strings.Contains(source, "://") {
		return release.Download(ctx, env.HTTPClient, source, env.Cache, d.Cluster.Release, func(format string, args ...any) {
			fmt.Fprintf(env.Stderr, format+"\n", args...)
		})
	}
	return release.Open(source, d.Cluster.Release)
}

// agentRelease is the release that the node whose status is s runs: its
// agent's version, which the release build versions as the release.
func agentRelease(s nodeclient.Status) (string, error) {
	for _, c := range s.Components {
		if c.Name == "agent" && c.Version != "" {
			return c.Version, nil
		}
	}
	return "", unknownReleaseError{}
}

// unknownReleaseError says a node's agent doesn't say which release it
// runs, as every release's agent does.
type unknownReleaseError struct{}

func (unknownReleaseError) Error() string { return "its agent doesn't say which release it runs" }

func (unknownReleaseError) Hint() string {
	return "Check the node with mooctl cluster status: its agent may be broken."
}

// mooctlRelease is the release this mooctl belongs to, as far as it
// knows it: its version and the release before it.
func mooctlRelease(env Env) release.Manifest {
	return release.Manifest{Version: env.Version, Previous: env.Previous}
}

// window says the releases that m updates nodes on, joined by conj, as in
// "1.1.0 and 1.0.0 only".
func window(m release.Manifest, conj string) string {
	return strings.Join(m.UpdatedFrom(), " "+conj+" ") + " only"
}

// operates reports whether this mooctl operates clusters on version: it
// operates its own release and the one before it, the releases its own
// release updates from. A mooctl that the release build didn't build has
// no release to compare, and operates any.
func operates(env Env, version string) bool {
	return env.Version == "" || mooctlRelease(env).UpdatesFrom(version)
}

// installable fails if this mooctl doesn't operate the release pinned,
// which an operation is to install, so that it never puts nodes on a
// release it can't operate.
func installable(env Env, pinned string) error {
	if operates(env, pinned) {
		return nil
	}
	return &uninstallableError{pinned: pinned, own: mooctlRelease(env)}
}

// uninstallableError says the cluster directory pins a release that
// this mooctl, of release own, doesn't operate.
type uninstallableError struct {
	pinned string
	own    release.Manifest
}

func (e *uninstallableError) Error() string {
	return fmt.Sprintf("this mooctl, of release %s, doesn't operate release %s, which the cluster directory pins: it operates clusters on %s", e.own.Version, e.pinned, window(e.own, "and"))
}

func (e *uninstallableError) Hint() string { return "Use the mooctl of release " + e.pinned + "." }

// hinted is an error that says what the operator can do about it.
type hinted interface {
	error
	Hint() string
}

// withHint is err, followed by the hint of the first hinted error it
// wraps, if it wraps one.
func withHint(err error) error {
	var h hinted
	if errors.As(err, &h) {
		return fmt.Errorf("%w\n%s", err, h.Hint())
	}
	return err
}

// operated fails if the node whose status is s runs a release that this
// mooctl doesn't operate, with an *unoperatedError. A node whose agent is
// a development build is on no release: development builds sit outside
// the releases mooctl operates, so that cluster update puts a release
// back.
func operated(env Env, s nodeclient.Status) error {
	if env.Version == "" {
		return nil
	}
	running, err := agentRelease(s)
	if err != nil {
		return err
	}
	if operates(env, running) || release.IsDevVersion(running) {
		return nil
	}
	return &unoperatedError{running: running, own: mooctlRelease(env)}
}

// unoperatedError says a node runs a release that this mooctl, of
// release own, doesn't operate.
type unoperatedError struct {
	running string
	own     release.Manifest
}

func (e *unoperatedError) Error() string {
	return fmt.Sprintf("it runs release %s, which this mooctl, of release %s, doesn't operate: it operates clusters on %s", e.running, e.own.Version, window(e.own, "and"))
}

func (e *unoperatedError) Hint() string {
	return "Operate the cluster with the mooctl of the release it runs, and update it one release at a time."
}

// updates fails, with a *skippedReleaseError, unless the node whose
// status is s may be updated to pinned, given what every node says, all.
// pinned updates nodes on itself and on the release before it, and, by
// this mooctl, moves nodes on this mooctl's release back to pinned, the
// release before it, while a node still runs pinned: until an update has
// finished, it can go back. Each part of the node that runs a release,
// its updater and each cluster service pinned has as well as its agent,
// must be one pinned updates, so that a node an update stopped partway
// skips no release for any of them; and any of them on pinned counts as
// a node that still runs it. A part that doesn't say which release it
// runs, as a service whose container the node can't read, may be on any:
// only the release the node's agent runs updates it, which repairs it, or
// any, if the agent is a development build. A part on a development build
// may be on any, which pinned replaces.
func updates(env Env, pinned release.Manifest, s nodeclient.Status, all []nodeclient.Status) error {
	agent, err := agentRelease(s)
	if err != nil {
		return err
	}
	for _, r := range releasesOf(s) {
		if (r.service != "" && !slices.ContainsFunc(pinned.Services, func(svc release.Service) bool { return svc.Name == r.service })) || release.IsDevVersion(r.release) {
			continue
		}
		if r.release == "" {
			if pinned.Version == agent || release.IsDevVersion(agent) {
				continue
			}
			return &skippedReleaseError{what: r.what, pinned: pinned}
		}
		if pinned.UpdatesFrom(r.release) {
			continue
		}
		e := &skippedReleaseError{what: r.what, running: r.release, pinned: pinned}
		if env.Version != "" && pinned.Version == env.Previous {
			e.back = env.Version
			if r.release == env.Version && slices.ContainsFunc(all, func(other nodeclient.Status) bool {
				return slices.ContainsFunc(releasesOf(other), func(o ofRelease) bool { return o.release == pinned.Version })
			}) {
				continue
			}
		}
		return e
	}
	return nil
}

// ofRelease is a part of a node that runs a release: the node itself, by
// its agent, its updater or one of its cluster services, called service.
// what is the part as a sentence says it, and release is empty if the
// node doesn't say.
type ofRelease struct{ what, service, release string }

// releasesOf are the parts of the node whose status is s that run a
// release. containerd and runc have versions of their own.
func releasesOf(s nodeclient.Status) []ofRelease {
	var rs []ofRelease
	for _, c := range s.Components {
		switch c.Name {
		case "agent":
			rs = append(rs, ofRelease{what: "it", release: c.Version})
		case "updater":
			rs = append(rs, ofRelease{what: "its updater", release: c.Version})
		}
	}
	for _, c := range s.Services() {
		rs = append(rs, ofRelease{what: "its " + c.Name + " service", service: c.Name, release: c.Version})
	}
	return rs
}

// skippedReleaseError says that what, the node or what of it, runs a
// release that the pinned release doesn't update. back is the release whose nodes the
// pinned one moves back to it while a node runs it, empty if none.
type skippedReleaseError struct {
	what, running, back string
	pinned              release.Manifest
}

func (e *skippedReleaseError) Error() string {
	s := fmt.Sprintf("%s runs release %s", e.what, e.running)
	if e.running == "" {
		s = e.what + " doesn't say which release it runs"
	}
	s += fmt.Sprintf(", and release %s updates nodes on %s", e.pinned.Version, window(e.pinned, "or"))
	if e.back != "" {
		s += fmt.Sprintf(", and moves nodes on %s back to it only while a node still runs it", e.back)
	}
	return s
}

func (e *skippedReleaseError) Hint() string {
	return "Pin the release the nodes run, or the one after it, and run the mooctl of that release."
}
