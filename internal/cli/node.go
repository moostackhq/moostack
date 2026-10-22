package cli

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"path/filepath"
	"strings"
	"time"

	"github.com/urfave/cli/v3"
	"golang.org/x/crypto/ssh"

	controlplanev1 "github.com/moostackhq/moostack/api/controlplane/v1"
	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/bootstrap"
	"github.com/moostackhq/moostack/internal/cluster"
	"github.com/moostackhq/moostack/internal/hostlayout"
	"github.com/moostackhq/moostack/internal/identity"
	"github.com/moostackhq/moostack/internal/interrupt"
	"github.com/moostackhq/moostack/internal/nodeclient"
	"github.com/moostackhq/moostack/internal/orchestrate"
	"github.com/moostackhq/moostack/internal/release"
	"github.com/moostackhq/moostack/internal/secretstore"
	"github.com/moostackhq/moostack/internal/sshclient"
	"github.com/moostackhq/moostack/internal/sshconfig"
)

// nodeCommand is mooctl node.
func nodeCommand(env Env) *cli.Command {
	return &cli.Command{
		Name:  "node",
		Usage: "add nodes to the cluster, and remove them",
		Commands: []*cli.Command{
			{
				Name:      "add",
				Usage:     "turn a host into a node of the cluster, over SSH",
				ArgsUsage: "<ssh target>",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "private-address",
						Usage: "the address other nodes reach this one at, if the host has more than one or the SSH target isn't one of them",
					},
					&cli.StringFlag{
						Name:  "name",
						Usage: "the node's name, instead of the first free one of node1, node2 and so on",
					},
					&cli.StringFlag{
						Name:  "join-token",
						Usage: "join the host to the control plane that issued the token, instead of the cluster directory's cluster, installing the release the directory pins",
					},
					forceFlag,
				},
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if err := wantArgs(cmd, 1); err != nil {
						return err
					}
					if cmd.IsSet("join-token") {
						return withHint(nodeJoin(ctx, env, cmd))
					}
					return withHint(nodeAdd(ctx, env, cmd))
				},
			},
			{
				Name:      "remove",
				Usage:     "stop a node's cluster services, remove its node components from its host, and take it out of the cluster, whose nodes then refuse its identity",
				ArgsUsage: "<node>",
				Flags: []cli.Flag{
					&cli.BoolFlag{
						Name:  "unreachable",
						Usage: "remove a node that can't be reached, without reaching it: the preflight skips it alone, and its host is left as it is",
					},
					forceFlag,
				},
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if err := wantArgs(cmd, 1); err != nil {
						return err
					}
					return withHint(nodeRemove(ctx, env, cmd))
				},
			},
		},
	}
}

// nodeAdd is mooctl node add.
func nodeAdd(ctx context.Context, env Env, cmd *cli.Command) error {
	target := cmd.Args().First()
	private, err := privateAddressFlag(cmd)
	if err != nil {
		return err
	}
	d, rel, err := pinnedRelease(ctx, env, cmd)
	if err != nil {
		return err
	}
	cl, err := preflight(ctx, env, cmd, d, "", nil)
	if err != nil {
		return err
	}
	defer cl.Close()
	// The rest runs only while the operation holds the lock.
	ctx = cl.Context()
	ca, err := openCA(env, cmd, d)
	if err != nil {
		return err
	}
	h, err := reachHost(ctx, env, target, private, "Pinning the host key of %s: %s")
	if err != nil {
		return err
	}
	c, host, key, private, settings := h.c, h.host, h.key, h.private, h.settings
	// Once the new node is added, the cluster closes the connection, after
	// it has released the lock on the node over it.
	added := false
	defer func() {
		if !added {
			c.Close()
		}
	}()
	name := cmd.String("name")
	if name == "" {
		if name, err = d.FreeNodeName(); err != nil {
			return err
		}
	}
	n := cluster.Node{
		Name:           name,
		Address:        hostOf(target),
		User:           settings.User,
		HostKey:        key,
		PrivateAddress: private,
		Architecture:   host.Architecture,
	}
	if err := d.CheckNewNode(n); err != nil {
		return err
	}
	if err := cl.CheckLock(); err != nil {
		return err
	}
	fmt.Fprintf(env.Stdout, "Installing release %s on %s (%s)\n", rel.Version, name, host.Architecture)
	// From here on, a signal cancels the install, which then rolls back,
	// rather than ending mooctl. Once the install has finished, the node
	// is recorded all the same.
	ctx, stop := interrupt.Context(ctx, env.Exit)
	defer stop()
	removed, err := d.RemovedIdentities()
	if err != nil {
		return err
	}
	n.Identity, err = bootstrap.Install(ctx, c, host, rel, hostlayout.NodeConfig{Cluster: d.Cluster.Name, Node: name, Mode: hostlayout.ModeDirect, PrivateAddress: private}, ca, cluster.Fingerprints(removed))
	if err != nil {
		if cause := context.Cause(ctx); errors.Is(cause, orchestrate.ErrLockLost) {
			return fmt.Errorf("adding %s: %w", name, cause)
		}
		if ctx.Err() != nil {
			return fmt.Errorf("adding %s: interrupted: %w", name, err)
		}
		return fmt.Errorf("adding %s: %w", name, err)
	}
	generation := cl.Next(d.Cluster.Generation)
	if err := d.AddNode(n, generation); err != nil {
		return bootstrap.Rollback(ctx, c, fmt.Errorf("recording %s: %w", name, err))
	}
	fmt.Fprintf(env.Stdout, "Added node %s, at generation %d of the cluster directory\n", name, generation)
	remindToCommit(env)
	// The other nodes record the generation even if the new node can't
	// be reached to.
	if node, err := nodeclient.Dial(c); err != nil {
		cl.Add(name, unreachedNode{err})
	} else {
		cl.Add(name, sshNode{node, c})
		added = true
	}
	if err := cl.Record(ctx, generation); err != nil {
		return fmt.Errorf("added %s, but cluster status shows these nodes behind until an operation that changes the cluster directory records its generation: %w", name, err)
	}
	return nil
}

// nodeJoin is mooctl node add --join-token: it installs the release the
// cluster directory pins on the host, and joins it to the control plane
// of the token, which operates it from then on. It runs no preflight,
// and leaves the cluster directory as it is.
func nodeJoin(ctx context.Context, env Env, cmd *cli.Command) error {
	target := cmd.Args().First()
	tok, err := controlplanev1.ParseToken(cmd.String("join-token"))
	if err != nil {
		return fmt.Errorf("--join-token: %w", err)
	}
	switch {
	case cmd.IsSet("name"):
		return errors.New("--name doesn't go with --join-token: the control plane names the node")
	case cmd.Bool("force"):
		return errors.New("--force doesn't go with --join-token: a node that joins a control plane applies no generation of the cluster directory")
	}
	private, err := privateAddressFlag(cmd)
	if err != nil {
		return err
	}
	_, rel, err := pinnedRelease(ctx, env, cmd)
	if err != nil {
		return err
	}
	h, err := reachHost(ctx, env, target, private, "Connecting to %s, whose host key is %s")
	if err != nil {
		return err
	}
	defer h.c.Close()
	c, host, private := h.c, h.host, h.private
	fmt.Fprintf(env.Stdout, "Installing release %s on %s (%s), and joining it to the control plane at %s\n", rel.Version, target, host.Architecture, tok.Address)
	// From here on, a signal cancels the install, which then rolls back,
	// rather than ending mooctl.
	ctx, stop := interrupt.Context(ctx, env.Exit)
	defer stop()
	joined, err := bootstrap.Join(ctx, c, host, rel, private, tok)
	if err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("joining %s: interrupted: %w", target, err)
		}
		return fmt.Errorf("joining %s: %w", target, err)
	}
	fmt.Fprintf(env.Stdout, "Joined %s to the control plane at %s, as node %s of cluster %s. The control plane operates it from now on, and its node API refuses changes over SSH.\n", target, tok.Address, joined.Node, joined.Cluster)
	return nil
}

// privateAddressFlag is --private-address, invalid if it isn't set.
func privateAddressFlag(cmd *cli.Command) (netip.Addr, error) {
	flag := cmd.String("private-address")
	if flag == "" {
		return netip.Addr{}, nil
	}
	private, err := netip.ParseAddr(flag)
	if err != nil {
		return netip.Addr{}, fmt.Errorf("--private-address: %w", err)
	}
	return private, nil
}

// pinnedRelease opens the cluster directory, and the release it pins,
// which this mooctl must operate.
func pinnedRelease(ctx context.Context, env Env, cmd *cli.Command) (*cluster.Dir, *release.Release, error) {
	d, err := cluster.Open(cmd.String("dir"))
	if err != nil {
		return nil, nil, err
	}
	if err := installable(env, d.Cluster.Release); err != nil {
		return nil, nil, err
	}
	rel, err := openRelease(ctx, env, cmd, d)
	if err != nil {
		return nil, nil, err
	}
	return d, rel, nil
}

// reachedHost is a host that reachHost reached over SSH and checked.
type reachedHost struct {
	c        *sshclient.Client
	settings sshconfig.Settings
	key      ssh.PublicKey
	host     bootstrap.Host
	// private is the address other nodes reach it at.
	private netip.Addr
}

// reachHost connects to target over SSH, trusting the host key it finds,
// which it prints as says, with target and the key, checks that the host
// can become a node, and finds the address other nodes reach it at,
// unless private is given. Close the client when done.
func reachHost(ctx context.Context, env Env, target string, private netip.Addr, says string) (*reachedHost, error) {
	opts := sshOptions(env)
	settings, err := sshconfig.Resolve(target, opts.SSH)
	if err != nil {
		return nil, err
	}
	key, err := sshclient.FetchHostKey(ctx, target, opts)
	if err != nil {
		return nil, err
	}
	fmt.Fprintf(env.Stdout, says+"\n", target, key.Type()+" "+ssh.FingerprintSHA256(key))
	c, err := sshclient.Dial(ctx, sshclient.Node{Target: target, HostKeys: []ssh.PublicKey{key}}, opts)
	if err != nil {
		return nil, err
	}
	host, err := bootstrap.Check(ctx, c)
	if err != nil {
		c.Close()
		return nil, fmt.Errorf("%s can't become a node: %w", target, err)
	}
	if !private.IsValid() {
		addrs, err := bootstrap.Addresses(ctx, c)
		if err == nil {
			private, err = bootstrap.PrivateAddress(addrs, hostOf(target), settings.HostName)
		}
		if err != nil {
			c.Close()
			return nil, fmt.Errorf("%s: %w", target, err)
		}
	}
	return &reachedHost{c: c, settings: settings, key: key, host: host, private: private}, nil
}

// nodeRemove is mooctl node remove.
func nodeRemove(ctx context.Context, env Env, cmd *cli.Command) error {
	name := cmd.Args().First()
	unreachable := cmd.Bool("unreachable")
	d, err := cluster.Open(cmd.String("dir"))
	if err != nil {
		return err
	}
	n, err := d.Node(name)
	if err != nil {
		return err
	}
	skip := ""
	if unreachable {
		skip = name
	}
	cl, err := preflight(ctx, env, cmd, d, skip, nil)
	if perr := (*orchestrate.PreflightError)(nil); !unreachable && errors.As(err, &perr) && perr.Unreachable(name) {
		return fmt.Errorf("%w\nIf %s can't be reached, remove it without reaching it with mooctl node remove %s --unreachable.", err, name, name)
	}
	if err != nil {
		return err
	}
	defer cl.Close()
	// The rest runs only while the operation holds the lock.
	ctx = cl.Context()
	if !unreachable {
		if err := uninstall(ctx, env, cl, n); err != nil {
			return err
		}
	}
	generation := cl.Next(d.Cluster.Generation)
	if err := d.RemoveNode(name, generation); err != nil {
		if unreachable {
			return fmt.Errorf("recording that %s is removed: %w", name, err)
		}
		return fmt.Errorf("%s's host is cleaned, but recording that it is removed failed, so remove it with mooctl node remove %s --unreachable: %w", name, name, err)
	}
	fmt.Fprintf(env.Stdout, "Removed node %s, at generation %d of the cluster directory\n", name, generation)
	if unreachable {
		fmt.Fprintf(env.Stdout, "%s's host was left as it is. If it comes back, the cluster's nodes refuse it as a peer, but it still runs Moostack's components: to clean it, %s.\n", name, bootstrap.CleanByHand)
	}
	remindToCommit(env)
	removed, err := d.RemovedIdentities()
	if err != nil {
		return err
	}
	if err := errors.Join(cl.RemoveIdentities(ctx, cluster.Fingerprints(removed)), cl.Record(ctx, generation)); err != nil {
		return fmt.Errorf("removed %s, but the next operation has to finish telling these nodes, until which they may still accept its identity: %w", name, err)
	}
	return nil
}

// uninstall stops n's cluster services and removes its node components
// from its host, over SSH, once it has left n out of the operation cl.
func uninstall(ctx context.Context, env Env, cl *orchestrate.Cluster, n cluster.Node) error {
	c, err := sshclient.Dial(ctx, sshclient.Node{Target: n.User + "@" + n.Address, HostKeys: []ssh.PublicKey{n.HostKey}}, sshOptions(env))
	if err != nil {
		return fmt.Errorf("%s stays in the cluster directory, since its host can't be reached to clean it: %w\nIf it can't be reached, remove it without reaching it with mooctl node remove %s --unreachable.", n.Name, err, n.Name)
	}
	defer c.Close()
	if err := cl.CheckLock(); err != nil {
		return err
	}
	fmt.Fprintf(env.Stdout, "Stopping the cluster services of %s and removing its node components\n", n.Name)
	cl.Remove(n.Name)
	// A signal doesn't stop the uninstall halfway; once it has finished,
	// the node is recorded as removed all the same.
	ctx, stop := interrupt.Context(ctx, env.Exit)
	defer stop()
	if err := bootstrap.Uninstall(ctx, c); err != nil {
		return fmt.Errorf("removing %s's node components: %w\n%s stays in the cluster directory. To finish, %s on its host, then remove it with mooctl node remove %s --unreachable.", n.Name, err, n.Name, bootstrap.CleanByHand, n.Name)
	}
	return nil
}

// unreachedNode is a node whose node API couldn't be reached, with why.
// It fails whatever it is asked to do.
type unreachedNode struct{ err error }

func (n unreachedNode) Status(context.Context) nodeclient.Status {
	return nodeclient.Status{AgentErr: n.err, UpdaterErr: n.err}
}

func (n unreachedNode) RecordGeneration(context.Context, int64, []string, string) error { return n.err }

func (n unreachedNode) AcquireLock(context.Context, nodeclient.Lock, time.Duration) (nodeclient.Lock, error) {
	return nodeclient.Lock{}, n.err
}

func (n unreachedNode) RenewLock(context.Context, string, time.Duration) (*nodeclient.Lock, error) {
	return nil, n.err
}

func (n unreachedNode) ReleaseLock(context.Context, string) error { return n.err }

func (n unreachedNode) BreakLock(context.Context) (*nodeclient.Lock, error) { return nil, n.err }

func (n unreachedNode) RemoveIdentities(context.Context, []string, string) error { return n.err }

func (n unreachedNode) UpdateAgent(context.Context, string, []byte, time.Duration) error {
	return n.err
}

func (n unreachedNode) UpdateUpdater(context.Context, string, []byte, time.Duration) error {
	return n.err
}

func (n unreachedNode) UpdateContainerd(context.Context, string, map[string][]byte, time.Duration) error {
	return n.err
}

func (n unreachedNode) ImportImage(context.Context, []byte) ([]string, error) { return nil, n.err }

func (n unreachedNode) RunService(context.Context, *nodev1.Service) error { return n.err }

func (n unreachedNode) Close() error { return nil }

// openCA opens the cluster CA of d, reading its key from the secret
// store with the operator's key. An operator who isn't a recipient gets
// an error that lists the recipients.
func openCA(env Env, cmd *cli.Command, d *cluster.Dir) (*identity.CA, error) {
	cert, err := d.CACert()
	if err != nil {
		return nil, fmt.Errorf("reading the cluster CA: %w", err)
	}
	t, closeTerminal := openTerminal(env)
	defer closeTerminal()
	store, err := secretstore.Unlock(filepath.Join(d.Path(), secretstore.FileName), keyOptions(env, cmd, t))
	if err != nil {
		return nil, err
	}
	key, err := store.Get(caKeySecret)
	if err != nil {
		return nil, fmt.Errorf("reading the cluster CA key, %s: %w", caKeySecret, err)
	}
	ca, err := identity.ParseCA(cert, []byte(key.Value))
	if err != nil {
		return nil, fmt.Errorf("the cluster CA: %w", err)
	}
	return ca, nil
}

// hostOf is the host that the SSH target user@host or host names.
func hostOf(target string) string {
	if i := strings.LastIndex(target, "@"); i >= 0 {
		return target[i+1:]
	}
	return target
}
