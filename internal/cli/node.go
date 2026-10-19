package cli

import (
	"context"
	"fmt"
	"net/netip"
	"path/filepath"
	"strings"

	"github.com/urfave/cli/v3"
	"golang.org/x/crypto/ssh"

	"github.com/moostackhq/moostack/internal/bootstrap"
	"github.com/moostackhq/moostack/internal/cluster"
	"github.com/moostackhq/moostack/internal/hostlayout"
	"github.com/moostackhq/moostack/internal/identity"
	"github.com/moostackhq/moostack/internal/interrupt"
	"github.com/moostackhq/moostack/internal/secretstore"
	"github.com/moostackhq/moostack/internal/sshclient"
	"github.com/moostackhq/moostack/internal/sshconfig"
)

// nodeCommand is mooctl node.
func nodeCommand(env Env) *cli.Command {
	return &cli.Command{
		Name:  "node",
		Usage: "add nodes to the cluster",
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
				},
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if err := wantArgs(cmd, 1); err != nil {
						return err
					}
					return nodeAdd(ctx, env, cmd)
				},
			},
		},
	}
}

// nodeAdd is mooctl node add.
func nodeAdd(ctx context.Context, env Env, cmd *cli.Command) error {
	target := cmd.Args().First()
	var private netip.Addr
	if flag := cmd.String("private-address"); flag != "" {
		var err error
		if private, err = netip.ParseAddr(flag); err != nil {
			return fmt.Errorf("--private-address: %w", err)
		}
	}
	d, err := cluster.Open(cmd.String("dir"))
	if err != nil {
		return err
	}
	rel, err := openRelease(cmd, d)
	if err != nil {
		return err
	}
	ca, err := openCA(env, cmd, d)
	if err != nil {
		return err
	}
	opts := sshOptions(env)
	settings, err := sshconfig.Resolve(target, opts.SSH)
	if err != nil {
		return err
	}
	key, err := sshclient.FetchHostKey(ctx, target, opts)
	if err != nil {
		return err
	}
	fmt.Fprintf(env.Stdout, "Pinning the host key of %s: %s %s\n", target, key.Type(), ssh.FingerprintSHA256(key))
	c, err := sshclient.Dial(ctx, sshclient.Node{Target: target, HostKeys: []ssh.PublicKey{key}}, opts)
	if err != nil {
		return err
	}
	defer c.Close()

	host, err := bootstrap.Check(ctx, c)
	if err != nil {
		return fmt.Errorf("%s can't become a node: %w", target, err)
	}
	if !private.IsValid() {
		addrs, err := bootstrap.Addresses(ctx, c)
		if err == nil {
			private, err = bootstrap.PrivateAddress(addrs, hostOf(target), settings.HostName)
		}
		if err != nil {
			return fmt.Errorf("%s: %w", target, err)
		}
	}
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
	fmt.Fprintf(env.Stdout, "Installing release %s on %s (%s)\n", rel.Version, name, host.Architecture)
	// From here on, a signal cancels the install, which then rolls back,
	// rather than ending mooctl. Once the install has finished, the node
	// is recorded all the same.
	ctx, stop := interrupt.Context(ctx, env.Exit)
	defer stop()
	err = bootstrap.Install(ctx, c, host, rel, hostlayout.NodeConfig{Cluster: d.Cluster.Name, Node: name, Mode: hostlayout.ModeDirect, PrivateAddress: private}, ca)
	if err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("adding %s: interrupted: %w", name, err)
		}
		return fmt.Errorf("adding %s: %w", name, err)
	}
	if err := d.AddNode(n); err != nil {
		return bootstrap.Rollback(ctx, c, fmt.Errorf("recording %s: %w", name, err))
	}
	fmt.Fprintf(env.Stdout, "Added node %s\n", name)
	return nil
}

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
