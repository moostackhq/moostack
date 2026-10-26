package cli

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"path/filepath"
	"slices"
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
				Usage:     "turn hosts into nodes of the cluster, over SSH, one after another",
				ArgsUsage: "<ssh target>[,name=<name>][,private-address=<address>]...",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "private-address",
						Usage: "the address other nodes reach this one at, if the host has more than one or the SSH target isn't one of them; with several targets, give each its own as <ssh target>,private-address=<address>",
					},
					&cli.StringFlag{
						Name:  "name",
						Usage: "the node's name, instead of the first free one of node1, node2 and so on; with several targets, give each its own as <ssh target>,name=<name>",
					},
					&cli.StringFlag{
						Name:  "join-token",
						Usage: "join the host to the control plane that issued the token, instead of the cluster directory's cluster, installing the release the directory pins",
					},
					forceFlag,
				},
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.IsSet("join-token") {
						if err := wantArgs(cmd, 1); err != nil {
							return err
						}
						return withHint(nodeJoin(ctx, env, cmd))
					}
					if cmd.NArg() == 0 {
						return fmt.Errorf("%s takes one or more SSH targets; see --help", cmd.FullName())
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

// nodeAdd is mooctl node add. It checks every target before it installs
// on any, then adds them one at a time, in order, and stops at the first
// that fails.
func nodeAdd(ctx context.Context, env Env, cmd *cli.Command) error {
	targets, err := nodeTargets(cmd)
	if err != nil {
		return err
	}
	d, err := openCluster(cmd)
	if err != nil {
		return err
	}
	// What the cluster directory refuses on its own comes before the
	// release's download, the CA key and the nodes.
	if err := checkTargets(env, d, targets); err != nil {
		return err
	}
	rel, err := openPinned(ctx, env, cmd, d)
	if err != nil {
		return err
	}
	ca, err := openCA(env, cmd, d)
	if err != nil {
		return err
	}
	// From here on, a signal ends the preflight, the reach of a host or
	// the install under way, which then rolls back, and node add gives
	// the lock back, rather than ending mooctl. Once an install has
	// finished, its node is recorded all the same.
	ctx, stop := interrupt.Context(ctx, env.Exit)
	defer stop()
	cl, err := preflight(ctx, env, cmd, d, "", nil)
	if err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("node add interrupted: %w", err)
		}
		return err
	}
	defer cl.Close()
	// The rest runs only while the operation holds the lock.
	ctx = cl.Context()
	hosts, err := reachHosts(ctx, env, d, targets)
	// Once a new node is added, the cluster closes its connection, after
	// it has released the lock on the node over it.
	defer func() {
		for _, h := range hosts {
			if !h.added {
				h.c.Close()
			}
		}
	}()
	if err != nil {
		if len(targets) > 1 {
			return fmt.Errorf("%w\nNothing was installed.", err)
		}
		return err
	}
	removed, err := d.RemovedIdentities()
	if err != nil {
		return err
	}
	return addEach(ctx, env, rerunNodeAdd(cmd), hosts, func(h *newHost, later bool) error {
		return addNode(ctx, env, cl, d, rel, ca, cluster.Fingerprints(removed), h, later)
	})
}

// addEach adds each of hosts in turn with add, which is told whether the
// host comes after another, and stops at the first that fails, or before
// a host once a signal or the loss of the operation lock has ended ctx.
// It reminds the operator to commit once, at the end, if it added any.
// again is the command line that runs node add again, for the hosts it
// didn't add.
func addEach(ctx context.Context, env Env, again string, hosts []*newHost, add func(h *newHost, later bool) error) error {
	for i, h := range hosts {
		err, tried := stoppedBefore(ctx, "adding "+h.target), false
		if err == nil {
			err, tried = add(h, i > 0), true
		}
		if err != nil {
			if i > 0 || errors.As(err, new(*addedError)) {
				remindToCommit(env)
			}
			return addedSoFar(err, again, hosts, i, tried)
		}
	}
	remindToCommit(env)
	return nil
}

// rerunNodeAdd is the start of the command line that runs node add as
// cmd ran it, but for other targets: in its cluster directory, from its
// release source, with its SSH key, which opens the secret store, and
// --force if it had it, each quoted for a shell.
func rerunNodeAdd(cmd *cli.Command) string {
	args := mooctlIn(cmd)
	if key := cmd.String("ssh-key"); key != "" {
		args = append(args, "--ssh-key", key)
	}
	args = append(args, "node", "add")
	if cmd.Bool("force") {
		args = append(args, "--force")
	}
	for i, arg := range args {
		args[i] = shellQuote(arg)
	}
	return strings.Join(args, " ")
}

// stoppedBefore is why an operation stops before doing, such as
// "adding web1", if a signal or the loss of the operation lock has
// stopped it.
func stoppedBefore(ctx context.Context, doing string) error {
	if cause := context.Cause(ctx); errors.Is(cause, orchestrate.ErrLockLost) {
		return fmt.Errorf("before %s: %w", doing, cause)
	}
	if ctx.Err() != nil {
		return fmt.Errorf("interrupted before %s", doing)
	}
	return nil
}

// stopped is err, from what node add was doing, saying so if a signal or
// the loss of the operation lock is what ended it.
func stopped(ctx context.Context, doing string, err error) error {
	if cause := context.Cause(ctx); errors.Is(cause, orchestrate.ErrLockLost) {
		return fmt.Errorf("%s: %w", doing, cause)
	}
	if ctx.Err() != nil {
		return fmt.Errorf("%s: interrupted: %w", doing, err)
	}
	return fmt.Errorf("%s: %w", doing, err)
}

// newHost is a host that node add reached and checked, to add as the
// node n, with its target as the operator gave it, arg, and its SSH
// target alone.
type newHost struct {
	*reachedHost
	arg, target string
	n           cluster.Node
	// added says the operation took over the host's connection.
	added bool
}

// reachHosts reaches and checks each target in turn, and stops at the
// first that fails. It returns the hosts reached, whose connections the
// caller closes, even when it fails.
func reachHosts(ctx context.Context, env Env, d *cluster.Dir, targets []nodeTarget) ([]*newHost, error) {
	var hosts []*newHost
	for _, t := range targets {
		h, err := reachHost(ctx, env, t.target, t.private, "Pinning the host key of %s: %s")
		if err != nil {
			if ctx.Err() != nil {
				return hosts, stopped(ctx, "reaching "+t.target, err)
			}
			return hosts, err
		}
		nh := &newHost{reachedHost: h, arg: t.arg, target: t.target, n: cluster.Node{
			Name:           t.name,
			Address:        hostOf(t.target),
			User:           h.settings.User,
			HostKey:        h.key,
			PrivateAddress: h.private,
			Architecture:   h.host.Architecture,
		}}
		hosts = append(hosts, nh)
		if err := d.CheckNewNode(nh.n); err != nil {
			return hosts, fmt.Errorf("%s: %w", t.target, err)
		}
	}
	return hosts, nil
}

// recheckTimeout bounds the check that addNode makes again of a host
// whose connection has sat idle through the installs before it, which a
// firewall may have dropped without a word.
const recheckTimeout = time.Minute

// addNode installs the release on h, records it in the cluster directory
// at the next generation, and has every node record that generation.
// A host that comes later, after another, it checks again first, since
// an earlier install may have changed it: two targets can reach the same
// host, which no host key tells apart from a clone.
func addNode(ctx context.Context, env Env, cl *orchestrate.Cluster, d *cluster.Dir, rel *release.Release, ca *identity.CA, removed []string, h *newHost, later bool) error {
	n, name := h.n, h.n.Name
	adding := fmt.Sprintf("adding %s as %s", h.target, name)
	if later {
		// The host must answer within recheckTimeout, but the check, which
		// a slow host may take longer to run, has no bound of its own, as
		// the first check had none.
		answerCtx, cancel := context.WithTimeout(ctx, recheckTimeout)
		_, err := h.c.Run(answerCtx, "true", nil)
		timedOut := err != nil && errors.Is(answerCtx.Err(), context.DeadlineExceeded)
		cancel()
		if err == nil {
			_, err = bootstrap.Check(ctx, h.c)
		}
		switch {
		case err == nil:
		case ctx.Err() != nil:
			return stopped(ctx, "checking "+h.target+" again", err)
		case timedOut:
			return fmt.Errorf("%s didn't answer when checked again before its install, within %s, and may be down or cut off: %w", h.target, recheckTimeout, err)
		default:
			return fmt.Errorf("%s can't become a node: %w", h.target, err)
		}
	}
	if err := cl.CheckLock(); err != nil {
		return err
	}
	fmt.Fprintf(env.Stdout, "Installing release %s on %s, as node %s (%s)\n", rel.Version, h.target, name, n.Architecture)
	var err error
	n.Identity, err = bootstrap.Install(ctx, h.c, h.host, rel, hostlayout.NodeConfig{Cluster: d.Cluster.Name, Node: name, Mode: hostlayout.ModeDirect, PrivateAddress: n.PrivateAddress}, ca, removed, cl.Lock)
	if err != nil {
		return stopped(ctx, adding, err)
	}
	generation := cl.Next(d.Cluster.Generation)
	if err := d.AddNode(n, generation); err != nil {
		return bootstrap.Rollback(ctx, h.c, fmt.Errorf("recording %s: %w", name, err))
	}
	fmt.Fprintf(env.Stdout, "Added node %s, at generation %d of the cluster directory\n", name, generation)
	// The other nodes record the generation even if the new node can't
	// be reached to.
	if node, err := nodeclient.Dial(h.c); err != nil {
		cl.Add(name, unreachedNode{err})
	} else {
		cl.Add(name, sshNode{node, h.c})
		h.added = true
	}
	interrupted, err := tellNodes(ctx, env, cl, h.target+" is added as "+name, func(ctx context.Context) error {
		return cl.Record(ctx, generation)
	})
	if err != nil {
		return &addedError{fmt.Errorf("added %s, but cluster status shows these nodes behind until an operation that changes the cluster directory records its generation: %w", name, err)}
	}
	if interrupted {
		return &addedError{fmt.Errorf("node add interrupted once %s was added as %s, which the nodes recorded", h.target, name)}
	}
	return nil
}

// addedError is a failure after the node was added.
type addedError struct{ error }

func (e *addedError) Unwrap() error { return e.error }

// addedSoFar is err, the failure of node add at hosts[failed], which it
// tried unless it stopped before, saying which hosts were added before
// it, which weren't tried, and the command line again begins that adds
// those not added, each target as it was given, options included, and
// quoted for a shell.
func addedSoFar(err error, again string, hosts []*newHost, failed int, tried bool) error {
	if len(hosts) == 1 {
		return err
	}
	var added, notTried, notAdded []string
	for _, h := range hosts[:failed] {
		added = append(added, h.n.Name+" ("+h.target+")")
	}
	if errors.As(err, new(*addedError)) {
		added = append(added, hosts[failed].n.Name+" ("+hosts[failed].target+")")
	} else if tried {
		notAdded = append(notAdded, shellQuote(hosts[failed].arg))
	}
	rest := hosts[failed+1:]
	if !tried {
		rest = hosts[failed:]
	}
	for _, h := range rest {
		notTried = append(notTried, shellQuote(h.arg))
	}
	notAdded = append(notAdded, notTried...)
	if len(added) > 0 {
		err = fmt.Errorf("%w\nAdded before it stopped: %s.", err, strings.Join(added, ", "))
	}
	if len(notTried) > 0 {
		err = fmt.Errorf("%w\nNot tried: %s", err, strings.Join(notTried, " "))
	}
	if len(notAdded) > 0 {
		err = fmt.Errorf("%w\nTo add the hosts not added, run: %s %s", err, again, strings.Join(notAdded, " "))
	}
	return err
}

// nodeJoin is mooctl node add --join-token: it installs the release the
// cluster directory pins on the host, and joins it to the control plane
// of the token, which operates it from then on. It runs no preflight,
// and leaves the cluster directory as it is.
func nodeJoin(ctx context.Context, env Env, cmd *cli.Command) error {
	tok, err := controlplanev1.ParseToken(cmd.String("join-token"))
	if err != nil {
		return fmt.Errorf("--join-token: %w", err)
	}
	switch {
	case cmd.String("name") != "":
		return errors.New("--name doesn't go with --join-token: the control plane names the node")
	case cmd.Bool("force"):
		return errors.New("--force doesn't go with --join-token: a node that joins a control plane applies no generation of the cluster directory")
	}
	targets, err := nodeTargets(cmd)
	if err != nil {
		return err
	}
	target, private := targets[0].target, targets[0].private
	if targets[0].name != "" {
		return errors.New("name= doesn't go with --join-token: the control plane names the node")
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

// targetOptions are the options a target of node add takes, each after a
// comma, as key=value, as the flags of their names, with what each value
// is.
var targetOptions = []targetOption{{"name", "name"}, {"private-address", "address"}}

// targetOption is an option of a target of node add: its key, and what
// its value is.
type targetOption struct{ key, value string }

// nodeTarget is an SSH target of node add, with the name and the private
// address given for it, each the zero value if not given.
type nodeTarget struct {
	// arg is the target as the operator gave it, options and all.
	arg     string
	target  string
	name    string
	private netip.Addr
}

// nodeTargets are node add's targets, with the options each carries.
// --name and --private-address go only with a single target, which then
// mustn't give the same option.
func nodeTargets(cmd *cli.Command) ([]nodeTarget, error) {
	var targets []nodeTarget
	asked := map[string]string{}
	for _, arg := range cmd.Args().Slice() {
		t, err := parseNodeTarget(arg)
		if err != nil {
			return nil, err
		}
		if other, ok := asked[t.name]; ok && t.name != "" {
			return nil, fmt.Errorf("%s and %s both ask for the name %s", other, t.target, t.name)
		}
		asked[t.name] = t.target
		targets = append(targets, t)
	}
	// An empty flag is absent, as from a variable that isn't set.
	for _, option := range targetOptions {
		flag := option.key
		if cmd.String(flag) == "" {
			continue
		}
		if len(targets) > 1 {
			return nil, fmt.Errorf("--%s goes with a single target; with several, give each its own as <ssh target>,%s=<%s>", flag, flag, option.value)
		}
		t := &targets[0]
		if flag == "name" && t.name != "" || flag == "private-address" && t.private.IsValid() {
			return nil, fmt.Errorf("%s is given both by --%s and in the target %s", flag, flag, cmd.Args().First())
		}
	}
	if len(targets) == 1 {
		t := &targets[0]
		if name := cmd.String("name"); name != "" {
			if err := cluster.CheckName(name); err != nil {
				return nil, fmt.Errorf("--name: node name %w", err)
			}
			t.name = name
		}
		private, err := privateAddressFlag(cmd)
		if err != nil {
			return nil, err
		}
		if private.IsValid() {
			t.private = private
		}
	}
	return targets, nil
}

// parseNodeTarget parses arg, an SSH target followed by options, each
// after a comma: name=<name> and private-address=<address>. The target
// ends at the first comma after its last @, or after its start if it
// has none: a host never holds a comma, and an option's value never
// holds an @.
func parseNodeTarget(arg string) (nodeTarget, error) {
	target, options, _ := strings.Cut(arg, ",")
	if i := strings.LastIndex(arg, "@"); i >= 0 {
		if j := strings.IndexByte(arg[i:], ','); j >= 0 {
			target, options = arg[:i+j], arg[i+j+1:]
		} else {
			target, options = arg, ""
		}
	}
	refuse := func(format string, a ...any) (nodeTarget, error) {
		return nodeTarget{}, fmt.Errorf("target %q: %s", arg, fmt.Sprintf(format, a...))
	}
	if target == "" {
		return refuse("no SSH target before its options")
	}
	if i := strings.LastIndex(target, "@"); i >= 0 {
		for _, option := range targetOptions {
			if strings.Contains(target[:i], ","+option.key+"=") {
				return refuse("an option's value holds an @, which no name or address does")
			}
		}
	}
	t := nodeTarget{arg: arg, target: target}
	if !strings.ContainsRune(arg[len(target):], ',') {
		return t, nil
	}
	given := map[string]bool{}
	for _, option := range strings.Split(options, ",") {
		if option == "" {
			return refuse("an empty option")
		}
		key, value, _ := strings.Cut(option, "=")
		if !slices.ContainsFunc(targetOptions, func(o targetOption) bool { return o.key == key }) {
			return refuse("unknown option %s; a target takes name=<name> and private-address=<address>", key)
		}
		if given[key] {
			return refuse("%s is given twice", key)
		}
		given[key] = true
		if value == "" {
			return refuse("%s has no value", key)
		}
		if key == "name" {
			if err := cluster.CheckName(value); err != nil {
				return refuse("node name %v", err)
			}
			t.name = value
			continue
		}
		private, err := netip.ParseAddr(value)
		if err != nil {
			return refuse("private-address: %v", err)
		}
		t.private = private
	}
	return t, nil
}

// checkTargets checks what it can of the targets before reaching them:
// that no two are the same, and none is a node of d already, each as the
// user and the host it names, the user resolved from the operator's SSH
// config, as a node file records them; and that no name asked for is
// taken. It gives each target without a name the first free one.
func checkTargets(env Env, d *cluster.Dir, targets []nodeTarget) error {
	nodes, err := d.Nodes()
	if err != nil {
		return err
	}
	opts := sshOptions(env)
	var names []string
	given := map[string]string{}
	for _, t := range targets {
		settings, err := sshconfig.Resolve(t.target, opts.SSH)
		if err != nil {
			return err
		}
		// Hosts are named without regard to case, as ssh matches them.
		as := settings.User + "@" + strings.ToLower(hostOf(t.target))
		if other, ok := given[as]; ok {
			return fmt.Errorf("%s and %s are the same target, %s: give each host once", other, t.target, as)
		}
		given[as] = t.target
		for _, n := range nodes {
			if strings.EqualFold(n.Address, hostOf(t.target)) && n.User == settings.User {
				return fmt.Errorf("%s is node %s of the cluster already: leave it out, or to add its host again, remove the node first with mooctl node remove %s", t.target, n.Name, n.Name)
			}
			if n.Name == t.name {
				return fmt.Errorf("%s asks for the name %s, which node %s of the cluster has already", t.target, t.name, n.Name)
			}
		}
		names = append(names, t.name)
	}
	for i := range targets {
		if targets[i].name == "" {
			if targets[i].name, err = d.FreeNodeName(names...); err != nil {
				return err
			}
			names = append(names, targets[i].name)
		}
	}
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
	d, err := openCluster(cmd)
	if err != nil {
		return nil, nil, err
	}
	rel, err := openPinned(ctx, env, cmd, d)
	if err != nil {
		return nil, nil, err
	}
	return d, rel, nil
}

// openPinned opens the release that d pins, which this mooctl must
// operate.
func openPinned(ctx context.Context, env Env, cmd *cli.Command, d *cluster.Dir) (*release.Release, error) {
	if err := installable(env, d.Cluster.Release); err != nil {
		return nil, err
	}
	return openRelease(ctx, env, cmd, d)
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
	d, err := openCluster(cmd)
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
	// From here on, a signal ends the preflight or the reach of the
	// node's host, and node remove gives the lock back, rather than
	// ending mooctl. It doesn't stop the uninstall halfway; once that has
	// finished, the node is recorded as removed all the same.
	ctx, stop := interrupt.Context(ctx, env.Exit)
	defer stop()
	cl, err := preflight(ctx, env, cmd, d, skip, nil)
	if err != nil && ctx.Err() != nil {
		return fmt.Errorf("node remove interrupted: %w", err)
	}
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
	} else if err := stoppedBefore(ctx, "removing "+name); err != nil {
		return err
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
	interrupted, err := tellNodes(ctx, env, cl, name+" is removed", func(ctx context.Context) error {
		return errors.Join(cl.RemoveIdentities(ctx, cluster.Fingerprints(removed)), cl.Record(ctx, generation))
	})
	if err != nil {
		return fmt.Errorf("removed %s, but the next operation has to finish telling these nodes, until which they may still accept its identity: %w", name, err)
	}
	if interrupted {
		return fmt.Errorf("node remove interrupted once %s was removed, which the nodes were told", name)
	}
	return nil
}

// tellNodes runs tell, which tells the nodes of change, made already, in
// the operation's held context, so that a signal, which ends ctx, the
// operation's, doesn't stop it: only the loss of the operation lock
// does. A signal that comes before tellNodes is done with tell is
// answered at once, on stderr. tellNodes returns whether one came, and
// tell's error, which says so if the lost lock ended it. Each call to a
// node keeps its own deadline.
func tellNodes(ctx context.Context, env Env, cl *orchestrate.Cluster, change string, tell func(context.Context) error) (interrupted bool, err error) {
	answered, said := make(chan struct{}), false
	stop := context.AfterFunc(ctx, func() {
		defer close(answered)
		// What ends ctx but the lost lock is a signal.
		if said = !errors.Is(context.Cause(ctx), orchestrate.ErrLockLost); said {
			fmt.Fprintf(env.Stderr, "Interrupted, but %s: mooctl tells the nodes first. Interrupt again to stop now, and leave them to the next operation.\n", change)
		}
	})
	held := cl.Held()
	err = tell(held)
	if err != nil && held.Err() != nil {
		err = stopped(held, "telling the nodes", err)
	}
	// Once stop has the answer, if it has begun, nothing races it, and a
	// signal came if, and only if, the answer said so.
	if !stop() {
		<-answered
		return said, err
	}
	return false, err
}

// uninstall stops n's cluster services and removes its node components
// from its host, over SSH, once it has left n out of the operation cl.
func uninstall(ctx context.Context, env Env, cl *orchestrate.Cluster, n cluster.Node) error {
	c, err := sshclient.Dial(ctx, sshclient.Node{Target: n.User + "@" + n.Address, HostKeys: []ssh.PublicKey{n.HostKey}}, sshOptions(env))
	if err != nil && ctx.Err() != nil {
		return stopped(ctx, "reaching "+n.Name+"'s host to clean it, which stays in the cluster directory", err)
	}
	if err != nil {
		return fmt.Errorf("%s stays in the cluster directory, since its host can't be reached to clean it: %w\nIf it can't be reached, remove it without reaching it with mooctl node remove %s --unreachable.", n.Name, err, n.Name)
	}
	defer c.Close()
	if err := cl.CheckLock(); err != nil {
		return err
	}
	fmt.Fprintf(env.Stdout, "Stopping the cluster services of %s and removing its node components\n", n.Name)
	cl.Remove(n.Name)
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

func (n unreachedNode) ImportImage(context.Context, []byte, string) ([]string, error) {
	return nil, n.err
}

func (n unreachedNode) PullImage(context.Context, string, string) (nodeclient.PulledImage, error) {
	return nodeclient.PulledImage{}, n.err
}

func (n unreachedNode) Run(context.Context, *nodev1.Container, string) (bool, error) {
	return false, n.err
}

func (n unreachedNode) Remove(context.Context, string, string) error { return n.err }

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
