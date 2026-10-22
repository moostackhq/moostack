package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/urfave/cli/v3"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/cluster"
	"github.com/moostackhq/moostack/internal/identity"
	"github.com/moostackhq/moostack/internal/interrupt"
	"github.com/moostackhq/moostack/internal/nodeclient"
	"github.com/moostackhq/moostack/internal/ociimage"
	"github.com/moostackhq/moostack/internal/orchestrate"
	"github.com/moostackhq/moostack/internal/release"
	"github.com/moostackhq/moostack/internal/secretstore"
)

// clusterCommand is mooctl cluster.
func clusterCommand(env Env) *cli.Command {
	return &cli.Command{
		Name:  "cluster",
		Usage: "create and inspect the cluster",
		Commands: []*cli.Command{
			{
				Name:  "init",
				Usage: "make the cluster directory, pinned to this mooctl's release",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "name",
						Usage: "the cluster's name, instead of the directory's",
					},
				},
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if err := wantArgs(cmd, 0); err != nil {
						return err
					}
					return clusterInit(env, cmd)
				},
			},
			{
				Name:  "status",
				Usage: "show each node's mode and node components, next to the pinned release",
				Flags: []cli.Flag{
					&cli.BoolFlag{
						Name:  "json",
						Usage: "print the status as JSON",
					},
				},
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if err := wantArgs(cmd, 0); err != nil {
						return err
					}
					return clusterStatus(ctx, env, cmd)
				},
			},
			{
				Name:      "update",
				Usage:     "move every node to the release pinned in the cluster directory, one node at a time, or with --source to development builds from a Moostack checkout",
				ArgsUsage: "[component...]",
				Description: "With --source, cluster update builds the components named, or all of them, from the checkout, for each node's architecture, and updates every node to them, leaving the pinned release as it is. " +
					"The components are " + sourceComponents("and") + ". " +
					"A development build is versioned as the checkout's commit, followed by -dirty if the checkout has uncommitted changes. " +
					"cluster update without --source puts the pinned release back.",
				Flags: []cli.Flag{
					forceFlag,
					&cli.StringFlag{
						Name:  "source",
						Usage: "the Moostack checkout to build development builds from, instead of installing the pinned release",
					},
				},
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.IsSet("source") && cmd.String("source") == "" {
						return errors.New("--source is empty; give it the Moostack checkout to build from, or leave it out to install the pinned release")
					}
					if cmd.String("source") == "" && cmd.Args().Len() > 0 {
						return fmt.Errorf("cluster update takes components to build only with --source, the Moostack checkout to build them from; without it, it installs the pinned release, all of it")
					}
					return withHint(clusterUpdate(ctx, env, cmd))
				},
			},
			{
				Name:  "unlock",
				Usage: "break the operation lock, whoever holds it, when their mooctl has died",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if err := wantArgs(cmd, 0); err != nil {
						return err
					}
					return clusterUnlock(ctx, env, cmd)
				},
			},
		},
	}
}

// caKeySecret is the name of the secret that holds the cluster CA key,
// PEM, in the secret store.
const caKeySecret = "cluster-ca-key"

// clusterInit is mooctl cluster init. It creates the cluster CA, and
// keeps its key in the secret store, creating the store or else adding
// the key to the one that is there. It refuses an existing cluster
// directory before asking the operator anything. If it fails, it undoes
// what it did, so that it can run again.
func clusterInit(env Env, cmd *cli.Command) error {
	dir, name := cmd.String("dir"), cmd.String("name")
	if env.Version == "" {
		return errors.New("this mooctl has no release to pin, since it wasn't built by the release build")
	}
	if name == "" {
		abs, err := filepath.Abs(dir)
		if err != nil {
			return err
		}
		name = filepath.Base(abs)
		if err := cluster.CheckName(name); err != nil {
			return fmt.Errorf("the directory's name can't name the cluster: %w; choose a name with --name", err)
		}
	}
	c := cluster.Cluster{Name: name, Release: env.Version}
	if err := cluster.CheckInit(dir); err != nil {
		return err
	}

	t, closeTerminal := openTerminal(env)
	defer closeTerminal()
	store, err := openCAStore(filepath.Join(dir, secretstore.FileName), keyOptions(env, cmd, t))
	if err != nil {
		return err
	}
	ca, err := identity.NewCA(name)
	if err != nil {
		return err
	}

	ignored, err := writeCluster(dir, c, ca, store)
	if err != nil {
		return err
	}
	fmt.Fprintf(env.Stdout, "Created cluster %s on release %s in %s\n", c.Name, c.Release, dir)
	if store.store == nil {
		fmt.Fprintf(env.Stdout, "Created %s for %s %s, with the cluster CA key\n", store.path, store.recipient.Fingerprint(), store.recipient.Comment)
	} else {
		fmt.Fprintf(env.Stdout, "Added the cluster CA key to %s\n", store.path)
	}
	if ignored {
		fmt.Fprintf(env.Stdout, "Added %s to .gitignore\n", secretstore.LockFileName)
	}
	remindToCommit(env)
	return nil
}

// caStore is the secret store that cluster init keeps the CA key in:
// the open store at path, or, if store is nil, a new one for recipient.
type caStore struct {
	path      string
	store     *secretstore.Store
	recipient secretstore.Recipient
}

// openCAStore opens the store at path for cluster init, refusing one
// that already has a CA key. If there is no store, it chooses the
// operator's key for a new one.
func openCAStore(path string, opts secretstore.KeyOptions) (caStore, error) {
	if _, err := os.Lstat(path); errors.Is(err, fs.ErrNotExist) {
		recipient, err := secretstore.ChooseKey(path, opts)
		return caStore{path: path, recipient: recipient}, err
	} else if err != nil {
		return caStore{}, err
	}
	store, err := secretstore.Unlock(path, opts)
	if err != nil {
		return caStore{}, err
	}
	if slices.Contains(store.Names(), caKeySecret) {
		return caStore{}, fmt.Errorf("%s already has a cluster CA key, %s", path, caKeySecret)
	}
	return caStore{path: path, store: store}, nil
}

// keep keeps key, the CA key, in the store, creating the store first if
// it is new, and adds to undo what puts the store back.
func (s caStore) keep(key []byte, undo *rollback) error {
	// Save makes the lock file if there is none. It is removed only once
	// no store is left next to it, since a lock file must never be
	// removed while it is in use.
	lock := filepath.Join(filepath.Dir(s.path), secretstore.LockFileName)
	if _, err := os.Lstat(lock); errors.Is(err, fs.ErrNotExist) {
		undo.add(func() {
			if _, err := os.Lstat(s.path); errors.Is(err, fs.ErrNotExist) {
				os.Remove(lock)
			}
		})
	}
	store := s.store
	if store == nil {
		var err error
		if store, err = secretstore.Init(s.path, s.recipient); err != nil {
			return err
		}
		undo.add(func() {
			if ok, err := store.Unchanged(); err == nil && ok {
				os.Remove(s.path)
			}
		})
	}
	if err := store.Put(caKeySecret, secretstore.Secret{Value: string(key)}); err != nil {
		return err
	}
	if err := store.Save(); err != nil {
		return fmt.Errorf("saving the cluster CA key in %s: %w", s.path, err)
	}
	return nil
}

// writeCluster writes the cluster directory dir for c and ca, and keeps
// ca's key in store. It reports whether it added the store's lock file
// to .gitignore. If it fails, it undoes what it wrote, but not what
// someone else wrote meanwhile, such as a save to the store.
func writeCluster(dir string, c cluster.Cluster, ca *identity.CA, store caStore) (ignored bool, err error) {
	var undo rollback
	defer func() {
		if err != nil {
			undo.run()
		}
	}()
	if err := makeDir(dir, &undo); err != nil {
		return false, err
	}
	if ignored, err = ignoreLockFile(dir, &undo); err != nil {
		return false, fmt.Errorf("adding %s to .gitignore: %w", secretstore.LockFileName, err)
	}
	if _, err := cluster.Init(dir, c, ca.CertPEM()); err != nil {
		return false, err
	}
	undo.putBack(filepath.Join(dir, cluster.CAFileName), nil, false)
	undo.putBack(filepath.Join(dir, cluster.FileName), nil, false)
	return ignored, store.keep(ca.KeyPEM(), &undo)
}

// makeDir makes dir and its missing parents, and adds to undo their
// removal, innermost first. Removing a directory that isn't empty fails,
// so undo leaves alone one that someone else wrote in.
func makeDir(dir string, undo *rollback) error {
	var made []string
	for p := dir; ; p = filepath.Dir(p) {
		if _, err := os.Lstat(p); !errors.Is(err, fs.ErrNotExist) {
			break
		}
		made = append(made, p)
		if filepath.Dir(p) == p {
			break
		}
	}
	undo.add(func() {
		for _, p := range made {
			os.Remove(p)
		}
	})
	return os.MkdirAll(dir, 0o755)
}

// ignoreLockFile is secretstore.IgnoreLockFile, adding to undo what puts
// the .gitignore back.
func ignoreLockFile(dir string, undo *rollback) (changed bool, err error) {
	path := filepath.Join(dir, ".gitignore")
	before, err := os.ReadFile(path)
	existed := err == nil
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return false, err
	}
	// Even a failure may have written part of the file.
	changed, err = secretstore.IgnoreLockFile(dir)
	if changed || err != nil {
		undo.putBack(path, before, existed)
	}
	return changed, err
}

// rollback is what undoes a command's writes, newest first.
type rollback []func()

// add adds f to the rollback.
func (u *rollback) add(f func()) { *u = append(*u, f) }

// run undoes the writes.
func (u rollback) run() {
	for _, f := range slices.Backward(u) {
		f()
	}
}

// putBack adds to the rollback what puts the file at path back as it
// was: before if it existed, and else no file. It notes the file as the
// command has just left it, and leaves it alone if someone else has
// changed it since.
func (u *rollback) putBack(path string, before []byte, existed bool) {
	left, err := os.ReadFile(path)
	if err != nil {
		return
	}
	u.add(func() {
		if now, err := os.ReadFile(path); err != nil || !bytes.Equal(now, left) {
			return
		}
		if existed {
			os.WriteFile(path, before, 0o644)
		} else {
			os.Remove(path)
		}
	})
}

// clusterStatus is mooctl cluster status. It reaches every node at once,
// and shows those it can't reach rather than failing.
func clusterStatus(ctx context.Context, env Env, cmd *cli.Command) error {
	d, err := cluster.Open(cmd.String("dir"))
	if err != nil {
		return err
	}
	// Status shows the nodes even without the pinned release, as when
	// its source is down: only what they run, without what is pinned.
	rel, err := openRelease(ctx, env, cmd, d)
	if err != nil {
		fmt.Fprintf(env.Stderr, "cluster status can't open release %s, which the cluster directory pins, so it is showing what the nodes run without it: %v\n", d.Cluster.Release, err)
		rel = nil
	}
	nodes, err := d.Nodes()
	if err != nil {
		return err
	}
	reached := orchestrate.Survey(ctx, nodesToDial(env, nodes))
	view := statusOf(d, rel, nodes, reached)
	// Status says to run cluster update only for a pin it would take.
	if rel != nil && (view.Update == "not_started" || view.Update == "stopped_partway" || view.Update == "development_builds") {
		if err := refusal(env, rel.Manifest, reached); err != nil {
			view.Update, view.Refusal = "refused", withHint(err).Error()
		}
	}
	if cmd.Bool("json") {
		e := json.NewEncoder(env.Stdout)
		e.SetIndent("", "  ")
		return e.Encode(view)
	}
	return printStatus(env, view)
}

// printStatus prints the cluster status view as a table, with the
// problems it found below.
func printStatus(env Env, view statusView) error {
	var b strings.Builder
	fmt.Fprintf(&b, "Cluster %s on release %s\n", view.Cluster, view.Release)
	fmt.Fprintf(&b, "Operation lock: %s\n", describeLocks(view.Lock, len(view.Nodes), view.answered))
	update := updateLines[view.Update]
	if view.Refusal != "" {
		update += "\n" + view.Refusal
	}
	fmt.Fprintf(&b, "Update: %s\n\n", update)
	var t statusTree
	for _, n := range view.Nodes {
		if !n.Reachable {
			t.node(n.Name, "unreachable")
			t.problem(n.Name + ": " + nodeclient.Printable(n.Error))
			continue
		}
		mode := n.Mode
		if mode == "" {
			mode = "mode unknown"
		}
		t.node(n.Name, mode)
		// The cluster services sit under the agent that runs them. A node
		// whose agent isn't listed has them after its node components, at
		// their level, so that they read as no other component's.
		services := n.Services
		for _, c := range n.Components {
			t.component(n.Name, 1, c)
			if c.Name == "agent" {
				for _, s := range services {
					t.component(n.Name, 2, s)
				}
				services = nil
			}
		}
		for _, s := range services {
			t.component(n.Name, 1, s)
		}
		if n.GenerationProblem != "" {
			t.problem(n.Name + ": " + nodeclient.Printable(n.GenerationProblem))
		}
	}
	t.print(&b)
	if len(t.problems) > 0 {
		fmt.Fprintf(&b, "\n%s\n", strings.Join(t.problems, "\n"))
	}
	_, err := io.WriteString(env.Stdout, b.String())
	return err
}

// statusTree is cluster status's tree: a line for each node, with its
// node components indented below it and its cluster services below the
// agent, their versions and health in columns under one header, and the
// problems it found, in the tree's order.
type statusTree struct {
	// lines are the tree's lines, each its cells: a node's line has one,
	// a component's four.
	lines    [][]string
	problems []string
}

// node adds the line of the node called name, a name of the cluster
// directory, in state: its mode, "mode unknown" or "unreachable".
func (t *statusTree) node(name, state string) {
	t.lines = append(t.lines, []string{name + "  " + state})
}

// component adds the line of c, a component of node, indented depth
// levels below it, and its problem, if it has one.
func (t *statusTree) component(node string, depth int, c componentStatusView) {
	running := nodeclient.Printable(dash(c.Running))
	if c.Development {
		running += " (development build)"
	}
	t.lines = append(t.lines, []string{strings.Repeat("  ", depth) + nodeclient.Printable(c.Name), running, nodeclient.Printable(dash(c.Pinned)), nodeclient.Printable(c.Health)})
	if c.Problem != "" {
		t.problem(fmt.Sprintf("%s: %s: %s", node, nodeclient.Printable(c.Name), nodeclient.Printable(c.Problem)))
	}
}

// problem adds p to the problems listed below the tree.
func (t *statusTree) problem(p string) { t.problems = append(t.problems, p) }

// print writes the tree to w, under its header, with each column of the
// components' lines as wide as its widest cell, in characters, two spaces
// apart. A node's line takes no part in the columns.
func (t *statusTree) print(w io.Writer) {
	lines := append([][]string{{"NODE", "RUNNING", "PINNED", "HEALTH"}}, t.lines...)
	widths := make([]int, 4)
	for _, l := range lines {
		if len(l) == 1 {
			continue
		}
		for i, cell := range l {
			widths[i] = max(widths[i], utf8.RuneCountInString(cell))
		}
	}
	for _, l := range lines {
		var b strings.Builder
		for i, cell := range l {
			b.WriteString(cell)
			if i < len(l)-1 {
				b.WriteString(strings.Repeat(" ", widths[i]-utf8.RuneCountInString(cell)+2))
			}
		}
		fmt.Fprintln(w, b.String())
	}
}

// statusView is what cluster status shows, as a table or as JSON.
type statusView struct {
	Cluster    string `json:"cluster"`
	Release    string `json:"release"`
	Generation int64  `json:"generation"`
	// Lock is the operation lock as the nodes hold it: one entry for each
	// operation that holds it on any node. It is empty if no node does.
	Lock []lockView `json:"lock,omitempty"`
	// Update is whether the nodes reached run the pinned release, as
	// updateOf says, or unknown if mooctl couldn't open it.
	Update string `json:"update"`
	// Refusal is why cluster update refuses the pinned release, if it
	// does.
	Refusal string           `json:"refusal,omitempty"`
	Nodes   []nodeStatusView `json:"nodes"`
	// answered is how many nodes' agents answered, and so could say
	// whether they hold the lock.
	answered int
}

// lockView is the operation lock as one operation holds it, on the nodes
// called Nodes.
type lockView struct {
	Holder  string    `json:"holder"`
	Machine string    `json:"machine"`
	Since   time.Time `json:"since"`
	Nodes   []string  `json:"nodes"`
	lock    nodeclient.Lock
}

// locksOf are the operation locks the reached nodes hold, one for each
// lease, in the order a node first reported it.
func locksOf(reached []orchestrate.Reached) []lockView {
	var views []lockView
	for _, r := range reached {
		l := r.Status.Lock
		if r.Err != nil || l == nil {
			continue
		}
		i := slices.IndexFunc(views, func(v lockView) bool { return v.lock.Lease == l.Lease })
		if i < 0 {
			views = append(views, lockView{Holder: l.Holder, Machine: l.Machine, Since: l.Since, lock: *l})
			i = len(views) - 1
		}
		// A node on a release before the lock recorded its operation
		// reports none.
		if views[i].lock.Operation == "" {
			views[i].lock.Operation = l.Operation
		}
		views[i].Nodes = append(views[i].Nodes, r.Name)
	}
	return views
}

// describeLocks says who holds the operation lock, as locks says, in a
// cluster of nodes nodes, of which answered could say: free, or held,
// with the nodes that hold it unless every node does.
func describeLocks(locks []lockView, nodes, answered int) string {
	if len(locks) == 0 && answered < nodes {
		return "free on the nodes whose agent answered"
	}
	if len(locks) == 0 {
		return "free"
	}
	var held []string
	for _, l := range locks {
		s := "held by " + orchestrate.DescribeLock(l.lock)
		if len(locks) > 1 || len(l.Nodes) < nodes {
			s += " on " + strings.Join(l.Nodes, ", ")
		}
		held = append(held, s)
	}
	return strings.Join(held, "; ")
}

// clusterUpdate is mooctl cluster update. It reads every file of the
// pinned release that the nodes need, checked against the release's
// checksums, before it reaches any node. After the preflight, which
// refuses a node on a release that the pinned one doesn't update, it
// updates each node in turn to the release's node components, for the
// node's architecture: the agent, then the updater, then containerd.
// Then it updates each node in turn to the release's cluster services.
// It skips what a node runs at the target already, and stops at the first
// step that fails. It doesn't change the cluster directory. With --source,
// it updates the nodes to development builds instead, as runSourceUpdate
// says.
func clusterUpdate(ctx context.Context, env Env, cmd *cli.Command) error {
	// A signal ends the download or the build, the preflight or the step
	// that runs, which goes on on the node, and the update gives the lock
	// back, rather than ending mooctl.
	ctx, stop := interrupt.Context(ctx, env.Exit)
	defer stop()
	run := runClusterUpdate
	if cmd.String("source") != "" {
		run = runSourceUpdate
	}
	err := run(ctx, env, cmd)
	if err != nil && ctx.Err() != nil {
		var stopped *stoppedError
		if errors.As(err, &stopped) {
			err = stopped.err
		}
		return &interruptedError{again: rerun(cmd), err: err}
	}
	return err
}

// rerun is the command line that runs cluster update as cmd ran it: in
// its cluster directory, from its release source, with --force and its
// --source and the components it names, if it has them, each quoted for
// a shell, so that it can be pasted. A release source with a user or a
// password in it is left out, so as not to show them.
func rerun(cmd *cli.Command) string {
	args := append(mooctlIn(cmd), "cluster", "update")
	if cmd.Bool("force") {
		args = append(args, "--force")
	}
	if source := cmd.String("source"); source != "" {
		args = append(append(args, "--source", source), cmd.Args().Slice()...)
	}
	for i, arg := range args {
		args[i] = shellQuote(arg)
	}
	return strings.Join(args, " ")
}

// mooctlIn is mooctl, run in cmd's cluster directory, from its release
// source, which it leaves out if it holds a user or a password, so as not
// to show them.
func mooctlIn(cmd *cli.Command) []string {
	args := []string{"mooctl"}
	if dir := cmd.String("dir"); dir != "." {
		args = append(args, "-C", dir)
	}
	if source := cmd.String("release-source"); source != "" {
		if u, err := url.Parse(source); err != nil || u.User == nil {
			args = append(args, "--release-source", source)
		}
	}
	return args
}

// shellQuote is arg as a shell, POSIX or zsh, reads it back: as it is,
// if it holds only characters no shell treats specially, else in single
// quotes.
func shellQuote(arg string) string {
	if arg != "" && strings.Trim(arg, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_./:@%+,-") == "" {
		return arg
	}
	return "'" + strings.ReplaceAll(arg, "'", `'\''`) + "'"
}

// stoppedError says cluster update stopped at a step that failed. again
// is the command line that runs it again.
type stoppedError struct {
	again string
	err   error
}

func (e *stoppedError) Error() string { return "cluster update stopped: " + e.err.Error() }

func (e *stoppedError) Unwrap() error { return e.err }

func (e *stoppedError) Hint() string {
	return "Once that is fixed, run " + e.again + " again: it goes on from there."
}

// interruptedError says a signal stopped cluster update. again is the
// command line that runs it again.
type interruptedError struct {
	again string
	err   error
}

func (e *interruptedError) Error() string { return "cluster update interrupted: " + e.err.Error() }

func (e *interruptedError) Unwrap() error { return e.err }

func (e *interruptedError) Hint() string {
	return "Run " + e.again + " again: it goes on from there."
}

// builtFor is what an update installs on each node: what it built or read
// for the node's architecture, by the architecture of each node, arch, as
// the cluster directory listed them when it did. A node the preflight
// reached since, as one added meanwhile, has nothing, and the update
// stops before any step.
func builtFor(arch map[string]string, builds map[string]orchestrate.NodeRelease) orchestrate.ReleaseOf {
	return func(node string) (orchestrate.NodeRelease, error) {
		a, ok := arch[node]
		if !ok {
			return orchestrate.NodeRelease{}, errors.New("it joined the cluster directory after cluster update read it; run cluster update again")
		}
		return builds[a], nil
	}
}

// tellStep prints each step of an update as it starts or is skipped, and
// sets changed once a step isn't skipped.
func tellStep(env Env, changed *bool) func(orchestrate.Step) {
	return func(s orchestrate.Step) {
		if s.Already {
			fmt.Fprintf(env.Stdout, "%s runs %s at %s already\n", s.Node, s.Component, s.Version)
			return
		}
		*changed = true
		fmt.Fprintf(env.Stdout, "Updating %s on %s to %s\n", s.Component, s.Node, s.Version)
	}
}

// runClusterUpdate is cluster update, apart from catching the signals.
func runClusterUpdate(ctx context.Context, env Env, cmd *cli.Command) error {
	d, err := cluster.Open(cmd.String("dir"))
	if err != nil {
		return err
	}
	if err := installable(env, d.Cluster.Release); err != nil {
		return err
	}
	rel, err := openRelease(ctx, env, cmd, d)
	if err != nil {
		return err
	}
	pinned := map[string]release.Component{}
	for _, name := range orchestrate.NodeComponents {
		pc, ok := rel.Component(name)
		if !ok {
			return fmt.Errorf("release %s pins no %s", rel.Version, name)
		}
		// The agent and the updater are a binary each, named for them,
		// and containerd is its binaries, each of them.
		want := []string{name}
		if name == "containerd" {
			want = release.ContainerdFiles
		}
		if !slices.Equal(slices.Sorted(slices.Values(pc.Files)), slices.Sorted(slices.Values(want))) {
			return fmt.Errorf("release %s pins %s as %s, not %s", rel.Version, orchestrate.ComponentNoun(name), strings.Join(pc.Files, ", "), strings.Join(want, ", "))
		}
		pinned[name] = pc
	}
	nodes, err := d.Nodes()
	if err != nil {
		return err
	}
	arch := map[string]string{}
	releases := map[string]orchestrate.NodeRelease{}
	for _, n := range nodes {
		arch[n.Name] = n.Architecture
		if _, ok := releases[n.Architecture]; ok {
			continue
		}
		if releases[n.Architecture], err = nodeRelease(rel, pinned, n.Architecture); err != nil {
			return err
		}
	}
	c, err := preflight(ctx, env, cmd, d, "", func(s nodeclient.Status, all []nodeclient.Status) error {
		return updates(env, rel.Manifest, s, all)
	})
	if err != nil {
		return err
	}
	defer c.Close()
	changed := false
	err = c.Update(c.Context(), builtFor(arch, releases), tellStep(env, &changed))
	if err != nil {
		return &stoppedError{again: rerun(cmd), err: err}
	}
	if !changed {
		fmt.Fprintf(env.Stdout, "Every node runs release %s already; nothing changed.\n", rel.Version)
		return nil
	}
	fmt.Fprintf(env.Stdout, "Every node runs release %s.\n", rel.Version)
	return nil
}

// nodeRelease is what an update installs on a node of arch from rel: the
// pinned node components, by name, and rel's cluster services, with the
// builds node status says them by, each file read and checked against
// rel's checksums.
func nodeRelease(rel *release.Release, pinned map[string]release.Component, arch string) (orchestrate.NodeRelease, error) {
	r := orchestrate.NodeRelease{Components: map[string]orchestrate.Component{}}
	for component, pc := range pinned {
		files := map[string][]byte{}
		for _, f := range pc.Files {
			data, err := rel.ReadFile(arch, f)
			if err != nil {
				return r, err
			}
			files[f] = data
		}
		build, err := rel.Build(arch, pc.Files)
		if err != nil {
			return r, err
		}
		r.Components[component] = orchestrate.Component{Version: pc.Version, Build: build, Files: files}
	}
	for _, s := range rel.Services {
		archive, err := rel.ReadFile(arch, s.Image)
		if err != nil {
			return r, err
		}
		svc, err := serviceOf(archive, s)
		if err != nil {
			return r, fmt.Errorf("release %s: %s: %w", rel.Version, s.Image, err)
		}
		r.Services = append(r.Services, svc)
	}
	return r, nil
}

// serviceOf is the cluster service s as an update runs it, from its
// image's archive, with the build node status says it by: the image's
// digest.
func serviceOf(archive []byte, s release.Service) (orchestrate.Service, error) {
	build, err := ociimage.Digest(archive)
	if err != nil {
		return orchestrate.Service{}, err
	}
	return orchestrate.Service{Name: s.Name, Version: s.Version, Build: build, API: s.API, Image: release.ImageRef(s.Name, s.Version), Archive: archive}, nil
}

// clusterUnlock is mooctl cluster unlock. It breaks the operation lock on
// every node it reaches, says whose lock it broke, warns until when a
// mooctl still running can go on, and fails naming the nodes it couldn't
// reach, whose lock expires on its own.
func clusterUnlock(ctx context.Context, env Env, cmd *cli.Command) error {
	d, err := cluster.Open(cmd.String("dir"))
	if err != nil {
		return err
	}
	nodes, err := d.Nodes()
	if err != nil {
		return err
	}
	var (
		broken   []orchestrate.Reached
		problems []string
	)
	for _, u := range orchestrate.Unlock(ctx, nodesToDial(env, nodes)) {
		switch {
		case u.Err != nil:
			problems = append(problems, fmt.Sprintf("  %s: %v", u.Name, u.Err))
		case u.Broken != nil:
			broken = append(broken, orchestrate.Reached{Name: u.Name, Status: nodeclient.Status{Lock: u.Broken}})
		}
	}
	locks := locksOf(broken)
	for _, l := range locks {
		fmt.Fprintf(env.Stdout, "Broke the operation lock held by %s, on %s.\n", orchestrate.DescribeLock(l.lock), strings.Join(l.Nodes, ", "))
	}
	if len(locks) > 0 {
		var until time.Time
		for _, b := range broken {
			if u := b.Status.Lock.Until; u.After(until) {
				until = u
			}
		}
		fmt.Fprintf(env.Stderr, "If that mooctl is still running, it can go on until %s at the latest, when its lease would have run out, and the next operation can overlap with it until then.\n", orchestrate.LocalTime(until))
	}
	if len(locks) == 0 && len(problems) == 0 {
		fmt.Fprintln(env.Stdout, "No node held the operation lock.")
	}
	if len(problems) > 0 {
		return fmt.Errorf("couldn't break the operation lock on these nodes, where it expires on its own if it is held:\n%s", strings.Join(problems, "\n"))
	}
	return nil
}

// nodeStatusView is a node in cluster status. An unreachable node has
// only its name, reachable and the error.
type nodeStatusView struct {
	Name      string `json:"name"`
	Reachable bool   `json:"reachable"`
	Error     string `json:"error,omitempty"`
	// Mode is empty when the node didn't say.
	Mode string `json:"mode,omitempty"`
	// Generation is nil when the node didn't say.
	Generation *int64 `json:"generation,omitempty"`
	// GenerationProblem says how the node's generation differs from the
	// cluster directory's, or why the node can't tell it.
	GenerationProblem string                `json:"generation_problem,omitempty"`
	Components        []componentStatusView `json:"components,omitempty"`
	// Services are the cluster services, as Components are the node
	// components.
	Services []componentStatusView `json:"services,omitempty"`
}

// componentStatusView is a component or a cluster service in cluster
// status: one for each that the release pins, in its order, then one for
// each it doesn't. Running is empty if the node didn't report a version, and
// Pinned if the release doesn't pin the component. A pinned component
// the node didn't report is of unknown health.
type componentStatusView struct {
	Name    string `json:"name"`
	Running string `json:"running,omitempty"`
	Pinned  string `json:"pinned,omitempty"`
	// Build is the build that runs, and PinnedBuild the build the
	// release pins for the node's architecture, each empty if unknown.
	Build       string `json:"build,omitempty"`
	PinnedBuild string `json:"pinned_build,omitempty"`
	// Development says Running is a development build's version, which no
	// release has.
	Development bool   `json:"development,omitempty"`
	Health      string `json:"health"`
	Problem     string `json:"problem,omitempty"`
}

// statusOf is the status of the cluster of d, pinned at rel, whose nodes
// are as reached. rel is nil if mooctl couldn't open it: the nodes show
// what they run, with nothing pinned, and the update is unknown.
func statusOf(d *cluster.Dir, rel *release.Release, nodes []cluster.Node, reached []orchestrate.Reached) statusView {
	view := statusView{Cluster: d.Cluster.Name, Release: d.Cluster.Release, Generation: d.Cluster.Generation, Lock: locksOf(reached), Nodes: []nodeStatusView{}}
	var pinnedComponents, pinnedServices []release.Pin
	if rel != nil {
		pinnedComponents, pinnedServices = rel.PinnedComponents(), rel.PinnedServices()
	}
	for _, r := range reached {
		n := nodeStatusView{Name: r.Name, Reachable: r.Err == nil}
		if r.Err != nil {
			n.Error = r.Err.Error()
			view.Nodes = append(view.Nodes, n)
			continue
		}
		if r.Status.AgentErr == nil {
			view.answered++
		}
		if r.Status.Mode != nodev1.Mode_MODE_UNSPECIFIED {
			n.Mode = nodeclient.ModeName(r.Status.Mode)
		}
		if applied, ok := r.KnownGeneration(); ok {
			n.Generation = &applied
		}
		if err := r.GenerationProblem(d.Cluster.Generation); err != nil {
			n.GenerationProblem = err.Error()
		}
		builds := pinnedBuilds(rel, archOf(nodes, r.Name))
		n.Components = componentsOf(pinnedComponents, builds, r.Status.Components)
		n.Services = componentsOf(pinnedServices, builds, r.Status.Services)
		view.Nodes = append(view.Nodes, n)
	}
	view.Update = "unknown"
	if rel != nil {
		view.Update = updateOf(view)
	}
	return view
}

// refusal is why cluster update would refuse to update the nodes it
// reached, whose statuses are reached, to pinned, as its preflight checks
// them, or nil if it wouldn't. A node whose agent doesn't answer is one
// the preflight can't reach, which status shows already.
func refusal(env Env, pinned release.Manifest, reached []orchestrate.Reached) error {
	if err := installable(env, pinned.Version); err != nil {
		return err
	}
	var all []nodeclient.Status
	for _, r := range reached {
		if r.Err == nil {
			all = append(all, r.Status)
		}
	}
	var errs []error
	for _, r := range reached {
		if r.Err != nil || r.Status.AgentErr != nil {
			continue
		}
		err := operated(env, r.Status)
		if err == nil {
			err = updates(env, pinned, r.Status, all)
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", r.Name, err))
		}
	}
	return errors.Join(errs...)
}

// updateOperation is cluster update's operation, as preflight records it
// in the operation lock: the command's name.
const updateOperation = "cluster update"

// updateLines say what each state updateOf tells means for the operator.
var updateLines = map[string]string{
	"in_progress":        "in progress",
	"up_to_date":         "none needed",
	"stopped_partway":    "stopped partway; run mooctl cluster update again to finish it",
	"not_started":        "not started; run mooctl cluster update",
	"development_builds": "development builds run; mooctl cluster update replaces them with the pinned release",
	"refused":            "refused, so cluster update won't run:",
	"unknown":            "unknown, as mooctl can't open the pinned release",
}

// updateOf says whether the nodes that view reached run its pinned
// release: "in_progress" while cluster update holds the operation lock,
// "up_to_date" if each runs every pinned component and service at the
// pinned version, healthy, as cluster update would leave it, runs no
// development build, and status reached each, and otherwise
// "development_builds" if any node runs a development build, of what the
// release pins or not, "stopped_partway" if any node's agent, which an
// update installs first, runs the pinned release, and "not_started" if
// none does.
func updateOf(view statusView) string {
	for _, l := range view.Lock {
		if l.lock.Operation == updateOperation {
			return "in_progress"
		}
	}
	upToDate, started, development := true, false, false
	for _, n := range view.Nodes {
		// What a node runs that status couldn't reach is unknown.
		if !n.Reachable {
			upToDate = false
		}
		for _, c := range append(slices.Clone(n.Components), n.Services...) {
			if c.Pinned != "" && (c.Running != c.Pinned || otherBuild(c) || c.Health != nodeclient.HealthName(nodev1.Health_HEALTH_HEALTHY)) {
				upToDate = false
			}
			// A development build of what the release doesn't pin runs on
			// too.
			if c.Development {
				upToDate = false
			}
			if c.Name == "agent" && c.Pinned != "" && c.Running == c.Pinned {
				started = true
			}
			development = development || c.Development
		}
	}
	switch {
	case upToDate:
		return "up_to_date"
	case development:
		return "development_builds"
	case started:
		return "stopped_partway"
	}
	return "not_started"
}

// dash is s, or "-" if it is empty.
func dash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// componentsOf are the components or cluster services that a node
// reported, pinned as pinned.
func componentsOf(pinned []release.Pin, builds map[string]string, reported []*nodev1.Component) []componentStatusView {
	byName := map[string]*nodev1.Component{}
	for _, c := range reported {
		byName[c.Name] = c
	}
	var views []componentStatusView
	for _, p := range pinned {
		c, ok := byName[p.Name]
		if !ok {
			views = append(views, componentStatusView{Name: p.Name, Pinned: p.Version, PinnedBuild: builds[p.Name], Health: nodeclient.HealthName(nodev1.Health_HEALTH_UNSPECIFIED)})
			continue
		}
		views = append(views, reportedComponent(c, p.Version, builds[p.Name]))
	}
	for _, c := range reported {
		if !slices.ContainsFunc(pinned, func(p release.Pin) bool { return p.Name == c.Name }) {
			views = append(views, reportedComponent(c, "", ""))
		}
	}
	return views
}

// reportedComponent is the reported component c, pinned at pinned.
func reportedComponent(c *nodev1.Component, pinned, pinnedBuild string) componentStatusView {
	v := componentStatusView{Name: c.Name, Running: c.Version, Pinned: pinned, Build: c.Build, PinnedBuild: pinnedBuild, Development: release.IsDevVersion(c.Version), Health: nodeclient.HealthName(c.Health), Problem: c.Problem}
	if v.Problem == "" && v.Running == pinned && otherBuild(v) {
		v.Problem = fmt.Sprintf("it runs another build of %s than the release pins", pinned)
	}
	if v.Problem == "" && v.Development && pinned == "" {
		v.Problem = "it is a development build of a service the pinned release doesn't have, which cluster update leaves running"
	}
	return v
}

// otherBuild reports whether c runs another build than the one pinned,
// when both are known: what cluster update replaces.
func otherBuild(c componentStatusView) bool {
	return c.Build != "" && c.PinnedBuild != "" && c.Build != c.PinnedBuild
}

// archOf is the architecture of the node called name, empty if nodes
// don't have it.
func archOf(nodes []cluster.Node, name string) string {
	for _, n := range nodes {
		if n.Name == name {
			return n.Architecture
		}
	}
	return ""
}

// pinnedBuilds are the builds rel pins for arch, by component and
// service name, none if rel is nil; a build mooctl can't tell is left
// out.
func pinnedBuilds(rel *release.Release, arch string) map[string]string {
	builds := map[string]string{}
	if rel == nil {
		return builds
	}
	for _, c := range rel.Components {
		if b, err := rel.Build(arch, c.Files); err == nil {
			builds[c.Name] = b
		}
	}
	for _, s := range rel.Services {
		if b, err := rel.ServiceBuild(arch, s); err == nil {
			builds[s.Name] = b
		}
	}
	return builds
}
