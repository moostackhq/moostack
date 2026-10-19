package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"text/tabwriter"

	"github.com/urfave/cli/v3"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/cluster"
	"github.com/moostackhq/moostack/internal/identity"
	"github.com/moostackhq/moostack/internal/nodeclient"
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
	rel, err := openRelease(cmd, d)
	if err != nil {
		return err
	}
	nodes, err := d.Nodes()
	if err != nil {
		return err
	}
	view := statusOf(d, rel, orchestrate.Survey(ctx, nodesToDial(env, nodes)))
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
	fmt.Fprintf(env.Stdout, "Cluster %s on release %s\n\n", view.Cluster, view.Release)
	w := tabwriter.NewWriter(env.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "NODE\tREACHABLE\tMODE\tCOMPONENT\tRUNNING\tPINNED\tHEALTH")
	var problems []string
	for _, n := range view.Nodes {
		if !n.Reachable {
			fmt.Fprintf(w, "%s\tno\t-\t-\t-\t-\t-\n", n.Name)
			problems = append(problems, fmt.Sprintf("%s: %s", n.Name, n.Error))
			continue
		}
		for _, c := range n.Components {
			fmt.Fprintf(w, "%s\tyes\t%s\t%s\t%s\t%s\t%s\n", n.Name, dash(n.Mode), c.Name, dash(c.Running), dash(c.Pinned), c.Health)
			if c.Problem != "" {
				problems = append(problems, fmt.Sprintf("%s: %s: %s", n.Name, c.Name, c.Problem))
			}
		}
		if n.GenerationProblem != "" {
			problems = append(problems, fmt.Sprintf("%s: %s", n.Name, n.GenerationProblem))
		}
	}
	if err := w.Flush(); err != nil {
		return err
	}
	if len(problems) > 0 {
		fmt.Fprintf(env.Stdout, "\n%s\n", strings.Join(problems, "\n"))
	}
	return nil
}

// statusView is what cluster status shows, as a table or as JSON.
type statusView struct {
	Cluster    string           `json:"cluster"`
	Release    string           `json:"release"`
	Generation int64            `json:"generation"`
	Nodes      []nodeStatusView `json:"nodes"`
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
}

// componentStatusView is a component in cluster status: one for each
// component the release pins, in its order, then one for each component
// it doesn't. Running is empty if the node didn't report a version, and
// Pinned if the release doesn't pin the component. A pinned component
// the node didn't report is of unknown health.
type componentStatusView struct {
	Name    string `json:"name"`
	Running string `json:"running,omitempty"`
	Pinned  string `json:"pinned,omitempty"`
	Health  string `json:"health"`
	Problem string `json:"problem,omitempty"`
}

// statusOf is the status of the cluster of d, pinned at rel, whose nodes
// are as reached.
func statusOf(d *cluster.Dir, rel *release.Release, reached []orchestrate.Reached) statusView {
	view := statusView{Cluster: d.Cluster.Name, Release: rel.Version, Generation: d.Cluster.Generation, Nodes: []nodeStatusView{}}
	for _, r := range reached {
		n := nodeStatusView{Name: r.Name, Reachable: r.Err == nil}
		if r.Err != nil {
			n.Error = r.Err.Error()
			view.Nodes = append(view.Nodes, n)
			continue
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
		n.Components = componentsOf(rel, r.Status.Components)
		view.Nodes = append(view.Nodes, n)
	}
	return view
}

// dash is s, or "-" if it is empty.
func dash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// componentsOf are the components of a node that reported components,
// pinned at rel.
func componentsOf(rel *release.Release, components []*nodev1.Component) []componentStatusView {
	reported := map[string]*nodev1.Component{}
	for _, c := range components {
		reported[c.Name] = c
	}
	var views []componentStatusView
	for _, p := range rel.Components {
		c, ok := reported[p.Name]
		if !ok {
			views = append(views, componentStatusView{Name: p.Name, Pinned: p.Version, Health: nodeclient.HealthName(nodev1.Health_HEALTH_UNSPECIFIED)})
			continue
		}
		views = append(views, reportedComponent(c, p.Version))
	}
	for _, c := range components {
		if _, ok := rel.Component(c.Name); !ok {
			views = append(views, reportedComponent(c, ""))
		}
	}
	return views
}

// reportedComponent is the reported component c, pinned at pinned.
func reportedComponent(c *nodev1.Component, pinned string) componentStatusView {
	return componentStatusView{Name: c.Name, Running: c.Version, Pinned: pinned, Health: nodeclient.HealthName(c.Health), Problem: c.Problem}
}
