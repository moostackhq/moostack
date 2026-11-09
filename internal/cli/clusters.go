package cli

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"text/tabwriter"

	"github.com/urfave/cli/v3"

	"github.com/moostackhq/moostack/internal/clusterdir"
)

// clusterList is mooctl cluster list.
func clusterList(env Env, cmd *cli.Command) error {
	if err := wantArgs(cmd, 0); err != nil {
		return err
	}
	r, err := root(env)
	if err != nil {
		return err
	}
	dirs, err := clusterDirs(r)
	if err != nil {
		return err
	}
	if len(dirs) == 0 {
		fmt.Fprintln(env.Stdout, "No clusters.")
		return nil
	}
	def, err := defaultCluster(env)
	if err != nil {
		return err
	}
	w := tabwriter.NewWriter(env.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "CLUSTER\tNODES\tDEFAULT")
	for _, d := range dirs {
		nodes, err := d.Nodes()
		if err != nil {
			return fmt.Errorf("%s: %w", d.Path, err)
		}
		mark := ""
		if d.Cluster.Ref == def {
			mark = "yes"
		}
		fmt.Fprintf(w, "%s\t%d\t%s\n", d.Cluster.Ref, len(nodes), mark)
	}
	return w.Flush()
}

// clusterDirs are the cluster directories under root, at
// <prefix>/<name>, in the order of their paths.
func clusterDirs(root string) ([]*clusterdir.Dir, error) {
	paths, err := filepath.Glob(filepath.Join(root, "*", "*"))
	if err != nil {
		return nil, err
	}
	var dirs []*clusterdir.Dir
	for _, p := range paths {
		if info, err := os.Stat(p); err != nil || !info.IsDir() {
			continue
		}
		d, err := clusterdir.Open(p)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("%s: %w", p, err)
		}
		dirs = append(dirs, d)
	}
	return dirs, nil
}
