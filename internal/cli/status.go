package cli

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"text/tabwriter"

	"github.com/urfave/cli/v3"

	"github.com/moostackhq/moostack/internal/clusterdir"
	"github.com/moostackhq/moostack/internal/gitrepo"
	"github.com/moostackhq/moostack/internal/model"
)

// clusterStatus is mooctl cluster status.
func clusterStatus(ctx context.Context, env Env, cmd *cli.Command) error {
	d, err := openCluster(env, cmd)
	if err != nil {
		return err
	}
	nodes, err := d.Nodes()
	if err != nil {
		return err
	}
	fmt.Fprintf(env.Stdout, "Cluster %s, k3s %s\n", d.Cluster.Ref, d.Cluster.K3s)
	cp, ok, err := d.ControlPlane()
	if err != nil {
		return err
	}
	if !ok {
		if len(nodes) > 0 {
			w := tabwriter.NewWriter(env.Stdout, 0, 0, 2, ' ', 0)
			printDirNodes(env, w, nodes)
			return errors.New("the cluster directory has nodes but no control plane")
		}
		fmt.Fprintln(env.Stdout, "\nNo nodes.")
		return nil
	}
	w := tabwriter.NewWriter(env.Stdout, 0, 0, 2, ' ', 0)
	cpClient, err := dialNode(ctx, env, cp)
	if err != nil {
		printDirNodes(env, w, nodes)
		return fmt.Errorf("the control plane can't be reached, so this is the cluster directory alone: %w", err)
	}
	defer cpClient.Close()

	orch := orchestratorThrough(env, cpClient)
	onCluster, err := orch.Nodes(ctx)
	if err != nil {
		printDirNodes(env, w, nodes)
		return err
	}
	apps, err := orch.Apps(ctx)
	if err != nil {
		return err
	}
	var drift []string
	if applied, err := (remoteHost{cpClient}).appliedCommit(ctx); err != nil {
		drift = append(drift, err.Error())
	} else if applied != "" && !commitID.MatchString(applied) {
		drift = append(drift, fmt.Sprintf("the control plane's %s doesn't hold a commit: %q; operations are refused until it does", AppliedCommitFile, applied))
	} else if applied != "" {
		if repo, err := gitrepo.Open(ctx, d.Path); err == nil {
			if ok, err := repo.IsAncestor(ctx, applied); err == nil && !ok {
				drift = append(drift, fmt.Sprintf("the cluster applied commit %.12s, which the cluster directory doesn't have: fetch it", applied))
			}
		}
	}

	fmt.Fprintln(env.Stdout)
	fmt.Fprintln(w, "NODE\tROLE\tREADY\tK3S\tARCH\tMEMORY\tPRIVATE ADDRESS")
	for _, n := range nodes {
		role := roleName(n.ControlPlane)
		i := slices.IndexFunc(onCluster, func(seen model.NodeState) bool { return seen.Name == n.Name })
		if i < 0 {
			fmt.Fprintf(w, "%s\t%s\t%s\t\t%s\t%s\t%s\n", n.Name, role, "not on the cluster", n.Architecture, mib(n.MemoryMiB), n.PrivateAddress)
			drift = append(drift, n.Name+" is in the cluster directory but not on the cluster")
			continue
		}
		seen := onCluster[i]
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n", n.Name, role, seen.Ready, seen.Version, seen.Architecture, capacity(seen), n.PrivateAddress)
		if seen.Version != d.Cluster.K3s {
			drift = append(drift, fmt.Sprintf("%s runs k3s %s, the cluster directory pins %s", n.Name, seen.Version, d.Cluster.K3s))
		}
		if n.ControlPlane != seen.ControlPlane {
			drift = append(drift, n.Name+" is a control plane on one side and not on the other")
		}
		if seen.Address != n.PrivateAddress.String() {
			drift = append(drift, fmt.Sprintf("%s is at %s on the cluster, and at %s in the cluster directory", n.Name, seen.Address, n.PrivateAddress))
		}
		if seen.Architecture != n.Architecture {
			drift = append(drift, fmt.Sprintf("%s is %s on the cluster, and %s in the cluster directory", n.Name, seen.Architecture, n.Architecture))
		}
	}
	for _, seen := range onCluster {
		if !slices.ContainsFunc(nodes, func(n clusterdir.Node) bool { return n.Name == seen.Name }) {
			fmt.Fprintf(w, "%s\t\t%s\t%s\t%s\t%s\t%s\n", seen.Name, seen.Ready, seen.Version, seen.Architecture, capacity(seen), seen.Address)
			drift = append(drift, seen.Name+" is on the cluster but not in the cluster directory; remove it with mooctl cluster node-remove "+seen.Name)
		}
	}
	w.Flush()

	fmt.Fprintln(env.Stdout)
	if len(apps) == 0 {
		fmt.Fprintln(env.Stdout, "No apps.")
	} else {
		fmt.Fprintln(w, "APP\tREADY\tIMAGE\tHOSTS")
		for _, app := range apps {
			var images []string
			for _, image := range app.Images {
				if image = shortImage(image); !slices.Contains(images, image) {
					images = append(images, image)
				}
			}
			fmt.Fprintf(w, "%s\t%d/%d\t%s\t%s\n", app.Name, app.Ready, app.Want, strings.Join(images, ", "), strings.Join(app.Hosts, ", "))
		}
		w.Flush()
	}

	if len(drift) > 0 {
		fmt.Fprintln(env.Stdout, "\nDrift:")
		for _, s := range drift {
			fmt.Fprintln(env.Stdout, "- "+s)
		}
	}
	return nil
}

// shortDigest is how many characters of an image's digest status shows.
const shortDigest = 12

// shortImage is image with its digest, if it has one, cut to
// shortDigest characters.
func shortImage(image string) string {
	name, digest, ok := strings.Cut(image, "@")
	if !ok {
		return image
	}
	algorithm, hex, ok := strings.Cut(digest, ":")
	if !ok || len(hex) <= shortDigest {
		return image
	}
	return name + "@" + algorithm + ":" + hex[:shortDigest]
}

// printDirNodes prints the nodes of the cluster directory alone.
func printDirNodes(env Env, w *tabwriter.Writer, nodes []clusterdir.Node) {
	fmt.Fprintln(env.Stdout)
	fmt.Fprintln(w, "NODE\tROLE\tARCH\tMEMORY\tPRIVATE ADDRESS")
	for _, n := range nodes {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", n.Name, roleName(n.ControlPlane), n.Architecture, mib(n.MemoryMiB), n.PrivateAddress)
	}
	w.Flush()
}

// capacity is the memory the cluster says a node has, or nothing for a
// node it has none for.
func capacity(n model.NodeState) string {
	if n.MemoryMiB == 0 {
		return ""
	}
	return mib(n.MemoryMiB)
}

func mib(n int) string { return fmt.Sprintf("%d MiB", n) }
