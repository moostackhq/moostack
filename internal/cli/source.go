package cli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/urfave/cli/v3"

	"github.com/moostackhq/moostack/internal/buildid"
	"github.com/moostackhq/moostack/internal/orchestrate"
	"github.com/moostackhq/moostack/internal/release"
)

// sourcePrograms are the node components that cluster update builds from
// a source, in the order an update installs them: Go programs of the
// checkout's cmd.
var sourcePrograms = []string{"agent", "updater"}

// sourceComponents are the components cluster update builds from a
// source, joined by conj, as in "agent, updater, registry, dns, proxy and
// telemetry".
func sourceComponents(conj string) string {
	names := slices.Clone(sourcePrograms)
	for _, s := range release.ClusterServices {
		names = append(names, s.Name)
	}
	return joinNouns(names, conj)
}

// namedComponents are the programs and the cluster services that args name,
// each once, in the order an update installs them: every one if args name
// none.
func namedComponents(args []string) (programs []string, services []release.ClusterService, err error) {
	for _, a := range args {
		if !slices.Contains(sourcePrograms, a) && !slices.ContainsFunc(release.ClusterServices, func(s release.ClusterService) bool { return s.Name == a }) {
			return nil, nil, fmt.Errorf("cluster update --source builds %s, not %q", sourceComponents("or"), a)
		}
	}
	named := func(name string) bool { return len(args) == 0 || slices.Contains(args, name) }
	for _, p := range sourcePrograms {
		if named(p) {
			programs = append(programs, p)
		}
	}
	for _, s := range release.ClusterServices {
		if named(s.Name) {
			services = append(services, s)
		}
	}
	return programs, services, nil
}

// runSourceUpdate is cluster update --source, apart from catching the
// signals. It builds the components named, or all, from the Moostack
// checkout for the architecture of each node, before it reaches any node,
// and then updates every node to them as cluster update updates it to a
// release. The builds are development builds, versioned as the checkout's
// commit, which sit outside the release rules. It doesn't change the
// cluster directory, so cluster update without --source puts the pinned
// release back.
func runSourceUpdate(ctx context.Context, env Env, cmd *cli.Command) error {
	programs, services, err := namedComponents(cmd.Args().Slice())
	if err != nil {
		return err
	}
	source := cmd.String("source")
	d, err := openCluster(cmd)
	if err != nil {
		return err
	}
	nodes, err := d.Nodes()
	if err != nil {
		return err
	}
	version, err := release.DevVersion(ctx, source)
	if err != nil {
		return err
	}
	var nouns []string
	for _, p := range programs {
		nouns = append(nouns, orchestrate.ComponentNoun(p))
	}
	for _, s := range services {
		nouns = append(nouns, "the "+s.Name+" service")
	}
	what := joinNouns(nouns, "and")
	fmt.Fprintf(env.Stderr, "Building %s from %s at %s\n", what, source, version)
	arch := map[string]string{}
	builds := map[string]orchestrate.NodeRelease{}
	for _, n := range nodes {
		arch[n.Name] = n.Architecture
		if _, ok := builds[n.Architecture]; ok {
			continue
		}
		if builds[n.Architecture], err = buildFromSource(ctx, source, n.Architecture, version, programs, services); err != nil {
			return err
		}
	}
	c, err := preflight(ctx, env, cmd, d, "", nil)
	if err != nil {
		return err
	}
	defer c.Close()
	err = c.Update(c.Context(), builtFor(arch, builds), tellStep(env, new(bool)))
	if err != nil {
		return &stoppedError{again: rerun(cmd), err: err}
	}
	fmt.Fprintf(env.Stdout, "Every node runs the development build %s of %s. The cluster directory still pins release %s: mooctl cluster update puts it back.\n", version, what, d.Cluster.Release)
	return nil
}

// joinNouns joins nouns as a sentence lists them, with conj: "a",
// "a and b", or "a, b and c".
func joinNouns(nouns []string, conj string) string {
	if len(nouns) == 1 {
		return nouns[0]
	}
	return strings.Join(nouns[:len(nouns)-1], ", ") + " " + conj + " " + nouns[len(nouns)-1]
}

// buildFromSource builds programs and services from the Moostack checkout
// at checkout for arch, at version, as what an update installs on a node
// of arch, with the builds node status says them by. Each names many
// builds, since the same commit built with other settings, such as
// GOAMD64 or cargo's flags, has the same version.
func buildFromSource(ctx context.Context, checkout, arch, version string, programs []string, services []release.ClusterService) (orchestrate.NodeRelease, error) {
	r := orchestrate.NodeRelease{Components: map[string]orchestrate.Component{}}
	dir, err := os.MkdirTemp("", "mooctl-source-")
	if err != nil {
		return r, err
	}
	defer os.RemoveAll(dir)
	for _, p := range programs {
		out := filepath.Join(dir, p)
		if err := release.GoBuild(ctx, checkout, "./cmd/"+p, "linux", arch, out, version, ""); err != nil {
			return r, err
		}
		binary, err := os.ReadFile(out)
		if err != nil {
			return r, err
		}
		sum := sha256.Sum256(binary)
		r.Components[p] = orchestrate.Component{
			Version:    version,
			Build:      buildid.FromSums(map[string]string{p: hex.EncodeToString(sum[:])}),
			Files:      map[string][]byte{p: binary},
			ManyBuilds: true,
		}
	}
	if len(services) == 0 {
		return r, nil
	}
	built, err := release.BuildServices(ctx, checkout, arch, version, dir, services)
	if err != nil {
		return r, err
	}
	for _, s := range built {
		archive, err := os.ReadFile(filepath.Join(dir, s.Image))
		if err != nil {
			return r, err
		}
		svc, err := serviceOf(archive, s)
		if err != nil {
			return r, err
		}
		svc.ManyBuilds = true
		r.Services = append(r.Services, svc)
	}
	return r, nil
}
