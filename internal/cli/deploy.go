package cli

import (
	"maps"

	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"syscall"

	"golang.org/x/term"

	"github.com/urfave/cli/v3"

	"github.com/moostackhq/moostack/internal/buildx"
	"github.com/moostackhq/moostack/internal/clusterdir"
	"github.com/moostackhq/moostack/internal/deployfile"
	"github.com/moostackhq/moostack/internal/interrupt"
	"github.com/moostackhq/moostack/internal/model"
	"github.com/moostackhq/moostack/internal/orchestrator"
	"github.com/moostackhq/moostack/internal/registry"
	"github.com/moostackhq/moostack/internal/secretstore"
)

func deployCommand(env Env) *cli.Command {
	return &cli.Command{
		Name:      "deploy",
		Usage:     "build the app in the current directory, as its deploy.yaml says, and deploy it",
		ArgsUsage: "[app, with --remove]",
		Flags: []cli.Flag{
			&cli.BoolFlag{Name: "remove", Usage: "remove the app, the current directory's or the one named, from the cluster instead"},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error { return deploy(ctx, env, cmd) },
	}
}

func deploy(ctx context.Context, env Env, cmd *cli.Command) error {
	if cmd.Bool("remove") {
		return removeApp(ctx, env, cmd)
	}
	if err := wantArgs(cmd, 0); err != nil {
		return err
	}
	app, err := deployfile.Read(".")
	if errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("there is no %s here; run mooctl deploy in the app's directory", deployfile.FileName)
	}
	if err != nil {
		return err
	}
	if err := orchestratorThrough(env, nil).Check(unbuilt(app)); err != nil {
		return err
	}
	ctx, stop := interrupt.Context(ctx, env.Exit)
	defer stop()
	d, err := openCluster(env, cmd)
	if err != nil {
		return err
	}
	if err := updateFromRemote(ctx, d.Path); err != nil {
		return err
	}
	if d, err = clusterdir.Open(d.Path); err != nil {
		return err
	}
	cp, ok, err := d.ControlPlane()
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("cluster %s has no nodes yet; add one with mooctl cluster node-add", d.Cluster.Ref)
	}
	say(env, "Deploying %s to %s", app.Name, d.Cluster.Ref)
	secrets, err := readSecrets(env, cmd, d, app.Secrets)
	if err != nil {
		return err
	}
	cpClient, err := dialNode(ctx, env, cp)
	if err != nil {
		return err
	}
	defer cpClient.Close()
	ctx, release, err := hold(ctx, cpClient)
	if err != nil {
		return err
	}
	defer release()
	orch := orchestratorThrough(env, cpClient)

	images, err := buildAll(ctx, env, d, app, cp, cpClient, orch)
	if err != nil {
		return err
	}
	err = orch.Deploy(ctx, toRun(app, images, secrets), func(step orchestrator.Step) {
		switch step {
		case orchestrator.WaitingForCluster:
			say(env, "Waiting for cert-manager and Traefik...")
		case orchestrator.Applying:
			say(env, "Applying %s...", app.Name)
		case orchestrator.WaitingForReady:
			say(env, "Waiting for %s to be ready...", app.Name)
		}
	})
	if err != nil {
		return err
	}
	if err := keepRunning(ctx, cp, cpClient, images); err != nil {
		return err
	}
	fmt.Fprintf(env.Stdout, "Deployed %s to %s\n", app.Name, d.Cluster.Ref)
	var printed []string
	for _, c := range app.Containers {
		if ref, ok := images[c.Name]; ok && !slices.Contains(printed, ref) {
			fmt.Fprintf(env.Stdout, "Image: %s\n", ref)
			printed = append(printed, ref)
		}
	}
	for _, c := range app.Containers {
		if c.Host == "" {
			continue
		}
		if err := orch.Certificate(ctx, app.Name, c.Name); err != nil {
			if ctx.Err() != nil {
				return context.Cause(ctx)
			}
			fmt.Fprintf(env.Stderr, "Warning: %s has no certificate yet: %v\n", c.Host, err)
		}
		fmt.Fprintf(env.Stdout, "URL: https://%s\n", c.Host)
	}
	return nil
}

// toRun is app as an orchestrator runs it: with images, those built for
// its containers, by container name, and secrets, the values of its
// secrets by name. The containers of an app with manifests or a chart
// are its images.
func toRun(app *deployfile.App, images, secrets map[string]string) model.App {
	out := model.App{Name: app.Name, Manifests: app.Manifests, Chart: app.Chart}
	if len(app.Secrets) > 0 {
		out.Secrets = map[string]string{}
		for _, name := range app.Secrets {
			out.Secrets[name] = secrets[name]
		}
	}
	if app.Manifests != "" || app.Chart != "" {
		if len(images) > 0 {
			out.Images = images
		}
		return out
	}
	for _, c := range app.Containers {
		run := model.Container{
			Name:     c.Name,
			Image:    cmp.Or(c.Image, images[c.Name]),
			Command:  c.Command,
			Port:     c.Port,
			Host:     c.Host,
			Replicas: c.Replicas,
		}
		if len(c.Env) > 0 {
			run.Env = map[string]model.Value{}
			for variable, v := range c.Env {
				run.Env[variable] = model.Value{Value: v.Value, Secret: v.Secret}
			}
		}
		out.Containers = append(out.Containers, run)
	}
	return out
}

// unbuilt is app as an orchestrator runs it, before anything is built
// or read: each image to build is its repository in the cluster's
// registry, and each secret is empty.
func unbuilt(app *deployfile.App) model.App {
	images := map[string]string{}
	for _, c := range app.Containers {
		if c.Build != nil {
			images[c.Name] = registry.Name + "/" + app.Name + "/" + c.Name
		}
	}
	return toRun(app, images, nil)
}

// buildAll builds and pushes the images of app's containers, which it
// returns by container name. Containers with the same build share one
// image.
func buildAll(ctx context.Context, env Env, d *clusterdir.Dir, app *deployfile.App, cp clusterdir.Node, cpClient remote, orch orchestrator.Orchestrator) (map[string]string, error) {
	type job struct {
		repo, label string
		build       deployfile.Build
	}
	var jobs []job
	for _, c := range app.Containers {
		if c.Build != nil && !slices.ContainsFunc(jobs, func(j job) bool { return j.build == *c.Build }) {
			jobs = append(jobs, job{app.Name + "/" + c.Name, c.Name, *c.Build})
		}
	}
	if len(jobs) == 0 {
		return nil, nil
	}
	arches, err := architectures(ctx, d, orch)
	if err != nil {
		return nil, err
	}
	built := map[deployfile.Build]string{}
	for _, j := range jobs {
		ref, err := buildAndPush(ctx, env, j.repo, j.label, j.build, arches, cp, cpClient)
		if err != nil {
			return nil, err
		}
		built[j.build] = ref
	}
	images := map[string]string{}
	for _, c := range app.Containers {
		if c.Build != nil {
			images[c.Name] = built[*c.Build]
		}
	}
	return images, nil
}

// architectures are the architectures of the cluster's nodes: those
// orch lists, but for a node that reports none, and those of d's nodes.
func architectures(ctx context.Context, d *clusterdir.Dir, orch orchestrator.Orchestrator) ([]string, error) {
	onCluster, err := orch.Nodes(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing the nodes' architectures: %w", err)
	}
	var arches []string
	for _, n := range onCluster {
		if n.Architecture != "" {
			arches = append(arches, n.Architecture)
		}
	}
	nodes, err := d.Nodes()
	if err != nil {
		return nil, err
	}
	for _, n := range nodes {
		arches = append(arches, n.Architecture)
	}
	slices.Sort(arches)
	return slices.Compact(arches), nil
}

// readSecrets reads the secrets called names from d's secret store.
func readSecrets(env Env, cmd *cli.Command, d *clusterdir.Dir, names []string) (map[string]string, error) {
	if len(names) == 0 {
		return nil, nil
	}
	t, closeTerminal := openTerminal(env)
	defer closeTerminal()
	store, err := secretstore.Unlock(filepath.Join(d.Path, secretstore.FileName), keyOptions(env, cmd, t))
	if err != nil {
		return nil, err
	}
	values := map[string]string{}
	for _, name := range names {
		s, err := store.Get(name)
		if errors.Is(err, secretstore.ErrNotFound) {
			return nil, fmt.Errorf("the secret store has no secret %s; add it with mooctl secret edit", name)
		}
		if err != nil {
			return nil, fmt.Errorf("secret %s: %w", name, err)
		}
		values[name] = s.Value
	}
	return values, nil
}

// buildDirPrefix starts the name of a build's temporary directory, which
// goes on with the building process's ID.
const buildDirPrefix = "mooctl-build-"

// buildAndPush builds the image of build, called label, for each of
// arches and pushes it to zot's repository repo on the control plane cp,
// and returns its reference by digest, as nodes pull it.
func buildAndPush(ctx context.Context, env Env, repo, label string, build deployfile.Build, arches []string, cp clusterdir.Node, cpClient remote) (string, error) {
	docker := buildx.Docker{Path: env.Docker, Output: env.Stderr}
	daemon, err := docker.Check(ctx)
	if err != nil {
		return "", err
	}
	docker.Builder = daemon.Builder
	sweepBuildDirs()
	tmp, err := os.MkdirTemp("", buildDirPrefix+strconv.Itoa(os.Getpid())+"-")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(tmp)

	buildDir, file := build.Dockerfile, ""
	var args map[string]string
	if build.Railpack != "" {
		buildDir = build.Railpack
		if file, args, err = railpackPlan(ctx, env, buildDir, tmp); err != nil {
			return "", err
		}
	}
	tag := registry.Name + "/" + repo + ":build"
	layouts := map[string]string{}
	for _, arch := range arches {
		say(env, "Building %s for linux/%s...", label, arch)
		dest := filepath.Join(tmp, arch)
		if err := docker.BuildFile(ctx, buildDir, file, args, tag, arch, dest); err != nil {
			return "", buildError(label, arch, daemon.Arch, err)
		}
		layouts[arch] = dest
	}
	progress, pushed := pushing(env, label)
	digest, err := registry.Push(ctx, layouts, registryAddr(cp), repo, cpClient.DialTCP, progress)
	pushed()
	if err != nil {
		return "", err
	}
	return registry.Image(repo, digest), nil
}

// pushing says that the image called label is being pushed. Where
// env.Stderr is a terminal, the progress it returns redraws that line
// with a bar of the push so far, once the push is a megabyte or more,
// and pushed ends the line; elsewhere progress is nil.
func pushing(env Env, label string) (progress func(complete, total int64), pushed func()) {
	f, ok := env.Stderr.(*os.File)
	if !ok || !term.IsTerminal(int(f.Fd())) {
		say(env, "Pushing %s...", label)
		return nil, func() {}
	}
	line := "Pushing " + label + "..."
	fmt.Fprint(f, line)
	return func(complete, total int64) {
		if total < megabyte {
			return
		}
		next := "Pushing " + label + "... " + pushBar(complete, total)
		if next == line {
			return
		}
		// Spaces cover what is left of a longer line.
		fmt.Fprintf(f, "\r%-*s", len(line), next)
		line = next
	}, func() { fmt.Fprintln(f) }
}

// megabyte is the unit a push's progress is told in.
const megabyte = 1000 * 1000

// pushBar is a bar of complete out of total bytes, 20 wide, with both
// in megabytes.
func pushBar(complete, total int64) string {
	const width = 20
	complete = max(0, min(complete, total))
	filled := 0
	if total > 0 {
		filled = int(complete * width / total)
	}
	return fmt.Sprintf("[%s%s] %d/%d MB", strings.Repeat("#", filled), strings.Repeat("-", width-filled), (complete+megabyte/2)/megabyte, (total+megabyte/2)/megabyte)
}

// keepRunning marks the images built for a deploy that is ready,
// images, as the ones that run, in zot on the control plane cp.
func keepRunning(ctx context.Context, cp clusterdir.Node, cpClient remote, images map[string]string) error {
	var marked []string
	for _, name := range slices.Sorted(maps.Keys(images)) {
		ref := images[name]
		repo, digest, ok := strings.Cut(strings.TrimPrefix(ref, registry.Name+"/"), "@sha256:")
		if !ok || slices.Contains(marked, ref) {
			continue
		}
		if err := registry.Tag(ctx, registryAddr(cp), repo, digest, registry.LiveTag, cpClient.DialTCP); err != nil {
			return err
		}
		marked = append(marked, ref)
	}
	return nil
}

// hold takes the cluster's operation lock on the control plane cp. The
// context it returns ends if the lock is lost, and release gives the
// lock back.
func hold(ctx context.Context, cp remote) (_ context.Context, release func(), err error) {
	lock, err := cp.Lock(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("taking the operation lock: %w", err)
	}
	held, cancel := context.WithCancelCause(ctx)
	go func() {
		select {
		case <-lock.Done():
			cancel(errLockLost)
		case <-held.Done():
		}
	}()
	return held, func() {
		cancel(context.Canceled)
		lock.Close()
	}, nil
}

// registryAddr is zot's address on the control plane cp.
func registryAddr(cp clusterdir.Node) string {
	return netip.AddrPortFrom(cp.PrivateAddress, registry.Port).String()
}

// buildDirName matches the name of a build's temporary directory, with
// the building process's ID.
var buildDirName = regexp.MustCompile(`^` + buildDirPrefix + `([0-9]+)-`)

// sweepBuildDirs removes the temporary directories of builds whose
// process has ended.
func sweepBuildDirs() {
	entries, _ := os.ReadDir(os.TempDir())
	for _, e := range entries {
		m := buildDirName.FindStringSubmatch(e.Name())
		if m == nil {
			continue
		}
		pid, err := strconv.Atoi(m[1])
		if err != nil {
			continue
		}
		if p, err := os.FindProcess(pid); err == nil && p.Signal(syscall.Signal(0)) == nil {
			continue
		}
		os.RemoveAll(filepath.Join(os.TempDir(), e.Name()))
	}
}

// buildError is err from building app for arch, on a Docker whose machine
// is dockerArch, with what to do about building for another architecture.
func buildError(app, arch, dockerArch string, err error) error {
	var be *buildx.BuildError
	if errors.As(err, &be) && be.ExecFormat && arch != dockerArch {
		return fmt.Errorf("building %s for linux/%s: %w\nThis machine is linux/%s: install QEMU's binfmt handlers for linux/%s (docker run --privileged --rm tonistiigi/binfmt --install %s); Docker Desktop has them already", app, arch, err, dockerArch, arch, arch)
	}
	return fmt.Errorf("building %s for linux/%s: %w", app, arch, err)
}

// railpackFrontend is the image of Railpack's BuildKit frontend, and
// railpackFrontendDigests its digests by Railpack version.
const railpackFrontend = "ghcr.io/railwayapp/railpack-frontend"

var railpackFrontendDigests = map[string]string{
	"0.43.0": "sha256:355ef61fae6f2551103cd8b10c710cb07167dd353d561f15c69b8d7ac1c676cc",
}

// railpackVersion is a version as railpack --version prints it.
var railpackVersion = regexp.MustCompile(`v?([0-9]+\.[0-9]+\.[0-9]+)`)

// railpackPlan has Railpack plan the build of the source in dir, into
// tmp, and returns the plan's path and the build arguments that build it
// with the frontend of the same version, by digest where mooctl knows it.
func railpackPlan(ctx context.Context, env Env, dir, tmp string) (string, map[string]string, error) {
	railpack := cmp.Or(env.Railpack, "railpack")
	if _, err := exec.LookPath(railpack); err != nil {
		return "", nil, fmt.Errorf("there is no %s command, which builds apps whose deploy.yaml says railpack; install it from https://railpack.com", railpack)
	}
	out, err := exec.CommandContext(ctx, railpack, "--version").Output()
	if err != nil {
		return "", nil, fmt.Errorf("%s --version: %w", railpack, err)
	}
	m := railpackVersion.FindStringSubmatch(string(out))
	if m == nil {
		return "", nil, fmt.Errorf("%s --version printed no version: %q", railpack, strings.TrimSpace(string(out)))
	}
	digest, ok := railpackFrontendDigests[m[1]]
	if !ok {
		return "", nil, fmt.Errorf("railpack is %s; mooctl builds with Railpack %s", m[1], strings.Join(slices.Sorted(maps.Keys(railpackFrontendDigests)), ", "))
	}
	frontend := railpackFrontend + ":v" + m[1] + "@" + digest
	plan := filepath.Join(tmp, "railpack-plan.json")
	prepare := exec.CommandContext(ctx, railpack, "prepare", dir, "--plan-out", plan, "--info-out", filepath.Join(tmp, "railpack-info.json"))
	var stderr bytes.Buffer
	prepare.Stdout, prepare.Stderr = env.Stderr, &stderr
	if err := prepare.Run(); err != nil {
		return "", nil, fmt.Errorf("railpack prepare: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return plan, map[string]string{"BUILDKIT_SYNTAX": frontend}, nil
}
