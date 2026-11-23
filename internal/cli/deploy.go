package cli

import (
	"io"
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
	"gopkg.in/yaml.v3"

	"github.com/urfave/cli/v3"

	"github.com/moostackhq/moostack/internal/buildx"
	"github.com/moostackhq/moostack/internal/clusterdir"
	"github.com/moostackhq/moostack/internal/deployfile"
	"github.com/moostackhq/moostack/internal/interrupt"
	"github.com/moostackhq/moostack/internal/k3s"
	"github.com/moostackhq/moostack/internal/objects"
	"github.com/moostackhq/moostack/internal/registry"
	"github.com/moostackhq/moostack/internal/secretstore"
	"github.com/moostackhq/moostack/internal/sshclient"
)

// rolloutTimeout bounds waiting for an app's replicas, its chart's
// install and its removal.
const rolloutTimeout = "300s"

// certificateTimeout bounds waiting for a container's certificate.
const certificateTimeout = "120s"

// objectsConfigMap is the ConfigMap in an app's namespace that lists the
// objects its last deploy applied there.
const objectsConfigMap = "moostack-objects"

// deployCommand is mooctl deploy.
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

// deploy is mooctl deploy.
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
	if err := objects.Check(app); err != nil {
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

	image, images, err := buildAll(ctx, env, cmd, d, app, cp, cpClient)
	if err != nil {
		return err
	}
	objs, err := objects.Make(objects.App{App: app, Image: image, Images: images, Secrets: secrets, Issuer: k3s.Issuer})
	if err != nil {
		return err
	}
	if err := waitServices(ctx, env, cpClient); err != nil {
		return err
	}
	before, err := readChartState(ctx, cpClient, app.Name)
	if err != nil {
		return err
	}
	say(env, "Applying %s...", app.Name)
	if err := apply(ctx, cpClient, app, objs); err != nil {
		return err
	}
	say(env, "Waiting for %s to be ready...", app.Name)
	if err := waitReady(ctx, cpClient, app, before); err != nil {
		return fmt.Errorf("%s isn't ready: %w", app.Name, err)
	}
	if err := keepRunning(ctx, cp, cpClient, image, images); err != nil {
		return err
	}
	fmt.Fprintf(env.Stdout, "Deployed %s to %s\n", app.Name, d.Cluster.Ref)
	if image != "" {
		fmt.Fprintf(env.Stdout, "Image: %s\n", image)
	}
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
		if err := waitCertificate(ctx, cpClient, app.Name, objects.TLSSecret(c.Name)); err != nil {
			fmt.Fprintf(env.Stderr, "Warning: %s has no certificate yet: %v\n", c.Host, err)
		}
		fmt.Fprintf(env.Stdout, "URL: https://%s\n", c.Host)
	}
	return nil
}

// buildAll builds and pushes app's images: the one of its manifests or
// chart, which it returns, and those of its containers, which it returns
// by container name. Containers with the same build share one image.
func buildAll(ctx context.Context, env Env, cmd *cli.Command, d *clusterdir.Dir, app *deployfile.App, cp clusterdir.Node, cpClient remote) (string, map[string]string, error) {
	type job struct {
		repo, label string
		build       deployfile.Build
	}
	var jobs []job
	if app.Build != nil {
		jobs = append(jobs, job{app.Name, app.Name, *app.Build})
	}
	for _, c := range app.Containers {
		if c.Build != nil && !slices.ContainsFunc(jobs, func(j job) bool { return j.build == *c.Build }) {
			jobs = append(jobs, job{app.Name + "/" + c.Name, c.Name, *c.Build})
		}
	}
	if len(jobs) == 0 {
		return "", nil, nil
	}
	arches, err := architectures(ctx, d, cpClient)
	if err != nil {
		return "", nil, err
	}
	built := map[deployfile.Build]string{}
	for _, j := range jobs {
		ref, err := buildAndPush(ctx, env, j.repo, j.label, j.build, arches, cp, cpClient)
		if err != nil {
			return "", nil, err
		}
		built[j.build] = ref
	}
	image, images := "", map[string]string{}
	if app.Build != nil {
		image = built[*app.Build]
	}
	for _, c := range app.Containers {
		if c.Build != nil {
			images[c.Name] = built[*c.Build]
		}
	}
	return image, images, nil
}

// architectures are the architectures of the cluster's nodes: those the
// control plane cp lists, and those of d's nodes.
func architectures(ctx context.Context, d *clusterdir.Dir, cp remote) ([]string, error) {
	out, err := run(ctx, cp, `k3s kubectl get nodes --request-timeout=30s -o jsonpath='{.items[*].status.nodeInfo.architecture}'`, "")
	if err != nil {
		return nil, fmt.Errorf("listing the nodes' architectures: %w", err)
	}
	arches := strings.Fields(out)
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

// apply applies objs of app on the control plane cp, server side: the
// cluster-wide objects, then the namespaced ones, and deletes the
// namespaced objects the app's last deploy applied that this one
// doesn't, and its HelmChart if it has no chart now.
func apply(ctx context.Context, cp remote, app *deployfile.App, objs objects.Objects) error {
	const ssa = "k3s kubectl apply --server-side --force-conflicts --field-manager=mooctl --request-timeout=120s"
	ns := sshclient.ShellQuote(app.Name)
	planned, err := objectNames(objs.Namespaced)
	if err != nil {
		return err
	}
	unscoped, err := unscopedKinds(ctx, cp)
	if err != nil {
		return err
	}
	for _, o := range planned {
		if unscoped[kindOf(o)] {
			return fmt.Errorf("%s isn't in a namespace; an app's manifests hold only objects of its namespace", o)
		}
	}
	if _, err := run(ctx, cp, ssa+" -f -", objs.Cluster); err != nil {
		return fmt.Errorf("applying %s's objects: %w", app.Name, err)
	}
	if app.Chart == "" {
		if _, err := run(ctx, cp, "k3s kubectl delete helmchart -n "+objects.ChartNamespace+" -l "+sshclient.ShellQuote(objects.AppLabel+"="+app.Name)+" --ignore-not-found --timeout="+rolloutTimeout, ""); err != nil {
			return fmt.Errorf("removing %s's chart: %w", app.Name, err)
		}
	}
	before, err := run(ctx, cp, "k3s kubectl get configmap "+objectsConfigMap+" -n "+ns+" --ignore-not-found --request-timeout=30s -o jsonpath='{.data.objects}'", "")
	if err != nil {
		return fmt.Errorf("reading what %s's last deploy applied: %w", app.Name, err)
	}
	var recorded []string
	for _, o := range strings.Fields(before) {
		// An object without a name was never applied, and one that
		// isn't in a namespace is never the app's to delete.
		if _, name, _ := strings.Cut(o, "/"); name != "" && !unscoped[kindOf(plainName(o))] {
			recorded = append(recorded, plainName(o))
		}
	}
	// The record holds every object before it is applied.
	if err := record(ctx, cp, app.Name, union(recorded, planned)); err != nil {
		return err
	}
	if objs.Namespaced != "" {
		if _, err := run(ctx, cp, ssa+" -n "+ns+" -f -", objs.Namespaced); err != nil {
			return fmt.Errorf("applying %s's objects: %w", app.Name, err)
		}
	}
	for _, o := range recorded {
		if slices.Contains(planned, o) {
			continue
		}
		// A kind the cluster no longer has leaves nothing to delete.
		script := "out=$(k3s kubectl delete -n " + ns + " --ignore-not-found --request-timeout=120s " + sshclient.ShellQuote(o) + ` 2>&1) || { echo "$out" | grep -q "doesn't have a resource type" || { echo "$out" >&2; exit 1; }; }`
		if _, err := run(ctx, cp, script, ""); err != nil {
			return fmt.Errorf("deleting %s's %s, which it no longer has: %w", app.Name, o, err)
		}
	}
	return record(ctx, cp, app.Name, planned)
}

// unscopedKinds are the kinds of object the cluster of the control plane
// cp keeps outside namespaces, such as Namespace, in lower case.
func unscopedKinds(ctx context.Context, cp remote) (map[string]bool, error) {
	out, err := run(ctx, cp, "k3s kubectl api-resources --namespaced=false --no-headers --request-timeout=30s", "")
	if err != nil {
		return nil, fmt.Errorf("listing the kinds that aren't in a namespace: %w", err)
	}
	kinds := map[string]bool{}
	for _, line := range strings.Split(out, "\n") {
		if fields := strings.Fields(line); len(fields) > 0 {
			kinds[strings.ToLower(fields[len(fields)-1])] = true
		}
	}
	return kinds, nil
}

// kindOf is the kind of the object called name, as kind/name.
func kindOf(name string) string {
	kind, _, _ := strings.Cut(name, "/")
	return kind
}

// record writes the names of the objects app's deploys applied.
func record(ctx context.Context, cp remote, app string, names []string) error {
	const ssa = "k3s kubectl apply --server-side --force-conflicts --field-manager=mooctl --request-timeout=120s"
	list := fmt.Sprintf("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: %s\n  namespace: %s\n  labels: {%s: %s}\ndata:\n  objects: %q\n",
		objectsConfigMap, app, objects.AppLabel, app, strings.Join(names, "\n"))
	if _, err := run(ctx, cp, ssa+" -f -", list); err != nil {
		return fmt.Errorf("recording %s's objects: %w", app, err)
	}
	return nil
}

// objectNames are the names of the objects in manifests, as kind/name,
// the kind in lower case.
func objectNames(manifests string) ([]string, error) {
	var names []string
	dec := yaml.NewDecoder(strings.NewReader(manifests))
	for {
		var doc struct {
			Kind     string `yaml:"kind"`
			Metadata struct {
				Name string `yaml:"name"`
			} `yaml:"metadata"`
		}
		err := dec.Decode(&doc)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		if doc.Kind == "" {
			continue
		}
		names = append(names, strings.ToLower(doc.Kind)+"/"+doc.Metadata.Name)
	}
	return names, nil
}

// plainName is kubectl's name of an object, as deployment.apps/web,
// without its API group: deployment/web.
func plainName(name string) string {
	kind, n, ok := strings.Cut(name, "/")
	if !ok {
		return name
	}
	kind, _, _ = strings.Cut(kind, ".")
	return strings.ToLower(kind) + "/" + n
}

// union is a and the names of b it lacks, in order.
func union(a, b []string) []string {
	out := slices.Clone(a)
	for _, n := range b {
		if !slices.Contains(out, n) {
			out = append(out, n)
		}
	}
	return out
}

// chartState is what deploy reads of an app's chart before applying it:
// its HelmChart's generation and its install job's UID, each empty if
// there is none.
type chartState struct {
	generation, job string
}

// readChartState reads the chart state of the app called name on the
// control plane cp.
func readChartState(ctx context.Context, cp remote, name string) (chartState, error) {
	out, err := run(ctx, cp, chartStateScript(name), "")
	if err != nil {
		return chartState{}, fmt.Errorf("reading %s's chart: %w", name, err)
	}
	g, j, _ := strings.Cut(strings.TrimSpace(out), "|")
	return chartState{generation: g, job: j}, nil
}

// chartStateScript prints the HelmChart generation and install job UID
// of the app called name, separated by |, each empty if there is none.
func chartStateScript(name string) string {
	return `g=$(k3s kubectl get helmchart ` + sshclient.ShellQuote(objects.ChartName(name)) + ` -n ` + objects.ChartNamespace + ` --ignore-not-found --request-timeout=30s -o jsonpath='{.metadata.generation}') || exit 1
j=$(k3s kubectl get job ` + sshclient.ShellQuote("helm-install-"+objects.ChartName(name)) + ` -n ` + objects.ChartNamespace + ` --ignore-not-found --request-timeout=30s -o jsonpath='{.metadata.uid}') || exit 1
echo "$g|$j"`
}

// waitReady waits on the control plane cp until app's chart, if it has
// one, is installed as it is now, and its Deployments, StatefulSets and
// DaemonSets are rolled out. before is the chart's state before the
// apply.
func waitReady(ctx context.Context, cp remote, app *deployfile.App, before chartState) error {
	if app.Chart != "" {
		if err := waitChart(ctx, cp, app.Name, before); err != nil {
			return err
		}
	}
	ns := sshclient.ShellQuote(app.Name)
	_, err := run(ctx, cp, `set -e
objs=$(k3s kubectl get deployments,statefulsets,daemonsets -n `+ns+` -o name --request-timeout=60s)
for o in $objs; do
	k3s kubectl rollout status "$o" -n `+ns+` --timeout=`+rolloutTimeout+` >/dev/null || {
		k3s kubectl get pods -n `+ns+` -o wide >&2
		k3s kubectl get events -n `+ns+` --sort-by=.lastTimestamp | tail -15 >&2
		exit 1
	}
done`, "")
	return err
}

// waitChart waits on the control plane cp until the Helm controller has
// installed the HelmChart of the app called name as it is now: until its
// install job, the one there before if the HelmChart's generation is
// unchanged, or else a new one, has succeeded. Three failed pods of that
// job fail it.
func waitChart(ctx context.Context, cp remote, name string, before chartState) error {
	_, err := run(ctx, cp, waitChartScript(name, before, chartWaitSeconds), "")
	return err
}

// chartWaitSeconds is how long waitChart waits.
var chartWaitSeconds = 300

// waitChartScript waits seconds for the chart of the app called name to
// be installed, as waitChart does.
func waitChartScript(name string, before chartState, seconds int) string {
	job := "helm-install-" + objects.ChartName(name)
	chart := sshclient.ShellQuote(objects.ChartName(name))
	return `old=` + sshclient.ShellQuote(before.job) + `
gen=$(k3s kubectl get helmchart ` + chart + ` -n ` + objects.ChartNamespace + ` --request-timeout=30s -o jsonpath='{.metadata.generation}')
[ "$gen" = ` + sshclient.ShellQuote(before.generation) + ` ] && changed= || changed=1
for i in $(seq ` + strconv.Itoa(seconds) + `); do
	IFS='|' read -r uid succeeded failed <<EOF
$(k3s kubectl get job ` + job + ` -n ` + objects.ChartNamespace + ` --ignore-not-found --request-timeout=10s -o jsonpath='{.metadata.uid}|{.status.succeeded}|{.status.failed}')
EOF
	if [ -n "$uid" ] && { [ -z "$changed" ] || [ "$uid" != "$old" ]; }; then
		[ "${succeeded:-0}" -ge 1 ] && exit 0
		if [ "${failed:-0}" -ge 3 ]; then
			k3s kubectl logs job/` + job + ` -n ` + objects.ChartNamespace + ` --tail=20 >&2
			exit 1
		fi
	fi
	sleep 1
done
echo "the chart isn't installed after ` + strconv.Itoa(seconds) + ` seconds" >&2
exit 1`
}

// waitServices waits until the cluster's services on the control plane
// cp are ready for apps, saying so if it has to.
func waitServices(ctx context.Context, env Env, cp remote) error {
	if _, err := run(ctx, cp, k3s.ServicesReadyScript, ""); err == nil {
		return nil
	}
	say(env, "Waiting for cert-manager and Traefik...")
	if _, err := run(ctx, cp, k3s.WaitServicesScript, ""); err != nil {
		return fmt.Errorf("the cluster isn't ready for apps: %w", err)
	}
	return nil
}

// waitCertificate waits on the control plane cp until the certificate
// called name of the app called app is issued.
func waitCertificate(ctx context.Context, cp remote, app, name string) error {
	ns := sshclient.ShellQuote(app)
	cert := sshclient.ShellQuote("certificate/" + name)
	_, err := run(ctx, cp, "for i in $(seq 30); do k3s kubectl get "+cert+" -n "+ns+" >/dev/null 2>&1 && break; sleep 1; done\nk3s kubectl wait --for=condition=Ready "+cert+" -n "+ns+" --timeout="+certificateTimeout+" >/dev/null 2>&1 || { k3s kubectl get "+cert+" -n "+ns+" -o jsonpath='{.status.conditions[*].message}' >&2; exit 1; }", "")
	return err
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
	tag := k3s.Registry + "/" + repo + ":build"
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
	return k3s.Registry + "/" + repo + "@sha256:" + digest, nil
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
		// A push of less than a megabyte has no bar.
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

// keepRunning marks the images built for a deploy that is ready, image
// and images, as the ones that run, in zot on the control plane cp.
func keepRunning(ctx context.Context, cp clusterdir.Node, cpClient remote, image string, images map[string]string) error {
	refs := []string{image}
	for _, name := range slices.Sorted(maps.Keys(images)) {
		refs = append(refs, images[name])
	}
	var marked []string
	for _, ref := range refs {
		repo, digest, ok := strings.Cut(strings.TrimPrefix(ref, k3s.Registry+"/"), "@sha256:")
		if !ok || slices.Contains(marked, ref) {
			continue
		}
		if err := registry.Tag(ctx, registryAddr(cp), repo, digest, k3s.LiveTag, cpClient.DialTCP); err != nil {
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
	return netip.AddrPortFrom(cp.PrivateAddress, k3s.RegistryPort).String()
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

// removeApp is mooctl deploy --remove: it removes the app named, or the
// current directory's, from the cluster: its HelmChart, which uninstalls
// its chart, its namespace, with everything in it, and its images.
func removeApp(ctx context.Context, env Env, cmd *cli.Command) error {
	var name string
	switch cmd.NArg() {
	case 0:
		_, n, err := deployfile.ReadRef(".")
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("which app? name it, or run mooctl deploy --remove in its directory")
		}
		if err != nil {
			return err
		}
		name = n
	case 1:
		name = cmd.Args().First()
		if err := deployfile.CheckName(name); err != nil {
			return err
		}
	default:
		return wantArgs(cmd, 1)
	}
	ctx, stop := interrupt.Context(ctx, env.Exit)
	defer stop()
	d, err := openCluster(env, cmd)
	if err != nil {
		return err
	}
	cp, ok, err := d.ControlPlane()
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("cluster %s has no nodes", d.Cluster.Ref)
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
	label := sshclient.ShellQuote(objects.AppLabel + "=" + name)
	out, err := run(ctx, cpClient, "k3s kubectl get namespaces -l "+label+" -o name --request-timeout=30s", "")
	if err != nil {
		return fmt.Errorf("looking for %s: %w", name, err)
	}
	if strings.TrimSpace(out) == "" {
		return fmt.Errorf("there is no app %s on %s", name, d.Cluster.Ref)
	}
	say(env, "Removing %s from %s...", name, d.Cluster.Ref)
	if err := registry.Remove(ctx, registryAddr(cp), name, cpClient.DialTCP); err != nil {
		return err
	}
	script := "set -e\n" +
		"k3s kubectl delete helmchart -n " + objects.ChartNamespace + " -l " + label + " --ignore-not-found --timeout=" + rolloutTimeout + " >/dev/null\n" +
		"k3s kubectl delete namespace " + sshclient.ShellQuote(name) + " --ignore-not-found --timeout=" + rolloutTimeout + " >/dev/null"
	if _, err := run(ctx, cpClient, script, ""); err != nil {
		return fmt.Errorf("removing %s: %w", name, err)
	}
	fmt.Fprintf(env.Stdout, "Removed %s from %s\n", name, d.Cluster.Ref)
	return nil
}
