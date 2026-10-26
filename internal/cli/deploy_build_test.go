package cli_test

import (
	"bytes"
	"cmp"
	"context"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/ociimage"
)

// builtAt is when the deploy tests build, and tag the tag of a build
// then.
var builtAt = time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)

const tag = "moostack.local/web:20261026T120000Z"

// docker is a fake docker command, as deploy runs it: it answers info as
// a Docker on store does, and builds by running during, printing output
// to stderr and copying the archive of the platform built for, or failing
// with exit code fail; slow has it send its parent SIGINT, as Ctrl-C
// would, as many times as it says, and then go on building. It logs the
// arguments of each call.
type docker struct {
	store, output string
	// uname is the architecture Docker's machine is, as uname says it;
	// this machine's if empty.
	uname  string
	fail   int
	slow   int
	during string
	// archives are the builds it makes, by architecture.
	archives map[string][]byte
	dir      string
}

// write writes the fake into a new directory, and returns its path.
func (d *docker) write(t *testing.T) string {
	t.Helper()
	d.dir = t.TempDir()
	for arch, a := range d.archives {
		writeFile(t, filepath.Join(d.dir, arch+".tar"), a)
	}
	writeFile(t, filepath.Join(d.dir, "output"), []byte(d.output))
	build := d.during + `
cat "$fake/output" >&2
if [ ` + strconv.Itoa(d.fail) + ` -ne 0 ]; then exit ` + strconv.Itoa(d.fail) + `; fi
cp "$fake/$arch.tar" "$dest"`
	switch {
	case d.slow == 1:
		build = `cat "$fake/output" >&2
kill -INT $PPID
sleep 30 &
wait`
	case d.slow > 1:
		// It builds on past the first, as a build that takes time to stop.
		build = `trap '' TERM
cat "$fake/output" >&2
for i in $(seq ` + strconv.Itoa(d.slow) + `); do kill -INT $PPID; sleep 0.2; done
exit 1`
	}
	script := `#!/bin/sh
fake='` + strings.ReplaceAll(d.dir, "'", `'\''`) + `'
printf '%s\n' "$*" >> "$fake/log"
case "$1" in
info) echo '29.7.2|` + cmp.Or(d.uname, thisUname()) + `|` + d.store + `' ;;
context) echo desktop-linux ;;
buildx)
	if [ "$2" = version ]; then echo "github.com/docker/buildx v0.36.1"; exit 0; fi
	if [ "$2" = inspect ]; then echo "Driver: docker"; exit 0; fi
	while [ $# -gt 0 ]; do
		case "$1" in
		--platform) arch=${2#linux/} ;;
		--output) dest=${2#type=oci,dest=}; dest=${dest%,rewrite-timestamp=true} ;;
		esac
		shift
	done
	` + build + `
	;;
esac
`
	path := filepath.Join(d.dir, "docker")
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

// calls are the fake's calls so far, each its arguments.
func (d *docker) calls(t *testing.T) []string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(d.dir, "log"))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
}

// builds are the fake's calls that build.
func (d *docker) builds(t *testing.T) []string {
	var builds []string
	for _, c := range d.calls(t) {
		if strings.HasPrefix(c, "buildx build ") {
			builds = append(builds, c)
		}
	}
	return builds
}

// containerdStore is what docker info says of the containerd image store.
const containerdStore = "io.containerd.snapshotter.v1"

// webBuild is the archive of a build of web, tagged tag, for arch, of
// content: the same content makes the same build. It returns the
// archive and its build's digest.
func webBuild(t *testing.T, arch, content string) ([]byte, string) {
	t.Helper()
	var b bytes.Buffer
	img := ociimage.Image{Name: tag, Architecture: arch, Entrypoint: []string{"/web"}, Files: []ociimage.File{{Name: "web", Mode: 0o755, Data: []byte(content)}}}
	if err := ociimage.Write(&b, img); err != nil {
		t.Fatal(err)
	}
	digest, err := ociimage.Digest(b.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	return b.Bytes(), digest
}

// builtApp is the app directory of web, built from the context build,
// relative to it, with a Dockerfile, and returns the app directory.
func builtApp(t *testing.T, build string) string {
	t.Helper()
	root := t.TempDir()
	app := filepath.Join(root, "app")
	writeFile(t, filepath.Join(app, "app.yml"), []byte("name: web\nbuild: "+build+"\n"))
	writeFile(t, filepath.Join(app, build, "Dockerfile"), []byte("FROM scratch\n"))
	return app
}

// onArch makes the node called name of the operator's cluster directory
// one of arch.
func onArch(t *testing.T, o *operator, name, arch string) {
	t.Helper()
	path := filepath.Join(o.dir, "nodes", name+".yml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	changed := strings.Replace(string(data), "architecture: amd64", "architecture: "+arch, 1)
	if changed == string(data) {
		t.Fatalf("%s has no architecture to change:\n%s", path, data)
	}
	writeFile(t, path, []byte(changed))
}

// buildDeploy is a deploy of a built app: the cluster's nodes, by name,
// the architecture of those not amd64, and the fake docker.
type buildDeploy struct {
	nodes    func() map[string]*fakeNode
	arches   map[string]string
	docker   docker
	leaseTTL time.Duration
}

// run runs mooctl deploy of app, piped and on a terminal, with $TMPDIR
// a new directory, on fresh nodes and a fresh fake docker each time,
// and checks that each prints exactly stdout, and stderr, and, if
// wantErr is empty, succeeds, or else fails with exactly wantErr, and
// that it leaves nothing in $TMPDIR. It returns the last run's nodes and
// fake docker.
func (b buildDeploy) run(t *testing.T, app, stdout, stderr, wantErr string) (map[string]*fakeNode, *docker) {
	t.Helper()
	var nodes map[string]*fakeNode
	var d docker
	for _, terminal := range []bool{false, true} {
		t.Setenv("TMPDIR", t.TempDir())
		o := newOperator(t, "op")
		if !terminal {
			o.terminal = nil
		}
		nodes = b.nodes()
		deployCluster(t, o, nodes)
		for name, arch := range b.arches {
			onArch(t, o, name, arch)
		}
		d = b.docker
		o.docker, o.now, o.leaseTTL = d.write(t), builtAt, b.leaseTTL
		r := o.run("deploy", app)
		if r.stdout != stdout {
			t.Errorf("on a terminal: %v: deploy printed\n%s\nwant\n%s", terminal, r.stdout, stdout)
		}
		if r.stderr != stderr {
			t.Errorf("on a terminal: %v: deploy printed to stderr\n%s\nwant\n%s", terminal, r.stderr, stderr)
		}
		switch {
		case wantErr == "" && r.err != nil:
			t.Errorf("on a terminal: %v: deploy failed: %v", terminal, r.err)
		case wantErr != "" && (r.err == nil || r.err.Error() != wantErr):
			t.Errorf("on a terminal: %v: deploy says\n%v\nwant\n%s", terminal, r.err, wantErr)
		}
		wantNoTempFiles(t)
	}
	return nodes, &d
}

// buildsWeb is the fake docker on the containerd image store, building
// web's archives for amd64 and arm64 from content.
func buildsWeb(t *testing.T, content string) docker {
	amd, _ := webBuild(t, "amd64", content)
	arm, _ := webBuild(t, "arm64", content)
	return docker{store: containerdStore, output: "#1 building\n", archives: map[string][]byte{"amd64": amd, "arm64": arm}}
}

// readyNode is a healthy node at generation 1 running containers.
func readyNode(containers ...*nodev1.ContainerStatus) *fakeNode {
	n := healthyNode(1)
	n.status.Components = upToDate()
	n.status.Containers = containers
	return n
}

func TestDeployBuildsTheAppThenImportsItOnTheNodeWithTheFewestApps(t *testing.T) {
	app := builtApp(t, ".")
	_, digest := webBuild(t, "amd64", "web 1")
	nodes, d := buildDeploy{
		nodes: func() map[string]*fakeNode {
			return map[string]*fakeNode{"node1": readyNode(appRunning("api", "docker.io/library/httpd:2.4", "sha256:aaaa")), "node2": readyNode()}
		},
		docker: buildsWeb(t, "web 1"),
	}.run(t, app, `Building web as `+tag+` for linux/amd64
web: node2 (fewest apps)
Importing `+tag+` on node2
web runs `+tag+` on node2
`, "#1 building\n", "")
	if got := d.calls(t); len(got) != 5 || got[0] != `info --format {{.ServerVersion}}|{{.Architecture}}|{{range .DriverStatus}}{{if eq (index . 0) "driver-type"}}{{index . 1}}{{end}}{{end}}` || got[1] != "buildx version" {
		t.Errorf("docker was called with\n%s\nwant info, buildx version, then the build", strings.Join(got, "\n"))
	}
	builds := d.builds(t)
	// The build is the checked Docker's own builder's, named for docker's
	// context, whatever builder the operator selected.
	wantBuild := "buildx build --builder desktop-linux --platform linux/amd64 --provenance=false --sbom=false --progress=plain --build-arg SOURCE_DATE_EPOCH=0 --tag " + tag + " --output type=oci,dest="
	if len(builds) != 1 || !strings.HasPrefix(builds[0], wantBuild) || !strings.HasSuffix(builds[0], ".tar,rewrite-timestamp=true -- "+app) {
		t.Errorf("docker built with\n%s\nwant once, %s<temp>.tar -- %s", strings.Join(builds, "\n"), wantBuild, app)
	}
	node2 := nodes["node2"]
	if got := node2.asked; len(got) != 1 || !proto.Equal(got[0], &nodev1.Container{Name: "app-web", Image: tag}) {
		t.Errorf("node2 was asked to run %v, want app-web from %s alone", got, tag)
	}
	if k := node2.status.Containers[0]; k.Digest != digest {
		t.Errorf("node2 runs %v, want the build %s", k, digest)
	}
	if len(nodes["node1"].apps.imported) != 0 || len(node2.pulls()) != 0 {
		t.Errorf("node1 imported %v, and node2 pulled %v, want neither", nodes["node1"].apps.imported, node2.pulls())
	}
}

// A cluster builds once for each architecture its nodes have, whichever
// node the app goes to, and the node imports the build for its own.
func TestDeployBuildsOnceForEachArchitectureAndImportsTheNodesOwn(t *testing.T) {
	_, armDigest := webBuild(t, "arm64", "web 1")
	nodes, d := buildDeploy{
		nodes: func() map[string]*fakeNode {
			return map[string]*fakeNode{
				"node1": readyNode(appRunning("api", "docker.io/library/httpd:2.4", "sha256:aaaa")),
				"node2": readyNode(),
				"node3": readyNode(appRunning("db", "docker.io/library/postgres:17", "sha256:bbbb")),
			}
		},
		arches: map[string]string{"node2": "arm64"},
		docker: buildsWeb(t, "web 1"),
	}.run(t, builtApp(t, "."), `Building web as `+tag+` for linux/amd64
Building web as `+tag+` for linux/arm64
web: node2 (fewest apps)
Importing `+tag+` on node2
web runs `+tag+` on node2
`, "#1 building\n#1 building\n", "")
	if builds := d.builds(t); len(builds) != 2 || !strings.Contains(builds[0], "linux/amd64") || !strings.Contains(builds[1], "linux/arm64") {
		t.Errorf("docker built with\n%s\nwant once for amd64, then once for arm64", strings.Join(builds, "\n"))
	}
	if k := nodes["node2"].status.Containers[0]; k.Digest != armDigest {
		t.Errorf("node2, an arm64 node, runs %v, want the arm64 build %s", k, armDigest)
	}
}

// The build context may lie outside the app's directory, relative to it,
// as in a monorepo, whatever the working directory.
func TestDeployBuildsAContextOutsideTheAppsDirectory(t *testing.T) {
	root := t.TempDir()
	app := filepath.Join(root, "apps", "web")
	writeFile(t, filepath.Join(app, "app.yml"), []byte("name: web\nbuild: ../..\n"))
	writeFile(t, filepath.Join(root, "Dockerfile"), []byte("FROM scratch\n"))
	_, d := buildDeploy{
		nodes:  func() map[string]*fakeNode { return map[string]*fakeNode{"node1": readyNode()} },
		docker: buildsWeb(t, "web 1"),
	}.run(t, app, `Building web as `+tag+` for linux/amd64
web: node1 (fewest apps)
Importing `+tag+` on node1
web runs `+tag+` on node1
`, "#1 building\n", "")
	if builds := d.builds(t); len(builds) != 1 || !strings.HasSuffix(builds[0], " -- "+root) {
		t.Errorf("docker built with\n%s\nwant the context %s", strings.Join(builds, "\n"), root)
	}
}

// The digest decides: a rebuild of the same image under a new tag changes
// nothing, the app taking the new tag's name, and another replaces it.
func TestARebuildIsUnchangedByItsDigestAndAChangedOneReplacesTheApp(t *testing.T) {
	_, same := webBuild(t, "amd64", "web 1")
	const oldTag = "moostack.local/web:20261025T090000Z"
	nodes, _ := buildDeploy{
		nodes: func() map[string]*fakeNode {
			return map[string]*fakeNode{"node1": readyNode(appRunning("web", oldTag, same))}
		},
		docker: buildsWeb(t, "web 1"),
	}.run(t, builtApp(t, "."), `Building web as `+tag+` for linux/amd64
web: node1 (runs there)
Importing `+tag+` on node1
web runs `+tag+` on node1 already; nothing changed
`, "#1 building\n", "")
	if k := nodes["node1"].status.Containers[0]; k.Image != tag || k.Digest != same {
		t.Errorf("node1 runs %v, want the same build under %s", k, tag)
	}
	_, changed := webBuild(t, "amd64", "web 2")
	nodes, _ = buildDeploy{
		nodes: func() map[string]*fakeNode {
			return map[string]*fakeNode{"node1": readyNode(appRunning("web", oldTag, same))}
		},
		docker: buildsWeb(t, "web 2"),
	}.run(t, builtApp(t, "."), `Building web as `+tag+` for linux/amd64
web: node1 (runs there)
Importing `+tag+` on node1
web runs `+tag+` on node1
`, "#1 building\n", "")
	if k := nodes["node1"].status.Containers[0]; k.Image != tag || k.Digest != changed {
		t.Errorf("node1 runs %v, want the changed build", k)
	}
}

// What the check of Docker finds missing, deploy says what to install or
// switch, before it builds anything or contacts a node.
func TestDeployWithoutDockerOnTheContainerdStoreSaysWhatToInstallOrSwitch(t *testing.T) {
	const nothing = "\nmooctl installs nothing; nothing was built, and nothing changed on the cluster"
	for name, c := range map[string]struct {
		docker docker
		path   string
		want   string
	}{
		"no docker": {path: "/nonexistent/docker",
			want: "mooctl can't build web: there is no /nonexistent/docker command: install Docker Engine with its buildx plugin, or Docker Desktop" + nothing},
		"the classic image store": {docker: docker{store: ""},
			want: `mooctl can't build web: Docker keeps its images in its classic image store, from which buildx can't write an image's archive; switch it to the containerd image store: set "features": {"containerd-snapshotter": true} in Docker Engine's daemon.json and restart Docker, or turn on "Use containerd for pulling and storing images" in Docker Desktop's settings` + nothing},
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("TMPDIR", t.TempDir())
			o := newOperator(t, "op")
			n := readyNode()
			deployCluster(t, o, map[string]*fakeNode{"node1": n})
			o.docker, o.now = c.docker.write(t), builtAt
			if c.path != "" {
				o.docker = c.path
			}
			r := o.run("deploy", builtApp(t, "."))
			if r.err == nil || r.err.Error() != c.want {
				t.Errorf("deploy says\n%v\nwant\n%s", r.err, c.want)
			}
			if builds := c.docker.builds(t); len(builds) != 0 {
				t.Errorf("docker built %v", builds)
			}
			if n.dialed() != 0 {
				t.Error("deploy contacted the node")
			}
			wantNoTempFiles(t)
		})
	}
}

// A failed build shows buildx's own output, says nothing changed, and
// contacts no node; "exec format error" comes with how to build for
// another architecture.
func TestAFailedBuildSaysNothingChangedOnTheCluster(t *testing.T) {
	const failed = "the build of web failed; nothing changed on the cluster"
	for name, c := range map[string]struct {
		output, want string
	}{
		"a Dockerfile's error": {"#5 [2/2] COPY missing /missing\nERROR: failed to build: \"/missing\": not found\n", failed},
		"exec format error": {"#5 0.241 exec /bin/sh: exec format error\nERROR: failed to build\n", failed +
			"\nThe build said \"exec format error\": a step ran a program this machine can't run, as one for linux/arm64 is without emulation. " +
			"On Linux, register QEMU for it once with docker run --privileged --rm tonistiigi/binfmt --install arm64, or install your distribution's qemu-user-static with its binfmt entries; " +
			"Docker Desktop has it already. Or build the program for linux/arm64 in a stage that runs on this machine's platform, FROM --platform=$BUILDPLATFORM."},
	} {
		t.Run(name, func(t *testing.T) {
			var n *fakeNode
			buildDeploy{
				nodes: func() map[string]*fakeNode {
					n = readyNode()
					return map[string]*fakeNode{"node1": n}
				},
				arches: map[string]string{"node1": "arm64"},
				docker: docker{store: containerdStore, output: c.output, fail: 1},
			}.run(t, builtApp(t, "."), "Building web as "+tag+" for linux/arm64\n", c.output, c.want)
			if n.dialed() != 0 {
				t.Error("deploy contacted the node after a failed build")
			}
		})
	}
}

// Ctrl-C during the build stops it, contacts no node, and leaves no
// archive behind.
func TestCtrlCDuringTheBuildStopsItAndLeavesNothing(t *testing.T) {
	var n *fakeNode
	_, d := buildDeploy{
		nodes: func() map[string]*fakeNode {
			n = readyNode()
			return map[string]*fakeNode{"node1": n}
		},
		docker: docker{store: containerdStore, output: "#1 building\n", slow: 1},
	}.run(t, builtApp(t, "."), "Building web as "+tag+" for linux/amd64\n", "#1 building\n", "mooctl stopped the build of web; nothing changed on the cluster")
	if n.dialed() != 0 {
		t.Error("deploy contacted the node after a stopped build")
	}
	if builds := d.builds(t); len(builds) != 1 {
		t.Errorf("docker built %v, want once", builds)
	}
}

// A node that refuses the build, as one of another architecture, leaves
// the app as it ran, and no archive behind.
func TestABuildTheNodeRefusesChangesNothing(t *testing.T) {
	const oldTag = "moostack.local/web:20261025T090000Z"
	nodes, _ := buildDeploy{
		nodes: func() map[string]*fakeNode {
			n := readyNode(appRunning("web", oldTag, "sha256:1111"))
			n.apps.importErr = status.Error(codes.FailedPrecondition, "the image is for linux/arm64, not for this node's linux/amd64")
			return map[string]*fakeNode{"node1": n}
		},
		docker: buildsWeb(t, "web 2"),
	}.run(t, builtApp(t, "."), `Building web as `+tag+` for linux/amd64
web: node1 (runs there)
Importing `+tag+` on node1
`, "#1 building\n", "node1 refused "+tag+": the image is for linux/arm64, not for this node's linux/amd64; nothing changed\nCheck the architecture that node1's node file in the cluster directory gives it")
	if n := nodes["node1"]; len(n.asked) != 0 || n.status.Containers[0].Image != oldTag {
		t.Errorf("after a refused import, node1 was asked to run %v and runs %v", n.asked, n.status.Containers)
	}
}

// A bad build context is refused before docker is asked anything.
func TestABadBuildContextIsRefusedBeforeDockerIsAsked(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	o := newOperator(t, "op")
	deployCluster(t, o, map[string]*fakeNode{"node1": readyNode()})
	d := buildsWeb(t, "web 1")
	o.docker = d.write(t)
	app := appDir(t, "name: web\nbuild: missing\n")
	r := o.run("deploy", app)
	want := filepath.Join(app, "app.yml") + ": build: missing doesn't exist; give the build context's directory, relative to the app directory; nothing was contacted"
	if r.err == nil || r.err.Error() != want {
		t.Errorf("deploy says\n%v\nwant\n%s", r.err, want)
	}
	if calls := d.calls(t); len(calls) != 0 {
		t.Errorf("docker was called with %v", calls)
	}
	wantNoTempFiles(t)
}

// deploy --remove of a built app needs no Docker.
func TestRemovingABuiltAppNeedsNoDocker(t *testing.T) {
	o := newOperator(t, "op")
	deployCluster(t, o, map[string]*fakeNode{"node1": readyNode(appRunning("web", tag, "sha256:1111"))})
	o.docker = "/nonexistent/docker"
	r := o.run("deploy", "--remove", builtApp(t, "."))
	if r.err != nil || r.stdout != "Removed web from node1\n" {
		t.Errorf("deploy --remove printed %q, %v", r.stdout, r.err)
	}
}

// Ctrl-C during the import stops it, saying nothing changed, and leaves
// no archive behind; a second Ctrl-C ends mooctl at once, with none left
// either.
func TestCtrlCDuringTheImportLeavesNoArchive(t *testing.T) {
	buildDeploy{
		nodes: func() map[string]*fakeNode {
			n := readyNode()
			n.apps.importing = interrupt
			return map[string]*fakeNode{"node1": n}
		},
		docker: buildsWeb(t, "web 1"),
	}.run(t, builtApp(t, "."), `Building web as `+tag+` for linux/amd64
web: node1 (fewest apps)
Importing `+tag+` on node1
`, "#1 building\n", "mooctl stopped the import of "+tag+" on node1: context canceled; nothing changed")

	t.Setenv("TMPDIR", t.TempDir())
	o := newOperator(t, "op")
	n := readyNode()
	n.apps.importing = func(ctx context.Context) {
		interrupt(ctx)
		syscall.Kill(os.Getpid(), syscall.SIGINT)
		for len(o.exitCodes()) == 0 {
			time.Sleep(time.Millisecond)
		}
	}
	deployCluster(t, o, map[string]*fakeNode{"node1": n})
	d := buildsWeb(t, "web 1")
	o.docker, o.now = d.write(t), builtAt
	o.run("deploy", builtApp(t, "."))
	if got := o.exitCodes(); !slices.Equal(got, []int{130}) {
		t.Fatalf("mooctl exited with %v on a second Ctrl-C, want 130", got)
	}
	o.mu.Lock()
	left := o.tempFilesAtExit
	o.mu.Unlock()
	if left != 0 {
		t.Errorf("mooctl left %d files in $TMPDIR as it exited", left)
	}
}

// The tag is the build's time in UTC, whatever this machine's zone.
func TestABuildIsTaggedWithItsTimeInUTC(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	o := newOperator(t, "op")
	deployCluster(t, o, map[string]*fakeNode{"node1": readyNode()})
	d := buildsWeb(t, "web 1")
	o.docker, o.now = d.write(t), builtAt.In(time.FixedZone("UTC+2", 2*60*60))
	r := o.run("deploy", builtApp(t, "."))
	if r.err != nil || !strings.HasPrefix(r.stdout, "Building web as "+tag+" for") {
		t.Errorf("deploy at %s printed\n%s%v\nwant the tag %s", o.now, r.stdout, r.err, tag)
	}
}

// "exec format error" in a build for this machine's own architecture
// isn't for want of emulation, and comes without the hint; and a build
// that ends without failing as a build does, killed, says why.
func TestAFailedBuildSaysOnlyWhatApplies(t *testing.T) {
	const failed = "the build of web failed; nothing changed on the cluster"
	for name, c := range map[string]struct {
		docker docker
		want   string
	}{
		"exec format error for this machine": {docker{store: containerdStore, output: "exec /garbage: exec format error\n", fail: 1}, failed},
		// Docker reached on a machine of another architecture than this
		// one's, as through DOCKER_HOST, builds for this one's under
		// emulation, if it has it.
		"exec format error for a remote Docker's other architecture": {docker{store: containerdStore, output: "exec /bin/sh: exec format error\n", fail: 1, uname: "riscv64"}, failed +
			"\nThe build said \"exec format error\": a step ran a program this machine can't run, as one for linux/" + runtime.GOARCH + " is without emulation. " +
			"On Linux, register QEMU for it once with docker run --privileged --rm tonistiigi/binfmt --install " + runtime.GOARCH + ", or install your distribution's qemu-user-static with its binfmt entries; " +
			"Docker Desktop has it already. Or build the program for linux/" + runtime.GOARCH + " in a stage that runs on this machine's platform, FROM --platform=$BUILDPLATFORM."},
		"killed": {docker{store: containerdStore, during: "kill -9 $$"}, failed + "\ndocker buildx ended: signal: killed"},
		"exit 2": {docker{store: containerdStore, fail: 2}, failed + "\ndocker buildx ended: exit status 2"},
	} {
		t.Run(name, func(t *testing.T) {
			buildDeploy{
				nodes:  func() map[string]*fakeNode { return map[string]*fakeNode{"node1": readyNode()} },
				arches: thisMachines(),
				docker: c.docker,
			}.run(t, builtApp(t, "."), "Building web as "+tag+" for linux/"+runtime.GOARCH+"\n", c.docker.output, c.want)
		})
	}
}

// thisUname is this machine's architecture, as uname says it.
func thisUname() string {
	return map[string]string{"amd64": "x86_64", "arm64": "aarch64"}[runtime.GOARCH]
}

// thisMachines makes node1 of this machine's architecture.
func thisMachines() map[string]string {
	if runtime.GOARCH == "amd64" {
		return nil
	}
	return map[string]string{"node1": runtime.GOARCH}
}

// Ctrl-C while mooctl checks Docker says mooctl stopped, not that Docker
// doesn't answer.
func TestCtrlCWhileCheckingDockerSaysMooctlStopped(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	o := newOperator(t, "op")
	n := readyNode()
	deployCluster(t, o, map[string]*fakeNode{"node1": n})
	docker := filepath.Join(t.TempDir(), "docker")
	writeFile(t, docker, []byte("#!/bin/sh\nkill -INT $PPID\nexec sleep 30\n"))
	if err := os.Chmod(docker, 0o755); err != nil {
		t.Fatal(err)
	}
	o.docker = docker
	r := o.run("deploy", builtApp(t, "."))
	if want := "mooctl stopped checking Docker, before it built web; nothing changed on the cluster"; r.err == nil || r.err.Error() != want {
		t.Errorf("deploy says %v, want %s", r.err, want)
	}
	if n.dialed() != 0 {
		t.Error("deploy contacted the node")
	}
	wantNoTempFiles(t)
}

// A second Ctrl-C during the build ends mooctl at once, leaving no
// archive behind.
func TestASecondCtrlCDuringTheBuildLeavesNoArchive(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	o := newOperator(t, "op")
	deployCluster(t, o, map[string]*fakeNode{"node1": readyNode()})
	d := docker{store: containerdStore, slow: 2}
	o.docker, o.now = d.write(t), builtAt
	done := make(chan struct{})
	go func() {
		defer close(done)
		o.run("deploy", builtApp(t, "."))
	}()
	deadline := time.Now().Add(20 * time.Second)
	for len(o.exitCodes()) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := o.exitCodes(); !slices.Equal(got, []int{130}) {
		t.Fatalf("mooctl exited with %v on a second Ctrl-C during the build, want 130", got)
	}
	o.mu.Lock()
	left := o.tempFilesAtExit
	o.mu.Unlock()
	if left != 0 {
		t.Errorf("mooctl left %d files in $TMPDIR as it exited", left)
	}
	<-done
}

// A node that joins the cluster directory while the app builds, as from
// a git pull, gets the build for its architecture if one was built, and
// otherwise the deploy says what happened and to deploy again.
func TestANodeThatJoinsDuringTheBuild(t *testing.T) {
	for name, c := range map[string]struct {
		arch, stdout, want string
	}{
		"of an architecture built for": {"amd64", "Building web as " + tag + " for linux/amd64\nweb: node2 (fewest apps)\nImporting " + tag + " on node2\nweb runs " + tag + " on node2\n", ""},
		"of another architecture": {"arm64", "Building web as " + tag + " for linux/amd64\nweb: node2 (fewest apps)\n",
			"reading the build of web for node2: node2, of linux/arm64, joined the cluster directory while web was built, so mooctl built nothing for it; deploy again; nothing changed"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("TMPDIR", t.TempDir())
			o := newOperator(t, "op")
			deployCluster(t, o, map[string]*fakeNode{"node1": readyNode(appRunning("api", "docker.io/library/httpd:2.4", "sha256:aaaa")), "node2": readyNode()})
			if c.arch != "amd64" {
				onArch(t, o, "node2", c.arch)
			}
			// node2 joins while web builds.
			file := filepath.Join(o.dir, "nodes", "node2.yml")
			aside := filepath.Join(t.TempDir(), "node2.yml")
			if err := os.Rename(file, aside); err != nil {
				t.Fatal(err)
			}
			d := buildsWeb(t, "web 1")
			d.during = "mv '" + aside + "' '" + file + "'"
			o.docker, o.now = d.write(t), builtAt
			r := o.run("deploy", builtApp(t, "."))
			if r.stdout != c.stdout || (c.want == "") != (r.err == nil) || r.err != nil && r.err.Error() != c.want {
				t.Errorf("deploy printed\n%s\nand says %v\nwant\n%s\n%s", r.stdout, r.err, c.stdout, c.want)
			}
			if builds := d.builds(t); len(builds) != 1 {
				t.Errorf("docker built %v, want once, for node1's amd64", builds)
			}
			wantNoTempFiles(t)
		})
	}
}

// Each way an import fails says what it means for the cluster, and what
// to do next.
func TestEachFailedImportSaysWhatItMeans(t *testing.T) {
	lost := "the operation lost the operation lock: node1: it is locked by bob on desktop since " +
		bobsLock.Since.Local().Format("2006-01-02 15:04:05 MST") + ", and expires at " + bobsLock.Until.Local().Format("2006-01-02 15:04:05 MST") + " unless renewed"
	const imported = "Building web as " + tag + " for linux/amd64\nweb: node1 (fewest apps)\nImporting " + tag + " on node1\n"
	for name, c := range map[string]struct {
		set  func(n *fakeNode)
		want string
	}{
		"the node failing": {func(n *fakeNode) { n.apps.importErr = status.Error(codes.Unavailable, "containerd doesn't answer") },
			"node1 couldn't import " + tag + ": containerd doesn't answer; nothing changed\nRun mooctl deploy again"},
		"refused for its platform": {func(n *fakeNode) {
			n.apps.importErr = status.Error(codes.FailedPrecondition, "the image is for linux/arm64, not for this node's linux/amd64")
		}, "node1 refused " + tag + ": the image is for linux/arm64, not for this node's linux/amd64; nothing changed\nCheck the architecture that node1's node file in the cluster directory gives it"},
		"refused as an index without the node's platform": {func(n *fakeNode) {
			n.apps.importErr = status.Error(codes.FailedPrecondition, "it has no image for this node's linux/amd64: no match for platform in manifest")
		}, "node1 refused " + tag + ": it has no image for this node's linux/amd64: no match for platform in manifest; nothing changed\nCheck the architecture that node1's node file in the cluster directory gives it"},
		"an archive the node can't read": {func(n *fakeNode) {
			n.apps.importErr = status.Error(codes.InvalidArgument, "importing the image: the archive has no index.json")
		}, "node1 couldn't import " + tag + ": importing the image: the archive has no index.json; nothing changed"},
		"the lock's fence": {func(n *fakeNode) {
			n.apps.importErr = status.Error(codes.FailedPrecondition, "the operation's lease doesn't hold the node's operation lock")
		}, "node1 refused " + tag + ": the operation's lease doesn't hold the node's operation lock; nothing changed\nRun mooctl deploy again"},
		"the lock lost": {func(n *fakeNode) { n.apps.importing = func(ctx context.Context) { stealLock(ctx, n) } },
			"mooctl stopped the import of " + tag + " on node1: " + lost + "; nothing changed"},
	} {
		t.Run(name, func(t *testing.T) {
			var n *fakeNode
			buildDeploy{
				nodes: func() map[string]*fakeNode {
					n = readyNode()
					c.set(n)
					return map[string]*fakeNode{"node1": n}
				},
				leaseTTL: 600 * time.Millisecond,
				docker:   buildsWeb(t, "web 1"),
			}.run(t, builtApp(t, "."), imported, "#1 building\n", c.want)
			if len(n.asked) != 0 {
				t.Errorf("node1 was asked to run %v after a failed import", n.asked)
			}
		})
	}
}
