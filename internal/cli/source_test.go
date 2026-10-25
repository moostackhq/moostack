package cli_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"

	"github.com/moostackhq/moostack/internal/release"
)

// devVersion is a development build's version, as cluster update
// --source versions one from a checkout with changes.
const devVersion = "0123456789ab-dirty"

// updatedTo are the components and services n was updated to, as
// "agent 1.1.0", "run service-dns 1.1.0" or "remove dns", without their
// binaries or images.
func updatedTo(n *fakeNode) []string {
	var got []string
	for _, u := range n.nodeUpdates() {
		f := strings.Fields(u)
		switch f[0] {
		case "run":
			got = append(got, "run "+f[1]+" "+f[2])
		case "remove":
			got = append(got, u)
		default:
			got = append(got, f[0]+" "+f[1])
		}
	}
	return got
}

// nodeArch is the architecture of each node of updateCluster.
var nodeArch = map[string]string{"node1": "amd64", "node2": "arm64", "node3": "amd64"}

// Without --release-source: a source update needs no release.
func TestClusterUpdateFromASourceBuildsEachComponentNamedForEachNodesArchitecture(t *testing.T) {
	o := newOperator(t, "op")
	updateCluster(t, o)
	checkout := devCheckout(t)
	version := commitOf(t, checkout)
	before := snapshot(t, o.dir)
	r := o.mustRun("cluster", "update", "--source", checkout, "dns", "agent")

	for name, n := range o.nodes {
		if got, want := updatedTo(n), []string{"agent " + version, "run service-dns " + version}; !slices.Equal(got, want) {
			t.Errorf("%s was updated to %q, want %q", name, got, want)
		}
		wantProgram(t, name+"'s agent", n.sentFile("agent"), nodeArch[name], version)
		if got, want := digest(t, n.sentFile(release.ImageRef("dns", version))), serviceDigest(t, "dns", version, nodeArch[name]); got != want {
			t.Errorf("%s imported a dns image of digest %q, want %s, of the dns that cargo built for %s", name, got, want, nodeArch[name])
		}
		if l := n.heldLock(); l != nil {
			t.Errorf("after the update, %s holds the lock %+v", name, l)
		}
		if g := n.granted(); len(g) != 1 || g[0].Operation != "cluster update" {
			t.Errorf("%s granted the locks %+v, want one, in the preflight, for cluster update", name, g)
		}
	}
	var want string
	for _, name := range []string{"node1", "node2", "node3"} {
		want += "Updating the agent on " + name + " to " + version + "\n"
	}
	for _, name := range []string{"node1", "node2", "node3"} {
		want += "Updating the dns service on " + name + " to " + version + "\n"
	}
	want += "Every node runs the development build " + version + " of the agent and the dns service. The cluster directory still pins release 1.1.0: mooctl cluster update puts it back.\n"
	if r.stdout != want {
		t.Errorf("cluster update printed\n%s\nwant\n%s", r.stdout, want)
	}
	wantContains(t, "cluster update's stderr", r.stderr, "Building the agent and the dns service from "+checkout+" at "+version)
	// cargo built dns only, once for each architecture.
	log := readFile(t, filepath.Join(checkout, "cargo.log"))
	if strings.Count(log, "\n") != 2 || strings.Count(log, "-p moostack-dns") != 2 || strings.Count(log, "-p ") != 2 ||
		!strings.Contains(log, "x86_64-unknown-linux-musl") || !strings.Contains(log, "aarch64-unknown-linux-musl") {
		t.Errorf("cargo was run as\n%s\nwant once for each architecture, for dns only", log)
	}
	if after := snapshot(t, o.dir); !maps.Equal(before, after) {
		t.Error("cluster update --source changed the cluster directory")
	}
}

func TestClusterUpdateFromASourceBuildsEveryComponentWhenItNamesNone(t *testing.T) {
	o := newOperator(t, "op")
	updateCluster(t, o)
	checkout := devCheckout(t)
	version := commitOf(t, checkout)
	r := o.mustRun("cluster", "update", "--source", checkout)
	want := []string{"agent " + version, "updater " + version}
	for _, s := range release.ClusterServices {
		want = append(want, "run "+release.ContainerName(s.Name)+" "+version)
	}
	for name, n := range o.nodes {
		if got := updatedTo(n); !slices.Equal(got, want) {
			t.Errorf("%s was updated to %q, want %q, containerd untouched", name, got, want)
		}
		wantProgram(t, name+"'s updater", n.sentFile("updater"), nodeArch[name], version)
	}
	wantContains(t, "cluster update's output", r.stdout,
		"Every node runs the development build "+version+" of the agent, the updater, the registry service, the dns service, the proxy service and the telemetry service.")
	log := readFile(t, filepath.Join(checkout, "cargo.log"))
	if strings.Count(log, "\n") != 2 || strings.Count(log, "-p ") != 2*len(release.ClusterServices) {
		t.Errorf("cargo was run as\n%s\nwant once for each architecture, for every service", log)
	}
}

func TestClusterUpdateFromASourceRefusesBeforeReachingANode(t *testing.T) {
	for _, c := range []struct {
		name string
		// args are cluster update's, given the checkout.
		args func(checkout string) []string
		want []string
	}{
		{"an unknown component", func(checkout string) []string { return []string{"--source", checkout, "agent", "containerd"} },
			[]string{`cluster update --source builds agent, updater, registry, dns, proxy or telemetry, not "containerd"`}},
		{"components without a source", func(string) []string { return []string{"agent"} },
			[]string{"cluster update takes components to build only with --source"}},
		{"a source outside git", func(string) []string { return []string{"--source", t.TempDir(), "agent"} },
			[]string{"isn't a git checkout with a commit"}},
		{"an agent that doesn't build", func(checkout string) []string {
			writeFile(t, filepath.Join(checkout, "cmd", "agent", "main.go"), []byte("package main\n\nfunc main() { undefined() }\n"))
			return []string{"--source", checkout, "agent"}
		}, []string{"building ./cmd/agent for linux/amd64"}},
		{"a service that doesn't build", func(checkout string) []string {
			writeFile(t, filepath.Join(filepath.SplitList(os.Getenv("PATH"))[0], "cargo"), []byte("#!/bin/sh\nexit 101\n"))
			return []string{"--source", checkout, "agent", "dns"}
		}, []string{"building the cluster services for amd64"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			o := newOperator(t, "op")
			updateCluster(t, o)
			r := o.run(append([]string{"cluster", "update"}, c.args(devCheckout(t))...)...)
			if r.err == nil {
				t.Fatalf("cluster update succeeded:\n%s", r.stdout)
			}
			wantContains(t, "cluster update's error", r.err.Error(), c.want...)
			for name, n := range o.nodes {
				if n.dialed() > 0 {
					t.Errorf("cluster update reached %s", name)
				}
			}
		})
	}
}

// The nodes are read again by the preflight, after the build: a node
// added in between, from another terminal or a pull of the cluster
// directory, has no build, and the update stops before any step.
func TestANodeAddedWhileTheSourceBuildsStopsTheUpdateBeforeAnyStep(t *testing.T) {
	o := newOperator(t, "op")
	updateCluster(t, o)
	checkout := devCheckout(t)
	file := filepath.Join(o.dir, "nodes", "node3.yml")
	aside := filepath.Join(t.TempDir(), "node3.yml")
	if err := os.Rename(file, aside); err != nil {
		t.Fatal(err)
	}
	cargo := filepath.Join(filepath.SplitList(os.Getenv("PATH"))[0], "cargo")
	writeFile(t, cargo, []byte(strings.Replace(readFile(t, cargo), "#!/bin/sh\n", "#!/bin/sh\nmv '"+aside+"' '"+file+"' 2>/dev/null\n", 1)))
	r := o.run("cluster", "update", "--source", checkout, "agent", "dns")
	if r.err == nil {
		t.Fatalf("cluster update succeeded:\n%s", r.stdout)
	}
	wantContains(t, "cluster update's error", r.err.Error(), "node3: it joined the cluster directory after cluster update read it; run cluster update again")
	for name, n := range o.nodes {
		if u := n.nodeUpdates(); len(u) > 0 {
			t.Errorf("%s was updated to %v", name, u)
		}
	}
}

func TestClusterUpdateFromASourceIsRefusedWhileAnotherOperationHoldsTheLock(t *testing.T) {
	o := newOperator(t, "op")
	updateCluster(t, o)
	o.nodes["node2"].locked(bobsLock)
	r := o.run("cluster", "update", "--source", devCheckout(t), "agent")
	if r.err == nil {
		t.Fatal("cluster update succeeded")
	}
	wantContains(t, "cluster update's error", r.err.Error(), "bob on desktop", "Another operation holds the operation lock.")
	for name, n := range o.nodes {
		if u := n.nodeUpdates(); len(u) > 0 {
			t.Errorf("%s was updated to %v", name, u)
		}
	}
}

func TestADevelopmentBuildOfACheckoutWithChangesIsVersionedDirty(t *testing.T) {
	o := newOperator(t, "op")
	updateCluster(t, o)
	checkout := devCheckout(t)
	writeFile(t, filepath.Join(checkout, "cmd", "updater", "changed.go"), []byte("package main\n"))
	r := o.mustRun("cluster", "update", "--source", checkout, "updater")
	version := commitOf(t, checkout) + "-dirty"
	wantContains(t, "cluster update's output", r.stdout, "Every node runs the development build "+version+" of the updater. ")
	for name, n := range o.nodes {
		if got, want := updatedTo(n), []string{"updater " + version}; !slices.Equal(got, want) {
			t.Errorf("%s was updated to %q, want %q", name, got, want)
		}
		wantProgram(t, name+"'s updater", n.sentFile("updater"), nodeArch[name], version)
	}
}

// The nodes report the builds they run, so a rerun skips what they run
// only if building the same checkout again gives the same builds.
func TestARerunFromTheSameSourceGoesOnFromTheStepThatFailed(t *testing.T) {
	o := newOperator(t, "op")
	updateCluster(t, o)
	checkout := devCheckout(t)
	version := commitOf(t, checkout)
	o.nodes["node2"].serviceErr = errors.New("the disk is full")
	r := o.run("cluster", "update", "--source", checkout, "agent", "proxy")
	if r.err == nil {
		t.Fatal("cluster update succeeded")
	}
	wantContains(t, "cluster update's error", r.err.Error(), "cluster update stopped: node2: updating the proxy service to "+version+": the disk is full",
		"Once that is fixed, run mooctl -C "+o.dir+" cluster update --source "+checkout+" agent proxy again: it goes on from there.")
	if got := updatedTo(o.nodes["node3"]); !slices.Equal(got, []string{"agent " + version}) {
		t.Errorf("node3 was updated to %q, want only its agent, as node2's proxy failed first", got)
	}

	o.nodes["node2"].serviceErr = nil
	r = o.mustRun("cluster", "update", "--source", checkout, "agent", "proxy")
	want := "node1 runs the agent at " + version + " already\n" +
		"node2 runs the agent at " + version + " already\n" +
		"node3 runs the agent at " + version + " already\n" +
		"node1 runs the proxy service at " + version + " already\n" +
		"Updating the proxy service on node2 to " + version + "\n" +
		"Updating the proxy service on node3 to " + version + "\n"
	if !strings.HasPrefix(r.stdout, want) {
		t.Errorf("the rerun printed\n%s\nwant it to start\n%s", r.stdout, want)
	}
}

// A checkout with changes is versioned the same however it changes:
// the build tells them apart.
func TestAnotherChangeUnderTheSameDirtyVersionIsUpdatedAgain(t *testing.T) {
	o := newOperator(t, "op")
	updateCluster(t, o)
	checkout := devCheckout(t)
	version := commitOf(t, checkout) + "-dirty"
	writeFile(t, filepath.Join(checkout, "cmd", "agent", "change.go"), []byte("package main\n\nfunc init() { println(\"one\") }\n"))
	o.mustRun("cluster", "update", "--source", checkout, "agent")
	writeFile(t, filepath.Join(checkout, "cmd", "agent", "change.go"), []byte("package main\n\nfunc init() { println(\"two\") }\n"))
	r := o.mustRun("cluster", "update", "--source", checkout, "agent")
	for _, name := range []string{"node1", "node2", "node3"} {
		wantContains(t, "the second update's output", r.stdout, "Updating the agent on "+name+" to "+version+"\n")
	}
	for name, n := range o.nodes {
		if got := updatedTo(n); !slices.Equal(got, []string{"agent " + version, "agent " + version}) {
			t.Errorf("%s was updated to %q, want the agent twice", name, got)
		}
	}
}

// shells are the shells an operator pastes a command line into that are
// on this machine: sh, and zsh, which reads some arguments sh doesn't.
func shells(t *testing.T) []string {
	var found []string
	for _, sh := range []string{"sh", "zsh"} {
		if _, err := exec.LookPath(sh); err == nil {
			found = append(found, sh)
		}
	}
	return found
}

// hintArgs are the arguments of the command line that err says to run
// again, as shell reads them, after "mooctl".
func hintArgs(t *testing.T, shell string, err error) []string {
	t.Helper()
	_, line, ok := strings.Cut(err.Error(), "Once that is fixed, run ")
	line, _, ok2 := strings.Cut(line, " again: it goes on from there.")
	if !ok || !ok2 || !strings.HasPrefix(line, "mooctl ") {
		t.Fatalf("cluster update's error has no command line to run again:\n%v", err)
	}
	out, cmdErr := exec.Command(shell, "-c", "printf '%s\\n' "+strings.TrimPrefix(line, "mooctl ")).Output()
	if cmdErr != nil {
		t.Fatalf("%s reading %s: %v", shell, line, cmdErr)
	}
	return strings.Split(strings.TrimSuffix(string(out), "\n"), "\n")
}

// The hint's command line, pasted into a shell, runs cluster update in
// the same cluster directory, from the same source, however their paths
// are named.
func TestTheRerunHintPastesBackTheSameArguments(t *testing.T) {
	o := newOperator(t, "op")
	updateCluster(t, o)
	checkout := filepath.Join(t.TempDir(), "my $HOME `id` 'dir';x\ty")
	if err := os.Rename(devCheckout(t), checkout); err != nil {
		t.Fatal(err)
	}
	o.nodes["node2"].updateErr = errors.New("the disk is full")
	r := o.run("cluster", "update", "--source", checkout, "agent")
	if r.err == nil {
		t.Fatal("cluster update succeeded")
	}
	for _, shell := range shells(t) {
		if got, want := hintArgs(t, shell, r.err), []string{"-C", o.dir, "cluster", "update", "--source", checkout, "agent"}; !slices.Equal(got, want) {
			t.Errorf("%s reads the hint as %q, want %q", shell, got, want)
		}
	}
}

// The hint of an update of the release keeps its release source, and
// --force, so that pasting it runs the same update. A release source
// named as zsh expands, from the directory mooctl ran in, stays as named.
func TestTheRerunHintOfAReleaseUpdateKeepsItsSourceAndForce(t *testing.T) {
	o := newOperator(t, "op")
	source := updateCluster(t, o)
	dir := t.TempDir()
	if err := os.Rename(source, filepath.Join(dir, "=releases")); err != nil {
		t.Fatal(err)
	}
	t.Chdir(dir)
	o.nodes["node2"].serviceErr = errors.New("the disk is full")
	r := o.run("--release-source", "=releases", "cluster", "update", "--force")
	if r.err == nil {
		t.Fatal("cluster update succeeded")
	}
	for _, shell := range shells(t) {
		if got, want := hintArgs(t, shell, r.err), []string{"-C", o.dir, "--release-source", "=releases", "cluster", "update", "--force"}; !slices.Equal(got, want) {
			t.Errorf("%s reads the hint as %q, want %q", shell, got, want)
		}
	}
}

// A release source with a password in it stays out of the hint, which
// would show it.
func TestTheRerunHintLeavesOutAReleaseSourceWithAPassword(t *testing.T) {
	o := newOperator(t, "op")
	source := updateCluster(t, o)
	s := serveSource(t, source)
	o.client = s.Client()
	url := strings.Replace(s.URL, "https://", "https://alice:hunter2@", 1)
	o.nodes["node2"].serviceErr = errors.New("the disk is full")
	r := o.run("--release-source", url, "cluster", "update")
	if r.err == nil {
		t.Fatal("cluster update succeeded")
	}
	if strings.Contains(r.err.Error(), "hunter2") {
		t.Errorf("cluster update's error shows the release source's password:\n%v", r.err)
	}
	if got, want := hintArgs(t, "sh", r.err), []string{"-C", o.dir, "cluster", "update"}; !slices.Equal(got, want) {
		t.Errorf("sh reads the hint as %q, want %q", got, want)
	}
}

// An empty --source, as from a variable that isn't set, isn't taken for
// none, which would install the release over the development builds.
func TestAnEmptySourceIsRefused(t *testing.T) {
	o := newOperator(t, "op")
	source := updateCluster(t, o)
	for _, args := range [][]string{{"--source", ""}, {"--source", "", "agent"}} {
		r := o.run(append([]string{"--release-source", source, "cluster", "update"}, args...)...)
		if r.err == nil || !strings.Contains(r.err.Error(), "--source is empty") {
			t.Errorf("cluster update %s: %v, want it refused", strings.Join(args, " "), r.err)
		}
	}
	for name, n := range o.nodes {
		if n.dialed() > 0 {
			t.Errorf("cluster update reached %s", name)
		}
	}
}

// A node that has just restarted says which build it runs only once it
// has hashed it: until then, a source build's version, which names many
// builds, as the same commit built with other settings does too, isn't
// taken for the one the update brings, of a clean checkout or one with
// changes.
func TestASourceBuildIsUpdatedOnANodeThatDoesntSayItsBuildYet(t *testing.T) {
	for name, dirty := range map[string]bool{"a clean checkout": false, "a checkout with changes": true} {
		t.Run(name, func(t *testing.T) {
			o := newOperator(t, "op")
			updateCluster(t, o)
			checkout := devCheckout(t)
			version := commitOf(t, checkout)
			if dirty {
				version += "-dirty"
				writeFile(t, filepath.Join(checkout, "cmd", "agent", "change.go"), []byte("package main\n\nfunc init() { println(\"one\") }\n"))
			}
			o.mustRun("cluster", "update", "--source", checkout, "agent", "dns")
			for _, n := range o.nodes {
				n.mu.Lock()
				for _, c := range n.status.Components {
					c.Build = ""
				}
				for _, c := range n.status.Containers {
					c.Digest = ""
				}
				n.mu.Unlock()
			}
			r := o.mustRun("cluster", "update", "--source", checkout, "agent", "dns")
			for _, name := range []string{"node1", "node2", "node3"} {
				wantContains(t, "the second update's output", r.stdout,
					"Updating the agent on "+name+" to "+version+"\n", "Updating the dns service on "+name+" to "+version+"\n")
			}
		})
	}
}

func TestCtrlCStopsClusterUpdateFromASourceAndGivesBackTheLock(t *testing.T) {
	interrupt := func(ctx context.Context) {
		syscall.Kill(os.Getpid(), syscall.SIGINT)
		<-ctx.Done()
	}
	for name, set := range map[string]func(o *operator){
		"the build": func(o *operator) {
			writeFile(t, filepath.Join(filepath.SplitList(os.Getenv("PATH"))[0], "cargo"), []byte(fmt.Sprintf("#!/bin/sh\nkill -INT %d\nsleep 5\n", os.Getpid())))
		},
		"the preflight": func(o *operator) { o.nodes["node2"].dialing = interrupt },
		"a step":        func(o *operator) { o.nodes["node1"].updating = interrupt },
	} {
		t.Run(name, func(t *testing.T) {
			o := newOperator(t, "op")
			updateCluster(t, o)
			checkout := devCheckout(t)
			set(o)
			r := o.run("cluster", "update", "--source", checkout, "agent", "dns")
			if r.err == nil {
				t.Fatal("cluster update succeeded")
			}
			wantContains(t, "cluster update's error", r.err.Error(), "cluster update interrupted",
				"Run mooctl -C "+o.dir+" cluster update --source "+checkout+" agent dns again: it goes on from there.")
			if got := o.exitCodes(); len(got) > 0 {
				t.Errorf("mooctl exited with %v, want it to return", got)
			}
			for name, n := range o.nodes {
				if l := n.heldLock(); l != nil {
					t.Errorf("after the interrupted update, %s holds the lock %+v", name, l)
				}
			}
			if u := o.nodes["node2"].nodeUpdates(); len(u) > 0 {
				t.Errorf("node2 was updated to %v after the interrupt", u)
			}
		})
	}
}

func TestClusterUpdateReplacesDevelopmentBuildsWithThePinnedRelease(t *testing.T) {
	o := newOperator(t, "op")
	source := updateCluster(t, o)
	o.nodes["node1"].runs(devVersion, "agent")
	o.nodes["node2"].runs(devVersion, "updater")
	o.nodes["node3"].runs("1.1.0", "agent", "updater").runsServices(devVersion, "dns")
	o.mustRun("--release-source", source, "cluster", "update")
	for name, want := range map[string][]string{
		"node1": {"agent 1.1.0", "updater 1.1.0", "containerd 2.1.0", "run service-dns 1.1.0", "run service-proxy 1.1.0"},
		"node2": {"agent 1.1.0", "updater 1.1.0", "containerd 2.1.0", "run service-dns 1.1.0", "run service-proxy 1.1.0"},
		"node3": {"containerd 2.1.0", "run service-dns 1.1.0", "run service-proxy 1.1.0"},
	} {
		if got := updatedTo(o.nodes[name]); !slices.Equal(got, want) {
			t.Errorf("%s was updated to %q, want %q", name, got, want)
		}
	}
}

// A development build of a service the pinned release doesn't have, as
// one a checkout adds, isn't replaced by cluster update, which removes no
// service: status says so, rather than that no update is needed.
func TestClusterStatusSaysADevelopmentBuildThePinnedReleaseLacksStays(t *testing.T) {
	o := newOperator(t, "op")
	source := updateCluster(t, o)
	updatedTo110(o)
	o.nodes["node2"].runsServices(devVersion, "registry")
	r := o.mustRun("--release-source", source, "cluster", "status")
	wantContains(t, "cluster status", r.stdout,
		"Update: development builds run; mooctl cluster update replaces them with the pinned release\n",
		"node2: service-registry: it is a development build of a service the pinned release doesn't have, which cluster update leaves running")
}

func TestClusterStatusMarksDevelopmentBuilds(t *testing.T) {
	o := newOperator(t, "op")
	source := updateCluster(t, o)
	o.nodes["node1"].runs(devVersion, "agent")
	o.nodes["node2"].runsServices(devVersion, "dns")
	r := o.mustRun("--release-source", source, "cluster", "status")
	wantContains(t, "cluster status", r.stdout,
		"\nnode1  direct\n  agent            "+devVersion+" (development build)  1.1.0   healthy\n",
		"\nnode2  direct\n  agent            1.0.0                                   1.1.0   healthy\n    service-dns    "+devVersion+" (development build)  1.1.0   healthy\n")
	if n := strings.Count(r.stdout, "(development build)"); n != 2 {
		t.Errorf("cluster status marks %d development builds, want 2:\n%s", n, r.stdout)
	}
	var view struct {
		Nodes []struct {
			Name                   string
			Components, Containers []struct {
				Name, Running string
				Development   bool
			}
		}
	}
	r = o.mustRun("--release-source", source, "cluster", "status", "--json")
	if err := json.Unmarshal([]byte(r.stdout), &view); err != nil {
		t.Fatal(err)
	}
	var marked []string
	for _, n := range view.Nodes {
		for _, c := range append(n.Components, n.Containers...) {
			if c.Development {
				marked = append(marked, n.Name+" "+c.Name+" "+c.Running)
			}
		}
	}
	if want := []string{"node1 agent " + devVersion, "node2 service-dns " + devVersion}; !slices.Equal(marked, want) {
		t.Errorf("cluster status --json marks %q as development builds, want %q", marked, want)
	}
}
