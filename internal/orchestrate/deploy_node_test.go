//go:build linux

package orchestrate_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/errdefs"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/hostlayout"
	"github.com/moostackhq/moostack/internal/nodeclient"
	"github.com/moostackhq/moostack/internal/nodetest"
	"github.com/moostackhq/moostack/internal/orchestrate"
)

func TestMain(m *testing.M) {
	code := m.Run()
	nodetest.Cleanup()
	os.Exit(code)
}

// deployOn deploys the app called name, running ref, on the agent of n,
// a cluster of one node, with deploy's own start check, and returns
// what the deploy left.
func deployOn(t *testing.T, n *nodetest.Node, name, ref string) (*orchestrate.Deployed, error) {
	t.Helper()
	cl := operateOn(t, n)
	defer cl.Close()
	return cl.Deploy(cl.Context(), orchestrate.App{Name: name, Image: ref}, func(orchestrate.DeployStep) {})
}

// operateOn is n's agent as a cluster of one node, node1, past its
// preflight.
func operateOn(t *testing.T, n *nodetest.Node) *orchestrate.Cluster {
	t.Helper()
	dial := func(_ context.Context, _ string) (orchestrate.Node, error) {
		return fixtureNode{&nodeclient.Node{Agent: n.Agent, Updater: n.Updater}}, nil
	}
	cl, err := orchestrate.Preflight(t.Context(), orchestrate.Nodes{Names: []string{"node1"}, Dial: dial}, 0, true, holder)
	if err != nil {
		t.Fatal(err)
	}
	return cl
}

// removeOn removes the app called name from the agent of n, a cluster of
// one node, and returns the nodes it was removed from.
func removeOn(t *testing.T, n *nodetest.Node, name string) []string {
	t.Helper()
	cl := operateOn(t, n)
	defer cl.Close()
	removed, err := cl.RemoveApp(cl.Context(), name, func(string) {})
	if err != nil {
		t.Fatalf("removing %s: %v", name, err)
	}
	return removed
}

// A deployed app removed from a real agent is gone from its status, and
// its log is empty; its image stays while another app runs from it, and
// goes with the last. Removing it again removes it from no node.
func TestRemoveAppOnANodeDeletesTheAppItsLogAndItsLastImage(t *testing.T) {
	n := nodetest.Start(t)
	r := nodetest.StartRegistry(t, n)
	n.EventuallyHealthy(t)
	ref, _ := r.Push(t, "web", "1", nodetest.Image{Name: "registry.test/web:1"})
	for _, app := range []string{"web", "api"} {
		if _, err := deployOn(t, n, app, ref); err != nil {
			t.Fatalf("deploying %s: %v", app, err)
		}
	}
	webLog := hostlayout.ContainerLogDir + "/app-web.log"
	nodetest.Eventually(t, 30*time.Second, func() error {
		if out, err := n.Output("root", "cat", webLog); err != nil || out == "" {
			return fmt.Errorf("web's log holds %q, %v, once deployed, so an empty one would prove nothing", out, err)
		}
		return nil
	})
	if got := removeOn(t, n, "web"); !slices.Equal(got, []string{"node1"}) {
		t.Errorf("web was removed from %v, want node1", got)
	}
	if appListed(t, n, "app-web") || !appListed(t, n, "app-api") {
		t.Error("once web was removed, the node keeps app-web, or app-api no more")
	}
	if out, err := n.Output("root", "cat", webLog); err != nil || out != "" {
		t.Errorf("the removed app's log holds %q, %v, want it there and empty", out, err)
	}
	if !nodeHasImage(t, n, ref) {
		t.Errorf("%s is gone, although api runs from it", ref)
	}
	removeOn(t, n, "api")
	if nodeHasImage(t, n, ref) {
		t.Errorf("%s is kept, although no app runs from it", ref)
	}
	if got := removeOn(t, n, "web"); len(got) != 0 {
		t.Errorf("removing web again removed it from %v, want none", got)
	}
}

// mooctl stopped while a real agent removes the app, once the agent has
// the call, hears that the removal may go on, which the agent finishes:
// the app goes, and its image with it.
func TestRemoveAppStoppedDuringARemovalOnANodeGoesOnThere(t *testing.T) {
	n := nodetest.Start(t)
	r := nodetest.StartRegistry(t, n)
	n.EventuallyHealthy(t)
	ref, _ := r.Push(t, "web", "1", nodetest.Image{Name: "registry.test/web:1"})
	if _, err := deployOn(t, n, "web", ref); err != nil {
		t.Fatal(err)
	}
	opCtx, stop := context.WithCancel(t.Context())
	defer stop()
	dial := func(context.Context, string) (orchestrate.Node, error) {
		return stopDuringRemove{fixtureNode{&nodeclient.Node{Agent: n.Agent, Updater: n.Updater}}, stop}, nil
	}
	cl, err := orchestrate.Preflight(opCtx, orchestrate.Nodes{Names: []string{"node1"}, Dial: dial}, 0, true, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	_, err = cl.RemoveApp(cl.Context(), "web", func(string) {})
	var re *orchestrate.RemoveError
	if !errors.As(err, &re) || !re.Stopped || !errors.Is(err, context.Canceled) {
		t.Fatalf("RemoveApp stopped during the agent's removal: %v, want a stopped *RemoveError", err)
	}
	nodetest.Eventually(t, 30*time.Second, func() error {
		if appListed(t, n, "app-web") || nodeHasImage(t, n, ref) {
			return fmt.Errorf("once mooctl stopped, the agent still keeps app-web or %s", ref)
		}
		return nil
	})
}

// appListed reports whether n's status lists the container called name.
func appListed(t *testing.T, n *nodetest.Node, name string) bool {
	t.Helper()
	s, err := n.Status(t)
	if err != nil {
		t.Fatal(err)
	}
	return slices.ContainsFunc(s.Containers, func(c *nodev1.ContainerStatus) bool { return c.Name == name })
}

// nodeHasImage reports whether n keeps the image called name.
func nodeHasImage(t *testing.T, n *nodetest.Node, name string) bool {
	t.Helper()
	ctx := namespaces.WithNamespace(t.Context(), hostlayout.ContainerdNamespace)
	_, err := n.Containerd(t).ImageService().Get(ctx, name)
	if err != nil && !errdefs.IsNotFound(err) {
		t.Fatal(err)
	}
	return err == nil
}

// A deploy on a real agent pulls the image from a registry the node
// trusts, runs the app, changes nothing when deployed again unchanged,
// and replaces it with a new build; an app that exits within the start
// check fails the deploy, and one that exits only after it doesn't.
func TestDeployOnANode(t *testing.T) {
	n := nodetest.Start(t)
	r := nodetest.StartRegistry(t, n)
	n.EventuallyHealthy(t)
	ref, first := r.Push(t, "web", "1", nodetest.Image{Name: "registry.test/web:1"})
	d, err := deployOn(t, n, "web", ref)
	if err != nil || *d != (orchestrate.Deployed{Node: "node1", Image: ref}) {
		t.Fatalf("the first deploy: %+v, %v", d, err)
	}
	d, err = deployOn(t, n, "web", ref)
	if err != nil || !d.Unchanged {
		t.Errorf("a deploy again: %+v, %v, want nothing changed", d, err)
	}
	// Another tag of the same build changes nothing: the app runs on under
	// it.
	retag, _ := r.Push(t, "web", "same", nodetest.Image{Name: "registry.test/web:1"})
	pid := appPID(t, n, "app-web")
	d, err = deployOn(t, n, "web", retag)
	if err != nil || !d.Unchanged || d.Image != retag {
		t.Errorf("a deploy of another tag of the same build: %+v, %v, want nothing changed, under %s", d, err, retag)
	}
	if again := appPID(t, n, "app-web"); again != pid {
		t.Errorf("a deploy of another tag of the same build restarted app-web: it ran as %s, and runs as %s", pid, again)
	}
	_, second := r.Push(t, "web", "1", nodetest.Image{Name: "registry.test/web:1", Rebuild: 1})
	d, err = deployOn(t, n, "web", ref)
	if err != nil || d.Unchanged {
		t.Errorf("a deploy of a new build: %+v, %v, want it changed", d, err)
	}
	s, err := n.Status(t)
	if err != nil {
		t.Fatal(err)
	}
	var web *nodev1.ContainerStatus
	for _, c := range s.Containers {
		if c.Name == "app-web" {
			web = c
		}
	}
	if web.GetDigest() != second || web.GetState() != nodev1.TaskState_TASK_STATE_RUNNING || second == first {
		t.Errorf("app-web after the new build: %v, want it running %s", web, second)
	}

	three := 3
	crash, _ := r.Push(t, "crash", "1", nodetest.Image{Name: "registry.test/crash:1", Exit: &three})
	_, err = deployOn(t, n, "crash", crash)
	var nr *orchestrate.NotRunningError
	if !errors.As(err, &nr) || !nr.Exited || nr.ExitCode != 3 || nr.Log != "/var/lib/moostack/logs/app-crash.log" {
		t.Errorf("a deploy of an app that exits at once: %v, want it exited with code 3", err)
	}
	// One that exits a second after it started fails the deploy too: the
	// check waits its time before it asks the node.
	soon, _ := r.Push(t, "soon", "1", nodetest.Image{Name: "registry.test/soon:1", Exit: &three, ExitAfter: time.Second})
	if _, err := deployOn(t, n, "soon", soon); !errors.As(err, &nr) || !nr.Exited || nr.ExitCode != 3 {
		t.Errorf("a deploy of an app that exits a second after it started: %v, want it exited with code 3", err)
	}
	late, _ := r.Push(t, "late", "1", nodetest.Image{Name: "registry.test/late:1", Exit: &three, ExitAfter: 5 * time.Second})
	if _, err := deployOn(t, n, "late", late); err != nil {
		t.Errorf("a deploy of an app that exits after five seconds: %v, want it deployed", err)
	}
}

// appPID is the pid of the one process in the container called name on n,
// as its cgroup holds it.
func appPID(t *testing.T, n *nodetest.Node, name string) string {
	t.Helper()
	out, err := n.Output("root", "sh", "-c", `for p in /proc/[0-9]*; do grep -qx "0::.*$1" $p/cgroup 2>/dev/null && echo ${p#/proc/}; done; true`, "-", hostlayout.CgroupParent+"/"+name)
	if pids := strings.Fields(out); err != nil || len(pids) != 1 {
		t.Fatalf("%s's cgroup holds %q: %v", name, out, err)
	}
	return strings.TrimSpace(out)
}

// deployBuildOn deploys web, built locally as img, on the agent of n, a
// cluster of one node, and returns what the deploy left.
func deployBuildOn(t *testing.T, n *nodetest.Node, img nodetest.Image) (*orchestrate.Deployed, error) {
	t.Helper()
	cl := operateOn(t, n)
	defer cl.Close()
	archive := img.Archive(t)
	app := orchestrate.App{Name: "web", Image: img.Name, Archive: func(string) (io.ReadSeekCloser, error) { return opened(archive), nil }}
	return cl.Deploy(cl.Context(), app, func(orchestrate.DeployStep) {})
}

// A local build deployed to a real agent runs; the same build tagged
// anew changes nothing, its process running on; another build replaces
// it; and a build for another architecture is refused at its import,
// the app running on as it ran.
func TestDeployOfALocalBuildOnANode(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	first := nodetest.Image{Name: "moostack.local/web:20261026T120000Z"}
	if d, err := deployBuildOn(t, n, first); err != nil || d.Unchanged || d.Image != first.Name {
		t.Fatalf("deploying the first build: %+v, %v", d, err)
	}
	before := runningApp(t, n, "app-web")
	pid := appPID(t, n, "app-web")
	retagged := nodetest.Image{Name: "moostack.local/web:20261026T130000Z"}
	if d, err := deployBuildOn(t, n, retagged); err != nil || !d.Unchanged || d.Image != retagged.Name {
		t.Fatalf("deploying the same build, tagged anew: %+v, %v, want it unchanged", d, err)
	}
	if after := runningApp(t, n, "app-web"); after.Image != retagged.Name || after.Digest != before.Digest {
		t.Errorf("once the same build was deployed tagged anew, the node runs %v, want %s of the same build", after, retagged.Name)
	}
	if now := appPID(t, n, "app-web"); now != pid {
		t.Errorf("the same build tagged anew restarted the app: it ran as %s, and runs as %s", pid, now)
	}
	// Each deploy of a build is a new tag: the one before goes.
	if nodeHasImage(t, n, first.Name) {
		t.Errorf("%s is kept once the app runs as %s", first.Name, retagged.Name)
	}
	changed := nodetest.Image{Name: "moostack.local/web:20261026T140000Z", Rebuild: 1}
	d, err := deployBuildOn(t, n, changed)
	if err != nil || d.Unchanged {
		t.Fatalf("deploying a changed build: %+v, %v, want it replaced", d, err)
	}
	replaced := runningApp(t, n, "app-web")
	if replaced.Image != changed.Name || replaced.Digest == before.Digest {
		t.Errorf("once a changed build was deployed, the node runs %v", replaced)
	}
	if nodeHasImage(t, n, retagged.Name) {
		t.Errorf("%s is kept once a changed build replaced it", retagged.Name)
	}
	other := nodetest.Image{Name: "moostack.local/web:20261026T150000Z", Rebuild: 2, Architecture: otherArch()}
	_, err = deployBuildOn(t, n, other)
	var ie *orchestrate.ImportError
	if !errors.As(err, &ie) || !strings.Contains(err.Error(), "the image is for linux/"+otherArch()) {
		t.Fatalf("deploying a build for %s: %v, want an *ImportError saying the node refused it", otherArch(), err)
	}
	if after := runningApp(t, n, "app-web"); after.Image != replaced.Image || after.Digest != replaced.Digest {
		t.Errorf("once a build for another architecture was refused, the node runs %v, want it as it ran", after)
	}
	if nodeHasImage(t, n, other.Name) {
		t.Errorf("the refused build %s is on the node", other.Name)
	}
}

// otherArch is an architecture other than this machine's.
func otherArch() string {
	if runtime.GOARCH == "arm64" {
		return "amd64"
	}
	return "arm64"
}

// runningApp is the container called name, as n's agent reports it once
// it runs.
func runningApp(t *testing.T, n *nodetest.Node, name string) *nodev1.ContainerStatus {
	t.Helper()
	var k *nodev1.ContainerStatus
	nodetest.Eventually(t, 30*time.Second, func() error {
		s, err := n.Status(t)
		if err != nil {
			return err
		}
		for _, c := range s.Containers {
			if c.Name == name && c.State == nodev1.TaskState_TASK_STATE_RUNNING {
				k = c
				return nil
			}
		}
		return fmt.Errorf("%s doesn't run: %v", name, s.Containers)
	})
	return k
}
