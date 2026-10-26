//go:build linux

package orchestrate_test

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

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
	dial := func(_ context.Context, _ string) (orchestrate.Node, error) {
		return fixtureNode{&nodeclient.Node{Agent: n.Agent, Updater: n.Updater}}, nil
	}
	cl, err := orchestrate.Preflight(t.Context(), orchestrate.Nodes{Names: []string{"node1"}, Dial: dial}, 0, true, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	return cl.Deploy(cl.Context(), orchestrate.App{Name: name, Image: ref}, func(orchestrate.DeployStep) {})
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
