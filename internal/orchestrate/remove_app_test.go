package orchestrate_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/appfile"
	"github.com/moostackhq/moostack/internal/orchestrate"
)

// running is a node at generation 1 that runs containers.
func running(containers ...*nodev1.ContainerStatus) *node {
	return &node{generation: 1, others: containers}
}

// removeWeb removes web on cl, and returns the nodes it was removed from,
// the nodes it told of, and why it failed.
func removeWeb(t *testing.T, ctx context.Context, cl *orchestrate.Cluster) ([]string, []string, error) {
	t.Helper()
	var told []string
	removed, err := cl.RemoveApp(ctx, "web", func(node string) { told = append(told, node) })
	return removed, told, err
}

// removals are the containers n was asked to remove.
func removals(n *node) []string {
	var names []string
	for _, u := range n.componentUpdates("") {
		if name, ok := strings.CutPrefix(u.component, "remove "); ok {
			names = append(names, name)
		}
	}
	return names
}

// RemoveApp removes the app from every node that runs it, in order of
// the nodes' names, telling each as it goes, and leaves the rest.
func TestRemoveAppRemovesTheAppFromEveryNodeThatRunsIt(t *testing.T) {
	api := appContainer("api", "docker.io/library/httpd:2.4", "sha256:aaaa")
	node1 := running(appContainer("web", nginx, nginxBuild), api)
	node2 := running(api)
	node3 := running(appContainer("web", nginx, nginxBuild))
	cl := deploying(t, fakes{"node3": node3, "node1": node1, "node2": node2}, nil)
	removed, told, err := removeWeb(t, t.Context(), cl)
	if err != nil || !slices.Equal(removed, []string{"node1", "node3"}) || !slices.Equal(told, removed) {
		t.Fatalf("RemoveApp = %v, %v, told %v, want web removed from node1 and node3, told in that order", removed, err, told)
	}
	for name, n := range map[string]*node{"node1": node1, "node3": node3} {
		if got := removals(n); !slices.Equal(got, []string{"app-web"}) {
			t.Errorf("%s was asked to remove %v, want app-web", name, got)
		}
		if slices.ContainsFunc(n.containers(), func(c *nodev1.ContainerStatus) bool { return c.Name == "app-web" }) {
			t.Errorf("%s still runs app-web", name)
		}
	}
	if got := node2.containers(); len(got) != 1 || got[0].Name != "app-api" {
		t.Errorf("node2, which doesn't run web, runs %v once asked to remove it, want app-api alone", got)
	}
	if !slices.ContainsFunc(node1.containers(), func(c *nodev1.ContainerStatus) bool { return c.Name == "app-api" }) {
		t.Error("removing web removed api from node1")
	}
}

// Removing an app no node runs removes nothing, and isn't an error.
func TestRemoveAppOfAnAppNoNodeRunsRemovesNothing(t *testing.T) {
	n := running(appContainer("api", "docker.io/library/httpd:2.4", "sha256:aaaa"), appContainer("webapp", nginx, nginxBuild))
	cl := deploying(t, fakes{"node1": n}, nil)
	removed, told, err := removeWeb(t, t.Context(), cl)
	if err != nil || len(removed) != 0 || len(told) != 0 {
		t.Errorf("RemoveApp of an app no node runs = %v, %v, told %v, want nothing removed", removed, err, told)
	}
	if got := n.containers(); len(got) != 2 {
		t.Errorf("the node runs %v once asked to remove web, want api and webapp", got)
	}
}

// Every node is asked, and what it says it removed decides, not what its
// status listed: a node that doesn't say what it runs has the app removed
// all the same.
func TestRemoveAppAsksEveryNodeWhateverItsStatus(t *testing.T) {
	n := running(appContainer("web", nginx, nginxBuild))
	cl := deploying(t, fakes{"node1": n}, nil)
	n.mu.Lock()
	n.app.statusErr = errors.New("connection reset by peer")
	n.mu.Unlock()
	removed, told, err := removeWeb(t, t.Context(), cl)
	if err != nil || !slices.Equal(removed, []string{"node1"}) || !slices.Equal(told, removed) {
		t.Errorf("RemoveApp with a node that doesn't answer status = %v, %v, told %v, want web removed from node1", removed, err, told)
	}
}

// A deploy stopped while its run goes on on the node, as the agent's run
// does once it has the call, leaves the app made after a removal read the
// node's status: the removal waits for the run, as the agent's does, and
// removes what it made.
func TestRemoveAppRemovesTheAppADeployStoppedDuringItsRunMakes(t *testing.T) {
	n := withRegistry(&node{}, nginxBuild)
	n.app.runDelay = 300 * time.Millisecond
	cl := deploying(t, fakes{"node1": n}, nil)
	ctx, cancel := context.WithCancel(t.Context())
	n.app.onRun = cancel
	_, err := cl.Deploy(ctx, orchestrate.App{Name: "web", Image: nginx}, func(orchestrate.DeployStep) {})
	var re *orchestrate.RunError
	if !errors.As(err, &re) || !re.Stopped {
		t.Fatalf("Deploy stopped during its run: %v, want a stopped *RunError", err)
	}
	removed, _, err := removeWeb(t, t.Context(), cl)
	if err != nil || !slices.Equal(removed, []string{"node1"}) {
		t.Errorf("RemoveApp once a stopped deploy's run went on = %v, %v, want web removed from node1", removed, err)
	}
	if slices.ContainsFunc(n.containers(), func(c *nodev1.ContainerStatus) bool { return c.Name == "app-web" }) {
		t.Error("the node runs app-web once it was removed")
	}
}

// A node whose removal fails stops the removal there: the nodes before it
// were removed from, and those after it are left for a rerun.
func TestRemoveAppStopsAtANodeWhoseRemovalFails(t *testing.T) {
	node1 := running(appContainer("web", nginx, nginxBuild))
	node2 := running(appContainer("web", nginx, nginxBuild))
	node2.failing, node2.updateErr = "remove app-web", errors.New("the disk is full")
	node3 := running(appContainer("web", nginx, nginxBuild))
	cl := deploying(t, fakes{"node1": node1, "node2": node2, "node3": node3}, nil)
	removed, told, err := removeWeb(t, t.Context(), cl)
	var re *orchestrate.RemoveError
	if !errors.As(err, &re) || re.Node != "node2" || re.App != "web" || re.Stopped || !strings.Contains(err.Error(), "the disk is full") {
		t.Errorf("RemoveApp with node2 failing: %v, want a *RemoveError naming node2 and why", err)
	} else if !slices.Equal(re.Unasked, []string{"node3"}) {
		t.Errorf("RemoveApp with node2 failing says it didn't ask %v, want node3", re.Unasked)
	}
	if !slices.Equal(removed, []string{"node1"}) || !slices.Equal(told, removed) {
		t.Errorf("RemoveApp with node2 failing removed %v, told %v, want node1 alone", removed, told)
	}
	if got := removals(node3); len(got) != 0 {
		t.Errorf("node3 was asked to remove %v after node2 failed", got)
	}
}

// A removal that can't be sure it holds the lock removes from no more
// nodes.
func TestRemoveAppThatLostTheLockRemovesNothing(t *testing.T) {
	n := running(appContainer("web", nginx, nginxBuild))
	cl := deploying(t, fakes{"node1": n}, func(nodes *orchestrate.Nodes) { nodes.LeaseTTL = 300 * time.Millisecond })
	n.lockFor("bobs", time.Hour)
	time.Sleep(400 * time.Millisecond)
	removed, _, err := removeWeb(t, t.Context(), cl)
	if !errors.Is(err, orchestrate.ErrLockLost) || !strings.Contains(err.Error(), "before mooctl removed web from any node; nothing changed") || len(removed) != 0 {
		t.Errorf("RemoveApp that lost the lock = %v, %v, want ErrLockLost, nothing removed, saying so", removed, err)
	}
	if got := removals(n); len(got) != 0 {
		t.Errorf("the node was asked to remove %v once the lock was lost", got)
	}
}

// A removal stopped before it reached a node changes nothing, and says so
// rather than blaming a node.
func TestRemoveAppStoppedBeforeItsFirstRemovalChangesNothing(t *testing.T) {
	n := running(appContainer("web", nginx, nginxBuild))
	cl := deploying(t, fakes{"node1": n}, nil)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, _, err := removeWeb(t, ctx, cl)
	if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "before it changed anything") || errors.As(err, new(*orchestrate.NodeError)) || errors.As(err, new(*orchestrate.RemoveError)) {
		t.Errorf("RemoveApp stopped before it began: %v, want it stopped, nothing changed, naming no node", err)
	}
	if got := removals(n); len(got) != 0 {
		t.Errorf("the node was asked to remove %v", got)
	}
}

// A removal stopped while a node removes the app says the node's removal
// may go on, as the agent's does.
func TestRemoveAppStoppedDuringARemovalSaysItMayGoOn(t *testing.T) {
	n := running(appContainer("web", nginx, nginxBuild))
	n.updateDelay = 200 * time.Millisecond
	cl := deploying(t, fakes{"node1": n}, nil)
	ctx, cancel := context.WithCancel(t.Context())
	n.onUpdateOf = map[string]func(){"remove app-web": cancel}
	removed, told, err := removeWeb(t, ctx, cl)
	var re *orchestrate.RemoveError
	if !errors.As(err, &re) || re.Node != "node1" || !re.Stopped || !errors.Is(err, context.Canceled) {
		t.Errorf("RemoveApp stopped during node1's removal: %v, want a stopped *RemoveError naming node1", err)
	}
	if len(removed) != 0 || len(told) != 0 {
		t.Errorf("RemoveApp stopped during node1's removal removed %v, told %v, want none", removed, told)
	}
}

// A removal stopped once one node removed the app, before the next, sends
// the next nothing, and says nothing changed there, rather than that a
// removal may go on.
func TestRemoveAppStoppedBetweenTwoNodesSendsTheNextNothing(t *testing.T) {
	node1 := running(appContainer("web", nginx, nginxBuild))
	node2 := running(appContainer("web", nginx, nginxBuild))
	cl := deploying(t, fakes{"node1": node1, "node2": node2}, nil)
	ctx, cancel := context.WithCancel(t.Context())
	removed, err := cl.RemoveApp(ctx, "web", func(string) { cancel() })
	var na *orchestrate.NotAskedError
	if !errors.Is(err, context.Canceled) || !errors.As(err, &na) || !slices.Equal(na.Unasked, []string{"node2"}) || errors.As(err, new(*orchestrate.RemoveError)) {
		t.Errorf("RemoveApp stopped between node1 and node2: %v, want a *NotAskedError naming node2", err)
	}
	if want := "mooctl stopped before it asked node2 to remove web: context canceled; nothing changed there"; err.Error() != want {
		t.Errorf("RemoveApp stopped between node1 and node2 says %q, want %q", err, want)
	}
	if !slices.Equal(removed, []string{"node1"}) {
		t.Errorf("RemoveApp stopped between node1 and node2 removed %v, want node1", removed)
	}
	if got := removals(node2); len(got) != 0 {
		t.Errorf("node2 was asked to remove %v after the removal stopped", got)
	}
}

// A removal that loses the lock once one node removed the app sends the
// next nothing, and says it lost the lock.
func TestRemoveAppThatLostTheLockAfterANodeRemovesFromNoMore(t *testing.T) {
	node1 := running(appContainer("web", nginx, nginxBuild))
	node2 := running(appContainer("web", nginx, nginxBuild))
	cl := deploying(t, fakes{"node1": node1, "node2": node2}, func(nodes *orchestrate.Nodes) { nodes.LeaseTTL = 300 * time.Millisecond })
	removed, err := cl.RemoveApp(t.Context(), "web", func(string) {
		node2.lockFor("bobs", time.Hour)
		time.Sleep(400 * time.Millisecond)
	})
	if !errors.Is(err, orchestrate.ErrLockLost) || !slices.Equal(removed, []string{"node1"}) {
		t.Errorf("RemoveApp that lost the lock after node1 = %v, %v, want ErrLockLost, node1 removed", removed, err)
	}
	if got := removals(node2); len(got) != 0 {
		t.Errorf("node2 was asked to remove %v once the lock was lost", got)
	}
}

// A removal, in the operation's context, that loses the lock while a node
// removes the app says the node's removal may go on, as the lost lock's
// doing, and asks no node after it.
func TestRemoveAppThatLostTheLockDuringARemovalSaysItMayGoOn(t *testing.T) {
	node1 := running(appContainer("web", nginx, nginxBuild))
	node1.updateDelay = time.Second
	node2 := running(appContainer("web", nginx, nginxBuild))
	node1.onUpdateOf = map[string]func(){"remove app-web": func() { node2.lockFor("bobs", time.Hour) }}
	cl := deploying(t, fakes{"node1": node1, "node2": node2}, func(nodes *orchestrate.Nodes) { nodes.LeaseTTL = 300 * time.Millisecond })
	removed, err := cl.RemoveApp(cl.Context(), "web", func(string) {})
	var re *orchestrate.RemoveError
	if !errors.As(err, &re) || re.Node != "node1" || !re.Stopped || !errors.Is(err, orchestrate.ErrLockLost) {
		t.Errorf("RemoveApp that lost the lock during node1's removal: %v, want a stopped *RemoveError of ErrLockLost naming node1", err)
	}
	if len(removed) != 0 {
		t.Errorf("RemoveApp that lost the lock during node1's removal removed %v", removed)
	}
	if got := removals(node2); len(got) != 0 {
		t.Errorf("node2 was asked to remove %v once the lock was lost", got)
	}
}

// A removal by another operation, once the stopped deploy gave the lock
// back, removes the app the deploy's run goes on to make: the removal
// waits for the run, which checked the deploy's lease before the lock
// went.
func TestARemovalAfterAStoppedDeployGaveTheLockBackRemovesTheApp(t *testing.T) {
	n := withRegistry(&node{}, nginxBuild)
	n.app.runDelay = 300 * time.Millisecond
	f := fakes{"node1": n}
	nodes := f.nodes()
	nodes.StartCheck = time.Millisecond
	deploy, err := orchestrate.Preflight(t.Context(), nodes, 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	n.app.onRun = cancel
	if _, err := deploy.Deploy(ctx, orchestrate.App{Name: "web", Image: nginx}, func(orchestrate.DeployStep) {}); !errors.As(err, new(*orchestrate.RunError)) {
		t.Fatalf("Deploy stopped during its run: %v, want a *RunError", err)
	}
	deploy.Close()
	remove, err := orchestrate.Preflight(t.Context(), f.nodes(), 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer remove.Close()
	removed, err := remove.RemoveApp(remove.Context(), "web", func(string) {})
	if err != nil || !slices.Equal(removed, []string{"node1"}) {
		t.Errorf("RemoveApp by another operation once the deploy gave the lock back = %v, %v, want web removed from node1", removed, err)
	}
	if slices.ContainsFunc(n.containers(), func(c *nodev1.ContainerStatus) bool { return c.Name == "app-web" }) {
		t.Error("the node runs app-web once it was removed")
	}
}

// BenchmarkRemoveApp removes an app that half the nodes run, on 1, 10
// and 100 nodes, so that how its time grows with the nodes shows.
func BenchmarkRemoveApp(b *testing.B) {
	for _, size := range []int{1, 10, 100} {
		b.Run(fmt.Sprintf("%d nodes", size), func(b *testing.B) {
			f := fakes{}
			for i := range size {
				f[fmt.Sprintf("node%03d", i)] = running()
			}
			cl, err := orchestrate.Preflight(b.Context(), f.nodes(), 1, false, holder)
			if err != nil {
				b.Fatal(err)
			}
			defer cl.Close()
			for b.Loop() {
				b.StopTimer()
				i := 0
				for _, n := range f {
					n.mu.Lock()
					n.others = nil
					if i%2 == 0 {
						n.others = []*nodev1.ContainerStatus{appContainer("web", nginx, nginxBuild)}
					}
					n.mu.Unlock()
					i++
				}
				b.StartTimer()
				if removed, err := cl.RemoveApp(b.Context(), "web", func(string) {}); err != nil || len(removed) != (size+1)/2 {
					b.Fatalf("RemoveApp = %d nodes, %v, want %d", len(removed), err, (size+1)/2)
				}
			}
		})
	}
}

// Random sequences of apps placed on nodes, doubled ones included, and
// removals, some failing at a node, leave the nodes as a model of which
// apps each runs says after each step: a removal that succeeds leaves no
// node running the app; one that fails at a node, whether that node runs
// the app or not, leaves the app on that node and those after it by
// name, and nowhere before it; and no other app is touched.
func TestRemoveAppSequencesMatchTheModel(t *testing.T) {
	seed := stateSeed(t)
	t.Logf("MOOSTACK_STATE_SEED=%d", seed)
	r := rand.New(rand.NewPCG(seed, seed))
	names := []string{"node1", "node2", "node3", "node4"}
	apps := []string{"web", "api", "db"}
	f := fakes{}
	for _, name := range names {
		f[name] = running()
	}
	cl := deploying(t, f, nil)
	model := map[string]map[string]bool{}
	for _, name := range names {
		model[name] = map[string]bool{}
	}
	runs := func(node string) map[string]bool {
		got := map[string]bool{}
		for _, c := range f[node].containers() {
			for _, app := range apps {
				if c.Name == appfile.ContainerName(app) {
					got[app] = true
				}
			}
		}
		return got
	}
	for step := range 200 {
		app := apps[r.IntN(len(apps))]
		if r.IntN(2) == 0 {
			node := names[r.IntN(len(names))]
			if !model[node][app] {
				f[node].mu.Lock()
				f[node].others = append(f[node].others, appContainer(app, nginx, nginxBuild))
				f[node].mu.Unlock()
				model[node][app] = true
			}
			continue
		}
		failing := ""
		if r.IntN(3) == 0 {
			failing = names[r.IntN(len(names))]
		}
		for _, name := range names {
			n := f[name]
			n.mu.Lock()
			n.failing, n.updateErr = "", nil
			if name == failing {
				n.failing, n.updateErr = "remove app-"+app, errors.New("the disk is full")
			}
			n.mu.Unlock()
		}
		removed, err := cl.RemoveApp(t.Context(), app, func(string) {})
		// Every node is asked, in order, so a failing one stops the
		// removal whether it runs the app or not.
		var want []string
		for _, name := range names {
			if name == failing {
				break
			}
			if model[name][app] {
				want = append(want, name)
				delete(model[name], app)
			}
		}
		var re *orchestrate.RemoveError
		if failing != "" {
			if !errors.As(err, &re) || re.Node != failing {
				t.Fatalf("step %d: removing %s with %s failing: %v, want a *RemoveError naming %s", step, app, failing, err, failing)
			}
			if after := names[slices.Index(names, failing)+1:]; !slices.Equal(re.Unasked, after) {
				t.Fatalf("step %d: removing %s with %s failing says it didn't ask %v, want %v", step, app, failing, re.Unasked, after)
			}
		} else if err != nil {
			t.Fatalf("step %d: removing %s: %v", step, app, err)
		}
		if !slices.Equal(removed, want) {
			t.Fatalf("step %d: removing %s with %q failing removed it from %v, want %v", step, app, failing, removed, want)
		}
		for _, name := range names {
			got := runs(name)
			if len(got) != len(model[name]) {
				t.Fatalf("step %d: %s runs %v, want %v", step, name, got, model[name])
			}
			for a := range model[name] {
				if !got[a] {
					t.Fatalf("step %d: %s runs %v, want %v", step, name, got, model[name])
				}
			}
		}
		// A deploy judges where each app runs as the removal does.
		containers := map[string][]*nodev1.ContainerStatus{}
		for _, name := range names {
			containers[name] = f[name].containers()
		}
		for _, a := range apps {
			var on []string
			for _, name := range names {
				if model[name][a] {
					on = append(on, name)
				}
			}
			p, err := orchestrate.Place(a, containers)
			var dup *orchestrate.DuplicateAppError
			switch {
			case len(on) > 1 && (!errors.As(err, &dup) || !slices.Equal(dup.Nodes, on)):
				t.Fatalf("step %d: %s is on %v, and Place says %+v, %v, want a *DuplicateAppError naming them", step, a, on, p, err)
			case len(on) == 1 && (err != nil || p != orchestrate.Placement{Node: on[0], RunsThere: true}):
				t.Fatalf("step %d: %s is on %s, and Place says %+v, %v", step, a, on[0], p, err)
			case len(on) == 0 && (err != nil || p.RunsThere):
				t.Fatalf("step %d: %s is on no node, and Place says %+v, %v", step, a, p, err)
			}
		}
	}
}

// stopAt is a context that ends, as Err says, from its nth call of Err,
// with err, once it ran hook, if set: the call RemoveApp makes as it
// checks whether it was stopped before a node.
type stopAt struct {
	context.Context
	calls atomic.Int32
	n     int32
	err   error
	hook  func()
}

func (c *stopAt) Err() error {
	k := c.calls.Add(1)
	if k == c.n && c.hook != nil {
		c.hook()
	}
	if k >= c.n {
		return c.err
	}
	return nil
}

// A removal stopped before a node, as the lock is lost, says the lock
// was lost, with nothing to run again, whether the lock was found lost
// as the removal stopped, or the operation was stopped for it, though
// the lease was granted again since; one stopped otherwise says which
// nodes it didn't ask.
func TestRemoveAppStoppedAsTheLockIsLostSaysSo(t *testing.T) {
	lost := fmt.Errorf("%w: node1 didn't renew the lease in time", orchestrate.ErrLockLost)
	for name, c := range map[string]struct {
		ttl  time.Duration
		ctx  func(node2 *node) *stopAt
		lost bool
	}{
		// Err's first call is askable's, its second the check before node1.
		"lost as it stopped": {300 * time.Millisecond, func(node2 *node) *stopAt {
			return &stopAt{n: 2, err: context.Canceled, hook: func() {
				node2.lockFor("bobs", time.Hour)
				time.Sleep(400 * time.Millisecond)
			}}
		}, true},
		"stopped for a lock granted again": {0, func(*node) *stopAt { return &stopAt{n: 2, err: lost} }, true},
		"stopped":                          {0, func(*node) *stopAt { return &stopAt{n: 2, err: context.Canceled} }, false},
	} {
		t.Run(name, func(t *testing.T) {
			node1, node2 := running(appContainer("web", nginx, nginxBuild)), running(appContainer("web", nginx, nginxBuild))
			cl := deploying(t, fakes{"node1": node1, "node2": node2}, func(nodes *orchestrate.Nodes) {
				if c.ttl != 0 {
					nodes.LeaseTTL = c.ttl
				}
			})
			ctx := c.ctx(node2)
			ctx.Context = t.Context()
			removed, err := cl.RemoveApp(ctx, "web", func(string) {})
			var na *orchestrate.NotAskedError
			switch {
			case c.lost && (!errors.Is(err, orchestrate.ErrLockLost) || errors.As(err, &na) || !strings.Contains(err.Error(), "before mooctl removed web from any node; nothing changed")):
				t.Errorf("RemoveApp = %v, want it to say it lost the lock, before it removed web from any node", err)
			case !c.lost && (!errors.As(err, &na) || !slices.Equal(na.Unasked, []string{"node1", "node2"})):
				t.Errorf("RemoveApp = %v, want a *NotAskedError naming node1 and node2", err)
			}
			if len(removed) != 0 || len(removals(node1))+len(removals(node2)) != 0 {
				t.Errorf("RemoveApp stopped before node1 removed web from %v", removed)
			}
		})
	}
}
