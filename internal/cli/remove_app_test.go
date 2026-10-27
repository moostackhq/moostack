package cli_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/appfile"
)

// wantRemove runs mooctl deploy --remove of app, piped and on a terminal,
// on fresh nodes from each call of nodes, and checks that each prints
// exactly stdout and, if wantErr is empty, nothing else and succeeds, or
// else fails with exactly wantErr.
func wantRemove(t *testing.T, nodes func() map[string]*fakeNode, app string, stdout, wantErr string) {
	t.Helper()
	wantRemoveBy(t, func(*operator) {}, nodes, app, stdout, wantErr)
}

// wantRemoveBy is wantRemove, run by an operator as edit leaves them.
func wantRemoveBy(t *testing.T, edit func(*operator), nodes func() map[string]*fakeNode, app string, stdout, wantErr string) {
	t.Helper()
	for _, terminal := range []bool{false, true} {
		o := newOperator(t, "op")
		if !terminal {
			o.terminal = nil
		}
		deployCluster(t, o, nodes())
		edit(o)
		r := o.run("deploy", "--remove", app)
		if r.stdout != stdout {
			t.Errorf("on a terminal: %v: deploy --remove printed\n%s\nwant\n%s", terminal, r.stdout, stdout)
		}
		if r.stderr != "" {
			t.Errorf("on a terminal: %v: deploy --remove printed to stderr:\n%s", terminal, r.stderr)
		}
		switch {
		case wantErr == "" && r.err != nil:
			t.Errorf("on a terminal: %v: deploy --remove failed: %v", terminal, r.err)
		case wantErr != "" && (r.err == nil || r.err.Error() != wantErr):
			t.Errorf("on a terminal: %v: deploy --remove says\n%v\nwant\n%s", terminal, r.err, wantErr)
		}
	}
}

// runsApp reports whether n runs the app called name.
func runsApp(n *fakeNode, name string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return slices.ContainsFunc(n.status.Containers, func(k *nodev1.ContainerStatus) bool { return k.Name == appfile.ContainerName(name) })
}

func TestRemoveRemovesTheAppFromItsNodeAndPullsNothing(t *testing.T) {
	var node1, node2 *fakeNode
	wantRemove(t, func() map[string]*fakeNode {
		node1 = pulling(nginxBuild, appRunning("api", "docker.io/library/httpd:2.4", "sha256:aaaa"))
		node2 = pulling(nginxBuild, appRunning("web", nginx, nginxBuild))
		return map[string]*fakeNode{"node1": node1, "node2": node2}
	}, web(t), "Removed web from node2\n", "")
	if runsApp(node2, "web") || !runsApp(node1, "api") {
		t.Errorf("after the removal, node1 runs %v and node2 %v, want web gone and api left", node1.status.Containers, node2.status.Containers)
	}
	if got := node1.status.Containers; len(got) != 1 {
		t.Errorf("node1, which doesn't run web, runs %v once asked to remove it, want api alone", got)
	}
	if got := node1.nodeUpdates(); !slices.Equal(got, []string{"remove app-web"}) {
		t.Errorf("node1, which doesn't run web, was asked %v, want it asked to remove app-web, whatever its status lists", got)
	}
	if len(node2.pulls()) != 0 || len(node2.asked) != 0 {
		t.Errorf("the removal pulled %v and ran %v", node2.pulls(), node2.asked)
	}
}

// An app doubled on two nodes is removed from both, in order of their
// names.
func TestRemoveRemovesTheAppFromEveryNodeThatRunsIt(t *testing.T) {
	var node1, node2 *fakeNode
	wantRemove(t, func() map[string]*fakeNode {
		node1 = pulling(nginxBuild, appRunning("web", nginx, nginxBuild))
		node2 = pulling(nginxBuild, appRunning("web", nginx, nginxBuild))
		return map[string]*fakeNode{"node2": node2, "node1": node1}
	}, web(t), "Removed web from node1\nRemoved web from node2\n", "")
	if runsApp(node1, "web") || runsApp(node2, "web") {
		t.Error("web runs on after it was removed")
	}
}

// An app doubled with another image on each node is refused by deploy,
// as one of the same image is, and removed from both: the app is its
// container's name, whatever it runs.
func TestAnAppDoubledWithTwoImagesIsRefusedAndRemovedFromBoth(t *testing.T) {
	const httpd = "docker.io/library/httpd:2.4"
	doubled := func() map[string]*fakeNode {
		return map[string]*fakeNode{"node1": pulling(nginxBuild, appRunning("web", nginx, nginxBuild)), "node2": pulling(nginxBuild, appRunning("web", httpd, "sha256:aaaa"))}
	}
	wantDeploy(t, doubled, web(t), "", "web runs on node1 and node2, more than one node, so mooctl can't tell which to replace; nothing changed\n"+
		"Run mooctl deploy --remove, which removes web from every node it runs on, then deploy it again")
	var nodes map[string]*fakeNode
	wantRemove(t, func() map[string]*fakeNode {
		nodes = doubled()
		return nodes
	}, web(t), "Removed web from node1\nRemoved web from node2\n", "")
	for name, n := range nodes {
		if runsApp(n, "web") {
			t.Errorf("web runs on %s after it was removed", name)
		}
	}
}

// Removing an app no node runs says so, and succeeds, so that a removal
// is safe to repeat.
func TestRemovingAnAppThatIsntDeployedSaysSoAndSucceeds(t *testing.T) {
	var n *fakeNode
	wantRemove(t, func() map[string]*fakeNode {
		n = pulling(nginxBuild, appRunning("webapp", nginx, nginxBuild))
		return map[string]*fakeNode{"node1": n}
	}, web(t), "web isn't deployed; nothing changed\n", "")
	if !runsApp(n, "webapp") {
		t.Error("removing web removed webapp")
	}
}

// --remove needs only the app file's name: an image or a build, given
// or not, valid or not, isn't used.
func TestRemoveNeedsOnlyTheAppFilesName(t *testing.T) {
	for _, data := range []string{
		"name: web\nimage: nginx:1.27\n",
		"name: web\nbuild: .\n",
		"name: web\n",
		"name: web\nimage: NGINX\n",
	} {
		wantRemove(t, func() map[string]*fakeNode {
			return map[string]*fakeNode{"node1": pulling(nginxBuild, appRunning("web", nginx, nginxBuild))}
		}, appDir(t, data), "Removed web from node1\n", "")
	}
}

func TestRemoveRefusesABadNameBeforeAnyNodeIsContacted(t *testing.T) {
	for data, want := range map[string]string{
		"name: Web\n":           `name: "Web" isn't lowercase letters, digits and single hyphens between them, starting with a letter, at most 59; nothing was contacted`,
		"image: nginx:1.27\n":   "name: is missing; give the app a name, such as web; nothing was contacted",
		"name: web\nport: 80\n": "port: isn't a field of an app file, which has name, image, build; nothing was contacted",
	} {
		dir := appDir(t, data)
		var nodes []*fakeNode
		wantRemove(t, func() map[string]*fakeNode {
			n := pulling(nginxBuild, appRunning("web", nginx, nginxBuild))
			nodes = append(nodes, n)
			return map[string]*fakeNode{"node1": n}
		}, dir, "", dir+"/app.yml: "+want)
		for _, n := range nodes {
			if n.dialed() != 0 {
				t.Errorf("deploy --remove of\n%s reached the node", data)
			}
		}
	}
}

// An empty directory, as an unset variable gives, names no app, rather
// than the current directory's, for a removal and a deploy alike.
func TestAnEmptyAppDirectoryIsRefusedBeforeAnyNodeIsContacted(t *testing.T) {
	for _, args := range [][]string{{"deploy", "--remove", ""}, {"deploy", ""}} {
		o := newOperator(t, "op")
		n := pulling(nginxBuild, appRunning("web", nginx, nginxBuild))
		deployCluster(t, o, map[string]*fakeNode{"node1": n})
		t.Chdir(web(t))
		want := "deploy was given an empty directory; give the app's directory, or nothing for the current one; nothing was contacted"
		if r := o.run(args...); r.err == nil || r.err.Error() != want {
			t.Errorf("%q says %v, want %s", args, r.err, want)
		}
		if n.dialed() != 0 {
			t.Errorf("%q reached the node", args)
		}
	}
}

// A removal goes through the preflight and the lock: one that fails the
// preflight changes nothing, and one that passes takes the lock, gives
// it back, and records no generation.
func TestRemoveGoesThroughThePreflightAndTheLock(t *testing.T) {
	for name, c := range map[string]struct {
		node func() *fakeNode
		want string
	}{
		"unreachable": {func() *fakeNode {
			n := pulling(nginxBuild, appRunning("web", nginx, nginxBuild))
			n.err = errors.New("dial tcp: no route to host")
			return n
		}, "the preflight failed, so nothing changed:\n  node1: dial tcp: no route to host"},
		"a later generation": {func() *fakeNode {
			n := pulling(nginxBuild, appRunning("web", nginx, nginxBuild))
			n.status.Generation = 2
			return n
		}, "the preflight failed, so nothing changed:\n  node1: it has applied generation 2, later than this cluster directory's 1\nPull the latest cluster directory, or pass --force to go on from this one anyway."},
		"locked": {func() *fakeNode { return pulling(nginxBuild, appRunning("web", nginx, nginxBuild)).locked(bobsLock) }, "the preflight failed"},
	} {
		o := newOperator(t, "op")
		n := c.node()
		deployCluster(t, o, map[string]*fakeNode{"node1": n})
		r := o.run("deploy", "--remove", web(t))
		// The lock's refusal names its holder and when it took it, which
		// the lock's own tests check.
		matches := func(got string) bool { return got == c.want }
		if name == "locked" {
			matches = func(got string) bool { return strings.HasPrefix(got, c.want) }
		}
		if r.err == nil || !matches(r.err.Error()) {
			t.Errorf("%s: deploy --remove says %v, want %s", name, r.err, c.want)
		}
		if got := n.nodeUpdates(); len(got) != 0 || !runsApp(n, "web") {
			t.Errorf("%s: the node was changed: %v", name, got)
		}
	}

	o := newOperator(t, "op")
	n := pulling(nginxBuild, appRunning("web", nginx, nginxBuild))
	deployCluster(t, o, map[string]*fakeNode{"node1": n})
	before := snapshot(t, o.dir)
	r := o.mustRun("deploy", "--remove", web(t))
	wantSnapshot(t, o.dir, before)
	if strings.Contains(r.stdout+r.stderr, "commit") {
		t.Errorf("deploy --remove reminded to commit:\n%s%s", r.stdout, r.stderr)
	}
	if taken := n.granted(); len(taken) != 1 || taken[0].Operation != "deploy --remove" {
		t.Errorf("deploy --remove took the locks %+v, want one for deploy --remove", taken)
	}
	if l := n.heldLock(); l != nil {
		t.Errorf("deploy --remove left the lock held: %+v", l)
	}
	if len(n.generations()) != 0 {
		t.Errorf("deploy --remove recorded generations %v", n.generations())
	}
}

// A node whose removal fails stops the removal: what was removed before
// stays printed, and a rerun finishes it.
func TestARemovalThatFailsOnANodeSaysWhatToDo(t *testing.T) {
	var node3 *fakeNode
	wantRemove(t, func() map[string]*fakeNode {
		node2 := pulling(nginxBuild, appRunning("web", nginx, nginxBuild))
		node2.removeErr = status.Error(codes.Internal, "removing app-web: the disk is full")
		node3 = pulling(nginxBuild, appRunning("web", nginx, nginxBuild))
		return map[string]*fakeNode{"node1": pulling(nginxBuild, appRunning("web", nginx, nginxBuild)), "node2": node2, "node3": node3}
	}, web(t), "Removed web from node1\n",
		"node2 failed to remove web: removing app-web: the disk is full; mooctl didn't ask node3\nRun mooctl deploy --remove again: it removes web from every node it is still on")
	if !runsApp(node3, "web") {
		t.Error("node3 removed web after node2 failed")
	}
}

// Ctrl-C while a node removes the app stops mooctl, gives back the lock,
// and says the removal may go on.
func TestCtrlCDuringARemovalSaysItMayGoOn(t *testing.T) {
	var nodes []*fakeNode
	wantRemove(t, func() map[string]*fakeNode {
		n := pulling(nginxBuild, appRunning("web", nginx, nginxBuild))
		n.apps.removing = interrupt
		nodes = append(nodes, n)
		return map[string]*fakeNode{"node1": n}
	}, web(t), "", "mooctl stopped waiting for node1 to remove web, which may go on on the node: context canceled\nRun mooctl deploy --remove again: it says whether web is deployed")
	for _, n := range nodes {
		if l := n.heldLock(); l != nil {
			t.Errorf("deploy --remove stopped kept the lock: %+v", l)
		}
	}
}

// Ctrl-C once a node that didn't keep the app answered, before the
// next, names the nodes not asked, which may run the app, and says what
// to run to remove it from them.
func TestCtrlCBeforeAnyNodeRemovedTheAppSaysToRemoveAgain(t *testing.T) {
	wantRemove(t, func() map[string]*fakeNode {
		node1 := pulling(nginxBuild)
		node1.apps.removed = interrupt
		return map[string]*fakeNode{"node1": node1, "node2": pulling(nginxBuild, appRunning("web", nginx, nginxBuild)), "node3": pulling(nginxBuild, appRunning("web", nginx, nginxBuild))}
	}, web(t), "",
		"mooctl stopped before it asked node2 or node3 to remove web: context canceled; nothing changed there\nRun mooctl deploy --remove again: it removes web from every node it is still on")
}

// Ctrl-C once the app was removed from one node, before the next, says
// what to run to remove it from the rest.
func TestCtrlCBetweenTwoNodesSaysToRemoveAgain(t *testing.T) {
	var node2s []*fakeNode
	wantRemove(t, func() map[string]*fakeNode {
		node1 := pulling(nginxBuild, appRunning("web", nginx, nginxBuild))
		node1.apps.removed = interrupt
		node2 := pulling(nginxBuild, appRunning("web", nginx, nginxBuild))
		node2s = append(node2s, node2)
		return map[string]*fakeNode{"node1": node1, "node2": node2}
	}, web(t), "Removed web from node1\n",
		"mooctl stopped before it asked node2 to remove web: context canceled; nothing changed there\nRun mooctl deploy --remove again: it removes web from every node it is still on")
	for _, n := range node2s {
		if !runsApp(n, "web") || len(n.nodeUpdates()) != 0 {
			t.Errorf("node2 was changed after mooctl stopped: %v", n.nodeUpdates())
		}
	}
}

// stealLock has bob take n's operation lock, as when the operation's lease
// ran out there and he took the lock, and waits for the call whose
// context is ctx to end, as mooctl ends it once it can't be sure it holds
// the lock.
func stealLock(ctx context.Context, n *fakeNode) {
	n.mu.Lock()
	n.lock = &bobsLock
	n.mu.Unlock()
	<-ctx.Done()
}

// A removal that loses the lock while a node removes the app says the
// removal may go on there, and that the lock was lost; one that loses it
// once a node was asked, before any removed the app, says nothing
// changed; and one that loses it once the app was removed from one node
// removes it from no more, and says to remove again.
func TestARemovalThatLosesTheLockSaysSo(t *testing.T) {
	short := func(o *operator) { o.leaseTTL = 600 * time.Millisecond }
	lost := func(node string) string {
		return "the operation lost the operation lock: " + node + ": it is locked by bob on desktop since " +
			bobsLock.Since.Local().Format("2006-01-02 15:04:05 MST") + ", and expires at " + bobsLock.Until.Local().Format("2006-01-02 15:04:05 MST") + " unless renewed"
	}
	wantRemoveBy(t, short, func() map[string]*fakeNode {
		n := pulling(nginxBuild, appRunning("web", nginx, nginxBuild))
		n.apps.removing = func(ctx context.Context) { stealLock(ctx, n) }
		return map[string]*fakeNode{"node1": n}
	}, web(t), "", "mooctl stopped waiting for node1 to remove web, which may go on on the node: "+lost("node1")+"\nRun mooctl deploy --remove again: it says whether web is deployed")
	var befores []*fakeNode
	wantRemoveBy(t, short, func() map[string]*fakeNode {
		node1 := pulling(nginxBuild)
		node2 := pulling(nginxBuild, appRunning("web", nginx, nginxBuild))
		node1.apps.removed = func(ctx context.Context) { stealLock(ctx, node2) }
		befores = append(befores, node2)
		return map[string]*fakeNode{"node1": node1, "node2": node2}
	}, web(t), "", lost("node2")+", before mooctl removed web from any node; nothing changed")
	for _, n := range befores {
		if !runsApp(n, "web") {
			t.Error("node2 removed web once the lock was lost before any removal")
		}
	}
	var node2s []*fakeNode
	wantRemoveBy(t, short, func() map[string]*fakeNode {
		node1 := pulling(nginxBuild, appRunning("web", nginx, nginxBuild))
		node2 := pulling(nginxBuild, appRunning("web", nginx, nginxBuild))
		node1.apps.removed = func(ctx context.Context) { stealLock(ctx, node2) }
		node2s = append(node2s, node2)
		return map[string]*fakeNode{"node1": node1, "node2": node2}
	}, web(t), "Removed web from node1\n", lost("node2")+", so mooctl removed web from no more nodes\nRun mooctl deploy --remove again: it removes web from every node it is still on")
	for _, n := range node2s {
		if !runsApp(n, "web") {
			t.Error("node2 removed web once the lock was lost")
		}
	}
}

// A node that took the call, but whose answer was lost, as when it
// crashed, may have removed the app; one that never got the call didn't;
// and an agent whose containerd doesn't answer says what it changed.
// mooctl can't tell them apart, and says so.
func TestARemovalThatsUnavailableSaysMooctlCantTell(t *testing.T) {
	for _, msg := range []string{
		"error reading from server: EOF",
		"reading the image docker.io/library/nginx:1.27, which the container ran from, changing nothing: connection refused",
	} {
		wantRemove(t, func() map[string]*fakeNode {
			n := pulling(nginxBuild, appRunning("web", nginx, nginxBuild))
			n.removeErr = status.Error(codes.Unavailable, msg)
			return map[string]*fakeNode{"node1": n}
		}, web(t), "", "mooctl can't tell whether node1 removed web: "+msg+"\nRun mooctl deploy --remove again: it says whether web is deployed")
	}
}

// A removal that mooctl can't tell the end of, on a node with nodes
// after it, says which nodes it didn't ask, and that removing again
// removes the app from them too.
func TestARemovalCutOffBeforeTheLastNodeSaysWhichItDidntAsk(t *testing.T) {
	for name, c := range map[string]struct {
		edit func(*fakeNode)
		want string
	}{
		"stopped": {func(n *fakeNode) { n.apps.removing = interrupt },
			"mooctl stopped waiting for node1 to remove web, which may go on on the node: context canceled; mooctl didn't ask node2 or node3"},
		"unavailable": {func(n *fakeNode) { n.removeErr = status.Error(codes.Unavailable, "error reading from server: EOF") },
			"mooctl can't tell whether node1 removed web: error reading from server: EOF; mooctl didn't ask node2 or node3"},
	} {
		t.Run(name, func(t *testing.T) {
			var after []*fakeNode
			wantRemove(t, func() map[string]*fakeNode {
				node1 := pulling(nginxBuild, appRunning("web", nginx, nginxBuild))
				c.edit(node1)
				node2, node3 := pulling(nginxBuild, appRunning("web", nginx, nginxBuild)), pulling(nginxBuild)
				after = append(after, node2, node3)
				return map[string]*fakeNode{"node3": node3, "node1": node1, "node2": node2}
			}, web(t), "", c.want+"\nRun mooctl deploy --remove again: it says whether web is deployed, and removes web from every node it is still on")
			for _, n := range after {
				n.mu.Lock()
				asked := slices.Contains(n.updates, "remove app-web")
				n.mu.Unlock()
				if asked {
					t.Error("mooctl asked a node after the one it couldn't tell the end of")
				}
			}
		})
	}
}

// Run again after a node failed its removal, the removal finishes on that
// node and those after it.
func TestARemovalRunAgainAfterAFailureFinishesIt(t *testing.T) {
	o := newOperator(t, "op")
	node2 := pulling(nginxBuild, appRunning("web", nginx, nginxBuild))
	node2.removeErr = status.Error(codes.Internal, "removing app-web: the disk is full")
	node3 := pulling(nginxBuild, appRunning("web", nginx, nginxBuild))
	deployCluster(t, o, map[string]*fakeNode{"node1": pulling(nginxBuild, appRunning("web", nginx, nginxBuild)), "node2": node2, "node3": node3})
	o.run("deploy", "--remove", web(t))
	node2.mu.Lock()
	node2.removeErr = nil
	node2.mu.Unlock()
	r := o.mustRun("deploy", "--remove", web(t))
	if want := "Removed web from node2\nRemoved web from node3\n"; r.stdout != want {
		t.Errorf("deploy --remove again printed\n%s\nwant\n%s", r.stdout, want)
	}
}

// --force removes past a node that applied a later generation, saying so,
// and records no generation.
func TestRemoveForcedPastALaterGenerationRemovesTheAppAndRecordsNoGeneration(t *testing.T) {
	o := newOperator(t, "op")
	n := pulling(nginxBuild, appRunning("web", nginx, nginxBuild))
	n.status.Generation = 2
	deployCluster(t, o, map[string]*fakeNode{"node1": n})
	r := o.mustRun("deploy", "--remove", "--force", web(t))
	if r.stdout != "Removed web from node1\n" || !strings.Contains(r.stderr, "--force goes on from this cluster directory although:\n  node1: it has applied generation 2, later than this cluster directory's 1\n") {
		t.Errorf("deploy --remove --force printed\n%s\nand to stderr\n%s", r.stdout, r.stderr)
	}
	if len(n.generations()) != 0 {
		t.Errorf("deploy --remove --force recorded generations %v", n.generations())
	}
}

// A node only a later generation lists, which mooctl can't ask, keeps a
// forced removal from claiming the app isn't deployed: it is refused,
// changing nothing.
func TestRemoveForcedPastAnUnlistedNodeChangesNothing(t *testing.T) {
	o := newOperator(t, "op")
	n := pulling(nginxBuild, appRunning("api", nginx, nginxBuild))
	n.status.Generation, n.status.Nodes = 2, []string{"node1", "node2"}
	deployCluster(t, o, map[string]*fakeNode{"node1": n})
	r := o.run("deploy", "--remove", "--force", web(t))
	if want := "generation 2 lists node2, which this cluster directory doesn't, so mooctl can't ask them which apps they run; pull the latest cluster directory; nothing changed"; r.err == nil || r.err.Error() != want || r.stdout != "" {
		t.Errorf("deploy --remove --force past an unlisted node printed\n%s\nand says %v, want\n%s", r.stdout, r.err, want)
	}
	if got := n.nodeUpdates(); len(got) != 0 {
		t.Errorf("the node was changed: %v", got)
	}
}

// --force with a cluster directory later than every node removes the app
// from the nodes it lists, though a node's older generation lists one the
// directory removed since.
func TestRemoveForcedFromADirectoryLaterThanTheNodesRemovesTheApp(t *testing.T) {
	o := newOperator(t, "op")
	n := pulling(nginxBuild, appRunning("web", nginx, nginxBuild))
	n.status.Nodes = []string{"node1", "node2"}
	deployCluster(t, o, map[string]*fakeNode{"node1": n})
	// The directory removed node2 at generation 2, which node1 didn't
	// record.
	file := filepath.Join(o.dir, "cluster.yml")
	data, err := os.ReadFile(file)
	if err != nil || !strings.Contains(string(data), "generation: 1\n") {
		t.Fatalf("cluster.yml: %q, %v", data, err)
	}
	writeFile(t, file, []byte(strings.Replace(string(data), "generation: 1\n", "generation: 2\n", 1)))
	r := o.run("deploy", "--remove", "--force", web(t))
	if r.err != nil || r.stdout != "Removed web from node1\n" || strings.Contains(r.stderr, "node2") {
		t.Errorf("deploy --remove --force from the latest directory: %v, printed\n%s\nand to stderr\n%s", r.err, r.stdout, r.stderr)
	}
}

// --remove=false deploys, and takes the lock as deploy.
func TestRemoveFalseDeploysTheApp(t *testing.T) {
	o := newOperator(t, "op")
	n := pulling(nginxBuild)
	deployCluster(t, o, map[string]*fakeNode{"node1": n})
	r := o.mustRun("deploy", "--remove=false", web(t))
	if !strings.HasSuffix(r.stdout, "web runs docker.io/library/nginx:1.27 on node1\n") {
		t.Errorf("deploy --remove=false printed\n%s\nwant web deployed", r.stdout)
	}
	if taken := n.granted(); len(taken) != 1 || taken[0].Operation != "deploy" {
		t.Errorf("deploy --remove=false took the locks %+v, want one for deploy", taken)
	}
}

// deploy --help says what --remove does.
func TestDeployHelpSaysWhatRemoveDoes(t *testing.T) {
	r := newOperator(t, "op").mustRun("deploy", "--help")
	wantContains(t, "deploy --help", r.stdout,
		"The app runs on the node that runs it already, or else on the node that runs the fewest apps.",
		"--remove removes the app from every node that runs it, with its image unless another container runs from it, and its log; it needs only the app file's name.",
		"--remove    remove the app from every node that runs it, rather than deploy it")
}

func TestRemoveTakesOneDirectoryAtMost(t *testing.T) {
	o := newOperator(t, "op")
	if r := o.run("deploy", "--remove", "a", "b"); r.err == nil || !strings.Contains(r.err.Error(), "deploy takes the app's directory, or nothing for the current one, not 2 arguments") {
		t.Errorf("deploy --remove with two directories: %v", r.err)
	}
}
