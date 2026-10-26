package cli_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/nodeclient"
)

// apps is how a fake node pulls images and runs apps, as the agent does.
type apps struct {
	// registry are the images the node can pull, by reference, each with
	// the digest of its build; pullErr, if set, is why a pull fails.
	registry map[string]string
	pullErr  error
	// pulled are the references the node pulled, in order, and digests the
	// digests of the builds it pulled, by name.
	pulled  []string
	digests map[string]string
	// exits has an app's container run from the image it names exit at
	// once with the code it gives, and fails one fail with the problem it
	// gives.
	exits map[string]uint32
	fails map[string]string
	// runErr, if set, is why running an app fails, stopsOld says the app's
	// old container is gone by then, and agentGone that the agent answers
	// no more status once it failed.
	runErr              error
	stopsOld, agentGone bool
	// leaves, if set, is the app's container a failed run leaves the node
	// running in place of the one before.
	leaves *nodev1.ContainerStatus
	// started says whether the last run started its container's process.
	started bool
	// pulling, running and removing, if set, are called with the call's
	// context as a pull, an app's run or a removal starts, and ran once an
	// app's run is done; removed, with the call's context, once a removal
	// is done.
	pulling, running, removing, removed func(ctx context.Context)
	ran                                 func()
}

// PullImage pulls ref from the node's registry, as the agent does,
// unless its call ended, lease doesn't hold the node's operation lock,
// or pullErr says why it fails; a reference the registry doesn't have
// isn't found.
func (n *fakeNode) PullImage(ctx context.Context, ref, lease string) (nodeclient.PulledImage, error) {
	if n.apps.pulling != nil {
		n.apps.pulling(ctx)
	}
	if err := ctx.Err(); err != nil {
		return nodeclient.PulledImage{}, status.FromContextError(err).Err()
	}
	if l := n.heldLock(); l == nil || l.Lease != lease {
		return nodeclient.PulledImage{}, status.Error(codes.FailedPrecondition, "the operation's lease doesn't hold the node's operation lock")
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.apps.pulled = append(n.apps.pulled, ref)
	if n.apps.pullErr != nil {
		return nodeclient.PulledImage{}, n.apps.pullErr
	}
	digest, ok := n.apps.registry[ref]
	if !ok {
		return nodeclient.PulledImage{}, status.Errorf(codes.NotFound, "%s: not found", ref)
	}
	if n.apps.digests == nil {
		n.apps.digests = map[string]string{}
	}
	n.apps.digests[ref] = digest
	return nodeclient.PulledImage{Name: ref, Digest: digest}, nil
}

// runApp runs the app's container c as the agent does, from an image the
// node pulled, refusing one with properties other than its name and
// image, as no app runs with, and replaces the container of its name,
// running unless exits or fails say otherwise. n.mu is held.
func (n *fakeNode) runApp(c *nodev1.Container) error {
	n.asked = append(n.asked, c)
	n.updates = append(n.updates, "run "+c.Name+" "+c.Image)
	if !proto.Equal(c, &nodev1.Container{Name: c.Name, Image: c.Image}) {
		return status.Errorf(codes.InvalidArgument, "%s has properties other than its name and image: %v", c.Name, c)
	}
	digest, ok := n.apps.digests[c.Image]
	if !ok {
		return status.Errorf(codes.FailedPrecondition, "the node has no image %s", c.Image)
	}
	if n.apps.runErr != nil {
		if n.apps.stopsOld || n.apps.leaves != nil {
			n.status.Containers = without(n.status.Containers, c.Name)
		}
		if n.apps.leaves != nil {
			n.status.Containers = append(n.status.Containers, n.apps.leaves)
		}
		if n.apps.agentGone {
			n.status.AgentErr = errors.New("connection reset by peer")
		}
		return n.apps.runErr
	}
	k := &nodev1.ContainerStatus{Name: c.Name, Image: c.Image, Digest: digest, State: nodev1.TaskState_TASK_STATE_RUNNING}
	// A running container of the same build takes the new name in place.
	if i := slices.IndexFunc(n.status.Containers, func(o *nodev1.ContainerStatus) bool { return o.Name == c.Name }); i >= 0 &&
		n.status.Containers[i].State == nodev1.TaskState_TASK_STATE_RUNNING && n.status.Containers[i].Digest == digest {
		n.status.Containers[i].Image = c.Image
		n.apps.started = false
		return nil
	}
	if code, ok := n.apps.exits[c.Image]; ok {
		k.State, k.ExitCode = nodev1.TaskState_TASK_STATE_EXITED, code
	}
	if problem, ok := n.apps.fails[c.Image]; ok {
		k.State, k.Problem = nodev1.TaskState_TASK_STATE_FAILED, problem
	}
	n.status.Containers = append(without(n.status.Containers, c.Name), k)
	return nil
}

// pulls are the references the node pulled so far.
func (n *fakeNode) pulls() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return slices.Clone(n.apps.pulled)
}

// The images the deploy tests pull.
const (
	nginx      = "docker.io/library/nginx:1.27"
	nginxBuild = "sha256:1111"
	newBuild   = "sha256:2222"
)

// appDir writes data as the app file of a new app directory, and returns
// the directory.
func appDir(t *testing.T, data string) string {
	t.Helper()
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "app.yml"), []byte(data))
	return dir
}

// web is the app directory of web, running nginx:1.27.
func web(t *testing.T) string { return appDir(t, "name: web\nimage: nginx:1.27\n") }

// pulling is a healthy node, at generation 1, that can pull nginx at
// build, running containers.
func pulling(build string, containers ...*nodev1.ContainerStatus) *fakeNode {
	n := healthyNode(1)
	n.status.Components = upToDate()
	n.status.Containers = containers
	n.apps.registry = map[string]string{nginx: build}
	return n
}

// appRunning is the container of the app called name, running image of
// the build digest.
func appRunning(name, image, digest string) *nodev1.ContainerStatus {
	return &nodev1.ContainerStatus{Name: "app-" + name, Image: image, Digest: digest, State: nodev1.TaskState_TASK_STATE_RUNNING}
}

// deployCluster is the operator's cluster directory, of nodes, each
// answered by its fake, and deploys that check an app at once.
func deployCluster(t *testing.T, o *operator, nodes map[string]*fakeNode) {
	t.Helper()
	statusCluster(t, o, slices.Sorted(func(yield func(string) bool) {
		for name := range nodes {
			if !yield(name) {
				return
			}
		}
	})...)
	o.nodes = nodes
	o.startCheck = time.Millisecond
}

// wantDeploy runs mooctl deploy with args, piped and on a terminal, on
// fresh nodes from each call of nodes, and checks that each prints
// exactly stdout and, if wantErr is empty, nothing else and succeeds, or
// else fails with exactly wantErr.
func wantDeploy(t *testing.T, nodes func() map[string]*fakeNode, app string, stdout, wantErr string) {
	t.Helper()
	for _, terminal := range []bool{false, true} {
		o := newOperator(t, "op")
		if !terminal {
			o.terminal = nil
		}
		deployCluster(t, o, nodes())
		r := o.run("deploy", app)
		if r.stdout != stdout {
			t.Errorf("on a terminal: %v: deploy printed\n%s\nwant\n%s", terminal, r.stdout, stdout)
		}
		if r.stderr != "" {
			t.Errorf("on a terminal: %v: deploy printed to stderr:\n%s", terminal, r.stderr)
		}
		switch {
		case wantErr == "" && r.err != nil:
			t.Errorf("on a terminal: %v: deploy failed: %v", terminal, r.err)
		case wantErr != "" && (r.err == nil || r.err.Error() != wantErr):
			t.Errorf("on a terminal: %v: deploy says\n%v\nwant\n%s", terminal, r.err, wantErr)
		}
	}
}

func TestDeployRunsANewAppOnTheNodeWithTheFewestApps(t *testing.T) {
	var node2 *fakeNode
	wantDeploy(t, func() map[string]*fakeNode {
		node2 = pulling(nginxBuild)
		return map[string]*fakeNode{"node1": pulling(nginxBuild, appRunning("api", "docker.io/library/httpd:2.4", "sha256:aaaa")), "node2": node2}
	}, web(t), `web: node2 (fewest apps)
Pulling docker.io/library/nginx:1.27 on node2
web runs docker.io/library/nginx:1.27 on node2
`, "")
	if got := node2.asked; len(got) != 1 || !proto.Equal(got[0], &nodev1.Container{Name: "app-web", Image: nginx}) {
		t.Errorf("node2 was asked to run %v, want app-web from %s alone", got, nginx)
	}
}

func TestARedeployOfTheSameBuildSaysNothingChanged(t *testing.T) {
	var node2 *fakeNode
	wantDeploy(t, func() map[string]*fakeNode {
		node2 = pulling(nginxBuild, appRunning("web", nginx, nginxBuild))
		return map[string]*fakeNode{"node1": pulling(nginxBuild), "node2": node2}
	}, web(t), `web: node2 (runs there)
Pulling docker.io/library/nginx:1.27 on node2
web runs docker.io/library/nginx:1.27 on node2 already; nothing changed
`, "")
	if len(node2.asked) != 0 {
		t.Errorf("node2 was asked to run %v, want nothing", node2.asked)
	}
}

func TestARedeployOfAnotherBuildReplacesTheAppInPlace(t *testing.T) {
	var node2 *fakeNode
	wantDeploy(t, func() map[string]*fakeNode {
		node2 = pulling(newBuild, appRunning("web", nginx, nginxBuild))
		return map[string]*fakeNode{"node1": pulling(newBuild), "node2": node2}
	}, web(t), `web: node2 (runs there)
Pulling docker.io/library/nginx:1.27 on node2
web runs docker.io/library/nginx:1.27 on node2
`, "")
	if len(node2.asked) != 1 || node2.status.Containers[0].Digest != newBuild {
		t.Errorf("node2 was asked to run %v, and runs %v, want web replaced by the new build", node2.asked, node2.status.Containers)
	}
}

// A redeploy with a new tag names the image by its full reference, and
// replaces the app where it runs.
func TestARedeployOfANewTagReplacesTheAppInPlace(t *testing.T) {
	wantDeploy(t, func() map[string]*fakeNode {
		n := pulling(nginxBuild, appRunning("web", nginx, nginxBuild))
		n.apps.registry["docker.io/library/nginx:1.28"] = newBuild
		return map[string]*fakeNode{"node1": pulling(nginxBuild), "node2": n}
	}, appDir(t, "name: web\nimage: nginx:1.28\n"), `web: node2 (runs there)
Pulling docker.io/library/nginx:1.28 on node2
web runs docker.io/library/nginx:1.28 on node2
`, "")
}

func TestAFailedPullSaysWhyAndThatNothingChanged(t *testing.T) {
	const pulled = "web: node1 (runs there)\nPulling docker.io/library/nginx:1.27 on node1\n"
	for name, c := range map[string]struct {
		err  error
		want string
	}{
		"not found": {status.Error(codes.NotFound, "docker.io/library/nginx:1.27: not found"),
			"node1 couldn't pull docker.io/library/nginx:1.27: the registry doesn't have it; check its name and tag\nThe node said: docker.io/library/nginx:1.27: not found\nNothing changed."},
		"access denied": {status.Error(codes.PermissionDenied, "pull access denied"),
			"node1 couldn't pull docker.io/library/nginx:1.27: the registry denied access to it, which Docker Hub answers for a private repository and for one that doesn't exist alike; check its name, and that it is public\nThe node said: pull access denied\nNothing changed."},
		"rate limited": {status.Error(codes.ResourceExhausted, "registry-1.docker.io answered 429 Too Many Requests"),
			"node1 couldn't pull docker.io/library/nginx:1.27: the registry rate limits node1; Docker Hub allows 100 anonymous pulls from each IPv4 address or IPv6 /64 within a window of time, which the nodes behind one address share; wait, then deploy again\nThe node said: registry-1.docker.io answered 429 Too Many Requests\nNothing changed."},
		"network": {status.Error(codes.Unavailable, "dial tcp: lookup registry-1.docker.io: no such host"),
			"node1 couldn't pull docker.io/library/nginx:1.27: node1 can't reach the registry, or the registry answers that it is down; check node1's network, or try again later\nThe node said: dial tcp: lookup registry-1.docker.io: no such host\nNothing changed."},
		"another platform": {status.Error(codes.FailedPrecondition, "the image is for linux/arm64, not for this node's linux/amd64"),
			"node1 couldn't pull docker.io/library/nginx:1.27: the image is for linux/arm64, not for this node's linux/amd64; nothing changed"},
		// A node of the release before has no PullImage.
		"an agent without PullImage": {status.Error(codes.Unimplemented, "unknown method PullImage for service moostack.node.v1.Agent"),
			"node1 can't pull images: its agent is of a release before deploy; update the cluster with mooctl cluster update; nothing changed"},
	} {
		t.Run(name, func(t *testing.T) {
			var n *fakeNode
			wantDeploy(t, func() map[string]*fakeNode {
				n = pulling(newBuild, appRunning("web", nginx, nginxBuild))
				n.apps.pullErr = c.err
				return map[string]*fakeNode{"node1": n}
			}, web(t), pulled, c.want)
			if len(n.asked) != 0 || n.status.Containers[0].Digest != nginxBuild {
				t.Errorf("after a failed pull, the node was asked to run %v and runs %v, want web as it ran", n.asked, n.status.Containers)
			}
		})
	}
}

func TestAFailedRunSaysWhetherTheAppStillRuns(t *testing.T) {
	const pulled = "web: node1 (runs there)\nPulling docker.io/library/nginx:1.27 on node1\n"
	const failed = "node1 failed to run web: running app-web: the disk is full\n"
	for _, c := range []struct {
		stopsOld, agentGone bool
		leaves              *nodev1.ContainerStatus
		want                string
	}{
		{false, false, nil, failed + "web still runs on node1 as it ran before."},
		{true, false, nil, failed + "web is not running on node1."},
		{false, true, nil, failed + "mooctl can't tell whether web runs on node1."},
		{false, false, &nodev1.ContainerStatus{Name: "app-web", State: nodev1.TaskState_TASK_STATE_UNSPECIFIED}, failed + "mooctl can't tell whether web runs on node1."},
		{false, false, appRunning("web", nginx, newBuild), failed + "web runs docker.io/library/nginx:1.27 on node1 all the same, as the run asked."},
		{false, false, appRunning("web", "docker.io/library/nginx:1.26", "sha256:3333"), failed + "web runs docker.io/library/nginx:1.26 on node1, which neither ran before nor was asked for."},
	} {
		wantDeploy(t, func() map[string]*fakeNode {
			n := pulling(newBuild, appRunning("web", nginx, nginxBuild))
			n.apps.runErr = status.Error(codes.Internal, "running app-web: the disk is full")
			n.apps.stopsOld, n.apps.agentGone, n.apps.leaves = c.stopsOld, c.agentGone, c.leaves
			return map[string]*fakeNode{"node1": n}
		}, web(t), pulled, c.want)
	}
}

func TestAnAppThatExitsAsItStartsFailsTheDeployWithItsCodeAndLog(t *testing.T) {
	wantDeploy(t, func() map[string]*fakeNode {
		n := pulling(nginxBuild)
		n.apps.exits = map[string]uint32{nginx: 3}
		return map[string]*fakeNode{"node1": n}
	}, web(t), "web: node1 (fewest apps)\nPulling docker.io/library/nginx:1.27 on node1\n",
		"web exited with code 3 on node1 within 1ms of starting; its output is in /var/lib/moostack/logs/app-web.log on node1")
	wantDeploy(t, func() map[string]*fakeNode {
		n := pulling(nginxBuild)
		n.apps.fails = map[string]string{nginx: "starting it: exec format error"}
		return map[string]*fakeNode{"node1": n}
	}, web(t), "web: node1 (fewest apps)\nPulling docker.io/library/nginx:1.27 on node1\n",
		"web doesn't run on node1 once started: starting it: exec format error")
}

// mooctl waits three seconds before it checks an app by default.
func TestDeployChecksTheAppThreeSecondsAfterItStarted(t *testing.T) {
	o := newOperator(t, "op")
	deployCluster(t, o, map[string]*fakeNode{"node1": pulling(nginxBuild)})
	o.startCheck = 0
	start := time.Now()
	o.mustRun("deploy", web(t))
	if took := time.Since(start); took < 3*time.Second {
		t.Errorf("deploy took %s, less than the three seconds it waits", took)
	}
}

func TestADeployOfAnAppOnTwoNodesChangesNothing(t *testing.T) {
	wantDeploy(t, func() map[string]*fakeNode {
		return map[string]*fakeNode{"node1": pulling(nginxBuild, appRunning("web", nginx, nginxBuild)), "node2": pulling(nginxBuild, appRunning("web", nginx, nginxBuild))}
	}, web(t), "", "web runs on node1 and node2, more than one node, so mooctl can't tell which to replace; nothing changed\nRun mooctl deploy --remove, which removes web from every node it runs on, then deploy it again")
}

func TestABadAppFileIsRefusedBeforeAnyNodeIsContacted(t *testing.T) {
	for data, want := range map[string]string{
		"name: Web\nimage: nginx:1.27\n":           `name: "Web" isn't lowercase letters, digits and single hyphens between them, starting with a letter, at most 59; nothing was contacted`,
		"name: web\nbuild: .\n":                    "build: isn't supported yet; name a public image in image; nothing was contacted",
		"name: web\nimage: nginx:1.27\nport: 80\n": "port: isn't a field of an app file, which has name, image, build; nothing was contacted",
		"name: web\nimage: NGINX\n":                "image: \"NGINX\" isn't an image's reference: invalid reference format: repository name (library/NGINX) must be lowercase; nothing was contacted",
	} {
		o := newOperator(t, "op")
		n := pulling(nginxBuild)
		deployCluster(t, o, map[string]*fakeNode{"node1": n})
		dir := appDir(t, data)
		r := o.run("deploy", dir)
		if r.err == nil || r.err.Error() != filepath.Join(dir, "app.yml")+": "+want {
			t.Errorf("deploy of\n%s= %v, want %s: %s", data, r.err, filepath.Join(dir, "app.yml"), want)
		}
		if n.dialed() != 0 {
			t.Errorf("deploy of\n%s reached the node", data)
		}
	}
	o := newOperator(t, "op")
	r := o.run("deploy", t.TempDir())
	if r.err == nil || !strings.Contains(r.err.Error(), "there is no app file; run mooctl deploy in the app's directory, or give it the directory; nothing was contacted") {
		t.Errorf("deploy without an app file: %v", r.err)
	}
}

// Deploy reads app.yml in the current directory when given none, and
// finds the cluster directory by MOOCTL_DIR when -C doesn't name it.
func TestDeployRunsFromTheAppsDirectoryWithTheClusterInMOOCTL_DIR(t *testing.T) {
	o := newOperator(t, "op")
	deployCluster(t, o, map[string]*fakeNode{"node1": pulling(nginxBuild)})
	t.Chdir(web(t))
	t.Setenv("MOOCTL_DIR", o.dir)
	r := o.runWithoutDir("deploy")
	if r.err != nil || !strings.HasSuffix(r.stdout, "web runs docker.io/library/nginx:1.27 on node1\n") {
		t.Errorf("deploy from the app's directory, with MOOCTL_DIR: %v\n%s", r.err, r.stdout)
	}
}

// Every command finds the cluster directory by MOOCTL_DIR, and one that
// finds none there says to name it.
func TestMOOCTL_DIRPointsEveryCommandAtTheClusterDirectory(t *testing.T) {
	o := newOperator(t, "op")
	deployCluster(t, o, map[string]*fakeNode{"node1": pulling(nginxBuild)})
	t.Chdir(t.TempDir())
	t.Setenv("MOOCTL_DIR", o.dir)
	if r := o.runWithoutDir("cluster", "status", "--json"); r.err != nil || !strings.Contains(r.stdout, `"cluster": "prod"`) {
		t.Errorf("cluster status with MOOCTL_DIR: %v\n%s", r.err, r.stdout)
	} else if want := "Cluster directory: " + o.dir + ", from MOOCTL_DIR\n"; !strings.HasPrefix(r.stderr, want) || strings.Count(r.stderr, "Cluster directory") != 1 {
		t.Errorf("cluster status with MOOCTL_DIR printed\n%s\nwant it to start with, once\n%s", r.stderr, want)
	}
	if r := o.run("cluster", "unlock"); r.err != nil || strings.Contains(r.stderr, "Cluster directory") {
		t.Errorf("cluster unlock with -C and MOOCTL_DIR: %v\n%s", r.err, r.stderr)
	}
	// Set but empty, it names the current directory, as -C . does.
	t.Chdir(o.dir)
	t.Setenv("MOOCTL_DIR", "")
	if r := o.runWithoutDir("cluster", "status", "--json"); r.err != nil || !strings.Contains(r.stdout, `"cluster": "prod"`) {
		t.Errorf("cluster status with MOOCTL_DIR empty, in the cluster directory: %v\n%s", r.err, r.stdout)
	}
	empty := t.TempDir()
	t.Setenv("MOOCTL_DIR", empty)
	t.Chdir(t.TempDir())
	// -C names the cluster directory over MOOCTL_DIR.
	if r := o.run("cluster", "status", "--json"); r.err != nil || !strings.Contains(r.stdout, `"cluster": "prod"`) {
		t.Errorf("cluster status with -C, and MOOCTL_DIR naming another directory: %v\n%s", r.err, r.stdout)
	}
	for _, args := range [][]string{{"deploy", web(t)}, {"cluster", "status"}, {"cluster", "update"}, {"cluster", "update", "--source", t.TempDir()},
		{"node", "add", "web.invalid"}, {"node", "add", "--join-token", joinToken, "web.invalid"}, {"node", "remove", "node1"}, {"cluster", "unlock"}} {
		r := o.runWithoutDir(args...)
		want := empty + " is not a cluster directory, since it has no cluster.yml: open " + filepath.Join(empty, "cluster.yml") + ": no such file or directory\nPass -C <cluster dir>, or set MOOCTL_DIR to the cluster directory."
		if r.err == nil || r.err.Error() != want {
			t.Errorf("mooctl %s without a cluster file: %v, want\n%s", strings.Join(args, " "), r.err, want)
		}
	}
}

// A current directory with a cluster file goes before MOOCTL_DIR, and
// mooctl says which it acts on, once, so that a MOOCTL_DIR left exported
// doesn't take a command run in another cluster's checkout elsewhere.
func TestTheCurrentClusterDirectoryGoesBeforeMOOCTL_DIR(t *testing.T) {
	o := newOperator(t, "op")
	deployCluster(t, o, map[string]*fakeNode{"node1": pulling(nginxBuild)})
	staging := t.TempDir()
	t.Chdir(staging)
	if r := o.runWithoutDir("cluster", "init", "--ssh-key", o.key, "--name", "staging"); r.err != nil {
		t.Fatalf("cluster init of staging: %v", r.err)
	}
	t.Setenv("MOOCTL_DIR", staging)
	t.Chdir(o.dir)
	r := o.runWithoutDir("cluster", "status", "--json")
	if r.err != nil || !strings.Contains(r.stdout, `"cluster": "prod"`) {
		t.Errorf("cluster status in prod's directory, with MOOCTL_DIR naming staging's: %v\n%s", r.err, r.stdout)
	}
	want := "Cluster directory: the current directory, which has a cluster.yml, not MOOCTL_DIR's " + staging + "\n"
	if !strings.HasPrefix(r.stderr, want) || strings.Count(r.stderr, "Cluster directory") != 1 {
		t.Errorf("cluster status in prod's directory printed\n%s\nwant it to start with, once\n%s", r.stderr, want)
	}
	// The secret commands read the cluster directory more than once, and
	// say which it is once.
	r = o.runWithoutDir("secret", "recipients", "ls", "--ssh-key", o.key)
	if r.err != nil || strings.Count(r.stderr, "Cluster directory") != 1 {
		t.Errorf("secret recipients ls in prod's directory: %v\n%s", r.err, r.stderr)
	}
	// MOOCTL_DIR naming the current directory itself says it is MOOCTL_DIR's.
	t.Setenv("MOOCTL_DIR", o.dir)
	if r := o.runWithoutDir("cluster", "status", "--json"); !strings.HasPrefix(r.stderr, "Cluster directory: "+o.dir+", from MOOCTL_DIR\n") {
		t.Errorf("cluster status in the directory MOOCTL_DIR names printed\n%s", r.stderr)
	}
}

// Where an app runs is kept on the nodes alone: deploy writes nothing to
// the cluster directory, records no generation, and doesn't remind the
// operator to commit.
func TestDeployDoesntChangeTheClusterDirectory(t *testing.T) {
	o := newOperator(t, "op")
	n := pulling(nginxBuild)
	deployCluster(t, o, map[string]*fakeNode{"node1": n})
	before := snapshot(t, o.dir)
	r := o.mustRun("deploy", web(t))
	wantSnapshot(t, o.dir, before)
	if len(n.generations()) != 0 {
		t.Errorf("deploy recorded generations %v", n.generations())
	}
	if strings.Contains(r.stdout+r.stderr, "commit") {
		t.Errorf("deploy reminded to commit:\n%s%s", r.stdout, r.stderr)
	}
	if l := n.heldLock(); l != nil {
		t.Errorf("deploy left the lock held: %+v", l)
	}
	if taken := n.granted(); len(taken) != 1 || taken[0].Operation != "deploy" {
		t.Errorf("deploy took the locks %+v, want one for deploy", taken)
	}
}

// A deploy goes through the preflight: a node it can't reach, or that
// applied a later generation, or whose lock another operation holds,
// changes nothing.
func TestADeployThatFailsThePreflightChangesNothing(t *testing.T) {
	for name, c := range map[string]struct {
		node func() *fakeNode
		want string
	}{
		"unreachable": {func() *fakeNode { n := pulling(nginxBuild); n.err = errors.New("dial tcp: no route to host"); return n },
			"the preflight failed, so nothing changed:\n  node1: dial tcp: no route to host"},
		"a later generation": {func() *fakeNode { n := pulling(nginxBuild); n.status.Generation = 2; return n },
			"the preflight failed, so nothing changed:\n  node1: it has applied generation 2, later than this cluster directory's 1\nPull the latest cluster directory, or pass --force to go on from this one anyway."},
		"a generation it can't tell": {func() *fakeNode { n := pulling(nginxBuild); n.status.GenerationUnknown = "unreadable"; return n },
			"the preflight failed, so nothing changed:\n  node1: it can't tell the generation it has applied: unreadable\nIf this cluster directory is the latest, pass --force to go on from it anyway; the next operation that writes it has every node record its generation."},
		"locked": {func() *fakeNode { return pulling(nginxBuild).locked(bobsLock) }, "the preflight failed"},
	} {
		o := newOperator(t, "op")
		n := c.node()
		deployCluster(t, o, map[string]*fakeNode{"node1": n})
		r := o.run("deploy", web(t))
		if r.err == nil || !strings.HasPrefix(r.err.Error(), c.want) {
			t.Errorf("%s: deploy says %v, want %s", name, r.err, c.want)
		}
		if len(n.pulls()) != 0 || len(n.asked) != 0 {
			t.Errorf("%s: the node pulled %v and was asked to run %v", name, n.pulls(), n.asked)
		}
	}
}

// --force deploys past a node that applied a later generation, saying so,
// and records no generation on it.
func TestDeployForcedPastALaterGenerationRunsTheAppAndRecordsNoGeneration(t *testing.T) {
	o := newOperator(t, "op")
	n := pulling(nginxBuild)
	n.status.Generation = 2
	deployCluster(t, o, map[string]*fakeNode{"node1": n})
	r := o.mustRun("deploy", "--force", web(t))
	if got := n.asked; len(got) != 1 || !proto.Equal(got[0], &nodev1.Container{Name: "app-web", Image: nginx}) {
		t.Errorf("node1 was asked to run %v, want app-web from %s", got, nginx)
	}
	if want := "--force goes on from this cluster directory although:\n  node1: it has applied generation 2, later than this cluster directory's 1\n"; !strings.Contains(r.stderr, want) {
		t.Errorf("deploy --force printed\n%s\nwant it to contain\n%s", r.stderr, want)
	}
	if len(n.generations()) != 0 {
		t.Errorf("deploy --force recorded generations %v", n.generations())
	}
}

// --force past a node that applied a generation listing a node this
// cluster directory doesn't has deploy refuse, rather than place the app
// without asking that node what it runs.
func TestDeployForcedPastAnUnlistedNodeChangesNothing(t *testing.T) {
	o := newOperator(t, "op")
	n := pulling(nginxBuild)
	n.status.Generation, n.status.Nodes = 2, []string{"node1", "node2"}
	deployCluster(t, o, map[string]*fakeNode{"node1": n})
	r := o.run("deploy", "--force", web(t))
	if want := "generation 2 lists node2, which this cluster directory doesn't, so mooctl can't ask them which apps they run; pull the latest cluster directory; nothing changed"; r.err == nil || r.err.Error() != want || r.stdout != "" {
		t.Errorf("deploy --force past an unlisted node printed\n%s\nand says %v, want\n%s", r.stdout, r.err, want)
	}
	if len(n.pulls()) != 0 || len(n.asked) != 0 {
		t.Errorf("the node pulled %v and was asked to run %v", n.pulls(), n.asked)
	}
}

func TestDeployTakesOneDirectoryAtMost(t *testing.T) {
	o := newOperator(t, "op")
	if r := o.run("deploy", "a", "b"); r.err == nil || !strings.Contains(r.err.Error(), "deploy takes the app's directory, or nothing for the current one, not 2 arguments") {
		t.Errorf("deploy with two directories: %v", r.err)
	}
}

// The fake node refuses an app's run as the agent does, so that the tests
// that use it fail when mooctl runs an app with properties, or from an
// image the node didn't pull.
func TestTheFakeNodeRefusesAnAppsRunAsTheAgentDoes(t *testing.T) {
	user := "1000"
	for name, c := range map[string]struct {
		container *nodev1.Container
		pull      bool
		want      bool
	}{
		"as mooctl runs an app":    {&nodev1.Container{Name: "app-web", Image: nginx}, true, true},
		"with a user":              {&nodev1.Container{Name: "app-web", Image: nginx, User: &user}, true, false},
		"with a read-only root":    {&nodev1.Container{Name: "app-web", Image: nginx, ReadOnlyRoot: true}, true, false},
		"with capabilities":        {&nodev1.Container{Name: "app-web", Image: nginx, Capabilities: &nodev1.Capabilities{}}, true, false},
		"with an API":              {&nodev1.Container{Name: "app-web", Image: nginx, Api: "moostack.dns.v1.DNS"}, true, false},
		"from an image not pulled": {&nodev1.Container{Name: "app-web", Image: nginx}, false, false},
	} {
		n := pulling(nginxBuild)
		if _, err := n.AcquireLock(t.Context(), nodeclient.Lock{Lease: "op"}, time.Minute); err != nil {
			t.Fatal(err)
		}
		if c.pull {
			if _, err := n.PullImage(t.Context(), nginx, "op"); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := n.Run(t.Context(), c.container, "op"); (err == nil) != c.want {
			t.Errorf("%s: the fake's run says %v, want it run: %v", name, err, c.want)
		}
	}
	n := pulling(nginxBuild)
	if _, err := n.PullImage(t.Context(), nginx, "other"); err == nil {
		t.Error("the fake pulled without the lease")
	}
}

// interrupt sends mooctl SIGINT, as Ctrl-C does, and waits for the call
// whose context is ctx to end.
func interrupt(ctx context.Context) {
	syscall.Kill(os.Getpid(), syscall.SIGINT)
	<-ctx.Done()
}

// Ctrl-C in each phase of a deploy stops it, gives back the lock, and
// says what it means for the app: before the pull ended, nothing changed;
// before the run's answer, the run may go on; and before the check, the
// app started but wasn't checked.
func TestCtrlCStopsADeployInEachPhaseAndSaysWhatChanged(t *testing.T) {
	const placed = "web: node1 (fewest apps)\nPulling docker.io/library/nginx:1.27 on node1\n"
	for name, c := range map[string]struct {
		set         func(n *fakeNode, o *operator)
		want        string
		wantRunning bool
	}{
		"the pull": {func(n *fakeNode, _ *operator) { n.apps.pulling = interrupt },
			"mooctl stopped the pull of docker.io/library/nginx:1.27 on node1: context canceled; nothing changed", false},
		"the run": {func(n *fakeNode, _ *operator) { n.apps.running = interrupt },
			"mooctl stopped waiting for node1 to run web, which may go on on the node: context canceled\nRun mooctl deploy again: it says whether web runs", true},
		"the check": {func(n *fakeNode, o *operator) {
			o.startCheck = time.Hour
			n.apps.ran = func() { go syscall.Kill(os.Getpid(), syscall.SIGINT) }
		}, "web started on node1, but mooctl couldn't check that it still runs: context canceled\nRun mooctl deploy again: it says whether web runs", true},
	} {
		t.Run(name, func(t *testing.T) {
			o := newOperator(t, "op")
			n := pulling(nginxBuild)
			deployCluster(t, o, map[string]*fakeNode{"node1": n})
			c.set(n, o)
			r := o.run("deploy", web(t))
			if r.err == nil || r.err.Error() != c.want {
				t.Errorf("deploy stopped in %s says\n%v\nwant\n%s", name, r.err, c.want)
			}
			if r.stdout != placed {
				t.Errorf("deploy stopped in %s printed\n%s\nwant\n%s", name, r.stdout, placed)
			}
			if l := n.heldLock(); l != nil {
				t.Errorf("deploy stopped in %s kept the lock: %+v", name, l)
			}
			running := slices.ContainsFunc(n.status.Containers, func(k *nodev1.ContainerStatus) bool { return k.Name == "app-web" })
			if running != c.wantRunning {
				t.Errorf("deploy stopped in %s left app-web on the node: %v, want %v", name, running, c.wantRunning)
			}
		})
	}
}

// MOOCTL_DIR set but empty names the current directory for the commands
// that make or read the cluster directory's files themselves too.
func TestAnEmptyMOOCTL_DIRNamesTheCurrentDirectoryForClusterInit(t *testing.T) {
	o := newOperator(t, "op")
	dir := t.TempDir()
	t.Chdir(dir)
	t.Setenv("MOOCTL_DIR", "")
	if r := o.runWithoutDir("cluster", "init", "--ssh-key", o.key, "--name", "prod"); r.err != nil {
		t.Fatalf("cluster init with MOOCTL_DIR empty: %v", r.err)
	}
	if _, err := os.Stat(filepath.Join(dir, "cluster.yml")); err != nil {
		t.Errorf("cluster init with MOOCTL_DIR empty made no cluster.yml in the current directory: %v", err)
	}
	if r := o.runWithoutDir("secret", "recipients", "ls", "--ssh-key", o.key); r.err != nil {
		t.Errorf("secret recipients ls with MOOCTL_DIR empty, in the cluster directory: %v", r.err)
	}
}

// A redeploy of a new tag of the build the app runs says nothing changed:
// the digest decides, not the tag.
func TestARedeployOfANewTagOfTheSameBuildSaysNothingChanged(t *testing.T) {
	var n *fakeNode
	wantDeploy(t, func() map[string]*fakeNode {
		n = pulling(nginxBuild, appRunning("web", nginx, nginxBuild))
		n.apps.registry["docker.io/library/nginx:1.28"] = nginxBuild
		return map[string]*fakeNode{"node1": n}
	}, appDir(t, "name: web\nimage: nginx:1.28\n"), `web: node1 (runs there)
Pulling docker.io/library/nginx:1.28 on node1
web runs docker.io/library/nginx:1.28 on node1 already; nothing changed
`, "")
	if len(n.asked) != 1 || n.status.Containers[0].Image != "docker.io/library/nginx:1.28" {
		t.Errorf("node1 was asked to run %v and runs %v, want app-web renamed to the new tag", n.asked, n.status.Containers)
	}
}

// MOOCTL_DIR names the cluster directory for the commands that make or
// read its files themselves too, run from another directory.
func TestMOOCTL_DIRNamesTheClusterDirectoryForClusterInitAndSecrets(t *testing.T) {
	o := newOperator(t, "op")
	dir := filepath.Join(t.TempDir(), "prod")
	t.Chdir(t.TempDir())
	t.Setenv("MOOCTL_DIR", dir)
	if r := o.runWithoutDir("cluster", "init", "--ssh-key", o.key, "--name", "prod"); r.err != nil {
		t.Fatalf("cluster init with MOOCTL_DIR: %v", r.err)
	}
	if _, err := os.Stat(filepath.Join(dir, "cluster.yml")); err != nil {
		t.Errorf("cluster init with MOOCTL_DIR made no cluster.yml there: %v", err)
	}
	if r := o.runWithoutDir("secret", "recipients", "ls", "--ssh-key", o.key); r.err != nil || !strings.Contains(r.stdout, "op") {
		t.Errorf("secret recipients ls with MOOCTL_DIR: %v\n%s", r.err, r.stdout)
	}
}

// without is cs without the container called name.
func without(cs []*nodev1.ContainerStatus, name string) []*nodev1.ContainerStatus {
	return slices.DeleteFunc(cs, func(k *nodev1.ContainerStatus) bool { return k.Name == name })
}
