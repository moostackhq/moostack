package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"

	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/moostackhq/moostack/internal/orchestrator"
	"github.com/moostackhq/moostack/internal/sshclient"
	"golang.org/x/crypto/ssh"
)

// A node only Kubernetes has, whose host still runs k3s, is removed with
// a warning that it registers again.
func TestRemovingANodeOnlyKubernetesHasWarnsItMayComeBack(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	h2 := c.hosts["root@h2"]
	h2.k3s = true
	h2.nodeName = "stray"
	c.nodes["stray"] = fakeKubeNode{ip: "10.0.0.3", arch: "amd64"}

	out := mustRun(t, c, home, "cluster", "node-remove", "stray")
	if !strings.Contains(out, "Warning: if k3s still runs on stray's host, it registers again") {
		t.Errorf("node-remove doesn't warn that the node comes back:\n%s", out)
	}
}

func TestTheLastNodeNeedsYesAfterTheDirectoryIsBroughtUpToItsRemote(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	mustRun(t, c, home, "cluster", "node-add", "root@h2")
	dir := prodDir(t, home).Path
	remote := filepath.Join(t.TempDir(), "prod.git")
	git(t, home, "init", "--bare", "--quiet", "--initial-branch=main", remote)
	mustRun(t, c, home, "cluster", "remote", remote)

	// A teammate removes node2 and pushes; this machine hasn't fetched.
	elsewhere := filepath.Join(t.TempDir(), "prod")
	git(t, home, "clone", "--quiet", remote, elsewhere)
	if err := os.Remove(filepath.Join(elsewhere, "nodes", "node2.yml")); err != nil {
		t.Fatal(err)
	}
	git(t, elsewhere, "commit", "--quiet", "-am", "node-remove node2")
	git(t, elsewhere, "push", "--quiet")
	delete(c.nodes, "node2")
	c.hosts["root@h2"].k3s = false
	c.hosts["root@h1"].files[AppliedCommitFile] = git(t, elsewhere, "rev-parse", "HEAD") + "\n"
	_ = dir

	out, err := onFake(t, c, home, "cluster", "node-remove", "node1")
	if err == nil || !c.hosts["root@h1"].k3s {
		t.Errorf("node-remove node1 without --yes = %v, k3s on h1 %v:\n%s", err, c.hosts["root@h1"].k3s, out)
	}
}

// probeConn is a fakeConn whose answers hook can replace.
type probeConn struct {
	*fakeConn
	hook func(cmd string) (sshclient.Result, error, bool)
}

func (p probeConn) Run(ctx context.Context, cmd string, stdin io.Reader) (sshclient.Result, error) {
	if res, err, ok := p.hook(cmd); ok {
		return res, err
	}
	return p.fakeConn.Run(ctx, cmd, stdin)
}

// onProbe runs mooctl like onFake, in ctx, with each connection's
// answers passed through hook first.
func onProbe(t *testing.T, ctx context.Context, c *fakeCluster, home string, hook func(target, cmd string) (sshclient.Result, error, bool), args ...string) (string, error) {
	t.Helper()
	var out bytes.Buffer
	env := fakeEnvFor(c, home, &out)
	env.Connect = func(ctx context.Context, target string, key ssh.PublicKey) (remote, ssh.PublicKey, error) {
		r, k, err := c.connect(ctx, target, key)
		if err != nil {
			return r, k, err
		}
		return probeConn{r.(*fakeConn), func(cmd string) (sshclient.Result, error, bool) { return hook(target, cmd) }}, k, nil
	}
	err := New(env).Run(ctx, append([]string{"mooctl", "-c", "prod"}, args...))
	return out.String(), err
}

// A control plane whose k3s is gone but whose SSH answers is removed.
func TestAControlPlaneWhoseUninstallWasCutOffCanBeRemoved(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	h1 := c.hosts["root@h1"]
	// What the cut-off uninstall left: k3s gone, the applied commit gone.
	h1.k3s, h1.server = false, false
	delete(h1.files, AppliedCommitFile)
	c.nodes = map[string]fakeKubeNode{}
	c.orch.Fail("Nodes", errors.New("reading the control plane's kubeconfig: exit 1: no such file"))
	if out, err := onFake(t, c, home, "cluster", "node-remove", "node1", "--yes"); err != nil {
		t.Errorf("node-remove node1 --yes: %v\n%s", err, out)
	}
	if l := left(h1); l != "" || len(nodeNames(t, home)) != 0 {
		t.Errorf("h1 still has %q, and the cluster directory the nodes %v", l, nodeNames(t, home))
	}
}

// The cluster's nodes can't be read, and the node isn't its last.
func TestARemovalThatCantReadTheClustersNodesRemovesNothing(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	mustRun(t, c, home, "cluster", "node-add", "root@h2")
	c.orch.Fail("Nodes", errors.New("the API is down"))
	out, err := onFake(t, c, home, "cluster", "node-remove", "node2")
	if err == nil || err.Error() != "listing the cluster's nodes: the API is down" {
		t.Errorf("node-remove node2: %v\n%s", err, out)
	}
	if len(c.orch.made("RemoveNode")) != 0 || len(nodeNames(t, home)) != 2 || c.locked {
		t.Errorf("the orchestrator was asked for the removals %q, the cluster directory has %v, the lock is held: %t", c.orch.made("RemoveNode"), nodeNames(t, home), c.locked)
	}
}

// The control plane's first connection fails and its next one doesn't.
func TestAnUnreachableRemovalOfAControlPlaneThatAnswersAfterAllLeavesNothingOnIt(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	var out bytes.Buffer
	env := fakeEnvFor(c, home, &out)
	dials := 0
	env.Connect = func(ctx context.Context, target string, key ssh.PublicKey) (remote, ssh.PublicKey, error) {
		if dials++; dials == 1 {
			return nil, nil, errors.New("connecting to " + target + ": no route to host")
		}
		return c.connect(ctx, target, key)
	}
	if err := New(env).Run(t.Context(), []string{"mooctl", "-c", "prod", "cluster", "node-remove", "--unreachable", "--yes", "node1"}); err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	if l := left(c.hosts["root@h1"]); l != "" || !strings.Contains(out.String(), "Removed node1 from prod") {
		t.Errorf("h1 still has %q:\n%s", l, out.String())
	}
}

// Ctrl-C during the uninstall of node-remove --unreachable leaves the
// node in the cluster directory.
func TestAnInterruptedUnreachableRemovalCommitsNothing(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	mustRun(t, c, home, "cluster", "node-add", "root@h2")
	c.hosts["root@h1"].unreachable = true
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	c.orch.onUninstall = func(string) error {
		cancel()
		return context.Canceled
	}
	unhooked := func(string, string) (sshclient.Result, error, bool) { return sshclient.Result{}, nil, false }
	out, err := onProbe(t, ctx, c, home, unhooked, "cluster", "node-remove", "--unreachable", "node2")
	if err == nil || !slices.Contains(nodeNames(t, home), "node2") {
		t.Errorf("an interrupted removal = %v, nodes %v:\n%s", err, nodeNames(t, home), out)
	}
}

func TestAnUnreachableRemovalDecidesOnTheDirectoryFromItsRemote(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	remote := filepath.Join(t.TempDir(), "prod.git")
	git(t, home, "init", "--bare", "--quiet", "--initial-branch=main", remote)
	mustRun(t, c, home, "cluster", "remote", remote)

	// A teammate's node-add of node2, pushed.
	elsewhere := filepath.Join(t.TempDir(), "prod")
	git(t, home, "clone", "--quiet", remote, elsewhere)
	node1, _ := os.ReadFile(filepath.Join(elsewhere, "nodes", "node1.yml"))
	node2 := strings.NewReplacer("name: node1", "name: node2", "root@h1", "root@h2", "10.0.0.2", "10.0.0.3", "control_plane: true", "control_plane: false", "controlplane: true", "controlplane: false").Replace(string(node1))
	os.WriteFile(filepath.Join(elsewhere, "nodes", "node2.yml"), []byte(node2), 0o644)
	git(t, elsewhere, "add", ".")
	git(t, elsewhere, "commit", "--quiet", "-m", "node-add node2")
	git(t, elsewhere, "push", "--quiet")

	c.hosts["root@h1"].unreachable = true
	out, err := onFake(t, c, home, "cluster", "node-remove", "--unreachable", "--yes", "node1")
	names := nodeNames(t, home)
	if err == nil && slices.Contains(names, "node2") && !slices.Contains(names, "node1") {
		st, _ := onFake(t, c, home, "cluster", "status")
		rm, rmErr := onFake(t, c, home, "cluster", "node-remove", "--unreachable", "node2")
		t.Errorf("removed the control plane though the fetched directory has node2 (%v):\n%s\nstatus:\n%s\nnode-remove node2: %v %s", names, out, st, rmErr, rm)
	}
}

// The control plane is refused while Kubernetes has another node.
func TestTheControlPlaneGoesAfterNodesOnlyKubernetesHas(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	c.nodes["stray"] = fakeKubeNode{ip: "10.0.0.9", arch: "amd64"}
	out, err := onFake(t, c, home, "cluster", "node-remove", "--yes", "node1")
	if err == nil {
		t.Errorf("the control plane went while Kubernetes has stray:\n%s", out)
	}
}

func TestAFailedUninstallKeepsTheNodeAndTheRemovalFinishesWhenRunAgain(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	mustRun(t, c, home, "cluster", "node-add", "root@h2")
	c.orch.onUninstall = func(target string) error {
		if target == "root@h2" {
			return errors.New("connection reset")
		}
		return nil
	}
	out, err := onFake(t, c, home, "cluster", "node-remove", "node2")
	if err == nil || err.Error() != "uninstalling from node2: connection reset; run node-remove again" {
		t.Errorf("node-remove with an uninstall that fails: %v\n%s", err, out)
	}
	if want := "Removing node2 from prod\nDraining node2...\nUninstalling...\n"; out != want {
		t.Errorf("output:\n%s\nwant:\n%s", out, want)
	}
	if _, onCluster := c.nodes["node2"]; !onCluster || !slices.Contains(nodeNames(t, home), "node2") || c.locked {
		t.Errorf("the cluster has node2: %t, the cluster directory has %v, the lock is held: %t", onCluster, nodeNames(t, home), c.locked)
	}
	c.orch.onUninstall = nil
	mustRun(t, c, home, "cluster", "node-remove", "node2")
	if _, onCluster := c.nodes["node2"]; onCluster || slices.Contains(nodeNames(t, home), "node2") || c.hosts["root@h2"].k3s {
		t.Errorf("run again, the cluster has node2: %t, the cluster directory has %v, k3s is on h2: %t", onCluster, nodeNames(t, home), c.hosts["root@h2"].k3s)
	}
}

// The control plane's uninstall fails at the orchestrator's part.
func TestAControlPlaneWhoseUninstallFailsIsKeptWithItsRegistryRemoved(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	c.orch.onUninstall = func(string) error { return errors.New("exit 3") }
	out, err := onFake(t, c, home, "cluster", "node-remove", "--yes", "node1")
	if err == nil || err.Error() != "uninstalling from node1: exit 3; run node-remove again" {
		t.Errorf("node-remove with an uninstall that fails: %v\n%s", err, out)
	}
	if !c.ran("root@h1", ownUninstallScript) || len(nodeNames(t, home)) != 1 {
		t.Errorf("the registry's uninstall ran: %t, and the cluster directory has %v", c.ran("root@h1", ownUninstallScript), nodeNames(t, home))
	}
	c.orch.onUninstall = nil
	mustRun(t, c, home, "cluster", "node-remove", "--yes", "node1")
	if l := left(c.hosts["root@h1"]); l != "" || len(nodeNames(t, home)) != 0 {
		t.Errorf("run again, h1 still has %q, and the cluster directory %v", l, nodeNames(t, home))
	}
}

func TestANodeThatHoldsVolumesIsRemovedOnlyWithLoseVolumes(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	mustRun(t, c, home, "cluster", "node-add", "root@h2")
	c.orch.Volumes["node2"] = []string{"pvc-1", "pvc-2"}
	out, err := onFake(t, c, home, "cluster", "node-remove", "node2")
	if err == nil || err.Error() != "node2 holds the volumes pvc-1, pvc-2, which go with it; pass --lose-volumes to remove it anyway" {
		t.Errorf("node-remove of a node that holds volumes: %v", err)
	}
	if out != "Removing node2 from prod\n" || !c.hosts["root@h2"].k3s || len(nodeNames(t, home)) != 2 {
		t.Errorf("the refusal printed\n%s\nk3s is on h2: %t, the cluster directory has %v", out, c.hosts["root@h2"].k3s, nodeNames(t, home))
	}
	mustRun(t, c, home, "cluster", "node-remove", "--lose-volumes", "node2")
	if !c.orch.removals["node2"].LoseVolumes || c.hosts["root@h2"].k3s || len(nodeNames(t, home)) != 1 {
		t.Errorf("with --lose-volumes node2 was removed with %+v, k3s is on h2: %t, the cluster directory has %v", c.orch.removals["node2"], c.hosts["root@h2"].k3s, nodeNames(t, home))
	}
}

func TestARemovalThatFailsBeforeTheUninstallSaysWhatFailedAlone(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	mustRun(t, c, home, "cluster", "node-add", "root@h2")
	c.orch.Fail("RemoveNode", errors.New("draining node2: no controller would start these pods again: shop/job"))
	out, err := onFake(t, c, home, "cluster", "node-remove", "node2")
	if err == nil || err.Error() != "draining node2: no controller would start these pods again: shop/job" {
		t.Errorf("node-remove with a drain that fails: %v\n%s", err, out)
	}
	if len(nodeNames(t, home)) != 2 || !c.hosts["root@h2"].k3s || c.locked {
		t.Errorf("the cluster directory has %v, k3s is on h2: %t, the lock is held: %t", nodeNames(t, home), c.hosts["root@h2"].k3s, c.locked)
	}
	c.orch.Fail("RemoveNode", nil)
	mustRun(t, c, home, "cluster", "node-remove", "node2")
	if len(nodeNames(t, home)) != 1 {
		t.Errorf("run again, the cluster directory has %v", nodeNames(t, home))
	}
}

func TestANodeIsNeverGivenTheControlPlanesOwnUninstall(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	mustRun(t, c, home, "cluster", "node-add", "root@h2")
	mustRun(t, c, home, "cluster", "node-remove", "node2")
	if c.ran("root@h2", "zot") || c.ran("root@h2", AppliedCommitFile) {
		t.Errorf("a node's removal ran on its host:\n%s", strings.Join(c.runs, "\n---\n"))
	}
	mustRun(t, c, home, "cluster", "node-remove", "--yes", "node1")
	if !c.ran("root@h1", ownUninstallScript) {
		t.Error("the control plane's removal left the registry and the applied commit")
	}
}

func TestTheControlPlanesOwnUninstallParsesAndDoesntWaitForItself(t *testing.T) {
	for _, sh := range []string{"sh", "bash", "dash"} {
		if _, err := exec.LookPath(sh); err != nil {
			continue
		}
		if out, err := exec.Command(sh, "-n", "-c", ownUninstallScript).CombinedOutput(); err != nil {
			t.Errorf("%s: the uninstall doesn't parse: %v\n%s", sh, err, out)
		}
	}
	waits := regexp.MustCompile(`pgrep -f '([^']+)'`).FindAllStringSubmatch(ownUninstallScript, -1)
	if len(waits) != 1 {
		t.Fatalf("%d pgreps, want 1, in\n%s", len(waits), ownUninstallScript)
	}
	if regexp.MustCompile(waits[0][1]).MatchString(ownUninstallScript) {
		t.Errorf("the script's own command line matches its pgrep pattern %q", waits[0][1])
	}
}

// writeUnder writes the file at path under root.
func writeUnder(t *testing.T, root, path, content string, mode os.FileMode) {
	t.Helper()
	path = filepath.Join(root, path)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatal(err)
	}
}

// The script runs with its paths under a directory of the test's, and a
// systemctl and a pgrep that log their calls. pgrep finds a download
// running the first downloading times it is asked.
func TestTheControlPlanesOwnUninstallRemovesTheRegistryAndTheAppliedCommitAndNothingElse(t *testing.T) {
	zots := []string{"/etc/moostack/zot.json", "/etc/systemd/system/moostack-zot.service", "/usr/local/bin/zot", "/usr/local/bin/zot.download", "/var/lib/zot/blob", "/var/lib/private/zot/blob"}
	for name, c := range map[string]struct {
		downloading   int
		kept, removed []string
		calls         string
	}{
		"a control plane": {
			kept:    []string{"/etc/other", "/etc/rancher/k3s/config.yaml", "/usr/local/bin/k3s"},
			removed: append([]string{AppliedCommitFile, "/etc/moostack", "/var/lib/moostack", "/var/lib/zot", "/var/lib/private/zot"}, zots...),
			calls:   "pgrep disable---now-moostack-zot.service daemon-reload",
		},
		"a control plane with files of others beside mooctl's": {
			kept:    []string{"/etc/moostack/other", "/var/lib/moostack/other"},
			removed: append([]string{AppliedCommitFile}, zots...),
			calls:   "pgrep disable---now-moostack-zot.service daemon-reload",
		},
		"a control plane with a download still running": {
			downloading: 1,
			removed:     append([]string{AppliedCommitFile}, zots...),
			calls:       "pgrep pgrep disable---now-moostack-zot.service daemon-reload",
		},
		"a host with nothing of it": {
			kept:  []string{"/etc/other"},
			calls: "pgrep disable---now-moostack-zot.service daemon-reload",
		},
	} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			write := func(path, content string, mode os.FileMode) { writeUnder(t, root, path, content, mode) }
			all := append(slices.Clone(c.kept), c.removed...)
			for _, f := range all {
				if !slices.ContainsFunc(all, func(o string) bool { return strings.HasPrefix(o, f+"/") }) {
					write(f, "x", 0o644)
				}
			}
			log := filepath.Join(root, "calls")
			write("/fake/systemctl", fmt.Sprintf("#!/bin/sh\necho \"$*\" | tr ' ' - >> %s\n", log), 0o755)
			write("/fake/pgrep", fmt.Sprintf("#!/bin/sh\necho pgrep >> %[1]s\n[ \"$(grep -c pgrep %[1]s)\" -le %[2]d ]\n", log, c.downloading), 0o755)
			write("/fake/sleep", "#!/bin/sh\n", 0o755)

			script := regexp.MustCompile(`(^|\s)(/usr/local/bin/|/etc/|/var/lib/)`).ReplaceAllString(ownUninstallScript, "${1}"+root+"${2}")
			for _, m := range regexp.MustCompile(`(?:^|[\s>])(/[^\s;)]*)`).FindAllStringSubmatch(script, -1) {
				if m[1] != "/dev/null" && !strings.HasPrefix(m[1], root+"/") {
					t.Fatalf("the script names %s, outside %s", m[1], root)
				}
			}
			cmd := exec.Command("sh", "-c", script)
			cmd.Env = []string{"PATH=" + filepath.Join(root, "fake") + ":/usr/bin:/bin"}
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Errorf("the script fails: %v\n%s", err, out)
			}
			for _, f := range c.removed {
				if _, err := os.Stat(filepath.Join(root, f)); err == nil {
					t.Errorf("%s is left", f)
				}
			}
			for _, f := range c.kept {
				if _, err := os.Stat(filepath.Join(root, f)); err != nil {
					t.Errorf("%s is gone", f)
				}
			}
			calls, _ := os.ReadFile(log)
			if got := strings.Join(strings.Fields(string(calls)), " "); got != c.calls {
				t.Errorf("the script ran\n%s\nwant\n%s", got, c.calls)
			}
		})
	}
}

func TestOnlyAFailedUninstallSaysToRunTheRemovalAgain(t *testing.T) {
	for step, want := range map[orchestrator.Step]string{
		orchestrator.Draining:     "draining node2: these pods are still there: shop/web-1",
		orchestrator.Uninstalling: "uninstalling from node2: exit 1; run node-remove again",
		orchestrator.Deleting:     "deleting node node2: it is still there after 1m0s",
	} {
		c, home := fakeProd(t)
		mustRun(t, c, home, "cluster", "node-add", "root@h1")
		mustRun(t, c, home, "cluster", "node-add", "root@h2")
		c.orch.failRemovalAt = map[orchestrator.Step]error{step: errors.New(strings.TrimSuffix(want, "; run node-remove again"))}
		out, err := onFake(t, c, home, "cluster", "node-remove", "node2")
		if err == nil || err.Error() != want {
			t.Errorf("node-remove that fails at step %v: %v\nwant %s\n%s", step, err, want, out)
		}
		if !slices.Contains(nodeNames(t, home), "node2") || c.locked {
			t.Errorf("step %v: the cluster directory has %v, and the lock is held: %t", step, nodeNames(t, home), c.locked)
		}
	}
}

func TestAnInterruptedRemovalThroughTheControlPlaneKeepsTheNode(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	mustRun(t, c, home, "cluster", "node-add", "root@h2")
	ctx, cancel := context.WithCancelCause(t.Context())
	c.orch.onUninstall = func(string) error {
		cancel(errors.New("interrupted"))
		return context.Canceled
	}
	_, _, err := streams(t, c, home, ctx, "cluster", "node-remove", "node2")
	if err == nil || !slices.Contains(nodeNames(t, home), "node2") || c.locked {
		t.Errorf("an interrupted removal: %v; the cluster directory has %v, and the lock is held: %t", err, nodeNames(t, home), c.locked)
	}
}
