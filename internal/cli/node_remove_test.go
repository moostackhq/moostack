package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"slices"

	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/moostackhq/moostack/internal/sshclient"
	"golang.org/x/crypto/ssh"

	"github.com/moostackhq/moostack/internal/k3s"
)

// A node only Kubernetes has, whose host still runs k3s, is removed with
// a warning that it registers again.
func TestRemovingANodeOnlyKubernetesHasWarnsItMayComeBack(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	h2 := c.hosts["root@h2"]
	h2.k3s = true
	h2.files[k3s.ConfigFile] = "node-name: stray\n"
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

// A k3s version mooctl has no install script checksum for is refused
// before node-add writes to the host.
func TestAVersionWithoutAChecksumIsRefusedBeforeTouchingTheHost(t *testing.T) {
	c, home := fakeProd(t)
	d := prodDir(t, home)
	path := filepath.Join(d.Path, "cluster.yml")
	data, _ := os.ReadFile(path)
	os.WriteFile(path, []byte(strings.Replace(string(data), K3sVersion, "v1.38.0+k3s1", 1)), 0o644)
	git(t, d.Path, "commit", "--quiet", "-am", "pin v1.38.0")

	out, err := onFake(t, c, home, "cluster", "node-add", "root@h1")
	if err == nil {
		t.Fatalf("an unknown version was installed:\n%s", out)
	}
	if c.ran("root@h1", "mkdir -p") {
		t.Errorf("refused after writing to the host (%v); ran:\n%s", err, strings.Join(c.runs, "\n---\n"))
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
	noK3s := func(target, cmd string) (sshclient.Result, error, bool) {
		if target == "root@h1" && strings.HasPrefix(cmd, "k3s ") {
			return sshclient.Result{ExitCode: 127, Stderr: []byte("sh: 1: k3s: not found")}, nil, true
		}
		return sshclient.Result{}, nil, false
	}
	for _, args := range [][]string{{"--yes"}} {
		out, err := onProbe(t, t.Context(), c, home, noK3s, append([]string{"cluster", "node-remove", "node1"}, args...)...)
		if err != nil {
			t.Errorf("node-remove node1 %v: %v\n%s", args, err, out)
		}
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
	cut := func(target, cmd string) (sshclient.Result, error, bool) {
		if target == "root@h2" && strings.Contains(cmd, "uninstall.sh") {
			cancel()
			return sshclient.Result{}, context.Canceled, true
		}
		return sshclient.Result{}, nil, false
	}
	out, err := onProbe(t, ctx, c, home, cut, "cluster", "node-remove", "--unreachable", "node2")
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

func TestAFailedUninstallKeepsTheNode(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	mustRun(t, c, home, "cluster", "node-add", "root@h2")
	drop := func(target, cmd string) (sshclient.Result, error, bool) {
		if target == "root@h2" && strings.Contains(cmd, "uninstall.sh") {
			return sshclient.Result{}, errors.New("connection reset"), true
		}
		return sshclient.Result{}, nil, false
	}
	out, err := onProbe(t, t.Context(), c, home, drop, "cluster", "node-remove", "node2")
	_, inKube := c.nodes["node2"]
	if err == nil && !slices.Contains(nodeNames(t, home), "node2") && inKube {
		t.Errorf("node-remove = nil, the node file dropped, Kubernetes still has node2:\n%s", out)
	}
}

func TestUninstallRemovesOnlyWhatMooctlWrote(t *testing.T) {
	agent := uninstallScript(k3s.Agent)
	for _, never := range []string{"/var/lib/moostack", "/etc/moostack", "/var/lib/zot", "rm -rf"} {
		if strings.Contains(agent, never) {
			t.Errorf("a node's uninstall touches %s:\n%s", never, agent)
		}
	}
	cp := uninstallScript(k3s.ControlPlane)
	for _, want := range []string{k3s.ConfigFile, AppliedCommitFile, "rmdir /var/lib/moostack", "/var/lib/zot", "/etc/moostack/zot.json"} {
		if !strings.Contains(cp, want) {
			t.Errorf("the control plane's uninstall lacks %s:\n%s", want, cp)
		}
	}
}

func TestUninstallParsesAndDoesntWaitForItself(t *testing.T) {
	for _, role := range []k3s.Role{k3s.ControlPlane, k3s.Agent} {
		script := uninstallScript(role)
		for _, sh := range []string{"sh", "bash", "dash"} {
			if _, err := exec.LookPath(sh); err != nil {
				continue
			}
			if out, err := exec.Command(sh, "-n", "-c", script).CombinedOutput(); err != nil {
				t.Errorf("%s: %s's uninstall doesn't parse: %v\n%s", sh, role, err, out)
			}
		}
		waits := regexp.MustCompile(`pgrep -f '([^']+)'`).FindAllStringSubmatch(script, -1)
		if want := map[bool]int{true: 2, false: 1}[role.ControlPlane]; len(waits) != want {
			t.Fatalf("%s: %d pgreps, want %d, in\n%s", role, len(waits), want, script)
		}
		for _, m := range waits {
			if regexp.MustCompile(m[1]).MatchString(script) {
				t.Errorf("%s: the script's own command line matches its pgrep pattern %q", role, m[1])
			}
		}
		if !strings.HasSuffix(script, "exit $s") {
			t.Errorf("%s: the script doesn't exit with the uninstall's status", role)
		}
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

// uninstallUnder runs the uninstall of a node of role with its paths
// under root and the commands of root's fake directory first on its
// PATH, and returns its exit status and output. It refuses a script that
// names a path elsewhere.
func uninstallUnder(root string, role k3s.Role) (int, string) {
	script := regexp.MustCompile(`(^|\s)(/usr/local/bin/|/etc/|/var/lib/)`).ReplaceAllString(uninstallScript(role), "${1}"+root+"${2}")
	for _, m := range regexp.MustCompile(`(?:^|[\s>])(/[^\s;)]*)`).FindAllStringSubmatch(script, -1) {
		if m[1] != "/dev/null" && !strings.HasPrefix(m[1], root+"/") {
			return -1, "the script names " + m[1] + ", outside " + root
		}
	}
	cmd := exec.Command("sh", "-c", script)
	cmd.Env = []string{"PATH=" + filepath.Join(root, "fake") + ":/usr/bin:/bin"}
	out, _ := cmd.CombinedOutput()
	return cmd.ProcessState.ExitCode(), string(out)
}

// Each script runs with its paths under a directory of the test's, and a
// systemctl and a pgrep that log their calls. k3s's uninstall script
// logs its call and exits with status, or is absent for status -1. pgrep
// finds an install running the first installing times it is asked.
func TestUninstallRemovesWhatIsThereAndFailsAsK3ssDoes(t *testing.T) {
	zots := []string{"/etc/moostack/zot.json", "/etc/systemd/system/moostack-zot.service", "/usr/local/bin/zot", "/usr/local/bin/zot.download", "/var/lib/zot/blob", "/var/lib/private/zot/blob"}
	for name, c := range map[string]struct {
		role       k3s.Role
		status     int
		installing int
		// kept are the files there that the script leaves, and removed
		// the files there, and their directories, that it removes.
		kept, removed []string
		// calls are what the script runs, in order.
		calls string
	}{
		"a control plane": {
			role:    k3s.ControlPlane,
			kept:    []string{"/etc/other"},
			removed: append([]string{k3s.ConfigFile, k3s.RegistriesFile, k3s.AutoDeployFile("issuer"), AppliedCommitFile, "/etc/rancher/k3s", "/etc/moostack", "/var/lib/moostack", "/var/lib/zot", "/var/lib/private/zot"}, zots...),
			calls:   "pgrep k3s-uninstall daemon-reload pgrep disable---now-moostack-zot.service daemon-reload",
		},
		"a control plane whose k3s fails to uninstall": {
			role:    k3s.ControlPlane,
			status:  3,
			removed: append([]string{k3s.ConfigFile, AppliedCommitFile}, zots...),
			calls:   "pgrep k3s-uninstall daemon-reload pgrep disable---now-moostack-zot.service daemon-reload",
		},
		"a control plane whose k3s is gone": {
			role:    k3s.ControlPlane,
			status:  -1,
			removed: append([]string{k3s.ConfigFile, AppliedCommitFile}, zots...),
			calls:   "pgrep daemon-reload pgrep disable---now-moostack-zot.service daemon-reload",
		},
		"a control plane with files of others beside mooctl's": {
			role:    k3s.ControlPlane,
			kept:    []string{"/etc/moostack/other", "/var/lib/moostack/other", "/etc/rancher/k3s/other"},
			removed: append([]string{k3s.ConfigFile, AppliedCommitFile}, zots...),
			calls:   "pgrep k3s-uninstall daemon-reload pgrep disable---now-moostack-zot.service daemon-reload",
		},
		"a control plane with an install still running": {
			role:       k3s.ControlPlane,
			installing: 1,
			removed:    append([]string{k3s.ConfigFile, AppliedCommitFile}, zots...),
			calls:      "pgrep pgrep k3s-uninstall daemon-reload pgrep disable---now-moostack-zot.service daemon-reload",
		},
		"a node": {
			role:    k3s.Agent,
			kept:    append([]string{AppliedCommitFile}, zots...),
			removed: []string{k3s.ConfigFile, k3s.RegistriesFile, "/etc/rancher/k3s"},
			calls:   "pgrep k3s-uninstall daemon-reload",
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
			if c.status >= 0 {
				write("/usr/local/bin/"+map[bool]string{true: "k3s-uninstall.sh", false: "k3s-agent-uninstall.sh"}[c.role.ControlPlane],
					fmt.Sprintf("#!/bin/sh\necho k3s-uninstall >> %s\nexit %d\n", log, c.status), 0o755)
			}
			write("/fake/systemctl", fmt.Sprintf("#!/bin/sh\necho \"$*\" | tr ' ' - >> %s\n", log), 0o755)
			write("/fake/pgrep", fmt.Sprintf("#!/bin/sh\necho pgrep >> %[1]s\n[ \"$(grep -c pgrep %[1]s)\" -le %[2]d ]\n", log, c.installing), 0o755)
			write("/fake/sleep", "#!/bin/sh\n", 0o755)

			if got, out := uninstallUnder(root, c.role); got != max(c.status, 0) {
				t.Errorf("the script exits %d, want %d:\n%s", got, max(c.status, 0), out)
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

// A host goes through installs that put some of mooctl's files there, and
// uninstalls, with k3s's own uninstall script there or not, passing or
// failing. After each uninstall the host has none of the role's files,
// keeps every other, and the script exited as k3s's did.
func TestSequencesOfInstallsAndUninstallsLeaveOnlyWhatIsntMooctls(t *testing.T) {
	zots := []string{"/etc/moostack/zot.json", "/etc/systemd/system/moostack-zot.service", "/usr/local/bin/zot", "/usr/local/bin/zot.download", "/var/lib/zot/blob", "/var/lib/private/zot/blob", AppliedCommitFile}
	others := []string{"/etc/other", "/etc/moostack/other", "/var/lib/moostack/other", "/etc/rancher/k3s/other", "/usr/local/bin/other", "/var/lib/other/blob"}
	for seed := range uint64(40) {
		rng := rand.New(rand.NewPCG(seed, 1))
		role := k3s.RoleOf(rng.IntN(2) == 0)
		root := t.TempDir()
		var mine []string
		for _, f := range (k3s.Written{Role: role}).Files() {
			mine = append(mine, f.Path)
		}
		if role.ControlPlane {
			mine = append(mine, zots...)
		}
		own := "/usr/local/bin/k3s-agent-uninstall.sh"
		if role.ControlPlane {
			own = "/usr/local/bin/k3s-uninstall.sh"
		}
		writeUnder(t, root, "/fake/systemctl", "#!/bin/sh\n", 0o755)
		writeUnder(t, root, "/fake/pgrep", "#!/bin/sh\nexit 1\n", 0o755)
		kept := map[string]bool{}
		for step := range 6 {
			for _, f := range mine {
				if rng.IntN(2) == 0 {
					writeUnder(t, root, f, "x", 0o644)
				}
			}
			for _, f := range others {
				if rng.IntN(3) == 0 {
					writeUnder(t, root, f, "x", 0o644)
					kept[f] = true
				}
			}
			status := 0
			if rng.IntN(3) > 0 {
				status = rng.IntN(2) * 3
				writeUnder(t, root, own, fmt.Sprintf("#!/bin/sh\nrm -f \"$0\"\nexit %d\n", status), 0o755)
			}
			got, out := uninstallUnder(root, role)
			if got != status {
				t.Fatalf("seed %d, step %d, %s: the script exits %d, want %d:\n%s", seed, step, role, got, status, out)
			}
			for _, f := range mine {
				if _, err := os.Stat(filepath.Join(root, f)); err == nil {
					t.Fatalf("seed %d, step %d, %s: %s is left", seed, step, role, f)
				}
			}
			for f := range kept {
				if _, err := os.Stat(filepath.Join(root, f)); err != nil {
					t.Fatalf("seed %d, step %d, %s: %s is gone", seed, step, role, f)
				}
			}
		}
	}
}
