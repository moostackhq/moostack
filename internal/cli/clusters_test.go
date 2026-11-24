package cli

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/moostackhq/moostack/internal/terminal"
)

// typist is a terminal whose operator types the lines, one per prompt.
type typist struct {
	out   *bytes.Buffer
	lines []string
}

func (t *typist) Write(p []byte) (int, error) { return t.out.Write(p) }

func (t *typist) ReadLine() (string, error) {
	if len(t.lines) == 0 {
		return "", io.EOF
	}
	line := t.lines[0]
	t.lines = t.lines[1:]
	return line, nil
}

func (t *typist) ReadPassword() ([]byte, error) { return nil, io.EOF }

func (t *typist) Close() error { return nil }

// typing runs mooctl with args on c's hosts, with an operator who types
// the lines at the terminal. The output holds what the terminal showed.
func typing(t *testing.T, c *fakeCluster, home string, lines []string, args ...string) (string, error) {
	t.Helper()
	var out bytes.Buffer
	env := fakeEnvFor(c, home, &out)
	env.Terminal = func(func(int)) (terminal.Terminal, io.Closer) {
		tt := &typist{out: &out, lines: lines}
		return tt, tt
	}
	err := New(env).Run(t.Context(), append([]string{"mooctl"}, args...))
	return out.String(), err
}

func TestClusterListShowsEveryClusterItsNodesAndTheDefault(t *testing.T) {
	c, home := fakeProd(t)
	key := filepath.Join(home, "id_ed25519")
	if out, err := mooctl(t, home, home, "cluster", "list"); err != nil || !strings.Contains(out, "prod") {
		t.Fatalf("list = %v\n%s", err, out)
	}
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	mustRun(t, c, home, "cluster", "node-add", "root@h2")
	if out, err := mooctl(t, home, home, "--ssh-key", key, "cluster", "init", "acme/staging"); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	if err := os.WriteFile(filepath.Join(home, "clusters", "default", "notes.txt"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := mooctl(t, home, home, "cluster", "list")
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	want := "CLUSTER       NODES  DEFAULT\nacme/staging  0      \nprod          2      yes\n"
	if out != want {
		t.Errorf("list:\n%s\nwant:\n%s", out, want)
	}
}

func TestClusterListWithoutClusters(t *testing.T) {
	home := newHome(t)
	if out, err := mooctl(t, home, home, "cluster", "list"); err != nil || out != "No clusters.\n" {
		t.Errorf("list = %v\n%s", err, out)
	}
}

func TestClusterInitSaysWhichClusterIsTheDefault(t *testing.T) {
	home := newHome(t)
	key := sshKey(t, home)
	out, err := mooctl(t, home, home, "--ssh-key", key, "cluster", "init", "first")
	if err != nil || !strings.HasPrefix(out, "Created cluster first\nDefault cluster: first\n") {
		t.Fatalf("init = %v\n%s", err, out)
	}
	out, err = mooctl(t, home, home, "--ssh-key", key, "cluster", "init", "second")
	if err != nil || !strings.HasPrefix(out, "Created cluster second\nDefault cluster: first\n") {
		t.Fatalf("init = %v\n%s", err, out)
	}
	out, err = mooctl(t, home, home, "--ssh-key", key, "-C", filepath.Join(home, "elsewhere"), "cluster", "init", "third")
	if err != nil || !strings.HasPrefix(out, "Created cluster third\n") || strings.Contains(out, "Default cluster") {
		t.Fatalf("init = %v\n%s", err, out)
	}
}

func TestNodeAndAppCommandsNameTheirClusterFirst(t *testing.T) {
	c, home := fakeProd(t)
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"cluster", "node-add", "root@h1"}, "Adding node1 to prod ("},
		{[]string{"cluster", "node-add", "root@h2"}, "Adding node2 to prod ("},
		{[]string{"cluster", "node-remove", "node2"}, "Removing node2 from prod\n"},
		{[]string{"cluster", "node-remove", "node1", "--yes"}, "Removing node1 from prod\n"},
	} {
		if out := mustRun(t, c, home, tc.args...); !strings.HasPrefix(out, tc.want) {
			t.Errorf("%v:\n%s\nwant it to start with %q", tc.args, out, tc.want)
		}
	}
}

func TestClusterRemoveUninstallsEveryNodeAndDeletesTheDirectory(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	mustRun(t, c, home, "cluster", "node-add", "root@h2")
	dir := prodDir(t, home).Path
	out, err := typing(t, c, home, []string{"prod"}, "cluster", "remove", "prod")
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	for _, want := range []string{
		"Removing cluster prod:\n",
		"- k3s is uninstalled from node2 (root@h2), node1 (root@h1): every app there stops, and its volumes are deleted\n",
		"- the cluster directory " + dir + " is deleted, with its secret store\n",
		"This can't be undone. Type the cluster's name to remove it: ",
		"Uninstalling k3s from node2...\nUninstalling k3s from node1...\n",
		"Removed cluster prod\nDefault cluster: none\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output lacks %q:\n%s", want, out)
		}
	}
	for _, h := range []string{"root@h1", "root@h2"} {
		if l := left(c.hosts[h]); l != "" {
			t.Errorf("%s still has %s", h, l)
		}
	}
	if strings.Contains(out, "remote") {
		t.Errorf("output names a remote the cluster doesn't have:\n%s", out)
	}
	for _, h := range []string{"root@h1", "root@h2"} {
		if c.hosts[h].k3s {
			t.Errorf("%s still has k3s", h)
		}
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Errorf("the cluster directory is still there: %v", err)
	}
	if _, err := os.Stat(filepath.Dir(dir)); !os.IsNotExist(err) {
		t.Errorf("the prefix's directory is still there: %v", err)
	}
	if out, err := mooctl(t, home, home, "cluster", "use"); err == nil {
		t.Errorf("there is still a default cluster: %s", out)
	}
}

func TestClusterRemoveKeepsTheDefaultAndThePrefixOfOtherClusters(t *testing.T) {
	c, home := fakeProd(t)
	key := filepath.Join(home, "id_ed25519")
	if out, err := mooctl(t, home, home, "--ssh-key", key, "cluster", "init", "other"); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	out, err := typing(t, c, home, []string{"other"}, "cluster", "remove", "other")
	if err != nil || !strings.HasSuffix(out, "Removed cluster other\n") {
		t.Fatalf("remove = %v\n%s", err, out)
	}
	if out, err := mooctl(t, home, home, "cluster", "list"); err != nil || out != "CLUSTER  NODES  DEFAULT\nprod     0      yes\n" {
		t.Errorf("list = %v\n%s", err, out)
	}
}

func TestClusterRemoveChangesNothingUnlessTheNameIsTyped(t *testing.T) {
	for name, lines := range map[string][]string{
		"another name":   {"prodd"},
		"yes":            {"yes"},
		"nothing":        {""},
		"a closed input": nil,
	} {
		t.Run(name, func(t *testing.T) {
			c, home := fakeProd(t)
			mustRun(t, c, home, "cluster", "node-add", "root@h1")
			out, err := typing(t, c, home, lines, "cluster", "remove", "prod")
			if err == nil {
				t.Fatalf("removed:\n%s", out)
			}
			if !c.hosts["root@h1"].k3s {
				t.Error("k3s was uninstalled")
			}
			if got := nodeNames(t, home); len(got) != 1 {
				t.Errorf("nodes = %v", got)
			}
		})
	}
}

func TestClusterRemoveNeedsATerminal(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	out, err := onFake(t, c, home, "cluster", "remove", "prod")
	if err == nil || !strings.Contains(err.Error(), "terminal") {
		t.Fatalf("remove = %v\n%s", err, out)
	}
	if !c.hosts["root@h1"].k3s {
		t.Error("k3s was uninstalled")
	}
}

func TestClusterRemoveOfAnUnreachableNode(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	mustRun(t, c, home, "cluster", "node-add", "root@h2")
	c.hosts["root@h1"].unreachable = true

	out, err := typing(t, c, home, []string{"prod"}, "cluster", "remove", "prod")
	if err == nil || !strings.Contains(err.Error(), "--unreachable") {
		t.Fatalf("remove = %v\n%s", err, out)
	}
	if strings.Contains(out, "Type the cluster's name") {
		t.Errorf("it asked before refusing:\n%s", out)
	}
	if !c.hosts["root@h2"].k3s {
		t.Error("k3s was uninstalled from the node that can be reached")
	}

	out, err = typing(t, c, home, []string{"prod"}, "cluster", "remove", "prod", "--unreachable")
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	for _, want := range []string{
		"- k3s is uninstalled from node2 (root@h2): every app",
		"- node1 (root@h1) can't be reached: k3s stays there\n",
		"Removed cluster prod\n",
		"Warning: node1 (root@h1) couldn't be reached; if k3s still runs there, uninstall it there.\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output lacks %q:\n%s", want, out)
		}
	}
	if c.hosts["root@h2"].k3s {
		t.Error("node2 still has k3s")
	}
	if _, err := os.Stat(filepath.Join(home, "clusters", "default", "prod")); !os.IsNotExist(err) {
		t.Errorf("the cluster directory is still there: %v", err)
	}
}

func TestClusterRemoveSaysItsRemoteKeepsACopy(t *testing.T) {
	c, home := fakeProd(t)
	bare := filepath.Join(t.TempDir(), "remote.git")
	git(t, home, "init", "--bare", "-q", bare)
	if out, err := mooctl(t, home, home, "-c", "prod", "cluster", "remote", bare); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	out, err := typing(t, c, home, []string{"prod"}, "cluster", "remove", "prod")
	if err != nil || !strings.Contains(out, "- its git remote keeps what was pushed\n") {
		t.Fatalf("remove = %v\n%s", err, out)
	}
}

func TestClusterRemoveOfAClusterThatIsntThere(t *testing.T) {
	c, home := fakeProd(t)
	out, err := typing(t, c, home, []string{"gone"}, "cluster", "remove", "gone")
	if err == nil || !strings.Contains(err.Error(), "no cluster directory") {
		t.Errorf("remove = %v\n%s", err, out)
	}
}

func TestClusterRemoveWaitsForNoOtherOperation(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	mustRun(t, c, home, "cluster", "node-add", "root@h2")
	c.locked = true
	out, err := typing(t, c, home, []string{"prod"}, "cluster", "remove", "prod")
	if err == nil || !strings.Contains(err.Error(), "another mooctl operation") {
		t.Fatalf("remove = %v\n%s", err, out)
	}
	if !c.hosts["root@h2"].k3s || !c.hosts["root@h1"].k3s {
		t.Error("k3s was uninstalled while another operation ran")
	}
}

func TestShortImage(t *testing.T) {
	for image, want := range map[string]string{
		"moostack.local/shop/web@sha256:0661d8f5d56599a9d355618bc035ff3ff6e45800674aa9e8063287a57637e17c": "moostack.local/shop/web@sha256:0661d8f5d565",
		"moostack.local/shop/web@sha256:0661d8f5d565":                                                     "moostack.local/shop/web@sha256:0661d8f5d565",
		"moostack.local/shop/web@sha256:0661d8f5d5659":                                                    "moostack.local/shop/web@sha256:0661d8f5d565",
		"moostack.local/shop/jobs@sha256:1":                                                               "moostack.local/shop/jobs@sha256:1",
		"valkey/valkey:8":                                                                                 "valkey/valkey:8",
		"registry.example.com:5000/app:v1.2.3-0123456789abcdef":                                           "registry.example.com:5000/app:v1.2.3-0123456789abcdef",
		"app@0123456789abcdef0123":                                                                        "app@0123456789abcdef0123",
		"":                                                                                                "",
	} {
		if got := shortImage(image); got != want {
			t.Errorf("%q is shown as %q, want %q", image, got, want)
		}
	}
}

// A cluster directory named with -C is removed only under its own name.
func TestClusterRemoveRefusesANameThatIsntTheDirectorysCluster(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	dir := prodDir(t, home).Path
	out, err := typing(t, c, home, []string{"staging"}, "-C", dir, "cluster", "remove", "staging")
	if err == nil || !strings.Contains(err.Error(), "is cluster prod, not staging") {
		t.Errorf("remove = %v\n%s", err, out)
	}
	if !c.hosts["root@h1"].k3s {
		t.Error("k3s was uninstalled from prod's node")
	}
	if _, err := os.Stat(dir); err != nil {
		t.Errorf("prod's directory: %v", err)
	}
	if out, err := typing(t, c, home, []string{"prod"}, "-C", dir, "cluster", "remove", "prod"); err != nil || c.hosts["root@h1"].k3s {
		t.Errorf("remove under its own name = %v, k3s on h1 %v\n%s", err, c.hosts["root@h1"].k3s, out)
	}
}

// teammateAddsNode2 gives prod, which has node1, a remote and a
// teammate's node-add of node2 pushed there, which this machine hasn't
// fetched. It returns the teammate's commit.
func teammateAddsNode2(t *testing.T, c *fakeCluster, home string) string {
	t.Helper()
	dir := prodDir(t, home).Path
	mustRun(t, c, home, "cluster", "node-add", "root@h2")
	node2, err := os.ReadFile(filepath.Join(dir, "nodes", "node2.yml"))
	if err != nil {
		t.Fatal(err)
	}
	git(t, dir, "reset", "--quiet", "--hard", "HEAD~1")
	remote := filepath.Join(t.TempDir(), "prod.git")
	git(t, home, "init", "--bare", "--quiet", "--initial-branch=main", remote)
	mustRun(t, c, home, "cluster", "remote", remote)
	elsewhere := filepath.Join(t.TempDir(), "prod")
	git(t, home, "clone", "--quiet", remote, elsewhere)
	if err := os.WriteFile(filepath.Join(elsewhere, "nodes", "node2.yml"), node2, 0o644); err != nil {
		t.Fatal(err)
	}
	git(t, elsewhere, "add", ".")
	git(t, elsewhere, "commit", "--quiet", "-m", "node-add node2")
	git(t, elsewhere, "push", "--quiet")
	return git(t, elsewhere, "rev-parse", "HEAD")
}

func TestClusterRemoveTakesTheNodesFromTheRemote(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	c.hosts["root@h1"].files[AppliedCommitFile] = teammateAddsNode2(t, c, home) + "\n"
	out, err := typing(t, c, home, []string{"prod"}, "cluster", "remove", "prod")
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	if !strings.Contains(out, "- k3s is uninstalled from node2 (root@h2), node1 (root@h1)") {
		t.Errorf("the consequences don't name node2:\n%s", out)
	}
	if c.hosts["root@h2"].k3s || c.hosts["root@h1"].k3s {
		t.Errorf("k3s on h1 %v, on h2 %v", c.hosts["root@h1"].k3s, c.hosts["root@h2"].k3s)
	}
}

func TestClusterRemoveRefusesACommitTheDirectoryLacks(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	dir := prodDir(t, home).Path
	c.hosts["root@h1"].files[AppliedCommitFile] = strings.Repeat("ab", 20) + "\n"
	out, err := typing(t, c, home, []string{"prod"}, "cluster", "remove", "prod")
	if err == nil || !strings.Contains(err.Error(), "another machine changed the cluster") {
		t.Errorf("remove = %v\n%s", err, out)
	}
	if strings.Contains(out, "Type the cluster's name") {
		t.Errorf("it asked for the name:\n%s", out)
	}
	if !c.hosts["root@h1"].k3s {
		t.Error("k3s was uninstalled")
	}
	if _, err := os.Stat(dir); err != nil {
		t.Errorf("the directory: %v", err)
	}
}
