package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

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
