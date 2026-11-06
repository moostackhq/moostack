package cli

import (
	"path/filepath"
	"strings"
	"testing"
)

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
