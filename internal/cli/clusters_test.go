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
