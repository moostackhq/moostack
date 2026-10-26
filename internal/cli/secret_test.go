package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// editorWriting sets $EDITOR to one that writes text into the file it
// edits.
func editorWriting(t *testing.T, text string) {
	t.Helper()
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "text"), []byte(text), 0o600)
	script := filepath.Join(dir, "editor")
	os.WriteFile(script, []byte("#!/bin/sh\nfor f; do :; done\ncat '"+filepath.Join(dir, "text")+"' > \"$f\"\n"), 0o700)
	t.Setenv("VISUAL", "")
	t.Setenv("EDITOR", script)
}

// A secret command refuses before it changes the store when git has no
// identity.
func TestASecretCommandChecksGitsIdentityFirst(t *testing.T) {
	home := newHome(t)
	key := sshKey(t, home)
	if out, err := mooctl(t, home, home, "--ssh-key", key, "cluster", "init", "prod"); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	dir := filepath.Join(home, "clusters", "default", "prod")
	git(t, home, "config", "--global", "--unset", "user.name")
	editorWriting(t, "db:\n  value: hunter2\n")
	out, err := mooctl(t, home, home, "--ssh-key", key, "secret", "edit")
	if err == nil {
		t.Fatalf("committed without an identity:\n%s", out)
	}
	if st := git(t, dir, "status", "--porcelain"); st != "" {
		t.Errorf("refused (%v) after the store changed: %s", err, st)
	}
	git(t, home, "config", "--global", "user.name", "Op")
	out, _ = mooctl(t, home, home, "--ssh-key", key, "secret", "edit")
	if st := git(t, dir, "status", "--porcelain"); st != "" {
		t.Errorf("the rerun left the change uncommitted (%s): %s", strings.TrimSpace(out), st)
	}
}
