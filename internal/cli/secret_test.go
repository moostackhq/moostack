package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/crypto/ssh"
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

func TestSecretCommandsCommitAndPushTheirChanges(t *testing.T) {
	home := newHome(t)
	key := sshKey(t, home)
	if out, err := mooctl(t, home, home, "--ssh-key", key, "cluster", "init", "prod"); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	dir := filepath.Join(home, "clusters", "default", "prod")
	remote := filepath.Join(t.TempDir(), "prod.git")
	git(t, home, "init", "--bare", "--quiet", "--initial-branch=main", remote)
	if out, err := mooctl(t, home, home, "cluster", "remote", remote); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	secret := func(args ...string) string {
		t.Helper()
		out, err := mooctl(t, home, home, append([]string{"--ssh-key", key, "secret"}, args...)...)
		if err != nil {
			t.Fatalf("secret %s: %v\n%s", strings.Join(args, " "), err, out)
		}
		return out
	}
	pushed := func(msg string) {
		t.Helper()
		if got := git(t, dir, "log", "-1", "--format=%s"); got != msg {
			t.Errorf("last commit %q, want %q", got, msg)
		}
		if local, remote := git(t, dir, "rev-parse", "HEAD"), git(t, remote, "rev-parse", "main"); local != remote {
			t.Errorf("%q isn't pushed", msg)
		}
		if status := git(t, dir, "status", "--porcelain"); status != "" {
			t.Errorf("uncommitted after %q: %s", msg, status)
		}
	}

	editorWriting(t, "db:\n  value: hunter2\nsession:\n  generate: {type: hex}\n")
	secret("edit")
	pushed("secret edit")
	if out := secret("view"); !strings.Contains(out, "hunter2") {
		t.Errorf("view:\n%s", out)
	}
	head := git(t, dir, "rev-parse", "HEAD")
	secret("view")
	if git(t, dir, "rev-parse", "HEAD") != head {
		t.Error("view committed")
	}

	secret("rotate", "session")
	pushed("secret rotate")

	other := t.TempDir()
	teammate := sshKey(t, other)
	secret("recipients", "add", teammate+".pub")
	pushed("secret recipients add")
	data, _ := os.ReadFile(teammate + ".pub")
	pub, _, _, _, err := ssh.ParseAuthorizedKey(data)
	if err != nil {
		t.Fatal(err)
	}
	out := secret("recipients", "rm", ssh.FingerprintSHA256(pub))
	pushed("secret recipients rm")
	if strings.Contains(out, "Run mooctl secret rotate") {
		t.Errorf("rm gives a tip:\n%s", out)
	}
}

func TestSecretEditStartsFromTheRemote(t *testing.T) {
	home := newHome(t)
	key := sshKey(t, home)
	if out, err := mooctl(t, home, home, "--ssh-key", key, "cluster", "init", "prod"); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	dir := filepath.Join(home, "clusters", "default", "prod")
	remote := filepath.Join(t.TempDir(), "prod.git")
	git(t, home, "init", "--bare", "--quiet", "--initial-branch=main", remote)
	if out, err := mooctl(t, home, home, "cluster", "remote", remote); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	elsewhere := filepath.Join(t.TempDir(), "prod")
	git(t, home, "clone", "--quiet", remote, elsewhere)
	os.WriteFile(filepath.Join(elsewhere, "note"), []byte("x\n"), 0o644)
	git(t, elsewhere, "add", "note")
	git(t, elsewhere, "commit", "--quiet", "-m", "elsewhere")
	git(t, elsewhere, "push", "--quiet")

	editorWriting(t, "db:\n  value: x\n")
	if out, err := mooctl(t, home, home, "--ssh-key", key, "secret", "edit"); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	if _, err := os.Stat(filepath.Join(dir, "note")); err != nil {
		t.Error("edit didn't bring the directory up to its remote first")
	}
	if git(t, dir, "rev-parse", "HEAD") != git(t, remote, "rev-parse", "main") {
		t.Error("the edit isn't pushed")
	}
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

// secret view reads the store when the remote can't be reached.
func TestSecretViewWorksWithoutTheRemote(t *testing.T) {
	home := newHome(t)
	key := sshKey(t, home)
	if out, err := mooctl(t, home, home, "--ssh-key", key, "cluster", "init", "prod"); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	remote := filepath.Join(t.TempDir(), "prod.git")
	git(t, home, "init", "--bare", "--quiet", "--initial-branch=main", remote)
	if out, err := mooctl(t, home, home, "cluster", "remote", remote); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	editorWriting(t, "db:\n  value: hunter2\n")
	if out, err := mooctl(t, home, home, "--ssh-key", key, "secret", "edit"); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	os.RemoveAll(remote)
	out, err := mooctl(t, home, home, "--ssh-key", key, "secret", "view")
	if err != nil || !strings.Contains(out, "hunter2") {
		t.Errorf("view with the remote unreachable: %v\n%s", err, out)
	}
}
