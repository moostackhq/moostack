package gitrepo_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/moostackhq/moostack/internal/gitrepo"
)

// Push and SetRemote don't run the operator's global pre-push hook.
func TestPushIgnoresTheOperatorsPrePushHook(t *testing.T) {
	a, _ := setup(t)
	ctx := t.Context()
	hooks := t.TempDir()
	os.WriteFile(filepath.Join(hooks, "pre-push"), []byte("#!/bin/sh\necho operator-hook-ran >&2\nexit 1\n"), 0o755)
	exec.Command("git", "config", "--global", "core.hooksPath", hooks).Run()
	write(t, a.Dir, "nodes.yml", "x\n")
	if _, err := a.Commit(ctx, "node-add node1"); err != nil {
		t.Fatal(err)
	}
	if err := a.Push(ctx); err != nil {
		t.Errorf("Push ran the operator's pre-push hook: %v", err)
	}
	r, err := gitrepo.Init(ctx, filepath.Join(t.TempDir(), "c"))
	if err != nil {
		t.Fatal(err)
	}
	write(t, r.Dir, "cluster.yml", "ref: c\n")
	r.Commit(ctx, "init")
	bare := filepath.Join(t.TempDir(), "r.git")
	exec.Command("git", "init", "--bare", "--quiet", "--initial-branch=main", bare).Run()
	if err := r.SetRemote(ctx, bare); err != nil {
		t.Errorf("SetRemote ran the operator's pre-push hook: %v", err)
	}
}

// The operator's push.gpgSign doesn't apply to mooctl's pushes.
func TestPushDoesntSign(t *testing.T) {
	a, _ := setup(t)
	ctx := t.Context()
	exec.Command("git", "config", "--global", "push.gpgSign", "true").Run()
	write(t, a.Dir, "nodes.yml", "x\n")
	a.Commit(ctx, "node-add node1")
	if err := a.Push(ctx); err != nil {
		t.Errorf("Push signs with the operator's push.gpgSign: %v", err)
	}
}

// A token given as a URL's user, as GitHub's https://<token>@github.com,
// is redacted.
func TestRedactHidesATokenGivenAsTheUser(t *testing.T) {
	in := "https://ghp_s3cretT0ken@github.com/acme/prod.git"
	if got := gitrepo.Redact(in); strings.Contains(got, "s3cret") {
		t.Errorf("Redact(%q) = %q", in, got)
	}
}

// A cancelled git command's error names the command.
func TestACancelledCommitNamesItsCommand(t *testing.T) {
	a, _ := setup(t)
	write(t, a.Dir, "x.yml", "x\n")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := a.Commit(ctx, "m")
	if err == nil || strings.Contains(err.Error(), "git -c:") {
		t.Errorf("Commit on an ended context = %v", err)
	}
}

// A failed push to a URL with credentials doesn't show them.
func TestSetRemoteErrorsHideCredentials(t *testing.T) {
	isolate(t)
	ctx := t.Context()
	r, err := gitrepo.Init(ctx, filepath.Join(t.TempDir(), "c"))
	if err != nil {
		t.Fatal(err)
	}
	write(t, r.Dir, "cluster.yml", "ref: c\n")
	r.Commit(ctx, "init")
	t.Setenv("GIT_TERMINAL_PROMPT", "0")
	for _, u := range []string{"https://op:s3cret@127.0.0.1:1/c.git", "https://s3cretTok@127.0.0.1:1/c.git"} {
		err := r.SetRemote(ctx, u)
		if err == nil || strings.Contains(err.Error(), "s3cret") {
			t.Errorf("SetRemote(%s) = %v", u, err)
		}
	}
}

// IsAncestor ignores GIT_DIR in the operator's environment.
func TestIsAncestorIgnoresGitDir(t *testing.T) {
	a, _ := setup(t)
	ctx := t.Context()
	head, _ := a.Head(ctx)
	if ok, err := a.IsAncestor(ctx, head); !ok || err != nil {
		t.Fatalf("HEAD before GIT_DIR: %v, %v", ok, err)
	}
	other, err := gitrepo.Init(ctx, filepath.Join(t.TempDir(), "other"))
	if err != nil {
		t.Fatal(err)
	}
	write(t, other.Dir, "x", "x\n")
	other.Commit(ctx, "other")
	t.Setenv("GIT_DIR", filepath.Join(other.Dir, ".git"))
	if ok, err := a.IsAncestor(ctx, head); !ok || err != nil {
		t.Errorf("with GIT_DIR set elsewhere, IsAncestor(HEAD) = %v, %v", ok, err)
	}
}
