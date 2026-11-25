package gitrepo_test

import (
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/moostackhq/moostack/internal/gitrepo"
)

// isolate gives git an identity and no user or system config.
func isolate(t *testing.T) {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")
	t.Setenv("GIT_AUTHOR_NAME", "Op")
	t.Setenv("GIT_AUTHOR_EMAIL", "op@example.com")
	t.Setenv("GIT_COMMITTER_NAME", "Op")
	t.Setenv("GIT_COMMITTER_EMAIL", "op@example.com")
	for _, kv := range [][2]string{{"user.name", "Op"}, {"user.email", "op@example.com"}} {
		if out, err := exec.Command("git", "config", "--global", kv[0], kv[1]).CombinedOutput(); err != nil {
			t.Fatalf("%v: %s", err, out)
		}
	}
}

func write(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

// setup is a repo pushed to a bare remote, and a clone of it.
func setup(t *testing.T) (a, b gitrepo.Repo) {
	t.Helper()
	isolate(t)
	ctx := t.Context()
	base := t.TempDir()
	remote := filepath.Join(base, "remote.git")
	if out, err := exec.Command("git", "init", "--bare", "--quiet", "--initial-branch=main", remote).CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	a, err := gitrepo.Init(ctx, filepath.Join(base, "a"))
	if err == nil {
		write(t, a.Dir, "cluster.yml", "ref: c\n")
		_, err = a.Commit(ctx, "cluster init c")
	}
	if err == nil {
		err = a.SetRemote(ctx, remote)
	}
	if err == nil {
		err = a.Push(ctx)
	}
	if err == nil {
		b, err = gitrepo.Clone(ctx, remote, filepath.Join(base, "b"))
	}
	if err != nil {
		t.Fatal(err)
	}
	return a, b
}

func TestUpdateFastForwards(t *testing.T) {
	a, b := setup(t)
	ctx := t.Context()
	write(t, a.Dir, "node1.yml", "x\n")
	first, err := a.Commit(ctx, "node-add node1")
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Push(ctx); err != nil {
		t.Fatal(err)
	}
	if ok, _ := b.IsAncestor(ctx, first); ok {
		t.Fatal("b has a's commit before it fetched")
	}
	if err := b.Update(ctx); err != nil {
		t.Fatal(err)
	}
	if head, _ := b.Head(ctx); head != first {
		t.Errorf("b is at %s, want %s", head, first)
	}
	// Ahead of the remote is fine.
	write(t, b.Dir, "node2.yml", "y\n")
	if _, err := b.Commit(ctx, "node-add node2"); err != nil {
		t.Fatal(err)
	}
	if err := b.Update(ctx); err != nil {
		t.Errorf("ahead: %v", err)
	}
}

func TestUpdateRefusesDivergedHistory(t *testing.T) {
	a, b := setup(t)
	ctx := t.Context()
	write(t, a.Dir, "node1.yml", "a\n")
	a.Commit(ctx, "node-add node1")
	if err := a.Push(ctx); err != nil {
		t.Fatal(err)
	}
	write(t, b.Dir, "node1.yml", "b\n")
	before, _ := b.Commit(ctx, "node-add node1")
	if err := b.Update(ctx); !errors.Is(err, gitrepo.ErrDiverged) {
		t.Errorf("Update = %v, want ErrDiverged", err)
	}
	if head, _ := b.Head(ctx); head != before {
		t.Error("a refused update moved HEAD")
	}
}

func TestCommitWithNothingToCommitKeepsHead(t *testing.T) {
	a, _ := setup(t)
	ctx := t.Context()
	head, _ := a.Head(ctx)
	got, err := a.Commit(ctx, "nothing")
	if err != nil || got != head {
		t.Errorf("Commit = %s, %v, want %s", got, err, head)
	}
}

func TestIsAncestorOfAnUnknownCommitIsFalse(t *testing.T) {
	a, _ := setup(t)
	if ok, err := a.IsAncestor(t.Context(), "0123456789abcdef0123456789abcdef01234567"); ok || err != nil {
		t.Errorf("IsAncestor = %v, %v", ok, err)
	}
}

func TestOpenRefusesADirectoryInsideAnotherRepo(t *testing.T) {
	a, _ := setup(t)
	inner := filepath.Join(a.Dir, "inner")
	os.MkdirAll(inner, 0o755)
	if _, err := gitrepo.Open(t.Context(), inner); err == nil || !strings.Contains(err.Error(), "no git repository of its own") {
		t.Errorf("Open = %v", err)
	}
	if _, err := gitrepo.Open(t.Context(), t.TempDir()); err == nil {
		t.Error("a directory outside any repo was taken")
	}
	if _, err := gitrepo.Open(t.Context(), a.Dir); err != nil {
		t.Error(err)
	}
	t.Setenv("GIT_DIR", filepath.Join(t.TempDir(), "elsewhere"))
	if _, err := gitrepo.Open(t.Context(), a.Dir); err != nil {
		t.Errorf("GIT_DIR in the environment: %v", err)
	}
}

func TestUpdateSaysWhatStopsIt(t *testing.T) {
	a, b := setup(t)
	ctx := t.Context()
	write(t, a.Dir, "cluster.yml", "ref: changed\n")
	a.Commit(ctx, "change")
	a.Push(ctx)
	write(t, b.Dir, "cluster.yml", "ref: local edit\n")
	err := b.Update(ctx)
	if err == nil || errors.Is(err, gitrepo.ErrDiverged) || !strings.Contains(err.Error(), "commit or discard them") {
		t.Errorf("an uncommitted local change: %v", err)
	}
	exec.Command("git", "-C", b.Dir, "checkout", "--quiet", "--", "cluster.yml").Run()
	exec.Command("git", "-C", b.Dir, "checkout", "--quiet", "--detach").Run()
	if err := b.Update(ctx); err == nil || !strings.Contains(err.Error(), "isn't on a branch") {
		t.Errorf("a detached HEAD: %v", err)
	}
}

func TestCommitIgnoresTheOperatorsHooksSigningAndExcludes(t *testing.T) {
	a, _ := setup(t)
	ctx := t.Context()
	hooks := t.TempDir()
	os.WriteFile(filepath.Join(hooks, "pre-commit"), []byte("#!/bin/sh\nexit 1\n"), 0o755)
	excludes := filepath.Join(t.TempDir(), "excludes")
	os.WriteFile(excludes, []byte("secrets*\n"), 0o644)
	for _, kv := range [][2]string{{"core.hooksPath", hooks}, {"commit.gpgsign", "true"}, {"gpg.program", "false"}, {"core.excludesFile", excludes}} {
		exec.Command("git", "config", "--global", kv[0], kv[1]).Run()
	}
	write(t, a.Dir, "secrets.enc.yml", "x\n")
	if _, err := a.Commit(ctx, "secret edit"); err != nil {
		t.Fatal(err)
	}
	out, _ := exec.Command("git", "-C", a.Dir, "ls-files").Output()
	if !strings.Contains(string(out), "secrets.enc.yml") {
		t.Errorf("the store isn't committed: %s", out)
	}
}

func TestSetRemoteSetsNothingWhenThePushFails(t *testing.T) {
	isolate(t)
	ctx := t.Context()
	r, err := gitrepo.Init(ctx, filepath.Join(t.TempDir(), "c"))
	if err != nil {
		t.Fatal(err)
	}
	write(t, r.Dir, "cluster.yml", "ref: c\n")
	r.Commit(ctx, "init")
	if err := r.SetRemote(ctx, filepath.Join(t.TempDir(), "missing.git")); err == nil {
		t.Fatal("a push to nowhere passed")
	}
	if has, _ := r.HasRemote(ctx); has {
		t.Error("the remote was set though the push failed")
	}
}

func TestRedactHidesAURLsPassword(t *testing.T) {
	for in, want := range map[string]string{
		"https://op:s3cret@git.example.com/c.git":                       "https://xxxxx@git.example.com/c.git",
		"https://ghp_t0ken@github.com/acme/c.git":                       "https://xxxxx@github.com/acme/c.git",
		"git@github.com:acme/c.git":                                     "git@github.com:acme/c.git",
		"ssh://git@git.example.com/c.git":                               "ssh://git@git.example.com/c.git",
		"ssh://op:s3cret@git.example.com/c.git":                         "ssh://op:xxxxx@git.example.com/c.git",
		"https://git.example.com/c.git":                                 "https://git.example.com/c.git",
		"https://op:a%zz@git.example.com/c.git":                         "https://xxxxx@git.example.com/c.git",
		"https://op:p@ss@git.example.com:x/c.git":                       "https://xxxxx@git.example.com:x/c.git",
		"fatal: unable to access 'https://op:s3cret@h/c.git/': refused": "fatal: unable to access 'https://xxxxx@h/c.git/': refused",
	} {
		if got := gitrepo.Redact(in); got != want {
			t.Errorf("Redact(%q) = %q", in, got)
		}
	}
	isolate(t)
	_, err := gitrepo.Clone(t.Context(), "https://op:s3cret@127.0.0.1:1/c.git", filepath.Join(t.TempDir(), "c"))
	if err == nil || strings.Contains(err.Error(), "s3cret") {
		t.Errorf("a clone's error shows the password: %v", err)
	}
}

func TestAFetchFromARemoteThatNeverAnswersEnds(t *testing.T) {
	a, _ := setup(t)
	ctx := t.Context()
	defer gitrepo.SetFetchTimeout(2 * time.Second)()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			defer c.Close()
		}
	}()
	exec.Command("git", "-C", a.Dir, "remote", "set-url", "origin", "http://"+l.Addr().String()+"/c.git").Run()
	start := time.Now()
	err = a.Update(ctx)
	if err == nil || !strings.Contains(err.Error(), "no answer") {
		t.Errorf("Update = %v", err)
	}
	if took := time.Since(start); took > 15*time.Second {
		t.Errorf("it took %v", took)
	}
}

func TestAFetchFromARemoteThatAsksForALoginFailsWithoutAsking(t *testing.T) {
	a, _ := setup(t)
	asks := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("WWW-Authenticate", `Basic realm="git"`)
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer asks.Close()
	exec.Command("git", "-C", a.Dir, "remote", "set-url", "origin", asks.URL+"/c.git").Run()
	defer gitrepo.SetFetchTimeout(10 * time.Second)()
	waits := filepath.Join(t.TempDir(), "askpass")
	if err := os.WriteFile(waits, []byte("#!/bin/sh\nsleep 30\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"GIT_ASKPASS", "SSH_ASKPASS"} {
		t.Setenv(name, waits)
	}
	exec.Command("git", "config", "--global", "core.askPass", waits).Run()
	t.Setenv("GIT_TERMINAL_PROMPT", "1")
	start := time.Now()
	err := a.Update(t.Context())
	if err == nil || !strings.Contains(err.Error(), "terminal prompts disabled") {
		t.Errorf("Update = %v, want git's refusal to ask on the terminal", err)
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Errorf("it took %v", took)
	}
}
