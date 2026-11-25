// Package gitrepo runs the system git, with the operator's SSH agent and
// credential helpers, on a cluster directory.
package gitrepo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"syscall"
	"time"
)

// remote is the name of the remote that mooctl pushes to.
const remote = "origin"

// fetchTimeout bounds a fetch from the remote.
var fetchTimeout = time.Minute

// Repo is a git repository's working tree.
type Repo struct {
	Dir string
}

// git runs git with args in the repo, and returns its trimmed stdout.
func (r Repo) git(ctx context.Context, args ...string) (string, error) {
	return run(ctx, r.Dir, args...)
}

// repoEnv are the environment variables that point git at another
// repository or index than the directory's own.
var repoEnv = []string{"GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GIT_OBJECT_DIRECTORY", "GIT_COMMON_DIR", "GIT_NAMESPACE", "GIT_CEILING_DIRECTORIES"}

// settings keep the operator's hooks, signing, global excludes and
// program that asks for a password out of every git command mooctl runs.
var settings = []string{
	"-c", "core.hooksPath=/dev/null",
	"-c", "commit.gpgSign=false",
	"-c", "push.gpgSign=false",
	"-c", "merge.verifySignatures=false",
	"-c", "core.excludesFile=/dev/null",
	"-c", "core.askPass=",
}

// askEnv are the environment variables that name a program git asks for
// a user name or a password with.
var askEnv = []string{"GIT_ASKPASS", "SSH_ASKPASS"}

// command is git with args in dir, with settings, without the
// environment variables of repoEnv and askEnv, in a process group of its
// own, which the terminal's signals don't reach and which is killed once
// ctx ends. It fails where git would ask for a user name or a password.
func command(ctx context.Context, dir string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, "git", append(slices.Clone(settings), args...)...)
	cmd.Dir = dir
	for _, kv := range os.Environ() {
		name, _, _ := strings.Cut(kv, "=")
		if !slices.Contains(repoEnv, name) && !slices.Contains(askEnv, name) {
			cmd.Env = append(cmd.Env, kv)
		}
	}
	cmd.Env = append(cmd.Env, "GIT_TERMINAL_PROMPT=0")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = 5 * time.Second
	return cmd
}

func run(ctx context.Context, dir string, args ...string) (string, error) {
	cmd := command(ctx, dir, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return "", fmt.Errorf("git %s: %w", args[0], context.Cause(ctx))
		}
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = err.Error()
		}
		return "", fmt.Errorf("git %s: %s", redactArgs(args), redact(msg))
	}
	return strings.TrimSpace(stdout.String()), nil
}

// redactArgs is args joined by spaces, with any password in a URL among
// them replaced.
func redactArgs(args []string) string {
	out := make([]string, len(args))
	for i, a := range args {
		out[i] = redact(a)
	}
	return strings.Join(out, " ")
}

// redact is s, with the password of a URL replaced.
func redact(s string) string {
	s = httpUser.ReplaceAllString(s, "${1}xxxxx@")
	return urlPassword.ReplaceAllString(s, "${1}:xxxxx@")
}

// httpUser matches the user, and password if any, of an http or https
// URL, which may hold a token; urlPassword the password of a URL of any
// other scheme. Each runs up to the last @ before the URL's path.
var (
	httpUser    = regexp.MustCompile(`(?i)(https?://)[^/\s]*@`)
	urlPassword = regexp.MustCompile(`([A-Za-z][A-Za-z0-9+.-]*://[^/:@\s]*):[^/\s]*@`)
)

// Redact is s with the user and password of each http or https URL in
// it, and the password of any other URL, replaced.
func Redact(s string) string { return redact(s) }

// Init makes dir a repository on the branch main.
func Init(ctx context.Context, dir string) (Repo, error) {
	if _, err := run(ctx, "", "init", "--quiet", "--initial-branch=main", "--", dir); err != nil {
		return Repo{}, err
	}
	return Repo{Dir: dir}, nil
}

// Clone clones url into dir.
func Clone(ctx context.Context, url, dir string) (Repo, error) {
	if _, err := run(ctx, "", "clone", "--quiet", "--origin", remote, "--", url, dir); err != nil {
		return Repo{}, err
	}
	return Repo{Dir: dir}, nil
}

// Open is the repository at dir, which must be the root of its own
// repository.
func Open(ctx context.Context, dir string) (Repo, error) {
	top, err := run(ctx, dir, "rev-parse", "--show-toplevel")
	if err != nil {
		return Repo{}, fmt.Errorf("%s isn't a git repository: %w", dir, err)
	}
	want, err := filepath.EvalSymlinks(dir)
	if err != nil {
		return Repo{}, err
	}
	got, err := filepath.EvalSymlinks(top)
	if err != nil {
		return Repo{}, err
	}
	if want, err = filepath.Abs(want); err != nil {
		return Repo{}, err
	}
	if got != want {
		return Repo{}, fmt.Errorf("%s has no git repository of its own, but is inside %s", dir, got)
	}
	return Repo{Dir: dir}, nil
}

// CheckIdentity fails unless git knows who commits.
func (r Repo) CheckIdentity(ctx context.Context) error {
	for _, key := range []string{"user.name", "user.email"} {
		if v, err := r.git(ctx, "config", "--get", key); err != nil || v == "" {
			return fmt.Errorf("git has no %s to commit with; set it with git config --global %s ...", key, key)
		}
	}
	return nil
}

// HasRemote reports whether the repo has the remote mooctl pushes to.
func (r Repo) HasRemote(ctx context.Context) (bool, error) {
	out, err := r.git(ctx, "remote")
	if err != nil {
		return false, err
	}
	for _, name := range strings.Fields(out) {
		if name == remote {
			return true, nil
		}
	}
	return false, nil
}

// SetRemote pushes the current branch to url, and only then makes url
// the remote mooctl pushes to.
func (r Repo) SetRemote(ctx context.Context, url string) error {
	if _, err := r.git(ctx, "push", "--quiet", "--", url, "HEAD"); err != nil {
		return err
	}
	has, err := r.HasRemote(ctx)
	if err != nil {
		return err
	}
	if has {
		_, err = r.git(ctx, "remote", "set-url", remote, url)
	} else {
		_, err = r.git(ctx, "remote", "add", remote, url)
	}
	if err != nil {
		return err
	}
	return r.Push(ctx)
}

// ErrDiverged is returned by Update when the local branch and the
// remote's have both changed.
var ErrDiverged = errors.New("the cluster directory and its remote have diverged")

// Update fetches the remote, for fetchTimeout at most, and fast-forwards
// to it. A remote without the branch is left as it is.
func (r Repo) Update(ctx context.Context) error {
	fetchCtx, cancel := context.WithTimeout(ctx, fetchTimeout)
	defer cancel()
	if _, err := r.git(fetchCtx, "fetch", "--quiet", remote); err != nil {
		if ctx.Err() == nil && fetchCtx.Err() != nil {
			return fmt.Errorf("fetching from %s: no answer in %v", remote, fetchTimeout)
		}
		return err
	}
	branch, err := r.git(ctx, "symbolic-ref", "--quiet", "--short", "HEAD")
	if err != nil {
		return fmt.Errorf("%s isn't on a branch; check one out", r.Dir)
	}
	upstream := remote + "/" + branch
	if _, err := r.git(ctx, "rev-parse", "--verify", "--quiet", upstream); err != nil {
		return nil
	}
	if ahead, err := r.IsAncestor(ctx, upstream); err != nil || ahead {
		return err
	}
	head, err := r.Head(ctx)
	if err != nil {
		return err
	}
	if head != "" {
		behind, err := r.isAncestorOf(ctx, head, upstream)
		if err != nil {
			return err
		}
		if !behind {
			return ErrDiverged
		}
	}
	if _, err := r.git(ctx, "merge", "--ff-only", "--quiet", upstream); err != nil {
		return fmt.Errorf("the cluster directory has changes that the remote's commits would overwrite; commit or discard them in %s: %w", r.Dir, err)
	}
	return nil
}

// Head is the commit HEAD is at, empty in a repo without commits.
func (r Repo) Head(ctx context.Context) (string, error) {
	if _, err := r.git(ctx, "rev-parse", "--verify", "--quiet", "HEAD"); err != nil {
		return "", nil
	}
	return r.git(ctx, "rev-parse", "HEAD")
}

// IsAncestor reports whether commit is HEAD or one of its ancestors. A
// commit the repo doesn't have isn't.
func (r Repo) IsAncestor(ctx context.Context, commit string) (bool, error) {
	return r.isAncestorOf(ctx, commit, "HEAD")
}

// isAncestorOf reports whether commit is of or one of its ancestors.
func (r Repo) isAncestorOf(ctx context.Context, commit, of string) (bool, error) {
	if _, err := r.git(ctx, "rev-parse", "--verify", "--quiet", "--end-of-options", commit+"^{commit}"); err != nil {
		return false, nil
	}
	cmd := command(ctx, r.Dir, "merge-base", "--is-ancestor", "--end-of-options", commit, of)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := cmd.Run()
	var exit *exec.ExitError
	switch {
	case err == nil:
		return true, nil
	case errors.As(err, &exit) && exit.ExitCode() == 1:
		return false, nil
	case ctx.Err() != nil:
		return false, fmt.Errorf("git merge-base: %w", context.Cause(ctx))
	}
	msg := strings.TrimSpace(stderr.String())
	if msg == "" {
		msg = err.Error()
	}
	return false, fmt.Errorf("git merge-base: %s", msg)
}

// Commit commits every change in the working tree with message, and
// returns the commit HEAD is at after. While another git command holds
// the index, it tries again, for 5 seconds.
func (r Repo) Commit(ctx context.Context, message string) (string, error) {
	var err error
	for range 25 {
		if err = r.commit(ctx, message); err == nil || !strings.Contains(err.Error(), "index.lock") {
			break
		}
		select {
		case <-ctx.Done():
			return "", err
		case <-time.After(200 * time.Millisecond):
		}
	}
	if err != nil {
		return "", err
	}
	return r.Head(ctx)
}

func (r Repo) commit(ctx context.Context, message string) error {
	if _, err := r.git(ctx, "add", "--all"); err != nil {
		return err
	}
	// diff --quiet fails when something is staged.
	if _, err := r.git(ctx, "diff", "--cached", "--quiet"); err == nil {
		return nil
	}
	_, err := r.git(ctx, "commit", "--quiet", "--no-verify", "--message", message)
	return err
}

// Push pushes the current branch to the remote.
func (r Repo) Push(ctx context.Context) error {
	_, err := r.git(ctx, "push", "--quiet", "--set-upstream", remote, "HEAD")
	return err
}
