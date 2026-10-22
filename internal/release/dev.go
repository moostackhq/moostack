package release

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"regexp"
	"strings"
	"unicode"
)

// devVersion is the shape of a development build's version: its
// checkout's commit, 12 to 40 lowercase hex digits, and a dirty marker
// if the checkout had changes.
var devVersion = regexp.MustCompile(`^[0-9a-f]{12,40}(-dirty)?$`)

// IsDevVersion reports whether version is a development build's, as
// DevVersion makes it, rather than a release's.
func IsDevVersion(version string) bool { return devVersion.MatchString(version) }

// DevVersion is the version of a development build from the git
// checkout at checkout: its commit, abbreviated to 12 digits, followed
// by -dirty if the checkout has uncommitted changes, untracked files
// included.
func DevVersion(ctx context.Context, checkout string) (string, error) {
	commit, err := gitOutput(ctx, checkout, "rev-parse", "--short=12", "--verify", "HEAD")
	if err != nil {
		return "", err
	}
	version := strings.TrimSpace(commit)
	dirty, err := hasChanges(ctx, checkout)
	if err != nil {
		return "", err
	}
	if dirty {
		version += "-dirty"
	}
	if !IsDevVersion(version) {
		return "", fmt.Errorf("the commit of %s, %q, isn't a commit hash", checkout, version)
	}
	return version, nil
}

// hasChanges reports whether the checkout differs from its commit in a way
// a build could see, whatever the contributor's git settings hide: a
// change git status lists, an untracked file that only the checkout's own
// .gitignore files may rule out, or a file git is told not to check, as
// skip-worktree or assume-unchanged, which counts as changed.
func hasChanges(ctx context.Context, checkout string) (bool, error) {
	for _, args := range [][]string{
		{"status", "--porcelain", "--untracked-files=normal"},
		{"ls-files", "--others", "--exclude-per-directory=.gitignore"},
	} {
		out, err := gitOutput(ctx, checkout, args...)
		if err != nil {
			return false, err
		}
		if strings.TrimSpace(out) != "" {
			return true, nil
		}
	}
	// ls-files -v tags a skip-worktree file S, and an assume-unchanged one
	// with a lowercase letter.
	out, err := gitOutput(ctx, checkout, "ls-files", "-v")
	if err != nil {
		return false, err
	}
	for _, line := range strings.Split(out, "\n") {
		if line != "" && (line[0] == 'S' || unicode.IsLower(rune(line[0]))) {
			return true, nil
		}
	}
	return false, nil
}

// gitOutput runs git with args in the checkout at dir, and returns what
// it printed.
func gitOutput(ctx context.Context, dir string, args ...string) (string, error) {
	var stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", dir}, args...)...)
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("%s isn't a git checkout with a commit: git %s: %w: %s", dir, strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return string(out), nil
}
