package release_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/moostackhq/moostack/internal/release"
)

// gitCheckout makes a git repository with one commit of a file, and of
// more files, as name and content in turn, and returns it.
func gitCheckout(t *testing.T, more ...string) string {
	t.Helper()
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "main.go"), "package main\n")
	for i := 0; i+1 < len(more); i += 2 {
		writeFile(t, filepath.Join(dir, more[i]), more[i+1])
	}
	git(t, dir, "init", "-q")
	git(t, dir, "add", ".")
	git(t, dir, "-c", "user.name=t", "-c", "user.email=t@example.com", "commit", "-q", "-m", "first")
	return dir
}

// git runs git with args in dir, and returns what it printed.
func git(t *testing.T, dir string, args ...string) string {
	t.Helper()
	out, err := exec.Command("git", append([]string{"-C", dir}, args...)...).CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out))
}

func TestADevelopmentBuildIsVersionedAsItsCommit(t *testing.T) {
	dir := gitCheckout(t)
	got, err := release.DevVersion(t.Context(), dir)
	if err != nil {
		t.Fatal(err)
	}
	commit := git(t, dir, "rev-parse", "HEAD")
	if !regexp.MustCompile(`^[0-9a-f]{12}$`).MatchString(got) || !strings.HasPrefix(commit, got) {
		t.Errorf("a clean checkout at %s is versioned %q, want its 12-digit commit", commit, got)
	}
}

func TestADevelopmentBuildOfACheckoutWithChangesIsMarkedDirty(t *testing.T) {
	for name, change := range map[string]func(dir string){
		"a modified file":   func(dir string) { writeFile(t, filepath.Join(dir, "main.go"), "package main // changed\n") },
		"an untracked file": func(dir string) { writeFile(t, filepath.Join(dir, "new.go"), "package main\n") },
		"a staged file": func(dir string) {
			writeFile(t, filepath.Join(dir, "staged.go"), "package main\n")
			git(t, dir, "add", "staged.go")
		},
		"a deleted file": func(dir string) { os.Remove(filepath.Join(dir, "main.go")) },
	} {
		t.Run(name, func(t *testing.T) {
			dir := gitCheckout(t)
			change(dir)
			got, err := release.DevVersion(t.Context(), dir)
			if err != nil {
				t.Fatal(err)
			}
			if want := git(t, dir, "rev-parse", "--short=12", "HEAD") + "-dirty"; got != want {
				t.Errorf("the checkout with %s is versioned %q, want %q", name, got, want)
			}
		})
	}
}

// The contributor's own git settings don't hide a change from the
// marker: only the checkout's .gitignore files say what isn't part of it.
func TestAChangeGitIsSetToHideMarksABuildDirty(t *testing.T) {
	for name, hide := range map[string]func(t *testing.T, dir string){
		"untracked files hidden from status": func(t *testing.T, dir string) {
			git(t, dir, "config", "status.showUntrackedFiles", "no")
			writeFile(t, filepath.Join(dir, "new.go"), "package main\n")
		},
		"an untracked file in the contributor's excludes": func(t *testing.T, dir string) {
			excludes := filepath.Join(t.TempDir(), "excludes")
			writeFile(t, excludes, "*_local.go\n")
			git(t, dir, "config", "core.excludesFile", excludes)
			writeFile(t, filepath.Join(dir, "debug_local.go"), "package main\n")
		},
		"an untracked file in .git/info/exclude": func(t *testing.T, dir string) {
			writeFile(t, filepath.Join(dir, ".git", "info", "exclude"), "*_local.go\n")
			writeFile(t, filepath.Join(dir, "debug_local.go"), "package main\n")
		},
		"a change to a file marked skip-worktree": func(t *testing.T, dir string) {
			git(t, dir, "update-index", "--skip-worktree", "main.go")
			writeFile(t, filepath.Join(dir, "main.go"), "package main // local\n")
		},
		"a change to a file marked assume-unchanged": func(t *testing.T, dir string) {
			git(t, dir, "update-index", "--assume-unchanged", "main.go")
			writeFile(t, filepath.Join(dir, "main.go"), "package main // local\n")
		},
	} {
		t.Run(name, func(t *testing.T) {
			dir := gitCheckout(t)
			hide(t, dir)
			got, err := release.DevVersion(t.Context(), dir)
			if err != nil {
				t.Fatal(err)
			}
			if want := git(t, dir, "rev-parse", "--short=12", "HEAD") + "-dirty"; got != want {
				t.Errorf("the checkout with %s is versioned %q, want %q", name, got, want)
			}
		})
	}
}

// What the checkout's own .gitignore ignores, as its build output, isn't
// a change.
func TestAFileTheCheckoutIgnoresLeavesABuildClean(t *testing.T) {
	dir := gitCheckout(t, ".gitignore", "/target/\n")
	writeFile(t, filepath.Join(dir, "target", "out"), "built\n")
	got, err := release.DevVersion(t.Context(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if want := git(t, dir, "rev-parse", "--short=12", "HEAD"); got != want {
		t.Errorf("the checkout with ignored build output is versioned %q, want %q", got, want)
	}
}

// A version names images, so git's answer is checked before it is used.
func TestADevelopmentBuildRefusesACommitThatIsntAHash(t *testing.T) {
	dir := gitCheckout(t)
	bin := t.TempDir()
	writeFile(t, filepath.Join(bin, "git"), "#!/bin/sh\necho '../evil'\n")
	if err := os.Chmod(filepath.Join(bin, "git"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	_, err := release.DevVersion(t.Context(), dir)
	if err == nil || !strings.Contains(err.Error(), "isn't a commit hash") {
		t.Errorf("DevVersion with a git that answers ../evil: %v, want it refused", err)
	}
}

func TestADevelopmentBuildNeedsAGitCheckoutWithACommit(t *testing.T) {
	empty := t.TempDir()
	git(t, empty, "init", "-q")
	for name, dir := range map[string]string{
		"a directory outside git":       t.TempDir(),
		"a repository without a commit": empty,
		"a missing directory":           filepath.Join(t.TempDir(), "missing"),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := release.DevVersion(t.Context(), dir)
			if err == nil || !strings.Contains(err.Error(), dir) {
				t.Errorf("DevVersion of %s: %v, want an error naming it", name, err)
			}
		})
	}
}

func TestADevelopmentBuildsVersionIsToldFromAReleases(t *testing.T) {
	for version, want := range map[string]bool{
		"0123456789ab":       true,
		"0123456789ab-dirty": true,
		"0123456789abcdef0123456789abcdef01234567": true,
		"0123456789a": false,
		"0123456789abcdef0123456789abcdef012345678": false,
		"0123456789AB":             false,
		"0123456789ab-clean":       false,
		"0123456789ab-dirty-dirty": false,
		"1.0.0":                    false,
		"0.1.0-qa":                 false,
		"":                         false,
	} {
		if got := release.IsDevVersion(version); got != want {
			t.Errorf("IsDevVersion(%q) is %v, want %v", version, got, want)
		}
	}
}
