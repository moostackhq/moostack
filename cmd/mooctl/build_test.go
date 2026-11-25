package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/moostackhq/moostack/internal/release"
)

func TestMooctlBuildsForEveryPlatform(t *testing.T) {
	if testing.Short() {
		t.Skip("builds mooctl four times")
	}
	for _, platform := range [][2]string{{"darwin", "arm64"}, {"darwin", "amd64"}, {"linux", "arm64"}, {"linux", "amd64"}} {
		t.Run(platform[0]+"/"+platform[1], func(t *testing.T) {
			cmd := exec.Command("go", "build", "-o", filepath.Join(t.TempDir(), "mooctl"), ".")
			cmd.Env = append(os.Environ(), "GOOS="+platform[0], "GOARCH="+platform[1], "CGO_ENABLED=0")
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Errorf("%v\n%s", err, out)
			}
		})
	}
}

func TestAReleaseBuildHasNoDevCommands(t *testing.T) {
	if testing.Short() {
		t.Skip("builds mooctl twice")
	}
	for tags, has := range map[string]bool{"": true, "release": false} {
		mooctl := filepath.Join(t.TempDir(), "mooctl")
		if out, err := exec.Command("go", "build", "-tags", tags, "-o", mooctl, ".").CombinedOutput(); err != nil {
			t.Fatalf("%v\n%s", err, out)
		}
		out, err := exec.Command(mooctl, "dev", "console", "deploy", "--help").CombinedOutput()
		if got := err == nil && strings.Contains(string(out), "mooctl dev console deploy"); got != has {
			t.Errorf("built with the tags %q, mooctl dev console deploy --help: %v\n%s", tags, err, out)
		}
	}
}

func TestTheReleaseBuildSetsTheCommit(t *testing.T) {
	if testing.Short() {
		t.Skip("builds mooctl")
	}
	root := filepath.Join("..", "..")
	workflow, err := os.ReadFile(filepath.Join(root, ".github", "workflows", "release.yml"))
	if err != nil {
		t.Fatal(err)
	}
	build := regexp.MustCompile(`(?m)^.*go build.*\./cmd/mooctl$`).Find(workflow)
	if build == nil {
		t.Fatal("the release workflow has no line that builds ./cmd/mooctl")
	}
	if set := "-X github.com/moostackhq/moostack/internal/cli.commit=$GITHUB_SHA"; !strings.Contains(string(build), set) {
		t.Fatalf("the release workflow builds mooctl without %s:\n%s", set, build)
	}
	const commit = "a-commit-only-this-test-builds-with"
	dir := t.TempDir()
	cmd := exec.Command("bash", "-euc", string(build))
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "GITHUB_SHA="+commit, "os=linux", "arch=amd64", "dir="+dir, "CGO_ENABLED=0")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	built, err := os.ReadFile(filepath.Join(dir, "mooctl"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(built, []byte(commit)) {
		t.Errorf("mooctl built by the release workflow's line doesn't hold the commit:\n%s", build)
	}
}

func TestTheReleaseWorkflowPublishesTheReleaseMooctlDownloadsFrom(t *testing.T) {
	workflow, err := os.ReadFile(filepath.Join("..", "..", ".github", "workflows", "release.yml"))
	if err != nil {
		t.Fatal(err)
	}
	const commit = "861df463416c35c9042a9df97e5ba73b942469bb"
	publish := strings.ReplaceAll(string(workflow), "$GITHUB_SHA", commit)
	asked := release.URL(commit)
	of := `gh release create "` + asked[strings.LastIndex(asked, "/")+1:] + `" dist/* `
	created := strings.Index(publish, of)
	if created < 0 {
		t.Fatalf("the release workflow doesn't run %s, the release mooctl built from %s downloads from", of, commit)
	}
	for _, edge := range []string{"gh release upload edge ", "gh release create edge "} {
		if moved := strings.Index(publish, edge); moved < created {
			t.Errorf("the release workflow runs %q before it creates the commit's release, or not at all", edge)
		}
	}
}
