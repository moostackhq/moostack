package release

import (
	"context"
	"fmt"
	"os"
	"os/exec"
)

// ldflags stamps a program with the release at version, and previous.
func ldflags(version, previous string) string {
	return "-X github.com/moostackhq/moostack/internal/buildinfo.Version=" + version +
		" -X github.com/moostackhq/moostack/internal/buildinfo.Previous=" + previous
}

// GoBuild builds the package pkg of the Moostack checkout at checkout
// for goos and goarch, static, into the file out, stamped with version
// and the previous release's, and with tags.
func GoBuild(ctx context.Context, checkout, pkg, goos, goarch, out, version, previous string, tags ...string) error {
	args := []string{"build", "-trimpath",
		"-ldflags", ldflags(version, previous),
		"-o", out}
	for _, tag := range tags {
		args = append(args, "-tags", tag)
	}
	cmd := exec.CommandContext(ctx, "go", append(args, pkg)...)
	cmd.Dir = checkout
	cmd.Env = append(os.Environ(), "GOOS="+goos, "GOARCH="+goarch, "CGO_ENABLED=0")
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("building %s for %s/%s: %w", pkg, goos, goarch, err)
	}
	return nil
}
