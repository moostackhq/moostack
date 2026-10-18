// Command release builds a Moostack release from this checkout, and the
// mooctl that belongs to it. Run it from the checkout's root:
//
//	go run ./cmd/release -version 0.1.0 [-out dist]
//
// It writes mooctl for this machine to <out>/mooctl, and the release to
// <out>/releases/<version>, so that <out>/releases is a release source.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"

	"github.com/moostackhq/moostack/internal/release"
)

func main() {
	version := flag.String("version", "", "the release's version")
	out := flag.String("out", "dist", "the directory to write to")
	flag.Parse()
	if err := build(context.Background(), *version, *out); err != nil {
		fmt.Fprintf(os.Stderr, "release: %v\n", err)
		os.Exit(1)
	}
}

func build(ctx context.Context, version, out string) error {
	if version == "" || flag.NArg() != 0 {
		return errors.New("usage: go run ./cmd/release -version VERSION [-out DIR]")
	}
	if err := release.CheckVersion(version); err != nil {
		return err
	}
	cache, err := os.UserCacheDir()
	if err != nil {
		return err
	}
	cache = filepath.Join(cache, "moostack-release")
	dir := filepath.Join(out, "releases", version)
	if err := os.RemoveAll(dir); err != nil {
		return err
	}
	for _, arch := range release.Architectures {
		archDir := filepath.Join(dir, "linux-"+arch)
		if err := goBuild(version, "linux", arch, filepath.Join(archDir, "agent"), "./cmd/agent"); err != nil {
			return err
		}
		if err := release.FetchContainerd(ctx, arch, cache, archDir); err != nil {
			return err
		}
	}
	err = release.Write(dir, release.Manifest{
		Version: version,
		Components: []release.Component{
			{Name: "agent", Version: version, Files: []string{"agent"}},
			{Name: "containerd", Version: release.ContainerdVersion, Files: release.ContainerdFiles},
		},
	})
	if err != nil {
		return err
	}
	mooctl := filepath.Join(out, "mooctl")
	if err := goBuild(version, runtime.GOOS, runtime.GOARCH, mooctl, "./cmd/mooctl"); err != nil {
		return err
	}
	fmt.Printf("Built release %s in %s and %s\n", version, dir, mooctl)
	return nil
}

// goBuild builds pkg for goos and goarch into the file out, stamped with
// version.
func goBuild(version, goos, goarch, out, pkg string) error {
	cmd := exec.Command("go", "build", "-trimpath",
		"-ldflags", "-X github.com/moostackhq/moostack/internal/buildinfo.Version="+version,
		"-o", out, pkg)
	cmd.Env = append(os.Environ(), "GOOS="+goos, "GOARCH="+goarch, "CGO_ENABLED=0")
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("building %s for %s/%s: %w", pkg, goos, goarch, err)
	}
	return nil
}
