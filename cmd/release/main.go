// Command release builds a Moostack release from this checkout, and the
// mooctl that belongs to it. Run it from the checkout's root:
//
//	go run ./cmd/release -version 0.1.0 [-out dist] [-dev]
//
// It writes mooctl for this machine to <out>/mooctl, and the release to
// <out>/releases/<version>, so that <out>/releases is a release source.
// With -dev, mooctl is a dev build, built with the dev tag, which reads
// settings for tests from MOOSTACK_DEV_* environment variables.
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
	dev := flag.Bool("dev", false, "make mooctl a dev build, which reads MOOSTACK_DEV_* settings")
	flag.Parse()
	if err := build(context.Background(), *version, *out, *dev); err != nil {
		fmt.Fprintf(os.Stderr, "release: %v\n", err)
		os.Exit(1)
	}
}

func build(ctx context.Context, version, out string, dev bool) error {
	if version == "" || flag.NArg() != 0 {
		return errors.New("usage: go run ./cmd/release -version VERSION [-out DIR] [-dev]")
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
		for _, program := range []string{"agent", "updater"} {
			if err := goBuild(version, "linux", arch, filepath.Join(archDir, program), "./cmd/"+program, false); err != nil {
				return err
			}
		}
		if err := release.FetchContainerd(ctx, arch, cache, archDir); err != nil {
			return err
		}
	}
	err = release.Write(dir, release.Manifest{
		Version: version,
		Components: []release.Component{
			{Name: "agent", Version: version, Files: []string{"agent"}},
			{Name: "updater", Version: version, Files: []string{"updater"}},
			{Name: "containerd", Version: release.ContainerdVersion, Files: release.ContainerdFiles},
		},
	})
	if err != nil {
		return err
	}
	mooctl := filepath.Join(out, "mooctl")
	if err := goBuild(version, runtime.GOOS, runtime.GOARCH, mooctl, "./cmd/mooctl", dev); err != nil {
		return err
	}
	fmt.Printf("Built release %s in %s and %s\n", version, dir, mooctl)
	if dev {
		fmt.Println("mooctl is a dev build, which reads MOOSTACK_DEV_* settings")
	}
	return nil
}

// goBuild builds pkg for goos and goarch into the file out, stamped with
// version, and with the dev tag if dev.
func goBuild(version, goos, goarch, out, pkg string, dev bool) error {
	args := []string{"build", "-trimpath",
		"-ldflags", "-X github.com/moostackhq/moostack/internal/buildinfo.Version=" + version,
		"-o", out}
	if dev {
		args = append(args, "-tags", "dev")
	}
	cmd := exec.Command("go", append(args, pkg)...)
	cmd.Env = append(os.Environ(), "GOOS="+goos, "GOARCH="+goarch, "CGO_ENABLED=0")
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("building %s for %s/%s: %w", pkg, goos, goarch, err)
	}
	return nil
}
