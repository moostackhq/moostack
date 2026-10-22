package release

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/moostackhq/moostack/internal/ociimage"
)

// ClusterService is a cluster service every release has: its name,
// which is that of its binary in the checkout's cargo workspace, and the
// gRPC service name of its API.
type ClusterService struct{ Name, API string }

// ClusterServices are the cluster services every release has.
var ClusterServices = []ClusterService{
	{"registry", "moostack.registry.v1.Registry"},
	{"dns", "moostack.dns.v1.DNS"},
	{"proxy", "moostack.proxy.v1.Proxy"},
	{"telemetry", "moostack.telemetry.v1.Telemetry"},
}

// CheckClusterServices fails unless m has each of ClusterServices,
// serving its API.
func (m Manifest) CheckClusterServices() error {
	var problems []string
	for _, want := range ClusterServices {
		s, ok := m.Service(want.Name)
		switch {
		case !ok:
			problems = append(problems, "it has no "+want.Name)
		case s.API != want.API:
			problems = append(problems, fmt.Sprintf("its %s serves %s, not %s", want.Name, s.API, want.API))
		}
	}
	if len(problems) > 0 {
		return fmt.Errorf("release %s doesn't have the cluster services: %s", m.Version, strings.Join(problems, "; "))
	}
	return nil
}

// rustTargets are the Rust targets the cluster services are built for,
// by architecture: static, with musl.
var rustTargets = map[string]string{
	"amd64": "x86_64-unknown-linux-musl",
	"arm64": "aarch64-unknown-linux-musl",
}

// remapRustc is a rustc that runs the rustc in MOOSTACK_RUSTC with the
// checkout's path and cargo's home, where the dependencies' sources are,
// mapped to fixed names, so that no binary holds a path of the machine
// that built it. The paths are added to each call of rustc, rather than
// to cargo's rustflags, which would take the place of those cargo is
// configured with.
const remapRustc = `#!/bin/sh
exec "$MOOSTACK_RUSTC" "$@" "--remap-path-prefix=$MOOSTACK_CARGO_HOME=/cargo" "--remap-path-prefix=$MOOSTACK_CHECKOUT=/moostack"
`

// remapPaths is the environment a build of the services in the checkout
// at checkout runs with, so that cargo runs remapRustc, which it writes
// into dir, in place of the rustc it would have run, and runs it through
// no wrapper: a caching one, such as sccache, doesn't see the remapping,
// and would serve what it compiled without it.
func remapPaths(ctx context.Context, checkout, dir string) ([]string, error) {
	abs, err := filepath.Abs(checkout)
	if err != nil {
		return nil, err
	}
	cargoHome := os.Getenv("CARGO_HOME")
	if cargoHome == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, err
		}
		cargoHome = filepath.Join(home, ".cargo")
	} else if !filepath.IsAbs(cargoHome) {
		// As cargo, which runs in the checkout, finds it.
		cargoHome = filepath.Join(abs, cargoHome)
	}
	// The rustc cargo would run: RUSTC's, found as cargo finds it, or the
	// checkout's toolchain's, which rustup's rustc runs only when run in
	// the checkout.
	rustc := os.Getenv("RUSTC")
	if rustc != "" && strings.ContainsRune(rustc, filepath.Separator) && !filepath.IsAbs(rustc) {
		rustc = filepath.Join(abs, rustc)
	}
	if rustc == "" {
		cmd := exec.CommandContext(ctx, "rustc", "--print", "sysroot")
		cmd.Dir = abs
		// What rustup says, as when it installs the checkout's toolchain or
		// can't, goes to the builder, as cargo's does.
		cmd.Stderr = os.Stderr
		sysroot, err := cmd.Output()
		if err != nil {
			return nil, fmt.Errorf("finding the checkout's rustc: %w", err)
		}
		rustc = filepath.Join(strings.TrimSpace(string(sysroot)), "bin", "rustc")
	}
	// It is written beside the build, where cargo runs build scripts, so
	// that a temporary directory mounted noexec doesn't stop it, and put in
	// place whole, so that a build that runs it meanwhile runs it whole.
	// It is written anew for each build: cargo asks a rustc again what it
	// is only when its file changes, so that a toolchain bumped since the
	// last build is noticed.
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	script := filepath.Join(dir, "rustc-remap")
	f, err := os.CreateTemp(dir, ".rustc-remap-")
	if err != nil {
		return nil, err
	}
	defer os.Remove(f.Name())
	if _, err := f.WriteString(remapRustc); err != nil {
		f.Close()
		return nil, err
	}
	if err := f.Close(); err != nil {
		return nil, err
	}
	if err := os.Chmod(f.Name(), 0o755); err != nil {
		return nil, err
	}
	if err := os.Rename(f.Name(), script); err != nil {
		return nil, err
	}
	// An empty wrapper is none, whatever cargo's config says.
	return []string{"RUSTC=" + script, "MOOSTACK_RUSTC=" + rustc, "MOOSTACK_CARGO_HOME=" + cargoHome, "MOOSTACK_CHECKOUT=" + abs,
		"RUSTC_WRAPPER=", "RUSTC_WORKSPACE_WRAPPER="}, nil
}

// BuildServices builds services, of ClusterServices, for arch with
// cargo, in the Moostack checkout at checkout, and writes each one's
// image of version into dir, as <name>.tar. It returns them as a
// manifest lists them.
func BuildServices(ctx context.Context, checkout, arch, version, dir string, services []ClusterService) ([]Service, error) {
	target, ok := rustTargets[arch]
	if !ok {
		return nil, fmt.Errorf("no Rust target for %s", arch)
	}
	// The target directory is given, so that neither CARGO_TARGET_DIR nor
	// cargo's config builds the binaries elsewhere than they are read. It
	// is the services' own, since cargo doesn't build again what it built
	// without the rustc that remaps the builder's paths.
	targetDir, err := filepath.Abs(filepath.Join(checkout, "target", "cluster-services"))
	if err != nil {
		return nil, err
	}
	args := []string{"build", "--release", "--locked", "--target", target, "--target-dir", targetDir}
	for _, s := range services {
		args = append(args, "-p", "moostack-"+s.Name)
	}
	remap, err := remapPaths(ctx, checkout, targetDir)
	if err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, "cargo", args...)
	cmd.Dir = checkout
	cmd.Env = append(os.Environ(), remap...)
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("building the cluster services for %s: %w", arch, err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	var built []Service
	for _, s := range services {
		binary, err := os.ReadFile(filepath.Join(targetDir, target, "release", s.Name))
		if err != nil {
			return nil, err
		}
		file := s.Name + ".tar"
		err = ociimage.WriteFile(filepath.Join(dir, file), ociimage.Image{
			Name:         ImageRef(s.Name, version),
			Architecture: arch,
			Entrypoint:   []string{"/" + s.Name},
			Files:        []ociimage.File{{Name: s.Name, Mode: 0o755, Data: binary}},
		})
		if err != nil {
			return nil, err
		}
		built = append(built, Service{Name: s.Name, Version: version, API: s.API, Image: file})
	}
	return built, nil
}
