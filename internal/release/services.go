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

// BuildServices builds ClusterServices for arch with cargo, in the
// Moostack checkout at checkout, and writes each one's image of version
// into dir, as <name>.tar. It returns them as a manifest lists them.
func BuildServices(ctx context.Context, checkout, arch, version, dir string) ([]Service, error) {
	target, ok := rustTargets[arch]
	if !ok {
		return nil, fmt.Errorf("no Rust target for %s", arch)
	}
	// The target directory is given, so that neither CARGO_TARGET_DIR nor
	// cargo's config builds the binaries elsewhere than they are read.
	targetDir, err := filepath.Abs(filepath.Join(checkout, "target"))
	if err != nil {
		return nil, err
	}
	args := []string{"build", "--release", "--locked", "--target", target, "--target-dir", targetDir}
	for _, s := range ClusterServices {
		args = append(args, "-p", "moostack-"+s.Name)
	}
	cmd := exec.CommandContext(ctx, "cargo", args...)
	cmd.Dir = checkout
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("building the cluster services for %s: %w", arch, err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	var services []Service
	for _, s := range ClusterServices {
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
		services = append(services, Service{Name: s.Name, Version: version, API: s.API, Image: file})
	}
	return services, nil
}
