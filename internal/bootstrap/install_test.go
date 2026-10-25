package bootstrap_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/moostackhq/moostack/internal/bootstrap"
	"github.com/moostackhq/moostack/internal/hostlayout"
	"github.com/moostackhq/moostack/internal/release"
)

func TestInstallRefusesAReleaseWithoutTheClusterServicesBeforeReachingTheHost(t *testing.T) {
	source := t.TempDir()
	dir := filepath.Join(source, "1.0.0")
	var services []release.Service
	for _, s := range release.ClusterServices {
		if s.Name != "telemetry" {
			services = append(services, release.Service{Name: s.Name, Version: "1.0.0", API: s.API, Image: s.Name + ".tar"})
		}
	}
	for _, arch := range release.Architectures {
		if err := os.MkdirAll(filepath.Join(dir, "linux-"+arch), 0o755); err != nil {
			t.Fatal(err)
		}
		for _, s := range services {
			if err := os.WriteFile(filepath.Join(dir, "linux-"+arch, s.Image), []byte(s.Name), 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := release.Write(dir, release.Manifest{Version: "1.0.0", Services: services}); err != nil {
		t.Fatal(err)
	}
	rel, err := release.Open(source, "1.0.0")
	if err != nil {
		t.Fatal(err)
	}
	// Without a connection to a host, Install fails on whatever it does
	// to the host first.
	_, err = bootstrap.Install(t.Context(), nil, bootstrap.Host{Architecture: "amd64"}, rel, hostlayout.NodeConfig{}, nil, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "has no telemetry") {
		t.Errorf("installing a release without telemetry: %v", err)
	}
}
