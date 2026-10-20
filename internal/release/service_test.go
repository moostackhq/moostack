package release_test

import (
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/moostackhq/moostack/internal/release"
)

// writeServiceRelease writes release 1.0.0 into a new release source,
// with an agent and the cluster services, each with its manifest entry
// changed by edit, and returns the source.
func writeServiceRelease(t *testing.T, edit func([]release.Service)) string {
	t.Helper()
	source := t.TempDir()
	dir := filepath.Join(source, "1.0.0")
	services := []release.Service{
		{Name: "registry", Version: "1.0.0", API: "moostack.registry.v1.Registry", Image: "registry.tar"},
		{Name: "dns", Version: "1.0.0", API: "moostack.dns.v1.DNS", Image: "dns.tar"},
	}
	for _, arch := range release.Architectures {
		writeFile(t, filepath.Join(dir, "linux-"+arch, "agent"), "agent for "+arch)
		for _, s := range services {
			writeFile(t, filepath.Join(dir, "linux-"+arch, s.Image), s.Name+" image for "+arch)
		}
	}
	m := release.Manifest{
		Version:    "1.0.0",
		Components: []release.Component{{Name: "agent", Version: "1.0.0", Files: []string{"agent"}}},
		Services:   services,
	}
	if err := release.Write(dir, m); err != nil {
		t.Fatal(err)
	}
	if edit != nil {
		// The checksums stay those Write wrote, as a forged manifest's
		// would.
		edit(m.Services)
		data, err := yaml.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		writeFile(t, filepath.Join(dir, "manifest.yml"), string(data))
	}
	return source
}

func TestOpenReadsTheServicesAndTheirImages(t *testing.T) {
	r, err := release.Open(writeServiceRelease(t, nil), "1.0.0")
	if err != nil {
		t.Fatal(err)
	}
	s, ok := r.Service("dns")
	if !ok || s.Version != "1.0.0" || s.API != "moostack.dns.v1.DNS" || s.Image != "dns.tar" {
		t.Errorf("dns is %+v, %v", s, ok)
	}
	data, err := r.ReadFile("arm64", "dns.tar")
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "dns image for arm64" {
		t.Errorf("ReadFile read %q", data)
	}
}

func TestReadFileRefusesATamperedImage(t *testing.T) {
	source := writeServiceRelease(t, nil)
	r, err := release.Open(source, "1.0.0")
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(source, "1.0.0", "linux-amd64", "registry.tar"), "something else")
	if _, err := r.ReadFile("amd64", "registry.tar"); err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Errorf("ReadFile of a tampered image: %v", err)
	}
}

func TestOpenRefusesAServiceImageWithoutAChecksum(t *testing.T) {
	source := writeServiceRelease(t, func(s []release.Service) {
		s[1].Image = "other.tar"
	})
	writeFile(t, filepath.Join(source, "1.0.0", "linux-amd64", "other.tar"), "other")
	if _, err := release.Open(source, "1.0.0"); err == nil || !strings.Contains(err.Error(), "linux-amd64/other.tar has no checksum") {
		t.Errorf("Open: %v", err)
	}
}

func TestOpenRefusesAServiceImageNameThatIsntPlain(t *testing.T) {
	for _, name := range []string{"../../etc/x", "images/dns.tar", "..", "."} {
		source := writeServiceRelease(t, func(s []release.Service) { s[1].Image = name })
		if _, err := release.Open(source, "1.0.0"); err == nil || !strings.Contains(err.Error(), "isn't a plain file name") {
			t.Errorf("Open of an image called %q: %v", name, err)
		}
	}
}

func TestOpenRefusesAnIncompleteService(t *testing.T) {
	for field, edit := range map[string]func([]release.Service){
		"name":    func(s []release.Service) { s[1].Name = "" },
		"version": func(s []release.Service) { s[1].Version = "" },
		"api":     func(s []release.Service) { s[1].API = "" },
		"image":   func(s []release.Service) { s[1].Image = "" },
	} {
		if _, err := release.Open(writeServiceRelease(t, edit), "1.0.0"); err == nil || !strings.Contains(err.Error(), "a service without") {
			t.Errorf("Open of a service without its %s: %v", field, err)
		}
	}
}

func TestOpenRefusesTwoServicesOfOneNameOrAPI(t *testing.T) {
	for what, edit := range map[string]func([]release.Service){
		"name": func(s []release.Service) { s[1].Name = "registry" },
		"API":  func(s []release.Service) { s[1].API = s[0].API },
	} {
		if _, err := release.Open(writeServiceRelease(t, edit), "1.0.0"); err == nil || !strings.Contains(err.Error(), "two services") {
			t.Errorf("Open of two services of one %s: %v", what, err)
		}
	}
}

func TestAServicesImageIsNamedForItsServiceAndVersion(t *testing.T) {
	if got := release.ImageRef("dns", "1.2.3"); got != "moostack.local/dns:1.2.3" {
		t.Errorf("ImageRef is %s", got)
	}
}

func TestCheckClusterServicesRefusesAReleaseWithoutOneOfTheFour(t *testing.T) {
	full := release.Manifest{Services: []release.Service{
		{Name: "registry", API: "moostack.registry.v1.Registry"},
		{Name: "dns", API: "moostack.dns.v1.DNS"},
		{Name: "proxy", API: "moostack.proxy.v1.Proxy"},
		{Name: "telemetry", API: "moostack.telemetry.v1.Telemetry"},
	}}
	if err := full.CheckClusterServices(); err != nil {
		t.Errorf("a release with the four cluster services: %v", err)
	}
	for i, s := range full.Services {
		without := release.Manifest{Services: slices.Delete(slices.Clone(full.Services), i, i+1)}
		if err := without.CheckClusterServices(); err == nil || !strings.Contains(err.Error(), s.Name) {
			t.Errorf("a release without %s: %v", s.Name, err)
		}
	}
	wrong := release.Manifest{Services: slices.Clone(full.Services)}
	wrong.Services[1].API = "moostack.dns.v2.DNS"
	if err := wrong.CheckClusterServices(); err == nil || !strings.Contains(err.Error(), "moostack.dns.v1.DNS") {
		t.Errorf("a release whose dns serves another API: %v", err)
	}
}
