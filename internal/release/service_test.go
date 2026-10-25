package release_test

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
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
		{Name: "registry", Version: "1.0.0", API: "moostack.registry.v1.Registry", Image: "registry.tar", Properties: release.Properties{User: "65534:65534"}},
		{Name: "dns", Version: "1.0.0", API: "moostack.dns.v1.DNS", Image: "dns.tar", Properties: release.Properties{User: "65534:65534"}},
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

// The release states how each service runs, and Open reads it back as
// written: its user, its root read-only or not, and its capabilities,
// none as none.
func TestOpenReadsTheServicesProperties(t *testing.T) {
	source := writeServiceRelease(t, func(s []release.Service) {
		s[0].Properties = release.Properties{User: "65534:65534", ReadOnlyRoot: true, Capabilities: []string{}}
		s[1].Properties = release.Properties{User: "1000:1000", Capabilities: []string{"CAP_NET_BIND_SERVICE", "CAP_KILL"}}
	})
	r, err := release.Open(source, "1.0.0")
	if err != nil {
		t.Fatal(err)
	}
	registry, _ := r.Service("registry")
	if p := registry.Properties; p.User != "65534:65534" || !p.ReadOnlyRoot || len(p.Capabilities) != 0 {
		t.Errorf("registry runs %+v, want as nobody, read-only, with no capabilities", p)
	}
	dns, _ := r.Service("dns")
	if p := dns.Properties; p.User != "1000:1000" || p.ReadOnlyRoot || !slices.Equal(p.Capabilities, []string{"CAP_NET_BIND_SERVICE", "CAP_KILL"}) {
		t.Errorf("dns runs %+v, want as 1000:1000, writable, with CAP_NET_BIND_SERVICE and CAP_KILL", p)
	}
}

// Every release the release build makes states each cluster service run
// as nobody, with a read-only root and no capabilities.
func TestEveryClusterServiceRunsLockedDown(t *testing.T) {
	for _, s := range release.ClusterServices {
		if p := s.Properties; p.User != "65534:65534" || !p.ReadOnlyRoot || p.Capabilities == nil || len(p.Capabilities) != 0 {
			t.Errorf("%s runs %+v, want as nobody, read-only, with no capabilities", s.Name, p)
		}
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
		"user":    func(s []release.Service) { s[1].User = "" },
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

// ServiceVersion reads back the version of each image ImageRef names for
// the service, run by the container ContainerName names for it, and no
// version of another image: another service's, one without a version, or
// one of another repository, its name the version it would have; nor of
// the service's image run under another name: the service's name alone,
// as releases before the service- names ran it, or an app's.
func TestServiceVersionReadsTheVersionOfTheServicesImageAlone(t *testing.T) {
	for _, version := range []string{"1.0.0", "20261024T153000Z", "0123456789ab-dirty"} {
		if got, ok := release.ServiceVersion("service-dns", release.ImageRef("dns", version)); !ok || got != version {
			t.Errorf("ServiceVersion of dns's image at %s is %q, %v", version, got, ok)
		}
	}
	for _, image := range []string{release.ImageRef("proxy", "1.0.0"), release.ImageRef("dns", ""), "1.0.0", "docker.io/library/dns:1.0.0", release.ImageRef("dnsx", "1.0.0"), ""} {
		if got, ok := release.ServiceVersion("service-dns", image); ok || got != "" {
			t.Errorf("ServiceVersion of service-dns from %q is %q, %v, want none", image, got, ok)
		}
	}
	for _, name := range []string{"dns", "app-dns", "service-", "service-proxy", "xservice-dns"} {
		if got, ok := release.ServiceVersion(name, release.ImageRef("dns", "1.0.0")); ok || got != "" {
			t.Errorf("ServiceVersion of %s from dns's image is %q, %v, want none", name, got, ok)
		}
	}
}

// Whatever the service's name and version, ServiceVersion reads back the
// version of the image ImageRef names, run by the container ContainerName
// names, and nothing of another service's, nor of a container named
// otherwise.
func FuzzServiceVersionReadsBackImageRef(f *testing.F) {
	f.Add("dns", "1.0.0", "proxy")
	f.Add("dns", "", "dns")
	f.Fuzz(func(t *testing.T, name, version, other string) {
		got, ok := release.ServiceVersion(release.ContainerName(name), release.ImageRef(name, version))
		if ok != (name != "" && version != "") || (ok && got != version) {
			t.Errorf("ServiceVersion(ContainerName(%q), ImageRef(%q, %q)) = %q, %v", name, name, version, got, ok)
		}
		if other != name && !strings.HasPrefix(release.ImageRef(other, version), release.ImageRef(name, "")) {
			if got, ok := release.ServiceVersion(release.ContainerName(name), release.ImageRef(other, version)); ok || got != "" {
				t.Errorf("ServiceVersion(ContainerName(%q), ImageRef(%q, %q)) = %q, %v, want none", name, other, version, got, ok)
			}
		}
		if !strings.HasPrefix(name, "service-") {
			if got, ok := release.ServiceVersion(name, release.ImageRef(name, version)); ok || got != "" {
				t.Errorf("ServiceVersion(%q, ImageRef(%q, %q)) = %q, %v, want none", name, name, version, got, ok)
			}
		}
	})
}

// ServiceName reads back the service ContainerName names a container
// for, and none from a name without the prefix or with nothing after it.
func TestServiceNameReadsBackContainerName(t *testing.T) {
	for _, s := range release.ClusterServices {
		if got, ok := release.ServiceName(release.ContainerName(s.Name)); !ok || got != s.Name {
			t.Errorf("ServiceName(ContainerName(%s)) = %q, %v", s.Name, got, ok)
		}
	}
	for _, name := range []string{"dns", "app-web", "service-", "", "xservice-dns"} {
		if got, ok := release.ServiceName(name); ok {
			t.Errorf("ServiceName(%q) = %q, want none", name, got)
		}
	}
}

// A container may be a cluster service if it is named for one and runs
// its service's image, or if the node can't tell its image; never with
// another image, nor under another name.
func TestMayBeServiceTakesTheServicesImageOrNoneKnown(t *testing.T) {
	for _, c := range []struct {
		name, image string
		want        bool
	}{
		{"service-dns", release.ImageRef("dns", "1.0.0"), true},
		{"service-dns", "", true},
		{"service-dns", release.ImageRef("proxy", "1.0.0"), false},
		{"service-dns", release.ImageRef("dns", ""), false},
		{"service-dns", "docker.io/library/nginx:1.27", false},
		{"dns", release.ImageRef("dns", "1.0.0"), false},
		{"dns", "", false},
		{"app-web", "", false},
		{"service-", "", false},
	} {
		if got := release.MayBeService(c.name, c.image); got != c.want {
			t.Errorf("MayBeService(%s, %q) = %v, want %v", c.name, c.image, got, c.want)
		}
	}
}

// A cluster service runs as the container that its release states: named
// for the service, from its image at its version, serving its API, with
// the release's properties, whatever they are; no capabilities as none,
// not containerd's default set.
func TestAServicesContainerIsTheOneItsReleaseStates(t *testing.T) {
	for _, c := range []struct {
		props release.Properties
		want  *nodev1.Container
	}{
		{release.Properties{User: "65534:65534", ReadOnlyRoot: true, Capabilities: []string{}},
			&nodev1.Container{Name: "service-dns", Image: "moostack.local/dns:1.2.0", User: proto.String("65534:65534"), ReadOnlyRoot: true, Capabilities: &nodev1.Capabilities{}, Api: "moostack.dns.v1.DNS"}},
		{release.Properties{User: "1000:1000"},
			&nodev1.Container{Name: "service-dns", Image: "moostack.local/dns:1.2.0", User: proto.String("1000:1000"), Capabilities: &nodev1.Capabilities{}, Api: "moostack.dns.v1.DNS"}},
		{release.Properties{User: "0", Capabilities: []string{"CAP_NET_BIND_SERVICE"}},
			&nodev1.Container{Name: "service-dns", Image: "moostack.local/dns:1.2.0", User: proto.String("0"), Capabilities: &nodev1.Capabilities{Names: []string{"CAP_NET_BIND_SERVICE"}}, Api: "moostack.dns.v1.DNS"}},
	} {
		got := release.Service{Name: "dns", Version: "1.2.0", API: "moostack.dns.v1.DNS", Image: "dns.tar", Properties: c.props}.Container()
		if !proto.Equal(got, c.want) {
			t.Errorf("Container with %+v is %v, want %v", c.props, got, c.want)
		}
	}
}

// The container is the service's own: changing what it was made of
// changes nothing of it.
func TestAServicesContainerKeepsItsOwnCapabilities(t *testing.T) {
	s := release.Service{Name: "dns", Version: "1.2.0", API: "moostack.dns.v1.DNS", Properties: release.Properties{User: "0", Capabilities: []string{"CAP_KILL"}}}
	c := s.Container()
	s.Capabilities[0] = "CAP_SYS_ADMIN"
	if got := c.Capabilities.Names; len(got) != 1 || got[0] != "CAP_KILL" {
		t.Errorf("the container's capabilities became %q with the release's changed", got)
	}
}

// A release stating a service the node API wouldn't run is refused when
// it opens, naming the service and why, before any node is reached: a
// user that isn't a uid, capabilities Linux lacks or listed twice.
func TestOpenRefusesAServiceTheNodeAPIWouldntRun(t *testing.T) {
	for what, c := range map[string]struct {
		props release.Properties
		want  string
	}{
		"a user by name":        {release.Properties{User: "nobody"}, "user is a uid"},
		"a user of three parts": {release.Properties{User: "1:2:3"}, "user is a uid"},
		"a gid left empty":      {release.Properties{User: "65534:"}, "user is a uid"},
		"an unknown capability": {release.Properties{User: "0", Capabilities: []string{"CAP_FLY"}}, "capabilities are Linux's"},
		"a capability twice":    {release.Properties{User: "0", Capabilities: []string{"CAP_KILL", "CAP_KILL"}}, "capabilities are Linux's"},
	} {
		source := writeServiceRelease(t, func(s []release.Service) { s[1].Properties = c.props })
		_, err := release.Open(source, "1.0.0")
		if err == nil || !strings.Contains(err.Error(), "its dns service can't run") || !strings.Contains(err.Error(), c.want) {
			t.Errorf("Open of a service with %s: %v, want it refused saying %q", what, err, c.want)
		}
	}
	for what, props := range map[string]release.Properties{
		"nobody, locked down": {User: "65534:65534", ReadOnlyRoot: true, Capabilities: []string{}},
		"a uid alone":         {User: "1000"},
		"every capability":    {User: "0", Capabilities: nodev1.LinuxCapabilities},
	} {
		source := writeServiceRelease(t, func(s []release.Service) { s[1].Properties = props })
		if _, err := release.Open(source, "1.0.0"); err != nil {
			t.Errorf("Open of a service with %s: %v", what, err)
		}
	}
}

// The release states each service's properties in its manifest under
// the keys user, read_only_root and capabilities, beside its other
// fields, as every release of this one reads them.
func TestAManifestStatesTheServicesPropertiesUnderTheirKeys(t *testing.T) {
	dir := t.TempDir()
	m := release.Manifest{Version: "1.0.0", Services: []release.Service{
		{Name: "dns", Version: "1.0.0", API: "moostack.dns.v1.DNS", Image: "dns.tar", Properties: release.Properties{User: "65534:65534", ReadOnlyRoot: true, Capabilities: []string{}}},
		{Name: "proxy", Version: "1.0.0", API: "moostack.proxy.v1.Proxy", Image: "proxy.tar", Properties: release.Properties{User: "0", Capabilities: []string{"CAP_NET_BIND_SERVICE"}}},
	}}
	for _, arch := range release.Architectures {
		for _, f := range []string{"dns.tar", "proxy.tar"} {
			writeFile(t, filepath.Join(dir, "linux-"+arch, f), f)
		}
	}
	if err := release.Write(dir, m); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "manifest.yml"))
	if err != nil {
		t.Fatal(err)
	}
	want := `services:
    - name: dns
      version: 1.0.0
      api: moostack.dns.v1.DNS
      image: dns.tar
      user: 65534:65534
      read_only_root: true
      capabilities: []
    - name: proxy
      version: 1.0.0
      api: moostack.proxy.v1.Proxy
      image: proxy.tar
      user: "0"
      read_only_root: false
      capabilities:
        - CAP_NET_BIND_SERVICE
`
	if _, services, _ := strings.Cut(string(data), "\nservices:\n"); "services:\n"+services != want {
		t.Errorf("the manifest is\n%s\nwant its services\n%s", data, want)
	}
}
