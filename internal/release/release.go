// Package release reads and writes Moostack releases: a manifest that
// pins the version of each node component and cluster service, their
// files for each architecture, and the files' checksums.
//
// A release source is a directory holding one directory per release,
// named for its version:
//
//	<version>/manifest.yml
//	<version>/SHA256SUMS
//	<version>/linux-<arch>/<file>
package release

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"google.golang.org/grpc/status"
	"gopkg.in/yaml.v3"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"

	"github.com/moostackhq/moostack/internal/buildid"
	"github.com/moostackhq/moostack/internal/ociimage"
	"github.com/moostackhq/moostack/internal/strictyaml"
)

const (
	manifestName = "manifest.yml"
	sumsName     = "SHA256SUMS"
)

// Architectures are the architectures every release has files for.
var Architectures = []string{"amd64", "arm64"}

// Manifest pins the version of each node component and cluster
// service.
type Manifest struct {
	Version string `yaml:"version"`
	// Previous is the version of the release before this one, which
	// updates to it, empty for the first.
	Previous   string      `yaml:"previous,omitempty"`
	Components []Component `yaml:"components"`
	Services   []Service   `yaml:"services,omitempty"`
}

// Component is a node component in a release.
type Component struct {
	Name    string `yaml:"name"`
	Version string `yaml:"version"`
	// Files are the component's files, the same for every architecture.
	Files []string `yaml:"files"`
}

// Service is a cluster service in a release.
type Service struct {
	Name    string `yaml:"name"`
	Version string `yaml:"version"`
	// API is the gRPC service name of the service's API, which the agent
	// routes calls to the service by.
	API string `yaml:"api"`
	// Image is the file of the service's image, an OCI image layout
	// archive, for every architecture. It names the image ImageRef.
	Image      string `yaml:"image"`
	Properties `yaml:",inline"`
}

// Properties are how a cluster service's container runs, as its release
// states them.
type Properties struct {
	// User is the uid the service runs as, with a gid after a colon.
	User         string `yaml:"user"`
	ReadOnlyRoot bool   `yaml:"read_only_root"`
	// Capabilities are the Linux capabilities the service runs with,
	// none if empty.
	Capabilities []string `yaml:"capabilities"`
}

// Container is the container that the cluster service s runs as, named
// as ContainerName names it, from its image at its version, serving its
// API, with the properties its release states.
func (s Service) Container() *nodev1.Container {
	user := s.User
	return &nodev1.Container{
		Name:         ContainerName(s.Name),
		Image:        ImageRef(s.Name, s.Version),
		User:         &user,
		ReadOnlyRoot: s.ReadOnlyRoot,
		Capabilities: &nodev1.Capabilities{Names: slices.Clone(s.Capabilities)},
		Api:          s.API,
	}
}

// ImageRef is the name of the image of version of the service called
// name, which containerd knows it by once imported.
func ImageRef(name, version string) string {
	return "moostack.local/" + name + ":" + version
}

// servicePrefix starts the name of every cluster service's container.
//
// Temporary: the prefix is what keeps an app, whose container mooctl
// names app-<name>, from replacing a cluster service by its name, until
// a container's name gets an owner on the node.
const servicePrefix = "service-"

// ContainerName is the name of the container that the cluster service
// called service runs as.
func ContainerName(service string) string { return servicePrefix + service }

// MayBeService reports whether the container called name, running
// image, may be one of the cluster services: one named for a service
// that runs the release's image of that service, or one so named whose
// image the node can't tell, as one it can't read.
func MayBeService(name, image string) bool {
	_, named := ServiceName(name)
	_, ok := ServiceVersion(name, image)
	return ok || (named && image == "")
}

// ServiceVersion is the version of the cluster service whose container
// is called name, as ContainerName names it, and whose image is image,
// as ImageRef names it, and whether they are one's: if they aren't, the
// version is empty.
func ServiceVersion(name, image string) (string, bool) {
	service, ok := ServiceName(name)
	if !ok {
		return "", false
	}
	version, ok := strings.CutPrefix(image, ImageRef(service, ""))
	if !ok || version == "" {
		return "", false
	}
	return version, true
}

// ServiceName is the name of the cluster service whose container is
// called name, as ContainerName names it, and whether name is one's.
func ServiceName(name string) (string, bool) {
	service, ok := strings.CutPrefix(name, servicePrefix)
	return service, ok && service != ""
}

// Service returns the service called name.
func (m Manifest) Service(name string) (Service, bool) {
	i := slices.IndexFunc(m.Services, func(s Service) bool { return s.Name == name })
	if i < 0 {
		return Service{}, false
	}
	return m.Services[i], true
}

// UpdatedFrom are the releases that m updates nodes on: m itself and the
// release before it, if there is one.
func (m Manifest) UpdatedFrom() []string {
	if m.Previous == "" {
		return []string{m.Version}
	}
	return []string{m.Version, m.Previous}
}

// UpdatesFrom reports whether m updates a node on the release running:
// m itself or the release before it, so that no update skips a release
// or goes back.
func (m Manifest) UpdatesFrom(running string) bool {
	return running != "" && (running == m.Version || running == m.Previous)
}

// Pin is the version that a release pins a component or a cluster
// service at, by its name.
type Pin struct{ Name, Version string }

// PinnedComponents are the versions m pins its components at.
func (m Manifest) PinnedComponents() []Pin {
	var pins []Pin
	for _, c := range m.Components {
		pins = append(pins, Pin{c.Name, c.Version})
	}
	return pins
}

// PinnedServices are the versions m pins its cluster services at.
func (m Manifest) PinnedServices() []Pin {
	var pins []Pin
	for _, s := range m.Services {
		pins = append(pins, Pin{s.Name, s.Version})
	}
	return pins
}

// files are the files of every component and service, each with what it
// belongs to.
func (m Manifest) files() []ownedFile {
	var files []ownedFile
	for _, c := range m.Components {
		for _, f := range c.Files {
			files = append(files, ownedFile{c.Name, f})
		}
	}
	for _, s := range m.Services {
		files = append(files, ownedFile{s.Name, s.Image})
	}
	return files
}

// ownedFile is a file of a release, with the component or service it
// belongs to.
type ownedFile struct{ owner, name string }

// Component returns the component called name.
func (m Manifest) Component(name string) (Component, bool) {
	i := slices.IndexFunc(m.Components, func(c Component) bool { return c.Name == name })
	if i < 0 {
		return Component{}, false
	}
	return m.Components[i], true
}

// CheckVersion checks that version can name a release: letters, digits,
// dots, hyphens and pluses, not starting with a dot, so that it names a
// directory in a release source and nothing outside it.
func CheckVersion(version string) error {
	if version == "" || version[0] == '.' || strings.Trim(version, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-+") != "" {
		return fmt.Errorf("%q isn't a release version", version)
	}
	return nil
}

// Release is a release in a release source.
type Release struct {
	Manifest
	dir string
	// sums are the hex SHA-256 sums by path in the release.
	sums map[string]string
}

// filePath is the path of a component's file in a release.
func filePath(arch, file string) string { return path.Join("linux-"+arch, file) }

// Open opens the release with version in the release source at source.
func Open(source, version string) (*Release, error) {
	if err := CheckVersion(version); err != nil {
		return nil, err
	}
	dir := filepath.Join(source, version)
	data, err := os.ReadFile(filepath.Join(dir, manifestName))
	if errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("release %s isn't in %s: %w", version, source, err)
	}
	if err != nil {
		return nil, err
	}
	r := &Release{dir: dir}
	// A newer release's manifest may have fields that this one doesn't
	// know, which a mooctl reads to say what it refuses.
	if err := strictyaml.UnmarshalNewer(data, &r.Manifest); err != nil {
		return nil, fmt.Errorf("release %s: %s: %w", version, manifestName, err)
	}
	if r.Version != version {
		return nil, fmt.Errorf("release %s: its manifest is for release %s", version, r.Version)
	}
	if r.sums, err = readSums(filepath.Join(dir, sumsName)); err != nil {
		return nil, fmt.Errorf("release %s: %w", version, err)
	}
	// The agent, the updater and the cluster services say which release a
	// node runs, so each is at the release's own version; containerd and
	// runc have versions of their own.
	for _, c := range r.Components {
		if c.Name == "" || c.Version == "" || len(c.Files) == 0 {
			return nil, fmt.Errorf("release %s: a component without a name, version or files", version)
		}
		if (c.Name == "agent" || c.Name == "updater") && c.Version != version {
			return nil, fmt.Errorf("release %s: its %s, at %s, isn't at the release's version", version, c.Name, c.Version)
		}
	}
	names, apis := map[string]bool{}, map[string]bool{}
	for _, s := range r.Services {
		if s.Name == "" || s.Version == "" || s.API == "" || s.Image == "" || s.User == "" {
			return nil, fmt.Errorf("release %s: a service without a name, version, API, image or user", version)
		}
		if s.Version != version {
			return nil, fmt.Errorf("release %s: its %s service, at %s, isn't at the release's version", version, s.Name, s.Version)
		}
		// A service the node API wouldn't run is refused before anything
		// changes, rather than once the container it replaces is gone.
		if err := nodev1.CheckContainer(s.Container()); err != nil {
			return nil, fmt.Errorf("release %s: its %s service can't run: %s", version, s.Name, status.Convert(err).Message())
		}
		if names[s.Name] || apis[s.API] {
			return nil, fmt.Errorf("release %s: two services called %s or serving %s", version, s.Name, s.API)
		}
		names[s.Name], apis[s.API] = true, true
	}
	for _, f := range r.files() {
		if f.name == "" || f.name == "." || f.name == ".." || strings.ContainsAny(f.name, `/\`) {
			return nil, fmt.Errorf("release %s: %s's file %q isn't a plain file name", version, f.owner, f.name)
		}
		for _, arch := range Architectures {
			if _, ok := r.sums[filePath(arch, f.name)]; !ok {
				return nil, fmt.Errorf("release %s: %s has no checksum", version, filePath(arch, f.name))
			}
		}
	}
	return r, nil
}

// readSums reads a SHA256SUMS file, as sha256sum writes it.
func readSums(file string) (map[string]string, error) {
	data, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	sums := map[string]string{}
	s := bufio.NewScanner(bytes.NewReader(data))
	for s.Scan() {
		sum, name, ok := strings.Cut(s.Text(), "  ")
		if !ok || len(sum) != sha256.Size*2 {
			return nil, fmt.Errorf("%s: a malformed line: %q", sumsName, s.Text())
		}
		sums[name] = sum
	}
	return sums, s.Err()
}

// ReadFile reads a component's file for arch, and checks it against its
// checksum.
func (r *Release) ReadFile(arch, file string) ([]byte, error) {
	p := filePath(arch, file)
	want, ok := r.sums[p]
	if !ok {
		return nil, fmt.Errorf("release %s has no %s", r.Version, p)
	}
	data, err := os.ReadFile(filepath.Join(r.dir, filepath.FromSlash(p)))
	if err != nil {
		return nil, err
	}
	if got := sha256.Sum256(data); hex.EncodeToString(got[:]) != want {
		return nil, fmt.Errorf("release %s: %s doesn't match its checksum", r.Version, p)
	}
	return data, nil
}

// Write writes m's manifest and checksums into dir, whose linux-<arch>
// directories already hold each component's files and service's image.
func Write(dir string, m Manifest) error {
	var sums bytes.Buffer
	for _, arch := range Architectures {
		for _, f := range m.files() {
			p := filePath(arch, f.name)
			data, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(p)))
			if err != nil {
				return err
			}
			sum := sha256.Sum256(data)
			fmt.Fprintf(&sums, "%s  %s\n", hex.EncodeToString(sum[:]), p)
		}
	}
	data, err := yaml.Marshal(m)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, manifestName), data, 0o644); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, sumsName), sums.Bytes(), 0o644)
}

// Build is the build of the component of the files files for arch, as
// node status says it, from the release's checksums.
func (r *Release) Build(arch string, files []string) (string, error) {
	sums := map[string]string{}
	for _, f := range files {
		sum, ok := r.sums[filePath(arch, f)]
		if !ok {
			return "", fmt.Errorf("release %s has no %s", r.Version, filePath(arch, f))
		}
		sums[f] = sum
	}
	return buildid.FromSums(sums), nil
}

// ServiceBuild is the build of the cluster service s for arch, as node
// status says it: its image's digest, read from its image, which is
// checked against its checksum.
func (r *Release) ServiceBuild(arch string, s Service) (string, error) {
	archive, err := r.ReadFile(arch, s.Image)
	if err != nil {
		return "", err
	}
	return ociimage.Digest(archive)
}
