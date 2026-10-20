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

	"gopkg.in/yaml.v3"

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
	Version    string      `yaml:"version"`
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
	Image string `yaml:"image"`
}

// ImageRef is the name of the image of version of the service called
// name, which containerd knows it by once imported.
func ImageRef(name, version string) string {
	return "moostack.local/" + name + ":" + version
}

// Service returns the service called name.
func (m Manifest) Service(name string) (Service, bool) {
	i := slices.IndexFunc(m.Services, func(s Service) bool { return s.Name == name })
	if i < 0 {
		return Service{}, false
	}
	return m.Services[i], true
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
	if err := strictyaml.Unmarshal(data, &r.Manifest); err != nil {
		return nil, fmt.Errorf("release %s: %s: %w", version, manifestName, err)
	}
	if r.Version != version {
		return nil, fmt.Errorf("release %s: its manifest is for release %s", version, r.Version)
	}
	if r.sums, err = readSums(filepath.Join(dir, sumsName)); err != nil {
		return nil, fmt.Errorf("release %s: %w", version, err)
	}
	for _, c := range r.Components {
		if c.Name == "" || c.Version == "" || len(c.Files) == 0 {
			return nil, fmt.Errorf("release %s: a component without a name, version or files", version)
		}
	}
	names, apis := map[string]bool{}, map[string]bool{}
	for _, s := range r.Services {
		if s.Name == "" || s.Version == "" || s.API == "" || s.Image == "" {
			return nil, fmt.Errorf("release %s: a service without a name, version, API or image", version)
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
