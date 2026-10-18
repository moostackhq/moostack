// Package release reads and writes Moostack releases: a manifest that
// pins each node component's version, the components' files for each
// architecture, and their checksums.
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

// Manifest pins the version of each node component.
type Manifest struct {
	Version    string      `yaml:"version"`
	Components []Component `yaml:"components"`
}

// Component is a node component in a release.
type Component struct {
	Name    string `yaml:"name"`
	Version string `yaml:"version"`
	// Files are the component's files, the same for every architecture.
	Files []string `yaml:"files"`
}

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
		for _, f := range c.Files {
			if f == "" || f == "." || f == ".." || strings.ContainsAny(f, `/\`) {
				return nil, fmt.Errorf("release %s: %s's file %q isn't a plain file name", version, c.Name, f)
			}
		}
		for _, arch := range Architectures {
			for _, f := range c.Files {
				if _, ok := r.sums[filePath(arch, f)]; !ok {
					return nil, fmt.Errorf("release %s: %s has no checksum", version, filePath(arch, f))
				}
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
// directories already hold each component's files.
func Write(dir string, m Manifest) error {
	var sums bytes.Buffer
	for _, arch := range Architectures {
		for _, c := range m.Components {
			for _, f := range c.Files {
				p := filePath(arch, f)
				data, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(p)))
				if err != nil {
					return err
				}
				sum := sha256.Sum256(data)
				fmt.Fprintf(&sums, "%s  %s\n", hex.EncodeToString(sum[:]), p)
			}
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
