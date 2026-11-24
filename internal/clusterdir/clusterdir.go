// Package clusterdir reads and writes a cluster directory: the cluster
// file, one file per node, and where the directory of a cluster ref is.
package clusterdir

import (
	"errors"
	"fmt"
	"io/fs"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"golang.org/x/crypto/ssh"
	"gopkg.in/yaml.v3"

	"github.com/moostackhq/moostack/internal/strictyaml"
)

// FileName is the cluster file's name in the cluster directory.
const FileName = "cluster.yml"

// nodesDir is the directory of node files, one per node, each named for
// its node.
const nodesDir = "nodes"

// DefaultPrefix is the prefix of a cluster ref that has none.
const DefaultPrefix = "default"

// Ref is a cluster ref: a name, filed under a prefix.
type Ref struct {
	Prefix, Name string
}

// ParseRef parses name or prefix/name. Both parts are DNS labels.
func ParseRef(s string) (Ref, error) {
	prefix, name, ok := strings.Cut(s, "/")
	if !ok {
		prefix, name = DefaultPrefix, s
	}
	if err := CheckName(prefix); err != nil {
		return Ref{}, fmt.Errorf("cluster ref %q: prefix: %w", s, err)
	}
	if err := CheckName(name); err != nil {
		return Ref{}, fmt.Errorf("cluster ref %q: name: %w", s, err)
	}
	return Ref{Prefix: prefix, Name: name}, nil
}

// String is the ref as an operator writes it, without the default
// prefix.
func (r Ref) String() string {
	if r.Prefix == DefaultPrefix {
		return r.Name
	}
	return r.Prefix + "/" + r.Name
}

// Dir is the ref's cluster directory under root.
func (r Ref) Dir(root string) string { return filepath.Join(root, r.Prefix, r.Name) }

// Root is the directory that holds every cluster directory:
// moostack/clusters in the user's config directory.
func Root() (string, error) {
	config, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(config, "moostack", "clusters"), nil
}

// CheckName refuses a name that isn't a DNS label: 1 to 63 lowercase
// letters, digits and hyphens, not starting or ending with a hyphen.
func CheckName(name string) error {
	if name == "" || len(name) > 63 {
		return fmt.Errorf("%q must be 1 to 63 characters", name)
	}
	if name[0] == '-' || name[len(name)-1] == '-' {
		return fmt.Errorf("%q must not start or end with a hyphen", name)
	}
	for _, c := range name {
		if !('a' <= c && c <= 'z' || '0' <= c && c <= '9' || c == '-') {
			return fmt.Errorf("%q may hold only lowercase letters, digits and hyphens", name)
		}
	}
	return nil
}

// Cluster is what the cluster file holds.
type Cluster struct {
	Ref string `yaml:"ref"`
	// K3s is the pinned k3s version, such as v1.37.2+k3s1.
	K3s string `yaml:"k3s"`
	// Email is the ACME account's email, for Let's Encrypt; empty for a
	// self-signed issuer.
	Email string `yaml:"email,omitempty"`
}

// Node is what a node file holds.
type Node struct {
	// Name is the node's name.
	Name string `yaml:"-"`
	// Target is the SSH target the node is reached at.
	Target string `yaml:"target"`
	// HostKey is the node's pinned SSH host key.
	HostKey ssh.PublicKey `yaml:"-"`
	// PrivateAddress is the node's address on the private network, and
	// Interface the network interface that has it.
	PrivateAddress netip.Addr `yaml:"private_address"`
	Interface      string     `yaml:"interface"`
	// PublicAddress is the node's public address, the zero Addr, written
	// as an empty string, if it has none.
	PublicAddress netip.Addr `yaml:"public_address"`
	// Architecture is amd64 or arm64.
	Architecture string `yaml:"architecture"`
	// MemoryMiB is the node's memory.
	MemoryMiB int `yaml:"memory_mib"`
	// GOGC is the orchestrator's GOGC, empty for Go's default, and
	// GOMEMLIMIT its GOMEMLIMIT.
	GOGC         string `yaml:"gogc,omitempty"`
	GOMEMLIMIT   string `yaml:"gomemlimit"`
	ControlPlane bool   `yaml:"control_plane,omitempty"`
}

// nodeFile is a node file as YAML holds it.
type nodeFile struct {
	Node    `yaml:",inline"`
	HostKey string `yaml:"host_key"`
}

// Dir is an open cluster directory.
type Dir struct {
	Path    string
	Cluster Cluster
}

// ErrExists is returned by Init for a directory that exists.
var ErrExists = errors.New("exists already")

// Init makes path a new cluster directory for c. It refuses a path that
// exists, even one made meanwhile.
func Init(path string, c Cluster) (*Dir, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	if err := os.Mkdir(path, 0o755); errors.Is(err, fs.ErrExist) {
		return nil, fmt.Errorf("%s %w", path, ErrExists)
	} else if err != nil {
		return nil, err
	}
	d := &Dir{Path: path, Cluster: c}
	if err := d.write(FileName, c); err != nil {
		return nil, err
	}
	return d, nil
}

// Open opens the cluster directory at path.
func Open(path string) (*Dir, error) {
	data, err := os.ReadFile(filepath.Join(path, FileName))
	if err != nil {
		return nil, err
	}
	var c Cluster
	if err := strictyaml.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("%s: %w", filepath.Join(path, FileName), err)
	}
	if _, err := ParseRef(c.Ref); err != nil {
		return nil, fmt.Errorf("%s: %w", filepath.Join(path, FileName), err)
	}
	if c.K3s == "" {
		return nil, fmt.Errorf("%s: no k3s version", filepath.Join(path, FileName))
	}
	return &Dir{Path: path, Cluster: c}, nil
}

// Nodes are the cluster's nodes, the control plane first, then by name.
func (d *Dir) Nodes() ([]Node, error) {
	if info, err := os.Lstat(filepath.Join(d.Path, nodesDir)); err == nil && !info.IsDir() {
		return nil, fmt.Errorf("%s isn't a directory", filepath.Join(d.Path, nodesDir))
	}
	entries, err := os.ReadDir(filepath.Join(d.Path, nodesDir))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var nodes []Node
	for _, e := range entries {
		name, ok := strings.CutSuffix(e.Name(), ".yml")
		if !ok || e.IsDir() {
			continue
		}
		if e.Type()&fs.ModeSymlink != 0 {
			return nil, fmt.Errorf("%s is a symbolic link", filepath.Join(d.Path, nodesDir, e.Name()))
		}
		if err := CheckName(name); err != nil {
			return nil, fmt.Errorf("%s: %w", filepath.Join(d.Path, nodesDir, e.Name()), err)
		}
		n, err := d.readNode(name)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, n)
	}
	slices.SortFunc(nodes, func(a, b Node) int {
		if a.ControlPlane != b.ControlPlane {
			if a.ControlPlane {
				return -1
			}
			return 1
		}
		return compareNames(a.Name, b.Name)
	})
	return nodes, nil
}

// compareNames orders node2 before node10.
func compareNames(a, b string) int {
	an, aErr := strconv.Atoi(strings.TrimPrefix(a, "node"))
	bn, bErr := strconv.Atoi(strings.TrimPrefix(b, "node"))
	if aErr == nil && bErr == nil && strings.HasPrefix(a, "node") && strings.HasPrefix(b, "node") {
		return an - bn
	}
	return strings.Compare(a, b)
}

// ControlPlane is the cluster's control plane, and false if it has none.
func (d *Dir) ControlPlane() (Node, bool, error) {
	nodes, err := d.Nodes()
	if err != nil {
		return Node{}, false, err
	}
	for _, n := range nodes {
		if n.ControlPlane {
			return n, true, nil
		}
	}
	return Node{}, false, nil
}

// Node is the node called name.
func (d *Dir) Node(name string) (Node, error) {
	if err := CheckName(name); err != nil {
		return Node{}, err
	}
	n, err := d.readNode(name)
	if errors.Is(err, fs.ErrNotExist) {
		return Node{}, fmt.Errorf("the cluster has no node %s", name)
	}
	return n, err
}

func (d *Dir) readNode(name string) (Node, error) {
	path := filepath.Join(d.Path, nodesDir, name+".yml")
	data, err := os.ReadFile(path)
	if err != nil {
		return Node{}, err
	}
	var f nodeFile
	if err := strictyaml.Unmarshal(data, &f); err != nil {
		return Node{}, fmt.Errorf("%s: %w", path, err)
	}
	key, _, _, _, err := ssh.ParseAuthorizedKey([]byte(f.HostKey))
	if err != nil {
		return Node{}, fmt.Errorf("%s: host_key: %w", path, err)
	}
	n := f.Node
	n.Name, n.HostKey = name, key
	return n, nil
}

// WriteNode writes n's node file.
func (d *Dir) WriteNode(n Node) error {
	if err := CheckName(n.Name); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(d.Path, nodesDir), 0o755); err != nil {
		return err
	}
	f := nodeFile{Node: n, HostKey: strings.TrimSpace(string(ssh.MarshalAuthorizedKey(n.HostKey)))}
	return d.write(filepath.Join(nodesDir, n.Name+".yml"), f)
}

// RemoveNode removes the node file of the node called name.
func (d *Dir) RemoveNode(name string) error {
	return os.Remove(filepath.Join(d.Path, nodesDir, name+".yml"))
}

// write writes v as YAML to the file at name in the directory, whole or
// not at all.
func (d *Dir) write(name string, v any) error {
	data, err := yaml.Marshal(v)
	if err != nil {
		return err
	}
	return WriteFile(filepath.Join(d.Path, name), data)
}

// WriteFile writes data to path, whole or not at all: to a temporary
// file beside it, synced, then renamed over it.
func WriteFile(path string, data []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	tmp := f.Name()
	if err := f.Chmod(0o644); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp, path)
	}
	if err != nil {
		os.Remove(tmp)
	}
	return err
}
