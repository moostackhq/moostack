// Package cluster reads and writes a cluster directory: the cluster file
// and one file per node.
package cluster

import (
	"errors"
	"fmt"
	"io/fs"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"golang.org/x/crypto/ssh"
	"gopkg.in/yaml.v3"

	"github.com/moostackhq/moostack/internal/release"
	"github.com/moostackhq/moostack/internal/strictyaml"
)

// FileName is the cluster file's name in the cluster directory.
const FileName = "cluster.yml"

// CAFileName is the cluster CA certificate's file name in the cluster
// directory. Its key is in the secret store.
const CAFileName = "ca.crt"

// nodesDir is the directory of node files, one per node, each named for
// its node.
const nodesDir = "nodes"

// Cluster is what the cluster file holds.
type Cluster struct {
	Name string `yaml:"name"`
	// Release is the pinned Moostack release.
	Release string `yaml:"release"`
	// Generation is the cluster directory's generation, 1 when created.
	Generation int64 `yaml:"generation"`
}

func (c Cluster) check() error {
	if err := CheckName(c.Name); err != nil {
		return fmt.Errorf("cluster name: %w", err)
	}
	if err := release.CheckVersion(c.Release); err != nil {
		return fmt.Errorf("release: %w", err)
	}
	if c.Generation < 1 {
		return errors.New("no generation")
	}
	return nil
}

// Node is a node's entry in the cluster directory.
type Node struct {
	// Name names the node file.
	Name string
	// Address is the SSH host, as the operator gave it.
	Address string
	// User is the SSH user.
	User string
	// HostKey is the node's pinned SSH host key.
	HostKey ssh.PublicKey
	// PrivateAddress is the address other nodes reach the node at.
	PrivateAddress netip.Addr
	// Architecture is amd64 or arm64.
	Architecture string
}

// nodeFile is a node file's content.
type nodeFile struct {
	Address        string `yaml:"address"`
	User           string `yaml:"user"`
	HostKey        string `yaml:"host_key"`
	PrivateAddress string `yaml:"private_address"`
	Architecture   string `yaml:"architecture"`
}

func (n Node) check() error {
	if err := CheckName(n.Name); err != nil {
		return fmt.Errorf("node name: %w", err)
	}
	switch {
	case n.Address == "":
		return errors.New("no address")
	case n.User == "":
		return errors.New("no user")
	case n.HostKey == nil:
		return errors.New("no host key")
	case !n.PrivateAddress.IsValid():
		return errors.New("no private address")
	case n.Architecture == "":
		return errors.New("no architecture")
	}
	return nil
}

// CheckName checks that name can name a cluster or a node: a DNS label
// of lowercase letters, digits and hyphens.
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

// Dir is an open cluster directory.
type Dir struct {
	path    string
	Cluster Cluster
}

// Path is the directory's path.
func (d *Dir) Path() string { return d.path }

// Init makes path a cluster directory for c, with the cluster CA
// certificate caCert, creating the directory if needed, at generation 1.
// It refuses a directory that already has a cluster file or a CA
// certificate, with an error that is fs.ErrExist. If it fails, it leaves
// neither behind.
func Init(path string, c Cluster, caCert []byte) (*Dir, error) {
	c.Generation = 1
	if err := c.check(); err != nil {
		return nil, err
	}
	data, err := yaml.Marshal(c)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(path, 0o755); err != nil {
		return nil, err
	}
	if err := CheckInit(path); err != nil {
		return nil, err
	}
	file := filepath.Join(path, FileName)
	caFile := filepath.Join(path, CAFileName)
	if err := createFile(caFile, caCert); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return nil, caExistsError(path)
		}
		return nil, err
	}
	if err := createFile(file, data); err != nil {
		os.Remove(caFile)
		if errors.Is(err, fs.ErrExist) {
			return nil, existsError(path)
		}
		return nil, err
	}
	return &Dir{path: path, Cluster: c}, nil
}

// CheckInit checks that Init would accept path, as far as it is now:
// that it has neither a cluster file nor a CA certificate.
func CheckInit(path string) error {
	if _, err := os.Lstat(filepath.Join(path, FileName)); err == nil {
		return existsError(path)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	if _, err := os.Lstat(filepath.Join(path, CAFileName)); err == nil {
		return caExistsError(path)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

// caExistsError says that path already has a CA certificate. It is
// fs.ErrExist.
func caExistsError(path string) error {
	return fmt.Errorf("%s already has a cluster CA, %s: %w", path, CAFileName, fs.ErrExist)
}

// existsError says that path is already a cluster directory. It is
// fs.ErrExist.
func existsError(path string) error {
	return fmt.Errorf("%s is already a cluster directory: %w", path, fs.ErrExist)
}

// Open opens the cluster directory at path. If it has no cluster file,
// the error is fs.ErrNotExist.
func Open(path string) (*Dir, error) {
	file := filepath.Join(path, FileName)
	data, err := os.ReadFile(file)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("%s is not a cluster directory, since it has no %s: %w", path, FileName, err)
	}
	if err != nil {
		return nil, err
	}
	var c Cluster
	if err := strictyaml.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("%s: %w", file, err)
	}
	if err := c.check(); err != nil {
		return nil, fmt.Errorf("%s: %w", file, err)
	}
	return &Dir{path: path, Cluster: c}, nil
}

// CACert is the cluster CA certificate, PEM.
func (d *Dir) CACert() ([]byte, error) {
	return os.ReadFile(filepath.Join(d.path, CAFileName))
}

// Nodes are the nodes in the directory, by name.
func (d *Dir) Nodes() ([]Node, error) {
	entries, err := os.ReadDir(filepath.Join(d.path, nodesDir))
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
		n, err := d.readNode(name)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", filepath.Join(d.path, nodesDir, e.Name()), err)
		}
		nodes = append(nodes, n)
	}
	slices.SortFunc(nodes, func(a, b Node) int { return strings.Compare(a.Name, b.Name) })
	return nodes, nil
}

// readNode reads the file of the node called name.
func (d *Dir) readNode(name string) (Node, error) {
	data, err := os.ReadFile(d.nodePath(name))
	if err != nil {
		return Node{}, err
	}
	var f nodeFile
	if err := strictyaml.Unmarshal(data, &f); err != nil {
		return Node{}, err
	}
	n := Node{Name: name, Address: f.Address, User: f.User, Architecture: f.Architecture}
	if f.HostKey != "" {
		if n.HostKey, _, _, _, err = ssh.ParseAuthorizedKey([]byte(f.HostKey)); err != nil {
			return Node{}, fmt.Errorf("host_key: %w", err)
		}
	}
	if f.PrivateAddress != "" {
		if n.PrivateAddress, err = netip.ParseAddr(f.PrivateAddress); err != nil {
			return Node{}, fmt.Errorf("private_address: %w", err)
		}
	}
	return n, n.check()
}

// FreeNodeName is the first of node1, node2 and so on that no node in
// the directory has.
func (d *Dir) FreeNodeName() (string, error) {
	nodes, err := d.Nodes()
	if err != nil {
		return "", err
	}
	taken := map[string]bool{}
	for _, n := range nodes {
		taken[n.Name] = true
	}
	for i := 1; ; i++ {
		if name := fmt.Sprintf("node%d", i); !taken[name] {
			return name, nil
		}
	}
}

// CheckNewNode checks that AddNode would accept n, as far as the
// directory is now.
func (d *Dir) CheckNewNode(n Node) error {
	if err := n.check(); err != nil {
		return err
	}
	if _, err := os.Lstat(d.nodePath(n.Name)); err == nil {
		return errTaken(n.Name, fs.ErrExist)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

// AddNode writes n's node file. It refuses a name already taken, with an
// error that is fs.ErrExist.
func (d *Dir) AddNode(n Node) error {
	if err := d.CheckNewNode(n); err != nil {
		return err
	}
	data, err := yaml.Marshal(nodeFile{
		Address:        n.Address,
		User:           n.User,
		HostKey:        strings.TrimSpace(string(ssh.MarshalAuthorizedKey(n.HostKey))),
		PrivateAddress: n.PrivateAddress.String(),
		Architecture:   n.Architecture,
	})
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(d.path, nodesDir), 0o755); err != nil {
		return err
	}
	if err := createFile(d.nodePath(n.Name), data); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return errTaken(n.Name, err)
		}
		return err
	}
	return nil
}

// errTaken says that name is taken, wrapping err.
func errTaken(name string, err error) error {
	return fmt.Errorf("the cluster already has a node called %s: %w", name, err)
}

func (d *Dir) nodePath(name string) string {
	return filepath.Join(d.path, nodesDir, name+".yml")
}

// createFile writes data to a new file at path, which appears whole or
// not at all. It refuses an existing path, with an error that is
// fs.ErrExist.
func createFile(path string, data []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	_, err = tmp.Write(data)
	if err == nil {
		err = tmp.Sync()
	}
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Chmod(tmp.Name(), 0o644)
	}
	if err != nil {
		return err
	}
	return os.Link(tmp.Name(), path)
}
