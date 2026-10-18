package cluster_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"io/fs"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/crypto/ssh"

	"github.com/moostackhq/moostack/internal/cluster"
)

func TestInitWritesTheClusterFile(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "prod")
	if _, err := cluster.Init(dir, cluster.Cluster{Name: "prod", Release: "1.2.3"}); err != nil {
		t.Fatal(err)
	}
	got := readFile(t, filepath.Join(dir, "cluster.yml"))
	want := "name: prod\nrelease: 1.2.3\ngeneration: 1\n"
	if got != want {
		t.Errorf("cluster.yml is\n%s\nwant\n%s", got, want)
	}
}

func TestInitRefusesAnExistingCluster(t *testing.T) {
	dir := t.TempDir()
	if _, err := cluster.Init(dir, cluster.Cluster{Name: "prod", Release: "1.2.3"}); err != nil {
		t.Fatal(err)
	}
	_, err := cluster.Init(dir, cluster.Cluster{Name: "other", Release: "2.0.0"})
	if !errors.Is(err, fs.ErrExist) {
		t.Errorf("second Init: %v, want an error that is fs.ErrExist", err)
	}
	if got := readFile(t, filepath.Join(dir, "cluster.yml")); !strings.Contains(got, "name: prod") {
		t.Errorf("the second Init changed cluster.yml to\n%s", got)
	}
}

func TestInitRefusesInvalidNames(t *testing.T) {
	for _, name := range []string{"", "Prod", "prod_1", "-prod", "prod-", strings.Repeat("a", 64), "a.b"} {
		dir := t.TempDir()
		if _, err := cluster.Init(dir, cluster.Cluster{Name: name, Release: "1.2.3"}); err == nil {
			t.Errorf("Init accepted the name %q", name)
		}
		if _, err := os.Stat(filepath.Join(dir, "cluster.yml")); err == nil {
			t.Errorf("Init with the name %q wrote cluster.yml", name)
		}
	}
}

func TestInitRefusesAnEmptyRelease(t *testing.T) {
	if _, err := cluster.Init(t.TempDir(), cluster.Cluster{Name: "prod"}); err == nil {
		t.Error("Init accepted an empty release")
	}
}

func TestOpenReadsWhatInitWrote(t *testing.T) {
	dir := t.TempDir()
	if _, err := cluster.Init(dir, cluster.Cluster{Name: "prod", Release: "1.2.3"}); err != nil {
		t.Fatal(err)
	}
	d, err := cluster.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	want := cluster.Cluster{Name: "prod", Release: "1.2.3", Generation: 1}
	if d.Cluster != want {
		t.Errorf("Open read %+v, want %+v", d.Cluster, want)
	}
}

func TestOpenWithoutAClusterFileIsNotExist(t *testing.T) {
	_, err := cluster.Open(t.TempDir())
	if !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("Open: %v, want an error that is fs.ErrNotExist", err)
	}
}

func TestOpenRefusesBadClusterFiles(t *testing.T) {
	for name, content := range map[string]string{
		"unknown field":  "name: prod\nrelease: 1.2.3\ngeneration: 1\nextra: x\n",
		"no name":        "release: 1.2.3\ngeneration: 1\n",
		"no release":     "name: prod\ngeneration: 1\n",
		"bad release":    "name: prod\nrelease: ../x\ngeneration: 1\n",
		"no generation":  "name: prod\nrelease: 1.2.3\n",
		"invalid name":   "name: Prod\nrelease: 1.2.3\ngeneration: 1\n",
		"two documents":  "name: prod\nrelease: 1.2.3\ngeneration: 1\n---\nname: x\n",
		"not a mapping":  "- prod\n",
		"bad generation": "name: prod\nrelease: 1.2.3\ngeneration: one\n",
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			writeFile(t, filepath.Join(dir, "cluster.yml"), content)
			if _, err := cluster.Open(dir); err == nil {
				t.Errorf("Open accepted\n%s", content)
			}
		})
	}
}

func TestAddNodeWritesANodeFile(t *testing.T) {
	d := newCluster(t)
	key := hostKey(t)
	n := cluster.Node{
		Name:           "web-1",
		Address:        "web1.example.com",
		User:           "op",
		HostKey:        key,
		PrivateAddress: netip.MustParseAddr("10.0.0.5"),
		Architecture:   "amd64",
	}
	if err := d.AddNode(n); err != nil {
		t.Fatal(err)
	}
	got := readFile(t, filepath.Join(d.Path(), "nodes", "web-1.yml"))
	want := "address: web1.example.com\n" +
		"user: op\n" +
		"host_key: " + strings.TrimSpace(string(ssh.MarshalAuthorizedKey(key))) + "\n" +
		"private_address: 10.0.0.5\n" +
		"architecture: amd64\n"
	if got != want {
		t.Errorf("web-1.yml is\n%s\nwant\n%s", got, want)
	}
}

func TestNodesReadsWhatAddNodeWrote(t *testing.T) {
	d := newCluster(t)
	for _, name := range []string{"web-2", "db", "web-1"} {
		if err := d.AddNode(node(t, name)); err != nil {
			t.Fatal(err)
		}
	}
	reopened, err := cluster.Open(d.Path())
	if err != nil {
		t.Fatal(err)
	}
	nodes, err := reopened.Nodes()
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, n := range nodes {
		names = append(names, n.Name)
	}
	if got, want := strings.Join(names, " "), "db web-1 web-2"; got != want {
		t.Errorf("Nodes are %s, want %s, by name", got, want)
	}
	want := node(t, "db")
	got := nodes[0]
	if got.Address != want.Address || got.User != want.User || got.PrivateAddress != want.PrivateAddress ||
		got.Architecture != want.Architecture || !keysEqual(got.HostKey, want.HostKey) {
		t.Errorf("Nodes read %+v, want %+v", got, want)
	}
}

func TestNodesOfANewClusterAreNone(t *testing.T) {
	nodes, err := newCluster(t).Nodes()
	if err != nil || len(nodes) != 0 {
		t.Errorf("Nodes: %v, %v, want none", nodes, err)
	}
}

func TestAddNodeRefusesATakenName(t *testing.T) {
	d := newCluster(t)
	if err := d.AddNode(node(t, "web-1")); err != nil {
		t.Fatal(err)
	}
	before := readFile(t, filepath.Join(d.Path(), "nodes", "web-1.yml"))
	other := node(t, "web-1")
	other.Address = "elsewhere"
	if err := d.AddNode(other); !errors.Is(err, fs.ErrExist) {
		t.Errorf("AddNode of a taken name: %v, want an error that is fs.ErrExist", err)
	}
	if got := readFile(t, filepath.Join(d.Path(), "nodes", "web-1.yml")); got != before {
		t.Errorf("AddNode of a taken name changed the node file to\n%s", got)
	}
}

func TestFreeNodeNameIsTheFirstFreeNodeN(t *testing.T) {
	d := newCluster(t)
	name, err := d.FreeNodeName()
	if err != nil || name != "node1" {
		t.Errorf("FreeNodeName of a new cluster: %q, %v, want node1", name, err)
	}
	for _, name := range []string{"node1", "node3", "web"} {
		if err := d.AddNode(node(t, name)); err != nil {
			t.Fatal(err)
		}
	}
	if name, err := d.FreeNodeName(); err != nil || name != "node2" {
		t.Errorf("FreeNodeName with node1 and node3 taken: %q, %v, want node2", name, err)
	}
	if err := d.AddNode(node(t, "node2")); err != nil {
		t.Fatal(err)
	}
	if name, err := d.FreeNodeName(); err != nil || name != "node4" {
		t.Errorf("FreeNodeName with node1 to node3 taken: %q, %v, want node4", name, err)
	}
}

func TestCheckNewNodeRefusesWhatAddNodeWould(t *testing.T) {
	d := newCluster(t)
	if err := d.CheckNewNode(node(t, "web-1")); err != nil {
		t.Errorf("CheckNewNode of a new node: %v", err)
	}
	if err := d.AddNode(node(t, "web-1")); err != nil {
		t.Fatal(err)
	}
	if err := d.CheckNewNode(node(t, "web-1")); !errors.Is(err, fs.ErrExist) {
		t.Errorf("CheckNewNode of a taken name: %v, want an error that is fs.ErrExist", err)
	}
	incomplete := node(t, "web-2")
	incomplete.User = ""
	if err := d.CheckNewNode(incomplete); err == nil {
		t.Error("CheckNewNode accepted a node without a user")
	}
}

func TestAddNodeRefusesIncompleteNodes(t *testing.T) {
	for name, change := range map[string]func(*cluster.Node){
		"invalid name":       func(n *cluster.Node) { n.Name = "Web_1" },
		"no address":         func(n *cluster.Node) { n.Address = "" },
		"no user":            func(n *cluster.Node) { n.User = "" },
		"no host key":        func(n *cluster.Node) { n.HostKey = nil },
		"no private address": func(n *cluster.Node) { n.PrivateAddress = netip.Addr{} },
		"no architecture":    func(n *cluster.Node) { n.Architecture = "" },
	} {
		t.Run(name, func(t *testing.T) {
			d := newCluster(t)
			n := node(t, "web-1")
			change(&n)
			if err := d.AddNode(n); err == nil {
				t.Error("AddNode accepted it")
			}
			if entries, _ := os.ReadDir(filepath.Join(d.Path(), "nodes")); len(entries) != 0 {
				t.Errorf("AddNode left %d files in nodes/", len(entries))
			}
		})
	}
}

func TestNodesRefusesBadNodeFiles(t *testing.T) {
	good := "address: web1\nuser: op\nhost_key: " + strings.TrimSpace(string(ssh.MarshalAuthorizedKey(hostKey(t)))) +
		"\nprivate_address: 10.0.0.5\narchitecture: amd64\n"
	for name, file := range map[string]struct{ name, content string }{
		"unknown field":   {"web-1.yml", good + "extra: x\n"},
		"bad host key":    {"web-1.yml", strings.Replace(good, "ssh-ed25519", "ssh-nonsense", 1)},
		"bad address":     {"web-1.yml", strings.Replace(good, "10.0.0.5", "10.0.0", 1)},
		"no architecture": {"web-1.yml", strings.Replace(good, "architecture: amd64\n", "", 1)},
		"invalid name":    {"Web_1.yml", good},
	} {
		t.Run(name, func(t *testing.T) {
			d := newCluster(t)
			writeFile(t, filepath.Join(d.Path(), "nodes", file.name), file.content)
			if _, err := d.Nodes(); err == nil {
				t.Errorf("Nodes accepted %s:\n%s", file.name, file.content)
			}
		})
	}
}

func newCluster(t *testing.T) *cluster.Dir {
	t.Helper()
	d, err := cluster.Init(t.TempDir(), cluster.Cluster{Name: "prod", Release: "1.2.3"})
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// node is a complete node named name, the same every time for a name.
func node(t *testing.T, name string) cluster.Node {
	t.Helper()
	seed := make([]byte, ed25519.SeedSize)
	copy(seed, name)
	key, err := ssh.NewPublicKey(ed25519.NewKeyFromSeed(seed).Public())
	if err != nil {
		t.Fatal(err)
	}
	return cluster.Node{
		Name:           name,
		Address:        name + ".example.com",
		User:           "root",
		HostKey:        key,
		PrivateAddress: netip.MustParseAddr("10.0.0.7"),
		Architecture:   "arm64",
	}
}

func hostKey(t *testing.T) ssh.PublicKey {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	key, err := ssh.NewPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func keysEqual(a, b ssh.PublicKey) bool {
	return a != nil && b != nil && string(a.Marshal()) == string(b.Marshal())
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}
