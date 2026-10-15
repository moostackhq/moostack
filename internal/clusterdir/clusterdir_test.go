package clusterdir_test

import (
	"errors"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"golang.org/x/crypto/ssh"

	"github.com/moostackhq/moostack/internal/clusterdir"
)

func TestParseRef(t *testing.T) {
	for in, want := range map[string]clusterdir.Ref{
		"prod":      {Prefix: "default", Name: "prod"},
		"acme/prod": {Prefix: "acme", Name: "prod"},
		"default/x": {Prefix: "default", Name: "x"},
	} {
		got, err := clusterdir.ParseRef(in)
		if err != nil || got != want {
			t.Errorf("ParseRef(%q) = %v, %v, want %v", in, got, err, want)
		}
	}
	for _, in := range []string{"", "a/b/c", "/prod", "acme/", "Acme/prod", "acme/-prod", "acme/pr_od", "../prod"} {
		if _, err := clusterdir.ParseRef(in); err == nil {
			t.Errorf("ParseRef(%q) took it", in)
		}
	}
}

func TestRefStringAndDir(t *testing.T) {
	bare, _ := clusterdir.ParseRef("prod")
	full, _ := clusterdir.ParseRef("acme/prod")
	if bare.String() != "prod" || full.String() != "acme/prod" {
		t.Errorf("String: %q, %q", bare, full)
	}
	if got := bare.Dir("/r"); got != filepath.Join("/r", "default", "prod") {
		t.Errorf("Dir = %s", got)
	}
	if got := full.Dir("/r"); got != filepath.Join("/r", "acme", "prod") {
		t.Errorf("Dir = %s", got)
	}
}

func TestRootIsUnderTheUserConfigDir(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", "/cfg")
	t.Setenv("HOME", "/home/op")
	root, err := clusterdir.Root()
	if err != nil {
		t.Fatal(err)
	}
	if root != "/cfg/moostack/clusters" && root != "/home/op/Library/Application Support/moostack/clusters" {
		t.Errorf("Root = %s", root)
	}
}

func hostKey(t *testing.T) ssh.PublicKey {
	t.Helper()
	k, _, _, _, err := ssh.ParseAuthorizedKey([]byte("ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIGbHmd8BqC8tF5oS4yqF0g6z5gzK9g3s0aFZ7y2H8U5c"))
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func TestNodesRoundTripControlPlaneFirst(t *testing.T) {
	path := filepath.Join(t.TempDir(), "default", "prod")
	d, err := clusterdir.Init(path, clusterdir.Cluster{Ref: "prod", K3s: "v1.37.2+k3s1"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := clusterdir.Init(path, clusterdir.Cluster{Ref: "prod", K3s: "v1"}); !errors.Is(err, clusterdir.ErrExists) {
		t.Errorf("a second Init: %v", err)
	}
	if _, ok, err := d.ControlPlane(); ok || err != nil {
		t.Errorf("an empty cluster has a control plane: %v", err)
	}
	key := hostKey(t)
	nodes := []clusterdir.Node{
		{Name: "node10", Target: "op@h10", HostKey: key, PrivateAddress: netip.MustParseAddr("10.0.0.10"), Interface: "ens10", Architecture: "arm64", MemoryMiB: 3900, GOGC: "25", GOMEMLIMIT: "390MiB"},
		{Name: "node2", Target: "h2", HostKey: key, PrivateAddress: netip.MustParseAddr("10.0.0.3"), Interface: "ens10", PublicAddress: netip.MustParseAddr("192.0.2.10"), Architecture: "amd64", MemoryMiB: 16000, GOMEMLIMIT: "1600MiB"},
		{Name: "node3", Target: "h3", HostKey: key, PrivateAddress: netip.MustParseAddr("10.0.0.2"), Interface: "ens10", Architecture: "amd64", MemoryMiB: 1966, GOGC: "25", GOMEMLIMIT: "491MiB", ControlPlane: true},
	}
	for _, n := range nodes {
		if err := d.WriteNode(n); err != nil {
			t.Fatal(err)
		}
	}
	d, err = clusterdir.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	got, err := d.Nodes()
	if err != nil {
		t.Fatal(err)
	}
	order := []string{"node3", "node2", "node10"}
	for i, n := range got {
		if n.Name != order[i] {
			t.Errorf("node %d = %s, want %s", i, n.Name, order[i])
		}
	}
	if n2, err := d.Node("node2"); err != nil || n2.PublicAddress != netip.MustParseAddr("192.0.2.10") || got[0].PublicAddress.IsValid() {
		t.Errorf("node2's public address = %v, %v; node3's = %v", n2.PublicAddress, err, got[0].PublicAddress)
	}
	cp, ok, _ := d.ControlPlane()
	if !ok || cp.Name != "node3" || cp.PrivateAddress != netip.MustParseAddr("10.0.0.2") || cp.GOMEMLIMIT != "491MiB" || !cp.ControlPlane || string(cp.HostKey.Marshal()) != string(key.Marshal()) {
		t.Errorf("control plane = %+v", cp)
	}
	if err := d.RemoveNode("node2"); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Node("node2"); err == nil {
		t.Error("a removed node is still there")
	}
}

func TestOpenRefusesAnUnknownField(t *testing.T) {
	path := t.TempDir()
	d, err := clusterdir.Init(filepath.Join(path, "c"), clusterdir.Cluster{Ref: "c", K3s: "v1.37.2+k3s1"})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(d.Path, clusterdir.FileName), []byte("ref: c\nk3s: v1\nrelease: x\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := clusterdir.Open(d.Path); err == nil {
		t.Error("an unknown field was taken")
	}
}

func TestInitRefusesADirectoryMadeMeanwhileAndLeavesIt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "acme", "prod")
	os.MkdirAll(path, 0o755)
	os.WriteFile(filepath.Join(path, "keep"), []byte("x"), 0o644)
	if _, err := clusterdir.Init(path, clusterdir.Cluster{Ref: "acme/prod", K3s: "v1"}); !errors.Is(err, clusterdir.ErrExists) {
		t.Errorf("Init = %v", err)
	}
	if _, err := os.Stat(filepath.Join(path, "keep")); err != nil {
		t.Error("Init touched the directory it refused")
	}
}

func TestNodesRefusesNamesAndLinksItWouldNotWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c")
	d, err := clusterdir.Init(path, clusterdir.Cluster{Ref: "c", K3s: "v1"})
	if err != nil {
		t.Fatal(err)
	}
	os.MkdirAll(filepath.Join(path, "nodes"), 0o755)
	os.WriteFile(filepath.Join(path, "nodes", "Bad_Name.yml"), []byte("target: x\n"), 0o644)
	if _, err := d.Nodes(); err == nil {
		t.Error("a node file with a bad name was taken")
	}
	os.RemoveAll(filepath.Join(path, "nodes"))
	elsewhere := t.TempDir()
	if err := os.Symlink(elsewhere, filepath.Join(path, "nodes")); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Nodes(); err == nil {
		t.Error("a nodes symlink was followed")
	}
}

func TestWriteFileReplacesWholeAndLeavesNoTemporaryFile(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "f")
	os.WriteFile(p, []byte("old"), 0o644)
	if err := clusterdir.WriteFile(p, []byte("new")); err != nil {
		t.Fatal(err)
	}
	if data, _ := os.ReadFile(p); string(data) != "new" {
		t.Errorf("f = %q", data)
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 1 {
		t.Errorf("left %d files", len(entries))
	}
	if err := clusterdir.WriteFile(filepath.Join(dir, "missing", "f"), []byte("x")); err == nil {
		t.Error("a write into a missing directory passed")
	}
}

func TestNodesRefusesANodeFileThatIsALink(t *testing.T) {
	d, err := clusterdir.Init(filepath.Join(t.TempDir(), "c"), clusterdir.Cluster{Ref: "c", K3s: "v1.37.2+k3s1"})
	if err != nil {
		t.Fatal(err)
	}
	other := filepath.Join(t.TempDir(), "elsewhere.yml")
	os.WriteFile(other, []byte("target: root@h\n"), 0o644)
	os.MkdirAll(filepath.Join(d.Path, "nodes"), 0o755)
	os.Symlink(other, filepath.Join(d.Path, "nodes", "node1.yml"))
	if _, err := d.Nodes(); err == nil || !strings.Contains(err.Error(), "symbolic link") {
		t.Errorf("Nodes = %v", err)
	}
}

func TestConcurrentWritesLeaveOneWholeFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "default-cluster")
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			clusterdir.WriteFile(path, []byte(strings.Repeat(string(rune('a'+i)), 4096)))
		}()
	}
	wg.Wait()
	data, err := os.ReadFile(path)
	if err != nil || len(data) != 4096 || strings.Count(string(data), string(data[0])) != 4096 {
		t.Errorf("the file holds %d bytes, mixed: %v", len(data), err)
	}
	if entries, _ := os.ReadDir(filepath.Dir(path)); len(entries) != 1 {
		t.Errorf("left: %v", entries)
	}
}
