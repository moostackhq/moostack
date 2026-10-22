package hostlayout_test

import (
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/moostackhq/moostack/internal/hostlayout"
)

func TestANodeFileReadsBackInEitherMode(t *testing.T) {
	for _, want := range []hostlayout.NodeConfig{
		{Cluster: "test", Node: "node1", Mode: hostlayout.ModeDirect, PrivateAddress: netip.MustParseAddr("10.0.0.1")},
		{Cluster: "test", Node: "node1", Mode: hostlayout.ModeControlPlane, ControlPlane: "cp.example.com:7444", PrivateAddress: netip.MustParseAddr("10.0.0.1")},
	} {
		data, err := want.Marshal()
		if err != nil {
			t.Fatal(err)
		}
		got, err := hostlayout.ParseNodeConfig(data)
		if err != nil {
			t.Fatalf("parsing\n%s: %v", data, err)
		}
		if got != want {
			t.Errorf("parsed %+v, want %+v", got, want)
		}
	}
}

func TestANodeFileInDirectModeLeavesOutTheControlPlane(t *testing.T) {
	data, err := hostlayout.NodeConfig{Cluster: "test", Node: "node1", Mode: hostlayout.ModeDirect, PrivateAddress: netip.MustParseAddr("10.0.0.1")}.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if want := "cluster: test\nnode: node1\nmode: direct\nprivate_address: 10.0.0.1\n"; string(data) != want {
		t.Errorf("the node file is\n%s\nwant\n%s", data, want)
	}
}

func TestParseNodeConfigRefusesAModeWithoutWhatItNeeds(t *testing.T) {
	base := "cluster: test\nnode: node1\nprivate_address: 10.0.0.1\n"
	for _, c := range []struct{ file, want string }{
		{base + "mode: control_plane\n", "control plane's address"},
		{base + "mode: control_plane\ncontrol_plane: cp.example.com\n", "control plane's address"},
		{base + "mode: control_plane\ncontrol_plane: :7444\n", "control plane's address"},
		{base + "mode: control_plane\ncontrol_plane: cp.example.com:0\n", "control plane's address"},
		{base + "mode: control_plane\ncontrol_plane: cp.example.com:http\n", "control plane's address"},
		{base + "mode: control_plane\ncontrol_plane: cp.example.com:65536\n", "control plane's address"},
		{base + "mode: direct\ncontrol_plane: cp.example.com:7444\n", "direct mode"},
		{base + "mode: other\n", "unknown mode"},
		{base, "unknown mode"},
	} {
		_, err := hostlayout.ParseNodeConfig([]byte(c.file))
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("parsing\n%s: %v, want an error about %s", c.file, err, c.want)
		}
	}
}

// Any node file ParseNodeConfig reads reads back the same once written
// again, and nothing makes it panic.
func FuzzParseNodeConfig(f *testing.F) {
	f.Add("cluster: test\nnode: node1\nmode: direct\nprivate_address: 10.0.0.1\n")
	f.Add("cluster: test\nnode: node1\nmode: control_plane\ncontrol_plane: cp.example.com:7444\nprivate_address: 10.0.0.1\n")
	f.Add("cluster: test\nnode: node1\nmode: control_plane\ncontrol_plane: \"[2001:db8::1]:443\"\nprivate_address: 2001:db8::2\n")
	f.Add("mode: other\n")
	f.Fuzz(func(t *testing.T, file string) {
		c, err := hostlayout.ParseNodeConfig([]byte(file))
		if err != nil {
			return
		}
		data, err := c.Marshal()
		if err != nil {
			t.Fatalf("writing %+v: %v", c, err)
		}
		again, err := hostlayout.ParseNodeConfig(data)
		if err != nil || again != c {
			t.Errorf("%q parsed as %+v, which reads back as %+v, %v", file, c, again, err)
		}
	})
}

func TestReadNodeConfigNamesTheFileItCantRead(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.yml")
	if _, err := hostlayout.ReadNodeConfig(path); err == nil || !strings.Contains(err.Error(), path) {
		t.Errorf("reading a node file that isn't there: %v, want an error naming it", err)
	}
	if err := os.WriteFile(path, []byte("mode: other\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := hostlayout.ReadNodeConfig(path); err == nil || !strings.Contains(err.Error(), path) {
		t.Errorf("reading a node file that isn't one: %v, want an error naming it", err)
	}
}
