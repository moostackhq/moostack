package hostlayout_test

import (
	"net/netip"
	"os"
	"path/filepath"
	"slices"
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

func TestTheRemovedIdentitiesFileKeepsEachNodesName(t *testing.T) {
	first, second := "sha256:"+strings.Repeat("a", 64), "sha256:"+strings.Repeat("b", 64)
	removed := []hostlayout.RemovedIdentity{{Node: "node2", Identity: first}, {Node: "node3", Identity: second}}
	data := hostlayout.MarshalRemovedIdentities(removed)
	if want := `[{"node":"node2","identity":"` + first + `"},{"node":"node3","identity":"` + second + `"}]` + "\n"; string(data) != want {
		t.Errorf("the file is %q, want %q", data, want)
	}
	got, err := hostlayout.ParseRemovedIdentities(data)
	if err != nil || !slices.Equal(got, removed) {
		t.Errorf("reading it back: %v, %v, want %v", got, err, removed)
	}
}

func TestNoRemovedIdentitiesReadBackAsNone(t *testing.T) {
	got, err := hostlayout.ParseRemovedIdentities(hostlayout.MarshalRemovedIdentities(nil))
	if err != nil || len(got) != 0 {
		t.Errorf("reading no identities back: %v, %v", got, err)
	}
}

func TestARemovedIdentitiesFileItCantUseIsRefused(t *testing.T) {
	for name, content := range map[string]string{
		"fingerprints alone": `["sha256:aa"]`,
		"no node":            `[{"identity":"sha256:` + strings.Repeat("a", 64) + `"}]`,
		"an empty node":      `[{"node":"","identity":"sha256:` + strings.Repeat("a", 64) + `"}]`,
		"a null entry":       `[null]`,
		"no fingerprint":     `[{"node":"node2","identity":"sha256:aa"}]`,
		"no identity":        `[{"node":"node2"}]`,
		"an empty identity":  `[{"node":"node2","identity":""}]`,
		"garbage":            `garbage`,
	} {
		if got, err := hostlayout.ParseRemovedIdentities([]byte(content)); err == nil {
			t.Errorf("%s: read as %v, want an error", name, got)
		}
	}
}

func FuzzRemovedIdentitiesReadBack(f *testing.F) {
	f.Add([]byte(`[{"node":"a","identity":"sha256:` + strings.Repeat("0", 64) + `"}]`))
	f.Add([]byte(`null`))
	f.Add([]byte(`[null]`))
	f.Add([]byte(`["sha256:aa"]`))
	f.Fuzz(func(t *testing.T, data []byte) {
		removed, err := hostlayout.ParseRemovedIdentities(data)
		if err != nil {
			return
		}
		for _, r := range removed {
			if err := r.Check(); err != nil {
				t.Fatalf("read %+v: %v", r, err)
			}
		}
		again, err := hostlayout.ParseRemovedIdentities(hostlayout.MarshalRemovedIdentities(removed))
		if err != nil || !slices.Equal(again, removed) {
			t.Fatalf("read %v back as %v, %v", removed, again, err)
		}
	})
}

func TestARemovedIdentityMustNameANodeAndAFingerprint(t *testing.T) {
	good := "sha256:" + strings.Repeat("0", 64)
	if err := (hostlayout.RemovedIdentity{Node: "a", Identity: good}).Check(); err != nil {
		t.Errorf("a good entry: %v", err)
	}
	for name, r := range map[string]hostlayout.RemovedIdentity{
		"no node":        {Identity: good},
		"no identity":    {Node: "a"},
		"no prefix":      {Node: "a", Identity: strings.Repeat("0", 64)},
		"too short":      {Node: "a", Identity: "sha256:" + strings.Repeat("0", 63)},
		"too long":       {Node: "a", Identity: "sha256:" + strings.Repeat("0", 65)},
		"uppercase":      {Node: "a", Identity: "sha256:" + strings.Repeat("A", 64)},
		"not hex":        {Node: "a", Identity: "sha256:" + strings.Repeat("g", 64)},
		"another prefix": {Node: "a", Identity: "sha512:" + strings.Repeat("0", 64)},
	} {
		if err := r.Check(); err == nil {
			t.Errorf("%s: passed", name)
		}
	}
}
