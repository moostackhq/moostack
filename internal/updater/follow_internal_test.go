//go:build linux

package updater

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	controlplanev1 "github.com/moostackhq/moostack/api/controlplane/v1"
	"github.com/moostackhq/moostack/internal/controlplane"
)

// A joined node's updater keeps its lockout while the node file is
// missing, unreadable or still says control plane mode, and lifts it
// once the file says direct mode.
func TestTheUpdaterFollowsADetachOnlyFromANodeFileThatSaysSo(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.yml")
	s := &updaterServer{cfg: Config{NodeFile: path}}
	s.controlPlane.Store(true)
	// The tunnel's listener isn't served, so it never dials.
	tunnel := controlplane.Listen(controlplane.Tunnel{Address: "cp.invalid:7444", Protocol: controlplanev1.ProtocolUpdater})
	defer tunnel.Close()
	done := make(chan struct{})
	go func() { s.followDetach(t.Context(), tunnel); close(done) }()
	base := "cluster: test\nnode: node-1\nprivate_address: 10.0.0.1\n"
	for _, c := range []struct{ name, file string }{
		{"missing", ""},
		{"not a node file", "not a node file\n"},
		{"in control plane mode", base + "mode: control_plane\ncontrol_plane: cp.example.com:7444\n"},
	} {
		os.Remove(path)
		if c.file != "" {
			if err := os.WriteFile(path, []byte(c.file), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		time.Sleep(2500 * time.Millisecond)
		if !s.controlPlane.Load() {
			t.Fatalf("with the node file %s, the updater lifted its lockout", c.name)
		}
	}
	if err := os.WriteFile(path, []byte(base+"mode: direct\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the updater didn't follow a node file in direct mode")
	}
	if s.controlPlane.Load() {
		t.Error("the updater kept its lockout once the node file said direct mode")
	}
}
