//go:build linux || darwin

package cli_test

import (
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/moostackhq/moostack/internal/secretstore"
)

func TestAFailedClusterInitRemovesTheDirectoriesItMade(t *testing.T) {
	for name, rel := range map[string]string{
		"the cluster directory": "prod",
		"its parent too":        filepath.Join("clusters", "prod"),
	} {
		t.Run(name, func(t *testing.T) {
			o := newOperator(t, "op")
			root := o.dir
			o.dir = filepath.Join(root, rel)
			before := snapshot(t, root)
			// The new store is 758 bytes, and 1028 with the CA key in it,
			// so cluster init fails at its last step, with everything else
			// written.
			var r result
			withFileSizeLimit(t, 900, func() { r = o.run("cluster", "init", "--ssh-key", o.key, "--name", "prod") })
			if r.err == nil || !strings.Contains(r.err.Error(), "saving the cluster CA key") {
				t.Fatalf("cluster init with files limited to 900 bytes: %v, want it to fail saving the CA key", r.err)
			}
			wantSnapshot(t, root, before)

			o.mustRun("cluster", "init", "--ssh-key", o.key, "--name", "prod")
		})
	}
}

func TestAFailedClusterInitKeepsTheLockFileOfTheStoreItLeaves(t *testing.T) {
	o := newOperator(t, "op")
	o.mustRun("secret", "init", "--ssh-key", o.key)
	want := snapshot(t, o.dir)
	// Save made the lock file, but the store is still there for someone
	// to lock, so the lock file must stay.
	want[filepath.Join(o.dir, secretstore.LockFileName)] = ""
	var r result
	withFileSizeLimit(t, 900, func() { r = o.run("cluster", "init", "--ssh-key", o.key, "--name", "prod") })
	if r.err == nil || !strings.Contains(r.err.Error(), "saving the cluster CA key") {
		t.Fatalf("cluster init with files limited to 900 bytes: %v, want it to fail saving the CA key", r.err)
	}
	wantSnapshot(t, o.dir, want)
}

// withFileSizeLimit calls f with every file this process writes limited
// to limit bytes. A write past the limit fails, since Go ignores the
// signal that would kill the process.
func withFileSizeLimit(t *testing.T, limit uint64, f func()) {
	t.Helper()
	var old syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_FSIZE, &old); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &syscall.Rlimit{Cur: limit, Max: old.Max}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &old); err != nil {
			t.Fatal(err)
		}
	}()
	f()
}
