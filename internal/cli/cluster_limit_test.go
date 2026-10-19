//go:build linux || darwin

package cli_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"

	"github.com/moostackhq/moostack/internal/cli"
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
			r := o.runLimited(900, "cluster", "init", "--ssh-key", o.key, "--name", "prod")
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
	r := o.runLimited(900, "cluster", "init", "--ssh-key", o.key, "--name", "prod")
	if r.err == nil || !strings.Contains(r.err.Error(), "saving the cluster CA key") {
		t.Fatalf("cluster init with files limited to 900 bytes: %v, want it to fail saving the CA key", r.err)
	}
	wantSnapshot(t, o.dir, want)
}

// runLimited runs mooctl with args, as run does, but in a process of its
// own whose files are each limited to limit bytes. A write past the limit
// fails, since Go ignores the signal that would kill the process. The
// test process itself isn't limited, since it writes its own files, such
// as go test's log, all the while.
func (o *operator) runLimited(limit uint64, args ...string) result {
	o.t.Helper()
	cmd := exec.Command(os.Args[0], append([]string{"-C", o.dir}, args...)...)
	cmd.Env = append(os.Environ(),
		limitedEnv+"="+strconv.FormatUint(limit, 10),
		"HOME="+o.home,
		"MOOCTL_TEST_VERSION="+o.version,
	)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	if err != nil {
		err = fmt.Errorf("%w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return result{stdout: stdout.String(), stderr: stderr.String(), err: err}
}

// limitedEnv, set to a size in bytes, makes the test binary run mooctl
// with its arguments, and every file limited to that size, instead of
// running the tests.
const limitedEnv = "MOOCTL_TEST_FILE_SIZE_LIMIT"

func TestMain(m *testing.M) {
	if s := os.Getenv(limitedEnv); s != "" {
		os.Exit(runLimitedMooctl(s))
	}
	os.Exit(m.Run())
}

// runLimitedMooctl runs mooctl as runLimited asks, with files limited to
// limit bytes, and returns its exit code.
func runLimitedMooctl(limit string) int {
	n, err := strconv.ParseUint(limit, 10, 64)
	if err == nil {
		var old syscall.Rlimit
		if err = syscall.Getrlimit(syscall.RLIMIT_FSIZE, &old); err == nil {
			err = syscall.Setrlimit(syscall.RLIMIT_FSIZE, &syscall.Rlimit{Cur: n, Max: old.Max})
		}
	}
	if err == nil {
		env := cli.Env{Version: os.Getenv("MOOCTL_TEST_VERSION"), Home: os.Getenv("HOME"), Stdout: os.Stdout, Stderr: os.Stderr, Exit: os.Exit}
		err = cli.New(env).Run(context.Background(), append([]string{"mooctl"}, os.Args[1:]...))
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return 0
}
