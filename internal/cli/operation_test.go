package cli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/moostackhq/moostack/internal/clusterdir"
	"github.com/moostackhq/moostack/internal/gitrepo"
)

// fakeHost is a lockHost in memory.
type fakeHost struct {
	lockErr   error
	recordErr error
	applied   string
	recorded  []string
	held      *fakeHeld
}

type fakeHeld struct {
	done   chan struct{}
	closed bool
}

func (h *fakeHeld) Done() <-chan struct{} { return h.done }
func (h *fakeHeld) Close() error {
	if !h.closed {
		h.closed = true
		close(h.done)
	}
	return nil
}

func (f *fakeHost) lock(context.Context) (held, error) {
	if f.lockErr != nil {
		return nil, f.lockErr
	}
	f.held = &fakeHeld{done: make(chan struct{})}
	return f.held, nil
}

func (f *fakeHost) appliedCommit(context.Context) (string, error) { return f.applied, nil }

func (f *fakeHost) recordCommit(_ context.Context, commit string) error {
	if f.recordErr != nil {
		return f.recordErr
	}
	f.recorded = append(f.recorded, commit)
	f.applied = commit
	return nil
}

// clusterRepos are a cluster directory pushed to a bare remote, and
// another machine's clone of it.
func clusterRepos(t *testing.T) (dir, other, remote string) {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")
	base := t.TempDir()
	git(t, base, "config", "--global", "user.name", "Op")
	git(t, base, "config", "--global", "user.email", "op@example.com")
	remote = filepath.Join(base, "remote.git")
	git(t, base, "init", "--bare", "--quiet", "--initial-branch=main", remote)
	dir = filepath.Join(base, "dir")
	git(t, base, "init", "--quiet", "--initial-branch=main", dir)
	os.WriteFile(filepath.Join(dir, "cluster.yml"), []byte("ref: c\n"), 0o644)
	git(t, dir, "add", "-A")
	git(t, dir, "commit", "--quiet", "-m", "cluster init c")
	git(t, dir, "remote", "add", "origin", remote)
	git(t, dir, "push", "--quiet", "-u", "origin", "HEAD")
	other = filepath.Join(base, "other")
	git(t, base, "clone", "--quiet", remote, other)
	return dir, other, remote
}

func testEnv() (Env, *bytes.Buffer) {
	var stderr bytes.Buffer
	return Env{Stdout: &bytes.Buffer{}, Stderr: &stderr}, &stderr
}

func TestAnOperationCommitsRecordsAndPushes(t *testing.T) {
	dir, other, _ := clusterRepos(t)
	env, _ := testEnv()
	host := &fakeHost{}
	ctx := t.Context()

	op, opCtx, err := begin(ctx, env, dir, host, true)
	if err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(dir, "node1.yml"), []byte("x\n"), 0o644)
	if err := op.finish(opCtx, "node-add node1", host); err != nil {
		t.Fatal(err)
	}
	op.release()
	head := git(t, dir, "rev-parse", "HEAD")
	if len(host.recorded) != 1 || host.recorded[0] != head {
		t.Errorf("recorded %v, want [%s]", host.recorded, head)
	}
	if !host.held.closed {
		t.Error("the lock is still held")
	}
	if msg := git(t, dir, "log", "-1", "--format=%s"); msg != "node-add node1" {
		t.Errorf("commit message %q", msg)
	}
	git(t, other, "fetch", "--quiet")
	if got := git(t, other, "rev-parse", "origin/main"); got != head {
		t.Errorf("the remote is at %s, want %s", got, head)
	}

	// The other machine fast-forwards to it, so the check passes there.
	op, _, err = begin(ctx, env, other, host, true)
	if err != nil {
		t.Fatalf("the other machine: %v", err)
	}
	op.release()
	if got := git(t, other, "rev-parse", "HEAD"); got != head {
		t.Errorf("the other machine is at %s, want %s", got, head)
	}
}

func TestABusyLockRefusesBeforeAnythingChanges(t *testing.T) {
	dir, other, _ := clusterRepos(t)
	env, _ := testEnv()
	os.WriteFile(filepath.Join(other, "node1.yml"), []byte("x\n"), 0o644)
	git(t, other, "add", "-A")
	git(t, other, "commit", "--quiet", "-m", "node-add node1")
	git(t, other, "push", "--quiet")
	before := git(t, dir, "rev-parse", "HEAD")

	busy := errors.New("another mooctl operation is running on this cluster")
	if _, _, err := begin(t.Context(), env, dir, &fakeHost{lockErr: busy}, true); !errors.Is(err, busy) {
		t.Fatalf("begin = %v", err)
	}
	if got := git(t, dir, "rev-parse", "HEAD"); got != before {
		t.Error("a refused operation fetched and moved the cluster directory")
	}
}

func TestDivergedHistoryRefusesAndReleasesTheLock(t *testing.T) {
	dir, other, _ := clusterRepos(t)
	env, _ := testEnv()
	os.WriteFile(filepath.Join(other, "node1.yml"), []byte("other\n"), 0o644)
	git(t, other, "add", "-A")
	git(t, other, "commit", "--quiet", "-m", "node-add node1")
	git(t, other, "push", "--quiet")
	os.WriteFile(filepath.Join(dir, "node1.yml"), []byte("here\n"), 0o644)
	git(t, dir, "add", "-A")
	git(t, dir, "commit", "--quiet", "-m", "node-add node1")
	before := git(t, dir, "rev-parse", "HEAD")

	host := &fakeHost{}
	_, _, err := begin(t.Context(), env, dir, host, true)
	if !errors.Is(err, gitrepo.ErrDiverged) || !strings.Contains(err.Error(), "git pull --no-rebase") {
		t.Fatalf("begin = %v", err)
	}
	if !host.held.closed {
		t.Error("the lock is still held")
	}
	if git(t, dir, "rev-parse", "HEAD") != before {
		t.Error("HEAD moved")
	}
}

func TestAnAppliedCommitThisMachineLacksRefuses(t *testing.T) {
	dir, _, _ := clusterRepos(t)
	env, _ := testEnv()
	// Applied from a machine that didn't push.
	host := &fakeHost{applied: "0123456789abcdef0123456789abcdef01234567"}
	_, _, err := begin(t.Context(), env, dir, host, true)
	if err == nil || !strings.Contains(err.Error(), "another machine changed the cluster") {
		t.Fatalf("begin = %v", err)
	}
	if !host.held.closed {
		t.Error("the lock is still held")
	}
}

func TestAFailedPushWarnsButTheOperationCounts(t *testing.T) {
	dir, _, remote := clusterRepos(t)
	env, stderr := testEnv()
	host := &fakeHost{}
	op, opCtx, err := begin(t.Context(), env, dir, host, true)
	if err != nil {
		t.Fatal(err)
	}
	defer op.release()
	os.RemoveAll(remote)
	os.WriteFile(filepath.Join(dir, "node1.yml"), []byte("x\n"), 0o644)
	if err := op.finish(opCtx, "node-add node1", host); err != nil {
		t.Fatalf("finish = %v", err)
	}
	if len(host.recorded) != 1 {
		t.Error("the commit wasn't recorded")
	}
	if !strings.Contains(stderr.String(), "pushing the cluster directory failed") {
		t.Errorf("stderr = %q", stderr)
	}
}

func TestLosingTheLockEndsTheOperation(t *testing.T) {
	dir, _, _ := clusterRepos(t)
	env, _ := testEnv()
	host := &fakeHost{}
	op, opCtx, err := begin(t.Context(), env, dir, host, true)
	if err != nil {
		t.Fatal(err)
	}
	defer op.release()
	host.held.Close()
	select {
	case <-opCtx.Done():
		if !errors.Is(context.Cause(opCtx), errLockLost) {
			t.Errorf("cause = %v", context.Cause(opCtx))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the operation goes on without its lock")
	}
}

func TestAFailedRecordStillPushes(t *testing.T) {
	dir, other, _ := clusterRepos(t)
	env, _ := testEnv()
	host := &fakeHost{recordErr: errors.New("disk full")}
	op, opCtx, err := begin(t.Context(), env, dir, host, true)
	if err != nil {
		t.Fatal(err)
	}
	defer op.release()
	os.WriteFile(filepath.Join(dir, "node1.yml"), []byte("x\n"), 0o644)
	if err := op.finish(opCtx, "node-add node1", host); err == nil || !strings.Contains(err.Error(), "disk full") {
		t.Errorf("finish = %v", err)
	}
	git(t, other, "fetch", "--quiet")
	if got, want := git(t, other, "rev-parse", "origin/main"), git(t, dir, "rev-parse", "HEAD"); got != want {
		t.Error("the commit wasn't pushed after its record failed")
	}
}

func TestAnInterruptedOperationStillCommitsItsChange(t *testing.T) {
	dir, other, _ := clusterRepos(t)
	env, _ := testEnv()
	host := &fakeHost{}
	ctx, cancel := context.WithCancel(t.Context())
	op, opCtx, err := begin(ctx, env, dir, host, true)
	if err != nil {
		t.Fatal(err)
	}
	defer op.release()
	os.WriteFile(filepath.Join(dir, "node1.yml"), []byte("x\n"), 0o644)
	cancel()
	if err := op.finish(opCtx, "node-add node1", host); err != nil {
		t.Fatalf("finish = %v", err)
	}
	head := git(t, dir, "rev-parse", "HEAD")
	if len(host.recorded) != 1 || host.recorded[0] != head {
		t.Errorf("recorded %v, want %s", host.recorded, head)
	}
	git(t, other, "fetch", "--quiet")
	if git(t, other, "rev-parse", "origin/main") != head {
		t.Error("the interrupted operation's commit wasn't pushed")
	}
}

func TestAnAppliedCommitThatIsNoCommitRefuses(t *testing.T) {
	dir, _, _ := clusterRepos(t)
	env, _ := testEnv()
	for _, v := range []string{"--help", "HEAD", "main", "garbage", "0123"} {
		host := &fakeHost{applied: v}
		if _, _, err := begin(t.Context(), env, dir, host, true); err == nil {
			t.Errorf("applied commit %q was taken", v)
		} else if !host.held.closed {
			t.Errorf("applied commit %q left the lock held", v)
		}
	}
}

func TestTheFirstNodeAddChecksNoAppliedCommit(t *testing.T) {
	dir, _, _ := clusterRepos(t)
	env, _ := testEnv()
	host := &fakeHost{applied: "0123456789abcdef0123456789abcdef01234567"}
	op, _, err := begin(t.Context(), env, dir, host, false)
	if err != nil {
		t.Fatalf("a host's leftover applied commit refused the first node-add: %v", err)
	}
	op.release()
}

func TestAnOperationWithoutTheControlPlaneTakesNoLock(t *testing.T) {
	dir, _, _ := clusterRepos(t)
	env, _ := testEnv()
	op, opCtx, err := begin(t.Context(), env, dir, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	defer op.release()
	if opCtx.Err() != nil {
		t.Error("the operation ended at once")
	}
}

func TestSameControlPlaneComparesTheHost(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c")
	d, err := clusterdir.Init(path, clusterdir.Cluster{Ref: "c", K3s: "v1"})
	if err != nil {
		t.Fatal(err)
	}
	if err := sameControlPlane(d, clusterdir.Node{}); err != nil {
		t.Errorf("no control plane before or after: %v", err)
	}
	key := testHostKey(t)
	cp := clusterdir.Node{Name: "node1", Target: "h1", HostKey: key, ControlPlane: true}
	if err := d.WriteNode(cp); err != nil {
		t.Fatal(err)
	}
	if err := sameControlPlane(d, clusterdir.Node{}); err == nil {
		t.Error("a control plane added meanwhile passed")
	}
	if err := sameControlPlane(d, cp); err != nil {
		t.Errorf("the same control plane: %v", err)
	}
	moved := cp
	moved.Target = "h2"
	if err := sameControlPlane(d, moved); err == nil {
		t.Error("a control plane rebuilt on another host under the same name passed")
	}
	otherKey := cp
	otherKey.HostKey = testHostKey(t)
	if err := sameControlPlane(d, otherKey); err == nil {
		t.Error("a control plane with another host key passed")
	}
}
