package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/moostackhq/moostack/internal/clusterdir"
	"github.com/moostackhq/moostack/internal/gitrepo"
)

// operation is a change to a cluster, under its operation lock.
type operation struct {
	env    Env
	repo   gitrepo.Repo
	remote bool
	lock   held
	stop   context.CancelFunc
}

type held interface {
	Done() <-chan struct{}
	Close() error
}

// LockScript holds the operation lock, printing LockReady once it has it,
// until its stdin closes or no line has come on it for 45 seconds.
const LockScript = `mkdir -p /run/moostack
exec 9>>/run/moostack/operation.lock
flock -n 9 || {
	echo "another mooctl operation is running on this cluster; wait for it to finish" >&2
	exit 1
}
echo ` + LockReady + `
while timeout 45 sh -c 'IFS= read -r _' 9>&-; do :; done`

// LockReady is what LockScript prints once it holds the lock.
const LockReady = "operation-lock-held"

// AppliedCommitFile holds, on the control plane, the commit of the
// cluster directory the cluster last applied.
const AppliedCommitFile = "/var/lib/moostack/applied-commit"

// AppliedCommitScript prints the commit the cluster last applied, nothing
// if it has applied none, or "empty" if its file is empty, and fails if
// the file can't be read.
const AppliedCommitScript = `if [ -e ` + AppliedCommitFile + ` ]; then c=$(cat ` + AppliedCommitFile + `) || exit 1; echo "${c:-empty}"; fi`

// lockHost is the host that holds a cluster's operation lock and the
// commit it last applied: its control plane, or its first node.
type lockHost interface {
	lock(ctx context.Context) (held, error)
	appliedCommit(ctx context.Context) (string, error)
	recordCommit(ctx context.Context, commit string) error
}

type remoteHost struct{ r remote }

func (h remoteHost) lock(ctx context.Context) (held, error) { return h.r.Lock(ctx) }

func (h remoteHost) appliedCommit(ctx context.Context) (string, error) {
	out, err := run(ctx, h.r, AppliedCommitScript, "")
	if err != nil {
		return "", fmt.Errorf("reading the commit the cluster applied: %w", err)
	}
	return strings.TrimSpace(out), nil
}

func (h remoteHost) recordCommit(ctx context.Context, commit string) error {
	return upload(ctx, h.r, AppliedCommitFile, commit+"\n", 0o644)
}

// commitID matches a commit's ID: 40 or 64 hex digits.
var commitID = regexp.MustCompile(`^([0-9a-f]{40}|[0-9a-f]{64})$`)

// finishTimeout bounds finishing an operation once the cluster changed,
// which no interrupt cuts short.
const finishTimeout = 2 * time.Minute

// errLockLost ends an operation's context when its lock's connection
// drops.
var errLockLost = errors.New("the connection that holds the operation lock dropped")

// begin starts an operation on the cluster directory at dir, before
// anything changes: it takes the operation lock on lockOn, the control
// plane or the first node, brings the directory up to its remote, and,
// with checkApplied, checks that the cluster last applied a commit the
// directory has. Without lockOn, as when the control plane is gone, it
// takes no lock and checks nothing on the cluster. The operation runs in
// the returned context, which ends if the lock is lost. The caller
// reopens the directory, which may have changed.
func begin(ctx context.Context, env Env, dir string, lockOn lockHost, checkApplied bool) (*operation, context.Context, error) {
	repo, err := gitrepo.Open(ctx, dir)
	if err != nil {
		return nil, nil, err
	}
	if err := repo.CheckIdentity(ctx); err != nil {
		return nil, nil, err
	}
	remote, err := repo.HasRemote(ctx)
	if err != nil {
		return nil, nil, err
	}
	var lock held = noLock{}
	if lockOn != nil {
		if lock, err = lockOn.lock(ctx); err != nil {
			return nil, nil, fmt.Errorf("taking the operation lock: %w", err)
		}
	}
	opCtx, cancel := context.WithCancelCause(ctx)
	go func() {
		select {
		case <-lock.Done():
			cancel(errLockLost)
		case <-opCtx.Done():
		}
	}()
	o := &operation{env: env, repo: repo, remote: remote, lock: lock, stop: func() { cancel(context.Canceled) }}
	if remote {
		if err := repo.Update(opCtx); err != nil {
			o.release()
			if errors.Is(err, gitrepo.ErrDiverged) {
				return nil, nil, fmt.Errorf("%w; merge the remote's commits into %s with git pull --no-rebase, then run again", err, dir)
			}
			return nil, nil, err
		}
	}
	if lockOn == nil || !checkApplied {
		return o, opCtx, nil
	}
	applied, err := lockOn.appliedCommit(opCtx)
	if err != nil {
		o.release()
		return nil, nil, err
	}
	if applied == "" {
		return o, opCtx, nil
	}
	if !commitID.MatchString(applied) {
		o.release()
		return nil, nil, fmt.Errorf("the control plane's %s doesn't hold a commit: %q", AppliedCommitFile, applied)
	}
	ok, err := repo.IsAncestor(opCtx, applied)
	if err != nil {
		o.release()
		return nil, nil, err
	}
	if !ok {
		o.release()
		return nil, nil, fmt.Errorf("the cluster last applied commit %.12s, which %s doesn't have: another machine changed the cluster; push from there, then run again", applied, dir)
	}
	return o, opCtx, nil
}

// noLock is the lock of an operation that takes none.
type noLock struct{}

func (noLock) Done() <-chan struct{} { return nil }
func (noLock) Close() error          { return nil }

// finishing is the context an operation finishes in once the cluster
// changed: ctx's values, without its cancellation, for finishTimeout.
func finishing(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), finishTimeout)
}

// finish commits the cluster directory with message, records the commit
// on recordOn, the control plane, unless it is nil, and pushes it, in
// ctx's values but neither its cancellation nor its deadline, for
// finishTimeout at most. A failed push only warns.
func (o *operation) finish(ctx context.Context, message string, recordOn lockHost) error {
	ctx, cancel := finishing(ctx)
	defer cancel()
	commit, err := o.repo.Commit(ctx, message)
	if err != nil {
		return fmt.Errorf("the cluster changed, but committing it failed: %w; commit %s yourself", err, o.repo.Dir)
	}
	var recordErr error
	if recordOn != nil {
		if err := recordOn.recordCommit(ctx, commit); err != nil {
			recordErr = fmt.Errorf("the cluster changed, but recording commit %.12s on the control plane failed: %w", commit, err)
		}
	}
	if o.remote {
		if err := o.repo.Push(ctx); err != nil {
			fmt.Fprintf(o.env.Stderr, "Warning: pushing the cluster directory failed: %v\nPush %s before anyone else operates the cluster.\n", err, o.repo.Dir)
		}
	}
	return recordErr
}

// release releases the operation lock.
func (o *operation) release() {
	o.stop()
	o.lock.Close()
}

// sameControlPlane fails unless d, reopened once the operation began,
// still has the control plane want, by name, SSH target and host key, or
// none if want has no name.
func sameControlPlane(d *clusterdir.Dir, want clusterdir.Node) error {
	cp, ok, err := d.ControlPlane()
	if err != nil {
		return err
	}
	same := !ok && want.Name == "" ||
		ok && cp.Name == want.Name && cp.Target == want.Target && bytes.Equal(cp.HostKey.Marshal(), want.HostKey.Marshal())
	if !same {
		return errors.New("the cluster's control plane changed in the remote meanwhile; run again")
	}
	return nil
}
