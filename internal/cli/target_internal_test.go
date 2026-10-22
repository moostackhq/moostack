package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/netip"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/urfave/cli/v3"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/cluster"
	"github.com/moostackhq/moostack/internal/identity"
	"github.com/moostackhq/moostack/internal/nodeclient"
	"github.com/moostackhq/moostack/internal/orchestrate"
)

func TestParseNodeTargetSplitsTheOptionsFromTheTarget(t *testing.T) {
	for arg, want := range map[string]nodeTarget{
		"web1":                          {target: "web1"},
		"op@web1":                       {target: "op@web1"},
		"op@web1,name=edge1":            {target: "op@web1", name: "edge1"},
		"web1,private-address=10.0.0.5": {target: "web1", private: netip.MustParseAddr("10.0.0.5")},
		"op@web1,private-address=fd00::5,name=edge1": {
			target: "op@web1", name: "edge1", private: netip.MustParseAddr("fd00::5"),
		},
		// The target ends at the first comma after its last @, so a user
		// may have commas.
		"a,b@web1,name=edge1": {target: "a,b@web1", name: "edge1"},
		"a,b@web1":            {target: "a,b@web1"},
		// A user may have an =.
		"a=b@web1": {target: "a=b@web1"},
		// A host that is an IPv6 address.
		"op@fd00::1,name=edge1": {target: "op@fd00::1", name: "edge1"},
	} {
		want.arg = arg
		got, err := parseNodeTarget(arg)
		if err != nil || got != want {
			t.Errorf("parseNodeTarget(%q) = %+v, %v, want %+v", arg, got, err, want)
		}
	}
}

func TestParseNodeTargetRefusesBadOptions(t *testing.T) {
	for arg, want := range map[string]string{
		"web1,":                      "an empty option",
		"web1,,name=edge1":           "an empty option",
		"web1,name":                  "name has no value",
		"web1,name=":                 "name has no value",
		"web1,private-address=":      "private-address has no value",
		"web1,zone=a":                "unknown option zone",
		"web1,Name=edge1":            "unknown option Name",
		"web1,name=edge1,name=edge2": "name is given twice",
		"web1,private-address=10.0.0.5,private-address=10.0.0.6": "private-address is given twice",
		"web1,name=Edge_1":                 "node name",
		"web1,private-address=web2":        "private-address",
		"web1,private-address=10.0.0.5:22": "private-address",
		",name=edge1":                      "no SSH target",
		// An @ in an option's value would make the target end after it.
		"web1,name=a@b":                   "an option's value holds an @",
		"web1,private-address=a@10.0.0.5": "an option's value holds an @",
	} {
		_, err := parseNodeTarget(arg)
		if err == nil {
			t.Errorf("parseNodeTarget(%q) succeeded, want it refused", arg)
			continue
		}
		if !strings.Contains(err.Error(), want) || !strings.Contains(err.Error(), arg) {
			t.Errorf("parseNodeTarget(%q): %v, want an error naming %q and saying %q", arg, err, arg, want)
		}
	}
}

func TestCheckTargetsGivesTheFreeNamesSkippingTheOnesAsked(t *testing.T) {
	ca, err := identity.NewCA("prod")
	if err != nil {
		t.Fatal(err)
	}
	d, err := cluster.Init(filepath.Join(t.TempDir(), "prod"), cluster.Cluster{Name: "prod", Release: "1.0.0", Generation: 1}, ca.CertPEM())
	if err != nil {
		t.Fatal(err)
	}
	var targets []nodeTarget
	for _, arg := range []string{"web1", "web2,name=node1", "web3", "web4,name=node3"} {
		target, err := parseNodeTarget(arg)
		if err != nil {
			t.Fatal(err)
		}
		targets = append(targets, target)
	}
	if err := checkTargets(Env{Home: t.TempDir()}, d, targets); err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, target := range targets {
		names = append(names, target.name)
	}
	if want := []string{"node2", "node1", "node4", "node3"}; !slices.Equal(names, want) {
		t.Errorf("checkTargets named the targets %v, want %v", names, want)
	}
}

// hosts are reached hosts of the targets, each with the node name given.
func hosts(t *testing.T, targets ...string) []*newHost {
	var hosts []*newHost
	for _, arg := range targets {
		target, err := parseNodeTarget(arg)
		if err != nil {
			t.Fatal(err)
		}
		hosts = append(hosts, &newHost{arg: target.arg, target: target.target, n: cluster.Node{Name: target.name}})
	}
	return hosts
}

func TestAddedSoFarNamesTheHostsAddedAndNotTried(t *testing.T) {
	hs := hosts(t, "web1,name=node1", "web2,name=node2", "web3,name=edge3,private-address=10.0.0.5", "web4,name=node4")
	failed := errors.New("it failed")
	const again = "mooctl -C /prod node add"
	for name, c := range map[string]struct {
		err   error
		at    int
		tried bool
		want  string
	}{
		"the first install fails":        {failed, 0, true, "it failed\nNot tried: 'web2,name=node2' 'web3,name=edge3,private-address=10.0.0.5' 'web4,name=node4'\nTo add the hosts not added, run: mooctl -C /prod node add 'web1,name=node1' 'web2,name=node2' 'web3,name=edge3,private-address=10.0.0.5' 'web4,name=node4'"},
		"a later install fails":          {failed, 1, true, "it failed\nAdded before it stopped: node1 (web1).\nNot tried: 'web3,name=edge3,private-address=10.0.0.5' 'web4,name=node4'\nTo add the hosts not added, run: mooctl -C /prod node add 'web2,name=node2' 'web3,name=edge3,private-address=10.0.0.5' 'web4,name=node4'"},
		"stopped before a host":          {failed, 2, false, "it failed\nAdded before it stopped: node1 (web1), node2 (web2).\nNot tried: 'web3,name=edge3,private-address=10.0.0.5' 'web4,name=node4'\nTo add the hosts not added, run: mooctl -C /prod node add 'web3,name=edge3,private-address=10.0.0.5' 'web4,name=node4'"},
		"recording fails":                {&addedError{failed}, 1, true, "it failed\nAdded before it stopped: node1 (web1), node2 (web2).\nNot tried: 'web3,name=edge3,private-address=10.0.0.5' 'web4,name=node4'\nTo add the hosts not added, run: mooctl -C /prod node add 'web3,name=edge3,private-address=10.0.0.5' 'web4,name=node4'"},
		"the last fails":                 {failed, 3, true, "it failed\nAdded before it stopped: node1 (web1), node2 (web2), edge3 (web3).\nTo add the hosts not added, run: mooctl -C /prod node add 'web4,name=node4'"},
		"the last is recorded but fails": {&addedError{failed}, 3, true, "it failed\nAdded before it stopped: node1 (web1), node2 (web2), edge3 (web3), node4 (web4)."},
	} {
		if got := addedSoFar(c.err, again, hs, c.at, c.tried).Error(); got != c.want {
			t.Errorf("%s: addedSoFar says\n%s\nwant\n%s", name, got, c.want)
		}
	}
	// A single target's error is as it was.
	if got := addedSoFar(failed, again, hs[:1], 0, true); got != failed {
		t.Errorf("addedSoFar of a single target is %v, want the error alone", got)
	}
}

func TestTheHostsNotAddedReadBackInAShell(t *testing.T) {
	args := []string{`CORP\jdoe@web2`, "o brien@web3,name=edge3", "svc$x@web4", "it's@web5", "web6,private-address=fd00::6"}
	hs := hosts(t, append([]string{"web1"}, args...)...)
	msg := addedSoFar(errors.New("it failed"), "mooctl node add", hs, 1, false).Error()
	for _, line := range []string{"Not tried: ", "To add the hosts not added, run: mooctl node add "} {
		_, rest, _ := strings.Cut(msg, "\n"+line)
		rest, _, _ = strings.Cut(rest, "\n")
		for _, shell := range []string{"sh", "zsh"} {
			if _, err := exec.LookPath(shell); err != nil {
				continue
			}
			out, err := exec.Command(shell, "-c", "printf '%s\\n' "+rest).CombinedOutput()
			if err != nil {
				t.Fatalf("%s reading %q: %v\n%s", shell, rest, err, out)
			}
			if got := strings.Split(strings.TrimSuffix(string(out), "\n"), "\n"); !slices.Equal(got, args) {
				t.Errorf("%s reads %q back as %q, want %q", shell, rest, got, args)
			}
		}
	}
}

func TestAddEachStopsBeforeAHostOnceNodeAddIsStopped(t *testing.T) {
	failed := errors.New("it failed")
	for name, c := range map[string]struct {
		// stop ends ctx once the first host is added.
		stop    func(cancel context.CancelCauseFunc)
		want    []string
		addedTo int
	}{
		"all added": {nil, nil, 3},
		"interrupted": {func(cancel context.CancelCauseFunc) { cancel(context.Canceled) },
			[]string{"interrupted before adding web2", "Added before it stopped: node1 (web1).", "Not tried: 'web2,name=node2' 'web3,name=node3'"}, 1},
		"the lock lost": {func(cancel context.CancelCauseFunc) {
			cancel(fmt.Errorf("node1 didn't answer: %w", orchestrate.ErrLockLost))
		},
			[]string{"before adding web2: ", "node1 didn't answer", "Not tried: 'web2,name=node2' 'web3,name=node3'"}, 1},
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			var out bytes.Buffer
			var added []string
			hs := hosts(t, "web1,name=node1", "web2,name=node2", "web3,name=node3")
			err := addEach(ctx, Env{Stdout: &out}, "mooctl node add", hs, func(h *newHost, later bool) error {
				if later != (len(added) > 0) {
					t.Errorf("adding %s, later is %t", h.target, later)
				}
				added = append(added, h.target)
				if c.stop != nil {
					c.stop(cancel)
				}
				return nil
			})
			if len(added) != c.addedTo {
				t.Errorf("addEach added %v, want the first %d", added, c.addedTo)
			}
			if c.want == nil && err != nil {
				t.Errorf("addEach: %v", err)
			}
			for _, want := range c.want {
				if err == nil || !strings.Contains(err.Error(), want) {
					t.Errorf("addEach: %v, want it to say %q", err, want)
				}
			}
			if n := strings.Count(out.String(), "commit it"); n != 1 {
				t.Errorf("addEach reminded to commit %d times, want once:\n%s", n, out.String())
			}
		})
	}
	// A first host that fails changes nothing, so there is nothing to
	// commit; one recorded but failing after, there is.
	for err, remind := range map[error]bool{failed: false, &addedError{failed}: true} {
		var out bytes.Buffer
		addEach(context.Background(), Env{Stdout: &out}, "mooctl node add", hosts(t, "web1", "web2"), func(*newHost, bool) error { return err })
		if got := strings.Contains(out.String(), "commit it"); got != remind {
			t.Errorf("with the first host failing with %T, addEach reminded to commit: %t, want %t", err, got, remind)
		}
	}
}

func TestStoppedSaysWhatStoppedNodeAdd(t *testing.T) {
	failed := errors.New("it failed")
	if got := stopped(context.Background(), "adding web2 as node2", failed).Error(); got != "adding web2 as node2: it failed" {
		t.Errorf("stopped of a failure: %q", got)
	}
	if err := stoppedBefore(context.Background(), "adding web2"); err != nil {
		t.Errorf("stoppedBefore of a running node add: %v", err)
	}
	interrupted, cancel := context.WithCancel(context.Background())
	cancel()
	if got := stopped(interrupted, "adding web2 as node2", failed).Error(); got != "adding web2 as node2: interrupted: it failed" {
		t.Errorf("stopped of an interrupt: %q", got)
	}
	if got := stoppedBefore(interrupted, "adding web2").Error(); got != "interrupted before adding web2" {
		t.Errorf("stoppedBefore of an interrupt: %q", got)
	}
	lost, cancelLost := context.WithCancelCause(context.Background())
	cancelLost(fmt.Errorf("node1 didn't answer: %w", orchestrate.ErrLockLost))
	for _, err := range []error{stopped(lost, "adding web2 as node2", failed), stoppedBefore(lost, "adding web2")} {
		if !errors.Is(err, orchestrate.ErrLockLost) || !strings.Contains(err.Error(), "node1 didn't answer") || strings.Contains(err.Error(), "interrupted") {
			t.Errorf("with the lock lost: %v, want the lost lock and its cause", err)
		}
	}
}

// failingWriter fails every write.
type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("no space left on device") }

func TestPrintStatusFailsWhenItCantWrite(t *testing.T) {
	err := printStatus(Env{Stdout: failingWriter{}}, statusView{Cluster: "prod", Nodes: []nodeStatusView{{Name: "node1"}}})
	if err == nil || !strings.Contains(err.Error(), "no space left on device") {
		t.Errorf("printStatus to a writer that fails: %v, want its error", err)
	}
}

// Any target parseNodeTarget takes reads back the same once its options
// are written again, its target ends with no comma after its last @, and
// nothing makes it panic.
func FuzzParseNodeTarget(f *testing.F) {
	for _, arg := range []string{
		"web1", "op@web1,name=edge1", "a,b@web1,private-address=fd00::5,name=edge1",
		"web1,name=a@b", "web1,,", "@,name=x", "web1,private-address=10.0.0.5%eth0",
	} {
		f.Add(arg)
	}
	f.Fuzz(func(t *testing.T, arg string) {
		got, err := parseNodeTarget(arg)
		if err != nil {
			return
		}
		if got.arg != arg || got.target == "" {
			t.Fatalf("%q parsed as %+v", arg, got)
		}
		if i := strings.LastIndex(got.target, "@"); strings.Contains(got.target[i+1:], ",") {
			t.Fatalf("%q parsed as the target %q, with a comma after its last @", arg, got.target)
		}
		again := got.target
		if got.name != "" {
			again += ",name=" + got.name
		}
		if got.private.IsValid() {
			again += ",private-address=" + got.private.String()
		}
		back, err := parseNodeTarget(again)
		if err != nil || back.target != got.target || back.name != got.name || back.private != got.private {
			t.Errorf("%q parsed as %+v, which written again as %q reads back as %+v, %v", arg, got, again, back, err)
		}
	})
}

func TestRerunNodeAddRunsItAsItRan(t *testing.T) {
	for _, c := range []struct {
		args []string
		want string
	}{
		{[]string{"node", "add", "web1", "web2"}, "mooctl node add"},
		{[]string{"-C", "/srv/prod cluster", "--release-source", "https://releases.example.com", "--ssh-key", "/keys/ops key", "node", "add", "--force", "web1", "web2"},
			"mooctl -C '/srv/prod cluster' --release-source https://releases.example.com --ssh-key '/keys/ops key' node add --force"},
		// A release source with a password is left out.
		{[]string{"--release-source", "https://op:secret@releases.example.com", "node", "add", "web1"}, "mooctl node add"},
	} {
		root := New(Env{})
		var got string
		for _, cmd := range root.Commands {
			if cmd.Name != "node" {
				continue
			}
			for _, sub := range cmd.Commands {
				if sub.Name == "add" {
					sub.Action = func(_ context.Context, cmd *cli.Command) error {
						got = rerunNodeAdd(cmd)
						return nil
					}
				}
			}
		}
		if err := root.Run(context.Background(), append([]string{"mooctl"}, c.args...)); err != nil {
			t.Fatal(err)
		}
		if got != c.want {
			t.Errorf("mooctl %q: the command to run it again is %q, want %q", c.args, got, c.want)
		}
	}
}

// writtenTo is a writer that closes written once it is first written
// to, and keeps what is written.
type writtenTo struct {
	mu      sync.Mutex
	buf     bytes.Buffer
	written chan struct{}
}

func (w *writtenTo) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.buf.Len() == 0 {
		close(w.written)
	}
	return w.buf.Write(p)
}

func (w *writtenTo) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

// lockNode is a node that grants the operation lock, and gives it to
// another operation once stolen.
type lockNode struct {
	orchestrate.Node
	mu     sync.Mutex
	lease  string
	stolen bool
}

func (n *lockNode) Status(context.Context) nodeclient.Status {
	return nodeclient.Status{Mode: nodev1.Mode_MODE_DIRECT, Generation: 1}
}

func (n *lockNode) AcquireLock(ctx context.Context, l nodeclient.Lock, _ time.Duration) (nodeclient.Lock, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.lease = l.Lease
	return l, ctx.Err()
}

func (n *lockNode) RenewLock(ctx context.Context, lease string, _ time.Duration) (*nodeclient.Lock, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stolen {
		return &nodeclient.Lock{Lease: "bobs", Holder: "bob"}, ctx.Err()
	}
	return &nodeclient.Lock{Lease: lease}, ctx.Err()
}

func (n *lockNode) ReleaseLock(context.Context, string) error { return nil }

func (n *lockNode) RemoveIdentities(ctx context.Context, _ []string, _ string) error {
	return ctx.Err()
}

func (n *lockNode) Close() error { return nil }

func (n *lockNode) steal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.stolen = true
}

// tellWithin is tellNodes, which must return within 10 s.
func tellWithin(t *testing.T, ctx context.Context, env Env, cl *orchestrate.Cluster, change string, tell func(context.Context) error) (bool, error) {
	t.Helper()
	type told struct {
		interrupted bool
		err         error
	}
	done := make(chan told, 1)
	go func() {
		interrupted, err := tellNodes(ctx, env, cl, change, tell)
		done <- told{interrupted, err}
	}()
	select {
	case r := <-done:
		return r.interrupted, r.err
	case <-time.After(10 * time.Second):
		t.Fatal("tellNodes didn't return within 10 s")
		return false, nil
	}
}

// lockedCluster is an operation that holds the lock on n, whose lease
// runs out within 300 ms of its last renewal, and whose context is ctx's.
func lockedCluster(t *testing.T, ctx context.Context, n *lockNode) *orchestrate.Cluster {
	t.Helper()
	cl, err := orchestrate.Preflight(ctx, orchestrate.Nodes{
		Names:    []string{"node1"},
		Dial:     func(context.Context, string) (orchestrate.Node, error) { return n, nil },
		LeaseTTL: 300 * time.Millisecond,
	}, 1, false, orchestrate.Holder{Who: "alice", Machine: "laptop"})
	if err != nil {
		t.Fatal(err)
	}
	return cl
}

func TestTellNodesTellsThemWhateverEndsTheOperationButTheLostLock(t *testing.T) {
	const interrupted = "Interrupted, but node3 is removed"
	t.Run("nothing ends it", func(t *testing.T) {
		var stderr bytes.Buffer
		cl := lockedCluster(t, context.Background(), &lockNode{})
		signalled, err := tellWithin(t, cl.Context(), Env{Stderr: &stderr}, cl, "node3 is removed", func(ctx context.Context) error { return ctx.Err() })
		cl.Close()
		if signalled || err != nil {
			t.Errorf("tellNodes: %t, %v, want it to tell the nodes, uninterrupted", signalled, err)
		}
		// Closing the operation, after, is no signal, however long one
		// waits for it to answer.
		time.Sleep(100 * time.Millisecond)
		if strings.Contains(stderr.String(), interrupted) {
			t.Errorf("an operation nothing interrupted said it was:\n%s", stderr.String())
		}
	})
	t.Run("a signal", func(t *testing.T) {
		stderr := &writtenTo{written: make(chan struct{})}
		ctx, signal := context.WithCancel(context.Background())
		cl := lockedCluster(t, ctx, &lockNode{})
		defer cl.Close()
		signal()
		signalled, err := tellWithin(t, cl.Context(), Env{Stderr: stderr}, cl, "node3 is removed", func(ctx context.Context) error {
			// The signal is answered at once, while the nodes are told.
			select {
			case <-stderr.written:
			case <-time.After(5 * time.Second):
				t.Error("the signal wasn't answered while the nodes were told")
			}
			// Told after the lease would have run out, had the signal
			// ended its renewals.
			time.Sleep(time.Second)
			return ctx.Err()
		})
		if !signalled || err != nil {
			t.Errorf("tellNodes after a signal: %t, %v, want it to tell the nodes, and say a signal came", signalled, err)
		}
		if !strings.Contains(stderr.String(), interrupted+": mooctl tells the nodes first.") {
			t.Errorf("tellNodes after a signal printed\n%s\nwant it to say it tells the nodes first", stderr.String())
		}
	})
	t.Run("the lost lock", func(t *testing.T) {
		var stderr bytes.Buffer
		n := &lockNode{}
		cl := lockedCluster(t, context.Background(), n)
		defer cl.Close()
		signalled, err := tellWithin(t, cl.Context(), Env{Stderr: &stderr}, cl, "node3 is removed", func(ctx context.Context) error {
			n.steal()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(5 * time.Second):
				return errors.New("the operation didn't lose the lock")
			}
		})
		if signalled || !errors.Is(err, orchestrate.ErrLockLost) || !strings.Contains(err.Error(), "telling the nodes: ") || strings.Contains(err.Error(), "interrupted") {
			t.Errorf("tellNodes with the lock lost: %t, %v, want the lost lock, as no signal", signalled, err)
		}
		if strings.Contains(stderr.String(), interrupted) {
			t.Errorf("an operation that lost its lock said it was interrupted:\n%s", stderr.String())
		}
	})
}
