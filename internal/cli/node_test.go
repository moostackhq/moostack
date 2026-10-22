package cli_test

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/moostackhq/moostack/internal/identity"
)

// unreachable is an SSH target that can't be reached, so node add fails
// once it tries SSH.
const unreachable = "node.invalid"

func TestNodeAddByANonRecipientListsTheRecipientsBeforeTryingTheHost(t *testing.T) {
	alice := newOperator(t, "alice")
	source := statusCluster(t, alice)
	bob := newOperator(t, "bob")
	bob.dir, bob.version = alice.dir, alice.version
	r := bob.run("--release-source", source, "node", "add", unreachable)
	if r.err == nil {
		t.Fatal("node add by someone who isn't a recipient succeeded")
	}
	wantContains(t, "the error", r.err.Error(), alice.recipient.Fingerprint(), "alice", "ask one of them to add you")
	if strings.Contains(r.err.Error(), unreachable) {
		t.Errorf("node add tried the host before reading the CA key: %v", r.err)
	}
}

func TestNodeAddByATeammateWhoIsARecipientGetsPastTheCAKey(t *testing.T) {
	alice := newOperator(t, "alice")
	source := statusCluster(t, alice)
	bob := newOperator(t, "bob")
	alice.mustRun("secret", "recipients", "add", bob.recipient.String())
	bob.dir, bob.version = alice.dir, alice.version
	r := bob.run("--release-source", source, "node", "add", unreachable)
	if r.err == nil || !strings.Contains(r.err.Error(), unreachable) {
		t.Errorf("node add by a recipient: %v, want it to fail reaching %s", r.err, unreachable)
	}
}

func TestNodeAddWithoutTheCAKeySaysSo(t *testing.T) {
	o := newOperator(t, "op")
	source := statusCluster(t, o)
	o.putSecret("other", "x")
	s := o.open()
	if err := s.Delete("cluster-ca-key"); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(); err != nil {
		t.Fatal(err)
	}
	r := o.run("--release-source", source, "node", "add", unreachable)
	if r.err == nil || !strings.Contains(r.err.Error(), "cluster-ca-key") {
		t.Errorf("node add without the CA key: %v, want an error naming cluster-ca-key", r.err)
	}
}

func TestNodeAddRefusesACAKeyThatIsntTheCAs(t *testing.T) {
	o := newOperator(t, "op")
	source := statusCluster(t, o)
	other, err := identity.NewCA("other")
	if err != nil {
		t.Fatal(err)
	}
	o.putSecret("cluster-ca-key", string(other.KeyPEM()))
	r := o.run("--release-source", source, "node", "add", unreachable)
	if r.err == nil || !strings.Contains(r.err.Error(), "isn't the CA certificate's key") {
		t.Errorf("node add with another CA's key: %v, want an error saying the key isn't the CA's", r.err)
	}
}

// addRefused runs node add with args, which must fail with an error
// containing each of want, before running the preflight or reaching any
// host, and without changing the cluster directory.
func addRefused(t *testing.T, o *operator, source string, args []string, want ...string) {
	t.Helper()
	before := snapshot(t, o.dir)
	r := o.run(append([]string{"--release-source", source, "node", "add"}, args...)...)
	if r.err == nil {
		t.Fatalf("node add %s succeeded", strings.Join(args, " "))
	}
	wantContains(t, "node add's error", r.err.Error(), want...)
	if strings.Contains(r.stdout, "host key") {
		t.Errorf("node add %s reached a host:\n%s", strings.Join(args, " "), r.stdout)
	}
	wantSnapshot(t, o.dir, before)
	for name, n := range o.nodes {
		if n.dialed() > 0 {
			t.Errorf("node add %s ran the preflight on %s", strings.Join(args, " "), name)
		}
	}
	wantReminder(t, "the refused node add", r, false)
}

func TestNodeAddRefusesTheFlagsWithSeveralTargets(t *testing.T) {
	o := newOperator(t, "op")
	source := preflightCluster(t, o)
	addRefused(t, o, source, []string{"web1", "web2", "--name", "edge1"}, "--name goes with a single target", "<ssh target>,name=<name>")
	addRefused(t, o, source, []string{"web1", "web2", "--private-address", "10.0.0.5"}, "--private-address goes with a single target", "<ssh target>,private-address=<address>")
}

func TestNodeAddRefusesAnOptionGivenAsAFlagAndInTheTarget(t *testing.T) {
	o := newOperator(t, "op")
	source := preflightCluster(t, o)
	addRefused(t, o, source, []string{"web1,name=edge1", "--name", "edge2"}, "name is given both by --name and in the target web1,name=edge1")
	addRefused(t, o, source, []string{"web1,private-address=10.0.0.5", "--private-address", "10.0.0.5"}, "private-address is given both")
}

func TestNodeAddRefusesBadTargets(t *testing.T) {
	o := newOperator(t, "op")
	source := preflightCluster(t, o)
	addRefused(t, o, source, nil, "one or more SSH targets")
	addRefused(t, o, source, []string{"web1", "web1,name=edge1"}, "web1 and web1 are the same target")
	addRefused(t, o, source, []string{"web1,zone=a"}, `target "web1,zone=a": unknown option zone`)
	addRefused(t, o, source, []string{"web1", "--name", "Edge_1"}, "--name: node name")
	// A node of the cluster already, as its node file records it.
	addRefused(t, o, source, []string{"web1", "root@node2.invalid"}, "root@node2.invalid is node node2 of the cluster already", "mooctl node remove node2")
}

func TestNodeAddRefusesANameTakenOrAskedForTwice(t *testing.T) {
	o := newOperator(t, "op")
	source := preflightCluster(t, o)
	addRefused(t, o, source, []string{"web1,name=edge1", "web2", "web3,name=edge1"}, "web1 and web3 both ask for the name edge1")
	addRefused(t, o, source, []string{"web1", "web2,name=node3"}, "web2 asks for the name node3, which node node3 of the cluster has already")
	addRefused(t, o, source, []string{"web1", "--name", "node3"}, "web1 asks for the name node3")
}

func TestNodeAddStopsAtTheFirstTargetThatFailsTheCheck(t *testing.T) {
	o := newOperator(t, "op")
	source := preflightCluster(t, o)
	before := snapshot(t, o.dir)
	r := o.run("--release-source", source, "node", "add", unreachable+",name=edge1", "other.invalid")
	if r.err == nil || !strings.Contains(r.err.Error(), unreachable) {
		t.Fatalf("node add of two unreachable hosts: %v, want it to fail reaching %s", r.err, unreachable)
	}
	if strings.Contains(r.err.Error()+r.stdout, "other.invalid") {
		t.Errorf("node add tried other.invalid after %s failed:\n%v\n%s", unreachable, r.err, r.stdout)
	}
	wantSnapshot(t, o.dir, before)
	wantReminder(t, "the failed node add", r, false)
}

func TestNodeAddRefusesTheSameTargetUnderTheUserItResolvesTo(t *testing.T) {
	o := newOperator(t, "op")
	source := preflightCluster(t, o)
	writeFile(t, filepath.Join(o.home, ".ssh", "config"), []byte("Host web1\n  User bob\n"))
	addRefused(t, o, source, []string{"web1", "bob@web1"}, "web1 and bob@web1 are the same target, bob@web1")
	// The user resolved is the one a node file records.
	addRefused(t, o, source, []string{"root@node1.invalid,name=edge"}, "root@node1.invalid is node node1 of the cluster already")
	writeFile(t, filepath.Join(o.home, ".ssh", "config"), []byte("Host node1.invalid\n  User root\n"))
	addRefused(t, o, source, []string{"node1.invalid"}, "node1.invalid is node node1 of the cluster already")
}

func TestNodeAddRefusesWhatTheDirectoryRefusesBeforeTheRelease(t *testing.T) {
	o := newOperator(t, "op")
	preflightCluster(t, o)
	// A release source that is down.
	s := serveSource(t, t.TempDir())
	o.client = s.Client()
	s.Close()
	addRefused(t, o, s.URL, []string{"root@node2.invalid"}, "root@node2.invalid is node node2 of the cluster already")
	addRefused(t, o, s.URL, []string{"web1,name=node3"}, "web1 asks for the name node3")
	addRefused(t, o, s.URL, []string{"web1", "web1"}, "are the same target")
}

func TestNodeAddTakesEmptyFlagsAsAbsentWithSeveralTargets(t *testing.T) {
	o := newOperator(t, "op")
	source := preflightCluster(t, o)
	r := o.run("--release-source", source, "node", "add", unreachable, "other.invalid", "--name", "", "--private-address", "")
	if r.err == nil || !strings.Contains(r.err.Error(), unreachable) {
		t.Errorf("node add of two targets with empty flags: %v, want it to go on to %s", r.err, unreachable)
	}
}

func TestNodeAddTakesANameFlagWithAnAddressInTheTarget(t *testing.T) {
	o := newOperator(t, "op")
	source := preflightCluster(t, o)
	r := o.run("--release-source", source, "node", "add", unreachable+",private-address=10.0.0.5", "--name", "edge")
	if r.err == nil || !strings.Contains(r.err.Error(), "fetching the host key of "+unreachable) {
		t.Errorf("node add with --name and an address in the target: %v, want it to go on to %s", r.err, unreachable)
	}
}

func TestNodeAddSaysNothingWasInstalledWhenALaterHostFailsTheCheck(t *testing.T) {
	o := newOperator(t, "op")
	source := preflightCluster(t, o)
	r := o.run("--release-source", source, "node", "add", unreachable, "other.invalid")
	if r.err == nil || !strings.HasSuffix(r.err.Error(), "\nNothing was installed.") {
		t.Errorf("node add of two unreachable hosts: %v, want it to say nothing was installed", r.err)
	}
	r = o.run("--release-source", source, "node", "add", unreachable)
	if r.err == nil || strings.Contains(r.err.Error(), "Nothing was installed") {
		t.Errorf("node add of one unreachable host: %v, want its error alone", r.err)
	}
}

func TestCtrlCStopsNodeAddBeforeItsInstallsAndGivesBackTheLock(t *testing.T) {
	interrupt := func(ctx context.Context) {
		syscall.Kill(os.Getpid(), syscall.SIGINT)
		<-ctx.Done()
	}
	for name, c := range map[string]struct {
		set  func(o *operator)
		want string
	}{
		"the preflight": {func(o *operator) { o.nodes["node2"].dialing = interrupt }, "node add interrupted"},
		// The host accepts the connection and never answers.
		"reaching a host": {func(o *operator) {
			l, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { l.Close() })
			go func() {
				for {
					c, err := l.Accept()
					if err != nil {
						return
					}
					t.Cleanup(func() { c.Close() })
					syscall.Kill(os.Getpid(), syscall.SIGINT)
				}
			}()
			_, port, _ := net.SplitHostPort(l.Addr().String())
			writeFile(t, filepath.Join(o.home, ".ssh", "config"), []byte("Host hang\n  HostName 127.0.0.1\n  Port "+port+"\n"))
		}, "reaching hang: interrupted"},
	} {
		t.Run(name, func(t *testing.T) {
			o := newOperator(t, "op")
			source := preflightCluster(t, o)
			c.set(o)
			before := snapshot(t, o.dir)
			r := o.run("--release-source", source, "node", "add", "hang", unreachable)
			if r.err == nil {
				t.Fatal("node add succeeded")
			}
			wantContains(t, "node add's error", r.err.Error(), c.want)
			if got := o.exitCodes(); len(got) > 0 {
				t.Errorf("mooctl exited with %v, want it to return", got)
			}
			for name, n := range o.nodes {
				if l := n.heldLock(); l != nil {
					t.Errorf("after the interrupted node add, %s holds the lock %+v", name, l)
				}
			}
			wantSnapshot(t, o.dir, before)
			wantReminder(t, "the interrupted node add", r, false)
		})
	}
}

func TestNodeAddTakesANodesHostUnderAnotherUserAsAnotherTarget(t *testing.T) {
	o := newOperator(t, "op")
	source := preflightCluster(t, o)
	// node1's node file records root on node1.invalid.
	r := o.run("--release-source", source, "node", "add", "bob@node1.invalid")
	if r.err == nil || strings.Contains(r.err.Error(), "is node node1") || !strings.Contains(r.err.Error(), "fetching the host key of bob@node1.invalid") {
		t.Errorf("node add of node1's host as bob: %v, want it to go on to the host", r.err)
	}
}

func TestNodeAddTakesHostsThatDifferInCaseAsTheSame(t *testing.T) {
	o := newOperator(t, "op")
	source := preflightCluster(t, o)
	addRefused(t, o, source, []string{"root@web1", "root@WEB1"}, "root@web1 and root@WEB1 are the same target")
	addRefused(t, o, source, []string{"root@NODE2.invalid"}, "root@NODE2.invalid is node node2 of the cluster already")
}
