package cli_test

import (
	"maps"
	"strings"
	"testing"

	controlplanev1 "github.com/moostackhq/moostack/api/controlplane/v1"
)

// joinToken is a join token of a control plane that can't be reached.
var joinToken = func() string {
	token, err := controlplanev1.EncodeToken(&controlplanev1.JoinToken{
		Address:       "cp.invalid:7444",
		CaFingerprint: "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		Secret:        "s3cret",
	})
	if err != nil {
		panic(err)
	}
	return token
}()

// joinRefused runs node add with args, which must fail with an error
// containing each of want, before reaching the host, the cluster's nodes
// or the CA key, and without changing the cluster directory.
func joinRefused(t *testing.T, o *operator, source string, args []string, want ...string) {
	t.Helper()
	before := snapshot(t, o.dir)
	r := o.run(append([]string{"--release-source", source, "node", "add", unreachable}, args...)...)
	if r.err == nil {
		t.Fatalf("node add %s succeeded", strings.Join(args, " "))
	}
	wantContains(t, "node add's error", r.err.Error(), want...)
	if strings.Contains(r.err.Error(), unreachable) {
		t.Errorf("node add %s reached the host: %v", strings.Join(args, " "), r.err)
	}
	if after := snapshot(t, o.dir); !maps.Equal(before, after) {
		t.Errorf("node add %s changed the cluster directory", strings.Join(args, " "))
	}
	for name, n := range o.nodes {
		if n.dialed() > 0 {
			t.Errorf("node add %s reached %s", strings.Join(args, " "), name)
		}
	}
	wantReminder(t, "the refused node add", r, false)
}

func TestNodeAddRefusesABadJoinTokenBeforeTryingTheHost(t *testing.T) {
	o := newOperator(t, "op")
	source := preflightCluster(t, o)
	joinRefused(t, o, source, []string{"--join-token", "nonsense"}, "--join-token", "moostack-join:")
	joinRefused(t, o, source, []string{"--join-token", ""}, "--join-token", "moostack-join:")
	joinRefused(t, o, source, []string{"--join-token", strings.TrimSuffix(joinToken, joinToken[len(joinToken)-8:])}, "--join-token")
}

func TestNodeAddWithAJoinTokenRefusesANameAndForce(t *testing.T) {
	o := newOperator(t, "op")
	source := preflightCluster(t, o)
	joinRefused(t, o, source, []string{"--join-token", joinToken, "--name", "web"}, "--name", "control plane names")
	joinRefused(t, o, source, []string{"--join-token", joinToken, "--force"}, "--force")
}

func TestNodeAddWithAJoinTokenRefusesANameInTheTarget(t *testing.T) {
	o := newOperator(t, "op")
	source := preflightCluster(t, o)
	r := o.run("--release-source", source, "node", "add", unreachable+",name=web", "--join-token", joinToken)
	if r.err == nil {
		t.Fatal("node add --join-token with a name in its target succeeded")
	}
	wantContains(t, "node add's error", r.err.Error(), "name=", "control plane names")
	if strings.Contains(r.stdout, "host key") {
		t.Errorf("node add --join-token with a name in its target reached the host:\n%s", r.stdout)
	}
}

func TestNodeAddWithAJoinTokenTakesOneTarget(t *testing.T) {
	o := newOperator(t, "op")
	source := preflightCluster(t, o)
	joinRefused(t, o, source, []string{"other.invalid", "--join-token", joinToken}, "1 argument, not 2")
}

func TestNodeAddWithAJoinTokenNeedsNeitherTheCAKeyNorTheClustersNodes(t *testing.T) {
	o := newOperator(t, "op")
	source := preflightCluster(t, o)
	o.putSecret("other", "x")
	s := o.open()
	if err := s.Delete("cluster-ca-key"); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(); err != nil {
		t.Fatal(err)
	}
	before := snapshot(t, o.dir)
	r := o.run("--release-source", source, "node", "add", unreachable, "--join-token", joinToken)
	if r.err == nil || !strings.Contains(r.err.Error(), unreachable) {
		t.Fatalf("node add --join-token: %v, want it to fail reaching %s", r.err, unreachable)
	}
	for name, n := range o.nodes {
		if n.dialed() > 0 {
			t.Errorf("node add --join-token reached %s", name)
		}
	}
	if after := snapshot(t, o.dir); !maps.Equal(before, after) {
		t.Error("node add --join-token changed the cluster directory")
	}
	wantReminder(t, "node add --join-token", r, false)
}

func TestNodeAddWithAJoinTokenRefusesAReleaseThisMooctlDoesntOperate(t *testing.T) {
	o := newOperator(t, "op")
	source := preflightCluster(t, o)
	o.version = "9.9.9"
	joinRefused(t, o, source, []string{"--join-token", joinToken}, "doesn't operate release")
}

// A --private-address that isn't an IP address is refused before the
// host is tried, with or without a join token, and an empty one is taken
// as absent.
func TestNodeAddRefusesAPrivateAddressThatIsntOne(t *testing.T) {
	for _, join := range [][]string{nil, {"--join-token", joinToken}} {
		o := newOperator(t, "op")
		source := preflightCluster(t, o)
		joinRefused(t, o, source, append([]string{"--private-address", "web1"}, join...), "--private-address")
		r := o.run(append([]string{"--release-source", source, "node", "add", unreachable, "--private-address", ""}, join...)...)
		if r.err == nil || !strings.Contains(r.err.Error(), unreachable) {
			t.Errorf("node add %v with an empty --private-address: %v, want it to go on to %s", join, r.err, unreachable)
		}
	}
}

func TestNodeAddWithAJoinTokenTakesAnAddressInTheTarget(t *testing.T) {
	o := newOperator(t, "op")
	source := preflightCluster(t, o)
	r := o.run("--release-source", source, "node", "add", unreachable+",private-address=10.0.0.5", "--join-token", joinToken)
	if r.err == nil || !strings.Contains(r.err.Error(), "fetching the host key of "+unreachable) {
		t.Errorf("node add --join-token with an address in the target: %v, want it to go on to %s", r.err, unreachable)
	}
}

func TestNodeAddWithAJoinTokenTakesAnEmptyNameAsAbsent(t *testing.T) {
	o := newOperator(t, "op")
	source := preflightCluster(t, o)
	r := o.run("--release-source", source, "node", "add", unreachable, "--join-token", joinToken, "--name", "")
	if r.err == nil || !strings.Contains(r.err.Error(), "fetching the host key of "+unreachable) {
		t.Errorf("node add --join-token with an empty --name: %v, want it to go on to %s", r.err, unreachable)
	}
}
