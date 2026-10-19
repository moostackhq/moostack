package cli_test

import (
	"strings"
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
	bob.dir = alice.dir
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
	bob.dir = alice.dir
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
