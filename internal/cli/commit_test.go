package cli_test

import (
	"strings"
	"testing"
)

// commitReminder is what mooctl prints after an operation that changed
// the cluster directory.
const commitReminder = "commit it, so that your teammates see the change."

// wantReminder fails unless what printed the reminder to commit, as its
// last line, if remind, and else doesn't print it at all.
func wantReminder(t *testing.T, what string, r result, remind bool) {
	t.Helper()
	switch printed := strings.Contains(r.stdout, commitReminder); {
	case remind && !strings.HasSuffix(r.stdout, commitReminder+"\n"):
		t.Errorf("%s printed\n%s\nwant it to end with the reminder to commit", what, r.stdout)
	case !remind && printed:
		t.Errorf("%s, which changed nothing, printed the reminder to commit:\n%s", what, r.stdout)
	}
}

func TestOperationsThatChangeTheClusterDirectoryRemindToCommit(t *testing.T) {
	o := newOperator(t, "op")
	wantReminder(t, "cluster init", o.mustRun("cluster", "init", "--ssh-key", o.key, "--name", "prod"), true)

	bob := newOperator(t, "bob")
	wantReminder(t, "secret recipients add", o.mustRun("secret", "recipients", "add", bob.recipient.String()), true)
	wantReminder(t, "secret recipients rm", o.mustRun("secret", "recipients", "rm", "bob"), true)
	wantReminder(t, "secret recipients ls", o.mustRun("secret", "recipients", "ls"), false)

	scriptEditor(t, "editor", "", "token:\n  generate: {type: hex}\n")
	wantReminder(t, "secret edit", o.mustRun("secret", "edit"), true)
	scriptEditor(t, "editor", "")
	wantReminder(t, "secret edit without changes", o.mustRun("secret", "edit"), false)
	wantReminder(t, "secret view", o.mustRun("secret", "view"), false)

	wantReminder(t, "secret rotate", o.mustRun("secret", "rotate", "token"), true)
	wantReminder(t, "secret rotate with nothing to rotate", o.mustRun("secret", "rotate"), false)
}

func TestSecretInitRemindsToCommit(t *testing.T) {
	o := newOperator(t, "op")
	wantReminder(t, "secret init", o.mustRun("secret", "init", "--ssh-key", o.key), true)
}

func TestClusterStatusDoesntRemindToCommit(t *testing.T) {
	o := newOperator(t, "op")
	source := statusCluster(t, o, "node1")
	o.nodes = map[string]*fakeNode{"node1": healthyNode(1)}
	wantReminder(t, "cluster status", o.mustRun("--release-source", source, "cluster", "status"), false)
}
