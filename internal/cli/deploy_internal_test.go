package cli

import (
	"fmt"
	"testing"

	"github.com/moostackhq/moostack/internal/orchestrate"
)

// A pull stopped because the operation lost the lock says so, and that
// nothing changed.
func TestAPullStoppedByALostLockSaysSo(t *testing.T) {
	err := deployError("web", &orchestrate.PullError{Node: "node1", Image: "docker.io/library/nginx:1.27",
		Err: fmt.Errorf("%w: node1: it is locked by bob", orchestrate.ErrLockLost)})
	want := "mooctl stopped the pull of docker.io/library/nginx:1.27 on node1: the operation lost the operation lock: node1: it is locked by bob; nothing changed"
	if err == nil || err.Error() != want {
		t.Errorf("a pull stopped by a lost lock says\n%v\nwant\n%s", err, want)
	}
}

// A removal that lost the lock says so, and how to finish it: before
// any node, it changed nothing; between two nodes, it says to remove
// again; and during a node's removal, that it may go on.
func TestARemovalThatLostTheLockSaysSo(t *testing.T) {
	lost := fmt.Errorf("%w: node1: it is locked by bob", orchestrate.ErrLockLost)
	for _, c := range []struct {
		removed []string
		err     error
		want    string
	}{
		{nil, fmt.Errorf("%w, before mooctl removed web from any node; nothing changed", lost),
			"the operation lost the operation lock: node1: it is locked by bob, before mooctl removed web from any node; nothing changed"},
		{[]string{"node1"}, fmt.Errorf("%w, so mooctl removed web from no more nodes", lost),
			"the operation lost the operation lock: node1: it is locked by bob, so mooctl removed web from no more nodes\nRun mooctl deploy --remove again: it removes web from every node it is still on"},
		{nil, &orchestrate.RemoveError{Node: "node1", App: "web", Err: lost, Stopped: true},
			"mooctl stopped waiting for node1 to remove web, which may go on on the node: the operation lost the operation lock: node1: it is locked by bob\nRun mooctl deploy --remove again: it says whether web is deployed"},
	} {
		if err := removeError("web", c.removed, c.err); err == nil || err.Error() != c.want {
			t.Errorf("a removal that lost the lock, from %v, says\n%v\nwant\n%s", c.removed, err, c.want)
		}
	}
}
