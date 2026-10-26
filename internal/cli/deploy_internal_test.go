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
