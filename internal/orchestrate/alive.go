package orchestrate

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/hostlayout"
)

const (
	// alivePoll is how often WaitAlive reads the nodes' views.
	alivePoll = 200 * time.Millisecond
	// aliveReadTimeout bounds one reading of a node's view.
	aliveReadTimeout = 10 * time.Second
)

// Empty says whether the cluster has a connection to no node.
func (c *Cluster) Empty() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.conns) == 0
}

// WaitAlive waits up to wait until the node called name and another node
// of the cluster hold each other alive, reading each node's view on its
// own. If heldOut, the others may hold name out until they record it, and
// only name's view counts. If name, or every other node, never reported its
// membership, it fails saying so; otherwise it fails saying the nodes that
// did and name can't reach each other. It fails with ctx's cause if ctx
// ends.
func (c *Cluster) WaitAlive(ctx context.Context, name string, wait time.Duration, heldOut bool) error {
	c.mu.Lock()
	conns := slices.Clone(c.conns)
	c.mu.Unlock()
	i, err := connIndex(conns, name)
	if err != nil {
		return err
	}
	conns[0], conns[i] = conns[i], conns[0]
	waiting, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	reports := make(chan viewReport)
	for i, n := range conns {
		go reportViews(waiting, i, n, reports)
	}
	views := make([]map[string]nodev1.MemberState, len(conns))
	silent := make([]error, len(conns))
	for i := range silent {
		silent[i] = errors.New("it didn't answer")
	}
	for {
		select {
		case r := <-reports:
			if r.err == nil {
				silent[r.i], views[r.i] = nil, r.view
				if bothAlive(conns, views, heldOut) {
					return nil
				}
			} else if silent[r.i] != nil {
				silent[r.i] = r.err
			}
		case <-waiting.Done():
			if err := context.Cause(ctx); err != nil {
				return err
			}
			return notAlive(conns, silent, wait)
		}
	}
}

// viewReport is what the node at index i of a wait said of its view: the
// view, or why it gave none.
type viewReport struct {
	i    int
	view map[string]nodev1.MemberState
	err  error
}

// reportViews sends what n says of its view to reports, every alivePoll
// after its last answer, until ctx ends.
func reportViews(ctx context.Context, i int, n *namedConn, reports chan<- viewReport) {
	for {
		r := readView(ctx, n)
		r.i = i
		select {
		case reports <- r:
		case <-ctx.Done():
			return
		}
		select {
		case <-time.After(alivePoll):
		case <-ctx.Done():
			return
		}
	}
}

// readView is what n says of its view now.
func readView(ctx context.Context, n *namedConn) viewReport {
	ctx, cancel := context.WithTimeout(ctx, aliveReadTimeout)
	defer cancel()
	s := n.conn.Status(ctx)
	if s.Membership == nil {
		return viewReport{err: cmp.Or(s.AgentErr, errors.New("it reports no membership"))}
	}
	view := map[string]nodev1.MemberState{}
	for _, member := range s.Membership.Members {
		view[member.Node] = member.State
	}
	return viewReport{view: view}
}

// bothAlive says whether conns[0] and one of the others hold each other
// alive, by views, which are theirs in order. If heldOut, conns[0]'s view
// alone counts.
func bothAlive(conns []*namedConn, views []map[string]nodev1.MemberState, heldOut bool) bool {
	for i, n := range conns[1:] {
		if views[0][n.name] == nodev1.MemberState_MEMBER_STATE_ALIVE && (heldOut || views[i+1][conns[0].name] == nodev1.MemberState_MEMBER_STATE_ALIVE) {
			return true
		}
	}
	return false
}

// notAlive is why conns[0] and conns[1:] didn't hold each other alive
// within wait, from silent, which says why each node never reported its
// membership, nil if it did.
func notAlive(conns []*namedConn, silent []error, wait time.Duration) error {
	if silent[0] != nil {
		return fmt.Errorf("%s didn't report its membership within %s: %w", conns[0].name, wait, silent[0])
	}
	var answered, reasons []string
	for i, n := range conns[1:] {
		if silent[i+1] == nil {
			answered = append(answered, n.name)
		} else {
			reasons = append(reasons, fmt.Sprintf("%s: %v", n.name, silent[i+1]))
		}
	}
	slices.Sort(answered)
	slices.Sort(reasons)
	if len(answered) == 0 {
		return fmt.Errorf("no other node reported its membership within %s:\n%s", wait, strings.Join(reasons, "\n"))
	}
	err := fmt.Errorf("%s and %s can't reach each other over UDP on the peer port, %d", conns[0].name, strings.Join(answered, ", "), hostlayout.PeerPort)
	if len(reasons) > 0 {
		return fmt.Errorf("%w\nNo membership reported by:\n%s", err, strings.Join(reasons, "\n"))
	}
	return err
}
