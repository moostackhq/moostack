//go:build linux

package agent

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math/rand/v2"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/moostackhq/moostack/internal/hivemindadapter"
	"github.com/moostackhq/moostack/internal/hostlayout"
)

// heldMembership keeps the names a node holds out, as membership does:
// a Forget holds a name, and a SetSeeds naming it lifts the hold.
type heldMembership struct {
	self string
	held map[string]bool
	cut  []string
}

func (m *heldMembership) Status() hivemindadapter.Status { return hivemindadapter.Status{} }

func (m *heldMembership) SetSeeds(seeds []hivemindadapter.Seed) error {
	for _, s := range seeds {
		delete(m.held, s.NodeID)
	}
	return nil
}

func (m *heldMembership) Forget(nodeID string) error {
	if nodeID == m.self {
		return fmt.Errorf("can't forget %s", nodeID)
	}
	m.held[nodeID] = true
	return nil
}

func (m *heldMembership) CutOff(fingerprint string) error {
	m.cut = append(m.cut, fingerprint)
	return nil
}

// fingerprintOf is a fingerprint, the same for the same i.
func fingerprintOf(i int) string { return fmt.Sprintf("sha256:%064x", i) }

// removalNode is what a node keeps across a restart, and its agent.
type removalNode struct {
	t      *testing.T
	dir    string
	server *agentServer
	m      *heldMembership
}

// start starts the agent, as Serve does with the node's removed
// identities and generation.
func (n *removalNode) start() {
	n.t.Helper()
	removed, err := loadRemoved(filepath.Join(n.dir, "removed"))
	if err != nil {
		n.t.Fatal(err)
	}
	n.m = &heldMembership{self: "self", held: map[string]bool{}}
	n.server = &agentServer{generationFile: filepath.Join(n.dir, "generation"), removed: removed, members: n.m, peerPort: 7443}
	removed.onAdd = n.server.applyRemoval
	if a, err := n.server.applied(); err == nil {
		n.server.forgetRemoved(a)
		n.server.seed(a.Nodes)
	}
}

// record records g of nodes, as RecordGeneration does.
func (n *removalNode) record(g int64, names []string) {
	n.t.Helper()
	nodes := make([]generationNode, len(names))
	for i, name := range names {
		nodes[i] = generationNode{Name: name, PrivateAddress: netip.MustParseAddr("192.0.2.1")}
	}
	a := appliedGeneration{Generation: g, Nodes: nodes}
	data, err := json.Marshal(a)
	if err != nil {
		n.t.Fatal(err)
	}
	if err := os.WriteFile(n.server.generationFile, data, 0o600); err != nil {
		n.t.Fatal(err)
	}
	n.server.forgetRemoved(a)
	n.server.seed(nodes)
}

// Generated sequences of removals, recordings, restarts and an unreadable
// generation. The model says of each name:
// held out once the node, knowing a generation that doesn't list it, is
// told of the removal, records, or starts; back once a generation lists
// it at a recording or a start; never for an unknown generation.
func TestRemovalsAgainstAModel(t *testing.T) {
	names := []string{"a", "b", "c", "self"}
	for seed := range uint64(200) {
		r := rand.New(rand.NewPCG(seed, 15))
		n := &removalNode{t: t, dir: t.TempDir()}
		n.start()
		var removed []string // a name per identity, in order
		listed := []string{} // what the last recording lists
		generation, readable := int64(0), true
		held := map[string]bool{}
		forget := func() {
			if !readable || generation == 0 {
				return
			}
			for _, name := range removed {
				if name != "self" && !slices.Contains(listed, name) {
					held[name] = true
				}
			}
		}
		lift := func() {
			for _, name := range listed {
				delete(held, name)
			}
		}
		for step := range 60 {
			var did string
			switch op := r.IntN(10); {
			case op < 4:
				name := names[r.IntN(len(names))]
				i := len(removed)
				if r.IntN(4) == 0 && i > 0 {
					i = r.IntN(i) // an identity held already
				} else {
					removed = append(removed, name)
					forget()
				}
				did = fmt.Sprintf("removal of %s (identity %d)", name, i)
				if err := n.server.removed.add([]hostlayout.RemovedIdentity{{Node: removed[i], Identity: fingerprintOf(i)}}); err != nil {
					t.Fatal(err)
				}
			case op < 7:
				generation++
				readable = true
				listed = []string{}
				for _, name := range names {
					if r.IntN(2) == 0 {
						listed = append(listed, name)
					}
				}
				did = fmt.Sprintf("recording of %v", listed)
				n.record(generation, listed)
				forget()
				lift()
			case op < 9:
				did = "restart"
				n.start()
				held = map[string]bool{}
				if readable {
					forget()
					lift()
				}
			default:
				did = "an unreadable generation"
				if generation > 0 {
					readable = false
					if err := os.WriteFile(n.server.generationFile, []byte("garbage"), 0o600); err != nil {
						t.Fatal(err)
					}
				}
			}
			if !maps.Equal(n.m.held, held) {
				t.Fatalf("seed %d, step %d, after %s: the node holds out %v, want %v", seed, step, did, n.m.held, held)
			}
		}
	}
}

func TestARemovalCutsOffOnlyWhatItAdds(t *testing.T) {
	n := &removalNode{t: t, dir: t.TempDir()}
	n.start()
	first := []hostlayout.RemovedIdentity{{Node: "a", Identity: fingerprintOf(1)}}
	for range 2 {
		if err := n.server.removed.add(first); err != nil {
			t.Fatal(err)
		}
	}
	if err := n.server.removed.add(append(first, hostlayout.RemovedIdentity{Node: "b", Identity: fingerprintOf(2)})); err != nil {
		t.Fatal(err)
	}
	if want := []string{fingerprintOf(1), fingerprintOf(2)}; !slices.Equal(n.m.cut, want) {
		t.Errorf("cut off %v, want %v", n.m.cut, want)
	}
}

// A connection tracked while its identity is removed is either refused or
// closed: none stays open once both are done.
func TestTrackRacingARemovalLeavesNoConnection(t *testing.T) {
	for i := range 200 {
		r, err := loadRemoved(filepath.Join(t.TempDir(), "removed"))
		if err != nil {
			t.Fatal(err)
		}
		a, b := net.Pipe()
		defer b.Close()
		var wg sync.WaitGroup
		wg.Go(func() {
			if err := r.add([]hostlayout.RemovedIdentity{{Node: "a", Identity: fingerprintOf(i)}}); err != nil {
				t.Error(err)
			}
		})
		wg.Go(func() { r.track(a, fingerprintOf(i)) })
		wg.Go(func() { r.has(fingerprintOf(i)); r.nodes() })
		wg.Wait()
		a.SetWriteDeadline(time.Now().Add(50 * time.Millisecond))
		if _, err := a.Write([]byte{0}); !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("round %d: writing to the connection of a removed identity: %v, want it closed", i, err)
		}
	}
}

func BenchmarkARecordingForgetsTheRemovedNodes(b *testing.B) {
	for _, removed := range []int{10, 1000} {
		b.Run(fmt.Sprint(removed), func(b *testing.B) {
			r, err := loadRemoved(filepath.Join(b.TempDir(), "removed"))
			if err != nil {
				b.Fatal(err)
			}
			for i := range removed {
				r.list = append(r.list, hostlayout.RemovedIdentity{Node: fmt.Sprint("node-", i), Identity: fingerprintOf(i)})
			}
			s := &agentServer{removed: r, members: &heldMembership{held: map[string]bool{}}}
			a := appliedGeneration{Generation: 1, Nodes: make([]generationNode, 100)}
			for b.Loop() {
				s.forgetRemoved(a)
			}
		})
	}
}
