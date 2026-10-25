package orchestrate_test

import (
	"errors"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/orchestrate"
)

// renameSeed is the seed of TestRenameSequencesMatchTheModel, from
// MOOSTACK_STATE_SEED if set, so that a failing sequence runs again.
func renameSeed(t *testing.T) uint64 {
	if s := os.Getenv("MOOSTACK_STATE_SEED"); s != "" {
		seed, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		return seed
	}
	return uint64(time.Now().UnixNano())
}

// Seeded sequences of updates of a node whose services run under their
// old names, at the target version or the one before, or renamed
// already, each update failing at a step picked at random or not at
// all, checked against the model after each: no service ever runs under
// both names, one that ran runs under neither only once its run failed
// in the update just ended, an update that fails nowhere leaves every service
// under its service- name at the target, and the next changes nothing.
func TestRenameSequencesMatchTheModel(t *testing.T) {
	seed := renameSeed(t)
	t.Logf("MOOSTACK_STATE_SEED=%d", seed)
	r := rand.New(rand.NewPCG(seed, seed))
	steps := []string{"import dns", "remove dns", "service dns", "import proxy", "remove proxy", "service proxy"}
	healthy := nodev1.Health_HEALTH_HEALTHY
	for range 100 {
		n := &node{generation: 1}
		for _, s := range []string{"dns", "proxy"} {
			version := []string{"1.0.0", "2.0.0"}[r.IntN(2)]
			switch r.IntN(3) {
			case 0:
				n.oldNames = append(n.oldNames, &nodev1.Component{Name: s, Version: version, Health: healthy})
			case 1:
				n.services = append(n.services, &nodev1.Component{Name: s, Version: version, Health: healthy})
				if n.images == nil {
					n.images = map[string]bool{}
				}
				n.images["moostack.local/"+s+":"+version] = true
			}
		}
		start := n.containerNames()
		cl, err := orchestrate.Preflight(t.Context(), inOrder(fakes{"node1": n}, "node1"), 1, false, holder)
		if err != nil {
			t.Fatal(err)
		}
		for range 1 + r.IntN(4) {
			failing := ""
			if r.IntN(2) == 0 {
				failing = steps[r.IntN(len(steps))]
			}
			n.failing, n.updateErr = failing, nil
			if failing != "" {
				n.updateErr = errors.New("the node failed")
			}
			before := n.updatesTold()
			present := map[string]bool{}
			for _, name := range n.containerNames() {
				present[strings.TrimPrefix(name, "service-")] = true
			}
			err := update(cl)
			names := n.containerNames()
			slices.Sort(names)
			for _, s := range []string{"dns", "proxy"} {
				old, renamed := slices.Contains(names, s), slices.Contains(names, "service-"+s)
				if old && renamed {
					t.Fatalf("seed %d, from %q, failing at %q: node1 runs %s under both names: %q", seed, start, failing, s, names)
				}
				if !old && !renamed && present[s] && failing != "service "+s {
					t.Fatalf("seed %d, from %q, failing at %q: node1 runs %s under neither name: %q", seed, start, failing, s, names)
				}
			}
			if err == nil {
				if !slices.Equal(names, []string{"service-dns", "service-proxy"}) {
					t.Fatalf("seed %d, from %q: the update succeeded, and node1 runs %q", seed, start, names)
				}
				n.failing, n.updateErr = "", nil
				again := n.updatesTold()
				if err := update(cl); err != nil || len(n.updatesTold()) != len(again) {
					t.Fatalf("seed %d, from %q: the update after one that succeeded: %v, did %q", seed, start, err, n.updatesTold()[len(again):])
				}
			} else if failing == "" {
				t.Fatalf("seed %d, from %q: the update failing nowhere failed: %v, after %q", seed, start, err, before)
			}
		}
		cl.Close()
	}
}
