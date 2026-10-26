package orchestrate_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/orchestrate"
)

// releaseOf gives each node the agent at 2.0.0, the updater at 2.0.1
// and containerd at 3.0.0, each of whose binaries names the node, and
// the dns and proxy services at 2.0.0, which replace the containers
// releases before the service- names ran them as.
func releaseOf(name string) (orchestrate.NodeRelease, error) {
	return orchestrate.NodeRelease{
		Components: map[string]orchestrate.Component{
			"agent":      {Version: "2.0.0", Files: map[string][]byte{"agent": []byte("agent for " + name)}},
			"updater":    {Version: "2.0.1", Files: map[string][]byte{"updater": []byte("updater for " + name)}},
			"containerd": {Version: "3.0.0", Files: map[string][]byte{"containerd": []byte("containerd for " + name), "shim": []byte("shim for " + name)}},
		},
		Services: []orchestrate.Service{
			{Name: "dns", Version: "2.0.0", Container: &nodev1.Container{Name: "service-dns", Image: "moostack.local/dns:2.0.0", Api: "moostack.dns.v1.DNS"},
				Archive: []byte("image moostack.local/dns:2.0.0"), Replaces: "dns", ReplacesImages: "moostack.local/dns:"},
			{Name: "proxy", Version: "2.0.0", Container: &nodev1.Container{Name: "service-proxy", Image: "moostack.local/proxy:2.0.0", Api: "moostack.proxy.v1.Proxy"},
				Archive: []byte("image moostack.local/proxy:2.0.0"), Replaces: "proxy", ReplacesImages: "moostack.local/proxy:"},
		},
	}, nil
}

// update has cl update its nodes to releaseOf's, telling nothing.
func update(cl *orchestrate.Cluster) error {
	return cl.Update(cl.Context(), releaseOf, func(orchestrate.Step) {})
}

// inOrder are fakes' nodes in the order of names.
func inOrder(f fakes, names ...string) orchestrate.Nodes {
	return orchestrate.Nodes{Names: names, Dial: f.dial}
}

func TestUpdateUpdatesEachNodesComponentsNodeByNodeThenEachNodesServices(t *testing.T) {
	f := fakes{}
	names := []string{"node1", "node2", "node3"}
	for _, name := range names {
		f[name] = &node{generation: 1, updateDelay: 50 * time.Millisecond}
	}
	nodes := inOrder(f, names...)
	nodes.LeaseTTL = 10 * time.Second
	cl, err := orchestrate.Preflight(t.Context(), nodes, 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	var told []string
	err = cl.Update(cl.Context(), releaseOf, func(s orchestrate.Step) {
		told = append(told, s.Node+" "+s.Component+" "+s.Version)
	})
	if err != nil {
		t.Fatal(err)
	}
	var all []componentUpdate
	for _, name := range names {
		all = append(all, f[name].componentUpdates("")...)
	}
	slices.SortFunc(all, func(a, b componentUpdate) int { return a.started.Compare(b.started) })
	var got []string
	var previous time.Time
	for _, u := range all {
		got = append(got, u.component+" "+u.version+" "+u.binary)
		if u.started.Before(previous) {
			t.Errorf("the update of %s started before the previous update ended", u.binary)
		}
		previous = u.ended
		// Each node component has a fifth of a lease time to become
		// healthy.
		if !strings.HasPrefix(u.component, "service ") && u.healthTimeout != 2*time.Second {
			t.Errorf("%s's health timeout is %s, want a fifth of the lease time, 2s", u.binary, u.healthTimeout)
		}
	}
	var want, wantTold []string
	for _, name := range names {
		want = append(want,
			"agent 2.0.0 agent for "+name,
			"updater 2.0.1 updater for "+name,
			"containerd 3.0.0 containerd: containerd for "+name+", shim: shim for "+name)
		wantTold = append(wantTold, name+" the agent 2.0.0", name+" the updater 2.0.1", name+" containerd 3.0.0")
	}
	for _, name := range names {
		want = append(want,
			"service dns 2.0.0 moostack.local/dns:2.0.0",
			"service proxy 2.0.0 moostack.local/proxy:2.0.0")
		wantTold = append(wantTold, name+" the dns service 2.0.0", name+" the proxy service 2.0.0")
	}
	if !slices.Equal(got, want) {
		t.Errorf("the nodes were updated in the order\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	if !slices.Equal(told, wantTold) {
		t.Errorf("the update told %q, want %q", told, wantTold)
	}
}

// A rerun reaches containerd on a node whose new containerd doesn't
// answer: the new agent would be refused while containerd is unhealthy,
// but the node runs it and the updater at the target already.
func TestUpdateSkipsTheAgentAndTheUpdaterWhereTheNodeRunsThemAlready(t *testing.T) {
	healthy, unhealthy := nodev1.Health_HEALTH_HEALTHY, nodev1.Health_HEALTH_UNHEALTHY
	f := fakes{
		"node1": {generation: 1, components: []*nodev1.Component{
			{Name: "agent", Version: "2.0.0", Health: healthy},
			{Name: "containerd", Health: unhealthy},
			{Name: "updater", Version: "2.0.1", Health: healthy},
		}},
		// node2's updater runs the target, but doesn't answer.
		"node2": {generation: 1, components: []*nodev1.Component{
			{Name: "agent", Version: "1.0.0", Health: healthy},
			{Name: "updater", Version: "2.0.1", Health: unhealthy},
		}},
	}
	cl, err := orchestrate.Preflight(t.Context(), inOrder(f, "node1", "node2"), 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	var told []string
	err = cl.Update(cl.Context(), releaseOf, func(s orchestrate.Step) {
		if !strings.HasSuffix(s.Component, " service") {
			told = append(told, fmt.Sprintf("%s %s %s %v", s.Node, s.Component, s.Version, s.Already))
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string][]string{"node1": {"containerd"}, "node2": {"agent", "updater", "containerd"}} {
		var got []string
		for _, u := range f[name].componentUpdates("") {
			if strings.HasPrefix(u.component, "service ") {
				continue
			}
			got = append(got, u.component)
		}
		if !slices.Equal(got, want) {
			t.Errorf("%s updated %v, want %v", name, got, want)
		}
	}
	want := []string{
		"node1 the agent 2.0.0 true", "node1 the updater 2.0.1 true", "node1 containerd 3.0.0 false",
		"node2 the agent 2.0.0 false", "node2 the updater 2.0.1 false", "node2 containerd 3.0.0 false",
	}
	if !slices.Equal(told, want) {
		t.Errorf("the update told\n%s\nwant\n%s", strings.Join(told, "\n"), strings.Join(want, "\n"))
	}
}

func TestUpdateStopsAtTheFirstStepThatFailsNamingTheNodeAndTheStep(t *testing.T) {
	for _, c := range []struct {
		failing string
		// want is in the error, and done are the steps node2 took.
		want string
		done []string
	}{
		{"agent", "updating the agent to 2.0.0", []string{"agent"}},
		{"updater", "updating the updater to 2.0.1", []string{"agent", "updater"}},
		{"containerd", "updating containerd to 3.0.0", []string{"agent", "updater", "containerd"}},
	} {
		t.Run(c.failing, func(t *testing.T) {
			f := fakes{"node1": {generation: 1}, "node2": {generation: 1, failing: c.failing, updateErr: errors.New("it broke")}, "node3": {generation: 1}}
			cl, err := orchestrate.Preflight(t.Context(), inOrder(f, "node1", "node2", "node3"), 1, false, holder)
			if err != nil {
				t.Fatal(err)
			}
			defer cl.Close()
			err = update(cl)
			var ne *orchestrate.NodeError
			if !errors.As(err, &ne) || ne.Node != "node2" {
				t.Fatalf("Update: %v, want a NodeError for node2", err)
			}
			wantContains(t, "the error", err.Error(), c.want+": it broke")
			if n := len(f["node1"].componentUpdates("")); n != 3 {
				t.Errorf("node1 had %d updates, want 3", n)
			}
			var done []string
			for _, u := range f["node2"].componentUpdates("") {
				done = append(done, u.component)
			}
			if !slices.Equal(done, c.done) {
				t.Errorf("node2 updated %v, want %v", done, c.done)
			}
			if n := len(f["node3"].componentUpdates("")); n != 0 {
				t.Errorf("node3 had %d updates after node2 failed, want none", n)
			}
		})
	}
}

func TestUpdateStopsBeforeTheNextStepOnceTheLockIsLost(t *testing.T) {
	f := fakes{"node1": {generation: 1, updateDelay: 500 * time.Millisecond}}
	f["node1"].onUpdateOf = map[string]func(){"updater": func() { f["node1"].BreakLock(context.Background()) }}
	nodes := inOrder(f, "node1")
	nodes.LeaseTTL = 600 * time.Millisecond
	cl, err := orchestrate.Preflight(t.Context(), nodes, 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if err := update(cl); !errors.Is(err, orchestrate.ErrLockLost) {
		t.Fatalf("Update: %v, want ErrLockLost", err)
	}
	if n := len(f["node1"].componentUpdates("containerd")); n != 0 {
		t.Errorf("containerd was updated %d times once the lock was lost, want never", n)
	}
}

func TestUpdateFailsNamingANodeWithoutItsAgent(t *testing.T) {
	f := fakes{"node1": {generation: 1}}
	cl, err := orchestrate.Preflight(t.Context(), inOrder(f, "node1"), 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	err = cl.Update(cl.Context(), func(string) (orchestrate.NodeRelease, error) {
		return orchestrate.NodeRelease{}, errors.New("no agent for riscv64")
	}, func(orchestrate.Step) {})
	var ne *orchestrate.NodeError
	if !errors.As(err, &ne) || ne.Node != "node1" {
		t.Fatalf("Update: %v, want a NodeError for node1", err)
	}
	if n := len(f["node1"].componentUpdates("")); n != 0 {
		t.Errorf("node1 was updated %d times, want never", n)
	}
}

func TestUpdateRenewsTheLeaseOnANodeOnceItsAgentRestarts(t *testing.T) {
	f := fakes{"node1": {generation: 1, restartFor: 300 * time.Millisecond}}
	nodes := inOrder(f, "node1")
	nodes.LeaseTTL = time.Minute
	cl, err := orchestrate.Preflight(t.Context(), nodes, 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if err := update(cl); err != nil {
		t.Fatal(err)
	}
	// keepLease renews only every twenty seconds, so a lease that runs
	// out a minute after the agent came back was renewed by the update.
	ended := f["node1"].agentUpdates()[0].ended
	if got, want := f["node1"].leaseExpires(), ended.Add(time.Minute+300*time.Millisecond); got.Before(want) {
		t.Errorf("node1's lease runs out %s after its update ended, want it renewed once the agent came back", got.Sub(ended))
	}
}

func TestUpdateFailsWhenARestartedAgentHoldsAnotherLease(t *testing.T) {
	f := fakes{"node1": {generation: 1}, "node2": {generation: 1}}
	f["node1"].onUpdate = func() { f["node1"].lockFor("bobs", time.Minute) }
	cl, err := orchestrate.Preflight(t.Context(), inOrder(f, "node1", "node2"), 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	err = update(cl)
	if !errors.Is(err, orchestrate.ErrLockLost) {
		t.Fatalf("Update: %v, want ErrLockLost", err)
	}
	if n := len(f["node2"].agentUpdates()); n != 0 {
		t.Errorf("node2 was updated %d times once the lock was lost, want never", n)
	}
}

func TestUpdateStopsBeforeTheNextNodeOnceTheLockIsLost(t *testing.T) {
	f := fakes{"node1": {generation: 1, updateDelay: 500 * time.Millisecond}, "node2": {generation: 1}}
	f["node1"].onUpdate = func() { f["node2"].BreakLock(context.Background()) }
	nodes := inOrder(f, "node1", "node2")
	nodes.LeaseTTL = 600 * time.Millisecond
	cl, err := orchestrate.Preflight(t.Context(), nodes, 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	err = update(cl)
	if !errors.Is(err, orchestrate.ErrLockLost) {
		t.Fatalf("Update: %v, want ErrLockLost", err)
	}
	if n := len(f["node2"].agentUpdates()); n != 0 {
		t.Errorf("node2 was updated %d times once the lock was lost, want never", n)
	}
}

func TestUpdateFailsNamingANodeWhoseRestartedAgentDoesntAnswer(t *testing.T) {
	f := fakes{"node1": {generation: 1, restartFor: time.Hour}, "node2": {generation: 1}}
	nodes := inOrder(f, "node1", "node2")
	nodes.LeaseTTL = 3 * time.Second
	cl, err := orchestrate.Preflight(t.Context(), nodes, 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	err = update(cl)
	var ne *orchestrate.NodeError
	if !errors.As(err, &ne) || ne.Node != "node1" {
		t.Fatalf("Update: %v, want a NodeError for node1", err)
	}
	wantContains(t, "the error", err.Error(), "doesn't answer mooctl", "the agent is restarting")
	if n := len(f["node2"].agentUpdates()); n != 0 {
		t.Errorf("node2 was updated %d times, want never", n)
	}
}

func TestUpdateFailsWhenTheLastNodesRestartedAgentHoldsAnotherLease(t *testing.T) {
	f := fakes{"node1": {generation: 1}}
	f["node1"].onUpdate = func() { f["node1"].lockFor("bobs", time.Minute) }
	cl, err := orchestrate.Preflight(t.Context(), inOrder(f, "node1"), 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if err := update(cl); !errors.Is(err, orchestrate.ErrLockLost) {
		t.Errorf("Update: %v, want ErrLockLost", err)
	}
}

// The agent stops answering renewals a third of a lease time before its
// restart, as when the last renewal before it was answered just after
// the preflight. The swap and the rollback both run out their time, and
// the lease must outlast them, so that the update fails with why, and
// not with the lock lost.
func TestARollbackEndsBeforeTheLeaseCanRunOut(t *testing.T) {
	f := fakes{"node1": {generation: 1, rollsBack: true}}
	nodes := inOrder(f, "node1")
	nodes.LeaseTTL = 1500 * time.Millisecond
	cl, err := orchestrate.Preflight(t.Context(), nodes, 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	f["node1"].mu.Lock()
	f["node1"].downUntil = time.Now().Add(time.Hour)
	f["node1"].mu.Unlock()
	slowly := func(name string) (orchestrate.NodeRelease, error) {
		time.Sleep(450 * time.Millisecond)
		return releaseOf(name)
	}
	err = cl.Update(cl.Context(), slowly, func(orchestrate.Step) {})
	if errors.Is(err, orchestrate.ErrLockLost) || err == nil || !strings.Contains(err.Error(), "rolled back to the agent it replaced") {
		t.Errorf("Update: %v, want the rollback's error, before the lease may run out", err)
	}
}

func TestUpdateSkipsWhatANodeRunsAtTheTargetHealthy(t *testing.T) {
	healthy, unhealthy := nodev1.Health_HEALTH_HEALTHY, nodev1.Health_HEALTH_UNHEALTHY
	upToDate := func() []*nodev1.Component {
		return []*nodev1.Component{
			{Name: "agent", Version: "2.0.0", Health: healthy},
			{Name: "containerd", Version: "3.0.0", Health: healthy},
			{Name: "updater", Version: "2.0.1", Health: healthy},
		}
	}
	f := fakes{
		"node1": {generation: 1, components: upToDate(), services: []*nodev1.Component{
			{Name: "dns", Version: "2.0.0", Health: healthy},
			{Name: "proxy", Version: "2.0.0", Health: healthy},
		}},
		// node2 runs containerd and the proxy at the target, but neither
		// is healthy, and the dns service at another version.
		"node2": {generation: 1, components: upToDate(), services: []*nodev1.Component{
			{Name: "dns", Version: "1.0.0", Health: healthy},
			{Name: "proxy", Version: "2.0.0", Health: unhealthy},
		}},
	}
	f["node2"].components[1].Health = unhealthy
	cl, err := orchestrate.Preflight(t.Context(), inOrder(f, "node1", "node2"), 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	var told []string
	err = cl.Update(cl.Context(), releaseOf, func(s orchestrate.Step) {
		told = append(told, fmt.Sprintf("%s %s %v", s.Node, s.Component, s.Already))
	})
	if err != nil {
		t.Fatal(err)
	}
	if u := f["node1"].componentUpdates(""); len(u) != 0 {
		t.Errorf("node1, up to date, was updated: %v", u)
	}
	var got []string
	for _, u := range f["node2"].componentUpdates("") {
		got = append(got, u.component)
	}
	if want := []string{"containerd", "service dns", "service proxy"}; !slices.Equal(got, want) {
		t.Errorf("node2 updated %v, want %v", got, want)
	}
	want := []string{
		"node1 the agent true", "node1 the updater true", "node1 containerd true",
		"node2 the agent true", "node2 the updater true", "node2 containerd false",
		"node1 the dns service true", "node1 the proxy service true",
		"node2 the dns service false", "node2 the proxy service false",
	}
	if !slices.Equal(told, want) {
		t.Errorf("the update told\n%s\nwant\n%s", strings.Join(told, "\n"), strings.Join(want, "\n"))
	}
}

func TestUpdateStopsAtTheFirstServiceStepThatFailsNamingTheNodeAndTheStep(t *testing.T) {
	for _, c := range []struct {
		name string
		// set makes node2 fail, and want is in the error.
		set  func(n *node)
		want string
	}{
		{"import", func(n *node) { n.failing, n.updateErr = "import proxy", errors.New("it broke") }, "updating the proxy service to 2.0.0: it broke"},
		{"run", func(n *node) { n.failing, n.updateErr = "service proxy", errors.New("it broke") }, "updating the proxy service to 2.0.0: it broke"},
		{"remove", func(n *node) {
			n.oldNames = []*nodev1.Component{{Name: "proxy", Version: "1.0.0", Health: nodev1.Health_HEALTH_HEALTHY}}
			n.failing, n.updateErr = "remove proxy", errors.New("it broke")
		}, "updating the proxy service to 2.0.0: removing its container proxy: it broke"},
		{"health", func(n *node) { n.sickService = "proxy" }, "updating the proxy service to 2.0.0: it didn't become healthy within 300ms"},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := fakes{"node1": {generation: 1}, "node2": {generation: 1}, "node3": {generation: 1}}
			c.set(f["node2"])
			nodes := inOrder(f, "node1", "node2", "node3")
			nodes.ServiceTimeout = 300 * time.Millisecond
			cl, err := orchestrate.Preflight(t.Context(), nodes, 1, false, holder)
			if err != nil {
				t.Fatal(err)
			}
			defer cl.Close()
			err = update(cl)
			var ne *orchestrate.NodeError
			if !errors.As(err, &ne) || ne.Node != "node2" {
				t.Fatalf("Update: %v, want a NodeError for node2", err)
			}
			wantContains(t, "the error", err.Error(), c.want)
			if n := len(f["node1"].componentUpdates("service proxy")); n != 1 {
				t.Errorf("node1's proxy had %d updates, want 1", n)
			}
			if n := len(f["node3"].componentUpdates("containerd")); n != 1 {
				t.Errorf("node3's containerd had %d updates, want 1, before any service", n)
			}
			for _, s := range []string{"service dns", "service proxy"} {
				if n := len(f["node3"].componentUpdates(s)); n != 0 {
					t.Errorf("node3's %s was updated after node2 failed", s)
				}
			}
		})
	}
}

func TestUpdateRefusesAServiceWhoseImageImportsUnderAnotherName(t *testing.T) {
	f := fakes{"node1": {generation: 1}}
	cl, err := orchestrate.Preflight(t.Context(), inOrder(f, "node1"), 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	err = cl.Update(cl.Context(), func(name string) (orchestrate.NodeRelease, error) {
		r, err := releaseOf(name)
		r.Services[0].Archive = []byte("image moostack.local/other:1.0.0")
		return r, err
	}, func(orchestrate.Step) {})
	wantContains(t, "the error", fmt.Sprint(err), "node1", "imported moostack.local/other:1.0.0, not moostack.local/dns:2.0.0")
	if n := len(f["node1"].componentUpdates("service dns")); n != 0 {
		t.Errorf("node1 ran the dns service %d times, want never", n)
	}
}

// A node that runs another build of the target version, as a bad build
// of a release built again, is updated; one that runs the target's own
// build, or that can't say its build, isn't.
func TestUpdateReplacesAnotherBuildOfTheTargetVersion(t *testing.T) {
	healthy := nodev1.Health_HEALTH_HEALTHY
	built := func(name, version, build string) *nodev1.Component {
		return &nodev1.Component{Name: name, Version: version, Build: build, Health: healthy}
	}
	runs := func(build string) *node {
		return &node{generation: 1, components: []*nodev1.Component{
			built("agent", "2.0.0", build), built("updater", "2.0.1", build), built("containerd", "3.0.0", build),
		}, services: []*nodev1.Component{built("dns", "2.0.0", build), built("proxy", "2.0.0", build)}}
	}
	f := fakes{"same": runs("sha256:good"), "other": runs("sha256:bad"), "unknown": runs("")}
	cl, err := orchestrate.Preflight(t.Context(), inOrder(f, "same", "other", "unknown"), 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	withBuilds := func(name string) (orchestrate.NodeRelease, error) {
		r, err := releaseOf(name)
		for n, c := range r.Components {
			c.Build = "sha256:good"
			r.Components[n] = c
		}
		for i := range r.Services {
			r.Services[i].Build = "sha256:good"
		}
		return r, err
	}
	if err := cl.Update(cl.Context(), withBuilds, func(orchestrate.Step) {}); err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]int{"same": 0, "other": 5, "unknown": 0} {
		if got := len(f[name].componentUpdates("")); got != want {
			t.Errorf("%s had %d updates, want %d", name, got, want)
		}
	}
}

func TestUpdateOfSomeComponentsAndServicesUpdatesOnlyThose(t *testing.T) {
	f := fakes{"node1": {generation: 1}, "node2": {generation: 1}}
	cl, err := orchestrate.Preflight(t.Context(), inOrder(f, "node1", "node2"), 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	some := func(name string) (orchestrate.NodeRelease, error) {
		all, _ := releaseOf(name)
		return orchestrate.NodeRelease{
			Components: map[string]orchestrate.Component{"updater": all.Components["updater"]},
			Services:   all.Services[1:],
		}, nil
	}
	var told []string
	if err := cl.Update(cl.Context(), some, func(s orchestrate.Step) { told = append(told, s.Node+" "+s.Component) }); err != nil {
		t.Fatal(err)
	}
	want := []string{"node1 the updater", "node2 the updater", "node1 the proxy service", "node2 the proxy service"}
	if !slices.Equal(told, want) {
		t.Errorf("the update told %q, want %q", told, want)
	}
	for _, name := range []string{"node1", "node2"} {
		var got []string
		for _, u := range f[name].componentUpdates("") {
			got = append(got, u.component)
		}
		if want := []string{"updater", "service proxy"}; !slices.Equal(got, want) {
			t.Errorf("%s was updated to %q, want %q", name, got, want)
		}
	}
}

// What each node is to install is known before any step, so a node that
// has none, as one added to the cluster directory after the release was
// read, stops the update before it changes anything.
func TestUpdateGetsEveryNodesReleaseBeforeAnyStep(t *testing.T) {
	f := fakes{"node1": {generation: 1}, "node2": {generation: 1}}
	cl, err := orchestrate.Preflight(t.Context(), inOrder(f, "node1", "node2"), 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	err = cl.Update(cl.Context(), func(name string) (orchestrate.NodeRelease, error) {
		if name == "node2" {
			return orchestrate.NodeRelease{}, errors.New("no build for it")
		}
		return releaseOf(name)
	}, func(orchestrate.Step) {})
	var nodeErr *orchestrate.NodeError
	if !errors.As(err, &nodeErr) || nodeErr.Node != "node2" {
		t.Fatalf("the update returned %v, want a NodeError naming node2", err)
	}
	if u := f["node1"].componentUpdates(""); len(u) > 0 {
		t.Errorf("node1 was updated to %v, before the update found node2 has nothing to install", u)
	}
}

// An update runs each cluster service as the container the release
// says, with its properties, whatever they are, for the operation's
// lease, which the node refuses any other, as it does the images'
// imports.
func TestUpdateRunsEachServiceAsItsContainerForTheOperationsLease(t *testing.T) {
	f := fakes{"node1": &node{generation: 1}}
	cl, err := orchestrate.Preflight(t.Context(), inOrder(f, "node1"), 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	user := "1000:1000"
	withProperties := func(name string) (orchestrate.NodeRelease, error) {
		r, err := releaseOf(name)
		r.Services[0].Container.User, r.Services[0].Container.Capabilities = &user, &nodev1.Capabilities{Names: []string{"CAP_NET_BIND_SERVICE"}}
		r.Services[1].Container.ReadOnlyRoot = true
		return r, err
	}
	if err := cl.Update(cl.Context(), withProperties, func(orchestrate.Step) {}); err != nil {
		t.Fatal(err)
	}
	want := []*nodev1.Container{
		{Name: "service-dns", Image: "moostack.local/dns:2.0.0", User: &user, Capabilities: &nodev1.Capabilities{Names: []string{"CAP_NET_BIND_SERVICE"}}, Api: "moostack.dns.v1.DNS"},
		{Name: "service-proxy", Image: "moostack.local/proxy:2.0.0", ReadOnlyRoot: true, Api: "moostack.proxy.v1.Proxy"},
	}
	got := f["node1"].containersRun()
	if !slices.EqualFunc(got, want, func(a, b *nodev1.Container) bool { return proto.Equal(a, b) }) {
		t.Errorf("the update ran %v, want %v", got, want)
	}
}

// underOldNames are the dns and proxy services at 1.0.0, healthy, as a
// release before the service- names ran them.
func underOldNames() []*nodev1.Component {
	return []*nodev1.Component{
		{Name: "dns", Version: "1.0.0", Health: nodev1.Health_HEALTH_HEALTHY},
		{Name: "proxy", Version: "1.0.0", Health: nodev1.Health_HEALTH_HEALTHY},
	}
}

// updatesTold are the service updates and the removals that node n
// had, as "<what> <version>".
func (n *node) updatesTold() []string {
	var got []string
	for _, u := range n.componentUpdates("") {
		if strings.HasPrefix(u.component, "service ") || strings.HasPrefix(u.component, "remove ") {
			got = append(got, strings.TrimSpace(u.component+" "+u.version))
		}
	}
	return got
}

// containerNames are the names of the containers node n reports.
func (n *node) containerNames() []string {
	var names []string
	for _, c := range n.containers() {
		names = append(names, c.Name)
	}
	return names
}

// An update of a node that runs the services under their names alone
// removes each one's old container before it runs it under its
// service- name, since no two containers serve one API, and prints each
// as a service step; a rerun changes nothing.
func TestUpdateRenamesEachServiceRemovingItsOldContainerFirst(t *testing.T) {
	f := fakes{"node1": &node{generation: 1, oldNames: underOldNames()}}
	cl, err := orchestrate.Preflight(t.Context(), inOrder(f, "node1"), 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	var told []string
	tell := func(s orchestrate.Step) {
		if strings.HasSuffix(s.Component, " service") {
			told = append(told, fmt.Sprintf("%s %s %s %v", s.Node, s.Component, s.Version, s.Already))
		}
	}
	if err := cl.Update(cl.Context(), releaseOf, tell); err != nil {
		t.Fatal(err)
	}
	if got, want := f["node1"].updatesTold(), []string{"remove dns", "service dns 2.0.0", "remove proxy", "service proxy 2.0.0"}; !slices.Equal(got, want) {
		t.Errorf("node1 had %q, want %q", got, want)
	}
	if got, want := f["node1"].containerNames(), []string{"service-dns", "service-proxy"}; !slices.Equal(got, want) {
		t.Errorf("node1 runs %q, want %q", got, want)
	}
	if err := cl.Update(cl.Context(), releaseOf, tell); err != nil {
		t.Fatal(err)
	}
	if got := len(f["node1"].updatesTold()); got != 4 {
		t.Errorf("the rerun changed node1: %q", f["node1"].updatesTold())
	}
	want := []string{
		"node1 the dns service 2.0.0 false", "node1 the proxy service 2.0.0 false",
		"node1 the dns service 2.0.0 true", "node1 the proxy service 2.0.0 true",
	}
	if !slices.Equal(told, want) {
		t.Errorf("the updates told %q, want %q", told, want)
	}
}

// An update stopped partway through the rename, at each of its steps,
// leaves a state that a rerun finishes, and a third run changes nothing.
func TestARerunFinishesARenameStoppedAtAnyStep(t *testing.T) {
	for _, failing := range []string{"import dns", "remove dns", "service dns", "import proxy", "remove proxy", "service proxy"} {
		t.Run(failing, func(t *testing.T) {
			n := &node{generation: 1, oldNames: underOldNames(), failing: failing, updateErr: errors.New("the node crashed")}
			f := fakes{"node1": n}
			cl, err := orchestrate.Preflight(t.Context(), inOrder(f, "node1"), 1, false, holder)
			if err != nil {
				t.Fatal(err)
			}
			defer cl.Close()
			if err := update(cl); err == nil {
				t.Fatal("the update succeeded with the node crashing")
			}
			// An image that won't import leaves its service's old container
			// running.
			if service, ok := strings.CutPrefix(failing, "import "); ok && !slices.Contains(n.containerNames(), service) {
				t.Errorf("after its import failed, node1 runs %q, want %s still", n.containerNames(), service)
			}
			n.failing, n.updateErr = "", nil
			if err := update(cl); err != nil {
				t.Fatalf("the rerun: %v", err)
			}
			if got, want := n.containerNames(), []string{"service-dns", "service-proxy"}; !slices.Equal(got, want) {
				t.Errorf("node1 runs %q, want %q", got, want)
			}
			before := len(n.updatesTold())
			if err := update(cl); err != nil {
				t.Fatalf("the third run: %v", err)
			}
			if got := n.updatesTold(); len(got) != before {
				t.Errorf("the third run changed node1: %q", got[before:])
			}
		})
	}
}

// An import, a removal or a run of a service for a lease that doesn't
// hold the node's lock, as once another operation took it, is refused,
// and the update stops.
func TestAServiceStepForALeaseTheNodeDoesntHoldIsRefused(t *testing.T) {
	n := &node{generation: 1, oldNames: underOldNames()}
	f := fakes{"node1": n}
	cl, err := orchestrate.Preflight(t.Context(), inOrder(f, "node1"), 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	n.lockFor("theirs", time.Hour)
	err = cl.Update(cl.Context(), func(name string) (orchestrate.NodeRelease, error) {
		r, err := releaseOf(name)
		return orchestrate.NodeRelease{Services: r.Services}, err
	}, func(orchestrate.Step) {})
	wantContains(t, "the error", fmt.Sprint(err), "node1", "the operation's lease doesn't hold the node's operation lock")
	if got := n.updatesTold(); len(got) > 0 {
		t.Errorf("node1 had %q for another operation's lease", got)
	}
	if got, want := n.containerNames(), []string{"dns", "proxy"}; !slices.Equal(got, want) {
		t.Errorf("node1 runs %q, want %q", got, want)
	}
}

// A rename whose old container runs the image the update runs the
// service from, as at the same version, runs the service: the removal
// deletes the old container's image, which the update imports again.
func TestARenameAtTheTargetVersionRunsTheService(t *testing.T) {
	n := &node{generation: 1, oldNames: []*nodev1.Component{
		{Name: "dns", Version: "2.0.0", Health: nodev1.Health_HEALTH_HEALTHY},
		{Name: "proxy", Version: "2.0.0", Health: nodev1.Health_HEALTH_HEALTHY},
	}}
	cl, err := orchestrate.Preflight(t.Context(), inOrder(fakes{"node1": n}, "node1"), 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if err := update(cl); err != nil {
		t.Fatal(err)
	}
	if got, want := n.containerNames(), []string{"service-dns", "service-proxy"}; !slices.Equal(got, want) {
		t.Errorf("node1 runs %q, want %q", got, want)
	}
}

// The fake node refuses an import, a removal and a run for a lease that
// doesn't hold its lock, a run from an image it lacks, and a run while
// the service runs under its name alone, and its removal deletes the
// image no other container runs from, as the agent does, so that the
// update's tests fail when the update calls the node as the agent would
// refuse.
func TestTheFakeNodeRefusesAsTheAgentDoes(t *testing.T) {
	ctx := t.Context()
	fresh := func() *node {
		n := &node{oldNames: []*nodev1.Component{{Name: "dns", Version: "1.0.0", Health: nodev1.Health_HEALTH_HEALTHY}}}
		n.lockFor("ours", time.Hour)
		return n
	}
	dns := &nodev1.Container{Name: "service-dns", Image: "moostack.local/dns:1.0.0"}
	// A call whose context ended is never sent; a removal under way goes
	// on, its caller told the call ended.
	ended, cancel := context.WithCancel(ctx)
	cancel()
	n := fresh()
	if _, err := n.ImportImage(ended, []byte("image moostack.local/dns:1.0.0"), "ours"); err == nil {
		t.Error("the fake imported for an ended call")
	}
	if err := n.Remove(ended, "dns", "ours"); err == nil || !slices.Contains(n.containerNames(), "dns") {
		t.Errorf("the fake removed dns for an ended call: %v", err)
	}
	ending, stop := context.WithCancel(ctx)
	n.onUpdateOf = map[string]func(){"remove dns": stop}
	if err := n.Remove(ending, "dns", "ours"); err == nil || slices.Contains(n.containerNames(), "dns") {
		t.Errorf("the fake's removal whose call ended meanwhile: %v, with %q left; want it told the call ended, and dns removed", err, n.containerNames())
	}
	n = fresh()
	if _, err := n.ImportImage(ctx, []byte("image moostack.local/dns:1.0.0"), "theirs"); err == nil {
		t.Error("the fake imported for another lease")
	}
	if err := n.Remove(ctx, "dns", "theirs"); err == nil {
		t.Error("the fake removed for another lease")
	}
	if _, err := n.ImportImage(ctx, []byte("image moostack.local/dns:1.0.0"), "ours"); err != nil {
		t.Fatal(err)
	}
	if _, err := n.Run(ctx, dns, "ours"); err == nil {
		t.Error("the fake ran service-dns while dns runs")
	}
	if err := n.Remove(ctx, "dns", "ours"); err != nil {
		t.Fatal(err)
	}
	if _, err := n.Run(ctx, dns, "ours"); err == nil {
		t.Error("the fake ran service-dns from the image the removal of dns deleted")
	}
	if _, err := n.ImportImage(ctx, []byte("image moostack.local/dns:1.0.0"), "ours"); err != nil {
		t.Fatal(err)
	}
	if _, err := n.Run(ctx, dns, "theirs"); err == nil {
		t.Error("the fake ran for another lease")
	}
	if _, err := n.Run(ended, dns, "ours"); err == nil {
		t.Error("the fake ran for an ended call")
	}
	if _, err := n.Run(ctx, dns, "ours"); err != nil {
		t.Errorf("the fake refused service-dns, its image imported again: %v", err)
	}
}

// mooctl stopped once a service's old container is removed still sends
// the run, so that the node runs the service, as mooctl says the update
// goes on on the node, rather than neither container.
func TestARenameStoppedOnceTheOldContainerIsGoneStillRunsTheService(t *testing.T) {
	n := &node{generation: 1, oldNames: underOldNames()}
	cl, err := orchestrate.Preflight(t.Context(), inOrder(fakes{"node1": n}, "node1"), 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, stop := context.WithCancel(cl.Context())
	defer stop()
	n.onUpdateOf = map[string]func(){"remove dns": stop}
	// The update stops at the dns service's wait, or later, as the fake
	// answers at once.
	if err := cl.Update(ctx, releaseOf, func(orchestrate.Step) {}); err != nil && !strings.Contains(err.Error(), "goes on on the node") {
		t.Errorf("the stopped update says %v", err)
	}
	if got := n.containerNames(); !slices.Contains(got, "service-dns") {
		t.Errorf("node1 runs %q once mooctl stopped after removing dns, want service-dns", got)
	}
}

// mooctl stopped once a service's old container is removed, whose import
// of the service's image then fails, says why, rather than that the
// update goes on on the node, which runs neither container.
func TestARenameStoppedThatFailsAfterwardsSaysWhy(t *testing.T) {
	n := &node{generation: 1, oldNames: underOldNames()}
	cl, err := orchestrate.Preflight(t.Context(), inOrder(fakes{"node1": n}, "node1"), 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, stop := context.WithCancel(cl.Context())
	defer stop()
	n.onUpdateOf = map[string]func(){"remove dns": func() {
		stop()
		n.failing, n.updateErr = "import dns", errors.New("no space left on device")
	}}
	err = cl.Update(ctx, releaseOf, func(orchestrate.Step) {})
	wantContains(t, "the error", fmt.Sprint(err), "node1: updating the dns service to 2.0.0: no space left on device")
	if strings.Contains(fmt.Sprint(err), "goes on on the node") {
		t.Errorf("the error says the update goes on on the node, which runs %q: %v", n.containerNames(), err)
	}
}

// The lock lost while the update waits for a service to become healthy
// is said, rather than that the service didn't become healthy.
func TestALockLostDuringTheServicesWaitIsSaid(t *testing.T) {
	n := &node{generation: 1, sickService: "dns"}
	n.onUpdateOf = map[string]func(){"service dns": func() { n.BreakLock(context.Background()) }}
	nodes := inOrder(fakes{"node1": n}, "node1")
	nodes.LeaseTTL = 600 * time.Millisecond
	nodes.ServiceTimeout = 30 * time.Second
	cl, err := orchestrate.Preflight(t.Context(), nodes, 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	err = update(cl)
	wantContains(t, "the error", fmt.Sprint(err), "lost the operation lock")
	if strings.Contains(fmt.Sprint(err), "didn't become healthy") {
		t.Errorf("the error blames the service: %v", err)
	}
}

// A rename's step keeps its deadline once the old container goes: the
// removal, the import and the run after it get what is left of the step's
// time, not a time of their own.
func TestARenamesStepKeepsItsDeadline(t *testing.T) {
	defer orchestrate.SetUpdateTimeout(400 * time.Millisecond)()
	// The step's import, removal, import and run take 500ms in all, the
	// part after the first import 350ms.
	n := &node{generation: 1, oldNames: underOldNames(), updateDelay: 100 * time.Millisecond, importDelay: 150 * time.Millisecond}
	cl, err := orchestrate.Preflight(t.Context(), inOrder(fakes{"node1": n}, "node1"), 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	start := time.Now()
	err = update(cl)
	wantContains(t, "the error", fmt.Sprint(err), "node1: updating the dns service to 2.0.0")
	if took := time.Since(start); took > 2*time.Second {
		t.Errorf("the update took %s, with a step's time of 400ms", took)
	}
	if got := n.updatesTold(); slices.Contains(got, "service dns 2.0.0") {
		t.Errorf("node1 ran dns, its step taking 500ms of its 400ms: %q", got)
	}
}

// A service's wait cut short by the step's own time says so, rather than
// naming the service's time, which didn't run out.
func TestAServicesWaitCutShortByTheStepsTimeSaysSo(t *testing.T) {
	defer orchestrate.SetUpdateTimeout(400 * time.Millisecond)()
	n := &node{generation: 1, sickService: "dns"}
	nodes := inOrder(fakes{"node1": n}, "node1")
	nodes.ServiceTimeout = 30 * time.Second
	cl, err := orchestrate.Preflight(t.Context(), nodes, 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	err = update(cl)
	wantContains(t, "the error", fmt.Sprint(err), "node1: updating the dns service to 2.0.0: it didn't become healthy within the step's 400ms")
}

// mooctl stopped while it waits for a service to become healthy says the
// update goes on on the node, rather than that the service didn't.
func TestAnUpdateStoppedDuringAServicesWaitSaysItGoesOn(t *testing.T) {
	n := &node{generation: 1, sickService: "dns"}
	nodes := inOrder(fakes{"node1": n}, "node1")
	nodes.ServiceTimeout = 30 * time.Second
	cl, err := orchestrate.Preflight(t.Context(), nodes, 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithCancel(cl.Context())
	defer cancel()
	n.onUpdateOf = map[string]func(){"service dns": func() { time.AfterFunc(300*time.Millisecond, cancel) }}
	err = cl.Update(ctx, releaseOf, func(orchestrate.Step) {})
	wantContains(t, "the error", fmt.Sprint(err), "node1: mooctl stopped waiting for the update of the dns service to 2.0.0, which goes on on the node")
}

// A container under a service's name alone that runs another image, as
// another holder of the lock may have run, isn't the service's: the
// update leaves it running, while it renames the service's own.
func TestARenameRemovesOnlyTheServicesOwnContainer(t *testing.T) {
	foreign := &nodev1.ContainerStatus{Name: "proxy", Image: "docker.io/library/proxy:1", State: nodev1.TaskState_TASK_STATE_RUNNING}
	n := &node{generation: 1, oldNames: underOldNames()[:1], others: []*nodev1.ContainerStatus{foreign}}
	cl, err := orchestrate.Preflight(t.Context(), inOrder(fakes{"node1": n}, "node1"), 1, false, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if err := update(cl); err != nil {
		t.Fatal(err)
	}
	got := n.containerNames()
	slices.Sort(got)
	if want := []string{"proxy", "service-dns", "service-proxy"}; !slices.Equal(got, want) {
		t.Errorf("node1 runs %q, want %q", got, want)
	}
	if u := n.updatesTold(); slices.Contains(u, "remove proxy") {
		t.Errorf("node1 had %q, with the proxy container not the service's", u)
	}
}
