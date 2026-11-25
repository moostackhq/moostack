package orchestratortest_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/netip"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/moostackhq/moostack/internal/model"
	"github.com/moostackhq/moostack/internal/orchestrator"
	"github.com/moostackhq/moostack/internal/orchestrator/orchestratortest"
)

func TestTheFakeKeepsThePortsPromises(t *testing.T) {
	f := cluster(t)
	before := state(t, f)
	hosts := 0
	orchestratortest.Contract(t, func(t *testing.T) orchestratortest.Cluster {
		return orchestratortest.Cluster{
			Orchestrator: f,
			Host: func(*testing.T) orchestrator.Host {
				hosts++
				return orchestratortest.Host{}
			},
		}
	})
	if hosts == 0 {
		t.Error("the contract made no node")
	}
	f.RunConsole(context.Background(), "example.test/console:1", nil)
	if after := state(t, f); after != before {
		t.Errorf("the contract left other apps or nodes than it found:\n from %s\n   to %s", before, after)
	}
}

func TestWithoutHostsTheContractSkipsWhatItTestsOfNodesAndPasses(t *testing.T) {
	orchestratortest.Contract(t, func(t *testing.T) orchestratortest.Cluster {
		return orchestratortest.Cluster{Orchestrator: &orchestratortest.Fake{}}
	})
}

// calls has a call of each method of the port that returns an error,
// on a fake that cluster made.
func calls(ctx context.Context) map[string]func(f *orchestratortest.Fake) error {
	host := orchestratortest.Host{}
	return map[string]func(f *orchestratortest.Fake) error{
		"Check": func(f *orchestratortest.Fake) error { return f.Check(model.App{Name: "shop"}) },
		"Deploy": func(f *orchestratortest.Fake) error {
			return f.Deploy(ctx, model.App{Name: "new"}, func(orchestrator.Step) {})
		},
		"Certificate": func(f *orchestratortest.Fake) error { return f.Certificate(ctx, "shop", "web") },
		"Remove":      func(f *orchestratortest.Fake) error { return f.Remove(ctx, "shop") },
		"Logs": func(f *orchestratortest.Fake) error {
			return f.Logs(ctx, "shop", orchestrator.LogOptions{}, &bytes.Buffer{})
		},
		"Apps":    func(f *orchestratortest.Fake) error { _, err := f.Apps(ctx); return err },
		"Running": func(f *orchestratortest.Fake) error { _, err := f.Running(ctx); return err },
		"Preflight": func(f *orchestratortest.Fake) error {
			_, err := f.Preflight(ctx, host, orchestrator.NodeRequest{Name: "node-9"})
			return err
		},
		"AddNode": func(f *orchestratortest.Fake) error {
			return f.AddNode(ctx, host, model.Node{Name: "node-9"}, func(orchestrator.Step) {})
		},
		"RemoveNode": func(f *orchestratortest.Fake) error {
			return f.RemoveNode(ctx, "node-1", host, orchestrator.RemoveOptions{}, func(orchestrator.Step) {})
		},
		"Uninstall":    func(f *orchestratortest.Fake) error { return f.Uninstall(ctx, host, model.Node{Name: "node-1"}) },
		"Nodes":        func(f *orchestratortest.Fake) error { _, err := f.Nodes(ctx); return err },
		"RunConsole":   func(f *orchestratortest.Fake) error { return f.RunConsole(ctx, "example.test/console:2", nil) },
		"ConsoleToken": func(f *orchestratortest.Fake) error { _, err := f.ConsoleToken(ctx); return err },
	}
}

// cluster is a fake with an app, a node and the console.
func cluster(t *testing.T) *orchestratortest.Fake {
	t.Helper()
	ctx := context.Background()
	f := &orchestratortest.Fake{}
	shop := model.App{Name: "shop", Containers: []model.Container{{Name: "web", Image: "example.test/web:1", Host: "shop.example.test"}}}
	if err := errors.Join(
		f.Deploy(ctx, shop, func(orchestrator.Step) {}),
		f.AddNode(ctx, orchestratortest.Host{}, model.Node{Name: "node-1"}, func(orchestrator.Step) {}),
		f.RunConsole(ctx, "example.test/console:1", nil),
	); err != nil {
		t.Fatal(err)
	}
	return f
}

// state is what the fake's Apps, Running and Nodes return.
func state(t *testing.T, f *orchestratortest.Fake) string {
	t.Helper()
	ctx := context.Background()
	apps, err1 := f.Apps(ctx)
	running, err2 := f.Running(ctx)
	nodes, err3 := f.Nodes(ctx)
	if err := errors.Join(err1, err2, err3); err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%+v %+v %+v", apps, running, nodes)
}

func TestAnyMethodOfTheFakeCanBeMadeToFail(t *testing.T) {
	calls := calls(context.Background())
	port := reflect.TypeFor[orchestrator.Orchestrator]()
	for i := range port.NumMethod() {
		if name := port.Method(i).Name; calls[name] == nil && name != "Can" {
			t.Errorf("no call of %s in this test", name)
		}
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			f := cluster(t)
			before := state(t, f)
			if err := call(f); err != nil {
				t.Fatalf("%s before a failure is set: %v", name, err)
			}
			f = cluster(t)
			broken := errors.New("broken")
			f.Fail(name, broken)
			if err := call(f); !errors.Is(err, broken) {
				t.Errorf("%s: %v, want the failure set", name, err)
			}
			f.Fail(name, nil)
			if after := state(t, f); after != before {
				t.Errorf("the failed %s changed the cluster:\n from %s\n   to %s", name, before, after)
			}
			if err := call(f); err != nil {
				t.Errorf("%s once the failure is unset: %v", name, err)
			}
		})
	}
}

func TestACallOfTheFakeOnAnEndedContextFailsWithItsCauseAndChangesNothing(t *testing.T) {
	interrupted := errors.New("interrupted")
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(interrupted)
	for name, call := range calls(ctx) {
		if name == "Check" {
			continue
		}
		f := cluster(t)
		before := state(t, f)
		if err := call(f); !errors.Is(err, interrupted) {
			t.Errorf("%s on an ended context: %v, want the context's cause", name, err)
		}
		if after := state(t, f); after != before {
			t.Errorf("%s on an ended context changed the cluster:\n from %s\n   to %s", name, before, after)
		}
	}
	told := false
	cluster(t).Deploy(ctx, model.App{Name: "new"}, func(orchestrator.Step) { told = true })
	if told {
		t.Error("a deploy on an ended context told a step")
	}
}

func TestADeployOfTheFakeWhoseContextEndsAtItsFirstStepFailsAndDeploysNothing(t *testing.T) {
	interrupted := errors.New("interrupted")
	ctx, cancel := context.WithCancelCause(context.Background())
	f := cluster(t)
	before := state(t, f)
	var steps []orchestrator.Step
	err := f.Deploy(ctx, model.App{Name: "new"}, func(s orchestrator.Step) {
		steps = append(steps, s)
		cancel(interrupted)
	})
	if !errors.Is(err, interrupted) || !slices.Equal(steps, []orchestrator.Step{orchestrator.Applying}) {
		t.Errorf("the deploy returned %v after the steps %v, want the context's cause after Applying alone", err, steps)
	}
	if after := state(t, f); after != before {
		t.Errorf("the deploy changed the cluster:\n from %s\n   to %s", before, after)
	}
}

// TestCallsOfTheFakeAtOnceLeaveWhatSomeOrderOfThemWould runs writers of
// apps and nodes of their own beside readers.
func TestCallsOfTheFakeAtOnceLeaveWhatSomeOrderOfThemWould(t *testing.T) {
	ctx := context.Background()
	f := &orchestratortest.Fake{Own: []model.Running{{Replica: "dns-1", Container: "dns", Image: "example.test/dns:1"}}}
	const writers, rounds = 8, 50
	var wg sync.WaitGroup
	for w := range writers {
		wg.Go(func() {
			app := fmt.Sprintf("app-%d", w)
			node := fmt.Sprintf("node-%d", w)
			for i := range rounds {
				deployed := model.App{Name: app, Containers: []model.Container{{Name: "web", Image: fmt.Sprintf("example.test/web:%d", i), Host: app + ".example.test"}}}
				err := errors.Join(
					f.Deploy(ctx, deployed, func(orchestrator.Step) {}),
					f.Certificate(ctx, app, "web"),
					f.Logs(ctx, app, orchestrator.LogOptions{}, io.Discard),
					f.AddNode(ctx, orchestratortest.Host{}, model.Node{Name: node}, func(orchestrator.Step) {}),
					f.RunConsole(ctx, "example.test/console:1", nil),
				)
				if i < rounds-1 {
					err = errors.Join(err, f.Remove(ctx, app), f.RemoveNode(ctx, node, nil, orchestrator.RemoveOptions{}, func(orchestrator.Step) {}))
				}
				if err != nil {
					t.Error(err)
					return
				}
			}
		})
		wg.Go(func() {
			for range rounds {
				_, err1 := f.Apps(ctx)
				_, err2 := f.Running(ctx)
				_, err3 := f.Nodes(ctx)
				_, err4 := f.ConsoleToken(ctx)
				if err := errors.Join(err1, err2, err3); err != nil {
					t.Error(err)
				}
				_ = err4
				f.Fail("Uninstall", errors.New("broken"))
			}
		})
	}
	wg.Wait()
	apps, _ := f.Apps(ctx)
	nodes, _ := f.Nodes(ctx)
	if len(apps) != writers || len(nodes) != writers {
		t.Fatalf("%d apps and %d nodes are left, want %d of each", len(apps), len(nodes), writers)
	}
	for w, a := range apps {
		if want := fmt.Sprintf("example.test/web:%d", rounds-1); a.Name != fmt.Sprintf("app-%d", w) || !slices.Equal(a.Images, []string{want}) {
			t.Errorf("app %d is %+v, want app-%d of %s", w, a, w, want)
		}
	}
}

func TestAStepAndASlowWriterDontHoldTheFake(t *testing.T) {
	ctx := context.Background()
	f := &orchestratortest.Fake{}
	var during []int
	err := f.Deploy(ctx, model.App{Name: "shop", Containers: []model.Container{{Name: "web"}}}, func(orchestrator.Step) {
		apps, err := f.Apps(ctx)
		if err != nil {
			t.Error(err)
		}
		during = append(during, len(apps))
	})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(during, []int{0, 1}) {
		t.Errorf("the cluster had %v apps at the two steps, want none when applying and the app when waiting", during)
	}

	var wrote string
	err = f.Logs(ctx, "shop", orchestrator.LogOptions{}, writerFunc(func(p []byte) (int, error) {
		if _, err := f.Running(ctx); err != nil {
			t.Error(err)
		}
		wrote += string(p)
		return len(p), nil
	}))
	if err != nil || wrote != "web-1 web: running\n" {
		t.Errorf("Logs wrote %q, %v", wrote, err)
	}
}

type writerFunc func(p []byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

func TestAFailedDeployTellsNoStep(t *testing.T) {
	f := &orchestratortest.Fake{}
	f.Fail("Deploy", errors.New("broken"))
	f.Deploy(context.Background(), model.App{Name: "shop"}, func(s orchestrator.Step) {
		t.Errorf("a failed deploy told the step %d", s)
	})
}

func TestFailPanicsForWhatIsNoMethodThatFails(t *testing.T) {
	for _, name := range []string{"Deplyo", "", "Can", "Fail"} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("Fail(%q) didn't panic", name)
				}
			}()
			(&orchestratortest.Fake{}).Fail(name, errors.New("broken"))
		}()
	}
}

func TestTheFakeListsItsOwnContainersAndWritesTheLogsSet(t *testing.T) {
	ctx := context.Background()
	own := model.Running{Replica: "dns-1", Container: "dns", Image: "example.test/dns:1"}
	f := &orchestratortest.Fake{Own: []model.Running{own}, Logged: map[string]string{"shop": "web-1 web: started\n"}}
	if err := f.Deploy(ctx, model.App{Name: "shop", Containers: []model.Container{{Name: "web", Image: "example.test/web:1", Replicas: 2}}}, func(orchestrator.Step) {}); err != nil {
		t.Fatal(err)
	}
	got, err := f.Running(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := []model.Running{
		own,
		{App: "shop", Replica: "web-1", Container: "web", Image: "example.test/web:1"},
		{App: "shop", Replica: "web-2", Container: "web", Image: "example.test/web:1"},
	}
	if !slices.Equal(got, want) {
		t.Errorf("Running:\n got %+v\nwant %+v", got, want)
	}
	var w bytes.Buffer
	if err := f.Logs(ctx, "shop", orchestrator.LogOptions{}, &w); err != nil || w.String() != "web-1 web: started\n" {
		t.Errorf("Logs wrote %q, %v, want the line set", w.String(), err)
	}
	if err := f.Logs(ctx, "blog", orchestrator.LogOptions{}, &w); !errors.Is(err, orchestrator.ErrNoApp) {
		t.Errorf("Logs of an app the fake doesn't have: %v, want ErrNoApp", err)
	}
	if len(f.Own) != 1 || f.Own[0] != own {
		t.Errorf("Running changed Own: %+v", f.Own)
	}
}

func TestTheFakeWritesALineForEachRunningContainerOfTheApp(t *testing.T) {
	ctx := context.Background()
	f := &orchestratortest.Fake{}
	for _, app := range []string{"shop", "blog"} {
		if err := f.Deploy(ctx, model.App{Name: app, Containers: []model.Container{{Name: "web", Replicas: 2}, {Name: "worker"}}}, func(orchestrator.Step) {}); err != nil {
			t.Fatal(err)
		}
	}
	var w bytes.Buffer
	if err := f.Logs(ctx, "shop", orchestrator.LogOptions{}, &w); err != nil {
		t.Fatal(err)
	}
	if want := "web-1 web: running\nweb-2 web: running\nworker-1 worker: running\n"; w.String() != want {
		t.Errorf("Logs wrote %q, want %q", w.String(), want)
	}
	if err := f.Deploy(ctx, model.App{Name: "charted", Chart: "chart"}, func(orchestrator.Step) {}); err != nil {
		t.Fatal(err)
	}
	if err := f.Logs(ctx, "charted", orchestrator.LogOptions{}, &w); !errors.Is(err, orchestrator.ErrNoReplicas) {
		t.Errorf("Logs of an app without containers: %v, want ErrNoReplicas", err)
	}
}

func TestTheFakeOrdersContainersOfOneReplicaByName(t *testing.T) {
	f := &orchestratortest.Fake{Own: []model.Running{
		{Replica: "proxy-2", Container: "a"},
		{Replica: "proxy-1", Container: "web"},
		{Replica: "proxy-1", Container: "sidecar"},
		{Replica: "dns-1", Container: "z"},
	}}
	got, err := f.Running(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := []model.Running{
		{Replica: "dns-1", Container: "z"},
		{Replica: "proxy-1", Container: "sidecar"},
		{Replica: "proxy-1", Container: "web"},
		{Replica: "proxy-2", Container: "a"},
	}
	if !slices.Equal(got, want) {
		t.Errorf("Running:\n got %+v\nwant %+v", got, want)
	}
	if f.Own[0].Replica != "proxy-2" || f.Own[3].Replica != "dns-1" {
		t.Errorf("Running ordered Own itself: %+v", f.Own)
	}
}

func TestTheFakesAppHasEachImageOnceItsOwnFirstAndOneReplicaOfAContainerThatDoesntSay(t *testing.T) {
	ctx := context.Background()
	f := &orchestratortest.Fake{}
	app := model.App{Name: "shop", Manifests: "deploy", Images: map[string]string{"shop": "example.test/shop:1"}, Containers: []model.Container{
		{Name: "web", Image: "example.test/web:1", Host: "shop.example.test"},
		{Name: "admin", Image: "example.test/web:1", Host: "admin.example.test", Replicas: 2},
		{Name: "worker", Image: "example.test/worker:1"},
	}}
	if err := f.Deploy(ctx, app, func(orchestrator.Step) {}); err != nil {
		t.Fatal(err)
	}
	apps, err := f.Apps(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := model.AppState{
		Name: "shop", Ready: 4, Want: 4,
		Images: []string{"example.test/shop:1", "example.test/web:1", "example.test/worker:1"},
		Hosts:  []string{"shop.example.test", "admin.example.test"},
	}
	if len(apps) != 1 || !reflect.DeepEqual(apps[0], want) {
		t.Errorf("Apps: %+v, want %+v", apps, want)
	}
	running, err := f.Running(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(running) != 4 {
		t.Errorf("%d containers run, want 4: %+v", len(running), running)
	}
}

func TestTheFakeAndItsHostSayWhatTheyCanDo(t *testing.T) {
	if got := (&orchestratortest.Fake{}).Can(); got != (orchestrator.Capabilities{KubernetesObjects: true}) {
		t.Errorf("Can: %+v, want an app's own manifests or chart", got)
	}
	ctx := context.Background()
	host := orchestratortest.Host{}
	_, run := host.Run(ctx, "true", nil)
	_, stoppable := host.RunStoppable(ctx, "true")
	_, dial := host.DialTCP(ctx, "127.0.0.1:1")
	for name, err := range map[string]error{"Run": run, "RunStoppable": stoppable, "Upload": host.Upload(ctx, "/tmp/file", strings.NewReader(""), 0o600), "DialTCP": dial} {
		if err == nil {
			t.Errorf("%s on the fake's host succeeded", name)
		}
	}
}

func TestTheFakeRefusesWhatItCantDo(t *testing.T) {
	ctx := context.Background()
	f := &orchestratortest.Fake{}
	if _, err := f.ConsoleToken(ctx); err == nil {
		t.Error("ConsoleToken before RunConsole succeeded")
	}
	if err := f.Deploy(ctx, model.App{Name: "shop", Containers: []model.Container{{Name: "web", Host: "shop.example.test"}, {Name: "worker"}}}, func(orchestrator.Step) {}); err != nil {
		t.Fatal(err)
	}
	if err := f.Certificate(ctx, "blog", "web"); !errors.Is(err, orchestrator.ErrNoApp) {
		t.Errorf("Certificate of an app the fake doesn't have: %v, want ErrNoApp", err)
	}
	for _, container := range []string{"worker", "cron"} {
		if err := f.Certificate(ctx, "shop", container); err == nil {
			t.Errorf("Certificate of %s, which has no host, succeeded", container)
		}
	}
	if err := f.AddNode(ctx, orchestratortest.Host{}, model.Node{Name: "node-1"}, func(orchestrator.Step) {}); err != nil {
		t.Fatal(err)
	}
	if err := f.AddNode(ctx, orchestratortest.Host{}, model.Node{Name: "node-1"}, func(orchestrator.Step) {}); err == nil {
		t.Error("adding a node the cluster has succeeded")
	}
	if err := f.RemoveNode(ctx, "node-7", nil, orchestrator.RemoveOptions{}, func(orchestrator.Step) {}); err != nil {
		t.Errorf("removing a node the cluster doesn't have: %v", err)
	}
	nameless := model.App{Containers: []model.Container{{Name: "web"}}}
	if err := f.Check(nameless); err == nil {
		t.Error("Check accepted an app without a name")
	}
	if err := f.Deploy(ctx, nameless, func(orchestrator.Step) { t.Error("the deploy of an app without a name told a step") }); err == nil {
		t.Error("an app without a name was deployed")
	}
	if running, _ := f.Running(ctx); len(running) != 2 {
		t.Errorf("the containers running after the refusals: %+v, want shop's two alone", running)
	}
}

func TestTheFakesPreflightTakesTheAddressAskedForAndANodeWithoutOneHasNone(t *testing.T) {
	ctx := context.Background()
	f := &orchestratortest.Fake{}
	address := netip.MustParseAddr("10.9.8.7")
	n, err := f.Preflight(ctx, orchestratortest.Host{}, orchestrator.NodeRequest{Name: "node-1", PrivateAddress: address})
	if err != nil || n.PrivateAddress != address {
		t.Errorf("Preflight returned the address %s, %v, want %s", n.PrivateAddress, err, address)
	}
	if n, _ := f.Preflight(ctx, orchestratortest.Host{}, orchestrator.NodeRequest{Name: "node-0", ControlPlane: true}); !n.ControlPlane {
		t.Error("Preflight returned a node that isn't the control plane asked for")
	} else if err := f.AddNode(ctx, orchestratortest.Host{}, n, func(orchestrator.Step) {}); err != nil {
		t.Fatal(err)
	} else if nodes, _ := f.Nodes(ctx); len(nodes) != 1 || !nodes[0].ControlPlane {
		t.Errorf("the control plane added is %+v", nodes)
	} else if err := f.RemoveNode(ctx, "node-0", nil, orchestrator.RemoveOptions{}, func(orchestrator.Step) {}); err != nil {
		t.Fatal(err)
	}
	if n, _ := f.Preflight(ctx, orchestratortest.Host{}, orchestrator.NodeRequest{Name: "node-2"}); n.PrivateAddress != netip.MustParseAddr("10.0.0.1") {
		t.Errorf("Preflight without an address asked for returned %s, want 10.0.0.1", n.PrivateAddress)
	}
	if err := f.AddNode(ctx, orchestratortest.Host{}, model.Node{Name: "node-3"}, func(orchestrator.Step) {}); err != nil {
		t.Fatal(err)
	}
	if nodes, _ := f.Nodes(ctx); len(nodes) != 1 || nodes[0].Address != "" {
		t.Errorf("a node added without an address is %+v, want no address", nodes)
	}
}

func TestTheFakeKeepsTheAppAsDeployed(t *testing.T) {
	ctx := context.Background()
	f := &orchestratortest.Fake{}
	app := model.App{Name: "shop", Containers: []model.Container{{Name: "web", Image: "example.test/web:1"}}}
	if err := f.Deploy(ctx, app, func(orchestrator.Step) {}); err != nil {
		t.Fatal(err)
	}
	app.Containers[0].Image = "example.test/web:2"
	apps, err := f.Apps(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(apps[0].Images, []string{"example.test/web:1"}) {
		t.Errorf("a change to the caller's app after the deploy changed the cluster's: %q", apps[0].Images)
	}
}

// TestSequencesOfOperationsMatchAModel runs generated sequences of
// deploys, removals, and nodes added and removed, and compares what the
// fake answers after each with a model: the replicas and image of each
// app's containers, and the names of the nodes.
func TestSequencesOfOperationsMatchAModel(t *testing.T) {
	ctx := context.Background()
	type container struct {
		replicas int
		image    string
	}
	for seed := range uint64(200) {
		rng := rand.New(rand.NewPCG(seed, 2))
		f := &orchestratortest.Fake{}
		apps := map[string]map[string]container{}
		nodes := map[string]bool{}
		var history []string
		fail := func(format string, args ...any) {
			t.Helper()
			t.Fatalf("seed %d, after %s: %s", seed, strings.Join(history, ", "), fmt.Sprintf(format, args...))
		}
		for range 30 {
			app := fmt.Sprintf("app-%d", rng.IntN(3))
			node := fmt.Sprintf("node-%d", rng.IntN(3))
			switch rng.IntN(4) {
			case 0:
				deployed := model.App{Name: app}
				want := map[string]container{}
				for _, name := range []string{"web", "worker", "cron"} {
					if rng.IntN(2) == 0 {
						continue
					}
					c := container{replicas: 1 + rng.IntN(3), image: fmt.Sprintf("example.test/%s:%d", name, rng.IntN(3))}
					want[name] = c
					deployed.Containers = append(deployed.Containers, model.Container{Name: name, Image: c.image, Replicas: c.replicas})
				}
				history = append(history, fmt.Sprintf("deploy %s %v", app, want))
				if err := f.Deploy(ctx, deployed, func(orchestrator.Step) {}); err != nil {
					fail("Deploy: %v", err)
				}
				apps[app] = want
			case 1:
				history = append(history, "remove "+app)
				_, had := apps[app]
				err := f.Remove(ctx, app)
				if had && err != nil || !had && !errors.Is(err, orchestrator.ErrNoApp) {
					fail("Remove: %v, with the app there: %t", err, had)
				}
				delete(apps, app)
			case 2:
				history = append(history, "add "+node)
				err := f.AddNode(ctx, orchestratortest.Host{}, model.Node{Name: node, PrivateAddress: netip.MustParseAddr("10.0.0.2")}, func(orchestrator.Step) {})
				if (err != nil) != nodes[node] {
					fail("AddNode: %v, with the node there: %t", err, nodes[node])
				}
				nodes[node] = true
			case 3:
				history = append(history, "remove "+node)
				if err := f.RemoveNode(ctx, node, orchestratortest.Host{}, orchestrator.RemoveOptions{}, func(orchestrator.Step) {}); err != nil {
					fail("RemoveNode: %v", err)
				}
				delete(nodes, node)
			}

			states, err := f.Apps(ctx)
			if err != nil {
				fail("Apps: %v", err)
			}
			if len(states) != len(apps) {
				fail("Apps has %d apps, the model %d", len(states), len(apps))
			}
			for _, s := range states {
				want, ok := apps[s.Name]
				if !ok {
					fail("Apps has %s, which the model doesn't", s.Name)
				}
				replicas := 0
				for _, c := range want {
					replicas += c.replicas
					if !slices.Contains(s.Images, c.image) {
						fail("%s lacks the image %s: %q", s.Name, c.image, s.Images)
					}
				}
				if s.Want != replicas || s.Ready != replicas {
					fail("%s has %d of %d replicas ready, the model %d", s.Name, s.Ready, s.Want, replicas)
				}
			}
			running, err := f.Running(ctx)
			if err != nil {
				fail("Running: %v", err)
			}
			counted := map[string]int{}
			for _, r := range running {
				if c, ok := apps[r.App][r.Container]; !ok || c.image != r.Image {
					fail("Running has %+v, which the model doesn't", r)
				}
				counted[r.App+"/"+r.Container]++
			}
			for app, containers := range apps {
				for name, c := range containers {
					if counted[app+"/"+name] != c.replicas {
						fail("%d replicas of %s's %s run, the model has %d", counted[app+"/"+name], app, name, c.replicas)
					}
				}
			}
			listed, err := f.Nodes(ctx)
			if err != nil {
				fail("Nodes: %v", err)
			}
			if len(listed) != len(nodes) {
				fail("Nodes has %d nodes, the model %d", len(listed), len(nodes))
			}
			for _, n := range listed {
				if !nodes[n.Name] {
					fail("Nodes has %s, which the model doesn't", n.Name)
				}
			}
		}
	}
}

func TestTheFakeRefusesToRemoveANodeThatHoldsVolumesUnlessToldToLoseThem(t *testing.T) {
	ctx := context.Background()
	f := &orchestratortest.Fake{Volumes: map[string][]string{"node-1": {"data", "cache"}}}
	steps := func(n *[]orchestrator.Step) func(orchestrator.Step) {
		return func(s orchestrator.Step) { *n = append(*n, s) }
	}
	if err := f.AddNode(ctx, orchestratortest.Host{}, model.Node{Name: "node-1"}, func(orchestrator.Step) {}); err != nil {
		t.Fatal(err)
	}
	var told []orchestrator.Step
	err := f.RemoveNode(ctx, "node-1", orchestratortest.Host{}, orchestrator.RemoveOptions{}, steps(&told))
	var held *orchestrator.VolumesError
	if !errors.As(err, &held) || held.Node != "node-1" || !slices.Equal(held.Volumes, []string{"data", "cache"}) {
		t.Fatalf("RemoveNode of a node that holds volumes: %v, want a VolumesError of node-1's data and cache", err)
	}
	if want := "node-1 holds the volumes data, cache, which go with it"; err.Error() != want {
		t.Errorf("the refusal reads %q, want %q", err, want)
	}
	if nodes, _ := f.Nodes(ctx); len(nodes) != 1 || len(told) != 0 {
		t.Errorf("the refusal left %d nodes and told the steps %v, want the node and no step", len(nodes), told)
	}
	if err := f.RemoveNode(ctx, "node-1", orchestratortest.Host{}, orchestrator.RemoveOptions{LoseVolumes: true}, steps(&told)); err != nil {
		t.Fatalf("RemoveNode, losing volumes: %v", err)
	}
	if want := []orchestrator.Step{orchestrator.Draining, orchestrator.Uninstalling, orchestrator.Deleting}; !slices.Equal(told, want) {
		t.Errorf("the steps were %v, want Draining, Uninstalling, Deleting", told)
	}
}

func TestTheFakeTellsTheStepsOfARemovalItHasToDo(t *testing.T) {
	ctx := context.Background()
	for name, tc := range map[string]struct {
		there bool
		host  orchestrator.Host
		o     orchestrator.RemoveOptions
		want  []orchestrator.Step
	}{
		"a node whose host can't be reached":             {there: true, o: orchestrator.RemoveOptions{Unreachable: true}, want: []orchestrator.Step{orchestrator.Deleting}},
		"a node without a host":                          {there: true, want: []orchestrator.Step{orchestrator.Draining, orchestrator.Deleting}},
		"a host the cluster has no node of":              {host: orchestratortest.Host{}, want: []orchestrator.Step{orchestrator.Uninstalling}},
		"a node that holds volumes and can't be reached": {there: true, o: orchestrator.RemoveOptions{Unreachable: true}, want: []orchestrator.Step{orchestrator.Deleting}},
	} {
		t.Run(name, func(t *testing.T) {
			f := &orchestratortest.Fake{Volumes: map[string][]string{}}
			if strings.Contains(name, "volumes") {
				f.Volumes["node-1"] = []string{"data"}
			}
			if tc.there {
				if err := f.AddNode(ctx, orchestratortest.Host{}, model.Node{Name: "node-1"}, func(orchestrator.Step) {}); err != nil {
					t.Fatal(err)
				}
			}
			var told []orchestrator.Step
			if err := f.RemoveNode(ctx, "node-1", tc.host, tc.o, func(s orchestrator.Step) { told = append(told, s) }); err != nil {
				t.Fatalf("RemoveNode: %v", err)
			}
			if !slices.Equal(told, tc.want) {
				t.Errorf("the steps were %v, want %v", told, tc.want)
			}
			if nodes, _ := f.Nodes(ctx); len(nodes) != 0 {
				t.Errorf("the cluster still has %d nodes", len(nodes))
			}
		})
	}
}

func TestTheFakeTellsNoStepWhenItRefusesANode(t *testing.T) {
	ctx := context.Background()
	f := &orchestratortest.Fake{}
	var told []orchestrator.Step
	tell := func(s orchestrator.Step) { told = append(told, s) }
	f.AddNode(ctx, orchestratortest.Host{}, model.Node{Name: "node-1"}, tell)
	told = nil
	if err := f.AddNode(ctx, orchestratortest.Host{}, model.Node{Name: "node-1"}, tell); err == nil || err.Error() != "the cluster has a node node-1 already" {
		t.Fatalf("adding a node the cluster has: %v", err)
	}
	if len(told) != 0 {
		t.Errorf("the steps were %v, want none", told)
	}
}

func TestANodeOperationOfTheFakeWhoseContextEndsAtAnyStepFailsAndChangesNothing(t *testing.T) {
	cause := errors.New("the operation lock is lost")
	for _, at := range []orchestrator.Step{orchestrator.Installing, orchestrator.Starting, orchestrator.WaitingForNode} {
		f := &orchestratortest.Fake{}
		ctx, cancel := context.WithCancelCause(context.Background())
		var told []orchestrator.Step
		err := f.AddNode(ctx, orchestratortest.Host{}, model.Node{Name: "node-1"}, func(s orchestrator.Step) {
			told = append(told, s)
			if s == at {
				cancel(cause)
			}
		})
		if !errors.Is(err, cause) || len(told) == 0 || told[len(told)-1] != orchestrator.Uninstalling {
			t.Errorf("AddNode whose context ends at step %v: %v after the steps %v, want the cause, with Uninstalling told last", at, err, told)
		}
		if nodes, _ := f.Nodes(context.Background()); len(nodes) != 0 {
			t.Errorf("the cluster has %d nodes after an add cut off at step %v", len(nodes), at)
		}
	}
	for _, at := range []orchestrator.Step{orchestrator.Draining, orchestrator.Uninstalling, orchestrator.Deleting} {
		f := &orchestratortest.Fake{}
		if err := f.AddNode(context.Background(), orchestratortest.Host{}, model.Node{Name: "node-1"}, func(orchestrator.Step) {}); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancelCause(context.Background())
		var last orchestrator.Step
		err := f.RemoveNode(ctx, "node-1", orchestratortest.Host{}, orchestrator.RemoveOptions{}, func(s orchestrator.Step) {
			last = s
			if s == at {
				cancel(cause)
			}
		})
		if !errors.Is(err, cause) || last != at {
			t.Errorf("RemoveNode whose context ends at step %v: %v after the last step %v, want the cause, and no step more", at, err, last)
		}
		if nodes, _ := f.Nodes(context.Background()); len(nodes) != 1 {
			t.Errorf("the cluster has %d nodes after a removal cut off at step %v, want the node kept", len(nodes), at)
		}
	}
}
