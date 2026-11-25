package orchestratortest

import (
	"bytes"
	"cmp"
	"context"
	"crypto/rand"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/moostackhq/moostack/internal/model"
	"github.com/moostackhq/moostack/internal/orchestrator"
)

// Cluster is a cluster Contract tests an Orchestrator on.
type Cluster struct {
	Orchestrator orchestrator.Orchestrator
	// Host returns a host that can be a node of the cluster and isn't
	// one, a new host each call. If it is nil, Contract skips what it
	// tests of nodes.
	Host func(t *testing.T) orchestrator.Host
}

// Contract tests that an Orchestrator keeps what the port promises a
// command. The cluster may hold what other tests put there: every app
// and node of Contract has a name of its own, and Contract removes them
// when it ends. The console that Contract runs stays. Each call it
// makes has a minute.
func Contract(t *testing.T, cluster func(t *testing.T) Cluster) {
	unique := func(name string) string { return name + "-" + strings.ToLower(rand.Text()[:8]) }
	within := func(t *testing.T) context.Context {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		t.Cleanup(cancel)
		return ctx
	}

	// shop is an app of two containers, named name.
	shop := func(name string) model.App {
		return model.App{Name: name, Containers: []model.Container{
			{Name: "web", Image: "example.test/web:1", Port: 8080, Host: name + ".example.test", Replicas: 2},
			{Name: "worker", Image: "example.test/worker:1", Command: []string{"work"}, Replicas: 1},
		}}
	}
	// removed removes the app when the test ends, if it is there.
	removed := func(t *testing.T, o orchestrator.Orchestrator, app string) {
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			if err := o.Remove(ctx, app); err != nil && !errors.Is(err, orchestrator.ErrNoApp) {
				t.Errorf("removing %s: %v", app, err)
			}
		})
	}
	// deploy deploys app, and removes it when the test ends.
	deploy := func(t *testing.T, o orchestrator.Orchestrator, app model.App) {
		t.Helper()
		removed(t, o, app.Name)
		if err := o.Deploy(within(t), app, func(orchestrator.Step) {}); err != nil {
			t.Fatalf("Deploy: %v", err)
		}
	}
	// state is the app's state, if the cluster has the app.
	state := func(t *testing.T, o orchestrator.Orchestrator, app string) (model.AppState, bool) {
		t.Helper()
		apps, err := o.Apps(within(t))
		if err != nil {
			t.Fatalf("Apps: %v", err)
		}
		i := slices.IndexFunc(apps, func(a model.AppState) bool { return a.Name == app })
		if i < 0 {
			return model.AppState{}, false
		}
		return apps[i], true
	}
	// running are the app's running containers.
	running := func(t *testing.T, o orchestrator.Orchestrator, app string) []model.Running {
		t.Helper()
		all, err := o.Running(within(t))
		if err != nil {
			t.Fatalf("Running: %v", err)
		}
		return slices.DeleteFunc(all, func(r model.Running) bool { return r.App != app })
	}
	// containers are what runs of the app, as "container image" for each
	// replica, ordered. It fails the test for a replica without a name,
	// or with the name of another replica of the same container.
	containers := func(t *testing.T, o orchestrator.Orchestrator, app string) []string {
		t.Helper()
		var out []string
		seen := map[string]bool{}
		for _, r := range running(t, o, app) {
			if r.Instance == "" || seen[r.Instance+"/"+r.Container] {
				t.Errorf("Running has %+v, a container without a replica of its own", r)
			}
			seen[r.Instance+"/"+r.Container] = true
			out = append(out, r.Container+" "+r.Image)
		}
		slices.Sort(out)
		return out
	}
	// node makes a new host the node asked for, telling step the steps,
	// and removes it when the test ends, if the cluster still has it. It
	// skips the test on a cluster without hosts.
	node := func(t *testing.T, c Cluster, want orchestrator.NodeRequest, step func(orchestrator.Step)) (model.Node, orchestrator.Host) {
		t.Helper()
		if c.Host == nil {
			t.Skip("the cluster has no hosts to make nodes of")
		}
		host := c.Host(t)
		n, err := c.Orchestrator.Preflight(within(t), host, want)
		if err != nil {
			t.Fatalf("Preflight: %v", err)
		}
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			nodes, err := c.Orchestrator.Nodes(ctx)
			if err != nil {
				t.Errorf("Nodes: %v", err)
			}
			if !slices.ContainsFunc(nodes, func(s model.NodeState) bool { return s.Name == want.Name }) {
				return
			}
			if err := c.Orchestrator.RemoveNode(ctx, want.Name, host, orchestrator.RemoveOptions{}, func(orchestrator.Step) {}); err != nil {
				t.Errorf("removing %s: %v", want.Name, err)
			}
		})
		if err := c.Orchestrator.AddNode(within(t), host, n, step); err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		return n, host
	}
	// nodeState is the node's state, if the cluster has the node.
	nodeState := func(t *testing.T, o orchestrator.Orchestrator, name string) (model.NodeState, bool) {
		t.Helper()
		nodes, err := o.Nodes(within(t))
		if err != nil {
			t.Fatalf("Nodes: %v", err)
		}
		i := slices.IndexFunc(nodes, func(s model.NodeState) bool { return s.Name == name })
		if i < 0 {
			return model.NodeState{}, false
		}
		return nodes[i], true
	}
	// letters name eight of a kind in an order that isn't theirs.
	letters := []string{"m", "z", "a", "q", "c", "x", "k", "e"}

	t.Run("Check accepts an app that Deploy runs, and refuses one without a name", func(t *testing.T) {
		o := cluster(t).Orchestrator
		if err := o.Check(shop(unique("check"))); err != nil {
			t.Errorf("Check: %v", err)
		}
		if err := o.Check(shop("")); err == nil {
			t.Error("Check accepted an app without a name")
		}
	})

	t.Run("a deployed app is one of the cluster's apps, and ready", func(t *testing.T) {
		o := cluster(t).Orchestrator
		name := unique("deployed")
		deploy(t, o, shop(name))
		got, ok := state(t, o, name)
		if !ok {
			t.Fatalf("Apps lacks %s after its deploy", name)
		}
		if got.Want != 3 || got.Ready != 3 {
			t.Errorf("%d of %d replicas are ready once Deploy returned, want 3 of 3", got.Ready, got.Want)
		}
		if images := slices.Sorted(slices.Values(got.Images)); !slices.Equal(images, []string{"example.test/web:1", "example.test/worker:1"}) {
			t.Errorf("Images %q, want web's and worker's", got.Images)
		}
		if !slices.Equal(got.Hosts, []string{name + ".example.test"}) {
			t.Errorf("Hosts %q, want %s.example.test", got.Hosts, name)
		}
	})

	t.Run("each replica of a deployed app's containers is running, with its image and a name of its own", func(t *testing.T) {
		o := cluster(t).Orchestrator
		name := unique("replicas")
		deploy(t, o, shop(name))
		want := []string{"web example.test/web:1", "web example.test/web:1", "worker example.test/worker:1"}
		if got := containers(t, o, name); !slices.Equal(got, want) {
			t.Errorf("Running:\n got %q\nwant %q", got, want)
		}
	})

	t.Run("a deploy replaces the app", func(t *testing.T) {
		o := cluster(t).Orchestrator
		name := unique("replaced")
		app := shop(name)
		deploy(t, o, app)
		app.Containers[0].Image = "example.test/web:2"
		app.Containers[0].Replicas = 1
		deploy(t, o, app)
		want := []string{"web example.test/web:2", "worker example.test/worker:1"}
		if got := containers(t, o, name); !slices.Equal(got, want) {
			t.Errorf("Running after the second deploy:\n got %q\nwant %q", got, want)
		}
		got, _ := state(t, o, name)
		if images := slices.Sorted(slices.Values(got.Images)); got.Want != 2 || !slices.Equal(images, []string{"example.test/web:2", "example.test/worker:1"}) {
			t.Errorf("the app after the second deploy is %+v, want 2 replicas of web:2 and worker:1", got)
		}
	})

	t.Run("a deploy removes the container the app no longer has, and its host", func(t *testing.T) {
		o := cluster(t).Orchestrator
		name := unique("dropped")
		app := shop(name)
		deploy(t, o, app)
		app.Containers = app.Containers[1:]
		deploy(t, o, app)
		want := []string{"worker example.test/worker:1"}
		if got := containers(t, o, name); !slices.Equal(got, want) {
			t.Errorf("Running after a deploy without web:\n got %q\nwant %q", got, want)
		}
		if got, _ := state(t, o, name); len(got.Hosts) != 0 || got.Want != 1 {
			t.Errorf("the app after a deploy without web is %+v, want 1 replica and no host", got)
		}
	})

	t.Run("a deploy tells each step as it starts it, in order", func(t *testing.T) {
		o := cluster(t).Orchestrator
		name := unique("steps")
		removed(t, o, name)
		var steps []orchestrator.Step
		if err := o.Deploy(within(t), shop(name), func(s orchestrator.Step) { steps = append(steps, s) }); err != nil {
			t.Fatalf("Deploy: %v", err)
		}
		if len(steps) > 0 && steps[0] == orchestrator.WaitingForCluster {
			steps = steps[1:]
		}
		if !slices.Equal(steps, []orchestrator.Step{orchestrator.Applying, orchestrator.WaitingForReady}) {
			t.Errorf("the steps after any wait for the cluster were %v, want Applying, then WaitingForReady", steps)
		}
	})

	t.Run("a call on an ended context fails, and a deploy and a removal change nothing", func(t *testing.T) {
		o := cluster(t).Orchestrator
		name, there := unique("ended"), unique("there")
		removed(t, o, name)
		deploy(t, o, shop(there))
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := o.Deploy(ctx, shop(name), func(orchestrator.Step) {}); err == nil {
			t.Error("Deploy on an ended context succeeded")
		}
		if _, ok := state(t, o, name); ok {
			t.Errorf("Apps has %s, deployed on an ended context", name)
		}
		if err := o.Remove(ctx, there); err == nil {
			t.Error("Remove on an ended context succeeded")
		}
		if got := containers(t, o, there); len(got) != 3 {
			t.Errorf("%d containers of %s run after its removal on an ended context, want 3", len(got), there)
		}
		_, apps := o.Apps(ctx)
		_, all := o.Running(ctx)
		_, nodes := o.Nodes(ctx)
		_, token := o.ConsoleToken(ctx)
		for method, err := range map[string]error{
			"Certificate":   o.Certificate(ctx, there, "web"),
			"Logs":          o.Logs(ctx, there, orchestrator.LogOptions{}, &bytes.Buffer{}),
			"ContainerLogs": o.ContainerLogs(ctx, running(t, o, there)[0].ID, orchestrator.LogOptions{}, &bytes.Buffer{}),
			"Apps":          apps,
			"Running":       all,
			"Nodes":         nodes,
			"RunConsole":    o.RunConsole(ctx, "example.test/console:1", nil),
			"ConsoleToken":  token,
		} {
			if err == nil {
				t.Errorf("%s on an ended context succeeded", method)
			}
		}
	})

	t.Run("the certificate of a deployed container's host is waited for", func(t *testing.T) {
		o := cluster(t).Orchestrator
		name := unique("certificate")
		deploy(t, o, shop(name))
		if err := o.Certificate(within(t), name, "web"); err != nil {
			t.Errorf("Certificate: %v", err)
		}
	})

	t.Run("a removed app is gone, and so are its containers", func(t *testing.T) {
		o := cluster(t).Orchestrator
		name, other := unique("removed"), unique("kept")
		deploy(t, o, shop(name))
		deploy(t, o, shop(other))
		if err := o.Remove(within(t), name); err != nil {
			t.Fatalf("Remove: %v", err)
		}
		if _, ok := state(t, o, name); ok {
			t.Errorf("Apps has %s after its removal", name)
		}
		if got := running(t, o, name); len(got) != 0 {
			t.Errorf("Running after the removal: %+v", got)
		}
		if _, ok := state(t, o, other); !ok {
			t.Errorf("removing %s removed %s", name, other)
		}
		if got := containers(t, o, other); len(got) != 3 {
			t.Errorf("removing %s left %d containers of %s running, want 3", name, len(got), other)
		}
	})

	t.Run("an operation on an app the cluster doesn't have is ErrNoApp", func(t *testing.T) {
		o := cluster(t).Orchestrator
		name := unique("absent")
		absent := func(when string) {
			t.Helper()
			if err := o.Remove(within(t), name); !errors.Is(err, orchestrator.ErrNoApp) {
				t.Errorf("Remove of an app %s: %v, want ErrNoApp", when, err)
			}
			if err := o.Logs(within(t), name, orchestrator.LogOptions{Tail: 10}, &bytes.Buffer{}); !errors.Is(err, orchestrator.ErrNoApp) {
				t.Errorf("Logs of an app %s: %v, want ErrNoApp", when, err)
			}
			if err := o.Certificate(within(t), name, "web"); !errors.Is(err, orchestrator.ErrNoApp) {
				t.Errorf("Certificate of an app %s: %v, want ErrNoApp", when, err)
			}
		}
		absent("never deployed")
		deploy(t, o, shop(name))
		if err := o.Remove(within(t), name); err != nil {
			t.Fatalf("Remove: %v", err)
		}
		absent("removed")
	})

	t.Run("the logs of a deployed app are read", func(t *testing.T) {
		o := cluster(t).Orchestrator
		name := unique("logs")
		deploy(t, o, shop(name))
		if err := o.Logs(within(t), name, orchestrator.LogOptions{Tail: 10}, &bytes.Buffer{}); err != nil {
			t.Errorf("Logs: %v", err)
		}
	})

	t.Run("each running container has an ID of its own and a start, and its logs are read by the ID", func(t *testing.T) {
		o := cluster(t).Orchestrator
		name := unique("container")
		deploy(t, o, shop(name))
		all, err := o.Running(within(t))
		if err != nil {
			t.Fatalf("Running: %v", err)
		}
		seen := map[string]bool{}
		for _, r := range all {
			if r.ID == "" || seen[r.ID] {
				t.Errorf("Running has %+v, a container without an ID of its own", r)
			}
			seen[r.ID] = true
		}
		mine := running(t, o, name)
		if len(mine) != 3 {
			t.Fatalf("%d containers of %s run, want 3", len(mine), name)
		}
		for _, r := range mine {
			if r.Started.IsZero() || r.Started.After(time.Now().Add(time.Minute)) {
				t.Errorf("%s started at %v, want a time that has passed", r.ID, r.Started)
			}
			if err := o.ContainerLogs(within(t), r.ID, orchestrator.LogOptions{Tail: 10}, &bytes.Buffer{}); err != nil {
				t.Errorf("ContainerLogs of %s: %v", r.ID, err)
			}
		}
	})

	t.Run("there are no logs of a container that doesn't run", func(t *testing.T) {
		o := cluster(t).Orchestrator
		name := unique("gone")
		deploy(t, o, shop(name))
		id := running(t, o, name)[0].ID
		if err := o.Remove(within(t), name); err != nil {
			t.Fatalf("Remove: %v", err)
		}
		for _, id := range []string{id, "", "no-such-container", unique("a") + "/b/c"} {
			var w bytes.Buffer
			if err := o.ContainerLogs(within(t), id, orchestrator.LogOptions{Tail: 10}, &w); !errors.Is(err, orchestrator.ErrNoContainer) || w.Len() != 0 {
				t.Errorf("ContainerLogs of %q wrote %q, %v, want nothing and ErrNoContainer", id, w.String(), err)
			}
		}
	})

	t.Run("apps are ordered by name", func(t *testing.T) {
		o := cluster(t).Orchestrator
		id := unique("order")
		var want []string
		for _, letter := range letters {
			want = append(want, id+"-"+letter)
			deploy(t, o, shop(id+"-"+letter))
		}
		slices.Sort(want)
		apps, err := o.Apps(within(t))
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for _, a := range apps {
			if strings.HasPrefix(a.Name, id) {
				got = append(got, a.Name)
			}
		}
		if !slices.Equal(got, want) {
			t.Errorf("Apps lists\n %q, want\n %q", got, want)
		}
	})

	t.Run("running containers are ordered by app, then instance, then container", func(t *testing.T) {
		o := cluster(t).Orchestrator
		id := unique("sorted")
		for _, letter := range letters {
			deploy(t, o, shop(id+"-"+letter))
		}
		all, err := o.Running(within(t))
		if err != nil {
			t.Fatal(err)
		}
		own := slices.DeleteFunc(slices.Clone(all), func(r model.Running) bool { return !strings.HasPrefix(r.App, id) })
		if len(own) != 24 {
			t.Fatalf("%d containers of the 8 apps run, want 24", len(own))
		}
		ordered := slices.IsSortedFunc(all, func(a, b model.Running) int {
			return cmp.Or(strings.Compare(a.App, b.App), strings.Compare(a.Instance, b.Instance), strings.Compare(a.Container, b.Container))
		})
		if !ordered {
			t.Errorf("Running isn't ordered: %+v", all)
		}
	})

	t.Run("a host becomes the node asked for, and the cluster has it", func(t *testing.T) {
		c := cluster(t)
		name := unique("node")
		n, _ := node(t, c, orchestrator.NodeRequest{Name: name}, func(orchestrator.Step) {})
		if n.Name != name || n.ControlPlane {
			t.Errorf("Preflight returned the node %q, control plane %t, want %q, which isn't one", n.Name, n.ControlPlane, name)
		}
		if !n.PrivateAddress.IsValid() || n.Architecture == "" || n.MemoryMiB <= 0 {
			t.Errorf("Preflight read no address, architecture or memory: %+v", n)
		}
		got, ok := nodeState(t, c.Orchestrator, name)
		if !ok {
			t.Fatalf("Nodes lacks %s once AddNode returned", name)
		}
		want := model.NodeState{Name: name, Address: n.PrivateAddress.String(), Architecture: n.Architecture, MemoryMiB: n.MemoryMiB, Ready: "yes", Version: got.Version}
		if got != want {
			t.Errorf("the node is %+v, want %+v", got, want)
		}
	})

	t.Run("nodes are ordered by name", func(t *testing.T) {
		c := cluster(t)
		id := unique("node")
		var want []string
		for _, letter := range letters {
			want = append(want, id+"-"+letter)
			node(t, c, orchestrator.NodeRequest{Name: id + "-" + letter}, func(orchestrator.Step) {})
		}
		slices.Sort(want)
		nodes, err := c.Orchestrator.Nodes(within(t))
		if err != nil {
			t.Fatalf("Nodes: %v", err)
		}
		var got []string
		for _, n := range nodes {
			if strings.HasPrefix(n.Name, id) {
				got = append(got, n.Name)
			}
		}
		if !slices.Equal(got, want) {
			t.Errorf("Nodes lists\n %q, want\n %q", got, want)
		}
	})

	t.Run("a removed node is gone from the cluster, with its host or without", func(t *testing.T) {
		c := cluster(t)
		for _, reached := range []bool{true, false} {
			name := unique("node")
			_, host := node(t, c, orchestrator.NodeRequest{Name: name}, func(orchestrator.Step) {})
			at := host
			if !reached {
				at = nil
			}
			if err := c.Orchestrator.RemoveNode(within(t), name, at, orchestrator.RemoveOptions{}, func(orchestrator.Step) {}); err != nil {
				t.Fatalf("RemoveNode, host reached %t: %v", reached, err)
			}
			if _, ok := nodeState(t, c.Orchestrator, name); ok {
				t.Errorf("Nodes has %s after its removal, host reached %t", name, reached)
			}
		}
	})

	t.Run("adding and removing a node tell each step as they start it, in order", func(t *testing.T) {
		c := cluster(t)
		name := unique("node")
		var steps []orchestrator.Step
		tell := func(s orchestrator.Step) { steps = append(steps, s) }
		_, host := node(t, c, orchestrator.NodeRequest{Name: name}, tell)
		if want := []orchestrator.Step{orchestrator.Installing, orchestrator.Starting, orchestrator.WaitingForNode}; !slices.Equal(steps, want) {
			t.Errorf("the steps of AddNode were %v, want Installing, Starting, WaitingForNode", steps)
		}
		steps = nil
		if err := c.Orchestrator.RemoveNode(within(t), name, host, orchestrator.RemoveOptions{}, tell); err != nil {
			t.Fatalf("RemoveNode: %v", err)
		}
		if want := []orchestrator.Step{orchestrator.Draining, orchestrator.Uninstalling, orchestrator.Deleting}; !slices.Equal(steps, want) {
			t.Errorf("the steps of RemoveNode were %v, want Draining, Uninstalling, Deleting", steps)
		}
	})

	t.Run("a node isn't added under the name of a node the cluster has, which stays", func(t *testing.T) {
		c := cluster(t)
		name := unique("node")
		n, _ := node(t, c, orchestrator.NodeRequest{Name: name}, func(orchestrator.Step) {})
		var steps []orchestrator.Step
		if err := c.Orchestrator.AddNode(within(t), c.Host(t), n, func(s orchestrator.Step) { steps = append(steps, s) }); err == nil {
			t.Error("AddNode under the name of a node the cluster has succeeded")
		}
		if _, ok := nodeState(t, c.Orchestrator, name); !ok || len(steps) != 0 {
			t.Errorf("after the refusal the cluster has the node: %t, and the steps told were %v", ok, steps)
		}
	})

	t.Run("removing a node the cluster doesn't have succeeds", func(t *testing.T) {
		o := cluster(t).Orchestrator
		var steps []orchestrator.Step
		if err := o.RemoveNode(within(t), unique("node"), nil, orchestrator.RemoveOptions{}, func(s orchestrator.Step) { steps = append(steps, s) }); err != nil {
			t.Errorf("RemoveNode: %v", err)
		}
		if len(steps) != 0 {
			t.Errorf("RemoveNode of a node that isn't there, without a host, told the steps %v", steps)
		}
	})

	t.Run("the console runs from its image, as none of the apps, and issues a token", func(t *testing.T) {
		o := cluster(t).Orchestrator
		image := "example.test/" + unique("console") + ":1"
		if err := o.RunConsole(within(t), image, nil); err != nil {
			t.Fatalf("RunConsole: %v", err)
		}
		all, err := o.Running(within(t))
		if err != nil {
			t.Fatalf("Running: %v", err)
		}
		if !slices.ContainsFunc(all, func(r model.Running) bool { return r.App == "" && r.Image == image }) {
			t.Errorf("Running has no container of the cluster's own from %s: %+v", image, all)
		}
		token, err := o.ConsoleToken(within(t))
		if err != nil || token == "" {
			t.Errorf("ConsoleToken: %q, %v, want a token", token, err)
		}
	})
}
