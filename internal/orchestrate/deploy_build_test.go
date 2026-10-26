package orchestrate_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/ociimage"
	"github.com/moostackhq/moostack/internal/orchestrate"
)

// The tags of the local builds the deploy tests import.
const (
	firstTag  = "moostack.local/web:20261026T120000Z"
	secondTag = "moostack.local/web:20261026T130000Z"
)

// buildArchive is the archive of a local build of web tagged tag, for
// arch, whose content is content: the same content makes the same
// build.
func buildArchive(t *testing.T, tag, arch, content string) ([]byte, string) {
	t.Helper()
	var b bytes.Buffer
	if err := ociimage.Write(&b, ociimage.Image{Name: tag, Architecture: arch, Entrypoint: []string{"/web"}, Files: []ociimage.File{{Name: "web", Mode: 0o755, Data: []byte(content)}}}); err != nil {
		t.Fatal(err)
	}
	digest, err := ociimage.Digest(b.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	return b.Bytes(), digest
}

// built is web, built locally as tag, its archive by node from archives,
// and the nodes whose archive the deploy read, in order.
func built(tag string, archives map[string][]byte) (orchestrate.App, *[]string) {
	var read []string
	return orchestrate.App{Name: "web", Image: tag, Archive: func(node string) (io.ReadSeekCloser, error) {
		read = append(read, node)
		if a, ok := archives[node]; ok {
			return opened(a), nil
		}
		return nil, errors.New("no build for " + node)
	}}, &read
}

// opened is data, as an archive opened.
func opened(data []byte) io.ReadSeekCloser {
	return struct {
		io.ReadSeeker
		io.Closer
	}{bytes.NewReader(data), io.NopCloser(nil)}
}

// deployApp deploys app on cl, and returns what it told.
func deployApp(t *testing.T, cl *orchestrate.Cluster, app orchestrate.App) (*orchestrate.Deployed, []orchestrate.DeployStep, error) {
	t.Helper()
	var steps []orchestrate.DeployStep
	d, err := cl.Deploy(t.Context(), app, func(s orchestrate.DeployStep) { steps = append(steps, s) })
	return d, steps, err
}

// containerOf is the container called name that n reports, nil if none.
func containerOf(n *node, name string) *nodev1.ContainerStatus {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, c := range n.others {
		if c.Name == name {
			return proto.Clone(c).(*nodev1.ContainerStatus)
		}
	}
	return nil
}

// ready is a node at generation 1 running others.
func ready(others ...*nodev1.ContainerStatus) *node {
	return &node{generation: 1, others: others}
}

func TestDeployImportsTheBuildForThePickedNodeAndRunsIt(t *testing.T) {
	amd, _ := buildArchive(t, firstTag, "amd64", "web 1")
	arm, _ := buildArchive(t, firstTag, "arm64", "web 1")
	busy, free := ready(appContainer("api", nginx, nginxBuild)), ready()
	cl := deploying(t, fakes{"node1": busy, "node2": free}, nil)
	app, read := built(firstTag, map[string][]byte{"node1": amd, "node2": arm})
	d, steps, err := deployApp(t, cl, app)
	if err != nil {
		t.Fatal(err)
	}
	if *d != (orchestrate.Deployed{Node: "node2", Image: firstTag}) {
		t.Errorf("Deploy = %+v, want web on node2, changed", d)
	}
	want := []orchestrate.DeployStep{{Placement: orchestrate.Placement{Node: "node2"}}, {Placement: orchestrate.Placement{Node: "node2"}, Fetching: true}}
	if !slices.Equal(steps, want) {
		t.Errorf("Deploy told %+v, want %+v", steps, want)
	}
	if !slices.Equal(*read, []string{"node2"}) {
		t.Errorf("Deploy read the builds for %v, want node2's alone", *read)
	}
	if got := free.imports(); !slices.Equal(got, []string{firstTag}) {
		t.Errorf("node2 imported %v, want %s", got, firstTag)
	}
	if got := free.containersRun(); len(got) != 1 || !proto.Equal(got[0], &nodev1.Container{Name: "app-web", Image: firstTag}) {
		t.Errorf("node2 ran %v, want app-web from %s with nothing else", got, firstTag)
	}
	if len(free.pulls()) != 0 || len(busy.imports()) != 0 || len(busy.containersRun()) != 0 {
		t.Errorf("node2 pulled %v; node1 imported %v and ran %v; want nothing", free.pulls(), busy.imports(), busy.containersRun())
	}
}

// The digest decides, not the tag: the same build, tagged anew, changes
// nothing, though the app's container takes the new tag's name.
func TestARedeployOfAnUnchangedBuildChangesNothing(t *testing.T) {
	archive, digest := buildArchive(t, secondTag, "amd64", "web 1")
	n := ready(appContainer("web", firstTag, digest))
	cl := deploying(t, fakes{"node1": n}, nil)
	app, _ := built(secondTag, map[string][]byte{"node1": archive})
	d, steps, err := deployApp(t, cl, app)
	if err != nil {
		t.Fatal(err)
	}
	if *d != (orchestrate.Deployed{Node: "node1", Image: secondTag, Unchanged: true}) || !steps[0].RunsThere {
		t.Errorf("Deploy = %+v, told %+v, want web on node1, where it runs, unchanged", d, steps)
	}
	if k := containerOf(n, "app-web"); k == nil || k.Image != secondTag || k.Digest != digest {
		t.Errorf("the node runs %v, want app-web from %s, the same build", k, secondTag)
	}
}

func TestARedeployOfAChangedBuildReplacesTheApp(t *testing.T) {
	_, old := buildArchive(t, firstTag, "amd64", "web 1")
	archive, digest := buildArchive(t, secondTag, "amd64", "web 2")
	n := ready(appContainer("web", firstTag, old))
	cl := deploying(t, fakes{"node1": n}, nil)
	app, _ := built(secondTag, map[string][]byte{"node1": archive})
	d, _, err := deployApp(t, cl, app)
	if err != nil || *d != (orchestrate.Deployed{Node: "node1", Image: secondTag}) {
		t.Fatalf("Deploy = %+v, %v, want web on node1, changed", d, err)
	}
	if k := containerOf(n, "app-web"); k == nil || k.Image != secondTag || k.Digest != digest {
		t.Errorf("the node runs %v, want app-web from the new build", k)
	}
}

func TestAFailedImportChangesNothing(t *testing.T) {
	archive, _ := buildArchive(t, secondTag, "arm64", "web 2")
	_, old := buildArchive(t, firstTag, "amd64", "web 1")
	refused := status.Error(codes.FailedPrecondition, "the image is for linux/arm64, not for this node's linux/amd64")
	for name, edit := range map[string]func(n *node){
		"refused":              func(n *node) { n.app.importErr = refused },
		"naming another image": func(n *node) { n.app.importNames = []string{"moostack.local/other:1"} },
		"naming no image":      func(n *node) { n.app.importNames = []string{} },
	} {
		n := ready(appContainer("web", firstTag, old))
		edit(n)
		cl := deploying(t, fakes{"node1": n}, nil)
		app, _ := built(secondTag, map[string][]byte{"node1": archive})
		_, _, err := deployApp(t, cl, app)
		var ie *orchestrate.ImportError
		if !errors.As(err, &ie) || ie.Node != "node1" || ie.Image != secondTag {
			t.Errorf("%s: Deploy = %v, want an *ImportError of node1 and %s", name, err, secondTag)
		}
		if name == "refused" && status.Code(err) != codes.FailedPrecondition {
			t.Errorf("%s: Deploy = %v, want the node's FAILED_PRECONDITION", name, err)
		}
		if got := n.containersRun(); len(got) != 0 {
			t.Errorf("%s: the node ran %v after a failed import", name, got)
		}
	}
}

func TestADeployWhoseBuildCantBeReadChangesNothing(t *testing.T) {
	n := ready()
	cl := deploying(t, fakes{"node1": n}, nil)
	for name, archives := range map[string]map[string][]byte{
		"none for the node": nil,
		"not an archive":    {"node1": []byte("not an archive")},
	} {
		app, _ := built(firstTag, archives)
		_, steps, err := deployApp(t, cl, app)
		// The import isn't told as it starts, as it never does.
		if want := []orchestrate.DeployStep{{Placement: orchestrate.Placement{Node: "node1"}}}; !slices.Equal(steps, want) {
			t.Errorf("%s: Deploy told %+v, want %+v", name, steps, want)
		}
		if err == nil || !strings.Contains(err.Error(), "nothing changed") {
			t.Errorf("%s: Deploy = %v, want it to fail saying nothing changed", name, err)
		}
		if got := n.imports(); len(got) != 0 {
			t.Errorf("%s: the node imported %v", name, got)
		}
	}
}

func TestADeployStoppedDuringTheImportChangesNothing(t *testing.T) {
	archive, _ := buildArchive(t, firstTag, "amd64", "web 1")
	n := ready()
	n.importDelay = 5 * time.Second
	cl := deploying(t, fakes{"node1": n}, nil)
	ctx, cancel := context.WithCancelCause(cl.Context())
	app, _ := built(firstTag, map[string][]byte{"node1": archive})
	stop := errors.New("interrupted")
	_, err := cl.Deploy(ctx, app, func(s orchestrate.DeployStep) {
		if s.Fetching {
			cancel(stop)
		}
	})
	var ie *orchestrate.ImportError
	if !errors.As(err, &ie) || !errors.Is(err, stop) {
		t.Errorf("Deploy stopped during the import = %v, want an *ImportError for the stop", err)
	}
	if got := n.containersRun(); len(got) != 0 {
		t.Errorf("the node ran %v", got)
	}
}

// A lock lost while the node imports the build fails the deploy with an
// *ImportError of the lost lock, running nothing.
func TestADeployThatLosesTheLockDuringTheImportRunsNothing(t *testing.T) {
	archive, _ := buildArchive(t, firstTag, "amd64", "web 1")
	n := ready()
	n.importDelay = time.Minute
	cl := deploying(t, fakes{"node1": n}, func(nodes *orchestrate.Nodes) { nodes.LeaseTTL = 300 * time.Millisecond })
	app, _ := built(firstTag, map[string][]byte{"node1": archive})
	_, err := cl.Deploy(cl.Context(), app, func(s orchestrate.DeployStep) {
		if s.Fetching {
			go func() { time.Sleep(50 * time.Millisecond); n.lockFor("bobs", time.Hour) }()
		}
	})
	var ie *orchestrate.ImportError
	if !errors.As(err, &ie) || !errors.Is(err, orchestrate.ErrLockLost) {
		t.Errorf("Deploy that lost the lock during its import: %v, want an *ImportError of ErrLockLost", err)
	}
	if got := n.containersRun(); len(got) != 0 {
		t.Errorf("the node ran %v", got)
	}
}

// A deploy that finds the lock lost, or is stopped, once the node
// imported the build says so of the import, runs nothing, and says
// nothing changed.
func TestADeployThatEndsOnceTheNodeImportedSaysSo(t *testing.T) {
	archive, _ := buildArchive(t, firstTag, "amd64", "web 1")

	n := ready()
	n.importDelay = 600 * time.Millisecond
	cl := deploying(t, fakes{"node1": n}, func(nodes *orchestrate.Nodes) { nodes.LeaseTTL = 300 * time.Millisecond })
	app, _ := built(firstTag, map[string][]byte{"node1": archive})
	_, err := cl.Deploy(context.Background(), app, func(s orchestrate.DeployStep) {
		if s.Fetching {
			go func() { time.Sleep(50 * time.Millisecond); n.lockFor("bobs", time.Hour) }()
		}
	})
	if !errors.Is(err, orchestrate.ErrLockLost) || !strings.Contains(err.Error(), ", once node1 imported "+firstTag+", so mooctl ran nothing; nothing changed") {
		t.Errorf("Deploy that found the lock lost once the node imported: %v", err)
	}
	if got := n.containersRun(); len(got) != 0 {
		t.Errorf("the node ran %v", got)
	}

	n = ready()
	cl = deploying(t, fakes{"node1": n}, nil)
	ctx, cancel := context.WithCancel(t.Context())
	n.app.afterImport = cancel
	app, _ = built(firstTag, map[string][]byte{"node1": archive})
	_, err = cl.Deploy(ctx, app, func(orchestrate.DeployStep) {})
	if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "mooctl stopped once node1 imported "+firstTag+", before it ran web") || !strings.Contains(err.Error(), "nothing changed") {
		t.Errorf("Deploy stopped once the node imported: %v", err)
	}
	if got := n.containersRun(); len(got) != 0 {
		t.Errorf("the node ran %v", got)
	}
}

// The archive's digest decides: the build that runs under the very tag
// deployed changes nothing, with no run; and a failed run whose new
// container started all the same says the build asked for runs.
func TestTheArchivesDigestIsTheBuilds(t *testing.T) {
	archive, digest := buildArchive(t, firstTag, "amd64", "web 1")
	n := ready(appContainer("web", firstTag, digest))
	cl := deploying(t, fakes{"node1": n}, nil)
	app, _ := built(firstTag, map[string][]byte{"node1": archive})
	if d, _, err := deployApp(t, cl, app); err != nil || !d.Unchanged {
		t.Errorf("Deploy of the build that runs under its tag = %+v, %v, want it unchanged", d, err)
	}
	if got := n.containersRun(); len(got) != 0 {
		t.Errorf("the node ran %v, want no run", got)
	}

	_, old := buildArchive(t, firstTag, "amd64", "web 1")
	archive, digest = buildArchive(t, secondTag, "amd64", "web 2")
	n = ready(appContainer("web", firstTag, old))
	n.app.runErr = status.Error(codes.Internal, "the disk is full")
	n.app.leaves = appContainer("web", secondTag, digest)
	cl = deploying(t, fakes{"node1": n}, nil)
	app, _ = built(secondTag, map[string][]byte{"node1": archive})
	_, _, err := deployApp(t, cl, app)
	var re *orchestrate.RunError
	if !errors.As(err, &re) || re.Runs != orchestrate.RunsAsAsked || re.Image != secondTag {
		t.Errorf("Deploy = %v (%+v), want the build asked for running, %s", err, re, secondTag)
	}
}
