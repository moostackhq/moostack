package cli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	ggcrregistry "github.com/google/go-containerregistry/pkg/registry"

	"github.com/moostackhq/moostack/internal/orchestrator"
)

// removing runs mooctl deploy --remove with args on prod, and returns
// what it printed on each stream.
func removing(t *testing.T, c *fakeCluster, home string, args ...string) (stdout, stderr string, err error) {
	t.Helper()
	var out, errs bytes.Buffer
	env := fakeEnvFor(c, home, nil)
	env.Stdout, env.Stderr = &out, &errs
	err = New(env).Run(t.Context(), append([]string{"mooctl", "-c", "prod", "deploy", "--remove"}, args...))
	return out.String(), errs.String(), err
}

func TestDeployRemove(t *testing.T) {
	c, home := deployingProd(t)
	app := manifestsApp(t, webDeployment)
	t.Chdir(app)
	mustRun(t, c, home, "deploy")

	t.Chdir(home)
	before := len(c.orch.made(""))
	stdout, stderr, err := removing(t, c, home, "web")
	if err != nil {
		t.Errorf("deploy --remove web: %v", err)
	}
	golden(t, "remove.stdout.golden", stdout)
	golden(t, "remove.stderr.golden", stderr)
	if calls, want := c.orch.made("")[before:], []string{"Apps", "Remove web"}; !slices.Equal(calls, want) {
		t.Errorf("the calls to the orchestrator were %q, want %q", calls, want)
	}
	if _, found := c.apps["web"]; found || c.locked {
		t.Errorf("after the removal the app is there: %t, and the lock held: %t", found, c.locked)
	}

	t.Chdir(app)
	mustRun(t, c, home, "deploy")
	if stdout, _, err := removing(t, c, home); err != nil || stdout != "Removed web from prod\n" {
		t.Errorf("deploy --remove in the app's directory: %v, printed %q", err, stdout)
	}
	if _, found := c.apps["web"]; found {
		t.Error("web is still there after a removal from its directory")
	}
}

func TestDeployRemoveRefusesWhatIsNoAppBeforeItRemovesAnything(t *testing.T) {
	c, home := deployingProd(t)
	t.Chdir(manifestsApp(t, webDeployment))
	mustRun(t, c, home, "deploy")
	t.Chdir(home)
	for _, tc := range []struct {
		args []string
		want string
		// reached says the cluster is asked for its apps.
		reached bool
	}{
		{[]string{"other"}, "there is no app other on prod", true},
		{nil, "which app? name it, or run mooctl deploy --remove in its directory", false},
		{[]string{"Web;rm"}, `"Web;rm" may hold only lowercase letters, digits and hyphens`, false},
		{[]string{"web", "other"}, "mooctl deploy takes 1 argument, not 2; see --help", false},
	} {
		before := len(c.orch.made(""))
		stdout, stderr, err := removing(t, c, home, tc.args...)
		if err == nil || err.Error() != tc.want {
			t.Errorf("deploy --remove %q: %v, want %q", tc.args, err, tc.want)
		}
		if stdout != "" || stderr != "" {
			t.Errorf("deploy --remove %q printed %q on stdout and %q on stderr", tc.args, stdout, stderr)
		}
		var want []string
		if tc.reached {
			want = []string{"Apps"}
		}
		if calls := c.orch.made("")[before:]; !slices.Equal(calls, want) {
			t.Errorf("deploy --remove %q made the calls %q, want %q", tc.args, calls, want)
		}
	}
	if _, found := c.apps["web"]; !found || c.locked {
		t.Errorf("after the refusals web is there: %t, and the lock held: %t", found, c.locked)
	}
}

func TestRemovingAnAppThatIsntThereAsksTheRegistryNothing(t *testing.T) {
	c, home := deployingProd(t)
	t.Chdir(manifestsApp(t, webDeployment))
	mustRun(t, c, home, "deploy")
	var requests atomic.Int32
	images := ggcrregistry.New(ggcrregistry.Logger(log.New(io.Discard, "", 0)))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		images.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	c.registry = server.Listener.Addr().String()
	if _, _, err := removing(t, c, home, "other"); err == nil || err.Error() != "there is no app other on prod" {
		t.Fatalf("deploy --remove other: %v", err)
	}
	if n := requests.Load(); n != 0 {
		t.Errorf("the removal of an app that isn't there made %d requests to the registry", n)
	}
	// The registry is the one a removal asks.
	if _, _, err := removing(t, c, home, "web"); err != nil || requests.Load() == 0 {
		t.Errorf("deploy --remove web: %v, with %d requests to the registry", err, requests.Load())
	}
}

func TestARemovalWhoseLockIsLostWhileItLooksForTheAppRemovesNothing(t *testing.T) {
	c, home := deployingProd(t)
	t.Chdir(manifestsApp(t, webDeployment))
	mustRun(t, c, home, "deploy")
	c.orch.onApps = func() {
		c.lock.Close()
		time.Sleep(100 * time.Millisecond)
	}
	stdout, stderr, err := removing(t, c, home)
	if err == nil || stdout != "" || stderr != "" {
		t.Errorf("deploy --remove: %v, printed %q and %q, want a failure and nothing printed", err, stdout, stderr)
	}
	if calls := c.orch.made("Remove"); len(calls) != 0 || c.apps["web"] == nil {
		t.Errorf("the removal went on without its lock, by the calls %q", calls)
	}
}

func TestARemoveThatCantReachTheRegistryRemovesNothing(t *testing.T) {
	c, home := deployingProd(t)
	t.Chdir(manifestsApp(t, webDeployment))
	mustRun(t, c, home, "deploy")
	registry := c.registry
	c.registry = ""
	if out, err := onFake(t, c, home, "deploy", "--remove", "web"); err == nil {
		t.Fatalf("--remove passed without the registry:\n%s", out)
	}
	if _, found := c.apps["web"]; !found || len(c.orch.made("Remove")) != 0 {
		t.Fatalf("web went though its images stayed, by the calls %q", c.orch.made("Remove"))
	}
	c.registry = registry
	mustRun(t, c, home, "deploy", "--remove", "web")
	if _, found := c.apps["web"]; found {
		t.Error("the rerun left web")
	}
}

func TestARemovalThatFailsOnTheClusterSaysWhyAndRunAgainFinishes(t *testing.T) {
	c, home := deployingProd(t)
	t.Chdir(manifestsApp(t, webDeployment))
	mustRun(t, c, home, "deploy")
	c.orch.Fail("Remove", errors.New("removing web: it is still there"))
	stdout, stderr, err := removing(t, c, home)
	if err == nil || err.Error() != "removing web: it is still there" {
		t.Errorf("deploy --remove: %v, want the orchestrator's error", err)
	}
	if stdout != "" || stderr != "Removing web from prod...\n" || c.locked {
		t.Errorf("the failed removal printed %q on stdout and %q on stderr, and holds the lock: %t", stdout, stderr, c.locked)
	}
	c.orch.Fail("Remove", nil)
	if stdout, _, err := removing(t, c, home); err != nil || stdout != "Removed web from prod\n" {
		t.Errorf("deploy --remove again: %v, printed %q", err, stdout)
	}

	mustRun(t, c, home, "deploy")
	c.orch.Fail("Apps", errors.New("the connection is lost"))
	if _, stderr, err := removing(t, c, home); err == nil || err.Error() != "looking for web: the connection is lost" || stderr != "" {
		t.Errorf("deploy --remove with the apps unread: %v, printed %q", err, stderr)
	}
	c.orch.Fail("Apps", nil)
	// The app goes between the look for it and its removal.
	c.orch.Fail("Remove", orchestrator.ErrNoApp)
	if stdout, _, err := removing(t, c, home); err == nil || err.Error() != "there is no app web on prod" || stdout != "" {
		t.Errorf("deploy --remove of an app gone meanwhile: %v, printed %q", err, stdout)
	}
}

func TestARemovalWhoseLockIsLostEndsWithThatAsItsCause(t *testing.T) {
	c, home := deployingProd(t)
	t.Chdir(manifestsApp(t, webDeployment))
	mustRun(t, c, home, "deploy")
	c.orch.onRemove = func() {
		c.lock.Close()
		time.Sleep(100 * time.Millisecond)
	}
	stdout, _, err := removing(t, c, home)
	if err == nil || err.Error() != "the connection that holds the operation lock dropped" || stdout != "" {
		t.Errorf("deploy --remove: %v, printed %q, want the lost lock and no result", err, stdout)
	}
	if _, found := c.apps["web"]; !found {
		t.Error("the removal went on without its lock")
	}
}

func TestARemovalInterruptedEndsWithItsCause(t *testing.T) {
	c, home := deployingProd(t)
	t.Chdir(manifestsApp(t, webDeployment))
	mustRun(t, c, home, "deploy")
	ctx, interrupt := context.WithCancelCause(t.Context())
	c.orch.onRemove = func() { interrupt(errors.New("interrupted")) }
	var out bytes.Buffer
	err := New(fakeEnvFor(c, home, &out)).Run(ctx, []string{"mooctl", "-c", "prod", "deploy", "--remove"})
	if err == nil || err.Error() != "interrupted" || strings.Contains(out.String(), "Removed") {
		t.Errorf("deploy --remove: %v, printed %q, want the interrupt and no result", err, out.String())
	}
}
