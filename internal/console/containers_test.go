package console_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/moostackhq/moostack/internal/console"
	"github.com/moostackhq/moostack/internal/model"
	"github.com/moostackhq/moostack/internal/orchestrator"
	"github.com/moostackhq/moostack/internal/orchestrator/orchestratortest"
)

// page asks a console over o for the containers page, with a session.
func page(t *testing.T, o orchestrator.Orchestrator) (*http.Response, string) {
	t.Helper()
	c := console.New(o)
	return send(t, c, http.MethodGet, "/console/", newSession(t, c), nil)
}

func TestThePageListsRunningContainersByAppAndTheClustersOwnLast(t *testing.T) {
	res, body := page(t, cluster)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200", res.StatusCode)
	}
	if got := res.Header.Get("Content-Type"); got != "text/html; charset=utf-8" {
		t.Errorf("Content-Type %q", got)
	}
	golden(t, "containers.golden.html", body)
}

func TestAClusterWithNothingRunningSaysSo(t *testing.T) {
	res, body := page(t, &orchestratortest.Fake{})
	if res.StatusCode != http.StatusOK {
		t.Errorf("status %d, want 200", res.StatusCode)
	}
	if !strings.Contains(body, "No running containers.") {
		t.Errorf("the page doesn't say that nothing runs:\n%s", body)
	}
	if strings.Contains(body, "<table") {
		t.Errorf("the page has a table with nothing running:\n%s", body)
	}
}

func TestEachAppHasOneSectionAndTheClustersOwnContainersOne(t *testing.T) {
	_, body := page(t, cluster)
	if got := strings.Count(body, "<section>"); got != 3 {
		t.Errorf("%d sections for 2 apps and the cluster's own containers:\n%s", got, body)
	}
	for _, heading := range []string{"<h2>blog</h2>", "<h2>shop</h2>", "<h2>The cluster's own</h2>"} {
		if got := strings.Count(body, heading); got != 1 {
			t.Errorf("%d headings %s, want 1:\n%s", got, heading, body)
		}
	}
}

func TestAClusterWithoutAppsHasOnlyItsOwnSection(t *testing.T) {
	_, body := page(t, &orchestratortest.Fake{Own: []model.Running{{Replica: "dns-1", Container: "dns", Image: "example.test/dns:1"}}})
	if strings.Count(body, "<section>") != 1 || !strings.Contains(body, "<h2>The cluster's own</h2>") {
		t.Errorf("the page of a cluster without apps:\n%s", body)
	}
}

func TestAClusterWithoutContainersOfItsOwnHasNoSectionForThem(t *testing.T) {
	f := &orchestratortest.Fake{}
	if err := f.Deploy(context.Background(), model.App{Name: "shop", Containers: []model.Container{{Name: "web", Image: "example.test/web:1"}}}, func(orchestrator.Step) {}); err != nil {
		t.Fatal(err)
	}
	_, body := page(t, f)
	if strings.Count(body, "<section>") != 1 || strings.Contains(body, "The cluster") {
		t.Errorf("the page of a cluster with nothing of its own running:\n%s", body)
	}
}

func TestNamesFromTheClusterAreEscaped(t *testing.T) {
	f := &orchestratortest.Fake{Own: []model.Running{{Replica: `"><script>p()</script>`, Container: "<i>c</i>", Image: "<script>alert(1)</script>"}}}
	app := model.App{Name: "<b>app</b>", Containers: []model.Container{{Name: "<u>web</u>", Image: "<script>alert(2)</script>"}}}
	if err := f.Deploy(context.Background(), app, func(orchestrator.Step) {}); err != nil {
		t.Fatal(err)
	}
	_, body := page(t, f)
	for _, raw := range []string{"<b>app</b>", "<script>", "<i>c</i>", "<u>web</u>"} {
		if strings.Contains(body, raw) {
			t.Errorf("the page holds %s unescaped:\n%s", raw, body)
		}
	}
	for _, shown := range []string{"&lt;script&gt;alert(1)&lt;/script&gt;", "&lt;script&gt;alert(2)&lt;/script&gt;", "<h2>&lt;b&gt;app&lt;/b&gt;</h2>"} {
		if !strings.Contains(body, shown) {
			t.Errorf("the page doesn't show %s:\n%s", shown, body)
		}
	}
}

func TestAClusterThatCantBeReadIsAnErrorPage(t *testing.T) {
	f := &orchestratortest.Fake{}
	f.Fail("Running", errors.New("dial tcp 10.43.0.1:443: connection refused"))
	res, body := page(t, f)
	if res.StatusCode != http.StatusBadGateway {
		t.Errorf("status %d, want 502", res.StatusCode)
	}
	if !strings.Contains(body, "The cluster can&#39;t be read.") {
		t.Errorf("the page doesn't say the cluster can't be read:\n%s", body)
	}
	if strings.Contains(body, "No running containers.") {
		t.Errorf("the page says nothing runs when it can't tell:\n%s", body)
	}
	if strings.Contains(body, "10.43.0.1") {
		t.Errorf("the page shows the error's detail:\n%s", body)
	}
}

// stalled is a cluster that never answers.
type stalled struct{ orchestrator.Orchestrator }

func (stalled) Running(ctx context.Context) ([]model.Running, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestAClusterThatDoesntAnswerIsAnErrorPageAfter20Seconds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		res, body := page(t, stalled{})
		if res.StatusCode != http.StatusBadGateway || !strings.Contains(body, "The cluster can&#39;t be read.") {
			t.Errorf("status %d, want the 502 page:\n%s", res.StatusCode, body)
		}
		if took := time.Since(start); took != 20*time.Second {
			t.Errorf("the page took %v, want 20s", took)
		}
	})
}

func TestWithoutASessionThePageRedirectsToTheLoginPage(t *testing.T) {
	c := console.New(cluster)
	for name, session := range map[string]*http.Cookie{
		"no cookie":      nil,
		"an unknown one": {Name: "__Secure-console", Value: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
		"an empty one":   {Name: "__Secure-console", Value: ""},
	} {
		res, body := send(t, c, http.MethodGet, "/console/", session, nil)
		if res.StatusCode != http.StatusSeeOther || res.Header.Get("Location") != "/console/login" {
			t.Errorf("%s: status %d to %q, want 303 to /console/login", name, res.StatusCode, res.Header.Get("Location"))
		}
		if strings.Contains(body, "coredns") {
			t.Errorf("%s: the answer lists containers:\n%s", name, body)
		}
	}
}

func TestOnlyGetIsAllowedOnThePage(t *testing.T) {
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch} {
		rec := httptest.NewRecorder()
		console.New(cluster).ServeHTTP(rec, httptest.NewRequest(method, "/console/", nil))
		if rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s /console/: status %d, want 405", method, rec.Code)
		}
	}
}

func TestThePageIsNotCached(t *testing.T) {
	res, _ := page(t, cluster)
	if got := res.Header.Get("Cache-Control"); got != "no-store" {
		t.Errorf("Cache-Control %q, want no-store", got)
	}
}

func TestThePageHasALogoutButton(t *testing.T) {
	_, body := page(t, cluster)
	if !strings.Contains(body, `<form method="post" action="/console/logout">`) {
		t.Errorf("no logout form on the page:\n%s", body)
	}
}

// counting counts how often the cluster is read.
type counting struct {
	orchestrator.Orchestrator
	reads atomic.Int32
}

func (c *counting) Running(ctx context.Context) ([]model.Running, error) {
	c.reads.Add(1)
	return c.Orchestrator.Running(ctx)
}

func TestWithoutASessionTheClusterIsNotRead(t *testing.T) {
	counted := &counting{Orchestrator: cluster}
	c := console.New(counted)
	send(t, c, http.MethodGet, "/console/", nil, nil)
	send(t, c, http.MethodGet, "/console/", &http.Cookie{Name: "__Secure-console", Value: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}, nil)
	if reads := counted.reads.Load(); reads != 0 {
		t.Errorf("the cluster was read %d times for requests without a session", reads)
	}
	send(t, c, http.MethodGet, "/console/", newSession(t, c), nil)
	if reads := counted.reads.Load(); reads != 1 {
		t.Errorf("the cluster was read %d times for one request with a session", reads)
	}
}

func TestAClientThatLeavesIsNotLoggedAsAClusterThatCantBeRead(t *testing.T) {
	written := recordOutput(t)
	c := console.New(stalled{})
	session := newSession(t, c)
	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/console/", nil).WithContext(ctx)
	req.AddCookie(session)
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.ServeHTTP(httptest.NewRecorder(), req)
	}()
	cancel()
	<-done
	if strings.Contains(written(), "reading the cluster") {
		t.Errorf("the log blames the cluster for a client that left:\n%s", written())
	}
}
