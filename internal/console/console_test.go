package console_test

import (
	"context"
	"errors"
	"flag"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/moostackhq/moostack/internal/console"
	"github.com/moostackhq/moostack/internal/orchestrator"
	"github.com/moostackhq/moostack/internal/orchestrator/orchestratortest"
)

var update = flag.Bool("update", false, "rewrite the golden files")

func get(t *testing.T, o orchestrator.Orchestrator, path string) (*http.Response, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	console.New(o).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	res := rec.Result()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	return res, string(body)
}

var cluster = &orchestratortest.Fake{Containers: []orchestrator.Container{
	{Namespace: "kube-system", Pod: "coredns-7b98449c4-x2x9v", Name: "coredns", Image: "rancher/mirrored-coredns-coredns:1.12.0"},
	{Namespace: "shop", Pod: "web-f6dfb8fb4-f9fsd", Name: "web", Image: "registry.moostack.internal:5000/shop/web@sha256:0a1b"},
	{Namespace: "shop", Pod: "web-f6dfb8fb4-f9fsd", Name: "worker", Image: "registry.moostack.internal:5000/shop/worker@sha256:2c3d"},
	{Namespace: "shop", Pod: "web-f6dfb8fb4-k2lmn", Name: "web", Image: "registry.moostack.internal:5000/shop/web@sha256:0a1b"},
}}

func TestThePageListsRunningContainersByNamespace(t *testing.T) {
	res, body := get(t, cluster, "/console/")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200", res.StatusCode)
	}
	if got := res.Header.Get("Content-Type"); got != "text/html; charset=utf-8" {
		t.Errorf("Content-Type %q", got)
	}
	const golden = "testdata/containers.golden.html"
	if *update {
		if err := os.WriteFile(golden, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatal(err)
	}
	if body != string(want) {
		t.Errorf("the page isn't %s; run with -update and review the change:\n%s", golden, body)
	}
}

func TestAClusterWithNothingRunningSaysSo(t *testing.T) {
	res, body := get(t, &orchestratortest.Fake{}, "/console/")
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

func TestEachNamespaceHasOneSection(t *testing.T) {
	_, body := get(t, cluster, "/console/")
	if got := strings.Count(body, "<section>"); got != 2 {
		t.Errorf("%d sections for 2 namespaces:\n%s", got, body)
	}
	if got := strings.Count(body, "<h2>shop</h2>"); got != 1 {
		t.Errorf("%d headings for shop, want 1:\n%s", got, body)
	}
}

func TestNamesFromTheClusterAreEscaped(t *testing.T) {
	_, body := get(t, &orchestratortest.Fake{Containers: []orchestrator.Container{
		{Namespace: "<b>ns</b>", Pod: `"><script>p()</script>`, Name: "<i>c</i>", Image: "<script>alert(1)</script>"},
	}}, "/console/")
	for _, raw := range []string{"<b>ns</b>", "<script>", "<i>c</i>"} {
		if strings.Contains(body, raw) {
			t.Errorf("the page holds %s unescaped:\n%s", raw, body)
		}
	}
	if !strings.Contains(body, "&lt;script&gt;alert(1)&lt;/script&gt;") {
		t.Errorf("the page doesn't show the image's name:\n%s", body)
	}
}

func TestAClusterThatCantBeReadIsAnErrorPage(t *testing.T) {
	res, body := get(t, &orchestratortest.Fake{Err: errors.New("dial tcp 10.43.0.1:443: connection refused")}, "/console/")
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
type stalled struct{}

func (stalled) Running(ctx context.Context) ([]orchestrator.Container, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestAClusterThatDoesntAnswerIsAnErrorPageAfter20Seconds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		res, body := get(t, stalled{}, "/console/")
		if res.StatusCode != http.StatusBadGateway || !strings.Contains(body, "The cluster can&#39;t be read.") {
			t.Errorf("status %d, want the 502 page:\n%s", res.StatusCode, body)
		}
		if took := time.Since(start); took != 20*time.Second {
			t.Errorf("the page took %v, want 20s", took)
		}
	})
}

func TestTheStylesheetIsServed(t *testing.T) {
	res, body := get(t, cluster, "/console/static/style.css")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200", res.StatusCode)
	}
	if got := res.Header.Get("Content-Type"); got != "text/css; charset=utf-8" {
		t.Errorf("Content-Type %q", got)
	}
	if !strings.Contains(body, "{") {
		t.Errorf("no rules in the stylesheet:\n%s", body)
	}
}

func TestConsoleWithoutASlashRedirectsToThePage(t *testing.T) {
	res, _ := get(t, cluster, "/console")
	if res.StatusCode != http.StatusPermanentRedirect || res.Header.Get("Location") != "/console/" {
		t.Errorf("status %d to %q, want 308 to /console/", res.StatusCode, res.Header.Get("Location"))
	}
}

func TestNothingIsServedOutsideTheRoutes(t *testing.T) {
	for _, path := range []string{"/", "/style.css", "/console/nothing", "/console/style.css", "/console/static/", "/console/static/nothing.css", "/console/static/style.css/x", "/console/templates/containers.html", "/consolex"} {
		if res, _ := get(t, cluster, path); res.StatusCode != http.StatusNotFound {
			t.Errorf("GET %s: status %d, want 404", path, res.StatusCode)
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

func TestEveryAnswerForbidsFramingAndOtherOrigins(t *testing.T) {
	for _, path := range []string{"/console/", "/console/static/style.css", "/nothing"} {
		res, _ := get(t, cluster, path)
		if got := res.Header.Get("Content-Security-Policy"); got != "default-src 'self'; frame-ancestors 'none'" {
			t.Errorf("GET %s: Content-Security-Policy %q", path, got)
		}
		if got := res.Header.Get("X-Content-Type-Options"); got != "nosniff" {
			t.Errorf("GET %s: X-Content-Type-Options %q", path, got)
		}
	}
}

func TestThePageIsNotCached(t *testing.T) {
	res, _ := get(t, cluster, "/console/")
	if got := res.Header.Get("Cache-Control"); got != "no-store" {
		t.Errorf("Cache-Control %q, want no-store", got)
	}
}

func FuzzAnyRequestGetsAnAnswer(f *testing.F) {
	for _, seed := range [][2]string{
		{"GET", "/console/"}, {"GET", "/console"}, {"GET", "/console/static/style.css"}, {"GET", "/console/static/"}, {"GET", "/console/static/.."}, {"POST", "/console/"},
		{"GET", "/"}, {"HEAD", "/console/"}, {"GET", "/console//"}, {"GET", "/console/%2e%2e/"}, {"OPTIONS", "*"},
		{"GET", "/console/?x=<script>"}, {"DELETE", "/console/static/style.css"},
	} {
		f.Add(seed[0], seed[1])
	}
	h := console.New(cluster)
	f.Fuzz(func(t *testing.T, method, target string) {
		req, err := http.NewRequest(method, "http://console.test"+target, nil)
		if err != nil || !strings.HasPrefix(req.URL.Path, "/") {
			t.Skip()
		}
		req.RequestURI = target
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code >= 500 {
			t.Errorf("%s %q: status %d", method, target, rec.Code)
		}
		if rec.Code == http.StatusOK && req.URL.Path != "/console/" && req.URL.Path != "/console/static/style.css" {
			t.Errorf("%s %q: served outside the routes", method, target)
		}
	})
}
