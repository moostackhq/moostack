package console_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/moostackhq/moostack/internal/console"
	"github.com/moostackhq/moostack/internal/model"
	"github.com/moostackhq/moostack/internal/orchestrator"
	"github.com/moostackhq/moostack/internal/orchestrator/orchestratortest"
)

// containerPage asks a console over o for the page of the container id,
// with a session.
func containerPage(t *testing.T, o orchestrator.Orchestrator, id string) (*http.Response, string) {
	t.Helper()
	c := console.New(o)
	return send(t, c, http.MethodGet, "/console/container?id="+url.QueryEscape(id), newSession(t, c), nil)
}

// dns is a cluster whose one container, with the ID "sys/dns-1/dns",
// logged logs.
func dns(logs string) *orchestratortest.Fake {
	return &orchestratortest.Fake{
		Own:             []model.Running{{ID: "sys/dns-1/dns", Instance: "dns-1", Container: "dns", Image: "example.test/dns:1", Started: time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)}},
		ContainerLogged: map[string]string{"sys/dns-1/dns": logs},
	}
}

func TestTheContainerPageShowsItsNameInstanceImageStartAndLogs(t *testing.T) {
	f := &orchestratortest.Fake{
		Own:             cluster.Own,
		ContainerLogged: map[string]string{"/coredns-7b98449c4-x2x9v/coredns": ".:53\n[INFO] plugin/reload: Running configuration\nCoreDNS-1.12.0\n"},
	}
	res, body := containerPage(t, f, "/coredns-7b98449c4-x2x9v/coredns")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200", res.StatusCode)
	}
	if got := res.Header.Get("Cache-Control"); got != "no-store" {
		t.Errorf("Cache-Control %q, want no-store", got)
	}
	golden(t, "container.golden.html", body)
}

func TestTheContainerPageOfAnAppsContainerShowsThatInstance(t *testing.T) {
	_, body := containerPage(t, cluster, "shop/web-2/web")
	for _, shown := range []string{"<td>web-2</td>", "<td>registry.moostack.internal:5000/shop/web@sha256:0a1b</td>", ">2026-01-02 03:04:05 UTC</time>", ">web-2 web: running\n</pre>"} {
		if !strings.Contains(body, shown) {
			t.Errorf("the page doesn't show %s:\n%s", shown, body)
		}
	}
	if strings.Contains(body, "web-1") {
		t.Errorf("the page shows another instance:\n%s", body)
	}
}

func TestTheStartIsShownInUTC(t *testing.T) {
	_, body := containerPage(t, cluster, "/coredns-7b98449c4-x2x9v/coredns")
	if want := `<time datetime="2026-01-01T08:30:00Z">2026-01-01 08:30:00 UTC</time>`; !strings.Contains(body, want) {
		t.Errorf("the page doesn't show %s:\n%s", want, body)
	}
}

func TestAContainerThatLoggedNothingSaysSo(t *testing.T) {
	res, body := containerPage(t, dns(""), "sys/dns-1/dns")
	if res.StatusCode != http.StatusOK || !strings.Contains(body, "No logs.") || strings.Contains(body, "<pre") {
		t.Errorf("status %d, want 200 and a page saying there are no logs:\n%s", res.StatusCode, body)
	}
}

// asked records the options ContainerLogs was called with.
type asked struct {
	orchestrator.Orchestrator
	options []orchestrator.LogOptions
}

func (a *asked) ContainerLogs(ctx context.Context, id string, o orchestrator.LogOptions, w io.Writer) error {
	a.options = append(a.options, o)
	return a.Orchestrator.ContainerLogs(ctx, id, o, w)
}

func TestThePageAsksForTheLast100LinesAndDoesntFollow(t *testing.T) {
	a := &asked{Orchestrator: dns("up\n")}
	containerPage(t, a, "sys/dns-1/dns")
	if len(a.options) != 1 || a.options[0] != (orchestrator.LogOptions{Tail: 100}) {
		t.Errorf("the page asked for the logs with %+v, want once, the last 100 lines", a.options)
	}
}

func TestLogsOverOneMebibyteAreCutThere(t *testing.T) {
	line := strings.Repeat("x", 1023) + "\n"
	for name, logs := range map[string]string{
		"exactly the most": strings.Repeat(line, 1024),
		"one byte more":    strings.Repeat(line, 1024) + "y",
		"twice the most":   strings.Repeat(line, 2048),
	} {
		res, body := containerPage(t, dns(logs), "sys/dns-1/dns")
		if res.StatusCode != http.StatusOK {
			t.Errorf("%s: status %d, want 200", name, res.StatusCode)
		}
		_, logs, _ := strings.Cut(body, "<pre")
		_, logs, _ = strings.Cut(logs, ">")
		logs, _, _ = strings.Cut(logs, "</pre>")
		if len(logs) != 1<<20 || strings.Contains(logs, "y") {
			t.Errorf("%s: the page shows %d bytes of logs, want the first %d", name, len(logs), 1<<20)
		}
	}
}

func TestNamesAndLogsOnTheContainerPageAreEscaped(t *testing.T) {
	f := &orchestratortest.Fake{
		Own:             []model.Running{{ID: `"><script>i()</script>`, Instance: `"><script>p()</script>`, Container: "<i>c</i>", Image: "<script>alert(1)</script>"}},
		ContainerLogged: map[string]string{`"><script>i()</script>`: "</pre><script>alert(2)</script>\n"},
	}
	res, body := containerPage(t, f, `"><script>i()</script>`)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200:\n%s", res.StatusCode, body)
	}
	for _, raw := range []string{"<script>", "<i>c</i>"} {
		if strings.Contains(body, raw) {
			t.Errorf("the page holds %s unescaped:\n%s", raw, body)
		}
	}
	for _, shown := range []string{"&lt;i&gt;c&lt;/i&gt;", "&lt;script&gt;alert(1)&lt;/script&gt;", "&lt;/pre&gt;&lt;script&gt;alert(2)&lt;/script&gt;"} {
		if !strings.Contains(body, shown) {
			t.Errorf("the page doesn't show %s:\n%s", shown, body)
		}
	}
}

func TestAContainerThatDoesntRunIsNotFound(t *testing.T) {
	gone := dns("up\n")
	gone.Fail("ContainerLogs", orchestrator.ErrNoContainer)
	for name, at := range map[string]struct {
		cluster orchestrator.Orchestrator
		id      string
	}{
		"an unknown ID":               {cluster, "shop/web-9/web"},
		"no ID":                       {cluster, ""},
		"one that ended as it's read": {gone, "sys/dns-1/dns"},
	} {
		res, body := containerPage(t, at.cluster, at.id)
		if res.StatusCode != http.StatusNotFound || !strings.Contains(body, "No such running container.") {
			t.Errorf("%s: status %d, want the 404 page:\n%s", name, res.StatusCode, body)
		}
		if strings.Contains(body, "<table") {
			t.Errorf("%s: the page shows a container:\n%s", name, body)
		}
	}
}

func TestLogsThatCantBeReadAreAnErrorOnTheContainersPage(t *testing.T) {
	f := dns("up\n")
	f.Fail("ContainerLogs", errors.New("dial tcp 10.43.0.1:443: connection refused"))
	res, body := containerPage(t, f, "sys/dns-1/dns")
	if res.StatusCode != http.StatusBadGateway || !strings.Contains(body, "The logs can&#39;t be read.") {
		t.Errorf("status %d, want 502 and a page saying the logs can't be read:\n%s", res.StatusCode, body)
	}
	if !strings.Contains(body, "<td>dns-1</td>") {
		t.Errorf("the page doesn't show the container:\n%s", body)
	}
	if strings.Contains(body, "10.43.0.1") || strings.Contains(body, "No logs.") {
		t.Errorf("the page shows the error's detail, or says there are no logs:\n%s", body)
	}
}

func TestTheContainerPageOfAClusterThatCantBeReadIsAnErrorPage(t *testing.T) {
	f := dns("up\n")
	f.Fail("Running", errors.New("dial tcp 10.43.0.1:443: connection refused"))
	res, body := containerPage(t, f, "sys/dns-1/dns")
	if res.StatusCode != http.StatusBadGateway || !strings.Contains(body, "The cluster can&#39;t be read.") || strings.Contains(body, "10.43.0.1") {
		t.Errorf("status %d, want the 502 page without the error's detail:\n%s", res.StatusCode, body)
	}
}

// stalledLogs is a cluster whose logs never come.
type stalledLogs struct{ orchestrator.Orchestrator }

func (stalledLogs) ContainerLogs(ctx context.Context, _ string, _ orchestrator.LogOptions, _ io.Writer) error {
	<-ctx.Done()
	return ctx.Err()
}

func TestLogsThatDontComeAreAnErrorAfter20Seconds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		res, body := containerPage(t, stalledLogs{dns("up\n")}, "sys/dns-1/dns")
		if res.StatusCode != http.StatusBadGateway || !strings.Contains(body, "The logs can&#39;t be read.") {
			t.Errorf("status %d, want 502 and a page saying the logs can't be read:\n%s", res.StatusCode, body)
		}
		if took := time.Since(start); took != 20*time.Second {
			t.Errorf("the page took %v, want 20s", took)
		}
	})
}

func TestWithoutASessionTheContainerPageRedirectsToTheLoginPage(t *testing.T) {
	c := console.New(cluster)
	for name, session := range map[string]*http.Cookie{
		"no cookie":      nil,
		"an unknown one": {Name: "__Secure-console", Value: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
	} {
		res, body := send(t, c, http.MethodGet, "/console/container?id=shop%2Fweb-1%2Fweb", session, nil)
		if res.StatusCode != http.StatusSeeOther || res.Header.Get("Location") != "/console/login" {
			t.Errorf("%s: status %d to %q, want 303 to /console/login", name, res.StatusCode, res.Header.Get("Location"))
		}
		if strings.Contains(body, "web-1") {
			t.Errorf("%s: the answer shows the container:\n%s", name, body)
		}
	}
}

func TestOnlyGetIsAllowedOnTheContainerPage(t *testing.T) {
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch} {
		rec := httptest.NewRecorder()
		console.New(cluster).ServeHTTP(rec, httptest.NewRequest(method, "/console/container?id=shop%2Fweb-1%2Fweb", nil))
		if rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s /console/container: status %d, want 405", method, rec.Code)
		}
	}
}
