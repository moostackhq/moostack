package console_test

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/moostackhq/moostack/internal/console"
	"github.com/moostackhq/moostack/internal/orchestrator"
	"github.com/moostackhq/moostack/internal/orchestrator/orchestratortest"
)

var update = flag.Bool("update", false, "rewrite the golden files")

var cluster = &orchestratortest.Fake{Containers: []orchestrator.Container{
	{Namespace: "kube-system", Pod: "coredns-7b98449c4-x2x9v", Name: "coredns", Image: "rancher/mirrored-coredns-coredns:1.12.0"},
	{Namespace: "shop", Pod: "web-f6dfb8fb4-f9fsd", Name: "web", Image: "registry.moostack.internal:5000/shop/web@sha256:0a1b"},
	{Namespace: "shop", Pod: "web-f6dfb8fb4-f9fsd", Name: "worker", Image: "registry.moostack.internal:5000/shop/worker@sha256:2c3d"},
	{Namespace: "shop", Pod: "web-f6dfb8fb4-k2lmn", Name: "web", Image: "registry.moostack.internal:5000/shop/web@sha256:0a1b"},
}}

// golden compares body with the golden file name in testdata, which
// -update rewrites.
func golden(t *testing.T, name, body string) {
	t.Helper()
	path := filepath.Join("testdata", name)
	if *update {
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if body != string(want) {
		t.Errorf("the page isn't %s; run with -update and review the change:\n%s", path, body)
	}
}

// recordOutput takes over the log, standard output and standard error
// until the test ends, and returns a function that returns what was
// written to them so far.
func recordOutput(t *testing.T) func() string {
	t.Helper()
	var log bytes.Buffer
	var mu sync.Mutex
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(writerFunc(func(p []byte) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		return log.Write(p)
	}), &slog.HandlerOptions{Level: slog.LevelDebug})))
	output, err := os.Create(filepath.Join(t.TempDir(), "output"))
	if err != nil {
		t.Fatal(err)
	}
	stdout, stderr := os.Stdout, os.Stderr
	os.Stdout, os.Stderr = output, output
	t.Cleanup(func() {
		slog.SetDefault(old)
		os.Stdout, os.Stderr = stdout, stderr
		output.Close()
	})
	return func() string {
		printed, err := os.ReadFile(output.Name())
		if err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		defer mu.Unlock()
		return log.String() + string(printed)
	}
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

// socketPath returns a path for a token socket in a new directory.
func socketPath(t testing.TB) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "console")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return filepath.Join(dir, "console.sock")
}

// A nil session sends no cookie, and a nil form no body.
func send(t *testing.T, c *console.Console, method, path string, session *http.Cookie, form url.Values) (*http.Response, string) {
	t.Helper()
	var reqBody io.Reader
	if form != nil {
		reqBody = strings.NewReader(form.Encode())
	}
	req := httptest.NewRequest(method, path, reqBody)
	if form != nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	if session != nil {
		req.AddCookie(session)
	}
	rec := httptest.NewRecorder()
	c.ServeHTTP(rec, req)
	res := rec.Result()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	return res, string(body)
}

// login logs in to c with token and returns the answer and the session's
// cookie, nil if the answer sets none.
func login(t *testing.T, c *console.Console, token string) (*http.Response, string, *http.Cookie) {
	t.Helper()
	res, body := send(t, c, http.MethodPost, "/console/login", nil, url.Values{"token": {token}})
	for _, cookie := range res.Cookies() {
		if cookie.Value != "" {
			return res, body, cookie
		}
	}
	return res, body, nil
}

func newSession(t *testing.T, c *console.Console) *http.Cookie {
	t.Helper()
	res, body, session := login(t, c, c.IssueToken())
	if session == nil {
		t.Fatalf("logging in with a new token: status %d, no session:\n%s", res.StatusCode, body)
	}
	return session
}

func newSessionWith(t *testing.T, c *console.Console, token string) *http.Cookie {
	t.Helper()
	res, body, session := login(t, c, token)
	if session == nil {
		t.Fatalf("logging in: status %d, no session:\n%s", res.StatusCode, body)
	}
	return session
}

// refused checks that an answer to a login is the login page saying the
// token wasn't accepted, with no session.
func refused(t *testing.T, what string, res *http.Response, body string, session *http.Cookie) {
	t.Helper()
	if res.StatusCode != http.StatusUnauthorized || !strings.Contains(body, "The token wasn&#39;t accepted.") || !strings.Contains(body, `name="token"`) {
		t.Errorf("%s: status %d, want the login page saying the token wasn't accepted:\n%s", what, res.StatusCode, body)
	}
	if session != nil {
		t.Errorf("%s: a session", what)
	}
}

func TestTheStylesheetIsServed(t *testing.T) {
	res, body := send(t, console.New(cluster), http.MethodGet, "/console/static/style.css", nil, nil)
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
	res, _ := send(t, console.New(cluster), http.MethodGet, "/console", nil, nil)
	if res.StatusCode != http.StatusPermanentRedirect || res.Header.Get("Location") != "/console/" {
		t.Errorf("status %d to %q, want 308 to /console/", res.StatusCode, res.Header.Get("Location"))
	}
}

func TestNothingIsServedOutsideTheRoutes(t *testing.T) {
	for _, path := range []string{"/", "/style.css", "/console/nothing", "/console/style.css", "/console/static/", "/console/static/nothing.css", "/console/static/style.css/x", "/console/templates/containers.html", "/consolex"} {
		if res, _ := send(t, console.New(cluster), http.MethodGet, path, nil, nil); res.StatusCode != http.StatusNotFound {
			t.Errorf("GET %s: status %d, want 404", path, res.StatusCode)
		}
	}
}

func FuzzAnyRequestWithoutASessionGetsOnlyTheLoginPageAndStaticFiles(f *testing.F) {
	for _, seed := range [][3]string{
		{"GET", "/console/", ""}, {"GET", "/console", ""}, {"GET", "/console/static/style.css", ""}, {"GET", "/console/static/", ""},
		{"GET", "/console/static/..", ""}, {"POST", "/console/", ""}, {"GET", "/", ""}, {"HEAD", "/console/", ""}, {"GET", "/console//", ""},
		{"GET", "/console/%2e%2e/", ""}, {"OPTIONS", "*", ""}, {"GET", "/console/?x=<script>", ""}, {"DELETE", "/console/static/style.css", ""},
		{"GET", "/console/login", ""}, {"POST", "/console/login", ""}, {"POST", "/console/logout", ""}, {"GET", "/console/logout", ""},
		{"POST", "/console/login?token=AAAAAAAAAAAAAAAAAAAAAAAAAA", ""}, {"GET", "/console/login/", ""},
		{"POST", "/console/login", "token=AAAAAAAAAAAAAAAAAAAAAAAAAA"}, {"POST", "/console/login", "token=%zz"}, {"POST", "/console/login", "token"},
		{"POST", "/console/login", "token=A&token=B;x"}, {"POST", "/console/logout", "token=AAAAAAAAAAAAAAAAAAAAAAAAAA"},
	} {
		f.Add(seed[0], seed[1], seed[2])
	}
	h := console.New(cluster)
	f.Fuzz(func(t *testing.T, method, target, form string) {
		req, err := http.NewRequest(method, "http://console.test"+target, strings.NewReader(form))
		if err != nil || !strings.HasPrefix(req.URL.Path, "/") {
			t.Skip()
		}
		req.RequestURI = target
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code >= 500 {
			t.Errorf("%s %q with %q: status %d", method, target, form, rec.Code)
		}
		if rec.Code == http.StatusOK && req.URL.Path != "/console/login" && req.URL.Path != "/console/static/style.css" {
			t.Errorf("%s %q with %q: served without a session", method, target, form)
		}
		if len(rec.Result().Cookies()) != 0 {
			t.Errorf("%s %q with %q: a cookie without a token or a session", method, target, form)
		}
	})
}

func TestRecordOutputRecordsTheLogStandardOutputAndStandardError(t *testing.T) {
	written := recordOutput(t)
	slog.Info("to the log")
	fmt.Fprintln(os.Stdout, "to standard output")
	fmt.Fprintln(os.Stderr, "to standard error")
	for _, want := range []string{"to the log", "to standard output", "to standard error"} {
		if !strings.Contains(written(), want) {
			t.Errorf("the record lacks %q:\n%s", want, written())
		}
	}
}

func TestEveryRouteForbidsFramingAndOtherOrigins(t *testing.T) {
	c := console.New(cluster)
	session := newSession(t, c)
	for _, request := range [][2]string{
		{http.MethodGet, "/console"}, {http.MethodGet, "/console/"}, {http.MethodGet, "/console/login"}, {http.MethodPost, "/console/login"},
		{http.MethodPost, "/console/logout"}, {http.MethodGet, "/console/static/style.css"}, {http.MethodGet, "/nothing"},
	} {
		for _, cookie := range []*http.Cookie{nil, session} {
			res, _ := send(t, c, request[0], request[1], cookie, nil)
			if got := res.Header.Get("Content-Security-Policy"); got != "default-src 'self'; frame-ancestors 'none'" {
				t.Errorf("%s %s: Content-Security-Policy %q", request[0], request[1], got)
			}
			if got := res.Header.Get("X-Content-Type-Options"); got != "nosniff" {
				t.Errorf("%s %s: X-Content-Type-Options %q", request[0], request[1], got)
			}
		}
	}
}
