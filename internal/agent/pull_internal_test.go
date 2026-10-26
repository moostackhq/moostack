//go:build linux

package agent

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/containerd/containerd/v2/pkg/reference"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
)

// These tests resolve references as a pull does, with the resolver and
// the transport a pull uses, against registries faked on this machine,
// and check the class each failure is told apart as.

// redirected is a transport that sends every request to addr through
// transport, whatever its host, which the request keeps in its Host
// header, so that a fake registry can answer for docker.io and its token
// service alike. The response is to the request as it was sent, since
// the resolver keys what a challenge asks of a host by the host the
// response names.
type redirected struct {
	transport http.RoundTripper
	addr      string
}

// to is a transport that sends every request to srv.
func to(srv *httptest.Server) redirected {
	return redirected{srv.Client().Transport, srv.Listener.Addr().String()}
}

func (t redirected) RoundTrip(req *http.Request) (*http.Response, error) {
	out := req.Clone(req.Context())
	out.Host = req.URL.Host
	out.URL.Host = t.addr
	resp, err := t.transport.RoundTrip(out)
	if resp != nil {
		resp.Request = req
	}
	return resp, err
}

// recorded is a response that Docker Hub answered on 2026-10-25, kept in
// testdata/dockerhub as it came, but for the address it named and the
// token it issued.
func recorded(t *testing.T, name string) *http.Response {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "dockerhub", name+".http"))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(bytes.NewReader(data)), nil)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// replay writes resp to w.
func replay(w http.ResponseWriter, resp *http.Response) {
	for k, v := range resp.Header {
		w.Header()[k] = v
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
	resp.Body.Close()
}

// fakeRegistry is a registry that answers each request with what answer
// gives for it, and counts the requests for each path.
type fakeRegistry struct {
	srv *httptest.Server
	mu  sync.Mutex
	got map[string]int
}

func newFakeRegistry(t *testing.T, answer func(r *http.Request) *http.Response) *fakeRegistry {
	t.Helper()
	f := &fakeRegistry{got: map[string]int{}}
	f.srv = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.got[r.Host+r.URL.Path]++
		f.mu.Unlock()
		replay(w, answer(r))
	}))
	t.Cleanup(f.srv.Close)
	return f
}

// requests is how many requests the registry got for host and path.
func (f *fakeRegistry) requests(hostPath string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.got[hostPath]
}

// resolveError is why the resolver of a pull, through transport, fails
// to resolve ref, as the pull answers it.
func resolveError(t *testing.T, transport http.RoundTripper, ref string) error {
	t.Helper()
	_, _, err := newResolver(transport).Resolve(t.Context(), ref)
	if err == nil {
		t.Fatalf("resolving %s succeeded", ref)
	}
	return pullError(ref, err)
}

// dockerHub is a fake of Docker Hub that answers as it answered on
// 2026-10-25: a challenge to fetch a token, then an unknown tag, an
// unknown repository, or a rate limit.
func dockerHub(t *testing.T) *fakeRegistry {
	return newFakeRegistry(t, func(r *http.Request) *http.Response {
		authorized := r.Header.Get("Authorization") != ""
		switch {
		case r.Host == "auth.docker.io":
			return recorded(t, "token")
		case r.URL.Path == "/v2/library/alpine/manifests/no-such-tag-xyz" && authorized:
			return recorded(t, "manifest-unknown")
		case r.URL.Path == "/v2/library/alpine/manifests/no-such-tag-xyz":
			return recorded(t, "challenge-alpine")
		case r.URL.Path == "/v2/moostack-no-such-user/no-such-repo/manifests/latest" && authorized:
			return recorded(t, "insufficient-scope")
		case r.URL.Path == "/v2/moostack-no-such-user/no-such-repo/manifests/latest":
			return recorded(t, "challenge-missing")
		case authorized:
			return recorded(t, "toomanyrequests")
		}
		return recorded(t, "challenge-alpine")
	})
}

func TestAPullTellsDockerHubsAnswersApart(t *testing.T) {
	hub := dockerHub(t)
	for ref, want := range map[string]codes.Code{
		"docker.io/library/alpine:no-such-tag-xyz":            codes.NotFound,
		"docker.io/moostack-no-such-user/no-such-repo:latest": codes.PermissionDenied,
		"docker.io/library/alpine:3.20":                       codes.ResourceExhausted,
	} {
		if err := resolveError(t, to(hub.srv), ref); status.Code(err) != want {
			t.Errorf("pulling %s from Docker Hub: %v, want %s", ref, err, want)
		}
	}
}

// containerd's resolver retries a 429 at once, up to five times, which
// would spend five of the pulls Docker Hub allows; a pull asks once.
func TestARateLimitedPullFailsAtTheFirstAnswer(t *testing.T) {
	hub := dockerHub(t)
	resolveError(t, to(hub.srv), "docker.io/library/alpine:3.20")
	if n := hub.requests("registry-1.docker.io/v2/library/alpine/manifests/3.20"); n != 2 {
		t.Errorf("the pull asked for the manifest %d times, want twice: once for the challenge, and once rate limited", n)
	}
	limited := newFakeRegistry(t, func(*http.Request) *http.Response { return recorded(t, "toomanyrequests") })
	if err := resolveError(t, to(limited.srv), "registry.test/app:1"); status.Code(err) != codes.ResourceExhausted {
		t.Errorf("a pull from a registry that rate limits it: %v, want RESOURCE_EXHAUSTED", err)
	}
	if n := limited.requests("registry.test/v2/app/manifests/1"); n != 1 {
		t.Errorf("the pull asked a registry that rate limits it %d times, want once", n)
	}
}

func TestAPullTellsARegistrysFailuresApart(t *testing.T) {
	answering := func(code int, header http.Header) http.RoundTripper {
		return to(newFakeRegistry(t, func(*http.Request) *http.Response {
			return &http.Response{StatusCode: code, Header: header, Body: io.NopCloser(bytes.NewReader(nil))}
		}).srv)
	}
	closed, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closed.Close()
	refused := redirected{http.DefaultTransport.(*http.Transport).Clone(), closed.Addr().String()}
	untrustedSrv := httptest.NewTLSServer(http.NotFoundHandler())
	t.Cleanup(untrustedSrv.Close)
	// The pull's own transport, which trusts the system's CAs alone.
	untrusted := redirected{pullTransport(), untrustedSrv.Listener.Addr().String()}
	challenge := http.Header{"Www-Authenticate": {`Bearer realm="https://auth.registry.test/token",service="registry.test"`}}
	limitedToken := to(newFakeRegistry(t, func(r *http.Request) *http.Response {
		if r.Host == "auth.registry.test" {
			return &http.Response{StatusCode: http.StatusTooManyRequests, Body: io.NopCloser(bytes.NewReader(nil))}
		}
		return &http.Response{StatusCode: http.StatusUnauthorized, Header: challenge, Body: io.NopCloser(bytes.NewReader(nil))}
	}).srv)
	for name, c := range map[string]struct {
		transport http.RoundTripper
		want      codes.Code
	}{
		"404":                              {answering(http.StatusNotFound, nil), codes.NotFound},
		"401 without a challenge":          {answering(http.StatusUnauthorized, nil), codes.PermissionDenied},
		"403":                              {answering(http.StatusForbidden, nil), codes.PermissionDenied},
		"429":                              {answering(http.StatusTooManyRequests, nil), codes.ResourceExhausted},
		"500":                              {answering(http.StatusInternalServerError, nil), codes.Unavailable},
		"502":                              {answering(http.StatusBadGateway, nil), codes.Unavailable},
		"503":                              {answering(http.StatusServiceUnavailable, nil), codes.Unavailable},
		"504":                              {answering(http.StatusGatewayTimeout, nil), codes.Unavailable},
		"a refused connection":             {refused, codes.Unavailable},
		"a certificate not trusted":        {untrusted, codes.Unavailable},
		"a token service that rate limits": {limitedToken, codes.ResourceExhausted},
	} {
		if err := resolveError(t, c.transport, "registry.test/app:1"); status.Code(err) != c.want {
			t.Errorf("%s: %v, want %s", name, err, c.want)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, err := newResolver(answering(http.StatusNotFound, nil)).Resolve(ctx, "registry.test/app:1"); status.Code(pullError("registry.test/app:1", err)) != codes.Canceled {
		t.Errorf("a pull whose call ended: %v, want CANCELED", pullError("registry.test/app:1", err))
	}
}

// Whatever reference a pull takes, containerd's resolver takes as it is:
// it parses, as the same reference, with a tag or a digest to resolve.
func FuzzPullImageReference(f *testing.F) {
	for _, seed := range []string{"docker.io/library/nginx:1.27", "nginx", "registry.test:5000/a/b@sha256:" + strings.Repeat("a", 64),
		"REGISTRY.test/web:1", "localhost/app:1", "[::1]:5000/app:1", "a.b/c:d@sha256:" + strings.Repeat("b", 64)} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, ref string) {
		if nodev1.CheckReference(ref) != nil {
			return
		}
		spec, err := reference.Parse(ref)
		if err != nil {
			t.Fatalf("a pull takes %q, which containerd doesn't parse: %v", ref, err)
		}
		if spec.Object == "" || spec.String() != ref {
			t.Fatalf("a pull takes %q, which containerd parses as %q, with the object %q", ref, spec.String(), spec.Object)
		}
	})
}
