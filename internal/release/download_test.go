package release_test

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/moostackhq/moostack/internal/release"
)

// server serves the release source at source over HTTPS, and counts the
// requests for each path.
type server struct {
	*httptest.Server
	// root is the release source served.
	root     string
	mu       sync.Mutex
	requests map[string]int
}

func serve(t *testing.T, source string) *server {
	t.Helper()
	s := &server{root: source, requests: map[string]int{}}
	files := http.FileServer(http.Dir(source))
	s.Server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		s.requests[r.URL.Path]++
		s.mu.Unlock()
		files.ServeHTTP(w, r)
	}))
	t.Cleanup(s.Close)
	return s
}

func (s *server) count(path string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requests[path]
}

func TestDownloadDownloadsTheReleaseOnceAndReadsTheCacheAfterwards(t *testing.T) {
	s := serve(t, writeRelease(t))
	cache := t.TempDir()
	for range 2 {
		r, err := release.Download(context.Background(), s.Client(), s.URL, cache, "1.0.0", nil)
		if err != nil {
			t.Fatal(err)
		}
		data, err := r.ReadFile("arm64", "shim")
		if err != nil {
			t.Fatal(err)
		}
		if string(data) != "shim for arm64" {
			t.Errorf("ReadFile read %q", data)
		}
	}
	for _, p := range []string{"/1.0.0/manifest.yml", "/1.0.0/SHA256SUMS", "/1.0.0/linux-amd64/agent", "/1.0.0/linux-arm64/shim"} {
		if n := s.count(p); n != 1 {
			t.Errorf("%s was requested %d times, want once", p, n)
		}
	}
}

func TestDownloadRefusesAFileThatDoesntMatchItsChecksumAndKeepsNothing(t *testing.T) {
	source := writeRelease(t)
	writeFile(t, filepath.Join(source, "1.0.0", "linux-arm64", "shim"), "tampered")
	s := serve(t, source)
	cache := t.TempDir()
	_, err := release.Download(context.Background(), s.Client(), s.URL, cache, "1.0.0", nil)
	if err == nil || !strings.Contains(err.Error(), "linux-arm64/shim doesn't match its checksum") {
		t.Fatalf("Download of a tampered release: %v", err)
	}
	if left := cached(t, cache); len(left) != 0 {
		t.Errorf("the cache holds %v after a refused download", left)
	}
	// A fixed source downloads anew.
	writeFile(t, filepath.Join(source, "1.0.0", "linux-arm64", "shim"), "shim for arm64")
	if _, err := release.Download(context.Background(), s.Client(), s.URL, cache, "1.0.0", nil); err != nil {
		t.Errorf("Download once the source is fixed: %v", err)
	}
}

func TestDownloadRefusesAMissingFileAndKeepsNothing(t *testing.T) {
	source := writeRelease(t)
	if err := os.Remove(filepath.Join(source, "1.0.0", "linux-amd64", "agent")); err != nil {
		t.Fatal(err)
	}
	s := serve(t, source)
	cache := t.TempDir()
	_, err := release.Download(context.Background(), s.Client(), s.URL, cache, "1.0.0", nil)
	if err == nil || !strings.Contains(err.Error(), "linux-amd64/agent") || !strings.Contains(err.Error(), "404") {
		t.Fatalf("Download of a release missing a file: %v", err)
	}
	if left := cached(t, cache); len(left) != 0 {
		t.Errorf("the cache holds %v after a refused download", left)
	}
}

func TestDownloadOfAMissingReleaseNamesIt(t *testing.T) {
	s := serve(t, writeRelease(t))
	_, err := release.Download(context.Background(), s.Client(), s.URL, t.TempDir(), "2.0.0", nil)
	if err == nil || !strings.Contains(err.Error(), "release 2.0.0") || !strings.Contains(err.Error(), "404") {
		t.Errorf("Download: %v", err)
	}
}

func TestDownloadTakesOnlyHTTPS(t *testing.T) {
	source := writeRelease(t)
	s := httptest.NewServer(http.FileServer(http.Dir(source)))
	defer s.Close()
	_, err := release.Download(context.Background(), s.Client(), s.URL, t.TempDir(), "1.0.0", nil)
	if err == nil || !strings.Contains(err.Error(), "https") {
		t.Errorf("Download over plain HTTP: %v", err)
	}
}

func TestDownloadRefusesAVersionOutsideTheCache(t *testing.T) {
	s := serve(t, writeRelease(t))
	for _, v := range []string{"../1.0.0", "..", ""} {
		if _, err := release.Download(context.Background(), s.Client(), s.URL, t.TempDir(), v, nil); err == nil || !strings.Contains(err.Error(), "isn't a release version") {
			t.Errorf("Download of %q: %v", v, err)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.requests) > 0 {
		t.Errorf("Download sent the requests %v for versions it refuses", s.requests)
	}
}

func TestDownloadRefusesARedirectAwayFromHTTPS(t *testing.T) {
	source := writeRelease(t)
	plain := httptest.NewServer(http.FileServer(http.Dir(source)))
	defer plain.Close()
	s := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, plain.URL+r.URL.Path, http.StatusFound)
	}))
	defer s.Close()
	cache := t.TempDir()
	_, err := release.Download(context.Background(), s.Client(), s.URL, cache, "1.0.0", nil)
	if err == nil || !strings.Contains(err.Error(), "redirect") || !strings.Contains(err.Error(), "https") {
		t.Errorf("Download redirected to plain HTTP: %v", err)
	}
	if left := cached(t, cache); len(left) != 0 {
		t.Errorf("the cache holds %v after a refused download", left)
	}
}

func TestDownloadRefusesASourceWhoseCertificateIsntTrusted(t *testing.T) {
	s := serve(t, writeRelease(t))
	cache := t.TempDir()
	// The default client trusts the system's roots only, not the test
	// server's own.
	_, err := release.Download(context.Background(), nil, s.URL, cache, "1.0.0", nil)
	if err == nil || !strings.Contains(err.Error(), "certificate") {
		t.Errorf("Download from a source with an untrusted certificate: %v", err)
	}
	if left := cached(t, cache); len(left) != 0 {
		t.Errorf("the cache holds %v after a refused download", left)
	}
}

func TestTwoDownloadsOfOneReleaseAtOnceLeaveItWholeInTheCache(t *testing.T) {
	s := serve(t, writeRelease(t))
	cache := t.TempDir()
	var wg sync.WaitGroup
	errs := make([]error, 4)
	for i := range errs {
		wg.Go(func() {
			r, err := release.Download(context.Background(), s.Client(), s.URL, cache, "1.0.0", nil)
			if err == nil {
				_, err = r.ReadFile("arm64", "shim")
			}
			errs[i] = err
		})
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("download %d: %v", i, err)
		}
	}
	if left := cached(t, cache); len(left) != 1 || filepath.Base(left[0]) != "1.0.0" {
		t.Errorf("the cache holds %v, want release 1.0.0 only", left)
	}
}

func TestDownloadDownloadsAgainACachedFileThatDoesntMatchItsChecksum(t *testing.T) {
	s := serve(t, writeRelease(t))
	cache := t.TempDir()
	if _, err := release.Download(context.Background(), s.Client(), s.URL, cache, "1.0.0", nil); err != nil {
		t.Fatal(err)
	}
	// A power loss tore the file once it was in the cache.
	writeFile(t, filepath.Join(cache, cached(t, cache)[0], "linux-arm64", "shim"), "shim for")
	r, err := release.Download(context.Background(), s.Client(), s.URL, cache, "1.0.0", nil)
	if err != nil {
		t.Fatal(err)
	}
	if data, err := r.ReadFile("arm64", "shim"); err != nil || string(data) != "shim for arm64" {
		t.Errorf("ReadFile read %q, %v", data, err)
	}
	if n := s.count("/1.0.0/linux-arm64/shim"); n != 2 {
		t.Errorf("the torn file was downloaded %d times, want twice", n)
	}
}

func TestDownloadRemovesWhatAKilledDownloadLeft(t *testing.T) {
	s := serve(t, writeRelease(t))
	cache := t.TempDir()
	if _, err := release.Download(context.Background(), s.Client(), s.URL, cache, "1.0.0", nil); err != nil {
		t.Fatal(err)
	}
	// A mooctl killed while it downloaded left its download beside the
	// release.
	dir := filepath.Dir(filepath.Join(cache, cached(t, cache)[0]))
	writeFile(t, filepath.Join(dir, ".download-123", "1.0.1", "manifest.yml"), "version: 1.0.1\n")
	if _, err := release.Download(context.Background(), s.Client(), s.URL, cache, "1.0.0", nil); err != nil {
		t.Fatal(err)
	}
	if left := cached(t, cache); len(left) != 1 || filepath.Base(left[0]) != "1.0.0" {
		t.Errorf("the cache holds %v, want release 1.0.0 only", left)
	}
}

// cached are the releases in cache, and the downloads left in it, by
// their paths in it.
func cached(t *testing.T, cache string) []string {
	t.Helper()
	var found []string
	err := filepath.WalkDir(cache, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && strings.HasPrefix(d.Name(), ".download-") {
			found = append(found, path)
			return filepath.SkipDir
		}
		if !d.IsDir() && d.Name() == "manifest.yml" {
			found = append(found, filepath.Dir(path))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for i, f := range found {
		found[i], _ = filepath.Rel(cache, f)
	}
	return found
}

func TestDownloadsFromTwoSourcesOfTheSameReleaseAreKeptApart(t *testing.T) {
	a, b := serve(t, writeRelease(t)), serve(t, writeRelease(t))
	writeFile(t, filepath.Join(b.root, "1.0.0", "linux-arm64", "shim"), "another shim")
	if err := release.Write(filepath.Join(b.root, "1.0.0"), release.Manifest{Version: "1.0.0", Components: []release.Component{
		{Name: "agent", Version: "1.0.0", Files: []string{"agent"}},
		{Name: "containerd", Version: "2.0.0", Files: []string{"containerd", "shim"}},
	}}); err != nil {
		t.Fatal(err)
	}
	cache := t.TempDir()
	for _, c := range []struct {
		s    *server
		want string
	}{{a, "shim for arm64"}, {b, "another shim"}, {a, "shim for arm64"}} {
		r, err := release.Download(context.Background(), c.s.Client(), c.s.URL, cache, "1.0.0", nil)
		if err != nil {
			t.Fatal(err)
		}
		if data, err := r.ReadFile("arm64", "shim"); err != nil || string(data) != c.want {
			t.Errorf("ReadFile of the release from %s read %q, %v, want %q", c.s.URL, data, err, c.want)
		}
	}
	if n := a.count("/1.0.0/linux-arm64/shim"); n != 1 {
		t.Errorf("the first source's shim was downloaded %d times, want once", n)
	}
}

func TestADownloadWaitingForAnotherGivesUpWhenItsContextEnds(t *testing.T) {
	defer release.SetStallTimeout(5 * time.Second)()
	s := stallingSource(t, writeRelease(t), func(w http.ResponseWriter, r *http.Request) {
		for _, b := range []byte("agent for amd64") {
			w.Write([]byte{b})
			w.(http.Flusher).Flush()
			time.Sleep(100 * time.Millisecond)
		}
	})
	cache := t.TempDir()
	first := make(chan error, 1)
	go func() {
		_, err := release.Download(context.Background(), s.Client(), s.URL, cache, "1.0.0", nil)
		first <- err
	}()
	time.Sleep(300 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := release.Download(ctx, s.Client(), s.URL, cache, "1.0.0", nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("the second Download: %v, want it to give up when its context ends", err)
	}
	if took := time.Since(start); took > time.Second {
		t.Errorf("the second Download gave up after %s", took)
	}
	if err := <-first; err != nil {
		t.Errorf("the first Download: %v", err)
	}
}

// stallingSource serves the release source at source over HTTPS, but
// serves linux-amd64/agent with serve.
func stallingSource(t *testing.T, source string, serve func(w http.ResponseWriter, r *http.Request)) *httptest.Server {
	t.Helper()
	files := http.FileServer(http.Dir(source))
	s := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/linux-amd64/agent") {
			serve(w, r)
			return
		}
		files.ServeHTTP(w, r)
	}))
	t.Cleanup(s.Close)
	return s
}

// hang waits until the request ends, or ten seconds.
func hang(r *http.Request) {
	select {
	case <-r.Context().Done():
	case <-time.After(10 * time.Second):
	}
}

func TestDownloadGivesUpOnASourceThatStopsSendingOrNeverAnswers(t *testing.T) {
	defer release.SetStallTimeout(200 * time.Millisecond)()
	for name, serve := range map[string]func(w http.ResponseWriter, r *http.Request){
		"stops sending": func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte("agent"))
			w.(http.Flusher).Flush()
			hang(r)
		},
		"never answers": func(w http.ResponseWriter, r *http.Request) { hang(r) },
	} {
		t.Run(name, func(t *testing.T) {
			s := stallingSource(t, writeRelease(t), serve)
			cache := t.TempDir()
			start := time.Now()
			_, err := release.Download(context.Background(), s.Client(), s.URL, cache, "1.0.0", nil)
			if err == nil || !strings.Contains(err.Error(), "linux-amd64/agent") || !strings.Contains(err.Error(), "no data for 200ms") {
				t.Errorf("Download from a source that %s: %v", name, err)
			}
			if took := time.Since(start); took > 5*time.Second {
				t.Errorf("Download gave up after %s", took)
			}
			if left := cached(t, cache); len(left) != 0 {
				t.Errorf("the cache holds %v after a refused download", left)
			}
		})
	}
}

func TestDownloadWaitsForASourceThatSendsSlowlyButSteadily(t *testing.T) {
	defer release.SetStallTimeout(200 * time.Millisecond)()
	s := stallingSource(t, writeRelease(t), func(w http.ResponseWriter, r *http.Request) {
		for _, b := range []byte("agent for amd64") {
			w.Write([]byte{b})
			w.(http.Flusher).Flush()
			time.Sleep(50 * time.Millisecond)
		}
	})
	r, err := release.Download(context.Background(), s.Client(), s.URL, t.TempDir(), "1.0.0", nil)
	if err != nil {
		t.Fatalf("Download from a slow source: %v", err)
	}
	if data, err := r.ReadFile("amd64", "agent"); err != nil || string(data) != "agent for amd64" {
		t.Errorf("ReadFile read %q, %v", data, err)
	}
}

func TestDownloadRefusesAFileLargerThanAReleaseFileCanBe(t *testing.T) {
	defer release.SetMaxFileSize(10)()
	s := serve(t, writeRelease(t))
	cache := t.TempDir()
	_, err := release.Download(context.Background(), s.Client(), s.URL, cache, "1.0.0", nil)
	if err == nil || !strings.Contains(err.Error(), "larger than 10 bytes") {
		t.Errorf("Download of a file over the limit: %v", err)
	}
	if left := cached(t, cache); len(left) != 0 {
		t.Errorf("the cache holds %v after a refused download", left)
	}
}

func TestDownloadErrorsNeverShowTheSourcesPassword(t *testing.T) {
	s := serve(t, writeRelease(t))
	withPassword := strings.Replace(s.URL, "https://", "https://operator:s3cret@", 1)
	redirects := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://operator:s3cret@127.0.0.1:1"+r.URL.Path, http.StatusFound)
	}))
	defer redirects.Close()
	for name, source := range map[string]string{
		"a missing release":                     withPassword,
		"a plain http source":                   strings.Replace(withPassword, "https://", "http://", 1),
		"a source that redirects to plain http": redirects.URL,
	} {
		client := s.Client()
		if source == redirects.URL {
			client = redirects.Client()
		}
		_, err := release.Download(context.Background(), client, source, t.TempDir(), "2.0.0", nil)
		if err == nil || strings.Contains(err.Error(), "s3cret") {
			t.Errorf("Download of %s: %v, want an error without the password", name, err)
		}
	}
}

// said collects what a download says, safe for the download's goroutines.
type said struct {
	mu    sync.Mutex
	lines []string
}

func (s *said) say(format string, args ...any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lines = append(s.lines, fmt.Sprintf(format, args...))
}

func (s *said) all() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return strings.Join(s.lines, "\n")
}

func TestDownloadSaysWhatItDownloadsAndWhatItWaitsFor(t *testing.T) {
	defer release.SetWaitNotice(50 * time.Millisecond)()
	s := stallingSource(t, writeRelease(t), func(w http.ResponseWriter, r *http.Request) {
		for _, b := range []byte("agent for amd64") {
			w.Write([]byte{b})
			w.(http.Flusher).Flush()
			time.Sleep(30 * time.Millisecond)
		}
	})
	cache := t.TempDir()
	var first, second said
	done := make(chan error, 1)
	go func() {
		_, err := release.Download(context.Background(), s.Client(), s.URL, cache, "1.0.0", first.say)
		done <- err
	}()
	time.Sleep(100 * time.Millisecond)
	if _, err := release.Download(context.Background(), s.Client(), s.URL, cache, "1.0.0", second.say); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := first.all(); got != "Downloading release 1.0.0 from "+s.URL {
		t.Errorf("the first download said %q", got)
	}
	if got := second.all(); got != "Waiting for another mooctl's download from "+s.URL {
		t.Errorf("the second download, which found the release in the cache once it was its turn, said %q", got)
	}
}
