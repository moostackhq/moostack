package release_test

import (
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/moostackhq/moostack/internal/release"
)

// sums is a sha256sums.txt that lists each of files, by name.
func sums(files map[string]string) string {
	var b strings.Builder
	for name, content := range files {
		fmt.Fprintf(&b, "%x  %s\n", sha256.Sum256([]byte(content)), name)
	}
	return b.String()
}

// serve serves files, by name, as a release, and returns its URL.
func serve(t *testing.T, files map[string]string) string {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		content, ok := files[strings.TrimPrefix(r.URL.Path, "/edge/")]
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Write([]byte(content))
	}))
	t.Cleanup(server.Close)
	return server.URL + "/edge"
}

func TestFetchDownloadsTheFileNamed(t *testing.T) {
	files := map[string]string{"console-linux-amd64.tar": "the amd64 image", "console-linux-arm64.tar": "the arm64 image", "mooctl-linux-amd64.tar.gz": "mooctl"}
	served := map[string]string{"sha256sums.txt": sums(files)}
	for name, content := range files {
		served[name] = content
	}
	url, dir := serve(t, served), t.TempDir()
	for name, want := range files {
		path, err := release.Fetch(t.Context(), http.DefaultClient, url, name, dir)
		if err != nil || path != filepath.Join(dir, name) {
			t.Errorf("Fetch of %s: %s, %v", name, path, err)
			continue
		}
		if got, _ := os.ReadFile(path); string(got) != want {
			t.Errorf("%s holds %q, want %q", name, got, want)
		}
	}
}

func TestAFileThatIsntTheReleasesIsRefusedAndNotKept(t *testing.T) {
	const name = "console-linux-amd64.tar"
	for what, c := range map[string]struct {
		files map[string]string
		want  string
	}{
		"a file that doesn't match its checksum": {
			map[string]string{name: "another image", "sha256sums.txt": sums(map[string]string{name: "the image"})},
			"doesn't match its checksum",
		},
		"a file cut short": {
			map[string]string{name: "the im", "sha256sums.txt": sums(map[string]string{name: "the image"})},
			"doesn't match its checksum",
		},
		"a file the checksums don't list": {
			map[string]string{name: "the image", "sha256sums.txt": sums(map[string]string{"x" + name: "the image", name + ".sig": "the image"})},
			"sha256sums.txt doesn't list " + name,
		},
		"no checksums": {
			map[string]string{name: "the image"},
			"sha256sums.txt: 404 Not Found",
		},
		"no file": {
			map[string]string{"sha256sums.txt": sums(map[string]string{name: "the image"})},
			name + ": 404 Not Found",
		},
	} {
		dir := t.TempDir()
		path, err := release.Fetch(t.Context(), http.DefaultClient, serve(t, c.files), name, dir)
		if err == nil || !strings.Contains(err.Error(), c.want) || path != "" {
			t.Errorf("%s: %q, %v; want an error with %q", what, path, err, c.want)
		}
		if left, _ := os.ReadDir(dir); len(left) != 0 {
			t.Errorf("%s: %s was kept", what, left[0].Name())
		}
	}
}

func TestTheReleaseIsTheOneBuiltFromTheCommit(t *testing.T) {
	for commit, want := range map[string]string{
		"": "https://github.com/moostackhq/moostack/releases/download/edge",
		"861df463416c35c9042a9df97e5ba73b942469bb": "https://github.com/moostackhq/moostack/releases/download/edge-861df463416c35c9042a9df97e5ba73b942469bb",
	} {
		if got := release.URL(commit); got != want {
			t.Errorf("URL(%q) is %s, want %s", commit, got, want)
		}
	}
}
