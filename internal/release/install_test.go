package release_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/moostackhq/moostack/internal/release"
)

// archiveOf is a mooctl archive whose mooctl is content.
func archiveOf(t *testing.T, content string) string {
	t.Helper()
	var b bytes.Buffer
	zw := gzip.NewWriter(&b)
	tw := tar.NewWriter(zw)
	if err := tw.WriteHeader(&tar.Header{Name: "mooctl", Mode: 0o755, Size: int64(len(content))}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write([]byte(content)); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return b.String()
}

func TestInstallShInstallsTheReleasesMooctlAndKeepsTheOldOneWhenItRefuses(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("install.sh has no build for " + runtime.GOOS)
	}
	script, err := filepath.Abs(filepath.Join("..", "..", "install.sh"))
	if err != nil {
		t.Fatal(err)
	}
	name := "mooctl-" + runtime.GOOS + "-" + runtime.GOARCH + ".tar.gz"
	built := archiveOf(t, "the release's mooctl")
	for what, c := range map[string]struct {
		files   map[string]string
		refusal string
	}{
		"the release's archive": {files: map[string]string{name: built, "sha256sums.txt": sums(map[string]string{name: built})}},
		"an archive that doesn't match its checksum": {
			files:   map[string]string{name: archiveOf(t, "another mooctl"), "sha256sums.txt": sums(map[string]string{name: built})},
			refusal: "install.sh: " + name + " doesn't match its checksum",
		},
		"an archive sha256sums.txt doesn't list": {
			files:   map[string]string{name: built, "sha256sums.txt": sums(map[string]string{"another.tar.gz": built})},
			refusal: "install.sh: sha256sums.txt has no checksum of " + name,
		},
		"a release without the archive": {
			files:   map[string]string{"sha256sums.txt": sums(map[string]string{name: built})},
			refusal: "/" + name + " failed",
		},
	} {
		t.Run(what, func(t *testing.T) {
			dir := t.TempDir()
			installed := filepath.Join(dir, "mooctl")
			if err := os.WriteFile(installed, []byte("the mooctl installed before"), 0o755); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command("sh", script)
			cmd.Env = append(os.Environ(), "MOOCTL_RELEASE_URL="+serve(t, c.files), "MOOCTL_INSTALL_DIR="+dir)
			out, err := cmd.CombinedOutput()
			got, rerr := os.ReadFile(installed)
			if rerr != nil {
				t.Fatal(rerr)
			}
			if c.refusal == "" {
				if err != nil || string(got) != "the release's mooctl" {
					t.Errorf("install.sh: %v\n%s\n%s holds %q, want the release's mooctl", err, out, installed, got)
				}
				if info, err := os.Stat(installed); err != nil || info.Mode().Perm()&0o111 == 0 {
					t.Errorf("the mooctl installed can't be run: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(string(out), c.refusal) {
				t.Errorf("install.sh: %v\n%s\nwant a refusal with %q", err, out, c.refusal)
			}
			if string(got) != "the mooctl installed before" {
				t.Errorf("%s holds %q after a refused install, want the mooctl installed before", installed, got)
			}
		})
	}
}

func TestInstallShDownloadsFromTheReleaseBuiltFromMain(t *testing.T) {
	script, err := os.ReadFile(filepath.Join("..", "..", "install.sh"))
	if err != nil {
		t.Fatal(err)
	}
	if want := "url=${MOOCTL_RELEASE_URL:-" + release.URL("") + "}"; !strings.Contains(string(script), want) {
		t.Errorf("install.sh doesn't hold %s", want)
	}
}
