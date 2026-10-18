package release_test

import (
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/moostackhq/moostack/internal/release"
)

// writeRelease writes release 1.0.0 into a new release source, with an
// agent at 1.0.0 and containerd at 2.0.0, and returns the source.
func writeRelease(t *testing.T) string {
	t.Helper()
	source := t.TempDir()
	dir := filepath.Join(source, "1.0.0")
	for _, arch := range release.Architectures {
		for _, f := range []string{"agent", "containerd", "shim"} {
			writeFile(t, filepath.Join(dir, "linux-"+arch, f), f+" for "+arch)
		}
	}
	m := release.Manifest{Version: "1.0.0", Components: []release.Component{
		{Name: "agent", Version: "1.0.0", Files: []string{"agent"}},
		{Name: "containerd", Version: "2.0.0", Files: []string{"containerd", "shim"}},
	}}
	if err := release.Write(dir, m); err != nil {
		t.Fatal(err)
	}
	return source
}

func TestOpenReadsWhatWriteWrote(t *testing.T) {
	r, err := release.Open(writeRelease(t), "1.0.0")
	if err != nil {
		t.Fatal(err)
	}
	c, ok := r.Component("containerd")
	if !ok || c.Version != "2.0.0" || strings.Join(c.Files, " ") != "containerd shim" {
		t.Errorf("containerd is %+v, %v", c, ok)
	}
	data, err := r.ReadFile("arm64", "shim")
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "shim for arm64" {
		t.Errorf("ReadFile read %q", data)
	}
}

func TestOpenOfAMissingReleaseNamesIt(t *testing.T) {
	_, err := release.Open(writeRelease(t), "2.0.0")
	if err == nil || !strings.Contains(err.Error(), "release 2.0.0 isn't in") {
		t.Errorf("Open: %v", err)
	}
}

func TestReadFileRefusesATamperedFile(t *testing.T) {
	source := writeRelease(t)
	r, err := release.Open(source, "1.0.0")
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(source, "1.0.0", "linux-amd64", "agent"), "something else")
	if _, err := r.ReadFile("amd64", "agent"); err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Errorf("ReadFile of a tampered file: %v", err)
	}
}

func TestOpenRefusesAFileWithoutAChecksum(t *testing.T) {
	source := writeRelease(t)
	sums := filepath.Join(source, "1.0.0", "SHA256SUMS")
	data, err := os.ReadFile(sums)
	if err != nil {
		t.Fatal(err)
	}
	var kept []string
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if !strings.HasSuffix(line, "linux-arm64/shim") {
			kept = append(kept, line)
		}
	}
	writeFile(t, sums, strings.Join(kept, "\n")+"\n")
	if _, err := release.Open(source, "1.0.0"); err == nil || !strings.Contains(err.Error(), "linux-arm64/shim has no checksum") {
		t.Errorf("Open: %v", err)
	}
}

func TestOpenRefusesAManifestOfAnotherVersion(t *testing.T) {
	source := writeRelease(t)
	if err := os.Rename(filepath.Join(source, "1.0.0"), filepath.Join(source, "1.0.1")); err != nil {
		t.Fatal(err)
	}
	if _, err := release.Open(source, "1.0.1"); err == nil || !strings.Contains(err.Error(), "for release 1.0.0") {
		t.Errorf("Open: %v", err)
	}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestOpenRefusesAVersionOutsideTheSource(t *testing.T) {
	source := writeRelease(t)
	for _, v := range []string{"../1.0.0", "1.0.0/..", "..", ".hidden", ""} {
		if _, err := release.Open(filepath.Join(source, "sub"), v); err == nil || !strings.Contains(err.Error(), "isn't a release version") {
			t.Errorf("Open of %q: %v", v, err)
		}
	}
}

func TestOpenRefusesFileNamesThatArentPlain(t *testing.T) {
	for _, name := range []string{"../../etc/sudoers.d/x", "bin/agent", `..\agent`, "..", "."} {
		source := writeRelease(t)
		dir := filepath.Join(source, "1.0.0")
		manifest := filepath.Join(dir, "manifest.yml")
		data, err := os.ReadFile(manifest)
		if err != nil {
			t.Fatal(err)
		}
		writeFile(t, manifest, strings.Replace(string(data), "- agent\n", "- '"+name+"'\n", 1))
		// The checksums list the file at the path a read would take, as a
		// forged release would.
		sums, err := os.ReadFile(filepath.Join(dir, "SHA256SUMS"))
		if err != nil {
			t.Fatal(err)
		}
		var extra string
		for _, arch := range release.Architectures {
			extra += strings.Repeat("0", 64) + "  " + path.Join("linux-"+arch, name) + "\n"
		}
		writeFile(t, filepath.Join(dir, "SHA256SUMS"), string(sums)+extra)
		if _, err := release.Open(source, "1.0.0"); err == nil || !strings.Contains(err.Error(), "isn't a plain file name") {
			t.Errorf("Open of a manifest listing %q: %v", name, err)
		}
	}
}
