package release_test

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/moostackhq/moostack/internal/buildid"
	"github.com/moostackhq/moostack/internal/ociimage"
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

func TestOpenReadsThePreviousReleaseWriteWrote(t *testing.T) {
	source := t.TempDir()
	dir := filepath.Join(source, "1.1.0")
	writeFile(t, filepath.Join(dir, "linux-amd64", "agent"), "a")
	writeFile(t, filepath.Join(dir, "linux-arm64", "agent"), "a")
	m := release.Manifest{Version: "1.1.0", Previous: "1.0.0", Components: []release.Component{{Name: "agent", Version: "1.1.0", Files: []string{"agent"}}}}
	if err := release.Write(dir, m); err != nil {
		t.Fatal(err)
	}
	r, err := release.Open(source, "1.1.0")
	if err != nil {
		t.Fatal(err)
	}
	if r.Previous != "1.0.0" {
		t.Errorf("Previous is %q, want 1.0.0", r.Previous)
	}
}

// Every part of a node that says which release it runs, the agent, the
// updater and each cluster service, runs the release it came in, which
// the rule of which release may update a node relies on.
func TestOpenRefusesAnAgentUpdaterOrServiceAtAnotherVersion(t *testing.T) {
	for _, c := range []struct{ name, agent, updater, service string }{
		{"agent", "1.0.1", "1.0.0", "1.0.0"},
		{"updater", "1.0.0", "1.0.0-broken", "1.0.0"},
		{"dns", "1.0.0", "1.0.0", "0.9.0"},
	} {
		t.Run(c.name, func(t *testing.T) {
			source := t.TempDir()
			dir := filepath.Join(source, "1.0.0")
			for _, arch := range release.Architectures {
				for _, f := range []string{"agent", "updater", "dns.tar"} {
					writeFile(t, filepath.Join(dir, "linux-"+arch, f), f+" for "+arch)
				}
			}
			m := release.Manifest{Version: "1.0.0", Components: []release.Component{
				{Name: "agent", Version: c.agent, Files: []string{"agent"}},
				{Name: "updater", Version: c.updater, Files: []string{"updater"}},
			}, Services: []release.Service{{Name: "dns", Version: c.service, API: "moostack.dns.v1.DNS", Image: "dns.tar", Properties: release.Properties{User: "65534:65534"}}}}
			if err := release.Write(dir, m); err != nil {
				t.Fatal(err)
			}
			_, err := release.Open(source, "1.0.0")
			if err == nil || !strings.Contains(err.Error(), "release 1.0.0: its "+c.name) || !strings.Contains(err.Error(), "isn't at the release's version") {
				t.Errorf("Open: %v, want %s refused", err, c.name)
			}
		})
	}
}

func TestUpdatesFromTakesTheReleaseItselfAndTheOneBeforeOnly(t *testing.T) {
	m := release.Manifest{Version: "1.1.0", Previous: "1.0.0"}
	for running, want := range map[string]bool{"1.1.0": true, "1.0.0": true, "0.9.0": false, "1.2.0": false, "": false} {
		if got := m.UpdatesFrom(running); got != want {
			t.Errorf("UpdatesFrom(%q) = %v, want %v", running, got, want)
		}
	}
	first := release.Manifest{Version: "1.0.0"}
	for running, want := range map[string]bool{"1.0.0": true, "": false, "0.9.0": false} {
		if got := first.UpdatesFrom(running); got != want {
			t.Errorf("a release without a previous: UpdatesFrom(%q) = %v, want %v", running, got, want)
		}
	}
}

// A mooctl reads the manifest of a release newer than it, which may have
// fields it doesn't know, so that it can say what it refuses.
func TestOpenReadsAManifestWithAFieldItDoesntKnow(t *testing.T) {
	source := writeRelease(t)
	manifest := filepath.Join(source, "1.0.0", "manifest.yml")
	data, err := os.ReadFile(manifest)
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, manifest, string(data)+"signed_by: someone\n")
	r, err := release.Open(source, "1.0.0")
	if err != nil {
		t.Fatal(err)
	}
	if r.Version != "1.0.0" {
		t.Errorf("the release is %s, want 1.0.0", r.Version)
	}
}

func TestOpenRefusesAManifestOfMoreThanOneDocument(t *testing.T) {
	source := writeRelease(t)
	manifest := filepath.Join(source, "1.0.0", "manifest.yml")
	data, err := os.ReadFile(manifest)
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, manifest, string(data)+"---\nversion: 9.9.9\n")
	if _, err := release.Open(source, "1.0.0"); err == nil || !strings.Contains(err.Error(), "more than one YAML document") {
		t.Errorf("Open of a manifest of two documents: %v", err)
	}
}

func TestTheBuildAReleasePinsIsTheBuildOfItsFiles(t *testing.T) {
	source := writeRelease(t)
	r, err := release.Open(source, "1.0.0")
	if err != nil {
		t.Fatal(err)
	}
	got, err := r.Build("arm64", []string{"containerd", "shim"})
	if err != nil {
		t.Fatal(err)
	}
	sums := map[string]string{}
	for _, f := range []string{"containerd", "shim"} {
		s := sha256.Sum256([]byte(f + " for arm64"))
		sums[f] = hex.EncodeToString(s[:])
	}
	if want := buildid.FromSums(sums); got != want {
		t.Errorf("the build of containerd for arm64 is %s, want %s", got, want)
	}
	if _, err := r.Build("arm64", []string{"nothing"}); err == nil {
		t.Error("the build of a file the release hasn't succeeded")
	}
}

func TestTheBuildOfAServiceIsItsImagesDigest(t *testing.T) {
	source := t.TempDir()
	dir := filepath.Join(source, "1.0.0")
	img := ociimage.Image{Name: release.ImageRef("dns", "1.0.0"), Architecture: "arm64", Entrypoint: []string{"/dns"}, Files: []ociimage.File{{Name: "dns", Mode: 0o755, Data: []byte("dns")}}}
	for _, arch := range release.Architectures {
		if err := os.MkdirAll(filepath.Join(dir, "linux-"+arch), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := ociimage.WriteFile(filepath.Join(dir, "linux-"+arch, "dns.tar"), img); err != nil {
			t.Fatal(err)
		}
	}
	if err := release.Write(dir, release.Manifest{Version: "1.0.0", Services: []release.Service{{Name: "dns", Version: "1.0.0", API: "moostack.dns.v1.DNS", Image: "dns.tar", Properties: release.Properties{User: "65534:65534"}}}}); err != nil {
		t.Fatal(err)
	}
	r, err := release.Open(source, "1.0.0")
	if err != nil {
		t.Fatal(err)
	}
	s, _ := r.Service("dns")
	got, err := r.ServiceBuild("arm64", s)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "linux-arm64", "dns.tar"))
	if err != nil {
		t.Fatal(err)
	}
	if want, _ := ociimage.Digest(data); got != want || want == "" {
		t.Errorf("dns's build is %q, want its image's digest %q", got, want)
	}
}
