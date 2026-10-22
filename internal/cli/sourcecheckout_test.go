package cli_test

import (
	"bytes"
	"debug/buildinfo"
	"debug/elf"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/moostackhq/moostack/internal/ociimage"
	"github.com/moostackhq/moostack/internal/release"
)

// devCheckout makes a Moostack checkout to build development builds
// from: a git repository with one commit, whose agent and updater are
// programs that print their version. It puts a cargo on PATH that builds
// each cluster service it is asked for as a file that says the service
// and its target, and logs its arguments to cargo.log in the checkout.
// It returns the checkout.
func devCheckout(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "go.mod"), []byte("module github.com/moostackhq/moostack\n\ngo 1.27.0\n"))
	writeFile(t, filepath.Join(dir, "internal", "buildinfo", "buildinfo.go"), []byte("package buildinfo\n\nvar Version, Previous string\n"))
	for _, program := range []string{"agent", "updater"} {
		writeFile(t, filepath.Join(dir, "cmd", program, "main.go"), []byte(`package main

import (
	"fmt"

	"github.com/moostackhq/moostack/internal/buildinfo"
)

func main() { fmt.Println("`+program+` at", buildinfo.Version) }
`))
	}
	writeFile(t, filepath.Join(dir, ".gitignore"), []byte("/target/\n/cargo.log\n"))
	gitIn(t, dir, "init", "-q")
	gitIn(t, dir, "add", ".")
	gitIn(t, dir, "-c", "user.name=t", "-c", "user.email=t@example.com", "commit", "-q", "-m", "first")

	bin := t.TempDir()
	writeFile(t, filepath.Join(bin, "cargo"), []byte(`#!/bin/sh
echo "$@" >> cargo.log
while [ $# -gt 0 ]; do
	case $1 in
	--target) target=$2; shift ;;
	--target-dir) dir=$2; shift ;;
	-p) services="$services ${2#moostack-}"; shift ;;
	esac
	shift
done
mkdir -p "$dir/$target/release"
for s in $services; do echo "$s for $target" > "$dir/$target/release/$s"; done
`))
	if err := os.Chmod(filepath.Join(bin, "cargo"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	return dir
}

// gitIn runs git with args in dir, and returns what it printed.
func gitIn(t *testing.T, dir string, args ...string) string {
	t.Helper()
	out, err := exec.Command("git", append([]string{"-C", dir}, args...)...).CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out))
}

// commitOf is the commit of the checkout, as a development build from it
// is versioned while it has no changes.
func commitOf(t *testing.T, checkout string) string {
	return gitIn(t, checkout, "rev-parse", "--short=12", "HEAD")
}

// elfMachines are the ELF machines of each architecture.
var elfMachines = map[string]elf.Machine{"amd64": elf.EM_X86_64, "arm64": elf.EM_AARCH64}

// wantProgram checks that binary, what, is a static program for arch,
// built without the builder's paths, and stamped with version, as the
// build's settings in it say.
func wantProgram(t *testing.T, what string, binary []byte, arch, version string) {
	t.Helper()
	f, err := elf.NewFile(bytes.NewReader(binary))
	if err != nil {
		t.Errorf("%s isn't an ELF binary: %v", what, err)
		return
	}
	if f.Machine != elfMachines[arch] {
		t.Errorf("%s is for %s, want %s", what, f.Machine, arch)
	}
	info, err := buildinfo.Read(bytes.NewReader(binary))
	if err != nil {
		t.Errorf("%s has no build information: %v", what, err)
		return
	}
	settings := map[string]string{}
	for _, s := range info.Settings {
		settings[s.Key] = s.Value
	}
	if got := stampedVersion(t, f); got != version {
		t.Errorf("%s is stamped with %q, want %s", what, got, version)
	}
	if settings["CGO_ENABLED"] != "0" || settings["-trimpath"] != "true" || settings["GOARCH"] != arch {
		t.Errorf("%s was built with CGO_ENABLED=%s, -trimpath=%s, GOARCH=%s, want a static build for %s without the builder's paths",
			what, settings["CGO_ENABLED"], settings["-trimpath"], settings["GOARCH"], arch)
	}
}

// stampedVersion is the value the program f has for buildinfo.Version:
// the string its symbol's header, a pointer and a length, points to.
func stampedVersion(t *testing.T, f *elf.File) string {
	t.Helper()
	symbols, err := f.Symbols()
	if err != nil {
		t.Fatal(err)
	}
	i := slices.IndexFunc(symbols, func(s elf.Symbol) bool {
		return s.Name == "github.com/moostackhq/moostack/internal/buildinfo.Version"
	})
	if i < 0 {
		t.Fatal("the program has no buildinfo.Version")
	}
	header := readAt(t, f, symbols[i].Value, 16)
	length := f.ByteOrder.Uint64(header[8:])
	if length == 0 {
		return ""
	}
	return string(readAt(t, f, f.ByteOrder.Uint64(header[:8]), length))
}

// readAt reads n bytes of f at the virtual address addr.
func readAt(t *testing.T, f *elf.File, addr, n uint64) []byte {
	t.Helper()
	for _, p := range f.Progs {
		if p.Type == elf.PT_LOAD && addr >= p.Vaddr && addr+n <= p.Vaddr+p.Filesz {
			data := make([]byte, n)
			if _, err := p.ReadAt(data, int64(addr-p.Vaddr)); err != nil {
				t.Fatal(err)
			}
			return data
		}
	}
	t.Fatalf("the program has no data at %#x", addr)
	return nil
}

// serviceDigest is the digest of the image of the cluster service name
// at version for arch, as devCheckout's cargo builds the service.
func serviceDigest(t *testing.T, name, version, arch string) string {
	t.Helper()
	target := map[string]string{"amd64": "x86_64-unknown-linux-musl", "arm64": "aarch64-unknown-linux-musl"}[arch]
	var b bytes.Buffer
	err := ociimage.Write(&b, ociimage.Image{Name: release.ImageRef(name, version), Architecture: arch, Entrypoint: []string{"/" + name},
		Files: []ociimage.File{{Name: name, Mode: 0o755, Data: []byte(name + " for " + target + "\n")}}})
	if err != nil {
		t.Fatal(err)
	}
	return digest(t, b.Bytes())
}

// digest is the digest of the image in archive, empty if it isn't one.
func digest(t *testing.T, archive []byte) string {
	t.Helper()
	d, err := ociimage.Digest(archive)
	if err != nil {
		return ""
	}
	return d
}
