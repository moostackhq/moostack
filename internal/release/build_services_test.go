package release_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/hex"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/moostackhq/moostack/internal/release"
)

func TestBuildServicesBuildsWhereItReadsWhateverCargoIsConfiguredWith(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the cluster services with cargo")
	}
	// CARGO_BUILD_TARGET_DIR is cargo's config build.target-dir.
	for _, variable := range []string{"CARGO_TARGET_DIR", "CARGO_BUILD_TARGET_DIR"} {
		t.Run(variable, func(t *testing.T) {
			elsewhere := t.TempDir()
			t.Setenv(variable, elsewhere)
			if _, err := release.BuildServices(t.Context(), filepath.Join("..", ".."), runtime.GOARCH, "1.0.0", t.TempDir(), release.ClusterServices); err != nil {
				t.Fatal(err)
			}
			if entries, _ := os.ReadDir(elsewhere); len(entries) > 0 {
				t.Errorf("cargo built into %s, not where BuildServices reads the binaries", variable)
			}
		})
	}
}

// A service binary holds no path of the machine that built it: neither
// the checkout's nor cargo's home, where its dependencies' sources are.
func TestBuildServicesBuildsWithoutTheBuildersPaths(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a cluster service with cargo")
	}
	checkout, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	binary := buildDNS(t, checkout)
	for _, path := range []string{checkout, home} {
		if n := bytes.Count(binary, []byte(path)); n > 0 {
			t.Errorf("the dns binary holds the builder's path %s %d times", path, n)
		}
	} // The checkout's toolchain built it, as rust-toolchain.toml pins it.
	pinned, err := os.ReadFile(filepath.Join(checkout, "rust-toolchain.toml"))
	if err != nil {
		t.Fatal(err)
	}
	channel := regexp.MustCompile(`(?m)^channel = "([^"]+)"`).FindSubmatch(pinned)
	if channel == nil || !bytes.Contains(binary, []byte("rustc version "+string(channel[1])+" ")) {
		t.Errorf("the dns binary wasn't built by the pinned rustc %s", channel)
	}
}

// The flags cargo is configured to give rustc reach it, in each place
// cargo takes them from, beside the remapping of the builder's paths.
func TestBuildServicesKeepsTheRustFlagsCargoIsConfiguredWith(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a cluster service with cargo")
	}
	checkout, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	target := strings.ToUpper(strings.ReplaceAll(rustTarget(), "-", "_"))
	// A build ID the linker writes into the binary as given.
	const id = "c0ffee15deadbeef0123"
	flag := "-Clink-arg=--build-id=0x" + id
	// target.<triple>.rustflags as cargo's config, which the checkout's
	// .cargo/config.toml sets too, and cargo adds to it, and RUSTFLAGS and
	// CARGO_ENCODED_RUSTFLAGS as cargo's environment. build.rustflags is
	// left out: cargo takes it only where no target.<triple>.rustflags is
	// set.
	for _, variable := range []string{"CARGO_TARGET_" + target + "_RUSTFLAGS", "RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS"} {
		t.Run(variable, func(t *testing.T) {
			t.Setenv(variable, flag)
			binary := buildDNS(t, checkout)
			if want, _ := hex.DecodeString(id); !bytes.Contains(binary, want) {
				t.Errorf("the dns binary has no build ID %s, which %s gave", id, variable)
			}
			for _, path := range []string{checkout, home} {
				if n := bytes.Count(binary, []byte(path)); n > 0 {
					t.Errorf("the dns binary holds the builder's path %s %d times", path, n)
				}
			}
		})
	}
}

// A rustc wrapper cargo is given, as a build cache, isn't run: a cache
// would serve what it compiled for a build by hand, without the
// remapping of the builder's paths.
func TestBuildServicesRunsNoRustcWrapperCargoIsGiven(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a cluster service with cargo")
	}
	checkout, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	ran := filepath.Join(dir, "ran")
	wrapper := filepath.Join(dir, "wrapper")
	if err := os.WriteFile(wrapper, []byte("#!/bin/sh\ntouch '"+ran+"'\nexec \"$@\"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	// CARGO_BUILD_RUSTC_WRAPPER is cargo's config build.rustc-wrapper.
	for _, variable := range []string{"RUSTC_WRAPPER", "CARGO_BUILD_RUSTC_WRAPPER", "RUSTC_WORKSPACE_WRAPPER", "CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER"} {
		t.Run(variable, func(t *testing.T) {
			os.Remove(ran)
			t.Setenv(variable, wrapper)
			buildDNS(t, checkout)
			if _, err := os.Stat(ran); err == nil {
				t.Errorf("the wrapper %s gave ran", variable)
			}
		})
	}
}

// The rustc cargo is given in RUSTC is the one that builds, found as
// cargo finds it: by its path, absolute or relative to the checkout, or
// by its name on PATH.
func TestBuildServicesBuildsWithTheRustcCargoIsGiven(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a cluster service with cargo")
	}
	checkout, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	sysroot, err := exec.Command("rustc", "--print", "sysroot").Output()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	ran := filepath.Join(dir, "ran")
	rustc := filepath.Join(dir, "rustc-given")
	if err := os.WriteFile(rustc, []byte("#!/bin/sh\ntouch '"+ran+"'\nexec '"+filepath.Join(strings.TrimSpace(string(sysroot)), "bin", "rustc")+"' \"$@\"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	relative, err := filepath.Rel(checkout, rustc)
	if err != nil {
		t.Fatal(err)
	}
	for name, given := range map[string]string{"absolute": rustc, "relative": relative, "on PATH": "rustc-given"} {
		t.Run(name, func(t *testing.T) {
			os.Remove(ran)
			t.Setenv("RUSTC", given)
			t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
			buildDNS(t, checkout)
			if _, err := os.Stat(ran); err != nil {
				t.Errorf("the rustc RUSTC gave as %s never ran: %v", given, err)
			}
		})
	}
}

// rustTarget is the Rust target the services are built for on this
// machine.
func rustTarget() string {
	return map[string]string{"amd64": "x86_64-unknown-linux-musl", "arm64": "aarch64-unknown-linux-musl"}[runtime.GOARCH]
}

// What a developer built by hand with cargo in the checkout, without
// BuildServices remapping the builder's paths, is built again rather
// than put in an image.
func TestBuildServicesBuildsAgainWhatCargoBuiltWithoutIt(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a cluster service with cargo")
	}
	checkout, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	// dns is built again by hand, whatever cargo built before.
	now := time.Now()
	if err := os.Chtimes(filepath.Join(checkout, "crates", "dns", "src", "main.rs"), now, now); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("cargo", "build", "--release", "--locked", "-p", "moostack-dns", "--target", rustTarget())
	cmd.Dir = checkout
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("cargo build: %v\n%s", err, out)
	}
	if n := bytes.Count(buildDNS(t, checkout), []byte(checkout)); n > 0 {
		t.Errorf("the dns binary holds the checkout's path %d times, from the build by hand", n)
	}
}

// A temporary directory that runs nothing, as one mounted noexec on a
// hardened host, doesn't stop the build. The test runs itself again in a
// user namespace with such a directory as TMPDIR, where it can mount one.
func TestBuildServicesBuildsWhereTheTemporaryDirectoryRunsNothing(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a cluster service with cargo")
	}
	checkout, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	if dir := os.Getenv("BUILD_SERVICES_NOEXEC_TMPDIR"); dir != "" {
		buildDNS(t, checkout)
		return
	}
	if exec.Command("unshare", "-rm", "true").Run() != nil {
		t.Skip("no user namespaces to mount a noexec directory in")
	}
	noexec := t.TempDir()
	cmd := exec.Command("unshare", "-rm", "sh", "-c", `mount -t tmpfs -o noexec none "$BUILD_SERVICES_NOEXEC_TMPDIR" && TMPDIR="$BUILD_SERVICES_NOEXEC_TMPDIR" exec "$0" -test.run='^TestBuildServicesBuildsWhereTheTemporaryDirectoryRunsNothing$'`, os.Args[0])
	cmd.Env = append(os.Environ(), "BUILD_SERVICES_NOEXEC_TMPDIR="+noexec)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Errorf("building with a TMPDIR that runs nothing: %v\n%s", err, out)
	}
}

// buildDNS builds the dns service with BuildServices in checkout, and
// returns the binary in its image.
func buildDNS(t *testing.T, checkout string) []byte {
	t.Helper()
	dns := release.ClusterServices[slices.IndexFunc(release.ClusterServices, func(s release.ClusterService) bool { return s.Name == "dns" })]
	dir := t.TempDir()
	if _, err := release.BuildServices(t.Context(), checkout, runtime.GOARCH, "1.0.0", dir, []release.ClusterService{dns}); err != nil {
		t.Fatal(err)
	}
	archive, err := os.Open(filepath.Join(dir, "dns.tar"))
	if err != nil {
		t.Fatal(err)
	}
	defer archive.Close()
	// The binary is in the image's one layer, a gzipped tar among its
	// blobs.
	blobs := tar.NewReader(archive)
	for {
		if _, err := blobs.Next(); err != nil {
			t.Fatalf("no dns in the image's layer: %v", err)
		}
		gz, err := gzip.NewReader(blobs)
		if err != nil {
			continue
		}
		layer := tar.NewReader(gz)
		for {
			h, err := layer.Next()
			if err != nil {
				break
			}
			if h.Name == "dns" {
				binary, err := io.ReadAll(layer)
				if err != nil {
					t.Fatal(err)
				}
				return binary
			}
		}
	}
}
