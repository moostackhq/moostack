//go:build !release

package cli

import (
	"archive/tar"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"

	"github.com/google/go-containerregistry/pkg/name"
	ggcrremote "github.com/google/go-containerregistry/pkg/v1/remote"

	"github.com/moostackhq/moostack/internal/k3s"
	"github.com/moostackhq/moostack/internal/registry"
)

// These tests compile the console with the go command and build its
// image with the Docker of the machine they run on.

// thisCheckout is the Moostack checkout these tests are part of. It
// is to be called before anything changes the working directory.
func thisCheckout(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	checkout := filepath.Dir(filepath.Dir(wd))
	if _, err := os.Stat(filepath.Join(checkout, "cmd", "console", "Dockerfile")); err != nil {
		t.Fatalf("%s is no checkout: %v", checkout, err)
	}
	return checkout
}

// keepTools has the go and docker commands keep the caches and the
// configuration they have now, whatever HOME becomes. It is to be
// called before anything changes HOME.
func keepTools(t *testing.T) {
	t.Helper()
	out, err := exec.Command("go", "env", "-json", "GOMODCACHE", "GOCACHE").Output()
	if err != nil {
		t.Fatal(err)
	}
	var caches struct{ GOMODCACHE, GOCACHE string }
	if err := json.Unmarshal(out, &caches); err != nil || caches.GOMODCACHE == "" || caches.GOCACHE == "" {
		t.Fatalf("go env says %q: %v", out, err)
	}
	t.Setenv("GOMODCACHE", caches.GOMODCACHE)
	t.Setenv("GOCACHE", caches.GOCACHE)
	config := os.Getenv("DOCKER_CONFIG")
	if config == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			t.Fatal(err)
		}
		config = filepath.Join(home, ".docker")
	}
	t.Setenv("DOCKER_CONFIG", config)
}

func TestDevConsoleDeployRunsTheCheckoutsConsoleOnTheCluster(t *testing.T) {
	if testing.Short() {
		t.Skip("compiles the console and builds its image")
	}
	checkout := thisCheckout(t)
	keepTools(t)
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	released, _ := consoleImage(t, c, "amd64")
	t.Setenv("TMPDIR", t.TempDir())
	defer syscall.Umask(syscall.Umask(0o077))

	out := mustRun(t, c, home, "dev", "console", "deploy", checkout)
	for _, want := range []string{"Deploying the console of " + checkout + " to prod\n", "Compiling the console for linux/amd64...", "Deployed the console to prod\nImage: moostack.local/kube-system/console@sha256:"} {
		if !strings.Contains(out, want) {
			t.Errorf("the output lacks %q:\n%s", want, out)
		}
	}
	_, digest, _ := strings.Cut(strings.TrimSpace(out[strings.LastIndex(out, "Image: "):]), "@sha256:")
	if slices.Contains(released, digest) {
		t.Fatalf("the image deployed, %s, is the release's", digest)
	}
	tags, _ := consoleImage(t, c, "amd64")
	if !slices.Contains(tags, digest) || !slices.Contains(tags, registry.LiveTag) {
		t.Errorf("the registry's tags are %q, want %s among them", tags, digest)
	}
	objects := c.hosts["root@h1"].files[k3s.AutoDeployFile("console")]
	if !strings.Contains(objects, "image: "+registry.Image(k3s.ConsoleRepository, digest)+"\n") {
		t.Errorf("the console's objects don't run the new image:\n%s", objects)
	}
	if mode := consoleFileMode(t, c, digest); mode != 0o755 {
		t.Errorf("the console in the image has the mode %o, want 755, which the image's user can run", mode)
	}
	if c.consoleSeen != registry.Image(k3s.ConsoleRepository, digest) {
		t.Errorf("deploy ended when it had seen the console run %s", c.consoleSeen)
	}
	if left, _ := filepath.Glob(filepath.Join(os.Getenv("TMPDIR"), "mooctl-*")); len(left) != 0 {
		t.Errorf("the build was left in %v", left)
	}
}

// consoleFileMode is the mode of the console's binary in the image for
// amd64 of the index digest in the cluster's registry.
func consoleFileMode(t *testing.T, c *fakeCluster, digest string) int64 {
	t.Helper()
	repo, err := name.NewRepository(c.registry+"/"+k3s.ConsoleRepository, name.Insecure)
	if err != nil {
		t.Fatal(err)
	}
	idx, err := ggcrremote.Index(repo.Tag(digest))
	if err != nil {
		t.Fatal(err)
	}
	m, err := idx.IndexManifest()
	if err != nil || len(m.Manifests) == 0 {
		t.Fatalf("the index holds no image: %v", err)
	}
	img, err := idx.Image(m.Manifests[0].Digest)
	if err != nil {
		t.Fatal(err)
	}
	layers, err := img.Layers()
	if err != nil {
		t.Fatal(err)
	}
	for _, layer := range layers {
		content, err := layer.Uncompressed()
		if err != nil {
			t.Fatal(err)
		}
		files := tar.NewReader(content)
		for {
			header, err := files.Next()
			if err != nil {
				break
			}
			if strings.TrimPrefix(header.Name, "/") == "usr/local/bin/console" {
				content.Close()
				return header.Mode & 0o777
			}
		}
		content.Close()
	}
	t.Fatal("the image holds no usr/local/bin/console")
	return 0
}

func TestTheConsoleOfACheckoutWhoseDockerfileSetsNoModeCanBeRun(t *testing.T) {
	if testing.Short() {
		t.Skip("compiles the console and builds its image")
	}
	this := thisCheckout(t)
	keepTools(t)
	older := t.TempDir()
	for _, dir := range []string{"cmd", "internal"} {
		if err := os.CopyFS(filepath.Join(older, dir), os.DirFS(filepath.Join(this, dir))); err != nil {
			t.Fatal(err)
		}
	}
	for _, file := range []string{"go.mod", "go.sum", "LICENSE"} {
		content, err := os.ReadFile(filepath.Join(this, file))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(older, file), content, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	dockerfile := "FROM scratch\nCOPY console /usr/local/bin/console\nCOPY LICENSE /LICENSE\nUSER 65532:65532\nENTRYPOINT [\"/usr/local/bin/console\"]\n"
	if err := os.WriteFile(filepath.Join(older, "cmd", "console", "Dockerfile"), []byte(dockerfile), 0o644); err != nil {
		t.Fatal(err)
	}
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	defer syscall.Umask(syscall.Umask(0o077))

	out := mustRun(t, c, home, "dev", "console", "deploy", older)
	_, digest, _ := strings.Cut(strings.TrimSpace(out[strings.LastIndex(out, "Image: "):]), "@sha256:")
	if mode := consoleFileMode(t, c, digest); mode != 0o755 {
		t.Errorf("the console in the image has the mode %o, want 755, which the image's user can run", mode)
	}
}

func TestADevConsoleThatDoesntGetReadyLeavesTheOneThatRunsMarked(t *testing.T) {
	if testing.Short() {
		t.Skip("compiles the console and builds its image")
	}
	checkout := thisCheckout(t)
	keepTools(t)
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	released, _ := consoleImage(t, c, "amd64")
	c.consoleFails = true
	out, err := onFake(t, c, home, "dev", "console", "deploy", checkout)
	if err == nil || !strings.Contains(err.Error(), "the console isn't ready") {
		t.Fatalf("deploy of a console that doesn't get ready: %v\n%s", err, out)
	}
	repo, _ := name.NewRepository(c.registry+"/"+k3s.ConsoleRepository, name.Insecure)
	live, err := ggcrremote.Head(repo.Tag(registry.LiveTag))
	if err != nil {
		t.Fatal(err)
	}
	if live.Digest.Hex != released[0] {
		t.Errorf("%s marks %s, want the console that still runs, %s", registry.LiveTag, live.Digest.Hex, released[0])
	}
}

func TestDevConsoleDeployRefusesBeforeItBuilds(t *testing.T) {
	checkout := thisCheckout(t)
	c, home := fakeProd(t)
	notACheckout := t.TempDir()
	out, err := onFake(t, c, home, "dev", "console", "deploy", notACheckout)
	if err == nil || err.Error() != notACheckout+" is no Moostack checkout: it has no cmd/console/Dockerfile" {
		t.Errorf("deploy from a directory that is no checkout: %v\n%s", err, out)
	}
	out, err = onFake(t, c, home, "dev", "console", "deploy", checkout)
	if err == nil || !strings.Contains(err.Error(), "cluster prod has no nodes yet") {
		t.Errorf("deploy to a cluster without nodes: %v\n%s", err, out)
	}
	if out, err = onFake(t, c, home, "dev", "console", "deploy", checkout, "more"); err == nil || !strings.Contains(err.Error(), "takes at most 1 argument") {
		t.Errorf("deploy with two arguments: %v\n%s", err, out)
	}
}

func TestDevIsNotListedInHelp(t *testing.T) {
	c, home := fakeProd(t)
	out := mustRun(t, c, home, "--help")
	if strings.Contains(out, "dev") {
		t.Errorf("mooctl --help lists dev:\n%s", out)
	}
	if out := mustRun(t, c, home, "dev", "console", "--help"); !strings.Contains(out, "deploy") {
		t.Errorf("mooctl dev console --help doesn't list deploy:\n%s", out)
	}
}
