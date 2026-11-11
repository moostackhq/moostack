package buildx_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/moostackhq/moostack/internal/buildx"
)

// These tests build with the Docker of the machine they run on, which
// must be there, on the containerd image store: they fail rather than
// skip without it. None reaches a registry: every image is built from
// scratch.

// buildContext writes a build context in a new directory, its Dockerfile
// dockerfile and its other files files, by name, executable, and
// returns the directory.
func buildContext(t *testing.T, dockerfile string, files map[string][]byte) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "Dockerfile"), []byte(dockerfile), 0o644); err != nil {
		t.Fatal(err)
	}
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// static is the context of an image of one file, which builds on any
// machine for any architecture, with no step to run.
func static(t *testing.T) string {
	return buildContext(t, "FROM scratch\nCOPY hello /hello\n", map[string][]byte{"hello": []byte("hello\n")})
}

// archive is what the OCI image layout archive at path holds: its
// index's manifests, and the platform of the config of its first.
type archive struct {
	manifests []struct {
		MediaType   string            `json:"mediaType"`
		Digest      string            `json:"digest"`
		Annotations map[string]string `json:"annotations"`
	}
	architecture, os string
}

func readArchive(t *testing.T, path string) archive {
	t.Helper()
	blobs := map[string][]byte{}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	r := tar.NewReader(f)
	for {
		h, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(r)
		if err != nil {
			t.Fatal(err)
		}
		blobs[h.Name] = data
	}
	var a archive
	var index struct{ Manifests json.RawMessage }
	if err := json.Unmarshal(blobs["index.json"], &index); err != nil {
		t.Fatalf("the archive's index.json: %v", err)
	}
	if err := json.Unmarshal(index.Manifests, &a.manifests); err != nil || len(a.manifests) == 0 {
		t.Fatalf("the archive's index lists %s: %v", index.Manifests, err)
	}
	blob := func(digest string) []byte {
		data, ok := blobs["blobs/"+strings.Replace(digest, ":", "/", 1)]
		if !ok {
			t.Fatalf("the archive has no blob %s", digest)
		}
		return data
	}
	var manifest struct{ Config struct{ Digest string } }
	if err := json.Unmarshal(blob(a.manifests[0].Digest), &manifest); err != nil {
		t.Fatal(err)
	}
	var config struct{ Architecture, OS string }
	if err := json.Unmarshal(blob(manifest.Config.Digest), &config); err != nil {
		t.Fatal(err)
	}
	a.architecture, a.os = config.Architecture, config.OS
	return a
}

// build builds the context at dir for arch, tagged tag, with docker,
// and returns the archive's path and what buildx printed.
func build(t *testing.T, docker buildx.Docker, dir, tag, arch string) (string, string, error) {
	t.Helper()
	var out bytes.Buffer
	docker.Output = &out
	dest := filepath.Join(t.TempDir(), "image.tar")
	err := docker.Build(t.Context(), dir, tag, arch, dest)
	return dest, out.String(), err
}

func TestDockerOnTheContainerdImageStorePassesTheCheck(t *testing.T) {
	if _, err := (buildx.Docker{}).Check(t.Context()); err != nil {
		t.Fatalf("Check of this machine's Docker: %v; these tests need Docker on the containerd image store", err)
	}
}

func TestABuildIsOneImageForTheArchitectureTaggedWithoutAttestations(t *testing.T) {
	dest, out, err := build(t, buildx.Docker{}, static(t), "moostack.local/web:20261026T120000Z", "amd64")
	if err != nil {
		t.Fatalf("Build: %v\n%s", err, out)
	}
	a := readArchive(t, dest)
	if len(a.manifests) != 1 || a.manifests[0].MediaType != "application/vnd.oci.image.manifest.v1+json" {
		t.Fatalf("the archive's index lists %+v, want one image manifest and no attestation", a.manifests)
	}
	if name := a.manifests[0].Annotations["io.containerd.image.name"]; name != "moostack.local/web:20261026T120000Z" {
		t.Errorf("the archive names its image %q, want the tag", name)
	}
	if a.architecture != "amd64" || a.os != "linux" {
		t.Errorf("the image is for %s/%s, want linux/amd64", a.os, a.architecture)
	}
	if !strings.Contains(out, "COPY hello /hello") {
		t.Errorf("buildx printed\n%s\nwithout its progress", out)
	}
}

func TestBuildingAgainUnchangedMakesTheSameImage(t *testing.T) {
	dir := static(t)
	first, _, err := build(t, buildx.Docker{}, dir, "moostack.local/web:1", "amd64")
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := build(t, buildx.Docker{}, dir, "moostack.local/web:2", "amd64")
	if err != nil {
		t.Fatal(err)
	}
	if a, b := readArchive(t, first).manifests[0].Digest, readArchive(t, second).manifests[0].Digest; a != b {
		t.Errorf("an unchanged context built as %s, then as %s", a, b)
	}
}

func TestABuildContextIsTakenRelativeToTheWorkingDirectoryAndOutsideIt(t *testing.T) {
	dir := static(t)
	app := filepath.Join(t.TempDir(), "app")
	if err := os.Mkdir(app, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Chdir(app)
	rel, err := filepath.Rel(app, dir)
	if err != nil || !strings.HasPrefix(rel, "..") {
		t.Fatalf("the context %s isn't outside %s: %s, %v", dir, app, rel, err)
	}
	dest, out, err := build(t, buildx.Docker{}, rel, "moostack.local/web:1", "amd64")
	if err != nil {
		t.Fatalf("Build of %s: %v\n%s", rel, err, out)
	}
	if a := readArchive(t, dest); len(a.manifests) != 1 {
		t.Errorf("the archive's index lists %+v", a.manifests)
	}
}

// A context named like a flag is taken as the context.
func TestABuildContextNamedLikeAFlagIsTheContext(t *testing.T) {
	dir := static(t)
	t.Chdir(filepath.Dir(dir))
	flagLike := "--help"
	if err := os.Rename(dir, flagLike); err != nil {
		t.Fatal(err)
	}
	if _, out, err := build(t, buildx.Docker{}, flagLike, "moostack.local/web:1", "amd64"); err != nil {
		t.Fatalf("Build of %s: %v\n%s", flagLike, err, out)
	}
}

func TestAFailedBuildShowsBuildxsErrorWithTheDockerfileLine(t *testing.T) {
	_, out, err := build(t, buildx.Docker{}, buildContext(t, "FROM scratch\nCOPY missing /missing\n", nil), "moostack.local/web:1", "amd64")
	var e *buildx.BuildError
	if !errors.As(err, &e) || e.ExecFormat {
		t.Fatalf("Build of a context without the file it copies = %v, want a *BuildError without exec format", err)
	}
	if !strings.Contains(out, "COPY missing /missing") || !strings.Contains(out, "ERROR") {
		t.Errorf("buildx printed\n%s\nwithout its error and the Dockerfile's line", out)
	}
}

// A program the machine can't run fails with "exec format error", as one
// for another architecture does without emulation; it is built for this
// machine's own architecture so that emulation can't run it either.
func TestABuildThatSaysExecFormatErrorIsToldApart(t *testing.T) {
	dir := buildContext(t, "FROM scratch\nCOPY garbage /garbage\nRUN [\"/garbage\"]\n", map[string][]byte{"garbage": []byte("not a program")})
	_, out, err := build(t, buildx.Docker{}, dir, "moostack.local/web:1", "amd64")
	var e *buildx.BuildError
	if !errors.As(err, &e) || !e.ExecFormat {
		t.Fatalf("Build = %v, want a *BuildError with exec format\n%s", err, out)
	}
	if !strings.Contains(out, "exec format error") {
		t.Errorf("buildx printed\n%s\nwithout its error", out)
	}
}

// An arm64 image whose step runs an arm64 program builds on an amd64
// machine with QEMU registered in binfmt_misc, which this machine must
// have for arm64.
func TestAnArm64BuildRunsItsStepsUnderQEMU(t *testing.T) {
	if _, err := os.Stat("/proc/sys/fs/binfmt_misc/qemu-aarch64"); err != nil {
		t.Fatalf("this machine has no QEMU for arm64 in binfmt_misc, which the test needs: %v; register it with docker run --privileged --rm tonistiigi/binfmt --install arm64, or the distribution's qemu-user-static with binfmt entries", err)
	}
	src := filepath.Join(t.TempDir(), "main.go")
	if err := os.WriteFile(src, []byte("package main\n\nfunc main() { println(\"hello from arm64\") }\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	dir := buildContext(t, "FROM scratch\nCOPY hello /hello\nRUN [\"/hello\"]\n", nil)
	cmd := exec.Command("go", "build", "-o", filepath.Join(dir, "hello"), src)
	cmd.Env = append(os.Environ(), "GOOS=linux", "GOARCH=arm64", "CGO_ENABLED=0")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go build for arm64: %v\n%s", err, out)
	}
	dest, out, err := build(t, buildx.Docker{}, dir, "moostack.local/web:1", "arm64")
	if err != nil {
		t.Fatalf("Build for arm64: %v\n%s", err, out)
	}
	if a := readArchive(t, dest); len(a.manifests) != 1 || a.architecture != "arm64" || a.os != "linux" {
		t.Errorf("the archive lists %+v, for %s/%s, want one image for linux/arm64", a.manifests, a.os, a.architecture)
	}
}

func TestABuildStoppedEndsBuildx(t *testing.T) {
	dir := static(t)
	errStopped := errors.New("stopped")
	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(errStopped)
	err := buildx.Docker{}.Build(ctx, dir, "moostack.local/web:1", "amd64", filepath.Join(t.TempDir(), "image.tar"))
	var e *buildx.BuildError
	if !errors.As(err, &e) || !errors.Is(err, errStopped) {
		t.Errorf("a stopped Build = %v, want a *BuildError for the stop", err)
	}
}

func TestWithoutTheDockerCommandTheCheckSaysToInstallDocker(t *testing.T) {
	_, err := buildx.Docker{Path: filepath.Join(t.TempDir(), "docker")}.Check(t.Context())
	if err == nil || !strings.Contains(err.Error(), "install Docker Engine with its buildx plugin, or Docker Desktop") {
		t.Errorf("Check without docker = %v, want it to say what to install", err)
	}
}

func TestTheCheckReachesDockerWhereDOCKER_HOSTSays(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "docker.sock")
	t.Setenv("DOCKER_HOST", "unix://"+socket)
	_, err := buildx.Docker{}.Check(t.Context())
	if err == nil || !strings.Contains(err.Error(), "Docker doesn't answer") || !strings.Contains(err.Error(), socket) ||
		!strings.Contains(err.Error(), "start Docker, check that you may reach it, or point DOCKER_HOST or the docker context at it") {
		t.Errorf("Check with DOCKER_HOST at no Docker = %v, want it to say Docker doesn't answer there", err)
	}
	t.Setenv("DOCKER_HOST", "unix:///var/run/docker.sock")
	if _, err := (buildx.Docker{}).Check(t.Context()); err != nil {
		t.Errorf("Check with DOCKER_HOST at this machine's Docker = %v", err)
	}
	if _, out, err := build(t, buildx.Docker{}, static(t), "moostack.local/web:1", "amd64"); err != nil {
		t.Errorf("Build with DOCKER_HOST at this machine's Docker = %v\n%s", err, out)
	}
}

// fakeDocker serves, on a Unix socket, the Docker API of a Docker whose
// info reports driver and driverStatus, and returns the socket's
// DOCKER_HOST.
func fakeDocker(t *testing.T, driver string, driverStatus [][2]string) string {
	t.Helper()
	socket := filepath.Join(t.TempDir(), "docker.sock")
	l, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Api-Version", "1.47")
		switch {
		case strings.HasSuffix(r.URL.Path, "/_ping"):
			io.WriteString(w, "OK")
		case strings.HasSuffix(r.URL.Path, "/info"):
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{"Driver": driver, "DriverStatus": driverStatus, "ServerVersion": "29.0.0", "Architecture": "x86_64"})
		default:
			http.NotFound(w, r)
		}
	})}
	go server.Serve(l)
	t.Cleanup(func() { server.Close() })
	return "unix://" + socket
}

func TestOnTheClassicImageStoreTheCheckSaysToSwitchToContainerds(t *testing.T) {
	t.Setenv("DOCKER_HOST", fakeDocker(t, "overlay2", [][2]string{{"Backing Filesystem", "extfs"}, {"Supports d_type", "true"}}))
	_, err := buildx.Docker{}.Check(t.Context())
	if err == nil || !strings.Contains(err.Error(), "classic image store") || !strings.Contains(err.Error(), `"containerd-snapshotter": true`) ||
		!strings.Contains(err.Error(), "Use containerd for pulling and storing images") {
		t.Errorf("Check on the classic image store = %v, want it to say how to switch", err)
	}
	// The fake answers info alone, so the check goes no further than the
	// image store, which it takes.
	t.Setenv("DOCKER_HOST", fakeDocker(t, "overlayfs", [][2]string{{"driver-type", "io.containerd.snapshotter.v1"}}))
	if _, err := (buildx.Docker{}).Check(t.Context()); err != nil && strings.Contains(err.Error(), "image store") {
		t.Errorf("Check on the containerd image store = %v", err)
	}
}

func TestWithoutBuildxTheCheckSaysToInstallIt(t *testing.T) {
	// A docker that answers info as Docker does, but has no buildx
	// plugin, as docker says without it.
	docker := filepath.Join(t.TempDir(), "docker")
	script := "#!/bin/sh\ncase \"$1\" in\ninfo) echo '29.7.2|x86_64|io.containerd.snapshotter.v1' ;;\n" +
		"*) echo \"docker: unknown command: docker $1\" >&2; exit 1 ;;\nesac\n"
	if err := os.WriteFile(docker, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	_, err := buildx.Docker{Path: docker}.Check(t.Context())
	if err == nil || !strings.Contains(err.Error(), "docker has no buildx plugin: docker: unknown command: docker buildx; install Docker's buildx plugin") {
		t.Errorf("Check without buildx = %v, want it to say to install it", err)
	}
}

// A build stopped ends with every process it started, such as docker's
// buildx plugin, even one that holds its output open.
func TestABuildStoppedEndsWithWhatItStarted(t *testing.T) {
	dir := t.TempDir()
	docker := filepath.Join(dir, "docker")
	pidFile := filepath.Join(dir, "plugin.pid")
	// The plugin, a child of docker, runs on with docker's output.
	script := "#!/bin/sh\nsleep 600 &\necho $! > " + pidFile + "\necho building >&2\nwait\n"
	if err := os.WriteFile(docker, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	out := &signalWriter{Buffer: &bytes.Buffer{}, wrote: make(chan struct{}, 1)}
	done := make(chan error, 1)
	go func() {
		done <- buildx.Docker{Path: docker, Output: out}.Build(ctx, static(t), "moostack.local/web:1", "amd64", filepath.Join(dir, "image.tar"))
	}()
	<-out.wrote
	cancel()
	select {
	case err := <-done:
		var e *buildx.BuildError
		if !errors.As(err, &e) || !errors.Is(err, context.Canceled) {
			t.Errorf("a stopped Build = %v, want a *BuildError for the stop", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a stopped Build didn't end within 5s")
	}
	pid, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatal(err)
	}
	if err := exec.Command("kill", "-0", strings.TrimSpace(string(pid))).Run(); err == nil {
		t.Errorf("the plugin, process %s, runs on after the build stopped", strings.TrimSpace(string(pid)))
	}
}

// signalWriter is a buffer that signals on wrote once it was written to.
type signalWriter struct {
	*bytes.Buffer
	wrote chan struct{}
}

func (w *signalWriter) Write(p []byte) (int, error) {
	select {
	case w.wrote <- struct{}{}:
	default:
	}
	return w.Buffer.Write(p)
}

// "exec format error" is told apart even when buildx prints it in two
// writes.
func TestExecFormatErrorSplitAcrossWritesIsToldApart(t *testing.T) {
	docker := filepath.Join(t.TempDir(), "docker")
	script := "#!/bin/sh\nprintf 'exec /bin/sh: exec for' >&2\nsleep 0.2\nprintf 'mat error\\n' >&2\nexit 1\n"
	if err := os.WriteFile(docker, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	_, out, err := build(t, buildx.Docker{Path: docker}, static(t), "moostack.local/web:1", "arm64")
	var e *buildx.BuildError
	if !errors.As(err, &e) || !e.ExecFormat {
		t.Errorf("Build = %v, want a *BuildError with exec format", err)
	}
	if out != "exec /bin/sh: exec format error\n" {
		t.Errorf("buildx's output came through as %q", out)
	}
}

// fakeDockerCommand writes a docker command that runs script, and
// returns its path.
func fakeDockerCommand(t *testing.T, script string) string {
	t.Helper()
	docker := filepath.Join(t.TempDir(), "docker")
	if err := os.WriteFile(docker, []byte("#!/bin/sh\n"+script), 0o755); err != nil {
		t.Fatal(err)
	}
	return docker
}

// A docker info that can't reach Docker but succeeds all the same, as it
// does when its dial times out, is Docker not answering, not the classic
// image store.
func TestADockerInfoThatReachesNoDockerIsDockerNotAnswering(t *testing.T) {
	docker := fakeDockerCommand(t, "echo ' '\necho 'failed to connect to the docker API at tcp://10.255.255.1:2375: dial tcp 10.255.255.1:2375: i/o timeout' >&2\n")
	_, err := buildx.Docker{Path: docker}.Check(t.Context())
	want := "Docker doesn't answer: failed to connect to the docker API at tcp://10.255.255.1:2375: dial tcp 10.255.255.1:2375: i/o timeout; start Docker, check that you may reach it, or point DOCKER_HOST or the docker context at it"
	if err == nil || err.Error() != want {
		t.Errorf("Check = %v, want %s", err, want)
	}
}

// A Docker that never answers fails the check once its time runs out.
func TestADockerThatHangsFailsTheCheckInTime(t *testing.T) {
	docker := fakeDockerCommand(t, "exec sleep 60\n")
	start := time.Now()
	_, err := buildx.Docker{Path: docker, CheckTimeout: 200 * time.Millisecond}.Check(t.Context())
	if err == nil || err.Error() != "Docker doesn't answer within 200ms: docker info didn't end; start Docker, check that you may reach it, or point DOCKER_HOST or the docker context at it" {
		t.Errorf("Check of a hanging Docker = %v, want it to say Docker doesn't answer in time", err)
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Errorf("Check took %s", took)
	}
}

// A check stopped says so, not that Docker doesn't answer.
func TestACheckStoppedFailsWithTheStop(t *testing.T) {
	docker := fakeDockerCommand(t, "exec sleep 60\n")
	stop := errors.New("interrupted")
	ctx, cancel := context.WithCancelCause(t.Context())
	time.AfterFunc(100*time.Millisecond, func() { cancel(stop) })
	_, err := buildx.Docker{Path: docker}.Check(ctx)
	if !errors.Is(err, stop) || strings.Contains(err.Error(), "doesn't answer") {
		t.Errorf("a stopped Check = %v, want the stop", err)
	}
}

// docker buildx version failing without a word still says what to
// install.
func TestABuildxThatFailsSilentlySaysToInstallIt(t *testing.T) {
	docker := fakeDockerCommand(t, "case \"$1\" in\ninfo) echo '29.7.2|x86_64|io.containerd.snapshotter.v1' ;;\n*) exit 1 ;;\nesac\n")
	_, err := buildx.Docker{Path: docker}.Check(t.Context())
	if err == nil || err.Error() != "docker has no buildx plugin: exit status 1; install Docker's buildx plugin" {
		t.Errorf("Check = %v", err)
	}
}

// The archive's path may hold a comma or a quote, as $TMPDIR may.
func TestAnArchivePathWithACommaOrAQuoteIsWritten(t *testing.T) {
	dir := filepath.Join(t.TempDir(), `a,b"c`)
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(dir, "image.tar")
	var out bytes.Buffer
	if err := (buildx.Docker{Output: &out}).Build(t.Context(), static(t), "moostack.local/web:1", "amd64", dest); err != nil {
		t.Fatalf("Build to %s: %v\n%s", dest, err, out.String())
	}
	if a := readArchive(t, dest); len(a.manifests) != 1 {
		t.Errorf("the archive lists %+v", a.manifests)
	}
}

// A context called -, relative to the working directory, is that
// directory, not stdin.
func TestABuildContextNamedDashIsTheDirectory(t *testing.T) {
	dir := static(t)
	t.Chdir(filepath.Dir(dir))
	if err := os.Rename(dir, "-"); err != nil {
		t.Fatal(err)
	}
	if _, out, err := build(t, buildx.Docker{}, "-", "moostack.local/web:1", "amd64"); err != nil {
		t.Fatalf("Build of -: %v\n%s", err, out)
	}
}

// The image's times are the epoch's, its config's and its files' alike,
// so that the same source builds the same image on any machine.
func TestABuildIsReproducible(t *testing.T) {
	dir := static(t)
	old := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := os.Chtimes(filepath.Join(dir, "hello"), old, old); err != nil {
		t.Fatal(err)
	}
	dest, out, err := build(t, buildx.Docker{}, dir, "moostack.local/web:1", "amd64")
	if err != nil {
		t.Fatalf("Build: %v\n%s", err, out)
	}
	created, mtimes := imageTimes(t, dest)
	if created != "1970-01-01T00:00:00Z" {
		t.Errorf("the image was created %q, want the epoch", created)
	}
	for name, mtime := range mtimes {
		if !mtime.Equal(time.Unix(0, 0)) {
			t.Errorf("the image's %s has the time %s, want the epoch", name, mtime)
		}
	}
	if len(mtimes) == 0 {
		t.Error("the image has no files")
	}
}

// buildx shows its progress, as plain text, whatever the operator's
// BUILDKIT_PROGRESS says.
func TestBuildxShowsItsProgressWhateverBUILDKIT_PROGRESSSays(t *testing.T) {
	for _, progress := range []string{"quiet", "rawjson", "tty"} {
		t.Setenv("BUILDKIT_PROGRESS", progress)
		_, out, err := build(t, buildx.Docker{}, static(t), "moostack.local/web:1", "amd64")
		if err != nil || !strings.Contains(out, "COPY hello /hello") || strings.Contains(out, `"vertexes"`) {
			t.Errorf("with BUILDKIT_PROGRESS=%s, Build = %v, printing\n%s\nwant its plain progress", progress, err, out)
		}
	}
}

// imageTimes are the created time of the image in the archive at path,
// and the times of the files of its layers, by name.
func imageTimes(t *testing.T, path string) (string, map[string]time.Time) {
	t.Helper()
	blobs := map[string][]byte{}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	r := tar.NewReader(f)
	for {
		h, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(r)
		if err != nil {
			t.Fatal(err)
		}
		blobs[h.Name] = data
	}
	blob := func(digest string) []byte { return blobs["blobs/"+strings.Replace(digest, ":", "/", 1)] }
	var index struct{ Manifests []struct{ Digest string } }
	var manifest struct {
		Config struct{ Digest string }
		Layers []struct{ Digest string }
	}
	var config struct{ Created string }
	if err := json.Unmarshal(blobs["index.json"], &index); err != nil || len(index.Manifests) != 1 {
		t.Fatalf("index.json: %v", err)
	}
	if err := json.Unmarshal(blob(index.Manifests[0].Digest), &manifest); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(blob(manifest.Config.Digest), &config); err != nil {
		t.Fatal(err)
	}
	mtimes := map[string]time.Time{}
	for _, l := range manifest.Layers {
		gz, err := gzip.NewReader(bytes.NewReader(blob(l.Digest)))
		if err != nil {
			t.Fatal(err)
		}
		lr := tar.NewReader(gz)
		for {
			h, err := lr.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			mtimes[h.Name] = h.ModTime
		}
	}
	return config.Created, mtimes
}

// Docker before Engine 26 ignores rewrite-timestamp, so its builds aren't
// reproducible; the check says to update it. The check says which
// architecture Docker's machine is, as Go names it.
func TestTheCheckWantsAReproducibleDockerAndSaysItsArchitecture(t *testing.T) {
	for version, want := range map[string]string{
		"25.0.3": "Docker is 25.0.3, whose builds aren't reproducible; update it to Docker Engine 26 or later, or Docker Desktop 4.29 or later",
		"dev":    "Docker is dev, whose builds aren't reproducible",
	} {
		docker := fakeDockerCommand(t, "case \"$1\" in\ninfo) echo '"+version+"|x86_64|io.containerd.snapshotter.v1' ;;\n*) echo 'Driver: docker' ;;\nesac\n")
		if _, err := (buildx.Docker{Path: docker}).Check(t.Context()); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("Check of Docker %s = %v, want %q", version, err, want)
		}
	}
	for uname, want := range map[string]string{"x86_64": "amd64", "aarch64": "arm64", "riscv64": "riscv64"} {
		docker := fakeDockerCommand(t, "case \"$1\" in\ninfo) echo '26.0.0|"+uname+"|io.containerd.snapshotter.v1' ;;\n*) echo 'Driver: docker' ;;\nesac\n")
		if d, err := (buildx.Docker{Path: docker}).Check(t.Context()); err != nil || d.Arch != want {
			t.Errorf("Check of Docker on %s = %+v, %v, want %s", uname, d, err, want)
		}
	}
	if d, err := (buildx.Docker{}).Check(t.Context()); err != nil || d.Arch != runtime.GOARCH {
		t.Errorf("Check of this machine's Docker = %+v, %v, want %s", d, err, runtime.GOARCH)
	}
}

// A docker that is a wrapper, whose own docker hangs holding the output,
// still fails the check in time.
func TestAWrappedDockerThatHangsFailsTheCheckInTime(t *testing.T) {
	docker := fakeDockerCommand(t, "sleep 60\n")
	start := time.Now()
	_, err := buildx.Docker{Path: docker, CheckTimeout: 200 * time.Millisecond}.Check(t.Context())
	if err == nil || !strings.Contains(err.Error(), "Docker doesn't answer within 200ms") {
		t.Errorf("Check of a hanging wrapped docker = %v", err)
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Errorf("Check of a hanging wrapped docker took %s", took)
	}
}

// The build runs on the checked Docker's own builder, whatever other
// builder the operator selected, here one that reaches nothing.
func TestTheBuildRunsOnTheCheckedDockersBuilderWhateverIsSelected(t *testing.T) {
	config := t.TempDir()
	t.Setenv("DOCKER_CONFIG", config)
	if out, err := exec.Command("docker", "buildx", "create", "--name", "elsewhere", "--driver", "remote", "tcp://127.0.0.1:1").CombinedOutput(); err != nil {
		t.Fatalf("making a builder that reaches nothing: %v\n%s", err, out)
	}
	t.Setenv("BUILDX_BUILDER", "elsewhere")
	docker := buildx.Docker{}
	d, err := docker.Check(t.Context())
	if err != nil || d.Builder != "default" {
		t.Fatalf("Check = %+v, %v, want the default context's builder", d, err)
	}
	docker.Builder = d.Builder
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	var out bytes.Buffer
	docker.Output = &out
	if err := docker.Build(ctx, static(t), "moostack.local/web:1", "amd64", filepath.Join(t.TempDir(), "image.tar")); err != nil {
		t.Errorf("Build with another builder selected: %v\n%s", err, out.String())
	}
}

// A builder of another driver that took the context's name, made before
// the context, would build elsewhere than the Docker checked: the check
// refuses it.
func TestTheCheckRefusesABuilderThatTookTheContextsName(t *testing.T) {
	config := t.TempDir()
	t.Setenv("DOCKER_CONFIG", config)
	for _, args := range [][]string{
		{"buildx", "create", "--name", "shadow", "--driver", "remote", "tcp://127.0.0.1:1"},
		{"context", "create", "shadow", "--docker", "host=unix:///var/run/docker.sock"},
	} {
		if out, err := exec.Command("docker", args...).CombinedOutput(); err != nil {
			t.Fatalf("docker %v: %v\n%s", args, err, out)
		}
	}
	t.Setenv("DOCKER_CONTEXT", "shadow")
	_, err := buildx.Docker{}.Check(t.Context())
	if err == nil || !strings.Contains(err.Error(), "the buildx builder shadow, named for docker's context, is one of the remote driver") {
		t.Errorf("Check with a remote builder named for the context = %v, want it refused", err)
	}
}

// A build stopped ends with every process it started, one that ignores
// SIGTERM included, killed once the stop's time passed.
func TestABuildStoppedKillsWhatIgnoresTheStop(t *testing.T) {
	buildx.SetStopDelay(t, 300*time.Millisecond)
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "plugin.pid")
	// The plugin ignores SIGTERM, and holds docker's output.
	docker := fakeDockerCommand(t, "(trap '' TERM; exec sleep 600) &\necho $! > "+pidFile+"\necho building >&2\nwait\n")
	ctx, cancel := context.WithCancel(t.Context())
	out := &signalWriter{Buffer: &bytes.Buffer{}, wrote: make(chan struct{}, 1)}
	done := make(chan error, 1)
	go func() {
		done <- buildx.Docker{Path: docker, Output: out}.Build(ctx, static(t), "moostack.local/web:1", "amd64", filepath.Join(dir, "image.tar"))
	}()
	<-out.wrote
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a stopped Build didn't end within 5s")
	}
	pid, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatal(err)
	}
	if err := exec.Command("kill", "-0", strings.TrimSpace(string(pid))).Run(); err == nil {
		t.Errorf("the plugin, process %s, which ignores SIGTERM, runs on after the build stopped", strings.TrimSpace(string(pid)))
	}
}

// A check stopped, as its time ran out, ends with every process docker
// started, one that ignores SIGTERM included.
func TestACheckStoppedKillsWhatIgnoresTheStop(t *testing.T) {
	pidFile := filepath.Join(t.TempDir(), "child.pid")
	docker := fakeDockerCommand(t, "(trap '' TERM; exec sleep 600) &\necho $! > "+pidFile+"\nwait\n")
	if _, err := (buildx.Docker{Path: docker, CheckTimeout: 200 * time.Millisecond}).Check(t.Context()); err == nil {
		t.Fatal("Check of a docker that hangs succeeded")
	}
	pid, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatal(err)
	}
	if exec.Command("kill", "-0", strings.TrimSpace(string(pid))).Run() == nil {
		exec.Command("kill", "-9", strings.TrimSpace(string(pid))).Run()
		t.Errorf("docker's child, process %s, which ignores SIGTERM, outlived the stopped check", strings.TrimSpace(string(pid)))
	}
}
