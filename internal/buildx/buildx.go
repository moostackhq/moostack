// Package buildx builds an app's image on the operator's machine with
// the docker buildx CLI, into an OCI image layout archive that a node
// imports. It installs nothing: it checks that Docker is there and can
// write such an archive, and says what to install or switch if not.
package buildx

import (
	"bytes"
	"cmp"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Docker is the docker CLI that builds images, which reaches Docker as
// its environment and its context say, as DOCKER_HOST does.
type Docker struct {
	// Path is the docker command, docker on the PATH if empty.
	Path string
	// Output is where buildx's progress and errors go, as buildx prints
	// them; nowhere if nil.
	Output io.Writer
	// CheckTimeout bounds each question Check asks of docker;
	// DefaultCheckTimeout if zero.
	CheckTimeout time.Duration
	// Builder is the buildx builder that builds, as Check finds it; the
	// one the operator selected if empty.
	Builder string
}

// reachDocker is what to do when Docker doesn't answer.
const reachDocker = "start Docker, check that you may reach it, or point DOCKER_HOST or the docker context at it"

// DefaultCheckTimeout is how long docker may take to answer each of
// Check's questions, when Docker's own dial doesn't end it first.
const DefaultCheckTimeout = time.Minute

// imageStore is what Docker's info says of its image store when it is
// containerd's, the one buildx writes an OCI archive from with Docker's
// own builder.
const imageStore = "io.containerd.snapshotter.v1"

// Daemon is the Docker that builds: Arch is the architecture of its
// machine, as Go names it, such as amd64, and Builder the buildx builder
// of its own that builds, named for docker's context, which Build is to
// be given rather than any the operator selected.
type Daemon struct{ Arch, Builder string }

// minEngine is the first major version of Docker Engine whose BuildKit
// rewrites a build's file times to SOURCE_DATE_EPOCH, as a reproducible
// build needs; an older one ignores rewrite-timestamp without a word.
const minEngine = 26

// Check fails, saying what to install, update or switch, unless the
// docker command is there with its buildx plugin, and Docker answers,
// within CheckTimeout, is Docker Engine minEngine or later, and keeps
// its images in the containerd image store. It returns the Docker that
// builds, whose own builder builds whatever other builder the operator
// selected, as with BUILDX_BUILDER, since it is the Docker checked. A
// check stopped fails with ctx's cause.
func (d Docker) Check(ctx context.Context) (Daemon, error) {
	path, err := exec.LookPath(d.command())
	if err != nil {
		return Daemon{}, fmt.Errorf("there is no %s command: install Docker Engine with its buildx plugin, or Docker Desktop", d.command())
	}
	// Docker's version says it answered: docker info succeeds with
	// nothing of the server's when it can't reach it, as when its dial
	// times out.
	out, err := d.ask(ctx, path, "info", "--format", `{{.ServerVersion}}|{{.Architecture}}|{{range .DriverStatus}}{{if eq (index . 0) "driver-type"}}{{index . 1}}{{end}}{{end}}`)
	if err != nil {
		return Daemon{}, err
	}
	fields := strings.SplitN(strings.TrimSpace(out.stdout), "|", 3)
	for len(fields) < 3 {
		fields = append(fields, "")
	}
	version, arch, store := fields[0], fields[1], fields[2]
	major, _, _ := strings.Cut(version, ".")
	switch n, err := strconv.Atoi(major); {
	case version == "":
		return Daemon{}, fmt.Errorf("Docker doesn't answer: %s; %s", cmp.Or(lastLine(out.stderr), "docker info says nothing of it"), reachDocker)
	case err != nil || n < minEngine:
		return Daemon{}, fmt.Errorf("Docker is %s, whose builds aren't reproducible; update it to Docker Engine %d or later, or Docker Desktop 4.29 or later", version, minEngine)
	case store != imageStore:
		return Daemon{}, errors.New(`Docker keeps its images in its classic image store, from which buildx can't write an image's archive; switch it to the containerd image store: set "features": {"containerd-snapshotter": true} in Docker Engine's daemon.json and restart Docker, or turn on "Use containerd for pulling and storing images" in Docker Desktop's settings`)
	}
	if _, err := d.ask(ctx, path, "buildx", "version"); err != nil {
		var failed *failedError
		if errors.As(err, &failed) {
			return Daemon{}, fmt.Errorf("docker has no buildx plugin: %v; install Docker's buildx plugin", failed.err)
		}
		return Daemon{}, err
	}
	// buildx names the builder of a context's own Docker for the context.
	current, err := d.ask(ctx, path, "context", "show")
	if err != nil {
		return Daemon{}, err
	}
	builder := strings.TrimSpace(current.stdout)
	// A builder of another driver made before a context of its name
	// takes the name: it builds elsewhere than the Docker checked.
	inspected, err := d.ask(ctx, path, "buildx", "inspect", "--", builder)
	if err != nil {
		return Daemon{}, err
	}
	driver := ""
	for _, line := range strings.Split(inspected.stdout, "\n") {
		if v, ok := strings.CutPrefix(line, "Driver:"); ok {
			driver = strings.TrimSpace(v)
			break
		}
	}
	if driver != "docker" {
		return Daemon{}, fmt.Errorf("the buildx builder %s, named for docker's context, is one of the %s driver, not the context's own Docker, which mooctl checked; remove it with docker buildx rm %s, or use another context", builder, cmp.Or(driver, "unknown"), builder)
	}
	return Daemon{Arch: goArch(arch), Builder: builder}, nil
}

// goArch is the architecture Docker says its machine is, as uname names
// it, as Go names it.
func goArch(uname string) string {
	switch uname {
	case "x86_64":
		return "amd64"
	case "aarch64":
		return "arm64"
	}
	return uname
}

// ask runs path with args, within CheckTimeout, and returns what it
// printed. It fails with ctx's cause if ctx ended, says Docker doesn't
// answer if the time ran out, and fails with a *failedError if path
// failed.
func (d Docker) ask(ctx context.Context, path string, args ...string) (output, error) {
	timeout := cmp.Or(d.CheckTimeout, DefaultCheckTimeout)
	askCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	out, err := run(askCtx, path, args...)
	switch {
	case ctx.Err() != nil:
		return out, context.Cause(ctx)
	case askCtx.Err() != nil:
		return out, fmt.Errorf("Docker doesn't answer within %s: docker %s didn't end; %s", timeout, args[0], reachDocker)
	case err != nil:
		if line := lastLine(out.stderr); line != "" {
			err = errors.New(line)
		}
		if args[0] == "info" {
			return out, fmt.Errorf("Docker doesn't answer: %v; %s", err, reachDocker)
		}
		return out, &failedError{err}
	}
	return out, nil
}

// failedError says a docker command failed, as err says.
type failedError struct{ err error }

func (e *failedError) Error() string { return e.err.Error() }

// Build builds the image of the build context at dir, from the
// Dockerfile at its root, for linux on arch, such as amd64, tagged tag,
// into an OCI image layout archive at dest. The archive holds the image
// alone, without attestations, which would wrap it in an index that a
// node of any architecture imports. The build is reproducible: its
// times are the epoch's, so that the same source builds the same image,
// of the same digest, on any machine and with any cache. buildx's
// progress and errors go to Output, as plain text. A build that fails,
// fails with a *BuildError.
func (d Docker) Build(ctx context.Context, dir, tag, arch, dest string) error {
	// An absolute context is never taken for stdin, as - is, or for a
	// flag.
	dir, err := filepath.Abs(dir)
	if err != nil {
		return &BuildError{Err: err}
	}
	args := []string{"buildx", "build"}
	if d.Builder != "" {
		args = append(args, "--builder", d.Builder)
	}
	cmd := exec.CommandContext(ctx, d.command(), append(args,
		"--platform", "linux/"+arch,
		"--provenance=false", "--sbom=false",
		"--progress=plain",
		"--build-arg", "SOURCE_DATE_EPOCH=0",
		"--tag", tag,
		"--output", csvLine("type=oci", "dest="+dest, "rewrite-timestamp=true"),
		"--", dir)...)
	out := d.Output
	if out == nil {
		out = io.Discard
	}
	watched := &watch{w: out, needle: []byte(execFormatError)}
	cmd.Stdout, cmd.Stderr = watched, watched
	killRest := stoppedWhole(cmd)
	// What goes on past a stop is killed, and its output let go of.
	cmd.WaitDelay = stopDelay
	err = cmd.Run()
	killRest()
	if err != nil {
		if ctx.Err() != nil {
			err = context.Cause(ctx)
		}
		return &BuildError{Err: err, ExecFormat: watched.seen}
	}
	return nil
}

// csvLine is fields as one line of CSV, as buildx reads an output's
// options, quoted where a field holds a comma or a quote.
func csvLine(fields ...string) string {
	var b strings.Builder
	w := csv.NewWriter(&b)
	w.Write(fields)
	w.Flush()
	return strings.TrimSuffix(b.String(), "\n")
}

// askStopDelay is how long a question of Check's stopped has to end
// before it is killed, and stopDelay a build stopped, with every process
// it started.
var (
	askStopDelay = 2 * time.Second
	stopDelay    = 10 * time.Second
)

// execFormatError is what a build says when a step runs a program for
// another architecture than the one the machine runs, without emulation.
const execFormatError = "exec format error"

// BuildError says a build failed, as Err says. ExecFormat says the build
// said "exec format error", as one does that runs a program for another
// architecture with no emulation for it.
type BuildError struct {
	Err        error
	ExecFormat bool
}

func (e *BuildError) Error() string { return "the build failed: " + e.Err.Error() }

func (e *BuildError) Unwrap() error { return e.Err }

func (d Docker) command() string {
	if d.Path == "" {
		return "docker"
	}
	return d.Path
}

// output is what a command printed.
type output struct{ stdout, stderr string }

// run runs path with args, and returns what it printed and how it ended.
// Once ctx ends, it stops path with every process path started, as a
// wrapper of docker's would leave its docker holding the output.
func run(ctx context.Context, path string, args ...string) (output, error) {
	var stdout, stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, path, args...)
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	killRest := stoppedWhole(cmd)
	cmd.WaitDelay = askStopDelay
	err := cmd.Run()
	killRest()
	return output{stdout.String(), stderr.String()}, err
}

// lastLine is the last line of s that isn't blank, empty if none is.
func lastLine(s string) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	return strings.TrimSpace(lines[len(lines)-1])
}

// watch writes to w and notes whether what it wrote ever held needle,
// across writes.
type watch struct {
	w      io.Writer
	needle []byte
	tail   []byte
	seen   bool
}

func (s *watch) Write(p []byte) (int, error) {
	if !s.seen {
		joined := append(s.tail, p...)
		s.seen = bytes.Contains(joined, s.needle)
		s.tail = append([]byte(nil), joined[max(0, len(joined)-len(s.needle)+1):]...)
	}
	return s.w.Write(p)
}
