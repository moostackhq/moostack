package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/moostackhq/moostack/internal/orchestrator"
	"github.com/moostackhq/moostack/internal/orchestrator/orchestratortest"
)

var update = flag.Bool("update", false, "rewrite the golden files")

// TestMain runs console itself, on a cluster that runs nothing, in a
// test binary started with CONSOLE_TEST_COMMAND set.
func TestMain(m *testing.M) {
	if os.Getenv("CONSOLE_TEST_COMMAND") != "" {
		os.Exit(command(os.Args[1:], os.Stdout, os.Stderr, fakeCluster))
	}
	os.Exit(m.Run())
}

// startProcess starts the console command as a process of its own with
// args, and returns it with what it writes to standard output and
// standard error.
func startProcess(t *testing.T, args ...string) (cmd *exec.Cmd, stdout, stderr *bytes.Buffer) {
	t.Helper()
	stdout, stderr = &bytes.Buffer{}, &bytes.Buffer{}
	cmd = exec.Command(os.Args[0], args...)
	cmd.Env = append(os.Environ(), "CONSOLE_TEST_COMMAND=1")
	cmd.Stdout, cmd.Stderr = stdout, stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cmd.Process.Kill()
		cmd.Wait()
	})
	return cmd, stdout, stderr
}

// wait returns what cmd.Wait returns, or fails the test after 30
// seconds.
func wait(t *testing.T, cmd *exec.Cmd) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		return err
	case <-time.After(30 * time.Second):
		cmd.Process.Kill()
		t.Fatal("the process didn't end within 30 seconds")
		return nil
	}
}

// answers waits until the console answers at addr.
func answers(t *testing.T, addr string, stderr *bytes.Buffer) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		res, err := http.Get("http://" + addr + "/console/login")
		if err == nil {
			res.Body.Close()
			http.DefaultClient.CloseIdleConnections()
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("console doesn't answer at %s: %v\n%s", addr, err, stderr.String())
		}
	}
}

func freeAddress(t *testing.T) string {
	t.Helper()
	free, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer free.Close()
	return free.Addr().String()
}

func fakeCluster() (orchestrator.Orchestrator, error) {
	return &orchestratortest.Fake{}, nil
}

// socketPath returns a path for a token socket in a new directory.
func socketPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "console")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return filepath.Join(dir, "console.sock")
}

// startConsole runs console on a free port and on a token socket in a
// new directory. It returns the address, the socket's path and a
// function that stops the console and returns what run returned.
func startConsole(t *testing.T) (addr, socket string, stop func() error) {
	t.Helper()
	addr, socket = freeAddress(t), socketPath(t)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var ended error
	go func() {
		defer close(done)
		ended = run(ctx, []string{"-listen", addr, "-socket", socket}, io.Discard, fakeCluster)
	}()
	stop = func() error {
		cancel()
		select {
		case <-done:
			return ended
		case <-time.After(30 * time.Second):
			t.Fatal("console didn't end within 30 seconds of being stopped")
			return nil
		}
	}
	t.Cleanup(func() { stop() })

	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		res, err := http.Get("http://" + addr + "/console/login")
		if err == nil {
			res.Body.Close()
			return addr, socket, stop
		}
		select {
		case <-done:
			t.Fatalf("console ended: %v", ended)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("console doesn't answer at %s: %v", addr, err)
		}
	}
}

func TestConsoleServesOnTheAddressAndTheSocketGivenUntilStopped(t *testing.T) {
	addr, socket, stop := startConsole(t)
	res, err := http.Get("http://" + addr + "/console/login")
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Errorf("the login page at %s: status %d", addr, res.StatusCode)
	}

	var stdout bytes.Buffer
	if err := run(context.Background(), []string{"token", "-socket", socket}, &stdout, fakeCluster); err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`^[A-Z2-7]{26}\n$`).MatchString(stdout.String()) {
		t.Errorf("console token printed %q, want a token of 26 characters and a newline", stdout.String())
	}

	http.DefaultClient.CloseIdleConnections()
	if err := stop(); err != nil {
		t.Errorf("console ended with %v", err)
	}
	if _, err := os.Stat(socket); !os.IsNotExist(err) {
		t.Errorf("the socket is still there: %v", err)
	}
	if _, err := http.Get("http://" + addr + "/console/login"); err == nil {
		t.Error("the console still answers")
	}
}

func TestASecondConsoleLeavesTheFirstItsSocket(t *testing.T) {
	addr, socket, _ := startConsole(t)
	other := freeAddress(t)
	for what, args := range map[string][]string{
		"on the same address and socket": {"-listen", addr, "-socket", socket},
		"on the same socket":             {"-listen", other, "-socket", socket},
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		err := run(ctx, args, io.Discard, fakeCluster)
		cancel()
		if err == nil {
			t.Errorf("a second console %s: no error", what)
		}
		var stdout bytes.Buffer
		if err := run(context.Background(), []string{"token", "-socket", socket}, &stdout, fakeCluster); err != nil {
			t.Errorf("console token after a second console %s: %v", what, err)
		}
	}
	l, err := net.Listen("tcp", other)
	if err != nil {
		t.Fatalf("the second console kept its address: %v", err)
	}
	l.Close()
}

func TestAConsoleWhoseAddressIsTakenLeavesAFileAtTheSocketsPath(t *testing.T) {
	taken, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer taken.Close()
	socket := socketPath(t)
	if err := os.WriteFile(socket, []byte("left behind"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := run(context.Background(), []string{"-listen", taken.Addr().String(), "-socket", socket}, io.Discard, fakeCluster); err == nil {
		t.Error("console on a taken address: no error")
	}
	if left, err := os.ReadFile(socket); err != nil || string(left) != "left behind" {
		t.Errorf("the file at the socket's path is %q, %v", left, err)
	}
}

func TestConsoleWithoutAClusterSaysSoAndListensOnNothing(t *testing.T) {
	addr, socket := freeAddress(t), socketPath(t)
	noCluster := func() (orchestrator.Orchestrator, error) { return nil, errors.New("not in a cluster") }
	err := run(context.Background(), []string{"-listen", addr, "-socket", socket}, io.Discard, noCluster)
	if err == nil || err.Error() != "not in a cluster" {
		t.Errorf("console without a cluster: %v", err)
	}
	if _, err := os.Stat(socket); !os.IsNotExist(err) {
		t.Errorf("the socket was made: %v", err)
	}
	l, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("the address was taken: %v", err)
	}
	l.Close()
}

func TestConsoleTokenWithoutAConsoleSaysSo(t *testing.T) {
	socket := socketPath(t)
	var stdout bytes.Buffer
	err := run(context.Background(), []string{"token", "-socket", socket}, &stdout, fakeCluster)
	if err == nil || !strings.Contains(err.Error(), "no console is listening at "+socket) {
		t.Errorf("console token without a console: %v, want an error that no console is listening at the socket", err)
	}
	if stdout.Len() != 0 {
		t.Errorf("console token printed %q", stdout.String())
	}
}

func TestConsoleRefusesArgumentsItDoesntKnowInOneLine(t *testing.T) {
	_, socket, _ := startConsole(t)
	output, err := os.Create(filepath.Join(t.TempDir(), "stderr"))
	if err != nil {
		t.Fatal(err)
	}
	defer output.Close()
	stderr := os.Stderr
	os.Stderr = output
	defer func() { os.Stderr = stderr }()

	for _, c := range []struct {
		args []string
		want string
	}{
		{[]string{"tokens"}, "console takes no arguments but token"},
		{[]string{"-nothing"}, "flag provided but not defined: -nothing"},
		{[]string{"token", "-socket", socket, "extra"}, "console token takes no arguments"},
		{[]string{"token", "-nothing"}, "flag provided but not defined: -nothing"},
	} {
		var stdout bytes.Buffer
		err := run(context.Background(), c.args, &stdout, fakeCluster)
		if err == nil || err.Error() != c.want {
			t.Errorf("console %s: %v, want %q", strings.Join(c.args, " "), err, c.want)
		}
		if stdout.Len() != 0 {
			t.Errorf("console %s printed %q", strings.Join(c.args, " "), stdout.String())
		}
	}
	if printed, _ := os.ReadFile(output.Name()); len(printed) != 0 {
		t.Errorf("run wrote to standard error itself:\n%s", printed)
	}
}

func TestConsoleRefusesAnEmptyAddressAndASocketPathThatIsntAbsolute(t *testing.T) {
	for _, c := range []struct {
		args []string
		want string
	}{
		{[]string{"-listen", "", "-socket", socketPath(t)}, "-listen is empty"},
		{[]string{"-listen", "127.0.0.1:0", "-socket", ""}, `the token socket's path "" isn't absolute`},
		{[]string{"-listen", "127.0.0.1:0", "-socket", "@console"}, `the token socket's path "@console" isn't absolute`},
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		err := run(ctx, c.args, io.Discard, fakeCluster)
		cancel()
		if err == nil || err.Error() != c.want {
			t.Errorf("console %q: %v, want %q", c.args, err, c.want)
		}
	}
}

func TestHelpPrintsTheFlagsWithTheirDefaults(t *testing.T) {
	for name, args := range map[string][]string{"help.golden": {"-h"}, "token-help.golden": {"token", "-h"}} {
		var stdout bytes.Buffer
		if err := run(context.Background(), args, &stdout, fakeCluster); err != nil {
			t.Errorf("console %s: %v", strings.Join(args, " "), err)
		}
		path := filepath.Join("testdata", name)
		if *update {
			if err := os.WriteFile(path, stdout.Bytes(), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		want, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if stdout.String() != string(want) {
			t.Errorf("console %s printed, not as %s; run with -update and review the change:\n%s", strings.Join(args, " "), path, stdout.String())
		}
	}
}

func TestSIGTERMStopsTheConsoleWithExit0AndRemovesTheSocket(t *testing.T) {
	addr, socket := freeAddress(t), socketPath(t)
	cmd, stdout, stderr := startProcess(t, "-listen", addr, "-socket", socket)
	answers(t, addr, stderr)
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := wait(t, cmd); err != nil {
		t.Errorf("console after SIGTERM: %v", err)
	}
	if stdout.Len() != 0 || stderr.Len() != 0 {
		t.Errorf("console printed %q and %q", stdout.String(), stderr.String())
	}
	if _, err := os.Stat(socket); !os.IsNotExist(err) {
		t.Errorf("the socket is still there: %v", err)
	}
}

func TestAFailurePrintsOneLineToStandardErrorAndExits1(t *testing.T) {
	cmd, stdout, stderr := startProcess(t, "-nothing")
	err := wait(t, cmd)
	if exit, ok := errors.AsType[*exec.ExitError](err); !ok || exit.ExitCode() != 1 {
		t.Errorf("console -nothing: %v, want exit 1", err)
	}
	if got := stderr.String(); got != "console: flag provided but not defined: -nothing\n" {
		t.Errorf("standard error is %q", got)
	}
	if stdout.Len() != 0 {
		t.Errorf("standard output is %q", stdout.String())
	}
}

func TestAnInterruptEndsConsoleTokenAtOnce(t *testing.T) {
	socket := socketPath(t)
	l, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	cmd, _, stderr := startProcess(t, "token", "-socket", socket)
	l.(*net.UnixListener).SetDeadline(time.Now().Add(30 * time.Second))
	conn, err := l.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	start := time.Now()
	if err := cmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	err = wait(t, cmd)
	if exit, ok := errors.AsType[*exec.ExitError](err); !ok || exit.ExitCode() != 1 {
		t.Errorf("console token after an interrupt: %v, want exit 1", err)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Errorf("console token ended %v after the interrupt, want at once", took)
	}
	if got := stderr.String(); got != "console: context canceled\n" {
		t.Errorf("standard error is %q", got)
	}
}

func TestASecondSIGTERMEndsAStopThatWaitsForARequest(t *testing.T) {
	addr, socket := freeAddress(t), socketPath(t)
	cmd, _, stderr := startProcess(t, "-listen", addr, "-socket", socket)
	answers(t, addr, stderr)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	io.WriteString(conn, "POST /console/login HTTP/1.1\r\nHost: console.test\r\nContent-Length: 100\r\n\r\ntoken=")
	time.Sleep(200 * time.Millisecond)

	start := time.Now()
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("the console ended on the first SIGTERM, with a request open: %v", err)
	}
	err = wait(t, cmd)
	if exit, ok := errors.AsType[*exec.ExitError](err); !ok || exit.Sys().(syscall.WaitStatus).Signal() != syscall.SIGTERM {
		t.Errorf("the console after a second SIGTERM: %v, want it ended by the signal", err)
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Errorf("the console ended %v after the first SIGTERM, want at once after the second", took)
	}
}

func TestTheBuiltConsolePrintsAFailureToStandardErrorAndExits1(t *testing.T) {
	if testing.Short() {
		t.Skip("builds console")
	}
	binary := filepath.Join(t.TempDir(), "console")
	if out, err := exec.Command("go", "build", "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	var stdout, stderr bytes.Buffer
	cmd := exec.Command(binary, "-nothing")
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	if exit, ok := errors.AsType[*exec.ExitError](err); !ok || exit.ExitCode() != 1 {
		t.Errorf("console -nothing: %v, want exit 1", err)
	}
	if stdout.Len() != 0 || stderr.String() != "console: flag provided but not defined: -nothing\n" {
		t.Errorf("console -nothing printed %q to standard output and %q to standard error", stdout.String(), stderr.String())
	}

	stdout.Reset()
	stderr.Reset()
	cmd = exec.Command(binary, "-h")
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		t.Errorf("console -h: %v", err)
	}
	if !strings.Contains(stdout.String(), "/run/console/console.sock") || stderr.Len() != 0 {
		t.Errorf("console -h printed %q to standard output and %q to standard error", stdout.String(), stderr.String())
	}

	stdout.Reset()
	stderr.Reset()
	cmd = exec.Command(binary)
	cmd.Env = slices.DeleteFunc(os.Environ(), func(kv string) bool { return strings.HasPrefix(kv, "KUBERNETES_SERVICE_") })
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err = cmd.Run()
	if exit, ok := errors.AsType[*exec.ExitError](err); !ok || exit.ExitCode() != 1 || !strings.HasPrefix(stderr.String(), "console: ") {
		t.Errorf("console outside a cluster: %v, with %q on standard error; want exit 1 and what is missing", err, stderr.String())
	}
}

func TestOfTwoConsolesStartedAtOnceOnTheSocketOneRuns(t *testing.T) {
	socket := socketPath(t)
	if left, err := net.ListenUnix("unix", &net.UnixAddr{Name: socket, Net: "unix"}); err != nil {
		t.Fatal(err)
	} else {
		left.SetUnlinkOnClose(false)
		left.Close()
	}
	type started struct {
		cmd    *exec.Cmd
		stderr *bytes.Buffer
		ended  chan error
	}
	var consoles []started
	for range 2 {
		cmd, _, stderr := startProcess(t, "-listen", freeAddress(t), "-socket", socket)
		ended := make(chan error, 1)
		go func() { ended <- cmd.Wait() }()
		consoles = append(consoles, started{cmd, stderr, ended})
	}

	refused := 0
	deadline := time.After(10 * time.Second)
	for refused == 0 {
		select {
		case <-consoles[0].ended:
			refused++
			consoles = consoles[1:]
		case <-consoles[1].ended:
			refused++
			consoles = consoles[:1]
		case <-deadline:
			t.Fatal("both consoles still run after 10 seconds")
		}
	}
	select {
	case err := <-consoles[0].ended:
		t.Fatalf("both consoles ended; the second with %v:\n%s", err, consoles[0].stderr.String())
	case <-time.After(500 * time.Millisecond):
	}
	var stdout bytes.Buffer
	if err := run(context.Background(), []string{"token", "-socket", socket}, &stdout, fakeCluster); err != nil {
		t.Errorf("console token from the console that runs: %v", err)
	}
	consoles[0].cmd.Process.Signal(syscall.SIGTERM)
	<-consoles[0].ended
}
