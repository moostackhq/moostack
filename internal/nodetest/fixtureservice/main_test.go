package main

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"
)

// program is fixtureservice, built.
func program(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fixtureservice")
	if out, err := exec.Command("go", "build", "-o", path, ".").CombinedOutput(); err != nil {
		t.Fatalf("building fixtureservice: %v\n%s", err, out)
	}
	return path
}

func TestAsAnAppThatExitsItExitsWithTheCodeGiven(t *testing.T) {
	// Bounded, so that one that runs on fails rather than hangs.
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, program(t), "-exit", "3").Output()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 3 || string(out) != "exiting with 3\n" {
		t.Errorf("-exit 3 printed %q and ended with %v, want it to say so and exit 3", out, err)
	}
}

func TestAsAnAppItSaysWhoItRunsAsAndRunsUntilTerminated(t *testing.T) {
	cmd := exec.Command(program(t))
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	want := "running as " + strconv.Itoa(os.Getuid()) + ":" + strconv.Itoa(os.Getgid()) + "\n"
	line := make([]byte, len(want))
	if _, err := stdout.Read(line); err != nil || string(line) != want {
		t.Fatalf("as an app, it printed %q, %v, want %q", line, err, want)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		t.Fatalf("as an app, it ended before it was terminated: %v", err)
	case <-time.After(500 * time.Millisecond):
	}
	cmd.Process.Signal(syscall.SIGTERM)
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("as an app, it ended with %v once terminated, want it to exit cleanly", err)
		}
	case <-time.After(10 * time.Second):
		t.Error("as an app, it runs on once terminated")
	}
}
