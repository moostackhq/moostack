package cli

import (
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// Ctrl-C at the terminal, which signals mooctl's whole process group,
// doesn't stop the push of an operation that changed the cluster.
func TestCtrlCDuringFinishLetsThePushEnd(t *testing.T) {
	dir, other, remote := clusterRepos(t)
	// A slow remote, so the push is under way when Ctrl-C comes.
	hook := filepath.Join(remote, "hooks", "pre-receive")
	os.WriteFile(hook, []byte("#!/bin/sh\nsleep 3\n"), 0o755)
	env, stderr := testEnv()
	host := &fakeHost{}
	op, opCtx, err := begin(t.Context(), env, dir, host, true)
	if err != nil {
		t.Fatal(err)
	}
	defer op.release()
	os.WriteFile(filepath.Join(dir, "node1.yml"), []byte("x\n"), 0o644)

	// This process stands in for mooctl at a terminal: its own process
	// group, the foreground one, catching SIGINT as interrupt.Context does.
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt)
	defer signal.Stop(sigs)
	if err := syscall.Setpgid(0, 0); err != nil {
		t.Fatal(err)
	}
	go func() {
		time.Sleep(1500 * time.Millisecond)
		syscall.Kill(-os.Getpid(), syscall.SIGINT)
	}()
	if err := op.finish(opCtx, "node-add node1", host); err != nil {
		t.Logf("finish = %v", err)
	}
	head := git(t, dir, "rev-parse", "HEAD")
	git(t, other, "fetch", "--quiet")
	if got := git(t, other, "rev-parse", "origin/main"); got != head {
		t.Errorf("Ctrl-C during finish: the commit wasn't pushed; stderr: %s", stderr)
	}
}

// The commit waits for another git command that holds the index.
func TestFinishWaitsForAnotherGitCommand(t *testing.T) {
	dir, _, _ := clusterRepos(t)
	env, _ := testEnv()
	host := &fakeHost{}
	op, opCtx, err := begin(t.Context(), env, dir, host, true)
	if err != nil {
		t.Fatal(err)
	}
	defer op.release()
	os.WriteFile(filepath.Join(dir, "node1.yml"), []byte("x\n"), 0o644)
	// git holds .git/index.lock while it works.
	lock := filepath.Join(dir, ".git", "index.lock")
	os.WriteFile(lock, nil, 0o644)
	go func() {
		time.Sleep(time.Second)
		os.Remove(lock)
	}()
	if err := op.finish(opCtx, "node-add node1", host); err != nil {
		t.Fatal(err)
	}
	if len(host.recorded) != 1 {
		t.Error("a busy index left the change unrecorded")
	}
}
