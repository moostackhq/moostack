//go:build unix

package buildx

import (
	"os/exec"
	"sync/atomic"
	"syscall"
)

// stoppedWhole has cmd, once its context ends, stopped with SIGTERM with
// every process it started, such as docker's buildx plugin, which would
// otherwise build on, holding cmd's output open. The returned killRest,
// called once cmd's Wait returned, kills what is left of them if cmd was
// stopped, as one that ignores SIGTERM, which Wait leaves running once
// cmd's WaitDelay passed, killing cmd alone.
func stoppedWhole(cmd *exec.Cmd) (killRest func()) {
	var stopped atomic.Bool
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		stopped.Store(true)
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
	}
	return func() {
		// The group is there while any of its processes is, so its id is
		// no other's.
		if stopped.Load() {
			syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
	}
}
