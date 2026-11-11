//go:build !unix

package buildx

import "os/exec"

// stoppedWhole leaves cmd to be killed once its context ends, where
// there are no process groups to stop it with.
func stoppedWhole(*exec.Cmd) (killRest func()) { return func() {} }
