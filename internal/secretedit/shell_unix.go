//go:build !windows

package secretedit

import "os/exec"

// defaultEditor is the editor when neither $VISUAL nor $EDITOR is set.
const defaultEditor = "vi"

// shellCommand runs the shell command editor with args appended, each
// passed as it is.
func shellCommand(editor string, args []string) *exec.Cmd {
	return exec.Command("/bin/sh", append([]string{"-c", editor + ` "$@"`, "sh"}, args...)...)
}
