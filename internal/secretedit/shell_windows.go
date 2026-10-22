//go:build windows

package secretedit

import (
	"os"
	"os/exec"
	"syscall"
)

// defaultEditor is the editor when neither $VISUAL nor $EDITOR is set.
const defaultEditor = "notepad"

// shellCommand runs the command editor with cmd.exe, with args appended,
// each quoted for the program that receives it. /S has cmd.exe strip
// only the outer quotes, so the rest is run as written.
func shellCommand(editor string, args []string) *exec.Cmd {
	shell := os.Getenv("ComSpec")
	if shell == "" {
		shell = "cmd.exe"
	}
	line := editor
	for _, a := range args {
		line += " " + syscall.EscapeArg(a)
	}
	cmd := exec.Command(shell)
	cmd.SysProcAttr = &syscall.SysProcAttr{CmdLine: syscall.EscapeArg(shell) + ` /S /C "` + line + `"`}
	return cmd
}
