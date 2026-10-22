package secretedit

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// vimArgs keep vi, vim and nvim from writing the plaintext to swap,
// backup, undo and viminfo files. The settings are given again after
// the vimrc, which could turn them back on.
var vimArgs = []string{
	"-n", "-i", "NONE",
	"--cmd", vimSettings,
	"-c", vimSettings,
}

// vimSettings turn off backup and undo files.
const vimSettings = "set nobackup nowritebackup noundofile"

// editorCommand is the operator's editor: $VISUAL, else $EDITOR, else
// defaultEditor. It is a shell command, so it can take arguments, as in
// "code --wait".
func editorCommand() string {
	for _, name := range []string{"VISUAL", "EDITOR"} {
		if e := strings.TrimSpace(os.Getenv(name)); e != "" {
			return e
		}
	}
	return defaultEditor
}

// runEditor opens path in the operator's editor and waits for it to
// exit. The editor gets mooctl's own standard input and output, which
// are the operator's terminal, not the streams mooctl prints to.
func runEditor(path string) error {
	editor := editorCommand()
	var args []string
	program := strings.TrimSuffix(strings.ToLower(filepath.Base(strings.Fields(editor)[0])), ".exe")
	switch program {
	case "vi", "vim", "nvim":
		args = slices.Clone(vimArgs)
	}
	args = append(args, path)
	cmd := shellCommand(editor, args)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("editor %q: %w", editor, err)
	}
	return nil
}
