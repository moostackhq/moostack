// Package cli is mooctl's command line.
package cli

import (
	"fmt"
	"io"
	"os"

	"github.com/urfave/cli/v3"

	"github.com/moostackhq/moostack/internal/terminal"
)

// Env is what mooctl's commands take from the machine they run on.
type Env struct {
	// Home is the operator's home directory.
	Home string
	// Stdout is where results are printed.
	Stdout io.Writer
	// Stderr is where warnings and problems are printed.
	Stderr io.Writer
	// Terminal opens the operator's terminal and returns it with a
	// Closer to close it with. Both are nil when there is no terminal.
	// A signal at a password prompt ends mooctl with exit.
	Terminal func(exit func(code int)) (terminal.Terminal, io.Closer)
	// Exit ends mooctl. It is called when a signal stops a command, once
	// what the command held is cleaned up.
	Exit func(code int)
}

// OSEnv is the Env of the running process.
func OSEnv() (Env, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return Env{}, err
	}
	return Env{
		Home:     home,
		Stdout:   os.Stdout,
		Stderr:   os.Stderr,
		Terminal: terminal.Open,
		Exit:     os.Exit,
	}, nil
}

// New is the mooctl command.
func New(env Env) *cli.Command {
	return &cli.Command{
		Name:      "mooctl",
		Usage:     "operate a Moostack cluster",
		Writer:    env.Stdout,
		ErrWriter: env.Stderr,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "dir",
				Aliases: []string{"C"},
				Usage:   "the cluster directory",
				Value:   ".",
			},
		},
		Commands: []*cli.Command{
			secretCommand(env),
		},
	}
}

// wantArgs refuses a command that wasn't given n arguments.
func wantArgs(cmd *cli.Command, n int) error {
	if cmd.NArg() != n {
		return fmt.Errorf("%s takes %s, not %d; see --help", cmd.FullName(), plural(n, "argument"), cmd.NArg())
	}
	return nil
}

// plural is n things, as in "1 argument" or "2 arguments".
func plural(n int, thing string) string {
	if n == 1 {
		return "1 " + thing
	}
	return fmt.Sprintf("%d %ss", n, thing)
}
