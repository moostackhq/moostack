// Package cli is mooctl's command line.
package cli

import (
	"io"
	"os"

	"github.com/urfave/cli/v3"
)

// Env is what mooctl's commands take from the machine they run on.
type Env struct {
	// Home is the operator's home directory.
	Home string
	// Stdout is where results are printed.
	Stdout io.Writer
	// Stderr is where progress, warnings and problems are printed.
	Stderr io.Writer
	// Exit ends mooctl.
	Exit func(code int)
}

// OSEnv is the Env of the running process.
func OSEnv() (Env, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return Env{}, err
	}
	return Env{
		Home:   home,
		Stdout: os.Stdout,
		Stderr: os.Stderr,
		Exit:   os.Exit,
	}, nil
}

// New is the mooctl command.
func New(env Env) *cli.Command {
	return &cli.Command{
		Name:      "mooctl",
		Usage:     "operate a Moostack cluster",
		Writer:    env.Stdout,
		ErrWriter: env.Stderr,
	}
}
