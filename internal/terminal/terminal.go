// Package terminal is the operator's terminal, where mooctl asks them
// questions.
package terminal

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"

	"golang.org/x/term"

	"github.com/moostackhq/moostack/internal/interrupt"
)

// Terminal is the operator's terminal. Prompts are written to it.
type Terminal interface {
	io.Writer
	// ReadLine reads a line, without its line ending.
	ReadLine() (string, error)
	// ReadPassword reads a line without echoing it.
	ReadPassword() ([]byte, error)
}

// tty is the controlling terminal: /dev/tty, or the console on Windows.
type tty struct {
	// in is read from, and out written to. They are the same file where
	// the terminal is one device.
	in, out *os.File
	r       *bufio.Reader
	// exit ends the process after a signal at a password prompt.
	exit func(code int)
}

// Open opens the controlling terminal, as a Terminal and a Closer to
// close it with. Both are nil if there is no terminal, so that a missing
// terminal is never a non-nil Terminal. A signal at a password prompt
// restores the terminal and then calls exit, or os.Exit if exit is nil.
func Open(exit func(code int)) (Terminal, io.Closer) {
	if exit == nil {
		exit = os.Exit
	}
	in, out, err := openTerminal()
	if err != nil {
		return nil, nil
	}
	t := &tty{in: in, out: out, r: bufio.NewReader(in), exit: exit}
	if !term.IsTerminal(int(in.Fd())) {
		t.Close()
		return nil, nil
	}
	return t, t
}

// Write writes p to the terminal.
func (t *tty) Write(p []byte) (int, error) { return t.out.Write(p) }

// ReadLine reads a line, without its line ending.
func (t *tty) ReadLine() (string, error) {
	line, err := t.r.ReadString('\n')
	if err != nil && (err != io.EOF || line == "") {
		return "", err
	}
	return strings.TrimRight(line, "\r\n"), nil
}

// ReadPassword reads a line without echoing it. Echo is off meanwhile,
// so a signal turns it back on and ends the prompt's line before the
// process exits, rather than leaving the terminal without echo.
func (t *tty) ReadPassword() ([]byte, error) {
	fd := int(t.in.Fd())
	state, err := term.GetState(fd)
	if err != nil {
		return nil, err
	}
	guard := interrupt.Catch(func() {
		term.Restore(fd, state)
		fmt.Fprintln(t.out)
	}, t.exit)
	defer guard.Stop()
	return term.ReadPassword(fd)
}

// Close closes the terminal.
func (t *tty) Close() error {
	err := t.in.Close()
	if t.out != t.in {
		if cerr := t.out.Close(); err == nil {
			err = cerr
		}
	}
	return err
}
