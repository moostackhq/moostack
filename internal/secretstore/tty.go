package secretstore

import (
	"bufio"
	"io"
	"os"
	"strings"

	"golang.org/x/term"
)

// tty is the controlling terminal, /dev/tty.
type tty struct {
	f *os.File
	r *bufio.Reader
}

// OpenTTY opens the controlling terminal, as a Terminal for KeyOptions
// and a Closer to close it with. Both are nil if there is no terminal, so
// that a missing terminal is never a non-nil Terminal.
func OpenTTY() (Terminal, io.Closer) {
	f, err := os.OpenFile("/dev/tty", os.O_RDWR, 0)
	if err != nil {
		return nil, nil
	}
	if !term.IsTerminal(int(f.Fd())) {
		f.Close()
		return nil, nil
	}
	t := &tty{f: f, r: bufio.NewReader(f)}
	return t, t
}

func (t *tty) Write(p []byte) (int, error) { return t.f.Write(p) }

// ReadLine reads a line, without its line ending.
func (t *tty) ReadLine() (string, error) {
	line, err := t.r.ReadString('\n')
	if err != nil && (err != io.EOF || line == "") {
		return "", err
	}
	return strings.TrimRight(line, "\r\n"), nil
}

// ReadPassword reads a line without echoing it.
func (t *tty) ReadPassword() ([]byte, error) {
	return term.ReadPassword(int(t.f.Fd()))
}

// Close closes the terminal.
func (t *tty) Close() error { return t.f.Close() }
