//go:build windows

package terminal

import "os"

// openTerminal opens the console: CONIN$ to read from, CONOUT$ to write
// to.
func openTerminal() (in, out *os.File, err error) {
	if in, err = os.OpenFile("CONIN$", os.O_RDWR, 0); err != nil {
		return nil, nil, err
	}
	if out, err = os.OpenFile("CONOUT$", os.O_RDWR, 0); err != nil {
		in.Close()
		return nil, nil, err
	}
	return in, out, nil
}
