// Package interrupt catches the signals that end a command, while it
// holds something that mustn't be left behind, such as a terminal
// without echo or a file of plaintext.
package interrupt

import (
	"os"
	"os/signal"
	"sync/atomic"
)

// Guard catches the signals that end a command until Stop: SIGINT,
// SIGTERM and SIGHUP where the system has them. One of them runs the
// cleanup and then exits, except that SIGINT is left to the program Pass
// runs. SIGKILL can't be caught.
type Guard struct {
	signals  chan os.Signal
	done     chan struct{}
	finished chan struct{}
	passing  atomic.Bool
}

// Catch catches the signals until Stop, running cleanup and then exit
// with 128 plus the signal's number if one arrives. The
// signals are caught rather than ignored, because a program started
// meanwhile would inherit an ignored signal.
func Catch(cleanup func(), exit func(code int)) *Guard {
	g := &Guard{
		signals:  make(chan os.Signal, 1),
		done:     make(chan struct{}),
		finished: make(chan struct{}),
	}
	signal.Notify(g.signals, stopSignals...)
	go func() {
		defer close(g.finished)
		for {
			select {
			case sig := <-g.signals:
				if sig == os.Interrupt && g.passing.Load() {
					continue
				}
				// A second signal ends the process even if the cleanup
				// hangs.
				cleaned := make(chan struct{})
				go func() {
					cleanup()
					close(cleaned)
				}()
				select {
				case <-cleaned:
				case <-g.signals:
				}
				exit(exitCode(sig))
				return
			case <-g.done:
				return
			}
		}
	}()
	return g
}

// Pass runs f, during which SIGINT is left to the program f runs: it
// shares the terminal, so Ctrl-C reaches it too. SIGTERM and SIGHUP
// still run the cleanup, since they end the session the program runs
// in.
func (g *Guard) Pass(f func() error) error {
	g.passing.Store(true)
	defer g.passing.Store(false)
	err := f()
	// A Ctrl-C pressed as the program exited may be waiting; it was the
	// program's. One that the runtime delivers after this still ends the
	// command, with its cleanup.
	for {
		select {
		case sig := <-g.signals:
			if sig != os.Interrupt {
				// Put it back for the guard. If the buffer has filled
				// again, the guard has a signal to act on anyway.
				select {
				case g.signals <- sig:
				default:
				}
				return err
			}
		default:
			return err
		}
	}
}

// Stop stops catching the signals.
func (g *Guard) Stop() {
	signal.Stop(g.signals)
	close(g.done)
	<-g.finished
}
