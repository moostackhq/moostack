// Command console serves a cluster's console from inside the cluster.
// console token prints a login token of the console running in the same
// container.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/moostackhq/moostack/internal/console"
	"github.com/moostackhq/moostack/internal/orchestrator"
	"github.com/moostackhq/moostack/internal/orchestrator/k3s"
)

const defaultTokenSocket = "/run/console/console.sock"

func main() {
	os.Exit(command(os.Args[1:], os.Stdout, os.Stderr, inCluster))
}

func inCluster() (orchestrator.Orchestrator, error) {
	return k3s.InCluster()
}

// command runs console with args until it ends, or until SIGTERM or an
// interrupt stops it; a second one ends the process at once. It prints
// an error to stderr and returns the exit code.
func command(args []string, stdout, stderr io.Writer, connect func() (orchestrator.Orchestrator, error)) int {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	go func() {
		<-ctx.Done()
		stop()
	}()
	if err := run(ctx, args, stdout, connect); err != nil {
		fmt.Fprintf(stderr, "console: %v\n", err)
		return 1
	}
	return 0
}

// run takes connect, which connects to the cluster the console is for,
// and ctx, which ends when the console is to stop.
func run(ctx context.Context, args []string, stdout io.Writer, connect func() (orchestrator.Orchestrator, error)) error {
	if len(args) > 0 && args[0] == "token" {
		return token(ctx, args[1:], stdout)
	}
	return serve(ctx, args, stdout, connect)
}

func serve(ctx context.Context, args []string, stdout io.Writer, connect func() (orchestrator.Orchestrator, error)) error {
	flags := flag.NewFlagSet("console", flag.ContinueOnError)
	listen := flags.String("listen", ":8080", "the address to serve on")
	socket := flags.String("socket", defaultTokenSocket, "the socket file to issue login tokens on")
	if help, err := parse(flags, args, stdout); help || err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("console takes no arguments but token")
	}
	if *listen == "" {
		return errors.New("-listen is empty")
	}

	cluster, err := connect()
	if err != nil {
		return err
	}
	web, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	tokenSocket, err := console.ListenTokenSocket(*socket)
	if err != nil {
		web.Close()
		return err
	}
	return console.New(cluster).Serve(ctx, web, tokenSocket)
}

func token(ctx context.Context, args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("console token", flag.ContinueOnError)
	socket := flags.String("socket", defaultTokenSocket, "the socket file the console issues login tokens on")
	if help, err := parse(flags, args, stdout); help || err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("console token takes no arguments")
	}

	t, err := console.RequestToken(ctx, *socket)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(stdout, t)
	return err
}

// For -h, parse prints the flags to stdout and reports true.
func parse(flags *flag.FlagSet, args []string, stdout io.Writer) (help bool, err error) {
	flags.SetOutput(io.Discard)
	err = flags.Parse(args)
	if errors.Is(err, flag.ErrHelp) {
		flags.SetOutput(stdout)
		flags.Usage()
		return true, nil
	}
	return false, err
}
