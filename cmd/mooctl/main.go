// Command mooctl operates a Moostack cluster.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/moostackhq/moostack/internal/cli"
)

func main() {
	env, err := cli.OSEnv()
	if err == nil {
		err = cli.New(env).Run(context.Background(), os.Args)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "mooctl: %v\n", err)
		os.Exit(1)
	}
}
