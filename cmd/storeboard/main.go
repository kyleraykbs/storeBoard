// Command storeboard is the storeboard CLI: validate configuration and query
// the local daemon. Every command takes --json for machine-readable output.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"codeberg.org/kyleraykbs/storeboard/internal/cli"
)

func main() {
	configPath := flag.String("config", "", "path to config file (default: $STOREBOARD_CONFIG, else $XDG_CONFIG_HOME/storeboard/config.json)")
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: storeboard [--config FILE] <command> [arguments]")
		fmt.Fprintln(os.Stderr)
		flag.PrintDefaults()
		fmt.Fprintln(os.Stderr, "\nrun 'storeboard help' for commands")
	}
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := cli.Run(ctx, flag.Args(), *configPath, os.Stdout, os.Stderr); err != nil {
		switch {
		case errors.Is(err, context.Canceled):
			// Interrupted is not success: a script running `storeboard send`
			// must not read a Ctrl-C or a SIGTERM as "the file is on the mesh".
			// 130 is the shell's own convention for death by SIGINT.
			fmt.Fprintln(os.Stderr, "storeboard: interrupted")
			os.Exit(130)
		case errors.Is(err, cli.ErrHandled):
			// The command already wrote its report; the exit code is the news.
			os.Exit(1)
		default:
			fmt.Fprintln(os.Stderr, "storeboard:", err)
			os.Exit(1)
		}
	}
}
