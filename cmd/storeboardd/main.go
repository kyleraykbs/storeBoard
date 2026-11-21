// Command storeboardd runs the storeboard daemon.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"codeberg.org/kyleraykbs/storeboard/internal/config"
	"codeberg.org/kyleraykbs/storeboard/internal/server"
)

func main() {
	// os.Exit skips deferred calls, so every exit path goes through run and
	// its Close: exiting from the middle left the FUSE mount of a streamed
	// store and the socket file behind.
	os.Exit(run())
}

func run() int {
	configPath := flag.String("config", "", "path to config file (default: $STOREBOARD_CONFIG, else $XDG_CONFIG_HOME/storeboard/config.json)")
	flag.Parse()

	// STOREBOARD_LOG_LEVEL=debug turns on the connection-level detail
	// (dial attempts, rejected handshakes, frame errors). The default stays
	// INFO: a home mesh does not need a line per dial.
	level := slog.LevelInfo
	if v := os.Getenv("STOREBOARD_LOG_LEVEL"); v != "" {
		if err := level.UnmarshalText([]byte(v)); err != nil {
			logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
			logger.Warn("ignoring STOREBOARD_LOG_LEVEL", "value", v, "error", err)
		}
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	cfg, err := config.Load(config.ResolvePath(*configPath))
	if err != nil {
		logger.Error("invalid config", "error", err)
		return 1
	}

	srv, err := server.New(cfg, logger)
	if err != nil {
		logger.Error("startup failed", "error", err)
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// The first signal starts a graceful shutdown; after it the default
	// disposition is back, so a second Ctrl-C (or SIGTERM from a stop job
	// that ran out of patience) kills a shutdown that hangs.
	go func() {
		<-ctx.Done()
		stop()
	}()

	code := 0
	if err := srv.Run(ctx); err != nil {
		logger.Error("server failed", "error", err)
		code = 1
	}
	if err := srv.Close(); err != nil {
		logger.Error("close failed", "error", err)
		code = 1
	}
	return code
}
