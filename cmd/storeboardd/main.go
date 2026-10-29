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
		os.Exit(1)
	}

	srv, err := server.New(cfg, logger)
	if err != nil {
		logger.Error("startup failed", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := srv.Close(); err != nil {
			logger.Error("close failed", "error", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := srv.Run(ctx); err != nil {
		logger.Error("server failed", "error", err)
		os.Exit(1)
	}
}
