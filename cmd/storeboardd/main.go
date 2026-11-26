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

	declared, err := config.Load(config.ResolvePath(*configPath))
	if err != nil {
		logger.Error("invalid config", "error", err)
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

	// Each pass is the declared config plus the stores programs requested
	// and the offered stores this machine subscribes to; a new request or a
	// change in those ends the pass and the next one carries it.
	for {
		req, err := config.LoadRequested(declared.DataDir)
		if err != nil {
			logger.Error("requested stores", "error", err)
			return 1
		}
		cfg, merged := declared.WithRequested(req)
		if len(merged) > 0 {
			logger.Info("carrying requested stores", "stores", merged)
		}
		offers, err := config.LoadOffers(declared.DataDir)
		if err != nil {
			logger.Error("offered stores", "error", err)
			return 1
		}
		subs, err := config.LoadSubscriptions(declared.DataDir)
		if err != nil {
			logger.Error("subscriptions", "error", err)
			return 1
		}
		if learned, names := cfg.WithLearned(offers, subs); len(names) > 0 {
			if err := learned.Validate(); err != nil {
				logger.Warn("offered stores left out: they would make the config invalid", "error", err)
			} else {
				logger.Info("carrying offered stores", "stores", names)
				cfg = learned
			}
		}
		srv, err := server.New(cfg, logger)
		if err != nil {
			logger.Error("startup failed", "error", err)
			return 1
		}
		pass, endPass := context.WithCancel(ctx)
		go func() {
			select {
			case <-srv.Restart():
				logger.Info("the stores to carry changed; starting again to carry them")
			case <-pass.Done():
			}
			endPass()
		}()
		code := 0
		if err := srv.Run(pass); err != nil {
			logger.Error("server failed", "error", err)
			code = 1
		}
		if err := srv.Close(); err != nil {
			logger.Error("close failed", "error", err)
			code = 1
		}
		restarting := ctx.Err() == nil && code == 0
		endPass()
		if !restarting {
			return code
		}
	}
}
