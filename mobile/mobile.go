// Package mobile is storeboard's engine as an app sees it: gomobile binds
// exactly this package, and everything crossing the boundary is a string or
// bytes holding JSON — the same shapes the daemon's /v1 document, the config
// file and the CLI use, so the phone, the desktop and the wire speak one
// contract.
//
// The app is a peer like any other: it owns its data directory, its journals,
// its content store and its connections. There is no local socket here (an
// app has no CLI to serve), so the engine is started directly and the UI asks
// this object for the same documents the daemon would serve.
package mobile

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"codeberg.org/kyleraykbs/storeboard/internal/config"
	"codeberg.org/kyleraykbs/storeboard/internal/syncer"
	"codeberg.org/kyleraykbs/storeboard/internal/transport"
	"codeberg.org/kyleraykbs/storeboard/internal/version"
	"codeberg.org/kyleraykbs/storeboard/pkg/api"
)

// Daemon is one running engine: config, journals, store, peers.
type Daemon struct {
	cfg       *config.Config
	eng       *syncer.Engine
	mgr       *transport.Manager
	startedAt time.Time
	cancel    context.CancelFunc
	logw      io.Closer
}

// Start launches the engine with a configuration (JSON text, exactly the
// file the CLI would read: root, peers, groups, stores, backup) and begins
// syncing. logPath, when non-empty, receives the daemon's log lines — the
// app can show them or attach them to a report.
func Start(configJSON, logPath string) (*Daemon, error) {
	cfg, err := config.Parse([]byte(configJSON))
	if err != nil {
		return nil, err
	}

	var out io.Writer = io.Discard
	var closer io.Closer
	if logPath != "" {
		if err := os.MkdirAll(filepath.Dir(logPath), 0o700); err != nil {
			return nil, err
		}
		f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			return nil, err
		}
		out, closer = f, f
	}
	logger := slog.New(slog.NewTextHandler(out, nil))

	eng, err := syncer.New(cfg, logger)
	if err != nil {
		if closer != nil {
			closer.Close()
		}
		return nil, err
	}
	mgr := transport.New(cfg, logger)
	mgr.SetHandler(eng)

	ctx, cancel := context.WithCancel(context.Background())
	d := &Daemon{
		cfg:       cfg,
		eng:       eng,
		mgr:       mgr,
		startedAt: time.Now(),
		cancel:    cancel,
		logw:      closer,
	}
	go eng.Run(ctx)
	go mgr.Run(ctx)
	logger.Info("storeboard started", "peer", cfg.Peer, "dataDir", cfg.DataDir, "stores", len(cfg.Stores()))
	return d, nil
}

// Stop ends syncing and releases the journals. The daemon is not usable after.
func (d *Daemon) Stop() error {
	if d.cancel == nil {
		return nil
	}
	d.cancel()
	d.cancel = nil
	d.mgr.Close()
	d.eng.Close()
	if d.logw != nil {
		d.logw.Close()
	}
	return nil
}

// Status is the daemon's document as JSON: the same api.Status the desktop
// daemon serves at /v1/status (there is no socket here, so the app reads it
// from this call instead).
func (d *Daemon) Status() (string, error) {
	st := api.Status{
		APIVersion:    api.Version,
		Version:       version.Version,
		StartedAt:     d.startedAt.UTC(),
		UptimeSeconds: int64(time.Since(d.startedAt).Seconds()),
		Socket:        d.cfg.Socket,
		DataDir:       d.cfg.DataDir,
		Root:          d.cfg.Root,
		Peer:          d.cfg.Peer,
		Peers:         d.cfg.PeerList(),
		Groups:        d.cfg.GroupList(),
		Stores:        d.cfg.Stores(),
		Connections:   d.mgr.Snapshot(),
		Sync:          d.eng.Snapshot(),
	}
	b, err := json.Marshal(st)
	return string(b), err
}

// Versions lists a file's recorded versions, newest first (JSON api.Versions).
// An empty path lists every file the store's history knows.
func (d *Daemon) Versions(store, path string) (string, error) {
	vs, err := d.eng.Versions(store, path)
	if err != nil {
		return "", err
	}
	b, err := json.Marshal(vs)
	return string(b), err
}

// Restore writes a recorded version back: at is RFC3339 or empty for the
// newest, blob picks an exact version or empty, to writes a copy there
// instead of restoring into the store (JSON api.RestoreResult).
func (d *Daemon) Restore(store, path, blob, at, to string) (string, error) {
	var when time.Time
	if at != "" {
		parsed, err := time.Parse(time.RFC3339, at)
		if err != nil {
			return "", fmt.Errorf("at: %w", err)
		}
		when = parsed
	}
	res, err := d.eng.Restore(store, os.Getuid(), path, blob, when, to)
	if err != nil {
		return "", err
	}
	b, err := json.Marshal(res)
	return string(b), err
}

// Send copies a file into one of this machine's stores — the share sheet's
// path onto the mesh. dst is relative inside the store; empty uses the
// source's name (JSON api.SendResult). The copy is an ordinary local change:
// it is indexed at once, recorded in the history and synced onwards.
//
// The acting verbs authorize per caller, and in an app the caller is the app:
// its own uid, which is the owner of the stores it created.
func (d *Daemon) Send(store, src, dst string) (string, error) {
	res, err := d.eng.Send(store, os.Getuid(), src, dst)
	if err != nil {
		return "", err
	}
	b, err := json.Marshal(res)
	return string(b), err
}
