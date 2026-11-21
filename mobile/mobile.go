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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"codeberg.org/kyleraykbs/storeboard/internal/config"
	"codeberg.org/kyleraykbs/storeboard/internal/statelock"
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
	lock      *statelock.Lock
	logw      io.Closer

	mu        sync.Mutex
	cancel    context.CancelFunc
	engineRun chan struct{}

	// ctx is the engine's run context: a browse read that has to go to a
	// holder runs under it, so Stop cuts a stalled fetch short instead of
	// waiting out its timeout.
	ctx context.Context
	// calls counts browse calls in flight. Stop waits for them before it
	// closes the journals they read: a document provider calls in from
	// binder threads the service's lifecycle knows nothing about.
	calls sync.WaitGroup
}

// errStopped is what a call into a stopped engine gets, instead of a read of
// closed journals.
var errStopped = errors.New("storeboard: the engine is not running")

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

	// The same single-owner rule as the daemon: an app whose old engine is
	// still draining (a service restarted under it, a second process) must
	// not open the same journals beside it.
	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
		if closer != nil {
			closer.Close()
		}
		return nil, err
	}
	lock, err := statelock.Acquire(cfg.DataDir)
	if err != nil {
		if closer != nil {
			closer.Close()
		}
		return nil, err
	}

	eng, err := syncer.New(cfg, logger)
	if err != nil {
		lock.Release()
		if closer != nil {
			closer.Close()
		}
		return nil, err
	}
	mgr := transport.New(cfg, logger)
	mgr.SetHandler(eng)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	d := &Daemon{
		cfg:       cfg,
		eng:       eng,
		mgr:       mgr,
		startedAt: time.Now(),
		lock:      lock,
		cancel:    cancel,
		engineRun: done,
		logw:      closer,
		ctx:       ctx,
	}
	go func() {
		defer close(done)
		eng.Run(ctx)
	}()
	// Synchronously, as the daemon does: Run only binds the listeners and
	// spawns the dialers, and it is what records the cancel the transport's
	// Close uses. Started in a goroutine, a Stop that came first found
	// nothing to cancel and the listeners bound afterwards were never closed.
	mgr.Run(ctx)
	logger.Info("storeboard started", "peer", cfg.Peer, "dataDir", cfg.DataDir, "stores", len(cfg.Stores()))
	return d, nil
}

// Stop ends syncing and releases the journals. The daemon is not usable after.
// The order is the daemon's (server.Close): frames stop before the transport
// closes, so a queued index cannot hold the teardown; engine.Run returns —
// its scanners write journals until it does — before the engine closes them;
// and the data directory's lock goes last.
func (d *Daemon) Stop() error {
	d.mu.Lock()
	cancel, done := d.cancel, d.engineRun
	d.cancel, d.engineRun = nil, nil
	d.mu.Unlock()
	if cancel == nil {
		return nil
	}
	cancel()
	d.eng.StopFrames()
	d.mgr.Close()
	<-done
	d.calls.Wait()
	d.eng.Close()
	d.lock.Release()
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
// path onto the mesh. src is an absolute path to a file the app owns (the
// share sheet's copy in the app's cache); it is copied, not moved, so the
// caller deletes it when it no longer needs it. dst is relative inside the
// store; empty uses the source's name (JSON api.SendResult). The copy is an
// ordinary local change: it is indexed at once, recorded in the history and
// synced onwards.
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

// enter admits one browse call, or refuses it once Stop has begun; the
// caller runs leave when it is done. A nil Daemon (a binding that handed out
// no engine) is refused the same way rather than crashing the app.
func (d *Daemon) enter() error {
	if d == nil {
		return errStopped
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.cancel == nil {
		return errStopped
	}
	d.calls.Add(1)
	return nil
}

func (d *Daemon) leave() { d.calls.Done() }

// Browse lists the immediate children of dir in a store ("" is its root) as
// a JSON array of api.BrowseEntry — what the phone's document provider shows
// as a folder. It works for every kind of store; a streamed store's entries
// come from its journal, with no bytes on the phone.
func (d *Daemon) Browse(store, dir string) (string, error) {
	if err := d.enter(); err != nil {
		return "", err
	}
	defer d.leave()
	ents, err := d.eng.Browse(store, dir)
	if err != nil {
		return "", err
	}
	if ents == nil {
		ents = []api.BrowseEntry{}
	}
	b, err := json.Marshal(ents)
	return string(b), err
}

// Stat describes one path in a store ("" is its root) as a JSON
// api.BrowseEntry.
func (d *Daemon) Stat(store, path string) (string, error) {
	if err := d.enter(); err != nil {
		return "", err
	}
	defer d.leave()
	ent, err := d.eng.Stat(store, path)
	if err != nil {
		return "", err
	}
	b, err := json.Marshal(ent)
	return string(b), err
}

// LocalPath is a file's absolute path on the phone when its bytes are here
// and current, so the app can hand out a plain file descriptor (and let the
// scanner see a write) rather than proxying reads. It errors when the bytes
// are elsewhere — always, for a streamed store — and the caller then reads
// through ReadAt.
func (d *Daemon) LocalPath(store, path string) (string, error) {
	if err := d.enter(); err != nil {
		return "", err
	}
	defer d.leave()
	return d.eng.LocalPath(store, path)
}

// ReadAt reads up to length bytes of a file at offset, from disk when the
// bytes are here and from a holder otherwise — a range at a time, so opening
// a large video does not download it. At or past the end it returns no
// bytes. It blocks while a holder answers, so the app calls it off the main
// and binder threads; Stop cancels a fetch that is still waiting.
func (d *Daemon) ReadAt(store, path string, offset, length int64) ([]byte, error) {
	if err := d.enter(); err != nil {
		return nil, err
	}
	defer d.leave()
	return d.eng.ReadAt(d.ctx, store, path, offset, length)
}

// Doctor is the health report as JSON (api.Doctor): the same checks
// `storeboard doctor` runs against the desktop daemon.
func (d *Daemon) Doctor() (string, error) {
	if err := d.enter(); err != nil {
		return "", err
	}
	defer d.leave()
	b, err := json.Marshal(d.eng.Doctor(d.mgr.Snapshot()))
	return string(b), err
}
