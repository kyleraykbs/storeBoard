// Package syncer is storeboard's sync engine: it keeps replica folders in
// step across peers and materialises streamed folders as mounts that appear
// live while a holder answers and become a writable empty dir while none
// does, merging whatever lands there back in on reconnect.
package syncer

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"syscall"
	"time"

	"codeberg.org/kyleraykbs/storeboard/internal/config"
	"codeberg.org/kyleraykbs/storeboard/internal/store"
	"codeberg.org/kyleraykbs/storeboard/pkg/api"
)

// scanInterval is how often each folder is rescanned: the default is tuned
// for a home mesh (a 15 s poll, cheap because the scanner only hashes files
// whose size or mtime moved). STOREBOARD_SCAN_INTERVAL overrides it, which
// is how the smoke test drives the engine fast; tests set it directly.
// (Rate limiting and ignore rules are a later pass.)
var scanInterval = 15 * time.Second

func init() {
	if v := os.Getenv("STOREBOARD_SCAN_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			scanInterval = d
		}
	}
}

// FolderState is one folder's engine-side state.
type FolderState struct {
	name      string
	group     string
	mount     string
	streamed  bool
	root      string // the root the scanner follows: mount, view, or detached dir
	state     string
	db        *FileDB
	conflicts int
	lastScan  time.Time

	ownerUID, ownerGID int
	scanMu             sync.Mutex
}

// Engine ties folders, journals and the peer wire together.
type Engine struct {
	cfg     *config.Config
	logger  *slog.Logger
	dataDir string
	me      string
	store   *store.Store

	mu       sync.Mutex
	folders  []*FolderState
	byName   map[string]*FolderState
	sends    map[string]func([]byte) error
	wants    map[string]*pending
	incoming map[string]*incomingBlob
}

// New prepares the engine. No folder is touched until Run.
func New(cfg *config.Config, logger *slog.Logger) (*Engine, error) {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	st, err := store.Open(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	journalDir := filepath.Join(cfg.DataDir, "journal")
	if err := os.MkdirAll(journalDir, 0o700); err != nil {
		return nil, err
	}
	// Scratch files from a previous run are junk: partial blobs, staged
	// writes. Journals and the store are the durable state.
	if entries, err := os.ReadDir(filepath.Join(cfg.DataDir, "tmp")); err == nil {
		for _, ent := range entries {
			_ = os.RemoveAll(filepath.Join(cfg.DataDir, "tmp", ent.Name()))
		}
	}

	e := &Engine{
		cfg:      cfg,
		logger:   logger,
		dataDir:  cfg.DataDir,
		me:       cfg.Peer,
		store:    st,
		byName:   map[string]*FolderState{},
		sends:    map[string]func([]byte) error{},
		wants:    map[string]*pending{},
		incoming: map[string]*incomingBlob{},
	}

	for _, f := range cfg.Folders() {
		db, err := OpenFileDB(filepath.Join(journalDir, f.Name+".ndjson"), cfg.Peer)
		if err != nil {
			return nil, err
		}
		fs := &FolderState{
			name:     f.Name,
			group:    f.Group,
			mount:    f.Mount,
			streamed: !f.KeepOffline,
			db:       db,
			ownerUID: -1,
			ownerGID: -1,
		}
		e.folders = append(e.folders, fs)
		e.byName[fs.name] = fs
	}
	return e, nil
}

// Run prepares the mounts and drives scanning until ctx ends.
func (e *Engine) Run(ctx context.Context) {
	for _, fs := range e.folders {
		e.prepare(fs)
		go e.scanLoop(ctx, fs)
	}
	<-ctx.Done()
}

func (e *Engine) prepare(fs *FolderState) {
	fs.scanMu.Lock()
	defer fs.scanMu.Unlock()
	if !fs.streamed {
		if err := os.MkdirAll(fs.mount, 0o755); err != nil {
			e.logger.Error("create folder root", "folder", fs.name, "error", err)
		}
	}
	e.ensureMounts(fs)
	fs.ownerUID, fs.ownerGID = e.folderOwner(fs)
}

// folderOwner is the uid/gid created entries are given: the owner of the
// user-facing root (the mount's parent for streamed folders, the folder
// itself for replicas).
func (e *Engine) folderOwner(fs *FolderState) (int, int) {
	probe := fs.mount
	if fs.streamed {
		probe = filepath.Dir(fs.mount)
	}
	info, err := os.Stat(probe)
	if err != nil {
		return -1, -1
	}
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		return int(st.Uid), int(st.Gid)
	}
	return -1, -1
}

func (e *Engine) scanLoop(ctx context.Context, fs *FolderState) {
	e.scanOnce(fs)
	t := time.NewTicker(scanInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			e.scanOnce(fs)
		}
	}
}

func (e *Engine) scanOnce(fs *FolderState) {
	changed, err := e.scan(fs)
	if err != nil {
		e.logger.Warn("scan", "folder", fs.name, "error", err)
	}
	if len(changed) > 0 {
		e.broadcastIndex(fs, changed)
	}
}

// broadcastIndex fans a delta out to every connected peer.
func (e *Engine) broadcastIndex(fs *FolderState, entries []*Entry) {
	sends := e.sendList()
	if len(sends) == 0 {
		return
	}
	b, err := json.Marshal(frame{Type: "index", Folder: fs.name, Entries: entries})
	if err != nil {
		return
	}
	b = append(b, '\n')
	for _, s := range sends {
		_ = s(b)
	}
}

func (e *Engine) sendList() []func([]byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]func([]byte) error, 0, len(e.sends))
	for _, s := range e.sends {
		out = append(out, s)
	}
	return out
}

// --- transport hooks -------------------------------------------------------

// OnPeerConnected opens a sync session: the peer gets this machine's full
// index for every folder, and streamed folders may now attach.
func (e *Engine) OnPeerConnected(peer string, send func([]byte) error) {
	e.mu.Lock()
	e.sends[peer] = send
	e.mu.Unlock()

	for _, fs := range e.folders {
		if err := e.sendIndexTo(peer, fs, fs.db.All()); err != nil {
			e.logger.Debug("send index", "peer", peer, "folder", fs.name, "error", err)
		}
	}
	e.evaluateMounts()
}

// OnPeerDisconnected drops the session and possibly detaches mounts. In-
// flight blobs are NOT dropped: during cross-dial churn a replacement link
// registers almost immediately and a half-received file may still complete
// over it. Stale scratch files are wiped at startup instead.
func (e *Engine) OnPeerDisconnected(peer string) {
	e.mu.Lock()
	delete(e.sends, peer)
	e.mu.Unlock()
	e.evaluateMounts()
}

// OnPeerFrame routes one wire frame.
func (e *Engine) OnPeerFrame(peer string, line []byte) {
	var fr frame
	if err := json.Unmarshal(line, &fr); err != nil {
		return
	}
	switch fr.Type {
	case "index":
		e.handleIndex(peer, &fr)
	case "want":
		e.handleWant(peer, &fr)
	case "blob":
		e.handleBlob(peer, &fr)
	case "blobEnd":
		e.handleBlobEnd(peer, &fr)
	}
}

// evaluateMounts attaches streamed folders whose group has a connected peer
// and detaches those without one.
func (e *Engine) evaluateMounts() {
	for _, fs := range e.folders {
		if !fs.streamed {
			continue
		}
		// A connection can beat the startup prepare (transport and engine
		// run in parallel); an unprepared folder has no state to switch on.
		fs.scanMu.Lock()
		unprepared := fs.state == ""
		fs.scanMu.Unlock()
		if unprepared {
			e.prepare(fs)
		}

		connected := e.participantConnected(fs)
		fs.scanMu.Lock()
		state := fs.state
		fs.scanMu.Unlock()
		switch {
		case connected && (state == stateDetached || state == ""):
			e.attach(fs)
			e.scanOnce(fs)
		case !connected && state == stateAttached:
			e.detach(fs)
			e.scanOnce(fs)
		}
	}
}

func (e *Engine) participantConnected(fs *FolderState) bool {
	members := e.cfg.Groups[fs.group].Peers
	e.mu.Lock()
	defer e.mu.Unlock()
	for p := range e.sends {
		if slices.Contains(members, p) {
			return true
		}
	}
	return false
}

// Snapshot is the status document's sync section.
func (e *Engine) Snapshot() []api.FolderSync {
	out := make([]api.FolderSync, 0, len(e.folders))
	for _, fs := range e.folders {
		out = append(out, api.FolderSync{
			Folder:    fs.name,
			Root:      fs.root,
			State:     fs.state,
			Entries:   fs.db.Count(),
			Conflicts: fs.conflicts,
			LastScan:  fs.lastScan,
		})
	}
	return out
}

// Close releases the journals.
func (e *Engine) Close() {
	for _, fs := range e.folders {
		fs.db.Close()
	}
}
