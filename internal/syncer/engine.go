// Package syncer is storeboard's sync engine: it keeps each store's canonical
// directory in step across peers and materialises the tree as symlinks. A
// store's bytes always live at <root>/<store>; a child store appears inside
// its parent as a symlink, and an external symlink points at the canonical
// dir from anywhere else. The scanner never crosses a symlink, which is what
// makes nesting free.
package syncer

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"codeberg.org/kyleraykbs/storeboard/internal/config"
	"codeberg.org/kyleraykbs/storeboard/internal/store"
	"codeberg.org/kyleraykbs/storeboard/pkg/api"
)

// scanInterval is how often each store is rescanned: the default is tuned
// for a home mesh (a 15 s poll, cheap because the scanner only hashes files
// whose size or mtime moved). STOREBOARD_SCAN_INTERVAL overrides it, which
// is how the smoke test drives the engine fast; tests set it directly.
var scanInterval = 15 * time.Second

// linkInterval is how often canonical dirs and declared symlinks are
// reconciled after startup. A deleted link or canonical dir comes back on the
// next pass; a non-empty directory remains protected until it becomes empty.
var linkInterval = 5 * time.Second

// indexInterval retries full index exchange after connect. A missed initial
// frame must not leave a peer permanently stale; this is deliberately slower
// than link reconciliation because an index can be large.
var indexInterval = 30 * time.Second

func init() {
	if v := os.Getenv("STOREBOARD_SCAN_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			scanInterval = d
		}
	}
	if v := os.Getenv("STOREBOARD_LINK_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			linkInterval = d
		}
	}
	if v := os.Getenv("STOREBOARD_INDEX_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			indexInterval = d
		}
	}
}

// Store states, mirrored in api.StoreSync docs.
const (
	stateReplica  = "replica"
	stateAttached = "attached"
	stateDetached = "detached"
)

// StoreState is one top-level store's engine-side state.
type StoreState struct {
	name      string
	group     string
	streamed  bool
	root      string // canonical directory: <root>/<id>
	symlinks  []string
	links     map[string]string // target store id -> relative path
	linkState []api.StoreSymlink
	state     string

	db        *FileDB
	hist      *History
	conflicts int
	lastScan  time.Time

	perm   perms
	scanMu sync.Mutex

	// metaMu guards published, the status document's copy of this store's
	// state. Snapshot refreshes it whenever the scan lock happens to be
	// free and otherwise serves the last values.
	metaMu    sync.Mutex
	published api.StoreSync
	local     int
	scanning  bool
}

// publish returns the store's status, refreshing the fields that need the
// scan lock when it is free and serving the last known values otherwise.
// Entries, conflicts and the last scan time are always live: they never
// needed that lock.
func (fs *StoreState) publish() api.StoreSync {
	fs.metaMu.Lock()
	links := append([]api.StoreSymlink(nil), fs.linkState...)
	if links == nil {
		links = []api.StoreSymlink{}
	}
	out := fs.published
	out.Symlinks = links
	fs.metaMu.Unlock()
	out.Store = fs.name
	out.Entries = fs.db.LiveCount()
	out.Conflicts = fs.conflicts
	out.LastScan = fs.lastScan
	out.Local = fs.local
	out.Scanning = fs.scanning
	if pending := out.Entries - fs.local; pending > 0 {
		out.Pending = pending
	}
	return out
}

// refreshLocked copies the fields that need the scan lock into published.
// Callers hold scanMu.
func (fs *StoreState) refreshLocked() {
	fs.metaMu.Lock()
	fs.published = api.StoreSync{
		Store:       fs.name,
		Root:        fs.root,
		State:       fs.state,
		Permissions: fs.perm.apiPerms(),
	}
	fs.metaMu.Unlock()
}

// addConflict and setLastScan record activity the status documents; both
// under metaMu so publish sees them without taking the scan lock.
func (fs *StoreState) addConflict() {
	fs.metaMu.Lock()
	fs.conflicts++
	fs.metaMu.Unlock()
}

func (fs *StoreState) setLastScan(t time.Time) {
	fs.metaMu.Lock()
	fs.lastScan = t
	fs.metaMu.Unlock()
}

// setLocal records how many of the store's entries exist on this machine
// (the rest are pending — the work a remote store is grinding through), and
// setScanning whether a walk is running right now. Both live under metaMu so
// publish sees them without the scan lock.
func (fs *StoreState) setLocal(local int) {
	fs.metaMu.Lock()
	fs.local = local
	fs.metaMu.Unlock()
}

func (fs *StoreState) setScanning(scanning bool) {
	fs.metaMu.Lock()
	fs.scanning = scanning
	fs.metaMu.Unlock()
}

// Engine ties stores, journals and the peer wire together.
type Engine struct {
	cfg     *config.Config
	logger  *slog.Logger
	dataDir string
	me      string
	store   *store.Store

	mu             sync.Mutex
	stores         []*StoreState
	byName         map[string]*StoreState
	sends          map[string]func([]byte) error
	wants          map[string]*pending
	incoming       map[string]*incomingBlob
	closed         chan struct{}
	closeOnce      sync.Once
	frameStartOnce sync.Once
	frameWG        sync.WaitGroup
	indexFrames    chan peerFrame
	wantFrames     chan peerFrame
	blobFrames     chan peerFrame
}

type peerFrame struct {
	peer string
	fr   frame
}

const syncFrameQueue = 256

func (e *Engine) startFrameWorkers() {
	e.frameStartOnce.Do(func() {
		e.indexFrames = make(chan peerFrame, syncFrameQueue)
		e.wantFrames = make(chan peerFrame, syncFrameQueue)
		e.blobFrames = make(chan peerFrame, syncFrameQueue)
		e.frameWG.Add(3)
		go func() {
			defer e.frameWG.Done()
			for {
				select {
				case <-e.closed:
					return
				case pf := <-e.indexFrames:
					e.handleIndex(pf.peer, &pf.fr)
				}
			}
		}()
		go func() {
			defer e.frameWG.Done()
			for {
				select {
				case <-e.closed:
					return
				case pf := <-e.wantFrames:
					e.handleWant(pf.peer, &pf.fr)
				}
			}
		}()
		go func() {
			defer e.frameWG.Done()
			for {
				select {
				case <-e.closed:
					return
				case pf := <-e.blobFrames:
					switch pf.fr.Type {
					case "blob":
						e.handleBlob(pf.peer, &pf.fr)
					case "blobEnd":
						e.handleBlobEnd(pf.peer, &pf.fr)
					}
				}
			}
		}()
	})
}

func (e *Engine) enqueueFrame(ch chan peerFrame, pf peerFrame) {
	select {
	case ch <- pf:
	case <-e.closed:
	}
}

// StopFrames makes frame callbacks return immediately and stops queued sync
// work without closing journals. Callers use it before stopping transport so
// a full work queue cannot hold network shutdown hostage.
func (e *Engine) StopFrames() {
	e.closeOnce.Do(func() { close(e.closed) })
}

// New prepares the engine. No store is touched until Run.
// New prepares the engine. No store is touched until Run.
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
		byName:   map[string]*StoreState{},
		sends:    map[string]func([]byte) error{},
		wants:    map[string]*pending{},
		incoming: map[string]*incomingBlob{},
		closed:   make(chan struct{}),
	}

	for _, v := range cfg.StoreViews() {
		db, err := OpenFileDB(filepath.Join(journalDir, v.ID+".ndjson"), cfg.Peer)
		if err != nil {
			return nil, err
		}
		streamed := v.Sync == config.SyncRemote
		var hist *History
		on, keep, herr := cfg.BackupFor(v.ID)
		if herr != nil {
			return nil, herr
		}
		if on {
			if !streamed {
				hist, err = OpenHistory(filepath.Join(cfg.DataDir, "history", v.ID+".ndjson"), keep)
				if err != nil {
					return nil, err
				}
			} else {
				// A remote store is a mirror, not a holder: its versions
				// belong to the machine that holds the content, so backup is
				// ignored on this one.
				logger.Debug("backup ignored: the store is remote on this machine", "store", v.ID)
			}
		}
		state := stateReplica
		if streamed {
			state = stateDetached
		}
		fs := &StoreState{
			name:     v.ID,
			group:    v.Group,
			streamed: streamed,
			root:     v.Dir,
			symlinks: v.Symlinks,
			links:    v.Links,
			state:    state,
			db:       db,
			hist:     hist,
			perm:     noPerms(),
			published: api.StoreSync{
				Store:       v.ID,
				Root:        v.Dir,
				State:       state,
				Permissions: noPerms().apiPerms(),
				Symlinks:    []api.StoreSymlink{},
			},
		}
		e.stores = append(e.stores, fs)
		e.byName[fs.name] = fs
	}
	e.startFrameWorkers()
	return e, nil
}

func (e *Engine) Run(ctx context.Context) {
	e.reconcileLinks()

	// Retention first (an in-memory pass), then unreferenced content: the
	// collector runs before anything is in flight, and a blob touched in the
	// last few minutes is kept regardless - a peer may be mid-transfer.
	e.pruneOnce(time.Now())
	e.Collect()
	go e.pruneLoop(ctx, pruneInterval)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		e.indexLoop(ctx)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		e.linkLoop(ctx)
	}()
	for _, fs := range e.stores {
		wg.Add(1)
		go func(fs *StoreState) {
			defer wg.Done()
			e.scanLoop(ctx, fs)
		}(fs)
	}
	<-ctx.Done()
	// Scanners and link reconciliation must be down before the caller tears
	// anything down: both can be writing paths or journals.
	wg.Wait()
}

func (e *Engine) indexLoop(ctx context.Context) {
	t := time.NewTicker(indexInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			e.resendIndexes()
		}
	}
}

func (e *Engine) resendIndexes() {
	e.mu.Lock()
	peers := make([]string, 0, len(e.sends))
	for peer := range e.sends {
		peers = append(peers, peer)
	}
	e.mu.Unlock()
	for _, peer := range peers {
		for _, fs := range e.stores {
			if e.storeSharedWith(fs, peer) {
				if err := e.sendIndexTo(peer, fs, fs.db.All()); err != nil {
					e.logger.Debug("resend index", "peer", peer, "store", fs.name, "error", err)
				}
			}
		}
	}
}

// reconcileLinks recreates missing canonical dirs and declared symlinks. It
// never moves content: non-empty directories and mountpoints remain busy,
// while stale symlink inodes are safely replaced.
func (e *Engine) reconcileLinks() {
	e.ensureRoot()
	for _, fs := range e.stores {
		e.prepareRoot(fs)
	}
	for _, fs := range e.stores {
		e.settleLinks(fs)
	}
}

func (e *Engine) linkLoop(ctx context.Context) {
	t := time.NewTicker(linkInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			e.reconcileLinks()
		}
	}
}

// ensureRoot creates the one container the store tree lives in. It is not a
// store: storeboard owns it, nothing syncs there directly.
func (e *Engine) ensureRoot() {
	if err := os.MkdirAll(e.cfg.Root, 0o755); err != nil {
		e.logger.Error("create root", "root", e.cfg.Root, "error", err)
	}
}

// prepareRoot creates a store's canonical directory and applies its
// permissions. Content is never moved into it: an existing directory is the
// user's, and a store that finds one simply adopts it.
func (e *Engine) prepareRoot(fs *StoreState) {
	fs.scanMu.Lock()
	defer fs.scanMu.Unlock()
	fs.perm = e.resolvePerms(fs)
	if err := os.MkdirAll(fs.root, 0o755); err != nil {
		e.logger.Error("create store root", "store", fs.name, "path", fs.root, "error", err)
	}
	e.applyDir(fs, fs.root)
	fs.refreshLocked()
	e.logger.Info("store ready", "store", fs.name, "root", fs.root, "state", fs.state)
}

func (e *Engine) scanLoop(ctx context.Context, fs *StoreState) {
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

func (e *Engine) scanOnce(fs *StoreState) {
	fs.scanMu.Lock()
	// Re-resolve every pass: an inherited permission follows the directory
	// it came from, so a chown or chmod of the root's parent is honoured on
	// the next scan rather than at the next restart. The canonical dir is
	// the daemon's own, so it is re-applied here and heals drift.
	fs.perm = e.resolvePerms(fs)
	e.applyDir(fs, fs.root)
	state := fs.state
	fs.refreshLocked()
	fs.scanMu.Unlock()

	// A replica owns its canonical dir, so absence there is a deletion. A
	// remote store is a mirror: without a holder answering, absence means
	// "not here yet", and reading it as a deletion emptied 448 files into a
	// holder's trash on 2026-10-09.
	tombstones := !fs.streamed || (state == stateAttached && e.participantConnected(fs))

	changed, err := e.scan(fs, tombstones)
	if err != nil {
		e.logger.Warn("scan", "store", fs.name, "error", err)
	}
	if len(changed) > 0 {
		e.broadcastIndex(fs, changed)
	}
}

// broadcastIndex fans a delta out to every connected peer that shares the
// store's group.
func (e *Engine) broadcastIndex(fs *StoreState, entries []*Entry) {
	sends := e.sendListFor(fs)
	if len(sends) == 0 {
		return
	}
	b, err := json.Marshal(frame{Type: "index", Store: fs.name, Entries: entries})
	if err != nil {
		return
	}
	b = append(b, '\n')
	for _, s := range sends {
		_ = s(b)
	}
}

func (e *Engine) sendListFor(fs *StoreState) []func([]byte) error {
	members := e.cfg.Groups[fs.group].Peers
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]func([]byte) error, 0, len(e.sends))
	for peer, s := range e.sends {
		if slices.Contains(members, peer) {
			out = append(out, s)
		}
	}
	return out
}

// --- transport hooks -------------------------------------------------------

// OnPeerConnected opens a sync session: the peer gets this machine's full
// index for every store the two share, and remote stores may now go live.
func (e *Engine) OnPeerConnected(peer string, send func([]byte) error) {
	e.mu.Lock()
	e.sends[peer] = send
	e.mu.Unlock()

	for _, fs := range e.stores {
		if !e.storeSharedWith(fs, peer) {
			continue
		}
		if err := e.sendIndexTo(peer, fs, fs.db.All()); err != nil {
			e.logger.Debug("send index", "peer", peer, "store", fs.name, "error", err)
		}
	}
	e.evaluatePeers()
}

// OnPeerDisconnected drops the session and re-evaluates remote stores. In-
// flight blobs are NOT dropped: during cross-dial churn a replacement link
// registers almost immediately and a half-received file may still complete
// over it. Stale scratch files are wiped at startup instead.
func (e *Engine) OnPeerDisconnected(peer string) {
	e.mu.Lock()
	delete(e.sends, peer)
	e.mu.Unlock()
	e.evaluatePeers()
}

// OnPeerFrame parses one wire frame and queues it by work class. The queues
// keep blob chunks ordered while allowing blob receive/apply to proceed while
// an index or a want handler is doing filesystem work.
func (e *Engine) OnPeerFrame(peer string, line []byte) {
	e.startFrameWorkers()
	var fr frame
	if err := json.Unmarshal(line, &fr); err != nil {
		return
	}
	pf := peerFrame{peer: peer, fr: fr}
	switch fr.Type {
	case "index":
		e.enqueueFrame(e.indexFrames, pf)
	case "want":
		e.enqueueFrame(e.wantFrames, pf)
	case "blob", "blobEnd":
		e.enqueueFrame(e.blobFrames, pf)
	}
}

// evaluatePeers records whether each remote store has a holder reachable. It
// never changes a path: the canonical dir is the same directory whether or
// not a holder answers, so there is no root to flip.
func (e *Engine) evaluatePeers() {
	for _, fs := range e.stores {
		if !fs.streamed {
			continue
		}
		connected := e.participantConnected(fs)
		fs.scanMu.Lock()
		was := fs.state
		if connected {
			fs.state = stateAttached
		} else {
			fs.state = stateDetached
		}
		fs.refreshLocked()
		fs.scanMu.Unlock()
		if connected && was != stateAttached {
			e.logger.Info("store is live (holder reachable)", "store", fs.name)
			e.scanOnce(fs)
		} else if !connected && was == stateAttached {
			e.logger.Info("store is not live (no holder reachable)", "store", fs.name)
		}
	}
}

// storeSharedWith reports whether peer is in the store's group.
func (e *Engine) storeSharedWith(fs *StoreState, peer string) bool {
	return slices.Contains(e.cfg.Groups[fs.group].Peers, peer)
}

func (e *Engine) participantConnected(fs *StoreState) bool {
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

// Snapshot is the status document's sync section. It never waits on the scan
// or apply locks.
func (e *Engine) Snapshot() []api.StoreSync {
	receiving := map[string]int{}
	e.mu.Lock()
	for _, inc := range e.incoming {
		receiving[inc.store]++
	}
	e.mu.Unlock()

	out := make([]api.StoreSync, 0, len(e.stores))
	for _, fs := range e.stores {
		s := fs.publish()
		s.Receiving = receiving[fs.name]
		out = append(out, s)
	}
	return out
}

// Close cancels frame application before closing durable state. Transport
// shutdown calls this first, so a large queued index cannot keep a systemd
// stop job waiting on journals or files.
func (e *Engine) Close() {
	e.StopFrames()
	e.frameWG.Wait()
	for _, fs := range e.stores {
		fs.db.Close()
		if fs.hist != nil {
			fs.hist.Close()
		}
	}
}
