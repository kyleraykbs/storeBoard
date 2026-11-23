// Package syncer is storeboard's sync engine: it keeps each store's canonical
// directory in step across peers and materialises the tree as symlinks. A
// store's bytes always live at <root>/<store>; a child store appears inside
// its parent as a symlink, and an external symlink points at the canonical
// dir from anywhere else. The scanner never crosses a symlink, which is what
// makes nesting free.
package syncer

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"codeberg.org/kyleraykbs/storeboard/internal/config"
	"codeberg.org/kyleraykbs/storeboard/internal/store"
	"codeberg.org/kyleraykbs/storeboard/internal/streamfs"
	"codeberg.org/kyleraykbs/storeboard/internal/transport"
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
	stream    bool   // the canonical path is storeboard's FUSE mount
	preferred string // declared holder preference, empty means none
	root      string // canonical directory: <root>/<id>
	symlinks  []string
	links     map[string]string // target store id -> relative path
	linkState []api.StoreSymlink
	state     string

	// tree is a streamed store's journal as a directory index, kept in step
	// with every journal write; mount serves it, non-nil while mounted.
	tree  *streamfs.Tree
	mount *streamfs.FS // guarded by scanMu
	// browseOnce builds tree on first use for a store that is not streamed:
	// only an app browsing it (see Browse) needs the directory index.
	browseOnce sync.Once
	// mountUp mirrors mount != nil for readers that cannot take scanMu.
	mountUp atomic.Bool

	// offline is the write policy for a streamed store; queue holds the
	// writes waiting for a holder, and queuedBytes bounds a capped queue.
	offline     *config.OfflineQueue
	queue       []queueOp
	queuedBytes int64
	// flushing and flushAgain coalesce flush requests (see flushQueue).
	flushing   atomic.Bool
	flushAgain atomic.Bool
	// lastFlush is when the queue was last handed to a holder (unix nanos).
	lastFlush atomic.Int64
	// rootEmpty records that the last scan found the root empty while this
	// machine held many of its files (see massDeleteFloor).
	rootEmpty atomic.Bool
	// migr is a local->stream move in progress, or why a mode change could
	// not be made (migrate.go). unstream marks a store configured local that
	// last ran streamed, converted on its first prepare.
	migr     *migration
	unstream bool
	// modeNote is why a store configured local still runs streamed.
	modeNote string
	// queueMu guards the queue. It is deliberately not scanMu: the mount's
	// handlers call into the queue, and a handler waiting on scanMu while the
	// engine waits on the mount is a deadlock.
	queueMu sync.Mutex

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
	prepared  bool // prepareRoot has run: the mount, if any, exists
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
	out.Conflicts = fs.conflicts
	out.LastScan = fs.lastScan
	out.Local = fs.local
	out.Scanning = fs.scanning
	fs.metaMu.Unlock()
	out.Store = fs.name
	out.Entries = fs.db.LiveCount()
	if pending := out.Entries - out.Local; pending > 0 {
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

	mu          sync.Mutex
	stores      []*StoreState
	byName      map[string]*StoreState
	sends       map[string]transport.Sender
	wants       map[string]map[string]*pending // wantKey -> slot -> want
	incoming    map[string]*incomingBlob
	reads       map[int64]chan readReply
	readSeq     int64
	holderRates map[string]holderRate
	// readGates bounds streamed reads in flight per holder (readgate.go).
	readGates map[string]*readGate
	// lastHeard is when each peer last sent a frame (*atomic.Int64 nanos).
	lastHeard sync.Map
	// readCancels are reads a reader gave up on, not to be served.
	cancelMu       sync.Mutex
	readCancels    map[cancelKey]time.Time
	verified       map[string]verifiedFile
	fullSent       map[string]fullIndexMark
	pendingMu      sync.Mutex
	pending        map[string]*pendingFile
	closed         chan struct{}
	closeOnce      sync.Once
	frameStartOnce sync.Once
	frameWG        sync.WaitGroup
	readWG         sync.WaitGroup
	indexFrames    map[string]chan peerFrame
	wantFrames     map[string]chan peerFrame

	// The loop intervals as they were when the engine was made: the package
	// variables are knobs for tests, and reading them from running loops
	// raced every test that turned one.
	scanEvery, linkEvery, indexEvery, pruneEvery time.Duration

	readFrames map[string]chan peerFrame
	blobFrames [blobShards]chan peerFrame

	// finishSem bounds completed transfers being verified and applied, and
	// finishWG lets Close wait for them.
	finishSem chan struct{}
	finishWG  sync.WaitGroup
	// blobLocks serialises finishing per blob (see lockBlob).
	blobLockMu sync.Mutex
	blobLocks  map[string]*blobLock

	// sendMu guards sendq, each peer's outgoing transfers; sendWG lets Close
	// wait for the running ones.
	sendMu sync.Mutex
	sendq  map[string]*peerSends
	sendWG sync.WaitGroup
}

func (e *Engine) registerPending(p *pendingFile) {
	p.mu.Lock()
	path := p.path
	p.mu.Unlock()
	e.pendingMu.Lock()
	e.pending[path] = p
	e.pendingMu.Unlock()
}

func (e *Engine) repathPending(from, to string) bool {
	e.pendingMu.Lock()
	p := e.pending[from]
	e.pendingMu.Unlock()
	if p == nil {
		return false
	}
	p.mu.Lock()
	p.path = to
	p.mu.Unlock()
	e.pendingMu.Lock()
	if e.pending[from] == p {
		delete(e.pending, from)
		e.pending[to] = p
		e.pendingMu.Unlock()
		return true
	}
	e.pendingMu.Unlock()
	return false
}

func (e *Engine) unregisterPending(p *pendingFile) {
	p.mu.Lock()
	path := p.path
	p.mu.Unlock()
	e.pendingMu.Lock()
	if e.pending[path] == p {
		delete(e.pending, path)
	}
	e.pendingMu.Unlock()
}

func (e *Engine) hasPending(path string) bool {
	e.pendingMu.Lock()
	_, ok := e.pending[path]
	e.pendingMu.Unlock()
	return ok
}

type peerFrame struct {
	peer string
	fr   frame
}

const syncFrameQueue = 256

func (e *Engine) startFrameWorkers() {
	e.frameStartOnce.Do(func() {
		e.indexFrames = make(map[string]chan peerFrame, len(e.stores))
		e.wantFrames = make(map[string]chan peerFrame, len(e.stores))
		e.readFrames = make(map[string]chan peerFrame, len(e.stores))
		for i := range e.blobFrames {
			e.blobFrames[i] = make(chan peerFrame, syncFrameQueue)
		}
		e.frameWG.Add(blobShards + 3*len(e.stores))
		// One worker per store for each class: a slow store delays only
		// itself, and serving a multi-gigabyte blob cannot hold another
		// store's index frames hostage.
		for _, fs := range e.stores {
			index := make(chan peerFrame, syncFrameQueue)
			e.indexFrames[fs.name] = index
			go func(ch chan peerFrame) {
				defer e.frameWG.Done()
				for {
					select {
					case <-e.closed:
						return
					case pf := <-ch:
						// Digests share the index worker: a pull reply
						// must be judged after the index frames before it.
						switch pf.fr.Type {
						case "digest":
							e.handleDigest(pf.peer, &pf.fr)
						case "pull":
							e.handlePull(pf.peer, &pf.fr)
						default:
							e.handleIndex(pf.peer, &pf.fr)
						}
					}
				}
			}(index)
			want := make(chan peerFrame, syncFrameQueue)
			e.wantFrames[fs.name] = want
			go func(ch chan peerFrame) {
				defer e.frameWG.Done()
				for {
					select {
					case <-e.closed:
						return
					case pf := <-ch:
						switch pf.fr.Type {
						case "rename":
							e.handleRename(pf.peer, &pf.fr)
						case "verbs":
							e.handleVerbs(pf.peer, &pf.fr)
						default:
							e.handleWant(pf.peer, &pf.fr)
						}
					}
				}
			}(want)

			reads := make(chan peerFrame, syncFrameQueue)
			e.readFrames[fs.name] = reads
			go func(ch chan peerFrame) {
				defer e.frameWG.Done()
				slots := make(chan struct{}, readWorkers)
				for {
					select {
					case <-e.closed:
						return
					case pf := <-ch:
						select {
						case slots <- struct{}{}:
						case <-e.closed:
							return
						}
						e.readWG.Add(1)
						go func() {
							defer e.readWG.Done()
							defer func() { <-slots }()
							e.handleRead(pf.peer, &pf.fr)
						}()
					}
				}
			}(reads)
		}
		for _, ch := range e.blobFrames {
			go func(ch chan peerFrame) {
				defer e.frameWG.Done()
				for {
					select {
					case <-e.closed:
						return
					case pf := <-ch:
						switch pf.fr.Type {
						case "blob":
							e.handleBlob(pf.peer, &pf.fr)
						case "blobEnd":
							e.handleBlobEnd(pf.peer, &pf.fr)
						}
					}
				}
			}(ch)
		}
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
		cfg:         cfg,
		logger:      logger,
		dataDir:     cfg.DataDir,
		me:          cfg.Peer,
		store:       st,
		byName:      map[string]*StoreState{},
		sends:       map[string]transport.Sender{},
		wants:       map[string]map[string]*pending{},
		finishSem:   make(chan struct{}, finishSlots),
		blobLocks:   map[string]*blobLock{},
		sendq:       map[string]*peerSends{},
		incoming:    map[string]*incomingBlob{},
		reads:       map[int64]chan readReply{},
		holderRates: map[string]holderRate{},
		readGates:   map[string]*readGate{},
		readCancels: map[cancelKey]time.Time{},
		verified:    map[string]verifiedFile{},
		fullSent:    map[string]fullIndexMark{},
		pending:     map[string]*pendingFile{},
		closed:      make(chan struct{}),
		scanEvery:   scanInterval,
		linkEvery:   linkInterval,
		indexEvery:  indexInterval,
		pruneEvery:  pruneInterval,
	}

	for _, v := range cfg.StoreViews() {
		db, err := OpenFileDB(filepath.Join(journalDir, v.ID+".ndjson"), cfg.Peer)
		if err != nil {
			return nil, err
		}
		streamMode := v.Sync == config.SyncStream
		unstream, blocked := false, ""
		if !streamMode && e.wasStreamed(v.ID, db, v.Dir) {
			if blocked = e.localModeFits(db, v.Dir); blocked != "" {
				// Filling the disk is worse than not switching: stay
				// streamed until there is room, and say why.
				logger.Error("store is configured local but its contents do not fit; it stays streamed", "store", v.ID, "why", blocked)
				streamMode = true
			} else {
				unstream = true
			}
		}
		var hist *History
		on, keep, herr := cfg.BackupFor(v.ID)
		if herr != nil {
			return nil, herr
		}
		if on {
			if !streamMode {
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
		if streamMode {
			state = stateDetached
		}
		fs := &StoreState{
			name:      v.ID,
			group:     v.Group,
			stream:    streamMode,
			preferred: v.Preferred,
			root:      v.Dir,
			offline:   v.Offline,
			symlinks:  v.Symlinks,
			links:     v.Links,
			state:     state,
			db:        db,
			hist:      hist,
			perm:      noPerms(),
			published: api.StoreSync{
				Store:       v.ID,
				Root:        v.Dir,
				State:       state,
				Permissions: noPerms().apiPerms(),
				Symlinks:    []api.StoreSymlink{},
			},
		}
		fs.unstream = unstream
		fs.modeNote = blocked
		e.stores = append(e.stores, fs)
		e.byName[fs.name] = fs
		if streamMode {
			if err := e.loadQueue(fs); err != nil {
				return nil, err
			}
			items, err := loadManifest(e.dataDir, fs.name)
			if err != nil {
				return nil, err
			}
			if len(items) > 0 {
				fs.migr = &migration{items: items}
			}
			fs.observeTree()
		}
	}
	e.cleanupStaleAtomicFiles()
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
	go e.pruneLoop(ctx, e.pruneEvery)

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
		if fs.stream && fs.migr != nil {
			wg.Add(1)
			go func(fs *StoreState) {
				defer wg.Done()
				e.migrateLoop(ctx, fs)
			}(fs)
		}
	}
	<-ctx.Done()
	// Scanners and link reconciliation must be down before the caller tears
	// anything down: both can be writing paths or journals.
	wg.Wait()
}

func (e *Engine) indexLoop(ctx context.Context) {
	t := time.NewTicker(e.indexEvery)
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

// fullIndexFloor is how often a store's whole index is resent to a peer even
// when nothing changed. The resend is a safety net for a frame lost in link
// churn; sending a hundred-thousand-entry index to every peer every 30 s, and
// making each of them walk it, was most of an idle mesh's traffic and CPU.
var fullIndexFloor = 10 * time.Minute

// queueRetry is how long a streamed store's queue may sit unacknowledged with
// a holder connected before it is handed over again. Flushes otherwise happen
// only on a write or on reconnecting, so an op the holder dropped waited for
// the next of those - possibly forever.
var queueRetry = 2 * time.Minute

type fullIndexMark struct {
	gen uint64
	at  time.Time
}

func (e *Engine) resendIndexes() {
	e.mu.Lock()
	peers := make([]string, 0, len(e.sends))
	for peer := range e.sends {
		peers = append(peers, peer)
	}
	e.mu.Unlock()
	now := time.Now()
	for _, fs := range e.stores {
		if !fs.stream || now.Sub(time.Unix(0, fs.lastFlush.Load())) < queueRetry {
			continue
		}
		fs.queueMu.Lock()
		waiting := len(fs.queue) > 0
		fs.queueMu.Unlock()
		if waiting {
			go e.flushQueue(fs)
		}
	}
	for _, peer := range peers {
		for _, fs := range e.stores {
			if !e.storeSharedWith(fs, peer) {
				continue
			}
			key := peer + "\x00" + fs.name
			gen := fs.db.Gen()
			e.mu.Lock()
			last, sent := e.fullSent[key]
			e.mu.Unlock()
			// A digest costs one frame when the peers agree, so it can go
			// on the same schedule the full index did: whenever the journal
			// moved, and on the floor otherwise. A change one side refused
			// ("leaving it to the next exchange") is retried by the next
			// one, so this cannot wait the whole floor while things move.
			if sent && last.gen == gen && now.Sub(last.at) < fullIndexFloor {
				continue
			}
			if err := e.offerIndex(peer, fs); err != nil {
				e.logger.Debug("resend index", "peer", peer, "store", fs.name, "error", err)
				continue
			}
			e.markFullSent(peer, fs, gen)
		}
	}
}

func (e *Engine) markFullSent(peer string, fs *StoreState, gen uint64) {
	e.mu.Lock()
	e.fullSent[peer+"\x00"+fs.name] = fullIndexMark{gen: gen, at: time.Now()}
	e.mu.Unlock()
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
	t := time.NewTicker(e.linkEvery)
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

// prepareRoot creates the canonical directory, repairs the container's
// ownership/traversal, and applies store permissions. Content is never moved
// into it: an existing directory is the user's, and a store adopts it.
func (e *Engine) prepareRoot(fs *StoreState) {
	// This runs on every link reconcile (every few seconds); say "ready"
	// once, not four lines per store every five seconds forever.
	fs.metaMu.Lock()
	first := !fs.prepared
	fs.metaMu.Unlock()
	ready := func(args ...any) {
		if first {
			e.logger.Info("store ready", args...)
		}
	}
	fs.scanMu.Lock()
	defer func() {
		fs.scanMu.Unlock()
		fs.metaMu.Lock()
		fs.prepared = true
		fs.metaMu.Unlock()
	}()
	fs.perm = e.resolvePerms(fs)
	if err := os.MkdirAll(e.cfg.Root, 0o755); err != nil {
		e.logger.Error("create store root", "store", fs.name, "path", e.cfg.Root, "error", err)
	}
	if fs.stream {
		// The canonical path is this daemon's mount, not a directory the
		// user owns: there is nothing to adopt, heal or chown here. Files
		// left from when it was local are moved aside first, or the mount
		// would hide them.
		if fs.migrationBlocked() {
			fs.refreshLocked()
			return
		}
		if !e.stageForStream(fs) {
			fs.refreshLocked()
			return
		}
		e.mountStream(fs)
		fs.refreshLocked()
		if first {
			if err := writeMode(e.dataDir, fs.name, config.SyncStream); err != nil {
				e.logger.Warn("record store mode", "store", fs.name, "error", err)
			}
		}
		ready("store", fs.name, "root", fs.root, "state", fs.state, "mount", fs.mounted())
		return
	}
	if fs.unstream {
		e.unstream(fs)
		fs.unstream = false
	}
	if err := os.MkdirAll(fs.root, 0o755); err != nil {
		e.logger.Error("create store root", "store", fs.name, "path", fs.root, "error", err)
	}
	if first {
		if err := writeMode(e.dataDir, fs.name, config.SyncLocal); err != nil {
			e.logger.Warn("record store mode", "store", fs.name, "error", err)
		}
	}
	e.healRoot(fs)
	e.applyDir(fs, fs.root)
	fs.refreshLocked()
	ready("store", fs.name, "root", fs.root, "state", fs.state)
}

func (e *Engine) scanLoop(ctx context.Context, fs *StoreState) {
	e.scanOnce(fs)
	// A pre-existing journal may contain files before the first scan. Send its
	// complete post-scan index once; fresh stores use ordinary scan deltas and
	// avoid duplicate startup index races.
	if len(fs.db.All()) > 0 {
		if err := e.broadcastFullIndex(fs); err != nil {
			e.logger.Debug("initial index", "store", fs.name, "error", err)
		}
	}
	t := time.NewTicker(e.scanEvery)
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

func (e *Engine) broadcastFullIndex(fs *StoreState) error {
	e.mu.Lock()
	peers := make([]string, 0, len(e.sends))
	for peer := range e.sends {
		peers = append(peers, peer)
	}
	e.mu.Unlock()
	for _, peer := range peers {
		if e.storeSharedWith(fs, peer) {
			if err := e.offerIndex(peer, fs); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *Engine) scanOnce(fs *StoreState) {
	fs.scanMu.Lock()
	// Re-resolve every pass: an inherited permission follows the directory
	// it came from, so a chown or chmod of the root's parent is honoured on
	// the next scan rather than at the next restart. The canonical dir is
	// the daemon's own, so it is re-applied here and heals drift.
	fs.perm = e.resolvePerms(fs)
	if fs.stream {
		// A streamed store has no local tree to walk: the journal is the
		// truth, and reading the mount to discover it would fetch every file
		// the store holds.
		fs.refreshLocked()
		fs.scanMu.Unlock()
		return
	}
	e.applyDir(fs, fs.root)
	fs.refreshLocked()
	fs.scanMu.Unlock()

	// A replica owns its canonical dir, so absence there is a deletion (of a
	// file this machine held: see Entry.Local).
	changed, err := e.scan(fs, true)
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
	for len(entries) > 0 {
		n := min(len(entries), indexChunk)
		b, err := json.Marshal(frame{Type: "index", Store: fs.name, Entries: entries[:n]})
		if err != nil {
			return
		}
		b = append(b, '\n')
		for _, s := range sends {
			_ = s.Send(b)
		}
		entries = entries[n:]
	}
}

func (e *Engine) sendListFor(fs *StoreState) []transport.Sender {
	members := e.cfg.Groups[fs.group].Peers
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]transport.Sender, 0, len(e.sends))
	for peer, s := range e.sends {
		if slices.Contains(members, peer) {
			out = append(out, s)
		}
	}
	return out
}

// --- transport hooks -------------------------------------------------------

var _ transport.UrgentHandler = (*Engine)(nil)

// OnPeerConnected opens a sync session: the peer gets this machine's full
// index for every store the two share, and remote stores may now go live.
func (e *Engine) OnPeerConnected(peer string, send transport.Sender) {
	e.mu.Lock()
	e.sends[peer] = send
	e.mu.Unlock()

	digest := peerHasDigest(send)
	for _, fs := range e.stores {
		if !e.storeSharedWith(fs, peer) {
			continue
		}
		gen := fs.db.Gen()
		if digest && !e.digestInitiator(peer) {
			// The peer opens the exchange; answering it is enough.
			e.markFullSent(peer, fs, gen)
			continue
		}
		if err := e.offerIndex(peer, fs); err != nil {
			e.logger.Debug("send index", "peer", peer, "store", fs.name, "error", err)
			continue
		}
		e.markFullSent(peer, fs, gen)
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
	case "index", "digest", "pull":
		ch, ok := e.indexFrames[fr.Store]
		if !ok {
			return // a store this machine does not carry
		}
		e.enqueueFrame(ch, pf)
	case "read":
		ch, ok := e.readFrames[fr.Store]
		if !ok {
			return
		}
		e.enqueueFrame(ch, pf)
	case "want", "rename", "verbs":
		ch, ok := e.wantFrames[fr.Store]
		if !ok {
			return // a store this machine does not carry
		}
		e.enqueueFrame(ch, pf)
	case "readData", "readErr", "verbsResult":
		// Cheap and latency-sensitive: a reply goes straight to the read
		// that is blocked on it, never behind a queue of store work.
		e.deliverRead(&fr)
	case "readCancel":
		e.noteReadCancel(peer, fr.ID)
	case "wantErr":
		e.handleWantErr(peer, &fr)
	case "blob", "blobEnd":
		e.enqueueFrame(e.blobFrames[blobShard(peer, &fr)], pf)
	}
}

// OnPeerUrgent takes read replies straight off the link's read loop. A reader
// blocked in the mount is waiting on exactly this frame, and queueing it behind
// a large index apply (or a full blob queue) turned a busy sync into EIO in the
// file manager. Frames are marshalled type-first, so a prefix check is enough
// to leave every other frame alone without decoding it twice.
func (e *Engine) OnPeerUrgent(peer string, line []byte) bool {
	// Every frame off the link passes here first: it is the evidence a
	// streamed read waits on that the holder is still sending (readStalled).
	e.heard(peer)
	for _, prefix := range urgentPrefixes {
		if bytes.HasPrefix(line, prefix) {
			var fr frame
			if err := json.Unmarshal(line, &fr); err != nil {
				return true
			}
			e.deliverRead(&fr)
			return true
		}
	}
	return false
}

var urgentPrefixes = [][]byte{
	[]byte(`{"type":"readData"`),
	[]byte(`{"type":"readErr"`),
	[]byte(`{"type":"verbsResult"`),
}

// evaluatePeers records whether each remote store has a holder reachable. It
// never changes a path: the canonical dir is the same directory whether or
// not a holder answers, so there is no root to flip.
func (e *Engine) evaluatePeers() {
	for _, fs := range e.stores {
		if !fs.stream {
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
			e.flushQueue(fs)
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
	e.cancelSends()
	e.frameWG.Wait()
	// The dispatchers are down, so nothing adds to these any more.
	e.readWG.Wait()
	e.finishWG.Wait()
	e.sendWG.Wait()
	e.mu.Lock()
	incoming := make([]*incomingBlob, 0, len(e.incoming))
	for key, inc := range e.incoming {
		delete(e.incoming, key)
		incoming = append(incoming, inc)
	}
	e.mu.Unlock()
	for _, inc := range incoming {
		_ = inc.f.Close()
		_ = os.Remove(inc.f.Name())
	}
	for _, fs := range e.stores {
		e.unmountStream(fs)
		fs.db.Close()
		if fs.hist != nil {
			fs.hist.Close()
		}
	}
}
