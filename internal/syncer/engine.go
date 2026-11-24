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
	"strings"
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
	qx      queueExtra // queue.go

	db   *FileDB
	hist *History
	// trashKeep is how long the trash keeps a file (config trash.keep).
	trashKeep time.Duration
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
	cancelMu sync.Mutex
	// trashMu: moving a file into the trash holds it shared (make the
	// directory, move in); an explicit cleanup removing emptied
	// directories holds it exclusively.
	trashMu        sync.RWMutex
	readCancels    map[cancelKey]time.Time
	verified       map[string]verifiedFile
	fullSent       map[string]fullIndexMark
	sx             streamExtra // stream.go: open write handles, read bookkeeping
	closed         chan struct{}
	closeOnce      sync.Once
	frameStartOnce sync.Once
	frameWG        sync.WaitGroup
	readWG         sync.WaitGroup
	// Per-store frame queues by class (see the dispatch notes at
	// frameQueue).
	indexFrames map[string]*frameQueue
	wantFrames  map[string]*frameQueue
	opsFrames   map[string]*frameQueue
	readFrames  map[string]*frameQueue

	// The loop intervals as they were when the engine was made: the package
	// variables are knobs for tests, and reading them from running loops
	// raced every test that turned one.
	scanEvery, linkEvery, indexEvery, pruneEvery time.Duration

	blobFrames [blobShards]*blobQueue
	// Blob flow control (credit.go): what this side may still send per
	// link, and what it owes each peer in acknowledgements.
	creditMu sync.Mutex
	credits  map[transport.Sender]*credit
	owed     map[string]int64

	// evalCh asks evaluateLoop to re-check which streamed stores have a
	// holder; capacity one, so requests coalesce.
	evalCh chan struct{}

	// finishSem bounds completed transfers being verified and applied, and
	// finishWG lets Close wait for them. finishQueue bounds the finished
	// transfers waiting for a slot: each holds a temporary file open.
	finishSem   chan struct{}
	finishQueue chan struct{}
	finishWG    sync.WaitGroup
	// blobLocks serialises finishing per blob (see lockBlob).
	blobLockMu sync.Mutex
	blobLocks  map[string]*blobLock

	// sendMu guards sendq, each peer's outgoing transfers; sendWG lets Close
	// wait for the running ones.
	// sendsClosed (under sendMu) refuses new transfers once Close has
	// cancelled the running ones, so none can Add to sendWG while Close
	// waits on it.
	sendMu      sync.Mutex
	sendq       map[string]*peerSends
	sendWG      sync.WaitGroup
	sendsClosed bool

	// safety is the replica data-safety bookkeeping (safety.go): fetches
	// backed off for lack of space.
	safety safetyState
}

type peerFrame struct {
	peer string
	fr   frame
	size int // the frame's wire size, for the queue's memory bound
}

// Frame dispatch. A link hands its frames to OnPeerFrame one at a time, from
// one goroutine, and anything OnPeerFrame waits for, the whole link waits for:
// its read loop stops, and with it the streamed reads' replies every mount on
// this machine is blocked on. A single store's index worker waiting out a long
// scan used to be enough to turn every other store's reads into EIO.
//
// So every class of frame has its own worker per store, behind a queue that
// never blocks the link. A queue bounds its memory instead; what overflows is
// dropped by class:
//
//   - index, digest, pull: idempotent, and the digest exchange recovers
//     anything missing. The queue remembers the peer, and once it has drained
//     sends that peer a fresh digest of the store (frameQueue.resync).
//   - want, wantErr: the asker re-asks on its next exchange or after
//     wantRefresh.
//   - read: the reader's stall detection retries.
//   - rename, verbs: tiny frames behind a generous bound; a streamed peer
//     resends its queue, and a verbs caller times out.
//
// Blob chunks are the exception: they keep backpressure, because dropping one
// abandons a transfer. Their workers only ever wait on the disk (finishing a
// transfer is handed off, see handleBlobEnd), so when they make the link wait
// it is because bytes are arriving faster than this machine can store them.
//
// indexQueueBytes is a var only so tests can make the queue overflow.
var indexQueueBytes = 32 << 20

const (
	wantQueueBytes = 4 << 20
	readQueueBytes = 4 << 20
	opsQueueBytes  = 16 << 20
	// blobFrameQueue holds a shard's incoming chunks. A peer that keeps a
	// blob window (credit.go) never has more than blobWindow in flight, so
	// this is room for several such peers in one shard without the link's
	// read loop ever waiting; an older peer still blocks it when full.
	blobFrameQueue = 128
)

// frameQueue is one worker's backlog: FIFO, bounded by bytes, and never
// blocking the producer.
type frameQueue struct {
	mu       sync.Mutex
	items    []peerFrame
	bytes    int
	maxBytes int
	// notify wakes the worker; capacity one, so a wake is never lost.
	notify chan struct{}
	// resync names peers whose frames were dropped for want of room.
	resync map[string]bool
}

func newFrameQueue(maxBytes int) *frameQueue {
	return &frameQueue{maxBytes: maxBytes, notify: make(chan struct{}, 1)}
}

// push queues a frame, reporting false (and remembering the peer) when the
// queue is full. An empty queue takes any one frame, however large.
func (q *frameQueue) push(pf peerFrame) bool {
	q.mu.Lock()
	if len(q.items) > 0 && q.bytes+pf.size > q.maxBytes {
		if q.resync == nil {
			q.resync = map[string]bool{}
		}
		q.resync[pf.peer] = true
		q.mu.Unlock()
		return false
	}
	q.items = append(q.items, pf)
	q.bytes += pf.size
	q.mu.Unlock()
	select {
	case q.notify <- struct{}{}:
	default:
	}
	return true
}

// pop waits for the next frame; false once closed is closed.
func (q *frameQueue) pop(closed <-chan struct{}) (peerFrame, bool) {
	for {
		q.mu.Lock()
		if len(q.items) > 0 {
			pf := q.items[0]
			q.items[0] = peerFrame{}
			q.items = q.items[1:]
			if len(q.items) == 0 {
				q.items = nil
			}
			q.bytes -= pf.size
			q.mu.Unlock()
			return pf, true
		}
		q.mu.Unlock()
		select {
		case <-closed:
			return peerFrame{}, false
		case <-q.notify:
		}
	}
}

// takeResync returns the peers that lost frames, once the backlog that made
// the queue overflow has drained.
func (q *frameQueue) takeResync() []string {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) > 0 || len(q.resync) == 0 {
		return nil
	}
	peers := make([]string, 0, len(q.resync))
	for p := range q.resync {
		peers = append(peers, p)
	}
	q.resync = nil
	return peers
}

// queued is the number of frames waiting.
func (q *frameQueue) queued() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

func (e *Engine) startFrameWorkers() {
	e.frameStartOnce.Do(func() {
		e.indexFrames = make(map[string]*frameQueue, len(e.stores))
		e.wantFrames = make(map[string]*frameQueue, len(e.stores))
		e.opsFrames = make(map[string]*frameQueue, len(e.stores))
		e.readFrames = make(map[string]*frameQueue, len(e.stores))
		for i := range e.blobFrames {
			e.blobFrames[i] = newBlobQueue()
		}
		e.frameWG.Add(1 + blobShards + 4*len(e.stores))
		go func() {
			defer e.frameWG.Done()
			e.evaluateLoop()
		}()
		serve := func(q *frameQueue, handle func(pf peerFrame)) {
			defer e.frameWG.Done()
			for {
				pf, ok := q.pop(e.closed)
				if !ok {
					return
				}
				handle(pf)
			}
		}
		// One worker per store for each class: a slow store delays only
		// itself, and serving a multi-gigabyte blob cannot hold another
		// store's index frames hostage.
		for _, fs := range e.stores {
			index := newFrameQueue(indexQueueBytes)
			e.indexFrames[fs.name] = index
			go serve(index, func(pf peerFrame) {
				// Digests share the index worker: a pull reply must be
				// judged after the index frames before it.
				switch pf.fr.Type {
				case "digest":
					e.handleDigest(pf.peer, &pf.fr)
				case "pull":
					e.handlePull(pf.peer, &pf.fr)
				default:
					e.handleIndex(pf.peer, &pf.fr)
				}
				for _, peer := range index.takeResync() {
					e.resyncStore(peer, fs)
				}
			})

			// Wants only queue a transfer, so they never wait on the scan
			// lock; renames and verbs do, and have a worker of their own.
			want := newFrameQueue(wantQueueBytes)
			e.wantFrames[fs.name] = want
			go serve(want, func(pf peerFrame) {
				if pf.fr.Type == "wantErr" {
					e.handleWantErr(pf.peer, &pf.fr)
					return
				}
				e.handleWant(pf.peer, &pf.fr)
			})
			ops := newFrameQueue(opsQueueBytes)
			e.opsFrames[fs.name] = ops
			go serve(ops, func(pf peerFrame) {
				switch pf.fr.Type {
				case "rename":
					e.handleRename(pf.peer, &pf.fr)
				case "verbs":
					e.handleVerbs(pf.peer, &pf.fr)
				}
			})

			reads := newFrameQueue(readQueueBytes)
			e.readFrames[fs.name] = reads
			slots := make(chan struct{}, readWorkers)
			go serve(reads, func(pf peerFrame) {
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
			})
		}
		for _, q := range e.blobFrames {
			go func(q *blobQueue) {
				defer e.frameWG.Done()
				for {
					pf, ok := q.pop(e.closed)
					if !ok {
						return
					}
					if hook := blobWorkHook.Load(); hook != nil {
						(*hook)(e, pf)
					}
					switch pf.fr.Type {
					case "blob":
						e.handleBlob(pf.peer, &pf.fr)
					case "blobEnd":
						e.handleBlobEnd(pf.peer, &pf.fr)
					}
					e.returnCredit(pf.peer, pf.size, q.len() == 0)
				}
			}(q)
		}
	})
}

// resyncStore recovers index-class frames a full queue dropped: a fresh
// digest makes the peer resend whatever differs and pull whatever it lacks.
func (e *Engine) resyncStore(peer string, fs *StoreState) {
	if !e.storeSharedWith(fs, peer) {
		return
	}
	e.logger.Info("index frames were dropped while the store was busy; resyncing", "peer", peer, "store", fs.name)
	if err := e.offerIndex(peer, fs); err != nil {
		e.logger.Debug("resync index", "peer", peer, "store", fs.name, "error", err)
		return
	}
	e.markFullSent(peer, fs, fs.db.Gen())
}

// enqueueFrame queues a frame for a worker without ever waiting for it.
func (e *Engine) enqueueFrame(q *frameQueue, pf peerFrame) {
	if q.push(pf) {
		return
	}
	switch pf.fr.Type {
	case "index", "digest", "pull":
		e.logger.Debug("store busy: index frame dropped, resync queued", "peer", pf.peer, "store", pf.fr.Store, "type", pf.fr.Type)
	case "rename", "verbs":
		e.logger.Warn("store busy: frame dropped", "peer", pf.peer, "store", pf.fr.Store, "type", pf.fr.Type)
	default:
		e.logger.Info("store busy: frame dropped", "peer", pf.peer, "store", pf.fr.Store, "type", pf.fr.Type)
	}
}

// enqueueBlob queues a blob chunk. A peer that keeps a blob window is never
// waited on - the window bounds what it can have queued - so the link's read
// loop goes on reading; an older peer waits for room (see the dispatch
// notes).
func (e *Engine) enqueueBlob(q *blobQueue, pf peerFrame, metered bool) {
	q.push(pf, !metered, e.closed)
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
		finishQueue: make(chan struct{}, finishBacklog),
		evalCh:      make(chan struct{}, 1),
		blobLocks:   map[string]*blobLock{},
		sendq:       map[string]*peerSends{},
		incoming:    map[string]*incomingBlob{},
		reads:       map[int64]chan readReply{},
		holderRates: map[string]holderRate{},
		readGates:   map[string]*readGate{},
		credits:     map[transport.Sender]*credit{},
		owed:        map[string]int64{},
		readCancels: map[cancelKey]time.Time{},
		verified:    map[string]verifiedFile{},
		fullSent:    map[string]fullIndexMark{},
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
		trashKeep, terr := cfg.TrashKeep()
		if terr != nil {
			return nil, terr
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
			trashKeep: trashKeep,
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
//
// A connect is also a reconnect, and nothing that was in flight on the old
// link survives it: its transfers are cancelled (they would carry on over the
// new link with the chunks lost in the old one missing, and the receiver
// would drop everything after the gap), the half-received files from that
// peer are discarded, and every want this side had pointed at the peer may be
// asked again at once rather than after wantRefresh.
func (e *Engine) OnPeerConnected(peer string, send transport.Sender) {
	e.cancelPeerSends(peer)
	e.mu.Lock()
	prev := e.sends[peer]
	e.sends[peer] = send
	stale := e.forgetPeerLocked(peer)
	e.mu.Unlock()
	if prev != nil && !sameSender(prev, send) {
		e.forgetCredit(prev)
	}
	// Acknowledgements owed for the old link's chunks mean nothing on this
	// one: the peer's window for it starts empty.
	e.creditMu.Lock()
	delete(e.owed, peer)
	e.creditMu.Unlock()
	e.peerLinkChanged(peer) // reads sent over a replaced link will not be answered
	closeIncoming(stale)

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
	e.requestEvaluate()
}

// OnPeerDisconnected drops the session and re-evaluates remote stores. Only
// the link that is going away is forgotten: a replacement that is already
// installed stays. Transfers to the peer stop, and wants pointed at it may go
// to the next peer that offers the version.
func (e *Engine) OnPeerDisconnected(peer string, send transport.Sender) {
	e.mu.Lock()
	cur, ok := e.sends[peer]
	if !ok || !sameSender(cur, send) {
		e.mu.Unlock()
		return
	}
	delete(e.sends, peer)
	stale := e.forgetPeerLocked(peer)
	e.mu.Unlock()
	e.peerLinkChanged(peer)
	closeIncoming(stale)
	e.cancelPeerSends(peer)
	e.forgetCredit(send)
	e.requestEvaluate()
}

// sameSender compares two senders without panicking on a sender type that is
// not comparable (a test fake holding a slice, say).
func sameSender(a, b transport.Sender) (same bool) {
	defer func() {
		if recover() != nil {
			same = false
		}
	}()
	return a == b
}

// forgetPeerLocked undoes what a link to the peer had in flight on the
// receiving side: wants it was asked for become due again, and its
// half-received transfers are taken out (returned for closing outside the
// lock). Callers hold e.mu.
func (e *Engine) forgetPeerLocked(peer string) []*incomingBlob {
	for _, set := range e.wants {
		for _, p := range set {
			if p.peer == peer {
				p.asked = time.Time{}
			}
		}
	}
	var stale []*incomingBlob
	prefix := peer + "\x00"
	for key, inc := range e.incoming {
		if strings.HasPrefix(key, prefix) {
			delete(e.incoming, key)
			stale = append(stale, inc)
		}
	}
	return stale
}

// closeIncoming discards abandoned transfers. A chunk worker may still be
// writing one; its next write fails and it gives the transfer up too.
func closeIncoming(incs []*incomingBlob) {
	for _, inc := range incs {
		_ = inc.f.Close()
		_ = os.Remove(inc.f.Name())
	}
}

// OnPeerFrame parses one wire frame and queues it by work class. It never
// waits for a store (see the dispatch notes at frameQueue); only blob chunks
// wait, for the disk.
func (e *Engine) OnPeerFrame(peer string, line []byte) {
	e.startFrameWorkers()
	var fr frame
	if err := json.Unmarshal(line, &fr); err != nil {
		return
	}
	pf := peerFrame{peer: peer, fr: fr, size: len(line)}
	queue := func(qs map[string]*frameQueue) {
		if q, ok := qs[fr.Store]; ok {
			e.enqueueFrame(q, pf)
		} // else a store this machine does not carry
	}
	switch fr.Type {
	case "index", "digest", "pull":
		queue(e.indexFrames)
	case "read":
		queue(e.readFrames)
	case "want", "wantErr":
		queue(e.wantFrames)
	case "rename", "verbs":
		queue(e.opsFrames)
	case "readData", "readErr", "verbsResult":
		// Cheap and latency-sensitive: a reply goes straight to the read
		// that is blocked on it, never behind a queue of store work.
		e.deliverRead(&fr)
	case "readCancel":
		e.noteReadCancel(peer, fr.ID)
	case "blob", "blobEnd":
		e.enqueueBlob(e.blobFrames[blobShard(peer, &fr)], pf, e.peerMetered(peer))
	case "blobCredit":
		e.gotCredit(peer, fr.Length)
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
	if bytes.HasPrefix(line, creditPrefix) {
		var fr frame
		if json.Unmarshal(line, &fr) == nil {
			e.gotCredit(peer, fr.Length)
		}
		return true
	}
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

// creditPrefix marks a receiver's blob acknowledgement: a sender's transfers
// wait on it, so it is taken off the read loop like a read's reply.
var creditPrefix = []byte(`{"type":"blobCredit"`)

var urgentPrefixes = [][]byte{
	[]byte(`{"type":"readData"`),
	[]byte(`{"type":"readErr"`),
	[]byte(`{"type":"verbsResult"`),
}

// requestEvaluate asks evaluateLoop for an evaluatePeers pass. Link callbacks
// used to run it themselves: it takes every streamed store's scan lock and
// may scan and flush a queue, all inside the transport's shutdown wait and
// ahead of the link's frames.
func (e *Engine) requestEvaluate() {
	select {
	case e.evalCh <- struct{}{}:
	default: // one is already due, and will see this change too
	}
}

func (e *Engine) evaluateLoop() {
	for {
		select {
		case <-e.closed:
			return
		case <-e.evalCh:
			e.evaluatePeers()
		}
	}
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
