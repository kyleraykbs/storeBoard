package syncer

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"codeberg.org/kyleraykbs/storeboard/internal/config"
	"codeberg.org/kyleraykbs/storeboard/internal/store"
	"codeberg.org/kyleraykbs/storeboard/internal/streamfs"
)

// Queue operations. A streamed store writes into a durable local queue and
// propagates each op with the ordinary index exchange, so a queued write
// merges under exactly the same conflict rules as any other change.
const (
	opPut    = "put"
	opDelete = "delete"
	opMkdir  = "mkdir"
	opRmdir  = "rmdir"
	opRename = "rename"
	// opDrop is a queue-file record, not an op: the op of that id is done.
	opDrop = "drop"
)

type renameMove struct {
	From string `json:"from"`
	Old  *Entry `json:"old"`
	New  *Entry `json:"new"`
}

// queueOp is one queued change. The bytes of a put live beside the index at
// <dataDir>/queue/<store>/<id>, so a crash between the journal and the wire
// never loses what the user wrote.
type queueOp struct {
	ID    string            `json:"id"`
	Op    string            `json:"op"`
	Path  string            `json:"path"`
	To    string            `json:"to,omitempty"`
	Mode  uint32            `json:"mode,omitempty"`
	MTime int64             `json:"mtime,omitempty"`
	Size  int64             `json:"size,omitempty"`
	Blob  string            `json:"blob,omitempty"`
	VV    map[string]uint64 `json:"vv,omitempty"`
	Dir   bool              `json:"dir,omitempty"`
	Moves []renameMove      `json:"moves,omitempty"`
}

// queueExtra is a streamed store's queue bookkeeping. Fields other than
// pathMu are guarded by queueMu.
type queueExtra struct {
	// lines is how many records the queue file holds, live or not. The file
	// is appended to; it is rewritten once dead records (acknowledged or
	// superseded ops) outnumber live ones, so every op costs O(1) amortised
	// instead of a rewrite and fsync of the whole queue.
	lines int
	// sent is the ops handed to sentTo over link generation sentGen since the
	// last full flush: a flush after a write sends only what is new. The
	// periodic retry and every new link send everything again.
	sent    map[string]bool
	sentTo  string
	sentGen uint64
	// ver counts changes to the queue; paths (built at pathsVer) is every
	// path an acknowledgement could match, so the index exchange does not
	// scan the queue for each of a store's entries.
	ver      uint64
	pathsVer uint64
	paths    map[string]bool

	// pathMu orders path changes made through the mount (rename, remove)
	// against a write handle committing. A handle that committed under the
	// path it was opened at after a rename moved that path found it
	// tombstoned and threw the write away: GIO's atomic save renames the
	// temporary file before the kernel delivers RELEASE. Lock order: scanMu,
	// then pathMu, then queueMu.
	pathMu sync.Mutex
}

// pendingKey names one open path in one store.
type pendingKey struct{ store, path string }

func (e *Engine) registerPending(p *pendingFile) {
	p.mu.Lock()
	k := pendingKey{p.fs.name, p.path}
	p.mu.Unlock()
	e.sx.pendingMu.Lock()
	defer e.sx.pendingMu.Unlock()
	if e.sx.pending == nil {
		e.sx.pending = map[pendingKey]map[*pendingFile]struct{}{}
	}
	if e.sx.pending[k] == nil {
		e.sx.pending[k] = map[*pendingFile]struct{}{}
	}
	e.sx.pending[k][p] = struct{}{}
}

func (e *Engine) unregisterPending(p *pendingFile) {
	e.sx.pendingMu.Lock()
	defer e.sx.pendingMu.Unlock()
	p.mu.Lock()
	k := pendingKey{p.fs.name, p.path}
	p.mu.Unlock()
	if set := e.sx.pending[k]; set != nil {
		delete(set, p)
		if len(set) == 0 {
			delete(e.sx.pending, k)
		}
	}
}

// hasPending reports whether a write handle is open on a path.
func (e *Engine) hasPending(fs *StoreState, path string) bool {
	e.sx.pendingMu.Lock()
	defer e.sx.pendingMu.Unlock()
	return len(e.sx.pending[pendingKey{fs.name, path}]) > 0
}

// repathPending moves every handle open on from to to, and reports whether
// one of them created the file (its bytes exist only in that handle).
func (e *Engine) repathPending(fs *StoreState, from, to string) (open, created bool) {
	e.sx.pendingMu.Lock()
	defer e.sx.pendingMu.Unlock()
	fk, tk := pendingKey{fs.name, from}, pendingKey{fs.name, to}
	set := e.sx.pending[fk]
	if len(set) == 0 {
		return false, false
	}
	delete(e.sx.pending, fk)
	if e.sx.pending[tk] == nil {
		e.sx.pending[tk] = map[*pendingFile]struct{}{}
	}
	for p := range set {
		p.mu.Lock()
		p.path = to
		if p.created {
			created = true
		}
		p.mu.Unlock()
		e.sx.pending[tk][p] = struct{}{}
	}
	return true, created
}

func queueIndexPath(dataDir, storeName string) string {
	return filepath.Join(dataDir, "queue", storeName+".ndjson")
}

func queueBytesDir(dataDir, storeName string) string {
	return filepath.Join(dataDir, "queue", storeName)
}

// loadQueue replays a store's queue at startup and drops byte files no op
// refers to: those are the remains of an op that completed before the crash.
func (e *Engine) loadQueue(fs *StoreState) error {
	index := queueIndexPath(e.dataDir, fs.name)
	if err := os.MkdirAll(queueBytesDir(e.dataDir, fs.name), 0o700); err != nil {
		return err
	}
	f, err := os.Open(index)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	live := map[string]bool{}
	lines, torn := 0, false
	var read []queueOp
	last := map[string]int{}
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		lines++
		var op queueOp
		if err := json.Unmarshal(line, &op); err != nil || op.ID == "" {
			// A torn final line is expected after a crash. It must not stay:
			// the next append would land on the end of it and be torn too.
			torn = true
			continue
		}
		last[op.ID] = len(read)
		read = append(read, op)
	}
	if err := sc.Err(); err != nil {
		return err
	}
	// The file is appended to: a later record of the same id is that op
	// changed (an upload moved by a rename), and it now sits there; a drop
	// record retires it.
	for i, op := range read {
		if last[op.ID] == i && op.Op != opDrop {
			fs.queue = append(fs.queue, op)
		}
	}
	loaded := len(fs.queue)
	fs.queue = e.dropUnservablePuts(fs, fs.queue)
	fs.queue, _ = compactQueue(fs.queue)
	for _, op := range fs.queue {
		live[op.ID] = true
		fs.queuedBytes += op.Size
	}
	fs.qx.lines = lines
	fs.qx.ver++
	if len(fs.queue) != loaded || torn || lines != len(fs.queue) {
		// A queue written before these rules, or one holding uploads whose
		// bytes are gone: rewrite it so the ops that can never complete stop
		// being re-sent and counted.
		e.logger.Info("compacted write queue", "store", fs.name, "before", loaded, "after", len(fs.queue))
		if err := e.writeQueue(fs); err != nil {
			return err
		}
	}
	entries, err := os.ReadDir(queueBytesDir(e.dataDir, fs.name))
	if err != nil {
		return err
	}
	for _, ent := range entries {
		if !live[ent.Name()] {
			_ = os.Remove(filepath.Join(queueBytesDir(e.dataDir, fs.name), ent.Name()))
		}
	}
	return nil
}

// dropUnservablePuts removes uploads whose bytes are nowhere on this machine.
// No holder can ever fetch them - every want is answered "missing" - so the op
// would sit in the queue for good, holding up nothing but the count. The
// version is already lost; keeping the op only hides that.
func (e *Engine) dropUnservablePuts(fs *StoreState, ops []queueOp) []queueOp {
	kept := ops[:0]
	for _, op := range ops {
		if op.Op == opPut {
			_, err := os.Stat(filepath.Join(queueBytesDir(e.dataDir, fs.name), op.ID))
			if err != nil && !e.store.Has(op.Blob) {
				e.logger.Warn("queued upload has no bytes; dropping it", "store", fs.name, "path", op.Path, "blob", op.Blob)
				continue
			}
		}
		kept = append(kept, op)
	}
	return kept
}

// compactQueue drops uploads no holder will ever need: a put followed by a
// later put or delete of the same path, with no rename in between carrying
// the path away. (A rename does need the bytes under the old name: the holder
// moves what it has.) It returns the ops kept and the ones dropped, in order.
func compactQueue(ops []queueOp) (kept, dropped []queueOp) {
	overwritten := map[string]bool{}
	keep := make([]bool, len(ops))
	for i := len(ops) - 1; i >= 0; i-- {
		op := ops[i]
		keep[i] = true
		switch op.Op {
		case opPut:
			if overwritten[op.Path] {
				keep[i] = false
			}
			overwritten[op.Path] = true
		case opDelete:
			overwritten[op.Path] = true
		case opRename:
			delete(overwritten, op.Path)
			for _, move := range op.Moves {
				delete(overwritten, move.From)
			}
		}
	}
	kept = make([]queueOp, 0, len(ops))
	for i, op := range ops {
		if keep[i] {
			kept = append(kept, op)
		} else {
			dropped = append(dropped, op)
		}
	}
	return kept, dropped
}

// compactLocked applies compactQueue to a store's live queue and removes the
// bytes of what it dropped. Callers hold queueMu.
func (e *Engine) compactLocked(fs *StoreState) {
	kept, dropped := compactQueue(fs.queue)
	if len(dropped) == 0 {
		return
	}
	ids := make(map[string]bool, len(kept))
	for _, op := range kept {
		ids[op.ID] = true
	}
	var records []queueOp
	for _, op := range dropped {
		fs.queuedBytes -= op.Size
		if !ids[op.ID] {
			_ = os.Remove(filepath.Join(queueBytesDir(e.dataDir, fs.name), op.ID))
			records = append(records, queueOp{ID: op.ID, Op: opDrop})
		}
	}
	fs.queue = kept
	fs.qx.ver++
	// Replaying the file would compact it the same way; the records only
	// spare a restart from mistaking the removed bytes for a lost upload.
	_ = e.appendQueue(fs, records, false)
	e.tidyQueueFileLocked(fs)
}

// appendQueue adds records to the end of the queue file. sync makes them
// durable before it returns: an op must be, a drop record need not be (a
// lost one only re-sends an op the holder has). A failed append is cut back
// off, so a half-written record never swallows the next one.
func (e *Engine) appendQueue(fs *StoreState, ops []queueOp, sync bool) error {
	if len(ops) == 0 {
		return nil
	}
	index := queueIndexPath(e.dataDir, fs.name)
	if err := os.MkdirAll(filepath.Dir(index), 0o700); err != nil {
		return err
	}
	var buf []byte
	for _, op := range ops {
		b, err := json.Marshal(op)
		if err != nil {
			return err
		}
		buf = append(append(buf, b...), '\n')
	}
	f, err := os.OpenFile(index, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	_, err = f.Write(buf)
	if err == nil && sync {
		err = f.Sync()
	}
	if err != nil {
		_ = f.Truncate(info.Size())
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	fs.qx.lines += len(ops)
	return nil
}

// tidyQueueFileLocked rewrites the queue file once it is mostly dead
// records, or removes it once nothing is queued. Callers hold queueMu.
func (e *Engine) tidyQueueFileLocked(fs *StoreState) {
	if len(fs.queue) > 0 && fs.qx.lines <= 2*len(fs.queue)+64 {
		return
	}
	if err := e.writeQueue(fs); err != nil {
		e.logger.Warn("rewrite the write queue", "store", fs.name, "error", err)
	}
}

// writeQueue persists the whole index in one atomic rewrite. Ops are
// appended as they come (appendQueue); this compacts the file.
func (e *Engine) writeQueue(fs *StoreState) error {
	index := queueIndexPath(e.dataDir, fs.name)
	if len(fs.queue) == 0 {
		if err := os.Remove(index); err != nil && !os.IsNotExist(err) {
			return err
		}
		fs.qx.lines = 0
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(index), 0o700); err != nil {
		return err
	}
	tmp := index + ".new"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	for _, op := range fs.queue {
		b, err := json.Marshal(op)
		if err != nil {
			f.Close()
			return err
		}
		w.Write(b)
		w.WriteByte('\n')
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, index); err != nil {
		return err
	}
	fs.qx.lines = len(fs.queue)
	return nil
}

// enqueue records one op. Callers hold queueMu - the one lock that guards the
// queue. (Some callers used to hold scanMu instead, so two writers could append
// at once, lose an op, or interleave two rewrites of the queue file.)
//
// A put or a delete supersedes an earlier put of the same path that no holder
// has acknowledged (compactQueue): those bytes are a version nobody will ever
// ask for again, and keeping them grew the queue - and a capped queue's count
// - forever. An upload a later rename still needs is kept.
//
// The op is durable in the queue file before it is in memory: an op only in
// memory is lost at the next restart, and with it - since loadQueue removes
// bytes no op names - the only copy of an upload. A failed append leaves the
// queue as it was.
func (e *Engine) enqueue(fs *StoreState, op queueOp) error {
	if err := e.appendQueue(fs, []queueOp{op}, true); err != nil {
		return err
	}
	fs.queue = append(fs.queue, op)
	fs.queuedBytes += op.Size
	fs.qx.ver++
	e.compactLocked(fs)
	return nil
}

// enqueueOp is enqueue for callers that do not hold queueMu.
func (e *Engine) enqueueOp(fs *StoreState, op queueOp) error {
	fs.queueMu.Lock()
	defer fs.queueMu.Unlock()
	return e.enqueue(fs, op)
}

// dropQueued forgets an op and its bytes. Callers hold queueMu.
func (e *Engine) dropQueued(fs *StoreState, id string) {
	e.dropQueuedSet(fs, map[string]bool{id: true})
}

// dropQueuedSet forgets several ops and their bytes in one pass and one
// write. Callers hold queueMu.
func (e *Engine) dropQueuedSet(fs *StoreState, ids map[string]bool) {
	if len(ids) == 0 {
		return
	}
	kept := fs.queue[:0]
	var records []queueOp
	for _, op := range fs.queue {
		if ids[op.ID] {
			fs.queuedBytes -= op.Size
			if op.Blob != "" {
				_ = os.Remove(filepath.Join(queueBytesDir(e.dataDir, fs.name), op.ID))
			}
			records = append(records, queueOp{ID: op.ID, Op: opDrop})
			delete(fs.qx.sent, op.ID)
			continue
		}
		kept = append(kept, op)
	}
	clear(fs.queue[len(kept):])
	fs.queue = kept
	fs.qx.ver++
	if len(records) > 0 && len(fs.queue) > 0 {
		_ = e.appendQueue(fs, records, false)
	}
	e.tidyQueueFileLocked(fs)
}

// ackPathsLocked is every path a remote entry must have to acknowledge
// something queued. Callers hold queueMu.
func (fs *StoreState) ackPathsLocked() map[string]bool {
	if fs.qx.paths != nil && fs.qx.pathsVer == fs.qx.ver {
		return fs.qx.paths
	}
	paths := make(map[string]bool, len(fs.queue))
	for _, op := range fs.queue {
		paths[op.Path] = true
		if op.Op == opPut {
			paths[conflictPath(op.Path, op.Blob)] = true
		}
		for _, move := range op.Moves {
			if move.New != nil {
				paths[move.New.Path] = true
			}
		}
	}
	fs.qx.paths, fs.qx.pathsVer = paths, fs.qx.ver
	return paths
}

// ackQueue drops the ops a peer has now seen. A remote entry that carries this
// machine's counter for the path is proof the change landed. Every kind of op
// must be acknowledged somehow: an op that never is stays queued forever, is
// re-sent on every flush, and (for a put) keeps its bytes on disk.
//
// Only a peer that holds the store's bytes can acknowledge: another streamed
// peer records the entry without them, and dropping the upload on its word
// deleted the only copy.
func (e *Engine) ackQueue(peer string, fs *StoreState, rem *Entry) {
	if !fs.stream {
		return
	}
	if !e.peerIsReplica(peer, fs) {
		return
	}
	fs.queueMu.Lock()
	defer fs.queueMu.Unlock()
	if len(fs.queue) == 0 || !fs.ackPathsLocked()[rem.Path] {
		return
	}
	drop := map[string]bool{}
	for _, op := range fs.queue {
		switch op.Op {
		case opPut:
			// Landed as written; or seen and then deleted; or parked whole
			// as a conflict copy because a concurrent version won the path.
			// A covering entry with different bytes is not enough: the
			// holder may still be about to fetch ours as its conflict copy.
			landed := rem.Path == op.Path && vvCovers(rem.VV, op.VV) && (rem.Blob == op.Blob || rem.Deleted)
			parked := rem.Path == conflictPath(op.Path, op.Blob) && rem.Blob == op.Blob && !rem.Deleted
			if landed || parked {
				drop[op.ID] = true
			}
		case opDelete, opRmdir, opMkdir:
			// No bytes ride on these: once the holder's entry covers ours,
			// it has seen the change, whatever it did next.
			if rem.Path == op.Path && vvCovers(rem.VV, op.VV) {
				drop[op.ID] = true
			}
		case opRename:
			// Landed, or landed and since deleted: a tombstone that covers
			// the moved entry's counter was made by a holder that saw the
			// move. (Requiring a live entry kept a rename whose destination
			// was later removed queued forever.)
			for _, move := range op.Moves {
				if move.New != nil && rem.Path == move.New.Path && vvCovers(rem.VV, move.New.VV) {
					drop[op.ID] = true
					break
				}
			}
		}
	}
	e.dropQueuedSet(fs, drop)
}

// vvCovers reports whether remote has seen every counter this side recorded.
func vvCovers(remote, mine map[string]uint64) bool {
	for k, v := range mine {
		if remote[k] < v {
			return false
		}
	}
	return true
}

// flushQueue hands queued changes to the best holder. Every op travels as an
// ordinary index entry (or a rename frame), so the holder's version-vector
// rules decide what wins and a queued write can never overwrite a newer one.
//
// Every write through the mount asks for a flush. Concurrent requests
// coalesce into one more pass rather than each re-sending the whole queue.
func (e *Engine) flushQueue(fs *StoreState) {
	if !fs.stream {
		return
	}
	if !fs.flushing.CompareAndSwap(false, true) {
		fs.flushAgain.Store(true)
		return
	}
	for {
		fs.flushAgain.Store(false)
		e.flushQueueOnce(fs)
		fs.flushing.Store(false)
		if !fs.flushAgain.Load() || !fs.flushing.CompareAndSwap(false, true) {
			return
		}
	}
}

// flushBatch bounds the entries one flushed index frame carries.
const flushBatch = 512

// flushQueueOnce sends the queue to the best holder of the store's bytes (a
// streamed peer would only journal it). A flush asked for by a write sends
// only the ops that holder has not been sent over its current link; the
// periodic retry (queueRetry since the last full flush), a different holder
// or a new link sends everything again, which is what recovers an op the
// holder lost.
func (e *Engine) flushQueueOnce(fs *StoreState) {
	peers := e.replicaHoldersFor(fs)
	if len(peers) == 0 {
		return
	}
	peer := peers[0]
	gen := e.linkGenOf(peer)
	now := time.Now()
	fs.queueMu.Lock()
	full := fs.qx.sentTo != peer || fs.qx.sentGen != gen ||
		now.Sub(time.Unix(0, fs.lastFlush.Load())) >= queueRetry
	if full || fs.qx.sent == nil {
		fs.qx.sent, fs.qx.sentTo, fs.qx.sentGen = map[string]bool{}, peer, gen
	}
	ops := make([]queueOp, 0, len(fs.queue))
	for _, op := range fs.queue {
		if !fs.qx.sent[op.ID] {
			ops = append(ops, op)
		}
	}
	fs.queueMu.Unlock()
	if len(ops) == 0 {
		return
	}
	if full {
		fs.lastFlush.Store(now.UnixNano())
	}
	markSent := func(ids []string) {
		fs.queueMu.Lock()
		if fs.qx.sentTo == peer && fs.qx.sentGen == gen && fs.qx.sent != nil {
			for _, id := range ids {
				fs.qx.sent[id] = true
			}
		}
		fs.queueMu.Unlock()
	}
	var batch []*Entry
	var batchIDs []string
	send := func() bool {
		if len(batch) == 0 {
			return true
		}
		err := e.sendIndexTo(peer, fs, batch)
		ids := batchIDs
		batch, batchIDs = nil, nil
		if err != nil {
			e.logger.Debug("flush queue", "store", fs.name, "error", err)
			return false
		}
		markSent(ids)
		return true
	}
	for _, op := range ops {
		if op.Op == opRename {
			// A rename is ordered against the entries around it.
			if !send() {
				return
			}
			fr := frame{Type: "rename", Store: fs.name, Path: op.Path, To: op.To, Moves: op.Moves}
			if err := e.sendTo(peer, fr); err != nil {
				e.logger.Debug("flush rename", "store", fs.name, "path", op.Path, "error", err)
				return
			}
			markSent([]string{op.ID})
			continue
		}
		ent := &Entry{Path: op.Path, Blob: op.Blob, Size: op.Size, MTime: op.MTime, Mode: op.Mode, VV: op.VV}
		switch op.Op {
		case opDelete, opRmdir:
			ent = &Entry{Path: op.Path, Deleted: true, Dir: op.Op == opRmdir, VV: op.VV}
		case opMkdir:
			ent = &Entry{Path: op.Path, Dir: true, Mode: op.Mode, VV: op.VV}
		}
		batch = append(batch, ent)
		batchIDs = append(batchIDs, op.ID)
		if len(batch) >= flushBatch && !send() {
			return
		}
	}
	send()
}

// queuedBlob returns the queued bytes for a blob this machine wrote but has
// not handed over yet, which is how a holder pulls an offline write.
func (e *Engine) queuedBlob(fs *StoreState, sum string) (string, bool) {
	if !fs.stream || sum == "" {
		return "", false
	}
	fs.queueMu.Lock()
	defer fs.queueMu.Unlock()
	for _, op := range fs.queue {
		if op.Op == opPut && op.Blob == sum {
			return filepath.Join(queueBytesDir(e.dataDir, fs.name), op.ID), true
		}
	}
	return "", false
}

// storeMutator is the write surface of one streamed store.
type storeMutator struct {
	e  *Engine
	fs *StoreState
}

func (m *storeMutator) policy() config.StreamPolicy {
	if m.fs.offline == nil || m.fs.offline.Policy == "" {
		return config.StreamQueueUnbounded
	}
	return m.fs.offline.Policy
}

// refuse reports why a write cannot be accepted under this store's policy.
func (m *storeMutator) refuse() error {
	if m.policy() == config.StreamQueueReadOnly {
		return os.ErrPermission
	}
	return nil
}

func (m *storeMutator) Create(path string, mode uint32) (streamfs.PendingFile, error) {
	if err := m.refuse(); err != nil {
		return nil, err
	}
	pf, err := m.pending(path, mode, 0, nil)
	if err != nil {
		return nil, err
	}
	if p, ok := pf.(*pendingFile); ok {
		p.created = true
		m.e.registerPending(p)
	}
	markTouched(pf)
	// Record the empty file now: `touch foo && rm foo` in one shell sends
	// Unlink before the kernel delivers RELEASE.
	m.fs.queueMu.Lock()
	prev := m.fs.db.Get(path)
	ent := &Entry{Path: path, Blob: emptyBlob(m.e), Size: 0, MTime: time.Now().UnixNano(), Mode: mode, VV: m.e.nextVV(m.fs, prev)}
	err = m.fs.db.Put(ent)
	m.fs.queueMu.Unlock()
	if err != nil {
		pf.Close()
		return nil, err
	}
	return pf, nil
}

// emptyBlob is the hash of no bytes, which is what a just-created file holds.
func emptyBlob(e *Engine) string {
	sum, err := store.SumFile(os.DevNull)
	if err != nil {
		return ""
	}
	return sum
}

// Open seeds a write with the holder's current bytes unless the caller asked
// to truncate. That is a full download: writing in place is the one case a
// streamed store cannot serve by ranges alone.
func (m *storeMutator) Open(path string, truncate bool) (streamfs.PendingFile, error) {
	if err := m.refuse(); err != nil {
		return nil, err
	}
	ent := m.fs.db.Get(path)
	if ent == nil || ent.Deleted || ent.Dir {
		return nil, os.ErrNotExist
	}
	base := ent.Size
	var fetch func(off, length int64) ([]byte, error)
	if truncate {
		base = 0
	} else {
		// Editing in place commits the whole file, so the bytes not
		// written must come from somewhere at release. With no holder and
		// no copy here that fetch fails and the edit is thrown away after
		// the application was told it succeeded: refuse it now instead.
		if ent.Size > 0 && !m.e.bytesHere(m.fs, ent.Blob) && len(m.e.replicaHoldersFor(m.fs)) == 0 {
			return nil, fmt.Errorf("%s: no holder reachable to edit it in place: %w", path, syscall.EROFS)
		}
		fetch = m.fetcher(ent)
	}
	pf, err := m.pending(path, ent.Mode, base, fetch)
	if err != nil {
		return nil, err
	}
	if p, ok := pf.(*pendingFile); ok {
		m.e.registerPending(p)
	}
	if truncate {
		markTouched(pf)
	}
	return pf, nil
}

// bytesHere reports whether this machine has a blob's bytes without a holder:
// queued, or staged by a switch from local.
func (e *Engine) bytesHere(fs *StoreState, sum string) bool {
	if _, ok := e.queuedBlob(fs, sum); ok {
		return true
	}
	_, ok, _ := e.readStaged(fs, sum, 0, 0)
	return ok
}

// fetcher reads an entry's bytes from a holder on demand. Opening a file for
// writing must not download it: only the ranges the writer actually touches
// come over the wire.
func (m *storeMutator) fetcher(ent *Entry) func(off, length int64) ([]byte, error) {
	return func(off, length int64) ([]byte, error) {
		// Bytes this machine wrote and has not handed over yet live in the
		// queue: that copy is newer than the holder's, so read it first.
		if path, ok := m.e.queuedBlob(m.fs, ent.Blob); ok {
			if f, err := os.Open(path); err == nil {
				defer f.Close()
				buf := make([]byte, length)
				n, err := f.ReadAt(buf, off)
				if err != nil && err != io.EOF {
					return nil, err
				}
				return buf[:n], nil
			}
		}
		return (&streamFetcher{e: m.e, fs: m.fs}).ReadRange(context.Background(),
			streamfs.Entry{Path: ent.Path, Blob: ent.Blob, Size: ent.Size}, off, length)
	}
}

func (m *storeMutator) Truncate(path string, size int64) error {
	if err := m.refuse(); err != nil {
		return err
	}
	if size < 0 {
		return os.ErrInvalid
	}
	ent := m.fs.db.Get(path)
	if ent == nil || ent.Deleted || ent.Dir {
		return os.ErrNotExist
	}
	pf, err := m.pending(path, ent.Mode, ent.Size, m.fetcher(ent))
	if err != nil {
		return err
	}
	if err := pf.Truncate(size); err != nil {
		pf.Close()
		return err
	}
	return pf.Close()
}

// readAll fetches an entry's whole content from a holder.
func (m *storeMutator) readAll(ent *Entry) ([]byte, error) {
	fetch := &streamFetcher{e: m.e, fs: m.fs}
	out := make([]byte, 0, ent.Size)
	for off := int64(0); off < ent.Size; {
		want := int64(maxReadChunk)
		if rest := ent.Size - off; want > rest {
			want = rest
		}
		buf, err := fetch.ReadRange(context.Background(), streamfs.Entry{Path: ent.Path, Blob: ent.Blob, Size: ent.Size}, off, want)
		if err != nil {
			return nil, err
		}
		if len(buf) == 0 {
			return nil, fmt.Errorf("%s: holder returned no bytes at offset %d", ent.Path, off)
		}
		out = append(out, buf...)
		off += int64(len(buf))
	}
	return out, nil
}

func (m *storeMutator) pending(path string, mode uint32, base int64, fetch func(off, length int64) ([]byte, error)) (streamfs.PendingFile, error) {
	dir := queueBytesDir(m.e.dataDir, m.fs.name)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	f, err := os.CreateTemp(dir, "new-*")
	if err != nil {
		return nil, err
	}
	return &pendingFile{
		e:     m.e,
		fs:    m.fs,
		path:  path,
		mode:  mode,
		f:     f,
		id:    filepath.Base(f.Name()),
		base:  base,
		size:  base,
		fetch: fetch,
	}, nil
}

func (m *storeMutator) Remove(path string) error {
	if err := m.refuse(); err != nil {
		return err
	}
	ent := m.fs.db.Get(path)
	if ent == nil || ent.Deleted {
		return os.ErrNotExist
	}
	m.fs.scanMu.Lock()
	defer m.fs.scanMu.Unlock()
	m.fs.qx.pathMu.Lock()
	defer m.fs.qx.pathMu.Unlock()
	if ent = m.fs.db.Get(path); ent == nil || ent.Deleted {
		return os.ErrNotExist
	}
	vv := m.e.nextVV(m.fs, ent)
	tomb := &Entry{Path: path, Deleted: true, Dir: ent.Dir, VV: vv}
	_ = m.fs.db.Put(tomb)
	if err := m.e.enqueueOp(m.fs, queueOp{ID: newOpID(m.e), Op: opDelete, Path: path, VV: vv}); err != nil {
		return err
	}
	go m.e.broadcastIndex(m.fs, []*Entry{tomb})
	go m.e.flushQueue(m.fs)
	return nil
}

func (m *storeMutator) MakeDir(path string, mode uint32) error {
	if err := m.refuse(); err != nil {
		return err
	}
	m.fs.scanMu.Lock()
	defer m.fs.scanMu.Unlock()
	vv := m.e.nextVV(m.fs, m.fs.db.Get(path))
	ent := &Entry{Path: path, Dir: true, Mode: mode, VV: vv}
	_ = m.fs.db.Put(ent)
	if err := m.e.enqueueOp(m.fs, queueOp{ID: newOpID(m.e), Op: opMkdir, Path: path, Mode: mode, VV: vv}); err != nil {
		return err
	}
	go m.e.broadcastIndex(m.fs, []*Entry{ent})
	go m.e.flushQueue(m.fs)
	return nil
}

func (m *storeMutator) RemoveDir(path string) error {
	if err := m.refuse(); err != nil {
		return err
	}
	m.fs.scanMu.Lock()
	defer m.fs.scanMu.Unlock()
	if len(m.fs.childrenLocked(path)) > 0 {
		return os.ErrExist
	}
	vv := m.e.nextVV(m.fs, m.fs.db.Get(path))
	tomb := &Entry{Path: path, Dir: true, Deleted: true, VV: vv}
	_ = m.fs.db.Put(tomb)
	if err := m.e.enqueueOp(m.fs, queueOp{ID: newOpID(m.e), Op: opRmdir, Path: path, VV: vv}); err != nil {
		return err
	}
	go m.e.broadcastIndex(m.fs, []*Entry{tomb})
	go m.e.flushQueue(m.fs)
	return nil
}

// moveQueuedPut changes the destination of a pending upload without making a
// second blob op. GIO writes to .goutputstream-* then renames it; the holder
// must pull those bytes under the final name, not receive a rename frame for a
// path it never materialised.
func (e *Engine) moveQueuedPut(fs *StoreState, from, to string, vv map[string]uint64) bool {
	fs.queueMu.Lock()
	defer fs.queueMu.Unlock()
	idx := -1
	for i := range fs.queue {
		if fs.queue[i].Op == opPut && fs.queue[i].Path == from {
			idx = i
			break
		}
	}
	if idx < 0 {
		return false
	}
	op := fs.queue[idx]
	op.Path = to
	op.VV = vv
	// Appended, the changed record replaces the old one when the queue is
	// read back. If it cannot be written the move is not made: the caller
	// queues a rename instead.
	if err := e.appendQueue(fs, []queueOp{op}, true); err != nil {
		e.logger.Warn("move a queued upload", "store", fs.name, "from", from, "to", to, "error", err)
		return false
	}
	// The upload now happens after everything already queued - including
	// the delete of the file it replaces, which must not reach the holder
	// after it - and an older upload of the destination is superseded.
	fs.queue = append(slices.Delete(fs.queue, idx, idx+1), op)
	delete(fs.qx.sent, op.ID)
	fs.qx.ver++
	e.compactLocked(fs)
	return true
}

func (m *storeMutator) Rename(from, to string) error {
	if err := m.refuse(); err != nil {
		return err
	}
	if from == "" || to == "" || !validRel(from) || !validRel(to) {
		return os.ErrInvalid
	}
	m.fs.scanMu.Lock()
	defer m.fs.scanMu.Unlock()
	m.fs.qx.pathMu.Lock()
	defer m.fs.qx.pathMu.Unlock()
	var entries []*Entry
	for _, ent := range m.fs.db.Under(from) {
		if !ent.Deleted {
			entries = append(entries, ent)
		}
	}
	if len(entries) == 0 {
		return os.ErrNotExist
	}
	dst := m.fs.db.Get(to)
	var replaced map[string]uint64
	if dst != nil && !dst.Deleted {
		if len(entries) != 1 || entries[0].Dir || dst.Dir {
			return os.ErrExist
		}
		vv := m.e.nextVV(m.fs, dst)
		replaced = vv
		_ = m.fs.db.Put(&Entry{Path: to, Deleted: true, Dir: dst.Dir, VV: vv})
		if err := m.e.enqueueOp(m.fs, queueOp{ID: newOpID(m.e), Op: opDelete, Path: to, VV: vv}); err != nil {
			return err
		}
	}
	moves := make([]renameMove, 0, len(entries))
	for _, ent := range entries {
		newPath := to
		if ent.Path != from {
			newPath = to + strings.TrimPrefix(ent.Path, from)
		}
		vv := m.e.nextVV(m.fs, ent)
		if replaced != nil {
			// The file that lands on the destination replaces what was
			// there: it must dominate that version, or a holder sees two
			// concurrent writes and parks one of them as a conflict.
			vv = MergeVV(vv, replaced)
		}
		old := &Entry{Path: ent.Path, Deleted: true, Dir: ent.Dir, VV: vv}
		moved := ent.clone()
		moved.Path = newPath
		moved.VV = vv
		_ = m.fs.db.Put(old)
		_ = m.fs.db.Put(moved)
		// A handle still open on the path follows it: it commits under the
		// new name when it is released. One that created the file holds its
		// only bytes, so there is nothing at the old name for a holder to
		// move - only the empty placeholder to delete.
		_, created := m.e.repathPending(m.fs, ent.Path, newPath)
		if m.e.moveQueuedPut(m.fs, ent.Path, newPath, vv) || created {
			if err := m.e.enqueueOp(m.fs, queueOp{ID: newOpID(m.e), Op: opDelete, Path: ent.Path, VV: vv}); err != nil {
				return err
			}
		} else {
			moves = append(moves, renameMove{From: ent.Path, Old: old, New: moved})
		}
	}
	if len(moves) > 0 {
		if err := m.e.enqueueOp(m.fs, queueOp{ID: newOpID(m.e), Op: opRename, Path: from, To: to, Dir: true, Moves: moves, VV: moves[0].New.VV}); err != nil {
			return err
		}
	}
	go m.e.flushQueue(m.fs)
	return nil
}

// childrenLocked lists a directory's immediate children. Callers hold scanMu.
func (fs *StoreState) childrenLocked(dir string) []string {
	seen := map[string]bool{}
	prefix := dir + "/"
	for _, ent := range fs.db.Under(dir) {
		if ent.Deleted || ent.Path == dir {
			continue
		}
		name, _, _ := strings.Cut(ent.Path[len(prefix):], "/")
		if name != "" {
			seen[name] = true
		}
	}
	out := make([]string, 0, len(seen))
	for name := range seen {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// nextVV is this machine's next version of a path: the version it saw, plus
// its own counter. A change that ignored what it was built on would be
// concurrent with it, and two concurrent versions resolve as a conflict rather
// than as the edit the user actually made.
func (e *Engine) nextVV(fs *StoreState, prev *Entry) map[string]uint64 {
	var seen map[string]uint64
	if prev != nil {
		seen = prev.VV
	}
	return MergeVV(seen, map[string]uint64{e.me: fs.db.Next()})
}

// admits reports whether the offline queue can hold a file of this size. A
// capped queue refuses at write time rather than at close: close(2) has no way
// to report a FUSE release, and the caller can still act on a failed write.
func (p *pendingFile) admits(size int64) error {
	if size < 0 {
		size = 0
	}
	policy := p.e.offlinePolicy(p.fs)
	if policy != config.StreamQueueCapped || p.fs.offline == nil {
		return nil
	}
	limit := p.fs.offline.MaxBytes
	p.fs.queueMu.Lock()
	queued := p.fs.queuedBytes
	p.fs.queueMu.Unlock()
	if queued+size > limit {
		return fmt.Errorf("offline queue for store %q is full (%d of %d bytes): %w",
			p.fs.name, queued+size, limit, syscall.ENOSPC)
	}
	return nil
}

// markTouched marks a fresh handle as a change on its own: creating an empty
// file, or truncating one, must reach the journal even if nothing is written.
func markTouched(pf streamfs.PendingFile) {
	if p, ok := pf.(*pendingFile); ok {
		p.touched = true
	}
}

// span is one range of bytes written through a handle.
type span struct{ start, end int64 }

// addSpan records a written range, merging it with what is already there.
func (p *pendingFile) addSpan(start, end int64) {
	out := append(p.spans, span{start, end})
	sort.Slice(out, func(i, j int) bool { return out[i].start < out[j].start })
	merged := out[:0]
	for _, s := range out {
		if n := len(merged); n > 0 && s.start <= merged[n-1].end {
			if s.end > merged[n-1].end {
				merged[n-1].end = s.end
			}
			continue
		}
		merged = append(merged, s)
	}
	p.spans = merged
}

// writtenWithin reports how much of [off, off+n) this handle wrote at the
// start of the range: the caller reads that part from the scratch file and
// asks the holder for the rest.
func (p *pendingFile) writtenWithin(off, n int64) int64 {
	for _, s := range p.spans {
		if off < s.start || off >= s.end {
			continue
		}
		if end := s.end - off; end < n {
			return end
		}
		return n
	}
	return 0
}

// newOpID makes a queue id that sorts by creation time and never collides.
func newOpID(e *Engine) string {
	e.mu.Lock()
	n := e.nextReadIDLocked()
	e.mu.Unlock()
	return fmt.Sprintf("%d-%d", time.Now().UnixNano(), n)
}

// pendingFile is a file being written through the mount.
type pendingFile struct {
	e    *Engine
	fs   *StoreState
	path string
	mode uint32
	f    *os.File
	id   string
	mu   sync.Mutex
	// base is the size the file already had on the holder; spans are the
	// ranges this handle wrote; fetch reads the untouched bytes from a
	// holder. Reads fall through to the holder instead of seeding a local
	// copy, so opening a 20 GiB video read-write costs nothing.
	base  int64
	spans []span
	size  int64
	fetch func(off, length int64) ([]byte, error)
	done  bool
	// dirty records bytes written through this handle; touched records that
	// the handle changed the file at all (created it, or truncated it). They
	// are separate because a handle that wrote nothing yet must still commit
	// when it created the file: `touch` is a change with no write.
	dirty   bool
	touched bool
	// created records that this handle brought the path into existence, so a
	// refused write can take the journal entry back out again.
	created bool
	// failed records that a write was refused; close must then undo the
	// create instead of committing the empty file.
	failed bool
}

// ReadAt serves the bytes this handle overwrote from the scratch file and the
// rest from the holder, so a file opened for writing still reads correctly
// without downloading it first.
func (p *pendingFile) ReadAt(b []byte, off int64) (int, error) {
	p.mu.Lock()
	size, base, fetch := p.size, p.base, p.fetch
	p.mu.Unlock()
	if off >= size {
		return 0, io.EOF
	}
	want := int64(len(b))
	if rest := size - off; want > rest {
		want = rest
	}
	var done int64
	for done < want {
		cur := off + done
		p.mu.Lock()
		local := p.writtenWithin(cur, want-done)
		p.mu.Unlock()
		if local > 0 {
			n, err := p.f.ReadAt(b[done:done+local], cur)
			done += int64(n)
			if err != nil && err != io.EOF {
				return int(done), err
			}
			if int64(n) < local {
				return int(done), io.EOF
			}
			continue
		}
		// A gap: the holder holds it, unless it is past the original end
		// (growth), where the bytes are zeroes by definition.
		next := want - done
		p.mu.Lock()
		for _, s := range p.spans {
			if s.start > cur && s.start-cur < next {
				next = s.start - cur
			}
		}
		p.mu.Unlock()
		if fetchable := base - cur; fetchable < next {
			next = fetchable
		}
		if next <= 0 {
			// Beyond the original end: zero fill.
			for i := done; i < want; i++ {
				b[i] = 0
			}
			done = want
			break
		}
		buf, err := fetch(cur, next)
		if err != nil {
			return int(done), err
		}
		copy(b[done:done+next], buf)
		done += next
	}
	if done < int64(len(b)) {
		return int(done), io.EOF
	}
	return int(done), nil
}

func (p *pendingFile) WriteAt(b []byte, off int64) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.done {
		return 0, os.ErrClosed
	}
	if err := p.admits(off + int64(len(b))); err != nil {
		p.failed = true
		return 0, err
	}
	p.dirty = true
	n, err := p.f.WriteAt(b, off)
	if n > 0 {
		p.addSpan(off, off+int64(n))
		if end := off + int64(n); end > p.size {
			p.size = end
		}
	}
	return n, err
}

func (p *pendingFile) Sync() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.done {
		return nil
	}
	return p.f.Sync()
}

func (p *pendingFile) Truncate(size int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.done {
		return os.ErrClosed
	}
	if err := p.admits(size); err != nil {
		p.failed = true
		return err
	}
	p.dirty = true
	if err := p.f.Truncate(size); err != nil {
		return err
	}
	kept := p.spans[:0]
	for _, s := range p.spans {
		if s.start >= size {
			continue
		}
		if s.end > size {
			s.end = size
		}
		kept = append(kept, s)
	}
	p.spans = kept
	p.size = size
	return nil
}

// Close finalises the write: hash, journal, queue, and hand it to a holder if
// one is reachable. A policy refusal happens here, before anything is
// journalled, so the tree never claims a file the queue rejected.
func (p *pendingFile) Close() error {
	defer p.e.unregisterPending(p)
	err := p.close()
	if err != nil {
		// Release has no error path back to the writer, so a refusal is
		// recorded here rather than lost.
		p.e.logger.Warn("write not accepted", "store", p.fs.name, "path", p.path, "error", err)
	}
	return err
}

func (p *pendingFile) close() error {
	p.mu.Lock()
	if p.done {
		p.mu.Unlock()
		return nil
	}
	// Close is the release: nothing writes through the handle after it.
	// Every path that does not commit the scratch file removes it, or each
	// read-write open left a descriptor and a file in the queue directory.
	p.done = true
	changed := p.dirty || p.touched
	failed := p.failed
	p.mu.Unlock()

	if !changed {
		// Nothing changed through this handle: closing it must not add a
		// version.
		p.discard()
		return nil
	}
	if failed {
		p.discard()
		p.dropCreatedEntry()
		return nil
	}

	// A delete that arrived while the file was open (touch; rm in one shell)
	// must not be undone by the commit that follows it. (Checked again at
	// the commit; this spares fetching the bytes of a file already gone.)
	if p.deletedWhileOpen() {
		p.discard()
		return nil
	}
	return p.recordCurrent()
}

// deletedWhileOpen reports whether the handle's path was removed after it
// was opened. A rename tombstones the old path and moves the handle in one
// step under pathMu, so a renamed handle never looks deleted.
func (p *pendingFile) deletedWhileOpen() bool {
	p.fs.qx.pathMu.Lock()
	defer p.fs.qx.pathMu.Unlock()
	return p.deletedLocked()
}

// deletedLocked is deletedWhileOpen for callers holding pathMu.
func (p *pendingFile) deletedLocked() bool {
	p.mu.Lock()
	path := p.path
	p.mu.Unlock()
	cur := p.fs.db.Get(path)
	return cur != nil && cur.Deleted
}

// discard closes the scratch file and removes it.
func (p *pendingFile) discard() {
	_ = p.f.Close()
	_ = os.Remove(p.f.Name())
}

// fillGaps writes the bytes from the holder into every range this handle did
// not overwrite, so one commit produces the complete file.
func (p *pendingFile) fillGaps(f *os.File) error {
	p.mu.Lock()
	size, base, fetch := p.size, p.base, p.fetch
	spans := append([]span(nil), p.spans...)
	p.mu.Unlock()
	if fetch == nil || base <= 0 {
		return nil
	}
	cur := int64(0)
	for _, s := range spans {
		if s.start > cur {
			if err := p.copyFromHolder(f, fetch, cur, min64(s.start, base)); err != nil {
				return err
			}
		}
		if s.end > cur {
			cur = s.end
		}
	}
	if cur < min64(size, base) {
		if err := p.copyFromHolder(f, fetch, cur, min64(size, base)); err != nil {
			return err
		}
	}
	return nil
}

func (p *pendingFile) copyFromHolder(f *os.File, fetch func(int64, int64) ([]byte, error), start, end int64) error {
	for start < end {
		want := end - start
		if want > maxReadChunk {
			want = maxReadChunk
		}
		buf, err := fetch(start, want)
		if err != nil {
			return err
		}
		if len(buf) == 0 {
			return fmt.Errorf("%s: holder returned no bytes at offset %d", p.path, start)
		}
		if _, err := f.WriteAt(buf, start); err != nil {
			return err
		}
		start += int64(len(buf))
	}
	return nil
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// dropCreatedEntry removes the journal entry a refused create left behind: the
// file must not appear in the tree when its bytes were never accepted.
func (p *pendingFile) dropCreatedEntry() {
	if !p.created {
		return
	}
	p.fs.qx.pathMu.Lock()
	defer p.fs.qx.pathMu.Unlock()
	p.mu.Lock()
	path := p.path
	p.mu.Unlock()
	p.fs.queueMu.Lock()
	tomb := &Entry{
		Path:    path,
		Deleted: true,
		VV:      p.e.nextVV(p.fs, p.fs.db.Get(path)),
	}
	_ = p.fs.db.Put(tomb)
	p.fs.queueMu.Unlock()
}

// recordCurrent journals and queues the bytes currently in the scratch file.
func (p *pendingFile) recordCurrent() error {
	p.mu.Lock()
	f := p.f
	p.mu.Unlock()

	// The scratch file holds only what this handle wrote. Fill the rest from
	// the holder so the queued copy is the whole file.
	if err := p.fillGaps(f); err != nil {
		f.Close()
		os.Remove(f.Name())
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(f.Name())
		return err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		os.Remove(f.Name())
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(f.Name())
		return err
	}
	if policy := p.e.offlinePolicy(p.fs); policy == config.StreamQueueCapped {
		limit := p.fs.offline.MaxBytes
		p.fs.queueMu.Lock()
		queued := p.fs.queuedBytes
		p.fs.queueMu.Unlock()
		if queued+info.Size() > limit {
			os.Remove(f.Name())
			p.e.logger.Warn("offline queue full; write refused",
				"store", p.fs.name, "path", p.path, "queued", queued, "size", info.Size(), "limit", limit)
			p.dropCreatedEntry()
			return fmt.Errorf("offline queue for store %q is full (%d of %d bytes): %w",
				p.fs.name, queued+info.Size(), limit, os.ErrPermission)
		}
	}

	sum, err := store.SumFile(f.Name())
	if err != nil {
		os.Remove(f.Name())
		return err
	}

	// The scratch file becomes the queued copy, so the holder can pull
	// exactly these bytes.
	queued := filepath.Join(queueBytesDir(p.e.dataDir, p.fs.name), p.id)
	if err := os.Rename(f.Name(), queued); err != nil {
		os.Remove(f.Name())
		return err
	}

	// Commit under the path the handle has now: a rename while it was open
	// moved it. pathMu keeps a rename from slipping between reading the path
	// and journalling it.
	p.fs.qx.pathMu.Lock()
	defer p.fs.qx.pathMu.Unlock()
	defer p.e.unregisterPending(p)
	if p.deletedLocked() {
		os.Remove(queued)
		return nil
	}
	p.mu.Lock()
	path := p.path
	p.mu.Unlock()
	mtime := time.Now().UnixNano()
	p.fs.queueMu.Lock()
	vv := p.e.nextVV(p.fs, p.fs.db.Get(path))
	ent := &Entry{
		Path:  path,
		Blob:  sum,
		Size:  info.Size(),
		MTime: mtime,
		Mode:  p.mode,
		VV:    vv,
	}
	op := queueOp{
		ID:    p.id,
		Op:    opPut,
		Path:  path,
		Blob:  sum,
		Size:  info.Size(),
		MTime: mtime,
		Mode:  p.mode,
		VV:    vv,
	}
	// The queue first: a journal entry whose op never reached the queue
	// file named bytes the next start would delete.
	if err := p.e.enqueue(p.fs, op); err != nil {
		p.fs.queueMu.Unlock()
		os.Remove(queued)
		return err
	}
	if err := p.fs.db.Put(ent); err != nil {
		// Queued all the same: the holder takes it and the journal learns
		// it back from the holder's index.
		p.e.logger.Warn("journal a queued write", "store", p.fs.name, "path", path, "error", err)
	}
	p.fs.queueMu.Unlock()

	go p.e.broadcastIndex(p.fs, []*Entry{ent})
	go p.e.flushQueue(p.fs)
	return nil
}

// offlinePolicy is the store's queue policy, defaulting to unbounded.
func (e *Engine) offlinePolicy(fs *StoreState) config.StreamPolicy {
	if fs.offline == nil || fs.offline.Policy == "" {
		return config.StreamQueueUnbounded
	}
	return fs.offline.Policy
}
