package syncer

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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
)

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
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var op queueOp
		if err := json.Unmarshal(line, &op); err != nil {
			continue // a torn final line is expected after a crash
		}
		live[op.ID] = true
		fs.queue = append(fs.queue, op)
		fs.queuedBytes += op.Size
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
	return sc.Err()
}

// writeQueue persists the whole index: queues hold a handful of offline
// writes, and a rewrite is the only shape that cannot be torn by a crash.
func (e *Engine) writeQueue(fs *StoreState) error {
	index := queueIndexPath(e.dataDir, fs.name)
	if len(fs.queue) == 0 {
		if err := os.Remove(index); err != nil && !os.IsNotExist(err) {
			return err
		}
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
	return os.Rename(tmp, index)
}

// enqueue records one op, then tries to hand it to a holder straight away.
// Callers hold scanMu.
func (e *Engine) enqueue(fs *StoreState, op queueOp) error {
	fs.queue = append(fs.queue, op)
	fs.queuedBytes += op.Size
	if err := e.writeQueue(fs); err != nil {
		return err
	}
	return nil
}

// dropQueued forgets an op and its bytes. Callers hold scanMu.
func (e *Engine) dropQueued(fs *StoreState, id string) {
	kept := fs.queue[:0]
	for _, op := range fs.queue {
		if op.ID == id {
			fs.queuedBytes -= op.Size
			if op.Blob != "" {
				_ = os.Remove(filepath.Join(queueBytesDir(e.dataDir, fs.name), op.ID))
			}
			continue
		}
		kept = append(kept, op)
	}
	fs.queue = kept
	_ = e.writeQueue(fs)
}

// ackQueue drops the ops a peer has now seen. A remote entry that carries this
// machine's counter for the path is proof the change landed.
func (e *Engine) ackQueue(fs *StoreState, rem *Entry) {
	if !fs.stream || len(fs.queue) == 0 {
		return
	}
	fs.queueMu.Lock()
	defer fs.queueMu.Unlock()
	for _, op := range append([]queueOp(nil), fs.queue...) {
		switch op.Op {
		case opPut:
			if rem.Path == op.Path && rem.Blob == op.Blob && vvCovers(rem.VV, op.VV) {
				e.dropQueued(fs, op.ID)
			}
		case opDelete, opRmdir:
			if rem.Path == op.Path && rem.Deleted && vvCovers(rem.VV, op.VV) {
				e.dropQueued(fs, op.ID)
			}
		case opMkdir:
			if rem.Path == op.Path && !rem.Deleted && rem.Dir && vvCovers(rem.VV, op.VV) {
				e.dropQueued(fs, op.ID)
			}
		case opRename:
			if rem.Path == op.To && !rem.Deleted && vvCovers(rem.VV, op.VV) {
				e.dropQueued(fs, op.ID)
			}
		}
	}
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
func (e *Engine) flushQueue(fs *StoreState) {
	if !fs.stream || len(fs.queue) == 0 {
		return
	}
	peers := e.holdersFor(fs)
	if len(peers) == 0 {
		return
	}
	peer := peers[0]
	fs.queueMu.Lock()
	ops := append([]queueOp(nil), fs.queue...)
	fs.queueMu.Unlock()
	for _, op := range ops {
		if op.Op == opRename {
			fr := frame{Type: "rename", Store: fs.name, Path: op.Path, To: op.To, Entry: &Entry{Path: op.To, VV: op.VV}}
			if err := e.sendTo(peer, fr); err != nil {
				e.logger.Debug("flush rename", "store", fs.name, "path", op.Path, "error", err)
				return
			}
			continue
		}
		ent := &Entry{Path: op.Path, Blob: op.Blob, Size: op.Size, MTime: op.MTime, Mode: op.Mode, VV: op.VV}
		switch op.Op {
		case opDelete, opRmdir:
			ent = &Entry{Path: op.Path, Deleted: true, Dir: op.Op == opRmdir, VV: op.VV}
		case opMkdir:
			ent = &Entry{Path: op.Path, Dir: true, Mode: op.Mode, VV: op.VV}
		}
		if err := e.sendIndexTo(peer, fs, []*Entry{ent}); err != nil {
			e.logger.Debug("flush queue", "store", fs.name, "path", op.Path, "error", err)
			return
		}
	}
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
	pf, err := m.pending(path, mode, nil)
	if err != nil {
		return nil, err
	}
	markTouched(pf)
	return pf, nil
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
	if truncate {
		pf, err := m.pending(path, ent.Mode, nil)
		if err != nil {
			return nil, err
		}
		markTouched(pf)
		return pf, nil
	}
	body, err := m.readAll(ent)
	if err != nil {
		return nil, err
	}
	return m.pending(path, ent.Mode, body)
}

// Truncate sets a file's length on the holder's content: the kept prefix is
// fetched, the rest is dropped, and the result is queued as a new version.
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
	var keep []byte
	if size > 0 {
		body, err := m.readAll(ent)
		if err != nil {
			return err
		}
		if int64(len(body)) > size {
			body = body[:size]
		}
		keep = body
	}
	pending, err := m.pending(path, ent.Mode, keep)
	if err != nil {
		return err
	}
	if err := pending.Truncate(size); err != nil {
		return err
	}
	return pending.Close()
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

func (m *storeMutator) pending(path string, mode uint32, seed []byte) (streamfs.PendingFile, error) {
	dir := queueBytesDir(m.e.dataDir, m.fs.name)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	f, err := os.CreateTemp(dir, "new-*")
	if err != nil {
		return nil, err
	}
	if len(seed) > 0 {
		if _, err := f.Write(seed); err != nil {
			f.Close()
			os.Remove(f.Name())
			return nil, err
		}
	}
	id := filepath.Base(f.Name())
	return &pendingFile{e: m.e, fs: m.fs, path: path, mode: mode, f: f, id: id}, nil
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
	vv := m.e.nextVV(m.fs, ent)
	tomb := &Entry{Path: path, Deleted: true, Dir: ent.Dir, VV: vv}
	_ = m.fs.db.Put(tomb)
	if err := m.e.enqueue(m.fs, queueOp{ID: newOpID(m.e), Op: opDelete, Path: path, VV: vv}); err != nil {
		return err
	}
	m.e.refreshTree(m.fs)
	m.e.broadcastIndex(m.fs, []*Entry{tomb})
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
	if err := m.e.enqueue(m.fs, queueOp{ID: newOpID(m.e), Op: opMkdir, Path: path, Mode: mode, VV: vv}); err != nil {
		return err
	}
	m.e.refreshTree(m.fs)
	m.e.broadcastIndex(m.fs, []*Entry{ent})
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
	if err := m.e.enqueue(m.fs, queueOp{ID: newOpID(m.e), Op: opRmdir, Path: path, VV: vv}); err != nil {
		return err
	}
	m.e.refreshTree(m.fs)
	m.e.broadcastIndex(m.fs, []*Entry{tomb})
	go m.e.flushQueue(m.fs)
	return nil
}

func (m *storeMutator) Rename(from, to string) error {
	if err := m.refuse(); err != nil {
		return err
	}
	m.fs.scanMu.Lock()
	defer m.fs.scanMu.Unlock()
	ent := m.fs.db.Get(from)
	if ent == nil || ent.Deleted {
		return os.ErrNotExist
	}
	if dst := m.fs.db.Get(to); dst != nil && !dst.Deleted {
		return os.ErrExist
	}
	vv := m.e.nextVV(m.fs, ent)
	old := &Entry{Path: from, Deleted: true, Dir: ent.Dir, VV: vv}
	moved := ent.clone()
	moved.Path = to
	moved.VV = vv
	_ = m.fs.db.Put(old)
	_ = m.fs.db.Put(moved)
	if err := m.e.enqueue(m.fs, queueOp{ID: newOpID(m.e), Op: opRename, Path: from, To: to, VV: vv}); err != nil {
		return err
	}
	m.e.refreshTree(m.fs)
	// Only the rename frame: an index pair would race it, and a peer that saw
	// the tombstone first would trash the file before the move arrived. The
	// holder journals both paths and broadcasts them itself.
	go m.e.flushQueue(m.fs)
	return nil
}

// childrenLocked lists a directory's immediate children. Callers hold scanMu.
func (fs *StoreState) childrenLocked(dir string) []string {
	var out []string
	prefix := dir + "/"
	for _, ent := range fs.db.All() {
		if ent.Deleted || !strings.HasPrefix(ent.Path, prefix) {
			continue
		}
		rest := ent.Path[len(prefix):]
		name := rest
		if i := strings.IndexByte(rest, '/'); i >= 0 {
			name = rest[:i]
		}
		if name != "" {
			out = append(out, name)
		}
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

// newOpID makes a queue id that sorts by creation time and never collides.
func newOpID(e *Engine) string {
	e.mu.Lock()
	e.readSeq++
	n := e.readSeq
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
	done bool
	// dirty records bytes written through this handle; touched records that
	// the handle changed the file at all (created it, or truncated it). They
	// are separate because a handle that wrote nothing yet must still commit
	// when it created the file: `touch` is a change with no write.
	dirty   bool
	touched bool
}

func (p *pendingFile) WriteAt(b []byte, off int64) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.done {
		return 0, os.ErrClosed
	}
	if err := p.admits(off + int64(len(b))); err != nil {
		return 0, err
	}
	p.dirty = true
	return p.f.WriteAt(b, off)
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
		return err
	}
	p.dirty = true
	return p.f.Truncate(size)
}

// Close finalises the write: hash, journal, queue, and hand it to a holder if
// one is reachable. A policy refusal happens here, before anything is
// journalled, so the tree never claims a file the queue rejected.
func (p *pendingFile) Close() error {
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
	if !p.dirty && !p.touched {
		// Nothing changed through this handle: closing it must not add a
		// version. The scratch file goes away when the process does.
		p.mu.Unlock()
		return nil
	}
	p.done = true
	f := p.f
	p.mu.Unlock()

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
			return fmt.Errorf("offline queue for store %q is full (%d of %d bytes): %w",
				p.fs.name, queued+info.Size(), limit, os.ErrPermission)
		}
	}

	sum, err := store.SumFile(f.Name())
	if err != nil {
		os.Remove(f.Name())
		return err
	}
	mtime := time.Now().UnixNano()
	prev := p.fs.db.Get(p.path)
	vv := p.e.nextVV(p.fs, prev)
	ent := &Entry{
		Path:  p.path,
		Blob:  sum,
		Size:  info.Size(),
		MTime: mtime,
		Mode:  p.mode,
		VV:    vv,
	}

	p.fs.queueMu.Lock()
	_ = p.fs.db.Put(ent)
	op := queueOp{
		ID:    p.id,
		Op:    opPut,
		Path:  p.path,
		Blob:  sum,
		Size:  info.Size(),
		MTime: mtime,
		Mode:  p.mode,
		VV:    vv,
	}
	if err := p.e.enqueue(p.fs, op); err != nil {
		p.fs.queueMu.Unlock()
		os.Remove(f.Name())
		return err
	}
	p.fs.queueMu.Unlock()

	p.e.refreshTree(p.fs)
	p.e.broadcastIndex(p.fs, []*Entry{ent})
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
