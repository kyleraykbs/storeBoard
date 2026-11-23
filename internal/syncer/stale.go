package syncer

import (
	"path/filepath"
	"strings"
	"time"
)

// staleAtomicAge is deliberately conservative: a GIO temporary output older
// than this is no longer an active save. Cleanup tombstones it rather than
// unlinking it, so complete bytes remain recoverable in holder trash.
const staleAtomicAge = time.Hour

func (e *Engine) cleanupStaleAtomicFiles() {
	now := time.Now()
	for _, fs := range e.stores {
		for _, ent := range fs.db.All() {
			if ent.Deleted || !strings.HasPrefix(filepath.Base(ent.Path), ".goutputstream-") || ent.MTime == 0 {
				continue
			}
			if now.Sub(time.Unix(0, ent.MTime)) < staleAtomicAge || e.hasPending(fs, ent.Path) {
				continue
			}
			e.cleanupStaleAtomic(fs, ent)
		}
	}
}

func (e *Engine) cleanupStaleAtomic(fs *StoreState, ent *Entry) {
	vv := e.nextVV(fs, ent)
	tomb := &Entry{Path: ent.Path, Deleted: true, Dir: ent.Dir, VV: vv}
	if fs.stream {
		fs.queueMu.Lock()
		// An upload of the file that no holder has taken yet holds its only
		// bytes: tombstoning it would drop them (a delete supersedes a
		// queued put). Leave it until a holder has it; the holder then keeps
		// the removed copy in its trash.
		for _, op := range fs.queue {
			if op.Op == opPut && op.Path == ent.Path {
				fs.queueMu.Unlock()
				e.logger.Debug("stale atomic-save artifact still uploading; keeping it", "store", fs.name, "path", ent.Path)
				return
			}
		}
		if err := fs.db.Put(tomb); err != nil {
			fs.queueMu.Unlock()
			return
		}
		if err := e.enqueue(fs, queueOp{ID: newOpID(e), Op: opDelete, Path: ent.Path, VV: vv}); err != nil {
			fs.queueMu.Unlock()
			return
		}
		fs.queueMu.Unlock()
		e.broadcastIndex(fs, []*Entry{tomb})
		go e.flushQueue(fs)
		e.logger.Info("removed stale atomic-save artifact", "store", fs.name, "path", ent.Path)
		return
	}
	if err := e.applyDelete(fs, ent); err != nil {
		e.logger.Warn("stale atomic-save cleanup failed", "store", fs.name, "path", ent.Path, "error", err)
		return
	}
	if err := fs.db.Put(tomb); err != nil {
		return
	}
	e.broadcastIndex(fs, []*Entry{tomb})
	e.logger.Info("removed stale atomic-save artifact", "store", fs.name, "path", ent.Path)
}
