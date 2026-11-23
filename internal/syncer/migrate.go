package syncer

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	iofs "io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"codeberg.org/kyleraykbs/storeboard/internal/config"
	"codeberg.org/kyleraykbs/storeboard/internal/store"
	"codeberg.org/kyleraykbs/storeboard/internal/streamfs"
)

// Changing a store's sync mode.
//
// local -> stream. The canonical dir holds the user's files; a stream store's
// canonical dir is a mount, and mounting over the files would hide them -
// never uploaded, never visible. Instead, at startup and before mounting,
// every file is moved (renamed, never copied or deleted) into a staging dir in
// the data dir, with a manifest that records which version of the path each
// one is. The mount then comes up at once and the staged files stay readable
// through it. In the background each staged file is settled with a holder:
//
//   - a holder has the same bytes (asked by content hash): the local copy
//     goes to the trash, kept for the ordinary retention period;
//   - no holder has them and they are still the store's current version:
//     they become a queued upload, exactly like a write through the mount;
//   - the path has since moved on to a version that knew this one: it was
//     superseded, and the copy goes to the trash;
//   - anything else (a concurrent change): kept, as a conflict copy upload.
//
// Nothing is removed until a holder has it or the trash has it. A crash at any
// point resumes from the manifest and the staging dir.
//
// stream -> local. The only bytes a streamed store has here are queued
// uploads. At startup, before the replica first scans, every upload that is
// still the current version is written into the canonical dir as the local
// file it is; the rest of the store then arrives as any replica's does. A
// store whose contents would not fit on the disk is left streamed, and doctor
// says so: filling the disk is worse than not switching.

// modePath records the mode a store last ran in, so a change is noticed.
func modePath(dataDir, storeName string) string {
	return filepath.Join(dataDir, "mode", storeName)
}

func readMode(dataDir, storeName string) string {
	b, err := os.ReadFile(modePath(dataDir, storeName))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

func writeMode(dataDir, storeName, mode string) error {
	p := modePath(dataDir, storeName)
	if readMode(dataDir, storeName) == mode {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		return err
	}
	tmp := p + ".new"
	if err := os.WriteFile(tmp, []byte(mode+"\n"), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, p)
}

func stagingDir(dataDir, storeName string) string {
	return filepath.Join(dataDir, "migrating", storeName)
}

func manifestPath(dataDir, storeName string) string {
	return filepath.Join(dataDir, "migrating", storeName+".ndjson")
}

// migItem is one staged file: which version of its path it is.
type migItem struct {
	Path  string            `json:"path"`
	Blob  string            `json:"blob"`
	Size  int64             `json:"size"`
	MTime int64             `json:"mtime"`
	Mode  uint32            `json:"mode"`
	VV    map[string]uint64 `json:"vv"`
}

// migration is a local->stream move in progress.
type migration struct {
	mu    sync.Mutex
	items map[string]*migItem // by path
	// blocked is why a requested mode change was not made (doctor).
	blocked string
}

func (m *migration) pending() (n int, bytes int64) {
	if m == nil {
		return 0, 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, it := range m.items {
		n++
		bytes += it.Size
	}
	return n, bytes
}

func (m *migration) byBlob(sum string) *migItem {
	if m == nil || sum == "" {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, it := range m.items {
		if it.Blob == sum {
			return it
		}
	}
	return nil
}

func loadManifest(dataDir, storeName string) (map[string]*migItem, error) {
	f, err := os.Open(manifestPath(dataDir, storeName))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()
	items := map[string]*migItem{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		var it migItem
		if json.Unmarshal(sc.Bytes(), &it) == nil && it.Path != "" {
			items[it.Path] = &it
		}
	}
	return items, sc.Err()
}

func writeManifest(dataDir, storeName string, items map[string]*migItem) error {
	p := manifestPath(dataDir, storeName)
	if len(items) == 0 {
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		return err
	}
	tmp := p + ".new"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	for _, it := range items {
		b, err := json.Marshal(it)
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
	return os.Rename(tmp, p)
}

// --- local -> stream ---------------------------------------------------------

// stageForStream moves a stream store's leftover local files out of the way
// of its mount. Called from prepareRoot, under scanMu, before mounting. It
// reports false when the files could not be staged; the caller then must not
// mount, or they would be hidden.
func (e *Engine) stageForStream(fs *StoreState) bool {
	if mountFSType(fs.root) != "" {
		return true // a mount (ours, stale or live): nothing of the user's under it to stage here
	}
	tops, err := os.ReadDir(fs.root)
	if err != nil || len(tops) == 0 {
		return true
	}
	staging := stagingDir(e.dataDir, fs.name)
	var stage []os.DirEntry
	for _, top := range tops {
		if strings.HasPrefix(top.Name(), tmpPrefix) {
			continue // our own scratch file from a replica-era transfer
		}
		if top.Type()&os.ModeSymlink != 0 && e.ownLink(filepath.Join(fs.root, top.Name())) {
			continue // a link to another store; settleLinks owns it
		}
		if _, err := os.Lstat(filepath.Join(staging, top.Name())); err == nil {
			e.logger.Error("cannot stage local files for streaming: the staging dir already has this name; leaving the store unmounted",
				"store", fs.name, "path", top.Name(), "staging", staging)
			e.noteMigrationBlocked(fs, "local files could not be moved aside for the mount: "+filepath.Join(staging, top.Name())+" already exists")
			return false
		}
		stage = append(stage, top)
	}
	if len(stage) == 0 {
		return true
	}

	// The manifest first, while the files are still where the journal
	// describes them: each file is the journal's version when its size and
	// mtime match, else a local edit the scanner had not recorded yet, which
	// becomes a new version now.
	items := map[string]*migItem{}
	if prev, err := loadManifest(e.dataDir, fs.name); err == nil {
		items = prev
		if items == nil {
			items = map[string]*migItem{}
		}
	}
	for _, top := range stage {
		root := filepath.Join(fs.root, top.Name())
		_ = filepath.WalkDir(root, func(p string, d iofs.DirEntry, err error) error {
			if err != nil || !d.Type().IsRegular() {
				return nil
			}
			info, err := d.Info()
			if err != nil {
				return nil
			}
			rel, err := filepath.Rel(fs.root, p)
			if err != nil || !validRel(filepath.ToSlash(rel)) {
				return nil
			}
			rel = filepath.ToSlash(rel)
			it, err := e.versionOf(fs, rel, p, info)
			if err != nil {
				e.logger.Warn("stage for streaming: cannot read file; it is moved aside and kept", "store", fs.name, "path", rel, "error", err)
				return nil
			}
			items[rel] = it
			return nil
		})
	}
	if err := writeManifest(e.dataDir, fs.name, items); err != nil {
		e.logger.Error("cannot record staged files; leaving the store unmounted", "store", fs.name, "error", err)
		e.noteMigrationBlocked(fs, "could not write the migration manifest: "+err.Error())
		return false
	}
	if err := os.MkdirAll(staging, 0o700); err != nil {
		e.noteMigrationBlocked(fs, "could not create "+staging+": "+err.Error())
		return false
	}
	for _, top := range stage {
		src := filepath.Join(fs.root, top.Name())
		if err := os.Rename(src, filepath.Join(staging, top.Name())); err != nil {
			// A different filesystem, most likely. Renames only: a copy of a
			// whole store must not happen behind the user's back.
			e.logger.Error("cannot move local files aside for streaming; leaving the store unmounted", "store", fs.name, "path", src, "error", err)
			e.noteMigrationBlocked(fs, fmt.Sprintf("could not move %s into %s: %v", src, staging, err))
			return false
		}
	}
	fs.migr = &migration{items: items}
	e.logger.Info("moved local files aside to stream the store; they upload or retire in the background",
		"store", fs.name, "files", len(items), "staging", staging)
	return true
}

// ownLink reports whether a symlink points into the storeboard root (a link
// the daemon made to another store).
func (e *Engine) ownLink(p string) bool {
	target, err := os.Readlink(p)
	if err != nil {
		return false
	}
	if !filepath.IsAbs(target) {
		target = filepath.Join(filepath.Dir(p), target)
	}
	rel, err := filepath.Rel(e.cfg.Root, target)
	return err == nil && !strings.HasPrefix(rel, "..")
}

// versionOf names the version a local file is, recording a new one in the
// journal when the file has changed since the last scan.
func (e *Engine) versionOf(fs *StoreState, rel, p string, info os.FileInfo) (*migItem, error) {
	ent := fs.db.Get(rel)
	mtime := info.ModTime().UnixNano()
	mode := uint32(info.Mode().Perm())
	if ent != nil && !ent.Deleted && !ent.Dir && ent.Blob != "" && ent.Size == info.Size() && ent.MTime == mtime {
		return &migItem{Path: rel, Blob: ent.Blob, Size: ent.Size, MTime: mtime, Mode: mode, VV: ent.VV}, nil
	}
	sum, err := store.SumFile(p)
	if err != nil {
		return nil, err
	}
	if ent != nil && !ent.Deleted && ent.Blob == sum {
		return &migItem{Path: rel, Blob: sum, Size: info.Size(), MTime: mtime, Mode: mode, VV: ent.VV}, nil
	}
	vv := e.nextVV(fs, ent)
	if err := fs.db.Put(&Entry{Path: rel, Blob: sum, Size: info.Size(), MTime: mtime, Mode: mode, VV: vv}); err != nil {
		return nil, err
	}
	return &migItem{Path: rel, Blob: sum, Size: info.Size(), MTime: mtime, Mode: mode, VV: vv}, nil
}

// migrationBlocked reports that the local files could not be staged, so the
// store must not be mounted over them.
func (fs *StoreState) migrationBlocked() bool {
	if fs.migr == nil {
		return false
	}
	fs.migr.mu.Lock()
	defer fs.migr.mu.Unlock()
	return fs.migr.blocked != ""
}

func (e *Engine) noteMigrationBlocked(fs *StoreState, why string) {
	if fs.migr == nil {
		fs.migr = &migration{items: map[string]*migItem{}}
	}
	fs.migr.mu.Lock()
	fs.migr.blocked = why
	fs.migr.mu.Unlock()
}

// readStaged serves a range of a file staged for streaming: until it is
// settled, this machine has the only copy it can be sure of.
func (e *Engine) readStaged(fs *StoreState, sum string, offset, length int64) ([]byte, bool, error) {
	it := fs.migr.byBlob(sum)
	if it == nil {
		return nil, false, nil
	}
	f, err := os.Open(filepath.Join(stagingDir(e.dataDir, fs.name), filepath.FromSlash(it.Path)))
	if err != nil {
		return nil, false, nil // settled between the lookup and the open
	}
	defer f.Close()
	buf := make([]byte, length)
	n, err := f.ReadAt(buf, offset)
	if err != nil && n < len(buf) && !errors.Is(err, os.ErrClosed) {
		if n == 0 {
			return nil, true, err
		}
	}
	return buf[:n], true, nil
}

// migrateEvery paces the settling passes while a migration is pending.
var migrateEvery = 15 * time.Second

func (e *Engine) migrateLoop(ctx context.Context, fs *StoreState) {
	if n, _ := fs.migr.pending(); n == 0 {
		return
	}
	t := time.NewTicker(migrateEvery)
	defer t.Stop()
	for {
		if fs.publish().State == stateAttached {
			e.migrateStep(ctx, fs)
		}
		if n, _ := fs.migr.pending(); n == 0 {
			e.finishMigration(fs)
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// probe reports whether some holder has a blob: yes, no, or unknown (no
// holder answered either way).
func (e *Engine) probe(ctx context.Context, fs *StoreState, it *migItem) (has, known bool) {
	for _, peer := range e.holdersFor(fs) {
		pctx, cancel := context.WithTimeout(ctx, readTimeout)
		_, err := e.readRangeFrom(pctx, peer, fs, streamfs.Entry{Path: it.Path, Blob: it.Blob, Size: it.Size}, 0, 1)
		cancel()
		if err == nil {
			return true, true
		}
		// Only a replica's "missing" is an answer: a streamed peer has no
		// bytes of its own to have.
		var refusal *holderRefusal
		if errors.As(err, &refusal) && e.peerIsReplica(peer, fs) {
			known = true
		}
	}
	return false, known
}

// migrateStep settles every staged file it can.
func (e *Engine) migrateStep(ctx context.Context, fs *StoreState) {
	fs.migr.mu.Lock()
	items := make([]*migItem, 0, len(fs.migr.items))
	for _, it := range fs.migr.items {
		items = append(items, it)
	}
	fs.migr.mu.Unlock()

	type result struct {
		it   *migItem
		done bool
	}
	work := make(chan *migItem)
	results := make(chan result)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for it := range work {
				results <- result{it, e.settle(ctx, fs, it)}
			}
		}()
	}
	go func() {
		defer close(work)
		for _, it := range items {
			select {
			case work <- it:
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() { wg.Wait(); close(results) }()
	settled := 0
	for r := range results {
		if r.done {
			fs.migr.mu.Lock()
			delete(fs.migr.items, r.it.Path)
			fs.migr.mu.Unlock()
			settled++
		}
	}
	if settled == 0 {
		return
	}
	fs.migr.mu.Lock()
	err := writeManifest(e.dataDir, fs.name, fs.migr.items)
	left := len(fs.migr.items)
	fs.migr.mu.Unlock()
	if err != nil {
		e.logger.Warn("record migration progress", "store", fs.name, "error", err)
	}
	e.logger.Info("migrating to stream", "store", fs.name, "settled", settled, "left", left)
}

// settle decides one staged file (see the top of this file). It reports
// whether the file has left the staging dir for good.
func (e *Engine) settle(ctx context.Context, fs *StoreState, it *migItem) bool {
	src := filepath.Join(stagingDir(e.dataDir, fs.name), filepath.FromSlash(it.Path))
	info, err := os.Stat(src)
	if err != nil {
		return os.IsNotExist(err) // already moved on (a crash after the move)
	}
	if info.Size() != it.Size {
		// Someone changed the staged copy by hand. Record what is there
		// now, so it is that which is settled.
		sum, err := store.SumFile(src)
		if err != nil {
			return false
		}
		it.Blob, it.Size, it.MTime = sum, info.Size(), info.ModTime().UnixNano()
	}
	has, known := e.probe(ctx, fs, it)
	if !known && !has {
		return false // no holder answered: try again later
	}
	if has {
		return e.retireStaged(fs, it, src, "a holder has the same bytes")
	}
	fs.queueMu.Lock()
	cur := fs.db.Get(it.Path)
	var op queueOp
	switch {
	case cur != nil && !cur.Deleted && cur.Blob == it.Blob:
		op = queueOp{Op: opPut, Path: it.Path, Blob: it.Blob, Size: it.Size, MTime: it.MTime, Mode: it.Mode, VV: cur.VV}
	case cur != nil && Dominates(cur.VV, it.VV):
		fs.queueMu.Unlock()
		return e.retireStaged(fs, it, src, "superseded by a newer version")
	default:
		// Changed concurrently elsewhere (or removed by a change that
		// never saw this version): keep it beside the winner.
		path := conflictPath(it.Path, it.Blob)
		vv := e.nextVV(fs, fs.db.Get(path))
		ent := &Entry{Path: path, Blob: it.Blob, Size: it.Size, MTime: it.MTime, Mode: it.Mode, VV: vv}
		if err := fs.db.Put(ent); err != nil {
			fs.queueMu.Unlock()
			return false
		}
		op = queueOp{Op: opPut, Path: path, Blob: it.Blob, Size: it.Size, MTime: it.MTime, Mode: it.Mode, VV: vv}
	}
	op.ID = newOpID(e)
	dst := filepath.Join(queueBytesDir(e.dataDir, fs.name), op.ID)
	if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
		fs.queueMu.Unlock()
		return false
	}
	if err := moveOrCopy(src, dst, info.Mode()); err != nil {
		fs.queueMu.Unlock()
		e.logger.Warn("queue a staged file", "store", fs.name, "path", it.Path, "error", err)
		return false
	}
	err = e.enqueue(fs, op)
	fs.queueMu.Unlock()
	if err != nil {
		e.logger.Warn("queue a staged file", "store", fs.name, "path", it.Path, "error", err)
		// The bytes are in the queue dir; put them back so nothing is lost.
		_ = moveOrCopy(dst, src, info.Mode())
		return false
	}
	go e.flushQueue(fs)
	return true
}

func (e *Engine) retireStaged(fs *StoreState, it *migItem, src, why string) bool {
	dst := filepath.Join(e.dataDir, "trash", fs.name, filepath.FromSlash(it.Path))
	if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
		return false
	}
	info, err := os.Lstat(src)
	if err != nil {
		return os.IsNotExist(err)
	}
	if err := moveOrCopy(src, dst, info.Mode()); err != nil {
		e.logger.Warn("retire a staged file", "store", fs.name, "path", it.Path, "error", err)
		return false
	}
	e.logger.Debug("staged file retired to the trash", "store", fs.name, "path", it.Path, "why", why)
	return true
}

// finishMigration removes what is left of the staging dir: empty directories
// only. Anything else (a symlink, a file the manifest never knew) stays, and
// doctor points at it.
func (e *Engine) finishMigration(fs *StoreState) {
	_ = writeManifest(e.dataDir, fs.name, nil)
	staging := stagingDir(e.dataDir, fs.name)
	removeEmptyDirs(staging)
	if _, err := os.Stat(staging); err == nil {
		e.logger.Warn("migration to stream finished; some items were left in the staging dir for you to review", "store", fs.name, "staging", staging)
		return
	}
	e.logger.Info("migration to stream finished", "store", fs.name)
}

// removeEmptyDirs removes dir and every directory under it that holds
// nothing else, deepest first.
func removeEmptyDirs(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, ent := range entries {
		if ent.IsDir() {
			removeEmptyDirs(filepath.Join(dir, ent.Name()))
		}
	}
	_ = os.Remove(dir) // fails, harmlessly, unless empty
}

// stagingLeftovers reports whether a finished migration left items behind.
func (e *Engine) stagingLeftovers(fs *StoreState) string {
	if n, _ := fs.migr.pending(); n > 0 {
		return ""
	}
	staging := stagingDir(e.dataDir, fs.name)
	if entries, err := os.ReadDir(staging); err == nil && len(entries) > 0 {
		return staging
	}
	return ""
}

// --- stream -> local ---------------------------------------------------------

// localModeFits decides, at startup, whether a store configured local that
// last ran streamed can switch: its contents must fit on the disk with room
// to spare. It returns why not, or "".
func (e *Engine) localModeFits(db *FileDB, root string) string {
	var need int64
	for _, ent := range db.All() {
		if !ent.Deleted && !ent.Dir && !ent.Local {
			need += ent.Size
		}
	}
	free, ok := freeBytes(root)
	if !ok {
		return ""
	}
	reserve := int64(1<<30) + need/50
	if free < need+reserve {
		return fmt.Sprintf("switching to local needs %s for the files plus %s to spare; %s is free on %s",
			humanSize(need), humanSize(reserve), humanSize(free), root)
	}
	return ""
}

// freeBytes reports the free space on the filesystem holding path (or its
// nearest existing parent).
func freeBytes(path string) (int64, bool) {
	for p := path; ; p = filepath.Dir(p) {
		var st syscall.Statfs_t
		if err := syscall.Statfs(p, &st); err == nil {
			return int64(st.Bavail) * int64(st.Bsize), true
		}
		if p == filepath.Dir(p) {
			return 0, false
		}
	}
}

// wasStreamed reports whether a store configured local last ran streamed.
func (e *Engine) wasStreamed(name string, db *FileDB, root string) bool {
	switch readMode(e.dataDir, name) {
	case config.SyncStream:
		return true
	case config.SyncLocal:
		return false
	}
	// No record (a build that kept none): queued uploads are proof, and so
	// is a journal that never held a single file here with nothing on disk.
	if _, err := os.Stat(queueIndexPath(e.dataDir, name)); err == nil {
		return true
	}
	if tops, err := os.ReadDir(root); err == nil && len(tops) > 0 {
		return false
	}
	any := false
	for _, ent := range db.All() {
		if ent.Deleted || ent.Dir {
			continue
		}
		if ent.Local {
			return false
		}
		any = true
	}
	return any
}

// unstream turns a formerly streamed store's queued uploads into the local
// files they are. Called from prepareRoot, under scanMu, before the first
// scan: the scanner then sees them as the versions the journal already has.
func (e *Engine) unstream(fs *StoreState) {
	if mountFSType(fs.root) == "fuse.storeboard" {
		if err := syscall.Unmount(fs.root, syscall.MNT_DETACH); err != nil {
			e.logger.Error("detach the old streamed mount", "store", fs.name, "path", fs.root, "error", err)
		}
	}
	if err := os.MkdirAll(fs.root, 0o755); err != nil {
		e.logger.Error("create store root", "store", fs.name, "error", err)
		return
	}
	if _, err := os.Stat(queueIndexPath(e.dataDir, fs.name)); err == nil {
		if err := e.loadQueue(fs); err != nil {
			e.logger.Error("read the streamed write queue; leaving it for the next start", "store", fs.name, "error", err)
			return
		}
		placed := 0
		for _, op := range fs.queue {
			if op.Op != opPut {
				continue // deletes, renames and directories are journal state already
			}
			cur := fs.db.Get(op.Path)
			if cur == nil || cur.Deleted || cur.Blob != op.Blob {
				continue // superseded
			}
			src := filepath.Join(queueBytesDir(e.dataDir, fs.name), op.ID)
			dst := filepath.Join(fs.root, filepath.FromSlash(op.Path))
			if _, err := os.Lstat(dst); err == nil {
				continue // something is there already; the scanner decides
			}
			if err := e.placeFile(fs, src, dst, op.Mode, op.MTime); err != nil {
				e.logger.Error("write a queued upload into the local copy; keeping the queue", "store", fs.name, "path", op.Path, "error", err)
				return
			}
			local := cur.clone()
			local.Local = true
			_ = fs.db.Put(local)
			placed++
		}
		fs.queue, fs.queuedBytes = nil, 0
		_ = os.Remove(queueIndexPath(e.dataDir, fs.name))
		_ = os.RemoveAll(queueBytesDir(e.dataDir, fs.name))
		e.logger.Info("switched a streamed store to local; queued writes are now local files", "store", fs.name, "files", placed)
	}
}

// placeFile moves src into the tree at dst with the given mode and mtime.
func (e *Engine) placeFile(fs *StoreState, src, dst string, mode uint32, mtime int64) error {
	if err := e.ensureDir(fs, filepath.Dir(dst)); err != nil {
		return err
	}
	tmp := filepath.Join(filepath.Dir(dst), tmpPrefix+filepath.Base(dst))
	_ = os.Remove(tmp)
	if mode == 0 {
		mode = 0o644
	}
	if err := moveOrCopy(src, tmp, os.FileMode(mode)); err != nil {
		return err
	}
	_ = os.Chmod(tmp, os.FileMode(mode).Perm())
	if mtime != 0 {
		t := time.Unix(0, mtime)
		_ = os.Chtimes(tmp, t, t)
	}
	return os.Rename(tmp, dst)
}
