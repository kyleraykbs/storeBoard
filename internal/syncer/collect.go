package syncer

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"codeberg.org/kyleraykbs/storeboard/internal/gatehouse"
	"codeberg.org/kyleraykbs/storeboard/internal/store"
	"codeberg.org/kyleraykbs/storeboard/pkg/api"
)

// recentBlob is the window in which an unreferenced blob is still kept:
// the transport runs alongside the engine, so a blob can be adopted
// moments before the collector looks for references.
var recentBlob = 10 * time.Minute

// Collect frees content-store blobs that nothing references any more: not a
// journal entry, not a kept version, not an orphan journal (a store removed
// from the config keeps its data — invariant 1 — so its journal on disk still
// counts as a reference). It runs at startup, when nothing is in flight, and
// is deliberately conservative: it refuses to act at all if any reference
// source cannot be read, because a blob it cannot account for is kept, and a
// blob it wrongly drops is a deleted file.
// keepSet is the set of blobs nothing on this machine may delete: every
// retained history version, any live entry whose file is missing or stale
// here, and every blob an orphan store's journal or history still names. It is
// the single judgement both the startup collector and the on-demand cleanup
// command share, so `storeboard cleanup` can never free something the engine
// would have kept.
func (e *Engine) keepSet() (map[string]bool, error) { return e.keepSetFor(nil) }

// keepSetFor is keepSet as it will be once an explicit cleanup's plan is
// applied: the versions it drops and the orphan logs it removes no longer
// count. A nil plan is the store as it is.
func (e *Engine) keepSetFor(plan *cleanupPlan) (map[string]bool, error) {
	keep := map[string]bool{}

	// Every retained version needs its bytes: that is the whole point of the
	// history, and the file itself may be gone.
	for _, fs := range e.stores {
		if fs.hist == nil {
			continue
		}
		drops := plan.dropsHistory(fs.name)
		for _, path := range fs.hist.Paths() {
			versions := fs.hist.Versions(path) // newest first
			var mask []bool
			if drops {
				chrono := make([]Version, len(versions))
				for i, v := range versions {
					chrono[len(versions)-1-i] = v
				}
				mask = dropMask(chrono, plan.cutoff, plan.current)
			}
			for i, v := range versions {
				if mask != nil && mask[len(versions)-1-i] {
					continue
				}
				if v.Blob != "" {
					keep[v.Blob] = true
				}
			}
		}
	}

	// A blob is also the only copy of a file this machine does not hold on
	// disk. Everything else is duplicated by the tree and can go.
	for _, fs := range e.stores {
		for _, ent := range fs.db.All() {
			if ent.Blob == "" || ent.Dir {
				continue
			}
			if !e.materialised(fs, ent) {
				keep[ent.Blob] = true
			}
		}
	}

	// Journals on disk that no loaded store claims: a store the config no
	// longer names still owns its bytes.
	journalDir := filepath.Join(e.dataDir, "journal")
	if entries, err := os.ReadDir(journalDir); err == nil {
		for _, ent := range entries {
			name := ent.Name()
			if filepath.Ext(name) != ".ndjson" {
				continue
			}
			store := name[:len(name)-len(".ndjson")]
			if _, loaded := e.byName[store]; loaded {
				continue
			}
			if plan != nil && plan.orphans[store] {
				continue
			}
			if err := refsFromJournal(filepath.Join(journalDir, name), keep); err != nil {
				return nil, fmt.Errorf("collector: cannot read a journal; keeping everything: %w", err)
			}
		}
	}

	// History files on disk: a store whose backup has since been turned off
	// keeps its versions reachable-by-re-enabling, so their blobs count as
	// referenced exactly like an orphan journal's.
	historyDir := filepath.Join(e.dataDir, "history")
	if entries, err := os.ReadDir(historyDir); err == nil {
		for _, ent := range entries {
			name := ent.Name()
			if filepath.Ext(name) != ".ndjson" {
				continue
			}
			store := name[:len(name)-len(".ndjson")]
			if plan != nil && plan.orphans[store] {
				continue
			}
			var err error
			if plan.dropsHistory(store) {
				err = refsFromHistoryPlanned(filepath.Join(historyDir, name), keep, plan)
			} else {
				err = refsFromHistory(filepath.Join(historyDir, name), keep)
			}
			if err != nil {
				return nil, fmt.Errorf("collector: cannot read a history; keeping everything: %w", err)
			}
		}
	}
	return keep, nil
}

func (e *Engine) Collect() (removed int, freed int64) {
	keep, err := e.keepSet()
	if err != nil {
		e.logger.Warn(err.Error())
		return 0, 0
	}

	// The store: <root>/<ab>/<cd>/<sum>, so the blob's name is its hash.
	storeDir := filepath.Join(e.dataDir, "store")
	for _, ab := range listDir(storeDir) {
		abPath := filepath.Join(storeDir, ab)
		for _, cd := range listDir(abPath) {
			cdPath := filepath.Join(abPath, cd)
			for _, sum := range listDir(cdPath) {
				if keep[sum] {
					continue
				}
				p := filepath.Join(cdPath, sum)
				fi, err := os.Stat(p)
				if err != nil || fi.IsDir() {
					continue
				}
				if time.Since(fi.ModTime()) < recentBlob {
					continue // possibly in flight: a peer may be mid-transfer
				}
				if err := os.Remove(p); err != nil {
					e.logger.Warn("collector: blob not removed", "blob", sum, "error", err)
					continue
				}
				removed++
				freed += fi.Size()
			}
			if rest, err := os.ReadDir(cdPath); err == nil && len(rest) == 0 {
				_ = os.Remove(cdPath)
			}
		}
		if rest, err := os.ReadDir(abPath); err == nil && len(rest) == 0 {
			_ = os.Remove(abPath)
		}
	}
	if removed > 0 {
		e.logger.Info("collected unreferenced content", "blobs", removed, "bytes", freed)
	}
	return removed, freed
}

// materialised reports whether a store's canonical file is present and looks
// like the entry's version: same kind, size and mtime. It is the evidence the
// collector needs before dropping the blob, and the same test the scanner uses
// to skip hashing an untouched file.
func (e *Engine) materialised(fs *StoreState, ent *Entry) bool {
	info, err := os.Stat(filepath.Join(fs.root, ent.Path))
	if err != nil || !info.Mode().IsRegular() {
		return false
	}
	return info.Size() == ent.Size && (ent.MTime == 0 || info.ModTime().UnixNano() == ent.MTime)
}

// refsFromJournal adds the blobs a journal file references.
func refsFromJournal(path string, refs map[string]bool) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 64*1024*1024)
	last := map[string]string{}
	for sc.Scan() {
		var rec struct {
			Entry *Entry `json:"entry"`
		}
		if json.Unmarshal(sc.Bytes(), &rec) != nil || rec.Entry == nil {
			continue
		}
		last[rec.Entry.Path] = rec.Entry.Blob
	}
	if err := sc.Err(); err != nil {
		return err
	}
	for _, blob := range last {
		if blob != "" {
			refs[blob] = true
		}
	}
	return nil
}

func listDir(dir string) []string {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.Name())
	}
	return out
}

// Restore writes a recorded version of a file back.
//
// With no version named it restores the newest recorded one — which is how a
// deleted file comes back, since the deletion is itself recorded with the
// content it removed. Restoring into the store is an ordinary local change:
// it becomes a new version, propagates like any other, and never replaces
// anything without keeping what was there (the journal keeps the old blob).
// With `to` set, the content is written there instead and the store is left
// alone. `to` must be absolute and clean, and the caller must be root or own
// the directory it lands in (see writeRestoreCopy): the daemon writes it as
// root, so without that rule any local user could have had it overwrite a
// file anywhere on the machine.
//
// uid is the requesting user, from the socket's peer credentials: root and
// the store's owner may restore, nobody else.
func (e *Engine) Restore(name string, uid int, path, blob string, at time.Time, to string) (api.RestoreResult, error) {
	fs := e.byName[name]
	if fs == nil {
		return api.RestoreResult{}, fmt.Errorf("unknown store %q", name)
	}
	if !validRel(path) {
		return api.RestoreResult{}, fmt.Errorf("unsafe path %q", path)
	}
	if err := e.checkOwner(fs, uid, gatehouse.Write, "restore from"); err != nil {
		return api.RestoreResult{}, err
	}
	if to != "" {
		if err := checkRestoreTarget(to); err != nil {
			return api.RestoreResult{}, err
		}
	}
	if fs.stream {
		if to != "" {
			// The holder would be the one writing `to`, on its own disk, as
			// the store's owner — a path named on another machine is not
			// one it can authorize, so holders refuse it outright.
			return api.RestoreResult{}, fmt.Errorf("store %q is streamed on this machine: restore --to is not available; restore into the store, then copy the file", name)
		}
		// The versions and the bytes both live on the holder; it performs the
		// restore and reports where the content landed.
		req := verbsRequest{Action: "restore", Path: path, Blob: blob, To: to}
		if !at.IsZero() {
			req.At = at.UnixNano()
		}
		var out api.RestoreResult
		if err := e.forwardVerbs(fs, req, &out); err != nil {
			return api.RestoreResult{}, err
		}
		return out, nil
	}
	if fs.hist == nil {
		return api.RestoreResult{}, fmt.Errorf("store %q keeps no history on this machine", name)
	}

	versions := fs.hist.Versions(path)
	if len(versions) == 0 {
		return api.RestoreResult{}, fmt.Errorf("no recorded versions of %q", path)
	}
	pick := versions[0]
	if blob != "" {
		found := false
		for _, v := range versions {
			if v.Blob == blob {
				pick, found = v, true
				break
			}
		}
		if !found {
			return api.RestoreResult{}, fmt.Errorf("no recorded version %q of %q", blob, path)
		}
	} else if !at.IsZero() {
		// The newest version at or before the moment asked for.
		found := false
		for _, v := range versions {
			if time.Unix(0, v.Time).Before(at.Add(time.Nanosecond)) {
				pick, found = v, true
				break
			}
		}
		if !found {
			return api.RestoreResult{}, fmt.Errorf("no recorded version of %q at or before %s", path, at.Format(time.RFC3339))
		}
	}
	if pick.Blob == "" {
		return api.RestoreResult{}, fmt.Errorf("the recorded version of %q holds no content", path)
	}
	if !e.store.Has(pick.Blob) {
		return api.RestoreResult{}, fmt.Errorf("the content of that version is no longer kept (blob %s)", pick.Blob[:8])
	}

	fs.scanMu.Lock()
	defer fs.scanMu.Unlock()

	if to != "" {
		if err := e.writeRestoreCopy(to, uid, pick.Blob, pick.Mode, pick.MTime); err != nil {
			return api.RestoreResult{}, err
		}
		return api.RestoreResult{Store: name, Path: to, Blob: pick.Blob, At: time.Unix(0, pick.Time).UTC(), Copied: true}, nil
	}

	dst := filepath.Join(fs.root, path)
	if err := e.ensureDir(fs, filepath.Dir(dst)); err != nil {
		return api.RestoreResult{}, err
	}
	// The file at the path may hold an edit the scanner has not recorded yet:
	// that is the user's newest work, and a restore must not be the thing
	// that destroys it. Park it first, exactly as a remote update would.
	before := statFile(dst)
	if before.exists {
		cur := fs.db.Get(path)
		if cur == nil || cur.Deleted || cur.Size != before.size || cur.MTime != before.mtime {
			sum, herr := store.SumFile(dst)
			if herr != nil {
				return api.RestoreResult{}, fmt.Errorf("cannot read the current %s to keep it: %w", path, herr)
			}
			if cur == nil || cur.Deleted || sum != cur.Blob {
				parkEnt, perr := e.parkLocked(fs, path)
				if perr != nil {
					return api.RestoreResult{}, fmt.Errorf("cannot park the unrecorded change to %s: %w", path, perr)
				}
				if parkEnt != nil {
					e.broadcastIndex(fs, []*Entry{parkEnt})
					e.logger.Warn("unrecorded change preserved as conflict copy before restore", "store", name, "path", path, "copy", parkEnt.Path)
				}
				before = statFile(dst)
			}
		} else if cur.Blob != "" && store.ValidSum(cur.Blob) {
			before.keep = cur.Blob
		}
	}
	if err := e.writeBlobTo(fs, dst, pick.Blob, pick.Mode, pick.MTime, &before); err != nil {
		return api.RestoreResult{}, err
	}
	// A restore is a local edit and builds on what this replica knew, exactly
	// like the scanner's: a fresh {me: n} vector is concurrent with a current
	// version written on another peer, so the restore would lose the next
	// conflict it was meant to win.
	var seen map[string]uint64
	if cur := fs.db.Get(path); cur != nil {
		seen = cur.VV
	}
	ent := &Entry{
		Path:  path,
		Blob:  pick.Blob,
		Size:  pick.Size,
		MTime: pick.MTime,
		Mode:  pick.Mode,
		VV:    MergeVV(seen, map[string]uint64{e.me: fs.db.Next()}),
		Local: true, // written to the canonical path just above
	}
	if err := fs.db.Put(ent); err != nil {
		return api.RestoreResult{}, err
	}
	e.record(fs, ent)
	e.broadcastIndex(fs, []*Entry{ent})
	e.logger.Info("restored a version", "store", name, "path", path,
		"blob", pick.Blob[:8], "at", time.Unix(0, pick.Time).UTC().Format(time.RFC3339))
	return api.RestoreResult{Store: name, Path: path, Blob: pick.Blob, At: time.Unix(0, pick.Time).UTC()}, nil
}

// checkRestoreTarget is the syntax half of the `to` rule: the daemon's
// working directory is its data dir, so a relative path would land among its
// own state, and an unclean one hides where it really points.
func checkRestoreTarget(to string) error {
	if !filepath.IsAbs(to) || filepath.Clean(to) != to || strings.ContainsRune(to, 0) {
		return fmt.Errorf("restore target %q must be an absolute, clean path", to)
	}
	if filepath.Dir(to) == to {
		return fmt.Errorf("restore target %q names no file", to)
	}
	return nil
}

// writeRestoreCopy writes a version to a path outside the store on behalf of
// uid, and only where uid could have written it themselves: the directory it
// lands in must already exist and belong to uid, and a file already at `to`
// must be a regular file of theirs. Root may write anywhere but still never
// replaces a directory, a symlink or a device. Nothing is created but the file
// itself, and nothing is chowned but the file: the old path went through
// ensureDir, which made every missing parent and handed the leaf directory to
// the store's owner — `--to /etc/x` would have given /etc away.
//
// Every step is relative to the directory as opened (an os.Root), so the
// ownership checked is the ownership of the directory written: swapping a
// path component for a symlink between check and write changes nothing.
func (e *Engine) writeRestoreCopy(to string, uid int, sum string, mode uint32, mtime int64) error {
	dir, base := filepath.Dir(to), filepath.Base(to)
	root, err := os.OpenRoot(dir)
	if err != nil {
		return fmt.Errorf("restore target directory: %w", err)
	}
	defer root.Close()
	dirInfo, err := root.Stat(".")
	if err != nil {
		return fmt.Errorf("restore target directory: %w", err)
	}
	dirStat, ok := dirInfo.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("restore target directory %s: cannot read its owner", dir)
	}
	if uid != 0 && int(dirStat.Uid) != uid {
		return fmt.Errorf("restore target directory %s belongs to uid %d: restore --to writes only into a directory you own", dir, dirStat.Uid)
	}
	ownerUID, ownerGID := uid, int(dirStat.Gid)
	if existing, err := root.Lstat(base); err == nil {
		if !existing.Mode().IsRegular() {
			return fmt.Errorf("restore target %s exists and is not a regular file", to)
		}
		st, ok := existing.Sys().(*syscall.Stat_t)
		if !ok {
			return fmt.Errorf("restore target %s: cannot read its owner", to)
		}
		if uid != 0 && int(st.Uid) != uid {
			return fmt.Errorf("restore target %s belongs to uid %d: it can only be replaced by its owner or root", to, st.Uid)
		}
		// Replacing a file keeps who it belonged to.
		ownerUID, ownerGID = int(st.Uid), int(st.Gid)
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("restore target %s: %w", to, err)
	}

	blob, err := e.store.Open(sum)
	if err != nil {
		return err
	}
	defer blob.Close()

	tmp := fmt.Sprintf("%s-restore-%d-%d", tmpPrefix, os.Getpid(), time.Now().UnixNano())
	f, err := root.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return fmt.Errorf("restore target: %w", err)
	}
	fail := func(err error) error {
		f.Close()
		root.Remove(tmp)
		return err
	}
	if _, err := io.Copy(f, blob); err != nil {
		return fail(err)
	}
	perm := os.FileMode(mode).Perm()
	if perm == 0 {
		perm = 0o644
	}
	if err := f.Chmod(perm); err != nil {
		return fail(err)
	}
	if ownerUID >= 0 && ownerUID != os.Geteuid() {
		if err := f.Chown(ownerUID, ownerGID); err != nil {
			e.logger.Warn("restore --to: chown", "path", to, "uid", ownerUID, "error", err)
		}
	}
	if err := f.Sync(); err != nil {
		return fail(err)
	}
	if err := f.Close(); err != nil {
		root.Remove(tmp)
		return err
	}
	if mtime != 0 {
		when := time.Unix(0, mtime)
		if err := root.Chtimes(tmp, when, when); err != nil {
			e.logger.Warn("restore --to: set mtime", "path", to, "error", err)
		}
	}
	if err := root.Rename(tmp, base); err != nil {
		root.Remove(tmp)
		return fmt.Errorf("restore target: %w", err)
	}
	return nil
}

// CheckOwner is checkOwner by store name, for callers outside the engine (the
// daemon's read-only verbs that still reveal a store's contents).
func (e *Engine) CheckOwner(name string, uid int, grant, what string) error {
	fs := e.byName[name]
	if fs == nil {
		return fmt.Errorf("unknown store %q", name)
	}
	return e.checkOwner(fs, uid, grant, what)
}

// checkOwner is the authorization shared by the acting verbs: root, the
// store's owner, or a person GateHouse grants `grant` on this store.
func (e *Engine) checkOwner(fs *StoreState, uid int, grant, what string) error {
	fs.scanMu.Lock()
	perm := e.resolvePerms(fs)
	fs.scanMu.Unlock()
	if uid == 0 || uid == perm.uid || e.granted(uid, grant, fs.name) {
		return nil
	}
	owner := "root"
	if perm.uid > 0 {
		owner = fmt.Sprintf("uid %d", perm.uid)
	}
	return fmt.Errorf("store %q belongs to %s: run %s as that user, or as root, or hold %s@%s in GateHouse", fs.name, owner, what, grant, fs.name)
}

// granted asks GateHouse; without it, nobody is granted anything.
func (e *Engine) granted(uid int, perm, store string) bool {
	return uid > 0 && e.grants != nil && e.grants(uid, perm, store)
}

// pruneLoop applies retention until ctx ends: once at startup, then on a
// ticker. Pruning is cheap (an in-memory pass, then a rewrite only when
// something was dropped).
func (e *Engine) pruneLoop(ctx context.Context, interval time.Duration) {
	e.pruneOnce(time.Now())
	t := time.NewTicker(interval)
	defer t.Stop()
	// Abandoned incoming transfers are reaped on a faster beat: each holds a
	// descriptor and a partial file, and wantRefresh is when they count as
	// abandoned.
	reap := time.NewTicker(wantRefresh / 2)
	defer reap.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			e.pruneOnce(now)
			// Retention just let go of versions; their blobs, and versions
			// displaced by updates, are only freed by the collector, which
			// otherwise ran once per start - a long-running daemon piled
			// dead blobs up until the disk was full.
			e.Collect()
		case now := <-reap.C:
			e.reapIncoming(now)
		}
	}
}

// pruneOnce applies every store's retention schedule and reports what it
// dropped.
func (e *Engine) pruneOnce(now time.Time) {
	for _, fs := range e.stores {
		e.pruneTrash(fs, now)
		expire := isScratchPath
		if fs.stream {
			// A queued op is retired by the holder's entry for its path;
			// keep ours until the queue no longer mentions it.
			queued := queuedPaths(fs)
			expire = func(p string) bool { return isScratchPath(p) && !queued[p] }
		}
		if n, err := fs.db.ExpireTombstones(now, scratchTombstoneAge, expire); err != nil {
			e.logger.Warn("expire tombstones", "store", fs.name, "error", err)
		} else if n > 0 {
			e.logger.Info("expired tombstones of scratch files", "store", fs.name, "count", n)
		}
		if fs.hist == nil {
			continue
		}
		if dropped := fs.hist.Prune(now); dropped != nil {
			fs.hist.mu.Lock()
			pruned := fs.hist.pruned
			fs.hist.mu.Unlock()
			e.logger.Info("pruned old versions", "store", fs.name, "dropped", len(dropped), "totalPruned", pruned)
		}
	}
}

// pruneInterval is how often retention runs. Tests shorten it.
var pruneInterval = time.Hour

// Versions lists a file's recorded versions, newest first, for the CLI and
// the daemon's versions endpoint.
func (e *Engine) Versions(name, path string) (api.Versions, error) {
	fs := e.byName[name]
	if fs == nil {
		return api.Versions{}, fmt.Errorf("unknown store %q", name)
	}
	if fs.stream {
		// This machine holds no history for the store; the holder keeps it.
		var out api.Versions
		if err := e.forwardVerbs(fs, verbsRequest{Action: "versions", Path: path}, &out); err != nil {
			return api.Versions{}, err
		}
		return out, nil
	}
	if fs.hist == nil {
		return api.Versions{}, fmt.Errorf("store %q keeps no history on this machine", name)
	}
	if path != "" && !validRel(path) {
		return api.Versions{}, fmt.Errorf("unsafe path %q", path)
	}
	paths := []string{path}
	if path == "" {
		paths = fs.hist.Paths()
		sort.Strings(paths)
	}
	out := api.Versions{Store: name, Path: path, Versions: []api.FileVersion{}}
	if fs.hist.keep > 0 {
		out.Keep = fs.hist.keep.String()
	}
	for _, p := range paths {
		for _, v := range fs.hist.Versions(p) {
			fv := api.FileVersion{
				Time:    time.Unix(0, v.Time).UTC(),
				Blob:    v.Blob,
				Size:    v.Size,
				Deleted: v.Deleted,
			}
			if v.MTime != 0 {
				fv.MTime = time.Unix(0, v.MTime).UTC()
			}
			if v.Mode != 0 {
				fv.Mode = fmt.Sprintf("%04o", os.FileMode(v.Mode).Perm())
			}
			fv.Path = p
			out.Versions = append(out.Versions, fv)
		}
	}
	return out, nil
}

// pruneTrash expires what deletes left behind: a file in
// <dataDir>/trash/<store>/ lives for the trash window (trash.keep), counted
// from when it was deleted (trashTime), not from its own mtime: an old photo
// deleted today used to be purged at the next prune. History is the
// longer-lived safety net: with backup on, the deletion record and its blob
// outlive the trash copy.
func (e *Engine) pruneTrash(fs *StoreState, now time.Time) {
	files, _ := e.emptyTrash(fs.name, now.Add(-fs.trashWindow()), false)
	if files > 0 {
		e.logger.Info("pruned the trash", "store", fs.name, "files", files)
	}
}

// trashWindow is how long this store's trash keeps a file.
func (fs *StoreState) trashWindow() time.Duration {
	if fs.trashKeep > 0 {
		return fs.trashKeep
	}
	if fs.hist != nil && fs.hist.keep > 0 {
		return fs.hist.keep
	}
	return DefaultKeep
}

// refsFromHistory adds the blobs a history file references.
func refsFromHistory(path string, refs map[string]bool) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		var rec struct {
			Version Version `json:"version"`
		}
		if json.Unmarshal(sc.Bytes(), &rec) != nil {
			continue
		}
		if rec.Version.Blob != "" {
			refs[rec.Version.Blob] = true
		}
	}
	return sc.Err()
}
