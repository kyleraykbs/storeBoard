package syncer

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

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
func (e *Engine) Collect() (removed int, freed int64) {
	keep := map[string]bool{}

	// Every retained version needs its bytes: that is the whole point of the
	// history, and the file itself may be gone.
	for _, fs := range e.stores {
		if fs.hist == nil {
			continue
		}
		for _, path := range fs.hist.Paths() {
			for _, v := range fs.hist.Versions(path) {
				if v.Blob != "" {
					keep[v.Blob] = true
				}
			}
		}
	}

	// A blob is also the only copy of a file this machine does not hold on
	// disk: a streamed store's cache, or an entry whose file is missing or
	// stale. Everything else is duplicated by the tree and can go.
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
			if err := refsFromJournal(filepath.Join(journalDir, name), keep); err != nil {
				e.logger.Warn("collector: cannot read a journal; keeping everything", "journal", name, "error", err)
				return 0, 0
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
			if err := refsFromHistory(filepath.Join(historyDir, name), keep); err != nil {
				e.logger.Warn("collector: cannot read a history; keeping everything", "history", name, "error", err)
				return 0, 0
			}
		}
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
// alone.
//
// uid is the requesting user, from the socket's peer credentials: root and
// the store's owner may restore, nobody else.
func (e *Engine) Restore(name string, uid int, path, blob string, at time.Time, to string) (api.RestoreResult, error) {
	fs := e.byName[name]
	if fs == nil {
		return api.RestoreResult{}, fmt.Errorf("unknown store %q", name)
	}
	if fs.hist == nil {
		return api.RestoreResult{}, fmt.Errorf("store %q keeps no history on this machine", name)
	}
	if !validRel(path) {
		return api.RestoreResult{}, fmt.Errorf("unsafe path %q", path)
	}
	if err := e.checkOwner(fs, uid, "restore from"); err != nil {
		return api.RestoreResult{}, err
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
		if err := e.writeBlobTo(fs, to, pick.Blob, pick.Mode, pick.MTime); err != nil {
			return api.RestoreResult{}, err
		}
		return api.RestoreResult{Store: name, Path: to, Blob: pick.Blob, At: time.Unix(0, pick.Time).UTC(), Copied: true}, nil
	}

	dst := filepath.Join(fs.root, path)
	if err := e.ensureDir(fs, filepath.Dir(dst)); err != nil {
		return api.RestoreResult{}, err
	}
	if err := e.writeBlobTo(fs, dst, pick.Blob, pick.Mode, pick.MTime); err != nil {
		return api.RestoreResult{}, err
	}
	ent := &Entry{
		Path:  path,
		Blob:  pick.Blob,
		Size:  pick.Size,
		MTime: pick.MTime,
		Mode:  pick.Mode,
		VV:    map[string]uint64{e.me: fs.db.Next()},
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

// checkOwner is the authorization shared by the acting verbs: root, or the
// store's owner.
func (e *Engine) checkOwner(fs *StoreState, uid int, what string) error {
	fs.scanMu.Lock()
	perm := e.resolvePerms(fs)
	fs.scanMu.Unlock()
	if uid == 0 || uid == perm.uid {
		return nil
	}
	owner := "root"
	if perm.uid > 0 {
		owner = fmt.Sprintf("uid %d", perm.uid)
	}
	return fmt.Errorf("store %q belongs to %s: run %s as that user, or as root", fs.name, owner, what)
}

// pruneLoop applies retention until ctx ends: once at startup, then on a
// ticker. Pruning is cheap (an in-memory pass, then a rewrite only when
// something was dropped).
func (e *Engine) pruneLoop(ctx context.Context, interval time.Duration) {
	e.pruneOnce(time.Now())
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			e.pruneOnce(now)
		}
	}
}

// pruneOnce applies every store's retention schedule and reports what it
// dropped.
func (e *Engine) pruneOnce(now time.Time) {
	for _, fs := range e.stores {
		e.pruneTrash(fs, now)
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

// pruneTrash applies the same deadline to what deletes left behind: a file in
// <dataDir>/trash/<store>/ is content the store once held, and it lives
// exactly as long as the retention window allows — by its own age, which a
// move out of the store preserves. History is the longer-lived safety net:
// with backup on, the deletion record and its blob outlive the trash copy.
func (e *Engine) pruneTrash(fs *StoreState, now time.Time) {
	trash := filepath.Join(e.dataDir, "trash", fs.name)
	keep := fs.keepWindow()
	var dropped int
	err := filepath.WalkDir(trash, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		fi, serr := d.Info()
		if serr != nil || now.Sub(fi.ModTime()) <= keep {
			return nil
		}
		if os.Remove(path) == nil {
			dropped++
		}
		return nil
	})
	if err != nil || dropped == 0 {
		return
	}
	e.logger.Info("pruned the trash", "store", fs.name, "files", dropped)
}

// keepWindow is the retention window this store runs under.
func (fs *StoreState) keepWindow() time.Duration {
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
