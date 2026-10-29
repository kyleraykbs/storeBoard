package syncer

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"codeberg.org/kyleraykbs/storeboard/internal/store"
)

// tmpPrefix marks scratch files the daemon writes inside store roots; the
// scanner never indexes them.
const tmpPrefix = ".storeboard-tmp"

// scan walks the store's active root and folds reality into the journal:
// new and changed files become blobs with a fresh version, vanished files
// become tombstones — but only when tombstones is set. A streamed store is
// a mirror, and its root can be empty or partial for its own reasons (a
// migration in flight, an index that arrived before its blobs, a store that
// is not live); absence there means "not here yet", and treating it as a
// deletion deletes the content everywhere. The incident that cost 448 files
// was exactly that. Replicas own their root, and a live streamed store can
// tell its peers about a real deletion, so both may tombstone. It returns
// the entries that changed, in their newest state, ready to hand to peers.
func (e *Engine) scan(fs *StoreState, tombstones bool) ([]*Entry, error) {
	fs.scanMu.Lock()
	defer fs.scanMu.Unlock()

	root := fs.root
	fs.setScanning(true)
	defer fs.setScanning(false)
	seen := make(map[string]bool)
	var dirs []string
	var changed []*Entry
	var unmaterialised int

	walkErr := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil // vanished mid-walk: tombstoning below covers it
			}
			return err
		}
		if d.IsDir() {
			// WalkDir does not follow directory symlinks. Every directory is
			// collected: reconcileDirs decides which of them need an entry.
			if path != root {
				if rel, rerr := filepath.Rel(root, path); rerr == nil {
					dirs = append(dirs, rel)
				}
			}
			return nil
		}
		name := d.Name()
		if strings.HasPrefix(name, tmpPrefix) {
			return nil
		}
		rel, rerr := filepath.Rel(root, path)
		if rerr != nil {
			return rerr
		}
		info, ierr := d.Info()
		if ierr != nil {
			return nil
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			// Symlinks, sockets, fifos: not synced yet (documented
			// limitation), and never followed.
			return nil
		}
		seen[rel] = true

		cur := fs.db.Get(rel)
		if cur != nil && !cur.Deleted && cur.Size == info.Size() && cur.MTime == info.ModTime().UnixNano() {
			return nil // untouched
		}

		sum, herr := store.SumFile(path)
		if herr != nil {
			return nil
		}
		// The dirent snapshot WalkDir handed us can be stale — a file
		// truncated and rewritten while the walk passed it reads as size 0
		// with a fresh mtime — so size, mtime and mode are re-read AFTER
		// the hash, and the hash itself comes from a copy of those same
		// bytes. An entry whose size contradicts its blob poisons every
		// later apply (each one parks the "unsynced local change" as a
		// false conflict).
		if fi, serr := os.Stat(path); serr == nil {
			info = fi
		}

		if cur != nil && !cur.Deleted && cur.Blob == sum {
			// Content identical, only the timestamp moved (or a stale size
			// is being repaired): fold it in without a version bump so a
			// touch does not churn the mesh.
			upd := cur.clone()
			upd.Size = info.Size()
			upd.MTime = info.ModTime().UnixNano()
			upd.Mode = uint32(info.Mode().Perm())
			return fs.db.Put(upd)
		}

		// Adopt the bytes into the content store only when they are needed
		// there (a kept version, or a streamed store with no tree of its
		// own). Copying every local file into the store and then deleting the
		// copy would double the write for no gain.
		if e.wantsBlob(fs, rel, sum) && !e.store.Has(sum) {
			stored, aerr := e.adoptFile(path, sum)
			if aerr != nil {
				return aerr
			}
			sum = stored
		}
		// A local change builds on what this replica already knew: the new
		// version vector inherits the old one and bumps our own counter.
		// Replacing it outright would make history look concurrent and turn
		// every edit into a conflict.
		vv := map[string]uint64{}
		if cur != nil {
			vv = MergeVV(cur.VV, nil)
		}
		vv[e.me] = fs.db.Next()
		ent := &Entry{
			Path:  rel,
			Blob:  sum,
			Size:  info.Size(),
			MTime: info.ModTime().UnixNano(),
			Mode:  uint32(info.Mode().Perm()),
			VV:    vv,
			Local: true, // this machine has the file
		}
		if err := fs.db.Put(ent); err != nil {
			return err
		}
		e.record(fs, ent)
		changed = append(changed, ent)
		return nil
	})
	if walkErr != nil {
		return changed, walkErr
	}
	fs.setLocal(len(seen))

	for _, ent := range fs.db.All() {
		if ent.Deleted || ent.Dir || seen[ent.Path] {
			continue
		}
		if !tombstones {
			continue
		}
		// An entry this machine never materialised is known from the mesh (an
		// index that arrived before its bytes, or a journal restored beside a
		// root that moved). Its absence from the walk says nothing, and
		// tombstoning it would delete the content everywhere: the 448-file
		// incident on amanita was exactly this, an empty storage root scanned
		// against a journal full of entries. Holding the blob was the old
		// evidence; now that a replica releases blobs once the file is
		// materialised, the entry records it directly.
		if !ent.Local && ent.Blob != "" && !e.store.Has(ent.Blob) {
			unmaterialised++
			continue
		}
		tomb := ent.clone()
		tomb.Deleted = true
		tomb.Blob = ""
		tomb.Size = 0
		// Same rule as an edit: the tombstone extends the version it saw.
		vv := MergeVV(ent.VV, nil)
		vv[e.me] = fs.db.Next()
		tomb.VV = vv
		if err := fs.db.Put(tomb); err != nil {
			return changed, err
		}
		// The version that vanished is worth keeping: this is the delete a
		// restore would undo, so it is recorded as a deletion holding the
		// content it removed - what `backup list` shows as gone and what a
		// bare `backup restore` brings back.
		gone := ent.clone()
		gone.Deleted = true
		e.record(fs, gone)
		changed = append(changed, tomb)
		// A directory exists because it held a file; once that file is gone
		// the directory goes too. A directory the user made is recorded as a
		// Dir entry and stops the walk.
		e.pruneEmptyParents(fs, ent.Path)
	}

	// Directories are reconciled after deletions: a directory this pass just
	// emptied has been pruned, and recording it now would resurrect it as a
	// user-created empty directory.
	changed = append(changed, e.reconcileDirs(fs, dirs, seen, tombstones)...)

	if unmaterialised > 0 {
		e.logger.Info("skipped entries this machine never materialised; their absence is not a deletion",
			"store", fs.name, "skipped", unmaterialised)
	}
	if len(changed) > 0 {
		if err := fs.db.Sync(); err != nil {
			return changed, err
		}
	}
	fs.setLastScan(time.Now().UTC())
	return changed, nil
}

// adoptFile copies a file into the content store, hashing WHILE it copies,
// and returns the hash of the bytes actually stored. Hashing first and
// copying after would let a mid-write file land in the store under a sum
// that does not describe its content — a poisoned blob every peer then
// fails to verify.
func (e *Engine) adoptFile(path, sum string) (string, error) {
	src, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer src.Close()

	tmp, err := e.store.Temp()
	if err != nil {
		return "", err
	}
	defer os.Remove(tmp.Name())

	h := sha256.New()
	if _, err := io.Copy(io.MultiWriter(tmp, h), src); err != nil {
		tmp.Close()
		return "", err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return "", err
	}
	if err := tmp.Close(); err != nil {
		return "", err
	}
	stored := hex.EncodeToString(h.Sum(nil))
	if err := e.store.Adopt(tmp.Name(), stored); err != nil {
		return "", err
	}
	if stored != sum {
		e.logger.Debug("file changed while being scanned", "path", path, "hashed", sum, "stored", stored)
	}
	return stored, nil
}

// reconcileDirs keeps the journal's directory entries in step with the tree.
//
// Only directories with no files below them are recorded. A directory holding
// files is already described by those paths, and applying a file creates its
// parents, so an entry would be churn. An empty directory — the one the user
// just made, or one holding nothing but other empty directories — has no
// other trace in the journal, and without an entry it would never reach a
// peer.
//
// The inverse matters just as much: once a directory gains a file its entry
// is tombstoned, so that when the file is later deleted nothing claims the
// directory should stay. That is what makes "the files left, so the directory
// goes" true without touching empty directories the user created.
func (e *Engine) reconcileDirs(fs *StoreState, dirs []string, files map[string]bool, tombstones bool) []*Entry {
	hasFiles := make(map[string]bool, len(dirs))
	for path := range files {
		for dir := filepath.Dir(path); dir != "." && dir != string(filepath.Separator); dir = filepath.Dir(dir) {
			if hasFiles[dir] {
				break
			}
			hasFiles[dir] = true
		}
	}
	present := make(map[string]bool, len(dirs))
	for _, d := range dirs {
		present[d] = true
	}

	var changed []*Entry
	for _, d := range dirs {
		cur := fs.db.Get(d)
		live := cur != nil && !cur.Deleted
		switch {
		case hasFiles[d]:
			// The tree now describes this directory. Drop the entry so a
			// later deletion of the last file can prune it.
			if live && cur.Dir {
				if tomb := e.tombstoneEntry(fs, cur); tomb != nil {
					changed = append(changed, tomb)
				}
			}
		case live && cur.Dir:
			// Already recorded and still empty: nothing to do.
		case live:
			// A file entry at this path and a directory on disk. The walk
			// saw the directory, so the stale file entry loses.
			if tomb := e.tombstoneEntry(fs, cur); tomb != nil {
				changed = append(changed, tomb)
			}
			fallthrough
		default:
			if info, err := os.Stat(filepath.Join(fs.root, d)); err != nil || !info.IsDir() {
				continue // pruned by this pass: the files left it
			}
			ent := &Entry{Path: d, Dir: true, Mode: 0o755}
			vv := map[string]uint64{}
			if cur != nil {
				vv = MergeVV(cur.VV, nil)
			}
			vv[e.me] = fs.db.Next()
			ent.VV = vv
			if err := fs.db.Put(ent); err != nil {
				e.logger.Warn("record directory", "store", fs.name, "path", d, "error", err)
				continue
			}
			changed = append(changed, ent)
		}
	}

	if !tombstones {
		return changed
	}
	for _, ent := range fs.db.All() {
		if !ent.Dir || ent.Deleted || present[ent.Path] {
			continue
		}
		if tomb := e.tombstoneEntry(fs, ent); tomb != nil {
			changed = append(changed, tomb)
		}
	}
	return changed
}

// tombstoneEntry records that a path is gone, extending the version it saw so
// the deletion dominates. The vanished version is kept in history, which is
// what a restore undoes; directories carry no content, so they are not
// recorded there.
func (e *Engine) tombstoneEntry(fs *StoreState, ent *Entry) *Entry {
	tomb := ent.clone()
	tomb.Deleted = true
	tomb.Blob = ""
	tomb.Size = 0
	vv := MergeVV(ent.VV, nil)
	vv[e.me] = fs.db.Next()
	tomb.VV = vv
	if err := fs.db.Put(tomb); err != nil {
		e.logger.Warn("tombstone", "store", fs.name, "path", ent.Path, "error", err)
		return nil
	}
	if !ent.Dir {
		gone := ent.clone()
		gone.Deleted = true
		e.record(fs, gone)
	}
	return tomb
}

// pruneEmptyParents removes the directories a deleted file leaves behind,
// walking up until something is non-empty or the store's declared empty
// directory is reached. Callers hold scanMu.
//
// A directory recorded as a Dir entry is never removed: that entry is the
// user's statement that the directory should exist, and it is exactly the
// difference between an empty directory they made and one the files left.
func (e *Engine) pruneEmptyParents(fs *StoreState, path string) {
	for dir := filepath.Dir(path); dir != "." && dir != string(filepath.Separator) && dir != ""; dir = filepath.Dir(dir) {
		if ent := fs.db.Get(dir); ent != nil && !ent.Deleted && ent.Dir {
			return
		}
		abs := filepath.Join(fs.root, dir)
		entries, err := os.ReadDir(abs)
		if err != nil || len(entries) > 0 {
			return
		}
		if err := os.Remove(abs); err != nil {
			return
		}
		e.logger.Info("removed empty directory", "store", fs.name, "path", dir)
	}
}
