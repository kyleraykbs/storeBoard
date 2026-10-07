package syncer

import (
	"os"
	"path/filepath"
	"strings"
	"time"

	"codeberg.org/kyleraykbs/storeboard/internal/store"
)

// tmpPrefix marks scratch files the daemon writes inside folder roots; the
// scanner never indexes them.
const tmpPrefix = ".storeboard-tmp"

// scan walks the folder's active root and folds reality into the journal:
// new and changed files become blobs with a fresh version, vanished files
// become tombstones. It returns the entries that changed, in their newest
// state, ready to hand to peers.
func (e *Engine) scan(fs *FolderState) ([]*Entry, error) {
	fs.scanMu.Lock()
	defer fs.scanMu.Unlock()

	root := fs.root
	seen := make(map[string]bool)
	var changed []*Entry

	walkErr := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil // vanished mid-walk: tombstoning below covers it
			}
			return err
		}
		if d.IsDir() {
			return nil // WalkDir does not follow directory symlinks
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
		if cur != nil && !cur.Deleted && cur.Blob == sum {
			// Content identical, only the timestamp moved: fold it in
			// without a version bump so a touch does not churn the mesh.
			upd := cur.clone()
			upd.MTime = info.ModTime().UnixNano()
			upd.Mode = uint32(info.Mode().Perm())
			return fs.db.Put(upd)
		}

		if !e.store.Has(sum) {
			if err := e.adoptFile(path, sum); err != nil {
				return err
			}
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
		}
		if err := fs.db.Put(ent); err != nil {
			return err
		}
		changed = append(changed, ent)
		return nil
	})
	if walkErr != nil {
		return changed, walkErr
	}

	for _, ent := range fs.db.All() {
		if ent.Deleted || seen[ent.Path] {
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
		changed = append(changed, tomb)
	}

	if len(changed) > 0 {
		if err := fs.db.Sync(); err != nil {
			return changed, err
		}
	}
	fs.lastScan = time.Now().UTC()
	return changed, nil
}

// adoptFile copies a file into the content store under its known sum.
func (e *Engine) adoptFile(path, sum string) error {
	src, err := os.Open(path)
	if err != nil {
		return err
	}
	defer src.Close()

	tmp, err := e.store.Temp()
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())

	if _, err := tmp.ReadFrom(src); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return e.store.Adopt(tmp.Name(), sum)
}
