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
	var unmaterialised int

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

		if !e.store.Has(sum) {
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
		// An entry whose blob this machine does not hold was never
		// materialised here — it is known from the mesh (an index that
		// arrived before its blobs, or a root that moved before its content
		// did). Its absence from the walk says nothing, and tombstoning it
		// would delete the content everywhere: the 448-file incident on
		// amanita was exactly this, an empty storage root scanned against a
		// journal full of entries.
		if ent.Blob != "" && !e.store.Has(ent.Blob) {
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
		changed = append(changed, tomb)
	}

	if unmaterialised > 0 {
		e.logger.Info("skipped entries this machine never materialised; their absence is not a deletion",
			"folder", fs.name, "skipped", unmaterialised)
	}
	if len(changed) > 0 {
		if err := fs.db.Sync(); err != nil {
			return changed, err
		}
	}
	fs.lastScan = time.Now().UTC()
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
