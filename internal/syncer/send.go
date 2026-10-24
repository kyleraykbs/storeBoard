package syncer

import (
	"fmt"
	"os"
	"path/filepath"

	"codeberg.org/kyleraykbs/storeboard/internal/store"
	"codeberg.org/kyleraykbs/storeboard/pkg/api"
)

// Send copies a local file into one of this machine's stores, which is how a
// share sheet puts a photo on the mesh: the copy is an ordinary local change,
// so the scanner indexes it, the history records it, and the peers get it.
// dst names the destination inside the store (a relative path); empty uses
// the source's base name. Nothing is ever overwritten — a name already taken
// is parked under the deterministic conflict name first, and the caller is
// told where it landed.
//
// uid is the requesting user, from the socket's peer credentials: root and
// the store's owner may send, nobody else.
func (e *Engine) Send(name string, uid int, src, dst string) (api.SendResult, error) {
	fs := e.byName[name]
	if fs == nil {
		return api.SendResult{}, fmt.Errorf("unknown store %q", name)
	}
	if err := e.checkOwner(fs, uid, "send into"); err != nil {
		return api.SendResult{}, err
	}
	info, err := os.Lstat(src)
	if err != nil {
		return api.SendResult{}, fmt.Errorf("source: %w", err)
	}
	if !info.Mode().IsRegular() {
		return api.SendResult{}, fmt.Errorf("%s is not a regular file (directories are not sent yet)", src)
	}
	if dst == "" {
		dst = filepath.Base(src)
	}
	if !validRel(dst) {
		return api.SendResult{}, fmt.Errorf("unsafe destination %q", dst)
	}

	fs.scanMu.Lock()
	defer fs.scanMu.Unlock()

	target := filepath.Join(fs.root, dst)
	if err := e.ensureDir(fs, filepath.Dir(target)); err != nil {
		return api.SendResult{}, err
	}
	parked := ""
	if _, serr := os.Lstat(target); serr == nil {
		// Taken: keep both. Parking the existing file keeps this a pure
		// addition from the sender's point of view.
		sum, herr := store.SumFile(target)
		if herr != nil {
			return api.SendResult{}, fmt.Errorf("destination %s exists and cannot be hashed: %w", dst, herr)
		}
		parked = conflictPath(dst, sum)
		if err := os.Rename(target, filepath.Join(fs.root, parked)); err != nil {
			return api.SendResult{}, fmt.Errorf("park existing %s: %w", dst, err)
		}
		if ent := fs.db.Get(dst); ent != nil {
			moved := ent.clone()
			moved.Path = parked
			moved.VV = map[string]uint64{e.me: fs.db.Next()}
			_ = fs.db.Put(moved)
			e.record(fs, moved)
		}
	}
	if err := moveOrCopy(src, target, info.Mode()); err != nil {
		return api.SendResult{}, err
	}
	e.chownPath(fs, target)

	// Index it now rather than waiting for the next scan: the sender wants to
	// know it is on the mesh, not that it will be in fifteen seconds. A scan
	// of just-sent content is a walk of that store, which is cheap next to
	// the transfer the caller just made.
	fs.scanMu.Unlock()
	e.scanOnce(fs)
	fs.scanMu.Lock()

	e.logger.Info("sent a file into the store", "store", name, "path", dst, "from", src)
	return api.SendResult{Store: name, Path: dst, Parked: parked, Size: info.Size()}, nil
}
