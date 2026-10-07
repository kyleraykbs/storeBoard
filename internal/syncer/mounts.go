package syncer

import (
	"fmt"
	"os"
	"path/filepath"

	"codeberg.org/kyleraykbs/storeboard/internal/store"
)

// Mount states, mirrored in api.FolderSync docs.
const (
	stateReplica  = "replica"
	stateAttached = "attached"
	stateDetached = "detached"
	stateBusy     = "busy"
)

// Locking contract: ensureMounts and mergeDetached run with the folder's
// scanMu held by their caller (prepare, attach); attach and detach take it
// themselves. Nothing here may re-take the lock.

// ensureMounts decides what a streamed folder's mount path is this run:
//   - missing                → create it as a symlink to the detached dir
//     (offline-safe until a holder connects)
//   - a symlink into our own views/detached dirs → adopt its state
//   - anything else (a real directory, a foreign symlink — a samba mount)
//     → busy: never touched, exactly the sambaMounts lesson inverted.
func (e *Engine) ensureMounts(fs *FolderState) {
	if !fs.streamed {
		fs.state = stateReplica
		fs.root = fs.mount
		return
	}

	view, detached := e.viewsDir(fs), e.detachedDir(fs)
	for _, d := range []string{view, detached} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			e.logger.Error("view dir", "folder", fs.name, "error", err)
		}
		e.chownPath(fs, d)
	}

	switch target, err := os.Readlink(fs.mount); {
	case err == nil && target == view:
		fs.state = stateAttached
		fs.root = view
	case err == nil && target == detached:
		fs.state = stateDetached
		fs.root = detached
	case err == nil:
		// A symlink we did not make: leave it alone.
		fs.state = stateBusy
		fs.root = detached
		e.logger.Warn("mount path is a foreign symlink; leaving it alone", "folder", fs.name, "path", fs.mount, "target", target)
	default:
		if _, serr := os.Lstat(fs.mount); serr == nil {
			fs.state = stateBusy
			fs.root = detached
			e.logger.Warn("mount path is occupied; leaving it alone", "folder", fs.name, "path", fs.mount)
			return
		}
		if err := symlinkSwap(fs.mount, detached); err != nil {
			fs.state = stateBusy
			fs.root = detached
			e.logger.Error("create mount symlink", "folder", fs.name, "error", err)
			return
		}
		e.chownLink(fs, fs.mount)
		fs.state = stateDetached
		fs.root = detached
	}
	if fs.state != stateBusy {
		e.logger.Info("folder mounted", "folder", fs.name, "state", fs.state, "mount", fs.mount)
	}
}

// attach flips a detached streamed folder back to the live view: whatever
// was written while offline is merged into the view first, so nothing is
// lost and the next scan hands it to the mesh.
func (e *Engine) attach(fs *FolderState) {
	// The folder lock serializes every root change against scans and
	// applies: a file must never be written into a root as it is swapped.
	fs.scanMu.Lock()
	defer fs.scanMu.Unlock()
	if !fs.streamed || fs.state == stateAttached || fs.state == stateBusy {
		return
	}
	view, detached := e.viewsDir(fs), e.detachedDir(fs)
	if err := e.mergeDetached(fs, detached, view); err != nil {
		e.logger.Error("remerge", "folder", fs.name, "error", err)
	}
	if err := symlinkSwap(fs.mount, view); err != nil {
		e.logger.Error("attach", "folder", fs.name, "error", err)
		return
	}
	e.chownLink(fs, fs.mount)
	fs.state = stateAttached
	fs.root = view
	e.logger.Info("folder attached", "folder", fs.name, "mount", fs.mount)
}

// detach replaces the live view with the detached dir (the "dummy folder"):
// the mount path exists and is writable, it is simply empty until something
// is put in it — and what is put in it is merged back on the next attach.
func (e *Engine) detach(fs *FolderState) {
	fs.scanMu.Lock()
	defer fs.scanMu.Unlock()
	if !fs.streamed || fs.state != stateAttached {
		return
	}
	detached := e.detachedDir(fs)
	if err := os.MkdirAll(detached, 0o700); err != nil {
		e.logger.Error("detach", "folder", fs.name, "error", err)
		return
	}
	e.chownPath(fs, detached)
	if err := symlinkSwap(fs.mount, detached); err != nil {
		e.logger.Error("detach", "folder", fs.name, "error", err)
		return
	}
	e.chownLink(fs, fs.mount)
	fs.state = stateDetached
	fs.root = detached
	e.logger.Info("folder detached (no holder reachable)", "folder", fs.name, "mount", fs.mount)
}

// mergeDetached moves everything written into the detached dir into the
// view: identical files are dropped, missing ones move in, and differing
// ones keep both (newer keeps the name, the other becomes a conflict copy).
func (e *Engine) mergeDetached(fs *FolderState, detached, view string) error {
	var moved int
	err := filepath.WalkDir(detached, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		rel, rerr := filepath.Rel(detached, path)
		if rerr != nil {
			return nil
		}
		target := filepath.Join(view, rel)
		st, serr := os.Stat(target)
		switch {
		case serr != nil && os.IsNotExist(serr):
			if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
				return err
			}
			if err := os.Rename(path, target); err != nil {
				return nil
			}
		default:
			srcInfo, ierr := d.Info()
			if ierr == nil && st.Size() == srcInfo.Size() && st.ModTime().UnixNano() == srcInfo.ModTime().UnixNano() {
				os.Remove(path) // identical copy
				return nil
			}
			sum, herr := store.SumFile(path)
			if herr != nil {
				return nil
			}
			dst := conflictPath(target, sum)
			if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
				return err
			}
			if err := os.Rename(path, dst); err != nil {
				return nil
			}
		}
		e.chownPath(fs, target)
		moved++
		return nil
	})
	if err == nil {
		// Only remove the detached tree once everything left it.
		filepath.WalkDir(detached, func(p string, d os.DirEntry, err error) error {
			if err == nil && d.IsDir() {
				os.Remove(p)
			}
			return nil
		})
		os.Remove(detached)
	}
	if moved > 0 {
		e.logger.Info("merged offline changes back in", "folder", fs.name, "files", moved)
	}
	return err
}

// viewsDir / detachedDir are the daemon-owned halves of a streamed folder,
// both under the one storage directory.
func (e *Engine) viewsDir(fs *FolderState) string { return filepath.Join(e.dataDir, "views", fs.name) }
func (e *Engine) detachedDir(fs *FolderState) string {
	return filepath.Join(e.dataDir, "detached", fs.name)
}

// conflictPath names a conflict copy deterministically from the content
// hash, so independent replicas pick the same name.
func conflictPath(path, sum string) string {
	short := sum
	if len(short) > 8 {
		short = short[:8]
	}
	return path + ".conflict-" + short
}

// symlinkSwap replaces path with a symlink to target, atomically: a fresh
// symlink is created next to it and renamed over the old one, so a reader
// never sees the path missing.
func symlinkSwap(path, target string) error {
	tmp := filepath.Join(filepath.Dir(path), tmpPrefix+filepath.Base(path))
	os.Remove(tmp)
	if err := os.Symlink(target, tmp); err != nil {
		return fmt.Errorf("symlink %s: %w", target, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("swap %s: %w", path, err)
	}
	return nil
}

// chownPath best-efforts ownership of a created entry to the folder's owner
// (the user whose home holds the mount), so the human can actually use it.
func (e *Engine) chownPath(fs *FolderState, path string) {
	if fs.ownerUID < 0 {
		return
	}
	_ = os.Chown(path, fs.ownerUID, fs.ownerGID)
}

// chownLink is chownPath for a symlink itself.
func (e *Engine) chownLink(fs *FolderState, path string) {
	if fs.ownerUID < 0 {
		return
	}
	_ = os.Lchown(path, fs.ownerUID, fs.ownerGID)
}
