package syncer

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"codeberg.org/kyleraykbs/storeboard/internal/store"
	"codeberg.org/kyleraykbs/storeboard/pkg/api"
)

// Takeover converts a mount path that is busy *because it holds the user's
// content* into a storeboard mount. The content moves into the folder's
// offline dir — the same landing zone offline writes use — the path is
// replaced by the mount symlink, and if a holder is reachable the ordinary
// attach merges that content into the view, exactly as it would after an
// offline session. Without a holder the mount simply stays offline, and the
// merge happens by itself when one returns.
//
// Nothing is ever overwritten: a name already parked in the offline dir is
// set aside under the same deterministic conflict name the merge uses, so
// both versions survive and converge.
//
// uid is the requesting user, taken from the socket's peer credentials.
// Root and the folder's owner may take a folder over, nobody else: the
// socket is world-writable, so an acting verb authorizes per caller
// (DESIGN invariant 10).
func (e *Engine) Takeover(name string, uid int) (api.TakeoverResult, error) {
	fs := e.byName[name]
	if fs == nil {
		return api.TakeoverResult{}, fmt.Errorf("unknown folder %q", name)
	}
	if !fs.streamed {
		return api.TakeoverResult{}, fmt.Errorf("folder %q is a replica: its directory is the source of truth, there is nothing to take over", name)
	}

	fs.scanMu.Lock()
	moved, err := e.takeoverLocked(fs, uid)
	state, root := fs.state, fs.root
	fs.scanMu.Unlock()
	if err != nil {
		return api.TakeoverResult{Folder: name, State: state, Root: root, Moved: moved}, err
	}

	// A holder that answers turns the offline mount live at once; the attach
	// merges the moved content into the view and the next scan hands it to
	// the mesh.
	if e.participantConnected(fs) {
		e.attach(fs)
		e.scanOnce(fs)
	}
	fs.scanMu.Lock()
	state, root = fs.state, fs.root
	fs.scanMu.Unlock()
	return api.TakeoverResult{Folder: name, State: state, Root: root, Moved: moved}, nil
}

// takeoverLocked does the work under the folder lock and leaves the folder
// detached over its offline dir. It returns how many top-level entries moved.
func (e *Engine) takeoverLocked(fs *FolderState, uid int) (int, error) {
	fs.perm = e.resolvePerms(fs)
	if uid != 0 && uid != fs.perm.uid {
		owner := "root"
		if fs.perm.uid > 0 {
			owner = "uid " + strconv.Itoa(fs.perm.uid)
		}
		return 0, fmt.Errorf("folder %q belongs to %s: run this as that user, or as root", fs.name, owner)
	}

	switch fs.state {
	case stateAttached, stateDetached:
		// Already ours: nothing to take over, and saying so is friendlier
		// than an error.
		return 0, nil
	case stateBusy:
	default:
		return 0, fmt.Errorf("folder %q is not ready yet (%s)", fs.name, fs.state)
	}

	// Only a plain directory can be taken over. A live mount belongs to the
	// system and a symlink to whoever made it; neither is ours to reorganize,
	// and the reasons match the ones the daemon reports as busy.
	if fstype := mountFSType(fs.mount); fstype != "" {
		return 0, fmt.Errorf("%s is a %s mount: unmount it first (or let the takeover unit do it)", fs.mount, fstype)
	}
	info, err := os.Lstat(fs.mount)
	if err != nil {
		return 0, fmt.Errorf("inspect %s: %w", fs.mount, err)
	}
	if !info.IsDir() {
		return 0, fmt.Errorf("%s is not a directory: move it aside first", fs.mount)
	}

	offline := e.detachedDir(fs)
	if err := os.MkdirAll(offline, 0o700); err != nil {
		return 0, err
	}
	ensureTraversable(e.dataDir, offline)
	e.applyDir(fs, offline)

	entries, err := os.ReadDir(fs.mount)
	if err != nil {
		return 0, fmt.Errorf("read %s: %w", fs.mount, err)
	}
	moved := 0
	for _, ent := range entries {
		src := filepath.Join(fs.mount, ent.Name())
		dst := filepath.Join(offline, ent.Name())
		if err := e.takeoverMove(fs, src, dst); err != nil {
			e.logger.Warn("takeover: entry left in place", "folder", fs.name, "entry", ent.Name(), "error", err)
			continue
		}
		moved++
	}
	// The directory itself can only go when it is empty: a crash or a
	// permission problem mid-way leaves the path busy, and running the same
	// command again simply continues where it stopped.
	if rest, rerr := os.ReadDir(fs.mount); rerr != nil || len(rest) != 0 {
		return moved, fmt.Errorf("%s still has entries; nothing was lost, run the takeover again", fs.mount)
	}
	if err := os.Remove(fs.mount); err != nil {
		return moved, fmt.Errorf("remove empty %s: %w", fs.mount, err)
	}
	if err := symlinkSwap(fs.mount, offline); err != nil {
		return moved, fmt.Errorf("mount %s: %w", fs.mount, err)
	}
	e.chownLink(fs, fs.mount)
	fs.state = stateDetached
	fs.busy = ""
	fs.root = offline
	e.logger.Info("mount path taken over", "folder", fs.name, "path", fs.mount, "moved", moved)
	return moved, nil
}

// takeoverMove moves one entry into the offline dir: recursively for
// directories, without following symlinks and without ever replacing what is
// already there.
func (e *Engine) takeoverMove(fs *FolderState, src, dst string) error {
	info, err := os.Lstat(src)
	if err != nil {
		return err
	}

	if info.IsDir() {
		if di, derr := os.Lstat(dst); derr == nil && !di.IsDir() {
			if err := e.parkAside(dst); err != nil {
				return err
			}
		}
		if err := os.MkdirAll(dst, 0o700); err != nil {
			return err
		}
		e.applyDir(fs, dst)
		children, err := os.ReadDir(src)
		if err != nil {
			return err
		}
		for _, c := range children {
			if err := e.takeoverMove(fs, filepath.Join(src, c.Name()), filepath.Join(dst, c.Name())); err != nil {
				return err
			}
		}
		return os.Remove(src)
	}

	if _, derr := os.Lstat(dst); derr == nil {
		if err := e.parkAside(dst); err != nil {
			return err
		}
	}
	if err := moveOrCopy(src, dst, info.Mode()); err != nil {
		return err
	}
	e.chownPath(fs, dst)
	return nil
}

// parkAside renames an entry out of the way, under the deterministic
// conflict name for its content where one can be computed (a directory gets
// a numbered suffix): a takeover never silently replaces anything.
func (e *Engine) parkAside(path string) error {
	if sum, err := store.SumFile(path); err == nil {
		return os.Rename(path, conflictPath(path, sum))
	}
	for i := 1; ; i++ {
		alt := path + ".storeboard-kept-" + strconv.Itoa(i)
		if _, err := os.Lstat(alt); os.IsNotExist(err) {
			return os.Rename(path, alt)
		}
	}
}
