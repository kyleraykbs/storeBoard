package syncer

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"

	"codeberg.org/kyleraykbs/storeboard/pkg/api"
)

// Takeover converts a declared symlink path that is busy *because it holds
// the user's own content* into a link to the store. The content moves into
// the store's canonical directory — never over anything already there — the
// path becomes a symlink, and the ordinary scan hands the files to the mesh.
//
// It is never automatic: a store's files are only ever moved by this explicit
// verb, so an existing directory is left exactly where it is until the user
// asks for it.
//
// uid is the requesting user, taken from the socket's peer credentials. Root
// and the store's owner may take a path over, nobody else: the socket is
// world-writable, so an acting verb authorizes per caller (DESIGN invariant
// 10).
func (e *Engine) Takeover(name, path string, uid int) (api.TakeoverResult, error) {
	fs := e.byName[name]
	if fs == nil {
		return api.TakeoverResult{}, fmt.Errorf("unknown store %q", name)
	}
	switch {
	case path == "" && len(fs.symlinks) == 0:
		return api.TakeoverResult{}, fmt.Errorf("store %q declares no symlinks: nothing to take over", name)
	case path == "" && len(fs.symlinks) == 1:
		path = fs.symlinks[0]
	case path == "":
		return api.TakeoverResult{}, fmt.Errorf("store %q declares %d symlinks: name one with --path", name, len(fs.symlinks))
	case !slices.Contains(fs.symlinks, path):
		return api.TakeoverResult{}, fmt.Errorf("store %q does not declare symlink %q", name, path)
	}

	fs.scanMu.Lock()
	moved, err := e.takeoverLocked(fs, path, uid)
	state, root := fs.state, fs.root
	fs.scanMu.Unlock()

	e.settleLinks(fs)
	if err != nil {
		return api.TakeoverResult{Store: name, Path: path, State: state, Root: root, Moved: moved}, err
	}
	// The next scan sees the moved files as ordinary local changes.
	e.scanOnce(fs)
	return api.TakeoverResult{Store: name, Path: path, State: state, Root: root, Moved: moved}, nil
}

// takeoverLocked does the work under the store lock. It returns how many
// top-level entries moved.
func (e *Engine) takeoverLocked(fs *StoreState, path string, uid int) (int, error) {
	fs.perm = e.resolvePerms(fs)
	if uid != 0 && uid != fs.perm.uid {
		owner := "root"
		if fs.perm.uid > 0 {
			owner = "uid " + strconv.Itoa(fs.perm.uid)
		}
		return 0, fmt.Errorf("store %q belongs to %s: run this as that user, or as root", fs.name, owner)
	}

	if info, err := os.Lstat(path); err == nil && info.Mode()&os.ModeSymlink != 0 {
		target, rerr := os.Readlink(path)
		switch {
		case rerr != nil:
			return 0, fmt.Errorf("read %s: %w", path, rerr)
		case filepath.Clean(target) == filepath.Clean(fs.root):
			return 0, nil // already linked: nothing to take over
		default:
			return 0, fmt.Errorf("%s is a symlink to %s, not this store: leaving it alone", path, target)
		}
	} else if err != nil && !os.IsNotExist(err) {
		return 0, fmt.Errorf("inspect %s: %w", path, err)
	}

	if fstype := mountFSType(path); fstype != "" {
		return 0, fmt.Errorf("%s is a %s mount: unmount it first", path, fstype)
	}
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return 0, nil // nothing there; settleLinks creates the link
	}
	if err != nil {
		return 0, fmt.Errorf("inspect %s: %w", path, err)
	}
	if !info.IsDir() {
		return 0, fmt.Errorf("%s is not a directory: move it aside first", path)
	}

	if _, err := os.ReadDir(path); err != nil {
		return 0, fmt.Errorf("read %s: %w", path, err)
	}
	moved, merr := e.moveEntries(fs, path, fs.root)
	if merr != nil {
		return moved, merr
	}

	// The directory itself can only go when it is empty: a crash or a
	// permission problem mid-way leaves the path busy, and running the same
	// command again simply continues where it stopped.
	if rest, rerr := os.ReadDir(path); rerr != nil || len(rest) != 0 {
		return moved, fmt.Errorf("%s still has entries; nothing was lost, run the takeover again", path)
	}
	if err := os.Remove(path); err != nil {
		return moved, fmt.Errorf("remove empty %s: %w", path, err)
	}
	if err := symlinkSwap(path, fs.root); err != nil {
		return moved, fmt.Errorf("link %s: %w", path, err)
	}
	e.chownLink(fs, path)
	e.logger.Info("path taken over", "store", fs.name, "path", path, "moved", moved)
	return moved, nil
}
