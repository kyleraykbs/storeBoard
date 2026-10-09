package syncer

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"codeberg.org/kyleraykbs/storeboard/internal/store"
	"codeberg.org/kyleraykbs/storeboard/pkg/api"
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
	if fs.storage != "" {
		if _, err := e.ensureStorage(fs); err != nil {
			fs.state = stateBusy
			fs.busy = api.BusyContent
			fs.root = fs.storage
			e.logger.Error("storage directory", "folder", fs.name, "path", fs.storage, "error", err)
			return
		}
	}

	if !fs.streamed {
		fs.state = stateReplica
		fs.busy = ""
		if fs.storage == "" {
			// Original layout: the mount path IS the content.
			fs.root = fs.mount
			return
		}
		// Storage-backed replica: content lives in the storage directory and
		// the mount path is a view onto it, so the mount can sit anywhere
		// without the daemon having to own it as data.
		if err := symlinkSwap(fs.mount, fs.storage); err != nil {
			fs.state = stateBusy
			fs.root = fs.storage
			e.logger.Error("mount storage-backed folder", "folder", fs.name, "error", err)
			return
		}
		e.chownLink(fs, fs.mount)
		fs.root = fs.storage
		e.logger.Info("folder mounted", "folder", fs.name, "state", fs.state, "mount", fs.mount, "storage", fs.storage)
		return
	}

	view, detached := e.viewsDir(fs), e.detachedDir(fs)
	if fs.storage != "" {
		// Storage-backed streamed folders never point the mount elsewhere:
		// the view is where the content already lives, so the path shows it
		// whether or not a holder is reachable, offline writes land in it
		// directly, and the root never flips (which is what used to make
		// detaches tombstone a folder).
		//
		// A symlink we did not make is still not ours to replace — the same
		// rule the original layout follows.
		if target, err := os.Readlink(fs.mount); err == nil {
			if target != fs.storage && target != detached && target != e.legacyViewDir(fs) {
				fs.state = stateBusy
				fs.busy = api.BusySymlink
				fs.root = view
				e.logger.Warn("mount path is a foreign symlink; leaving it alone", "folder", fs.name, "path", fs.mount, "target", target)
				return
			}
		}
		if err := symlinkSwap(fs.mount, view); err != nil {
			fs.state = stateBusy
			fs.root = view
			e.logger.Error("mount storage-backed folder", "folder", fs.name, "error", err)
			return
		}
		e.chownLink(fs, fs.mount)
		if fs.state != stateAttached {
			fs.state = stateDetached
		}
		fs.busy = ""
		fs.root = view
		e.logger.Info("folder mounted", "folder", fs.name, "state", fs.state, "mount", fs.mount, "storage", fs.storage)
		return
	}
	for _, d := range []string{view, detached} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			e.logger.Error("view dir", "folder", fs.name, "error", err)
		}
		// The mount symlink points into the storage directory: every
		// component on the way must be walkable, or the permissions on the
		// target are moot.
		ensureTraversable(e.dataDir, d)
		// The target itself takes the folder's resolved permissions — by
		// default the same as the directory the mount lives in. Applied on
		// every transition, not just at creation.
		e.applyDir(fs, d)
	}

	switch target, err := os.Readlink(fs.mount); {
	case err == nil && target == view:
		fs.state = stateAttached
		fs.busy = ""
		fs.root = view
	case err == nil && target == detached:
		fs.state = stateDetached
		fs.busy = ""
		fs.root = detached
	case err == nil:
		// A symlink we did not make: leave it alone.
		fs.state = stateBusy
		fs.busy = api.BusySymlink
		fs.root = detached
		e.logger.Warn("mount path is a foreign symlink; leaving it alone", "folder", fs.name, "path", fs.mount, "target", target)
	default:
		info, serr := os.Lstat(fs.mount)
		switch {
		case os.IsNotExist(serr):
			e.logger.Info("mount path is not present yet; creating the view link", "folder", fs.name, "path", fs.mount)
			// A fresh machine: the path simply is not there yet — and
			// neither, quite possibly, is the directory it belongs in (a
			// declaration like ~/remote/Videos on a machine that never had
			// a ~/remote). The mount point is a promise the daemon keeps,
			// so the chain up to the nearest existing ancestor is created
			// the way a replica's mkdir -p would be.
			if err := e.ensureMountParent(fs); err != nil {
				fs.state = stateBusy
				fs.busy = api.BusyNotDir
				fs.root = detached
				e.logger.Error("create mount path parents", "folder", fs.name, "path", fs.mount, "error", err)
				return
			}
		case serr != nil:
			fs.state = stateBusy
			fs.busy = api.BusyNotDir
			fs.root = detached
			e.logger.Error("inspect mount path", "folder", fs.name, "path", fs.mount, "error", serr)
			return
		case info.IsDir():
			// An EMPTY directory is a spent mount point (the samba mounts
			// sat on exactly these, and a retired cifs mount leaves one
			// behind): nothing to protect, adopt it. Anything with content
			// is somebody's data — never touch.
			entries, derr := os.ReadDir(fs.mount)
			if derr != nil || len(entries) != 0 {
				fs.state = stateBusy
				fs.busy = api.BusyContent
				fs.root = detached
				e.logger.Warn("mount path has content; leaving it alone", "folder", fs.name, "path", fs.mount)
				return
			}
			if err := os.Remove(fs.mount); err != nil {
				fs.state = stateBusy
				fs.root = detached
				if fstype := mountFSType(fs.mount); fstype != "" {
					fs.busy = api.BusyMount
					e.logger.Warn("mount path is still a mount point; something must unmount it first",
						"folder", fs.name, "path", fs.mount, "fstype", fstype)
					return
				}
				fs.busy = api.BusyNoRemove
				e.logger.Warn("mount path is an empty dir that cannot be removed; leaving it alone", "folder", fs.name, "path", fs.mount, "error", err)
				return
			}
			e.logger.Info("adopted empty mount path", "folder", fs.name, "path", fs.mount)
		default:
			fs.state = stateBusy
			fs.busy = api.BusyNotDir
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

// ensureStorage prepares a folder's storage directory and migrates what is
// still at the mount path into it. The move is the same entry-by-entry,
// never-overwrite one takeover uses, so an interrupted migration leaves both
// sides intact and the next transition picks up where it stopped.
func (e *Engine) ensureStorage(fs *FolderState) (int, error) {
	// The chain up to the storage root is the daemon's layout, not the
	// user's data: missing levels are created traverse-only (0711) so the
	// mount symlink resolves for its owner, and the root itself gets the
	// folder's resolved owner and mode. Without this a storage directory
	// under the data directory is unreachable from the mount ("Permission
	// denied" on ls) — the data dir is 0750 by default.
	if err := e.mkdirChain(fs, fs.storage, 0o711, false); err != nil {
		return 0, err
	}
	ensureTraversable(e.dataDir, fs.storage)
	e.applyDir(fs, fs.storage)

	// Content left in the legacy view directory (dataDir/views/<folder>,
	// where streamed content lived before storage directories existed) moves
	// into the storage directory whatever the mount points at: a folder that
	// was detached at upgrade time would otherwise start with an empty
	// storage root and a full journal.
	if legacy := e.legacyViewDir(fs); legacy != fs.storage {
		if _, err := os.Stat(legacy); err == nil {
			moved, err := e.moveEntries(fs, legacy, fs.storage)
			if err != nil {
				return moved, err
			}
			if moved > 0 {
				e.logger.Info("moved the legacy view into the storage directory",
					"folder", fs.name, "from", legacy, "to", fs.storage, "moved", moved)
			}
			if rest, _ := os.ReadDir(legacy); len(rest) == 0 {
				_ = os.Remove(legacy)
			}
		}
	}

	// Nothing to migrate when the mount is already our symlink. The legacy
	// view directory (dataDir/views/<folder>, where streamed content lived
	// before storage directories existed) counts as ours: its content moves
	// into the storage directory and the symlink is replaced.
	if target, err := os.Readlink(fs.mount); err == nil {
		switch target {
		case fs.storage:
			// The steady state. Logged once per start so the journal says
			// what was found, not only what was changed — the question
			// "where did that content go" deserves an answer from the log.
			e.logger.Info("mount path already points at the storage root", "folder", fs.name, "path", fs.mount, "storage", fs.storage)
			return 0, nil
		case e.detachedDir(fs):
			e.logger.Info("mount path points at the offline scratchpad", "folder", fs.name, "path", fs.mount, "scratchpad", target)
			return 0, nil
		}
		if target == e.legacyViewDir(fs) {
			// The content already moved above; here the stale symlink gives
			// way to the storage root.
			_ = os.Remove(target)
			if err := symlinkSwap(fs.mount, fs.storage); err != nil {
				return 0, err
			}
			e.chownLink(fs, fs.mount)
			e.logger.Info("mount path now points at the storage root (was the legacy view)",
				"folder", fs.name, "path", fs.mount, "storage", fs.storage)
			return 0, nil
		}
		// A symlink we did not make: not ours to reorganize.
		return 0, nil
	}
	if fstype := mountFSType(fs.mount); fstype != "" {
		return 0, fmt.Errorf("%s is a %s mount: unmount it (or let the takeover unit) before its content can move to %s", fs.mount, fstype, fs.storage)
	}
	info, err := os.Lstat(fs.mount)
	if err != nil {
		if os.IsNotExist(err) {
			e.logger.Info("mount path is not present yet", "folder", fs.name, "path", fs.mount, "storage", fs.storage)
			return 0, nil // nothing there: the symlink is the only thing to add
		}
		return 0, err
	}
	if !info.IsDir() {
		return 0, fmt.Errorf("%s is not a directory: move it aside before its content can move to %s", fs.mount, fs.storage)
	}
	entries, err := os.ReadDir(fs.mount)
	if err != nil {
		return 0, err
	}
	if len(entries) == 0 {
		// A spent mount point: adopt the path.
		if err := os.Remove(fs.mount); err != nil {
			return 0, err
		}
		e.logger.Info("adopted empty mount path", "folder", fs.name, "path", fs.mount)
		return 0, nil
	}

	moved, err := e.moveEntries(fs, fs.mount, fs.storage)
	if err != nil {
		return moved, err
	}
	if rest, rerr := os.ReadDir(fs.mount); rerr != nil || len(rest) != 0 {
		return moved, fmt.Errorf("%s still has entries; nothing was lost, the migration continues on the next transition", fs.mount)
	}
	if err := os.Remove(fs.mount); err != nil {
		return moved, fmt.Errorf("remove empty %s: %w", fs.mount, err)
	}
	e.logger.Info("migrated folder content into the storage directory",
		"folder", fs.name, "from", fs.mount, "to", fs.storage, "moved", moved)
	return moved, nil
}

// moveEntries moves every top-level entry of src into dst: recursively for
// directories, never following symlinks and never replacing what is already
// there (a collision is parked under the deterministic conflict name).
func (e *Engine) moveEntries(fs *FolderState, src, dst string) (int, error) {
	entries, err := os.ReadDir(src)
	if err != nil {
		return 0, err
	}
	moved := 0
	for _, ent := range entries {
		if err := e.takeoverMove(fs, filepath.Join(src, ent.Name()), filepath.Join(dst, ent.Name())); err != nil {
			e.logger.Warn("entry left in place", "folder", fs.name, "entry", ent.Name(), "error", err)
			continue
		}
		moved++
	}
	return moved, nil
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
	fs.perm = e.resolvePerms(fs)
	view, detached := e.viewsDir(fs), e.detachedDir(fs)
	// Anything an older layout left in the scratchpad merges into the view
	// before the folder goes live, storage-backed or not.
	if err := e.mergeDetached(fs, detached, view); err != nil {
		e.logger.Error("remerge", "folder", fs.name, "error", err)
	}
	e.applyDir(fs, view)
	if fs.storage == "" {
		// The mount has to be flipped onto the live view.
		if err := symlinkSwap(fs.mount, view); err != nil {
			e.logger.Error("attach", "folder", fs.name, "error", err)
			return
		}
		e.chownLink(fs, fs.mount)
	}
	fs.state = stateAttached
	fs.busy = ""
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
	fs.perm = e.resolvePerms(fs)
	view := e.viewsDir(fs)
	if fs.storage != "" {
		// The view stays mounted: losing the holder means "not live", not
		// "content disappears". Writes keep landing in the folder and sync
		// when a holder returns.
		fs.state = stateDetached
		fs.busy = ""
		fs.root = view
		e.logger.Info("folder detached (no holder reachable)", "folder", fs.name, "mount", fs.mount)
		return
	}
	detached := e.detachedDir(fs)
	if err := os.MkdirAll(detached, 0o700); err != nil {
		e.logger.Error("detach", "folder", fs.name, "error", err)
		return
	}
	ensureTraversable(e.dataDir, detached)
	e.applyDir(fs, detached)
	if err := symlinkSwap(fs.mount, detached); err != nil {
		e.logger.Error("detach", "folder", fs.name, "error", err)
		return
	}
	e.chownLink(fs, fs.mount)
	fs.state = stateDetached
	fs.busy = ""
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
			if err := e.ensureDir(fs, filepath.Dir(target)); err != nil {
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
			// Byte-identical is not a conflict, whatever the timestamps say:
			// the same file arriving from both sides is one file. Keep the
			// newer timestamp and drop the duplicate — the same rule the
			// mesh uses to pick a conflict winner, applied before inventing
			// one.
			if existing, eerr := store.SumFile(target); eerr == nil && existing == sum {
				if ierr == nil && srcInfo.ModTime().After(st.ModTime()) {
					when := srcInfo.ModTime()
					if cerr := os.Chtimes(target, when, when); cerr != nil {
						e.logger.Warn("keep newer mtime", "folder", fs.name, "path", target, "error", cerr)
					}
				}
				os.Remove(path)
				return nil
			}
			dst := conflictPath(target, sum)
			if err := e.ensureDir(fs, filepath.Dir(dst)); err != nil {
				return err
			}
			if err := os.Rename(path, dst); err != nil {
				return nil
			}
			e.chownPath(fs, dst)
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

// viewsDir / detachedDir are the daemon-owned halves of a folder: the live
// content and the offline scratchpad. A folder with a storage directory
// keeps its content there (the path the config names); everything else keeps
// it under the one data directory.
func (e *Engine) viewsDir(fs *FolderState) string {
	if fs.storage != "" {
		return fs.storage
	}
	return filepath.Join(e.dataDir, "views", fs.name)
}

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

// mountFSType reports the filesystem type mounted exactly at path, or "".
// It reads /proc/self/mountinfo so a "busy" diagnosis names the offender
// (a leftover cifs mount, the samba placeholder tmpfs, anything else).
func mountFSType(path string) string {
	data, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return ""
	}
	want := filepath.Clean(path)
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 10 {
			continue
		}
		sep := -1
		for i, f := range fields {
			if f == "-" {
				sep = i
				break
			}
		}
		if sep < 0 || sep+1 >= len(fields) {
			continue
		}
		point := unescapeMount(fields[4])
		if filepath.Clean(point) == want {
			return fields[sep+1]
		}
	}
	return ""
}

// unescapeMount decodes the octal escapes mountinfo uses for spaces and the
// like in paths.
func unescapeMount(s string) string {
	return strings.NewReplacer(
		`\040`, " ",
		`\011`, "\t",
		`\012`, "\n",
		`\134`, `\`,
	).Replace(s)
}

// chownPath best-efforts ownership of a created entry to the folder's
// resolved owner, so the human can actually use it.
func (e *Engine) chownPath(fs *FolderState, path string) {
	if fs.perm.uid < 0 {
		return
	}
	_ = os.Chown(path, fs.perm.uid, fs.perm.gid)
}

// chownLink is chownPath for a symlink itself.
func (e *Engine) chownLink(fs *FolderState, path string) {
	if fs.perm.uid < 0 {
		return
	}
	_ = os.Lchown(path, fs.perm.uid, fs.perm.gid)
}

// mkdirChain creates every missing component of dir, top down, giving each
// new level mode — chmod'ed explicitly, since Mkdir respects the umask — and,
// when apply is set, the folder's resolved owner and mode (user-facing paths:
// a mount's parents). Returns the levels it created.
func (e *Engine) mkdirChain(fs *FolderState, dir string, mode os.FileMode, apply bool) error {
	var missing []string
	for p := filepath.Clean(dir); p != "/" && p != "." && p != ""; p = filepath.Dir(p) {
		if _, err := os.Stat(p); err == nil {
			break
		}
		missing = append(missing, p)
	}
	for i := len(missing) - 1; i >= 0; i-- {
		if err := os.Mkdir(missing[i], mode); err != nil {
			return err
		}
		if err := os.Chmod(missing[i], mode); err != nil {
			return err
		}
		e.logger.Info("created path", "folder", fs.name, "path", missing[i])
		if apply {
			e.applyDir(fs, missing[i])
		}
	}
	return nil
}

// ensureMountParent creates the directories between a streamed mount path
// and the nearest existing ancestor, so a declared mount point works on a
// machine that has never had one (the whole ~/remote style tree is the
// daemon's to make, per the config's declaration). The new levels get the
// folder's resolved mode — and, once they exist, they become the reference
// the folder's own permissions are inherited from, exactly as if the user
// had made them.
func (e *Engine) ensureMountParent(fs *FolderState) error {
	return e.mkdirChain(fs, filepath.Dir(fs.mount), 0o755, true)
}

// legacyViewDir is where a streamed folder's content lived before storage
// directories existed. Upgrades meet it: the mount may still be a symlink
// onto it.
func (e *Engine) legacyViewDir(fs *FolderState) string {
	return filepath.Join(e.dataDir, "views", fs.name)
}
