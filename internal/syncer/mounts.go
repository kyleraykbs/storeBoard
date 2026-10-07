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
	if err := e.mergeDetached(fs, detached, view); err != nil {
		e.logger.Error("remerge", "folder", fs.name, "error", err)
	}
	e.applyDir(fs, view)
	if err := symlinkSwap(fs.mount, view); err != nil {
		e.logger.Error("attach", "folder", fs.name, "error", err)
		return
	}
	e.chownLink(fs, fs.mount)
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

// ensureMountParent creates the directories between a streamed mount path
// and the nearest existing ancestor, so a declared mount point works on a
// machine that has never had one (the whole ~/remote style tree is the
// daemon's to make, per the config's declaration). The new levels get the
// folder's resolved mode — and, once they exist, they become the reference
// the folder's own permissions are inherited from, exactly as if the user
// had made them.
func (e *Engine) ensureMountParent(fs *FolderState) error {
	parent := filepath.Dir(fs.mount)
	var missing []string
	for p := parent; p != "/" && p != "." && p != ""; p = filepath.Dir(p) {
		if _, err := os.Stat(p); err == nil {
			break
		}
		missing = append(missing, p)
	}
	for i := len(missing) - 1; i >= 0; i-- {
		if err := os.Mkdir(missing[i], 0o755); err != nil {
			return err
		}
		e.logger.Info("created mount path parent", "folder", fs.name, "path", missing[i])
		e.applyDir(fs, missing[i])
	}
	return nil
}
