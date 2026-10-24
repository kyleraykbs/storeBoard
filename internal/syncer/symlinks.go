package syncer

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"codeberg.org/kyleraykbs/storeboard/internal/store"
	"codeberg.org/kyleraykbs/storeboard/pkg/api"
)

// settleLinks makes the tree real: this store's declared external symlinks,
// and a symlink inside it for each child store. Nothing is ever overwritten —
// a path that belongs to something else is reported busy and left alone.
func (e *Engine) settleLinks(fs *StoreState) {
	states := make([]api.StoreSymlink, 0, len(fs.symlinks))
	for _, path := range fs.symlinks {
		states = append(states, e.settleOne(fs, fs, path, fs.root))
	}
	fs.metaMu.Lock()
	fs.links = states
	fs.metaMu.Unlock()

	// Present this store's children inside it. This is the only thing that
	// composes the tree; the canonical dirs stay flat under root.
	for _, child := range e.stores {
		if child.parent != fs.name {
			continue
		}
		e.settleOne(child, fs, filepath.Join(fs.root, child.path), child.root)
	}
}

// settleOne makes path a symlink to target, or reports why it is not. owner
// is the store the link belongs to (permissions, logging); dirOwner governs
// directories created on the way (a child link inherits the parent's).
func (e *Engine) settleOne(owner, dirOwner *StoreState, path, target string) api.StoreSymlink {
	st := api.StoreSymlink{Path: path}
	target = filepath.Clean(target)

	if err := e.mkdirChain(dirOwner, filepath.Dir(path), 0o755, true); err != nil {
		st.State = api.SymlinkAbsent
		e.logger.Warn("cannot create symlink parent", "store", owner.name, "path", path, "error", err)
		return st
	}

	if got, err := os.Readlink(path); err == nil {
		if got == target {
			st.State = api.SymlinkLinked
			return st
		}
		st.State, st.Busy = api.SymlinkBusy, api.BusySymlink
		e.logger.Warn("symlink path points elsewhere; leaving it alone",
			"store", owner.name, "path", path, "target", got, "want", target)
		return st
	}

	info, err := os.Lstat(path)
	switch {
	case err == nil:
		if !info.IsDir() {
			st.State, st.Busy = api.SymlinkBusy, api.BusyNotDir
			e.logger.Warn("symlink path is occupied; leaving it alone", "store", owner.name, "path", path)
			return st
		}
		if fstype := mountFSType(path); fstype != "" {
			st.State, st.Busy = api.SymlinkBusy, api.BusyMount
			e.logger.Warn("symlink path is a mount point; leaving it alone",
				"store", owner.name, "path", path, "fstype", fstype)
			return st
		}
		entries, derr := os.ReadDir(path)
		if derr != nil || len(entries) != 0 {
			st.State, st.Busy = api.SymlinkBusy, api.BusyContent
			e.logger.Warn("symlink path has content; leaving it alone (store takeover resolves this)",
				"store", owner.name, "path", path)
			return st
		}
		if rerr := os.Remove(path); rerr != nil {
			st.State, st.Busy = api.SymlinkBusy, api.BusyNoRemove
			e.logger.Warn("empty path cannot be removed; leaving it alone", "store", owner.name, "path", path, "error", rerr)
			return st
		}
		e.logger.Info("adopted empty path", "store", owner.name, "path", path)
	case !os.IsNotExist(err):
		st.State, st.Busy = api.SymlinkBusy, api.BusyNotDir
		e.logger.Warn("inspect symlink path", "store", owner.name, "path", path, "error", err)
		return st
	}

	if err := os.Symlink(target, path); err != nil {
		st.State = api.SymlinkAbsent
		e.logger.Warn("create symlink", "store", owner.name, "path", path, "target", target, "error", err)
		return st
	}
	e.chownLink(owner, path)
	st.State = api.SymlinkLinked
	e.logger.Info("linked", "store", owner.name, "path", path, "target", target)
	return st
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
// It reads /proc/self/mountinfo so a "busy" diagnosis names the offender (a
// leftover cifs mount, the samba placeholder tmpfs, anything else).
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

// chownPath best-efforts ownership of a created entry to the store's resolved
// owner, so the human can actually use it.
func (e *Engine) chownPath(fs *StoreState, path string) {
	if fs.perm.uid < 0 {
		return
	}
	_ = os.Chown(path, fs.perm.uid, fs.perm.gid)
}

// chownLink is chownPath for a symlink itself.
func (e *Engine) chownLink(fs *StoreState, path string) {
	if fs.perm.uid < 0 {
		return
	}
	_ = os.Lchown(path, fs.perm.uid, fs.perm.gid)
}

// mkdirChain creates every missing component of dir, top down, giving each
// new level mode — chmod'ed explicitly, since Mkdir respects the umask — and,
// when apply is set, the store's resolved owner and mode.
func (e *Engine) mkdirChain(fs *StoreState, dir string, mode os.FileMode, apply bool) error {
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
		if apply {
			e.applyDir(fs, missing[i])
		}
	}
	return nil
}

// moveEntries moves every top-level entry of src into dst: recursively for
// directories, never following symlinks and never replacing what is already
// there (a collision is parked under the deterministic conflict name).
func (e *Engine) moveEntries(fs *StoreState, src, dst string) (int, error) {
	entries, err := os.ReadDir(src)
	if err != nil {
		return 0, err
	}
	moved := 0
	for _, ent := range entries {
		if err := e.takeoverMove(fs, filepath.Join(src, ent.Name()), filepath.Join(dst, ent.Name())); err != nil {
			e.logger.Warn("entry left in place", "store", fs.name, "entry", ent.Name(), "error", err)
			continue
		}
		moved++
	}
	return moved, nil
}

// takeoverMove moves one entry into dst: recursively for directories,
// without following symlinks and without ever replacing what is already
// there.
func (e *Engine) takeoverMove(fs *StoreState, src, dst string) error {
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

// parkAside renames an entry out of the way, under the deterministic conflict
// name for its content where one can be computed (a directory gets a numbered
// suffix): a takeover never silently replaces anything.
func (e *Engine) parkAside(path string) error {
	if sum, err := store.SumFile(path); err == nil {
		return os.Rename(path, conflictPath(path, sum))
	}
	for i := 1; ; i++ {
		alt := path + ".storeboard-kept-" + fmt.Sprint(i)
		if _, err := os.Lstat(alt); os.IsNotExist(err) {
			return os.Rename(path, alt)
		}
	}
}
