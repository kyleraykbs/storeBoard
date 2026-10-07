package syncer

import (
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"codeberg.org/kyleraykbs/storeboard/internal/config"
	"codeberg.org/kyleraykbs/storeboard/pkg/api"
)

// perms is a folder's resolved ownership and mode: what the engine gives
// every entry it creates — the mount target, the directories under it and
// the files materialised into it. A uid or gid of -1 means "leave
// ownership alone".
type perms struct {
	uid, gid int
	dirMode  os.FileMode
}

func noPerms() perms { return perms{uid: -1, gid: -1, dirMode: 0o755} }

// resolvePerms computes a folder's permissions: this machine's per-folder
// declaration over the group's default over the directory the mount lives
// in — its owner, its group, its mode. The directory is re-read on every
// call, which is what makes an inherited permission track its parent: a
// chown or chmod of the containing directory is picked up at the next mount
// transition, scan or apply. A declared name that does not resolve is a
// warning, not a failure: the inherited value stands.
func (e *Engine) resolvePerms(fs *FolderState) perms {
	// The directory the mount lives in is the reference: for a streamed
	// folder that is the parent of the symlink (the user-facing directory
	// full of siblings); for a replica it is the folder itself, because
	// that directory IS the user's.
	probe := fs.mount
	if fs.streamed {
		probe = filepath.Dir(fs.mount)
	}
	p := noPerms()
	if info, err := os.Stat(probe); err == nil {
		if st, ok := info.Sys().(*syscall.Stat_t); ok {
			p.uid, p.gid = int(st.Uid), int(st.Gid)
		}
		p.dirMode = info.Mode().Perm()
	}

	decl := e.cfg.PermissionsFor(fs.name, fs.group)
	if decl == nil {
		return p
	}
	if decl.User != "" {
		if uid, err := lookupUser(decl.User); err == nil {
			p.uid = uid
		} else {
			e.logger.Warn("permissions: unknown user; inheriting instead", "folder", fs.name, "user", decl.User, "error", err)
		}
	}
	if decl.Group != "" {
		if gid, err := lookupGroup(decl.Group); err == nil {
			p.gid = gid
		} else {
			e.logger.Warn("permissions: unknown group; inheriting instead", "folder", fs.name, "group", decl.Group, "error", err)
		}
	}
	if decl.Mode != "" {
		if m, err := config.ParseMode(decl.Mode); err == nil {
			p.dirMode = m
		} else {
			e.logger.Warn("permissions: bad mode; inheriting instead", "folder", fs.name, "mode", decl.Mode, "error", err)
		}
	}
	return p
}

// applyDir makes path match the folder's resolved permissions: chmod to the
// resolved directory mode, chown to the resolved owner. Best-effort — a
// failure is logged, never fatal: permissions must not stop a sync.
func (e *Engine) applyDir(fs *FolderState, path string) {
	if _, err := os.Stat(path); err != nil {
		return
	}
	if err := os.Chmod(path, fs.perm.dirMode); err != nil {
		e.logger.Warn("chmod", "folder", fs.name, "path", path, "error", err)
	}
	if fs.perm.uid >= 0 {
		if err := os.Chown(path, fs.perm.uid, fs.perm.gid); err != nil {
			e.logger.Warn("chown", "folder", fs.name, "path", path, "error", err)
		}
	}
}

// ensureDir creates dir and every missing parent, then gives each component
// under the folder root the folder's permissions, so a path materialised
// from the mesh is usable all the way down — not just at the leaf.
func (e *Engine) ensureDir(fs *FolderState, dir string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	rel, err := filepath.Rel(fs.root, dir)
	if err != nil || strings.HasPrefix(rel, "..") {
		// Outside this folder's root (a conflict copy parked elsewhere, a
		// mounted path): permission the leaf alone.
		e.applyDir(fs, dir)
		return nil
	}
	// The root itself is left alone: for a replica it is the user's own
	// directory, never ours to chmod.
	cur := fs.root
	for _, part := range strings.Split(rel, string(filepath.Separator)) {
		if part == "" || part == "." {
			continue
		}
		cur = filepath.Join(cur, part)
		e.applyDir(fs, cur)
	}
	return nil
}

// ensureTraversable opens every directory between base and dir (exclusive)
// for traversal only — 0711, listable by nobody but walkable by whoever
// knows the way. The mount symlink points into the storage directory, so
// without a traversable chain the resolved permissions on the target would
// be moot: the user could not reach it.
func ensureTraversable(base, dir string) {
	rel, err := filepath.Rel(base, filepath.Dir(dir))
	if err != nil || strings.HasPrefix(rel, "..") {
		return
	}
	_ = os.Chmod(base, 0o711)
	cur := base
	for _, part := range strings.Split(rel, string(filepath.Separator)) {
		if part == "" || part == "." {
			continue
		}
		cur = filepath.Join(cur, part)
		_ = os.Chmod(cur, 0o711)
	}
}

// apiPerms renders the resolved permissions for the status document: names
// where they resolve, numbers where they do not, and the mode in octal.
func (p perms) apiPerms() api.Permissions {
	out := api.Permissions{}
	if p.uid >= 0 {
		out.User = strconv.Itoa(p.uid)
		if u, err := user.LookupId(out.User); err == nil {
			out.User = u.Username
		}
	}
	if p.gid >= 0 {
		out.Group = strconv.Itoa(p.gid)
		if g, err := user.LookupGroupId(out.Group); err == nil {
			out.Group = g.Name
		}
	}
	out.Mode = fmt.Sprintf("%04o", p.dirMode)
	return out
}

// lookupUser resolves a user name or a numeric uid.
func lookupUser(s string) (int, error) {
	if n, err := strconv.Atoi(s); err == nil {
		return n, nil
	}
	u, err := user.Lookup(s)
	if err != nil {
		return -1, err
	}
	n, err := strconv.Atoi(u.Uid)
	if err != nil {
		return -1, fmt.Errorf("uid %q: %w", u.Uid, err)
	}
	return n, nil
}

// lookupGroup resolves a group name or a numeric gid.
func lookupGroup(s string) (int, error) {
	if n, err := strconv.Atoi(s); err == nil {
		return n, nil
	}
	g, err := user.LookupGroup(s)
	if err != nil {
		return -1, err
	}
	n, err := strconv.Atoi(g.Gid)
	if err != nil {
		return -1, fmt.Errorf("gid %q: %w", g.Gid, err)
	}
	return n, nil
}
