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

// perms is a store's resolved ownership and mode: what the engine gives
// every entry it creates — the mount target, the directories under it and
// the files materialised into it. A uid or gid of -1 means "leave
// ownership alone".
type perms struct {
	uid, gid int
	dirMode  os.FileMode
}

func noPerms() perms { return perms{uid: -1, gid: -1, dirMode: 0o755} }

// resolvePerms computes a store's permissions: this machine's per-store
// declaration over the store's effective declaration (own, then nearest
// ancestor) over the directory the canonical dir lives in — its owner, its
// group, its mode. The directory is re-read on every call, which is what
// makes an inherited permission track its parent: a chown or chmod of the
// container is picked up at the next scan or apply. A declared name that does
// not resolve is a warning, not a failure: the inherited value stands.
func (e *Engine) resolvePerms(fs *StoreState) perms {
	// The directory the root lives in is the stable reference. The root and
	// the canonical dirs below it are daemon-owned, so reading either would
	// lock in whatever mode they happen to have; the container is the user's
	// own directory and is re-read on every call.
	probe := nearestExisting(filepath.Dir(e.cfg.Root))
	p := noPerms()
	if info, err := os.Stat(probe); err == nil {
		if st, ok := info.Sys().(*syscall.Stat_t); ok {
			p.uid, p.gid = int(st.Uid), int(st.Gid)
		}
		p.dirMode = info.Mode().Perm()
	}

	decl := e.cfg.PermissionsFor(fs.name)
	if decl == nil {
		return p
	}
	if decl.User != "" {
		if uid, err := lookupUser(decl.User); err == nil {
			p.uid = uid
		} else {
			e.logger.Warn("permissions: unknown user; inheriting instead", "store", fs.name, "user", decl.User, "error", err)
		}
	}
	if decl.Group != "" {
		if gid, err := lookupGroup(decl.Group); err == nil {
			p.gid = gid
		} else {
			e.logger.Warn("permissions: unknown group; inheriting instead", "store", fs.name, "group", decl.Group, "error", err)
		}
	}
	if decl.Mode != "" {
		if m, err := config.ParseMode(decl.Mode); err == nil {
			p.dirMode = m
		} else {
			e.logger.Warn("permissions: bad mode; inheriting instead", "store", fs.name, "mode", decl.Mode, "error", err)
		}
	}
	return p
}

// nearestExisting walks up from dir to the first directory that exists, so a
// root declared under a path the daemon has not made yet still inherits from
// something real.
func nearestExisting(dir string) string {
	for {
		if _, err := os.Stat(dir); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir || parent == "." {
			return dir
		}
		dir = parent
	}
}

// healRoot repairs the container itself, not every file below it. A blanket
// chown of the store tree would walk hundreds of gigabytes at startup;
// descendants are permissioned when created, and each store root is handled
// by applyDir in prepareRoot.
func (e *Engine) healRoot(fs *StoreState) {
	info, err := os.Lstat(e.cfg.Root)
	if err != nil || !info.IsDir() {
		return
	}
	if fs.perm.uid >= 0 {
		if err := os.Chown(e.cfg.Root, fs.perm.uid, fs.perm.gid); err != nil {
			e.logger.Warn("chown root", "path", e.cfg.Root, "error", err)
		}
	}
	if st, err := os.Stat(e.cfg.Root); err == nil {
		if err := os.Chmod(e.cfg.Root, st.Mode().Perm()|0o700); err != nil {
			e.logger.Warn("chmod root", "path", e.cfg.Root, "error", err)
		}
	}
}

// applyDir makes path match the store's resolved permissions: chmod to the
// resolved directory mode, chown to the resolved owner. Best-effort — a
// failure is logged, never fatal: permissions must not stop a sync.
func (e *Engine) applyDir(fs *StoreState, path string) {
	if _, err := os.Stat(path); err != nil {
		return
	}
	if err := os.Chmod(path, fs.perm.dirMode); err != nil {
		e.logger.Warn("chmod", "store", fs.name, "path", path, "error", err)
	}
	if fs.perm.uid >= 0 {
		if err := os.Chown(path, fs.perm.uid, fs.perm.gid); err != nil {
			e.logger.Warn("chown", "store", fs.name, "path", path, "error", err)
		}
	}
}

// ensureDir creates dir and every missing parent, then gives each component
// under the store root the store's permissions, so a path materialised from
// the mesh is usable all the way down — not just at the leaf.
func (e *Engine) ensureDir(fs *StoreState, dir string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	rel, err := filepath.Rel(fs.root, dir)
	if err != nil || strings.HasPrefix(rel, "..") {
		// Outside this store's root (a restore target, a conflict copy
		// parked elsewhere): permission the leaf alone.
		e.applyDir(fs, dir)
		return nil
	}
	// The canonical root is the daemon's own directory, so it is permissioned
	// too; everything below it follows.
	e.applyDir(fs, fs.root)
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
