package syncer

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"codeberg.org/kyleraykbs/storeboard/internal/config"
	"codeberg.org/kyleraykbs/storeboard/internal/store"
	"codeberg.org/kyleraykbs/storeboard/internal/transport"
	"codeberg.org/kyleraykbs/storeboard/pkg/api"
)

// testWait bounds every waitFor in this file. It is deliberately far larger
// than any single step needs (a scan tick, one round trip, one apply): the
// deploy builds run these tests on machines that are also syncing large
// folders, and a starved 50 ms scan tick can take seconds. The waits are
// assertions that a thing eventually happens, not that it happens fast.
const testWait = 90 * time.Second

// node is one daemon's engine + wire in a test.
type node struct {
	t      *testing.T
	name   string
	dir    string
	cfg    *config.Config
	eng    *Engine
	mgr    *transport.Manager
	cancel context.CancelFunc
}

// twoEndpoints returns two distinct loopback addresses. The listeners stay
// open while both are allocated: asking one at a time lets the kernel hand
// out the same port twice, which made one daemon dial itself.
func twoEndpoints(t *testing.T) (string, string) {
	t.Helper()
	l1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l1.Close()
	l2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	return l1.Addr().String(), l2.Addr().String()
}

// start brings the engine and its transport up.
func startNode(t *testing.T, n *node) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	n.cancel = cancel
	var logger *slog.Logger
	if os.Getenv("STOREBOARD_TEST_LOG") != "" {
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	n.mgr = transport.New(n.cfg, logger)
	n.mgr.SetHandler(n.eng)
	go n.eng.Run(ctx)
	go n.mgr.Run(ctx)
	t.Cleanup(func() { n.stop() })
}

func (n *node) stop() {
	if n.cancel == nil {
		return
	}
	n.cancel()
	n.cancel = nil
	n.mgr.Close()
	n.eng.Close()
}

// mkNode builds a two-peer config: both in group g, folder docs with the
// given mount and keepOffline value.
func mkNode(t *testing.T, name, other, epSelf, epOther, mount string, docsKeep bool) *node {
	t.Helper()
	dir := t.TempDir()
	// Old layout: these tests pin content at the mount path, which an empty
	// storageDir asks for; the default is a storage directory under dataDir.
	noStorage := ""
	cfg := &config.Config{
		ConfigVersion: config.ConfigVersion,
		DataDir:       filepath.Join(dir, "data"),
		Socket:        filepath.Join(dir, "s.sock"),
		Peer:          name,
		StorageDir:    &noStorage,
		Peers: map[string]config.Peer{
			name:  {Endpoints: []string{epSelf}},
			other: {Endpoints: []string{epOther}},
		},
		Groups: map[string]config.Group{
			"g": {
				Peers:   []string{name, other},
				Folders: map[string]config.GroupFolder{"docs": {Mount: mount}},
			},
		},
		Mounts:      map[string]string{"docs": mount},
		KeepOffline: map[string]bool{"docs": docsKeep},
	}
	var logger *slog.Logger
	if os.Getenv("STOREBOARD_TEST_LOG") != "" {
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	eng, err := New(cfg, logger)
	if err != nil {
		t.Fatal(err)
	}
	return &node{t: t, name: name, dir: dir, cfg: cfg, eng: eng}
}

func waitFor(t *testing.T, d time.Duration, what string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func writeFile(t *testing.T, path, content string, mtime time.Time) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, mtime, mtime); err != nil {
		t.Fatal(err)
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func TestReplicaSyncsEditsAndDeletes(t *testing.T) {
	old := scanInterval
	scanInterval = 50 * time.Millisecond
	defer func() { scanInterval = old }()

	epA, epB := twoEndpoints(t)
	rootA := filepath.Join(t.TempDir(), "docs")
	rootB := filepath.Join(t.TempDir(), "docs")

	a := mkNode(t, "a", "b", epA, epB, rootA, true)
	b := mkNode(t, "b", "a", epB, epA, rootB, true)
	startNode(t, a)
	startNode(t, b)

	// A creates a file: B must receive the content.
	writeFile(t, filepath.Join(rootA, "hello.txt"), "one", time.Now())
	waitFor(t, testWait, "file on B", func() bool {
		return exists(filepath.Join(rootB, "hello.txt"))
	})
	if got := readFile(t, filepath.Join(rootB, "hello.txt")); got != "one" {
		t.Fatalf("B content = %q", got)
	}

	// B edits it: A must converge on the newer content.
	time.Sleep(150 * time.Millisecond)
	writeFile(t, filepath.Join(rootB, "hello.txt"), "two", time.Now().Add(2*time.Second))
	waitFor(t, testWait, "edit on A", func() bool {
		p := filepath.Join(rootA, "hello.txt")
		data, err := os.ReadFile(p)
		return err == nil && string(data) == "two"
	})

	// Nothing about a clean create→edit sequence is a conflict.
	for _, root := range []string{rootA, rootB} {
		if names, _ := filepath.Glob(filepath.Join(root, "hello.txt.conflict-*")); len(names) != 0 {
			t.Fatalf("false conflict on %s: %v", root, names)
		}
	}

	// A deletes it: B's copy goes to the trash, not to nowhere.
	if err := os.Remove(filepath.Join(rootA, "hello.txt")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, testWait, "delete propagated", func() bool {
		return !exists(filepath.Join(rootB, "hello.txt"))
	})
	waitFor(t, testWait, "trashed copy", func() bool {
		return exists(filepath.Join(b.cfg.DataDir, "trash", "docs", "hello.txt"))
	})
	if got := readFile(t, filepath.Join(b.cfg.DataDir, "trash", "docs", "hello.txt")); got != "two" {
		t.Fatalf("trashed content = %q", got)
	}
}

func TestConcurrentWritesConflictAndConverge(t *testing.T) {
	old := scanInterval
	scanInterval = 50 * time.Millisecond
	defer func() { scanInterval = old }()

	epA, epB := twoEndpoints(t)
	rootA := filepath.Join(t.TempDir(), "docs")
	rootB := filepath.Join(t.TempDir(), "docs")

	a := mkNode(t, "a", "b", epA, epB, rootA, true)
	b := mkNode(t, "b", "a", epB, epA, rootB, true)

	// Both sides change the same path while disconnected; B's version is
	// newer, so it must win on both machines, and A's text must survive as
	// a conflict copy on both.
	base := time.Now().Add(-time.Hour)
	writeFile(t, filepath.Join(rootA, "note.txt"), "AAA", base)
	writeFile(t, filepath.Join(rootB, "note.txt"), "BBB", base.Add(time.Minute))

	startNode(t, a)
	startNode(t, b)

	conflictIn := func(root string) string {
		names, _ := filepath.Glob(filepath.Join(root, "note.txt.conflict-*"))
		if len(names) != 1 {
			return ""
		}
		return names[0]
	}

	converged := func(root string) bool {
		data, err := os.ReadFile(filepath.Join(root, "note.txt"))
		if err != nil || string(data) != "BBB" {
			return false
		}
		c := conflictIn(root)
		if c == "" {
			return false
		}
		cd, err := os.ReadFile(c)
		return err == nil && string(cd) == "AAA"
	}
	for _, n := range []struct {
		name string
		root string
	}{{"a", rootA}, {"b", rootB}} {
		root := n.root
		waitFor(t, testWait, "conflict convergence on "+n.name, func() bool {
			return converged(root)
		})
	}

	// The conflict name is content-addressed, so both sides agree on it.
	nameA := filepath.Base(conflictIn(rootA))
	nameB := filepath.Base(conflictIn(rootB))
	if nameA != nameB {
		t.Fatalf("conflict names differ: %s vs %s", nameA, nameB)
	}

	// And the conflict counter is visible in the status document.
	snap := a.eng.Snapshot()
	if len(snap) != 1 || snap[0].Conflicts == 0 {
		t.Fatalf("snapshot = %+v, want a conflict counted", snap)
	}
}

func TestStreamedFolderAttachDetachAndRemerge(t *testing.T) {
	old := scanInterval
	scanInterval = 50 * time.Millisecond
	defer func() { scanInterval = old }()

	epA, epB := twoEndpoints(t)
	rootA := filepath.Join(t.TempDir(), "docs")         // holder
	mountParent := t.TempDir()                          // user's home-ish dir
	mountB := filepath.Join(mountParent, "remote-docs") // streamed mount path

	a := mkNode(t, "a", "b", epA, epB, rootA, true)
	b := mkNode(t, "b", "a", epB, epA, mountB, false)

	writeFile(t, filepath.Join(rootA, "hello.txt"), "from-holder", time.Now())

	startNode(t, a)
	startNode(t, b)

	// While the holder answers, the mount is live: the file is visible
	// through the mount path, which is a symlink into the daemon's views.
	waitFor(t, testWait, "attached view", func() bool {
		if b.eng.Snapshot()[0].State != stateAttached {
			return false
		}
		return exists(filepath.Join(mountB, "hello.txt"))
	})
	if got := readFile(t, filepath.Join(mountB, "hello.txt")); got != "from-holder" {
		t.Fatalf("mounted content = %q", got)
	}
	if fi, err := os.Lstat(mountB); err != nil || fi.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("mount path is not a symlink: %v %v", fi, err)
	}

	// The holder goes away: the folder is replaced by a writable empty dir.
	a.stop()
	waitFor(t, testWait, "detach", func() bool {
		return b.eng.Snapshot()[0].State == stateDetached
	})
	waitFor(t, testWait, "mount looks empty", func() bool {
		entries, err := os.ReadDir(mountB)
		return err == nil && len(entries) == 0
	})

	// Work written while offline.
	writeFile(t, filepath.Join(mountB, "offline.txt"), "kept", time.Now())

	// The holder comes back: the offline work merges into the live view and
	// propagates to the holder.
	a2 := mkNode(t, "a", "b", epA, epB, rootA, true)
	startNode(t, a2)

	waitFor(t, testWait, "reattach with both files", func() bool {
		if b.eng.Snapshot()[0].State != stateAttached {
			return false
		}
		return exists(filepath.Join(mountB, "offline.txt")) && exists(filepath.Join(mountB, "hello.txt"))
	})
	waitFor(t, testWait, "offline file synced to holder", func() bool {
		return exists(filepath.Join(rootA, "offline.txt"))
	})
	if got := readFile(t, filepath.Join(rootA, "offline.txt")); got != "kept" {
		t.Fatalf("holder content = %q", got)
	}
}

func TestForeignMountPathIsLeftAlone(t *testing.T) {
	mountParent := t.TempDir()
	mount := filepath.Join(mountParent, "remote-docs")
	if err := os.MkdirAll(mount, 0o755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(mount, "real.txt"), "samba was here", time.Now())

	n := mkNode(t, "b", "a", "127.0.0.1:1", "127.0.0.1:2", mount, false)
	n.eng.prepare(n.eng.folders[0])

	if state := n.eng.Snapshot()[0].State; state != stateBusy {
		t.Fatalf("state = %q, want busy", state)
	}
	// The foreign directory keeps its content and is not a symlink now.
	if fi, err := os.Lstat(mount); err != nil || fi.Mode()&os.ModeSymlink != 0 {
		t.Fatalf("foreign path was replaced: %v %v", fi, err)
	}
	if got := readFile(t, filepath.Join(mount, "real.txt")); got != "samba was here" {
		t.Fatalf("foreign content = %q", got)
	}
}

// An empty directory at a streamed mount path is what a retired cifs mount
// leaves behind: nothing to protect, so it is adopted.
func TestEmptyMountPathIsAdopted(t *testing.T) {
	mountParent := t.TempDir()
	mount := filepath.Join(mountParent, "remote-docs")
	if err := os.MkdirAll(mount, 0o777); err != nil {
		t.Fatal(err)
	}

	n := mkNode(t, "b", "a", "127.0.0.1:1", "127.0.0.1:2", mount, false)
	n.eng.prepare(n.eng.folders[0])

	if state := n.eng.Snapshot()[0].State; state != stateDetached {
		t.Fatalf("state = %q, want detached (adopted)", state)
	}
	fi, err := os.Lstat(mount)
	if err != nil || fi.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("mount path was not replaced by a symlink: %v %v", fi, err)
	}
	if target, _ := os.Readlink(mount); target != n.eng.detachedDir(n.eng.folders[0]) {
		t.Fatalf("symlink target = %q, want the detached dir", target)
	}
}

func TestValidRelRejectsEscapes(t *testing.T) {
	bad := []string{"", "/etc/passwd", "../x", "a/../../b", "a\x00b"}
	for _, p := range bad {
		if validRel(p) {
			t.Errorf("validRel(%q) = true, want false", p)
		}
	}
	good := []string{"a", "a/b.txt", "dir/sub/file"}
	for _, p := range good {
		if !validRel(p) {
			t.Errorf("validRel(%q) = false, want true", p)
		}
	}
	if !strings.Contains(conflictPath("x/y.txt", "abcdef123456"), "abcdef12") {
		t.Error("conflictPath should carry the content prefix")
	}
}

// currentIDs is the running user's ids and names: tests chown to a
// resolvable target without needing root.
func currentIDs(t *testing.T) (uname, gname string) {
	t.Helper()
	u, err := user.Current()
	if err != nil {
		t.Skipf("no passwd entry for the test user: %v", err)
	}
	g, err := user.LookupGroupId(strconv.Itoa(os.Getgid()))
	if err != nil {
		t.Fatalf("no group name for gid %d: %v", os.Getgid(), err)
	}
	return u.Username, g.Name
}

// Nothing declared: the folder is permissioned exactly like the directory
// the mount lives in, and every component of the storage chain is walkable.
func TestMountInheritsParentPermissions(t *testing.T) {
	mountParent := t.TempDir()
	if err := os.Chmod(mountParent, 0o750); err != nil {
		t.Fatal(err)
	}
	mount := filepath.Join(mountParent, "remote-docs")

	n := mkNode(t, "b", "a", "127.0.0.1:1", "127.0.0.1:2", mount, false)
	fs := n.eng.folders[0]
	n.eng.prepare(fs)

	fs.scanMu.Lock()
	p := fs.perm
	fs.scanMu.Unlock()
	if p.uid != os.Getuid() || p.gid != os.Getgid() {
		t.Fatalf("owner = %d:%d, want the parent's %d:%d", p.uid, p.gid, os.Getuid(), os.Getgid())
	}
	if p.dirMode != 0o750 {
		t.Fatalf("dirMode = %04o, want the parent's 0750", p.dirMode)
	}

	for _, d := range []string{n.eng.viewsDir(fs), n.eng.detachedDir(fs)} {
		fi, err := os.Stat(d)
		if err != nil {
			t.Fatal(err)
		}
		if fi.Mode().Perm() != 0o750 {
			t.Errorf("%s mode = %04o, want 0750", d, fi.Mode().Perm())
		}
	}
	for _, d := range []string{
		n.cfg.DataDir,
		filepath.Join(n.cfg.DataDir, "views"),
		filepath.Join(n.cfg.DataDir, "detached"),
	} {
		fi, err := os.Stat(d)
		if err != nil {
			t.Fatal(err)
		}
		if fi.Mode().Perm()&0o011 != 0o011 {
			t.Errorf("%s = %04o: the chain to the mount target must be traversable", d, fi.Mode().Perm())
		}
	}
}

// Declared permissions merge field by field: the machine-local folder entry
// beats the group's default, and whatever neither declares is inherited. An
// unresolvable name is a warning, not a failure — the inherited value stays.
func TestPermissionsOverrides(t *testing.T) {
	uname, gname := currentIDs(t)

	mountParent := t.TempDir()
	if err := os.Chmod(mountParent, 0o750); err != nil {
		t.Fatal(err)
	}
	mount := filepath.Join(mountParent, "remote-docs")

	n := mkNode(t, "b", "a", "127.0.0.1:1", "127.0.0.1:2", mount, false)
	g := n.cfg.Groups["g"]
	g.Permissions = &config.Permissions{Group: gname, Mode: "0770"}
	n.cfg.Groups["g"] = g
	n.cfg.Permissions = map[string]config.Permissions{"docs": {User: uname}}
	fs := n.eng.folders[0]

	n.eng.prepare(fs)
	fs.scanMu.Lock()
	p := fs.perm
	fs.scanMu.Unlock()
	if p.uid != os.Getuid() || p.gid != os.Getgid() || p.dirMode != 0o770 {
		t.Fatalf("perm = %d:%d %04o, want %d:%d 0770 (local user + group defaults)", p.uid, p.gid, p.dirMode, os.Getuid(), os.Getgid())
	}

	// The folder entry's mode wins over the group's.
	n.cfg.Permissions["docs"] = config.Permissions{User: uname, Mode: "0700"}
	n.eng.prepare(fs)
	fs.scanMu.Lock()
	p = fs.perm
	fs.scanMu.Unlock()
	if p.dirMode != 0o700 {
		t.Fatalf("dirMode = %04o, want the folder entry's 0700", p.dirMode)
	}

	// An unknown user falls back to the inherited owner.
	n.cfg.Permissions["docs"] = config.Permissions{User: "definitely-not-a-user-xyz"}
	n.eng.prepare(fs)
	fs.scanMu.Lock()
	p = fs.perm
	fs.scanMu.Unlock()
	if p.uid != os.Getuid() {
		t.Fatalf("uid = %d, want the parent's %d when the name does not resolve", p.uid, os.Getuid())
	}
}

// Permissions are re-applied "every time": a manual chmod of the target is
// corrected on the next scan and on the next mount transition.
func TestMountPermissionsReapplied(t *testing.T) {
	mountParent := t.TempDir()
	if err := os.Chmod(mountParent, 0o750); err != nil {
		t.Fatal(err)
	}
	mount := filepath.Join(mountParent, "remote-docs")

	n := mkNode(t, "b", "a", "127.0.0.1:1", "127.0.0.1:2", mount, false)
	fs := n.eng.folders[0]
	n.eng.prepare(fs)

	view, detached := n.eng.viewsDir(fs), n.eng.detachedDir(fs)
	for _, d := range []string{view, detached} {
		if err := os.Chmod(d, 0o777); err != nil {
			t.Fatal(err)
		}
	}

	n.eng.scanOnce(fs) // root (the detached dir) is restored by the scan
	if fi, err := os.Stat(detached); err != nil || fi.Mode().Perm() != 0o750 {
		t.Fatalf("detached after scan = %v %v, want 0750", fi.Mode().Perm(), err)
	}

	n.eng.ensureMounts(fs) // a transition re-applies to both targets
	for _, d := range []string{view, detached} {
		fi, err := os.Stat(d)
		if err != nil || fi.Mode().Perm() != 0o750 {
			t.Fatalf("%s after transition = %v %v, want 0750", d, fi.Mode().Perm(), err)
		}
	}
}

// Materialised files take the folder's owner and their own synced mode;
// every directory created on the way takes the folder's directory mode.
func TestMaterialisedEntriesTakeFolderPermissions(t *testing.T) {
	mountParent := t.TempDir()
	if err := os.Chmod(mountParent, 0o750); err != nil {
		t.Fatal(err)
	}
	mount := filepath.Join(mountParent, "remote-docs")

	n := mkNode(t, "b", "a", "127.0.0.1:1", "127.0.0.1:2", mount, false)
	fs := n.eng.folders[0]
	n.eng.prepare(fs)

	content := []byte("hello from the mesh")
	f, err := n.eng.store.Temp()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(content); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	sum, err := store.SumFile(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	if err := n.eng.store.Adopt(f.Name(), sum); err != nil {
		t.Fatal(err)
	}

	ent := &Entry{Path: "sub/dir/a.txt", Blob: sum, Size: int64(len(content)), MTime: time.Now().Add(-3 * time.Hour).UnixNano(), Mode: 0o600}
	if err := n.eng.applyEntry(fs, ent); err != nil {
		t.Fatal(err)
	}

	file := filepath.Join(fs.root, "sub", "dir", "a.txt")
	fi, err := os.Stat(file)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Errorf("file mode = %04o, want the entry's 0600", fi.Mode().Perm())
	}
	if fi.ModTime().UnixNano() != ent.MTime {
		t.Errorf("file mtime = %d, want the entry's %d — a materialised file must look like the one it mirrors", fi.ModTime().UnixNano(), ent.MTime)
	}
	if st, ok := fi.Sys().(*syscall.Stat_t); !ok || int(st.Uid) != os.Getuid() || int(st.Gid) != os.Getgid() {
		t.Errorf("file owner = %v, want the resolved %d:%d", fi.Sys(), os.Getuid(), os.Getgid())
	}
	for _, d := range []string{filepath.Join(fs.root, "sub"), filepath.Join(fs.root, "sub", "dir")} {
		fi, err := os.Stat(d)
		if err != nil {
			t.Fatal(err)
		}
		if fi.Mode().Perm() != 0o750 {
			t.Errorf("%s mode = %04o, want the folder's 0750", d, fi.Mode().Perm())
		}
	}
}

// A folder root on another filesystem (a pool, an external disk): a delete
// must still land in the trash, copying when rename cannot cross — a failed
// delete that still tombstones makes the scanner resurrect the file.
func TestTrashAcrossFilesystems(t *testing.T) {
	base, err := os.MkdirTemp("/dev/shm", "storeboard-xdev")
	if err != nil {
		t.Skipf("no second filesystem to cross with: %v", err)
	}
	defer os.RemoveAll(base)

	mount := filepath.Join(base, "docs")
	n := mkNode(t, "b", "a", "127.0.0.1:1", "127.0.0.1:2", mount, true)
	fs := n.eng.folders[0]
	n.eng.prepare(fs)

	if sameDevice(fs.root, n.cfg.DataDir) {
		t.Skip("data dir and folder root share a filesystem here; nothing to cross")
	}

	writeFile(t, filepath.Join(fs.root, "gone.txt"), "bye", time.Now())
	if err := n.eng.applyDelete(fs, &Entry{Path: "gone.txt"}); err != nil {
		t.Fatalf("applyDelete: %v", err)
	}
	if _, err := os.Stat(filepath.Join(fs.root, "gone.txt")); !os.IsNotExist(err) {
		t.Fatalf("file still in the folder: %v", err)
	}
	if got := readFile(t, filepath.Join(n.cfg.DataDir, "trash", "docs", "gone.txt")); got != "bye" {
		t.Fatalf("trashed content = %q", got)
	}
}

// sameDevice reports whether two existing paths live on one filesystem.
func sameDevice(a, b string) bool {
	sa, ea := os.Stat(a)
	sb, eb := os.Stat(b)
	if ea != nil || eb != nil {
		return true
	}
	da, oka := sa.Sys().(*syscall.Stat_t)
	db, okb := sb.Sys().(*syscall.Stat_t)
	return !oka || !okb || da.Dev == db.Dev
}

// Losing the holder must not cost the folder its content. The detached dir
// is a scratchpad, not the folder: scanning it used to tombstone every known
// file, and each reattach then resurrected the whole folder with fresh
// versions — rider and holder rewrote gigabtyes on every reconnect.
func TestDetachedFolderDoesNotTombstoneItsContent(t *testing.T) {
	old := scanInterval
	scanInterval = 50 * time.Millisecond
	defer func() { scanInterval = old }()

	epA, epB := twoEndpoints(t)
	rootA := filepath.Join(t.TempDir(), "docs")
	mountB := filepath.Join(t.TempDir(), "remote-docs")

	a := mkNode(t, "a", "b", epA, epB, rootA, true)
	b := mkNode(t, "b", "a", epB, epA, mountB, false)
	writeFile(t, filepath.Join(rootA, "hello.txt"), "from-holder", time.Now())

	startNode(t, a)
	startNode(t, b)

	waitFor(t, testWait, "attached with the holder's file", func() bool {
		return exists(filepath.Join(mountB, "hello.txt"))
	})
	before := b.eng.folders[0].db.Get("hello.txt")
	if before == nil || before.Deleted {
		t.Fatalf("no live entry after attach: %+v", before)
	}

	a.stop()
	waitFor(t, testWait, "detached", func() bool {
		return b.eng.Snapshot()[0].State == stateDetached
	})

	for i := 0; i < 5; i++ {
		b.eng.scanOnce(b.eng.folders[0])
		time.Sleep(20 * time.Millisecond)
	}

	after := b.eng.folders[0].db.Get("hello.txt")
	if after == nil || after.Deleted {
		t.Fatalf("entry tombstoned while detached: %+v", after)
	}
	if after.VV["b"] != before.VV["b"] {
		t.Fatalf("version bumped while detached: %v -> %v", before.VV, after.VV)
	}
	if got := readFile(t, filepath.Join(rootA, "hello.txt")); got != "from-holder" {
		t.Fatalf("holder content = %q", got)
	}
}

// Busy can say exactly one thing at a time, and the reason follows the path:
// content, a foreign symlink, a mount point, a file in the way.
func TestBusyReasonsAreReported(t *testing.T) {
	uname, _ := currentIDs(t)
	mountParent := t.TempDir()
	mount := filepath.Join(mountParent, "remote-docs")
	if err := os.MkdirAll(mount, 0o755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(mount, "mine.txt"), "local", time.Now())

	n := mkNode(t, "b", "a", "127.0.0.1:1", "127.0.0.1:2", mount, false)
	n.eng.prepare(n.eng.folders[0])
	snap := n.eng.Snapshot()
	if snap[0].State != stateBusy || snap[0].Busy != api.BusyContent {
		t.Fatalf("state/busy = %q/%q, want busy/%q", snap[0].State, snap[0].Busy, api.BusyContent)
	}

	// A symlink somebody else made is not content, and takeover refuses it.
	other := filepath.Join(mountParent, "elsewhere")
	if err := os.MkdirAll(other, 0o755); err != nil {
		t.Fatal(err)
	}
	mount2 := filepath.Join(mountParent, "linked")
	if err := os.Symlink(other, mount2); err != nil {
		t.Fatal(err)
	}
	n2 := mkNode(t, "b", "a", "127.0.0.1:1", "127.0.0.1:2", mount2, false)
	n2.eng.prepare(n2.eng.folders[0])
	if s := n2.eng.Snapshot()[0]; s.State != stateBusy || s.Busy != api.BusySymlink {
		t.Fatalf("symlink case = %q/%q, want busy/%q", s.State, s.Busy, api.BusySymlink)
	}
	if _, err := n2.eng.Takeover("docs", os.Getuid()); err == nil {
		t.Fatal("takeover accepted a foreign symlink")
	}

	// Root or the folder's owner may take over; nobody else.
	if _, err := n.eng.Takeover("docs", os.Getuid()+1); err == nil {
		t.Fatal("takeover accepted a stranger's uid")
	}
	_ = uname
}

// The whole point: a path full of the user's files becomes a storeboard
// mount, the files move into the offline dir, and the normal merge hands
// them to the mesh — first to the view, then to the holder.
func TestTakeoverMountsContentAndMergesIt(t *testing.T) {
	epA, epB := twoEndpoints(t)
	rootA := filepath.Join(t.TempDir(), "docs") // holder
	mountB := filepath.Join(t.TempDir(), "remote-docs")
	if err := os.MkdirAll(filepath.Join(mountB, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(mountB, "mine.txt"), "my own file", time.Now())
	writeFile(t, filepath.Join(mountB, "sub", "deep.txt"), "deeper", time.Now())

	holder := mkNode(t, "a", "b", epA, epB, rootA, true)
	client := mkNode(t, "b", "a", epB, epA, mountB, false)
	writeFile(t, filepath.Join(rootA, "theirs.txt"), "from holder", time.Now())

	startNode(t, holder)
	startNode(t, client)

	waitFor(t, testWait, "busy before takeover", func() bool {
		return client.eng.Snapshot()[0].State == stateBusy
	})

	res, err := client.eng.Takeover("docs", os.Getuid())
	if err != nil {
		t.Fatalf("takeover: %v", err)
	}
	if res.Moved != 2 {
		t.Fatalf("moved = %d, want 2 top-level entries", res.Moved)
	}
	waitFor(t, testWait, "attached after takeover", func() bool {
		return client.eng.Snapshot()[0].State == stateAttached
	})

	// The user's files are at the same path — now through the mount — and
	// the holder's file is there too.
	for _, want := range []string{"mine.txt", "sub/deep.txt", "theirs.txt"} {
		waitFor(t, testWait, "visible through the mount: "+want, func() bool {
			return exists(filepath.Join(mountB, want))
		})
	}
	if got := readFile(t, filepath.Join(mountB, "mine.txt")); got != "my own file" {
		t.Fatalf("mine.txt = %q", got)
	}
	if fi, err := os.Lstat(mountB); err != nil || fi.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("mount path is not a symlink after takeover: %v %v", fi, err)
	}

	// And the mesh has them: the holder ends up with the content the user
	// had on the client, byte for byte.
	waitFor(t, testWait, "holder received the taken-over files", func() bool {
		return exists(filepath.Join(rootA, "mine.txt")) && exists(filepath.Join(rootA, "sub", "deep.txt"))
	})
	if got := readFile(t, filepath.Join(rootA, "mine.txt")); got != "my own file" {
		t.Fatalf("holder mine.txt = %q", got)
	}

	// Idempotent: a second takeover on a mounted folder changes nothing.
	res2, err := client.eng.Takeover("docs", os.Getuid())
	if err != nil || res2.Moved != 0 || res2.State != stateAttached {
		t.Fatalf("second takeover = %+v, %v; want a no-op on an attached folder", res2, err)
	}
}

// A takeover without a holder parks the content in the offline mount and
// lets the normal reconnect merge it: the path stays usable immediately.
func TestTakeoverOfflineThenMergesOnReconnect(t *testing.T) {
	epA, epB := twoEndpoints(t)
	rootA := filepath.Join(t.TempDir(), "docs")
	mountB := filepath.Join(t.TempDir(), "remote-docs")
	if err := os.MkdirAll(mountB, 0o755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(mountB, "mine.txt"), "offline content", time.Now())

	holder := mkNode(t, "a", "b", epA, epB, rootA, true)
	client := mkNode(t, "b", "a", epB, epA, mountB, false)
	writeFile(t, filepath.Join(rootA, "theirs.txt"), "from holder", time.Now())

	// Only the client runs: no holder, so nothing can attach.
	startNode(t, client)
	waitFor(t, testWait, "busy before takeover", func() bool {
		return client.eng.Snapshot()[0].State == stateBusy
	})
	res, err := client.eng.Takeover("docs", os.Getuid())
	if err != nil {
		t.Fatalf("takeover: %v", err)
	}
	if res.State != stateDetached || res.Moved != 1 {
		t.Fatalf("takeover = %+v, want detached with 1 entry moved", res)
	}
	if got := readFile(t, filepath.Join(mountB, "mine.txt")); got != "offline content" {
		t.Fatalf("content not readable through the offline mount: %q", got)
	}

	// The holder appears: the ordinary attach merges the offline content in,
	// and the mesh receives it.
	startNode(t, holder)
	waitFor(t, testWait, "merged and attached on reconnect", func() bool {
		return client.eng.Snapshot()[0].State == stateAttached && exists(filepath.Join(rootA, "mine.txt"))
	})
	if got := readFile(t, filepath.Join(rootA, "mine.txt")); got != "offline content" {
		t.Fatalf("holder mine.txt = %q", got)
	}
}

// The same bytes arriving from both sides are one file: a merge must not
// manufacture a conflict copy just because the timestamps moved. This is the
// screenshot case — identical files, mtimes from different machines.
func TestMergeDetachedIdenticalContentIsNotAConflict(t *testing.T) {
	mount := filepath.Join(t.TempDir(), "remote-docs")
	n := mkNode(t, "b", "a", "127.0.0.1:1", "127.0.0.1:2", mount, false)
	fs := n.eng.folders[0]
	n.eng.prepare(fs)

	view, detached := n.eng.viewsDir(fs), n.eng.detachedDir(fs)
	older := time.Now().Add(-2 * time.Hour)
	newer := time.Now()
	const shot = "PNG-ish bytes, byte identical"
	writeFile(t, filepath.Join(view, "shot.png"), shot, older)
	writeFile(t, filepath.Join(detached, "shot.png"), shot, newer)

	n.eng.attach(fs)

	names, _ := filepath.Glob(filepath.Join(view, "shot.png*"))
	if len(names) != 1 {
		t.Fatalf("view holds %d entries for shot.png, want exactly 1 (no conflict copy): %v", len(names), names)
	}
	if got := readFile(t, filepath.Join(view, "shot.png")); got != shot {
		t.Fatalf("content = %q, want the identical bytes kept", got)
	}
	fi, err := os.Stat(filepath.Join(view, "shot.png"))
	if err != nil {
		t.Fatal(err)
	}
	if !fi.ModTime().After(older) {
		t.Errorf("mtime = %v, want the newer timestamp %v to survive", fi.ModTime(), newer)
	}
	if _, err := os.Stat(detached); !os.IsNotExist(err) {
		t.Errorf("detached dir still there: %v", err)
	}
}

// The other direction: genuinely different bytes at the same path still keep
// both versions — the fix must not turn a real conflict into a silent loss.
func TestMergeDetachedDifferentContentStillConflicts(t *testing.T) {
	mount := filepath.Join(t.TempDir(), "remote-docs")
	n := mkNode(t, "b", "a", "127.0.0.1:1", "127.0.0.1:2", mount, false)
	fs := n.eng.folders[0]
	n.eng.prepare(fs)

	view, detached := n.eng.viewsDir(fs), n.eng.detachedDir(fs)
	writeFile(t, filepath.Join(view, "note.txt"), "the mesh's version", time.Now().Add(-2*time.Hour))
	writeFile(t, filepath.Join(detached, "note.txt"), "the offline version", time.Now())

	n.eng.attach(fs)

	names, _ := filepath.Glob(filepath.Join(view, "note.txt*"))
	if len(names) != 2 {
		t.Fatalf("view holds %d entries for note.txt, want the winner and a conflict copy: %v", len(names), names)
	}
}

// A declared streamed mount works on a machine that has never had one: the
// daemon creates the path's whole chain, like mkdir -p, and mounts there.
func TestStreamedMountCreatesItsPath(t *testing.T) {
	base := t.TempDir()
	mount := filepath.Join(base, "remote", "nested", "Videos")

	n := mkNode(t, "b", "a", "127.0.0.1:1", "127.0.0.1:2", mount, false)
	fs := n.eng.folders[0]
	n.eng.prepare(fs)

	if s := n.eng.Snapshot()[0]; s.State != stateDetached {
		t.Fatalf("state = %q (busy %q), want detached with the path created", s.State, s.Busy)
	}
	fi, err := os.Lstat(mount)
	if err != nil || fi.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("mount path is not a symlink: %v %v", fi, err)
	}
	for _, dir := range []string{filepath.Join(base, "remote"), filepath.Join(base, "remote", "nested")} {
		if fi, err := os.Stat(dir); err != nil || !fi.IsDir() {
			t.Fatalf("%s was not created: %v %v", dir, fi, err)
		}
	}
	// The user's files land in the mount as usual.
	writeFile(t, filepath.Join(mount, "note.txt"), "offline", time.Now())
	if got := readFile(t, filepath.Join(n.eng.detachedDir(fs), "note.txt")); got != "offline" {
		t.Fatalf("content through the mount = %q", got)
	}
}

// mkStorageNode is mkNode with a storage directory: the folder's content
// lives in <storage>/docs and its mount path is a view onto that.
func mkStorageNode(t *testing.T, name, other, epSelf, epOther, mount, storage string, docsKeep bool) *node {
	t.Helper()
	dir := t.TempDir()
	cfg := &config.Config{
		ConfigVersion: config.ConfigVersion,
		DataDir:       filepath.Join(dir, "data"),
		Socket:        filepath.Join(dir, "s.sock"),
		Peer:          name,
		StorageDir:    &storage,
		Peers: map[string]config.Peer{
			name:  {Endpoints: []string{epSelf}},
			other: {Endpoints: []string{epOther}},
		},
		Groups: map[string]config.Group{
			"g": {
				Peers:   []string{name, other},
				Folders: map[string]config.GroupFolder{"docs": {Mount: mount}},
			},
		},
		Mounts:      map[string]string{"docs": mount},
		KeepOffline: map[string]bool{"docs": docsKeep},
	}
	eng, err := New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	return &node{t: t, name: name, dir: dir, cfg: cfg, eng: eng}
}

// A replica with a storage directory keeps its content there and shows it
// through the mount: content and mount point are independent.
func TestStorageBackedReplica(t *testing.T) {
	storage := t.TempDir()
	mount := filepath.Join(t.TempDir(), "docs")

	n := mkStorageNode(t, "b", "a", "127.0.0.1:1", "127.0.0.1:2", mount, storage, true)
	fs := n.eng.folders[0]
	n.eng.prepare(fs)

	snap := n.eng.Snapshot()[0]
	want := filepath.Join(storage, "docs")
	if snap.State != stateReplica || snap.Root != want {
		t.Fatalf("state/root = %q/%q, want replica/%q", snap.State, snap.Root, want)
	}
	target, err := os.Readlink(mount)
	if err != nil || target != want {
		t.Fatalf("mount = %q (%v), want a symlink to %q", target, err, want)
	}

	writeFile(t, filepath.Join(mount, "note.txt"), "through the mount", time.Now())
	if got := readFile(t, filepath.Join(want, "note.txt")); got != "through the mount" {
		t.Fatalf("content in storage = %q", got)
	}
	// The scanner indexes the storage root, not the mount.
	changed, err := n.eng.scan(fs, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(changed) != 1 || changed[0].Path != "note.txt" {
		t.Fatalf("scan changed = %+v, want note.txt from the storage root", changed)
	}
}

// Declaring a storage directory over a folder whose files are still at the
// mount path migrates them: entry by entry, never overwriting, and running
// again is a no-op.
func TestStorageMigrationFromMountPath(t *testing.T) {
	storage := t.TempDir()
	mount := filepath.Join(t.TempDir(), "docs")
	if err := os.MkdirAll(filepath.Join(mount, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(mount, "keep.txt"), "keep me", time.Now())
	writeFile(t, filepath.Join(mount, "sub", "deep.txt"), "deeper", time.Now())

	n := mkStorageNode(t, "b", "a", "127.0.0.1:1", "127.0.0.1:2", mount, storage, true)
	fs := n.eng.folders[0]
	n.eng.prepare(fs)

	want := filepath.Join(storage, "docs")
	if got := readFile(t, filepath.Join(want, "keep.txt")); got != "keep me" {
		t.Fatalf("migrated content = %q", got)
	}
	if got := readFile(t, filepath.Join(want, "sub", "deep.txt")); got != "deeper" {
		t.Fatalf("migrated nested content = %q", got)
	}
	if target, err := os.Readlink(mount); err != nil || target != want {
		t.Fatalf("mount after migration = %q (%v), want a symlink to %q", target, err, want)
	}
	if fi, err := os.Stat(filepath.Join(want, "keep.txt")); err != nil || fi.IsDir() {
		t.Fatalf("migrated file is gone: %v %v", fi, err)
	}

	// Idempotent: the next transition sees the symlink and does nothing.
	n.eng.prepare(fs)
	if got := readFile(t, filepath.Join(want, "keep.txt")); got != "keep me" {
		t.Fatalf("content after a second prepare = %q", got)
	}
	names, _ := filepath.Glob(filepath.Join(want, "*"))
	if len(names) != 2 {
		t.Fatalf("storage root holds %v, want exactly the two migrated entries", names)
	}
}

// A storage-backed streamed folder works like any other: detached it is an
// offline scratchpad, and the reconnect merges into the storage directory
// and out to the holder.
func TestStorageBackedStreamedMergesOnReconnect(t *testing.T) {
	epA, epB := twoEndpoints(t)
	holderStorage, clientStorage := t.TempDir(), t.TempDir()
	holderMount := filepath.Join(t.TempDir(), "docs") // a view onto the storage
	rootA := filepath.Join(holderStorage, "docs")
	mountB := filepath.Join(t.TempDir(), "remote-docs")

	holder := mkStorageNode(t, "a", "b", epA, epB, holderMount, holderStorage, true)
	client := mkStorageNode(t, "b", "a", epB, epA, mountB, clientStorage, false)
	writeFile(t, filepath.Join(rootA, "theirs.txt"), "from holder", time.Now())

	startNode(t, client)
	waitFor(t, testWait, "detached", func() bool {
		return client.eng.Snapshot()[0].State == stateDetached
	})
	writeFile(t, filepath.Join(mountB, "mine.txt"), "offline content", time.Now())

	startNode(t, holder)
	waitFor(t, testWait, "attached and merged", func() bool {
		return exists(filepath.Join(clientStorage, "docs", "mine.txt")) &&
			exists(filepath.Join(clientStorage, "docs", "theirs.txt")) &&
			exists(filepath.Join(rootA, "mine.txt"))
	})
	if got := readFile(t, filepath.Join(rootA, "mine.txt")); got != "offline content" {
		t.Fatalf("holder content = %q", got)
	}
	if got := readFile(t, filepath.Join(clientStorage, "docs", "theirs.txt")); got != "from holder" {
		t.Fatalf("client storage content = %q", got)
	}
}

// The upgrade path: a streamed folder whose mount still points at the legacy
// view directory (dataDir/views/<folder>) moves its content into the storage
// directory and swaps the symlink, instead of going busy on a symlink it no
// longer recognizes.
func TestLegacyViewMigratesIntoStorage(t *testing.T) {
	storage := t.TempDir()
	mount := filepath.Join(t.TempDir(), "remote-docs")

	n := mkStorageNode(t, "b", "a", "127.0.0.1:1", "127.0.0.1:2", mount, storage, false)
	fs := n.eng.folders[0]

	// Recreate the pre-storage layout by hand: content in dataDir/views/docs,
	// the mount a symlink onto it.
	legacy := filepath.Join(n.cfg.DataDir, "views", "docs")
	if err := os.MkdirAll(legacy, 0o755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(legacy, "old.txt"), "from the legacy view", time.Now())
	if err := os.Symlink(legacy, mount); err != nil {
		t.Fatal(err)
	}

	n.eng.prepare(fs)

	want := filepath.Join(storage, "docs")
	if got := readFile(t, filepath.Join(want, "old.txt")); got != "from the legacy view" {
		t.Fatalf("legacy content not migrated: %q", got)
	}
	if target, err := os.Readlink(mount); err != nil || target != want {
		t.Fatalf("mount = %q (%v), want a symlink to the storage root %q", target, err, want)
	}
	// No holder is connected, so the folder reports detached — but the
	// storage-backed mount stays on the view either way, which is what the
	// symlink and content assertions above pin.
	if s := n.eng.Snapshot()[0]; s.State != stateDetached || s.Root != want {
		t.Fatalf("state/root = %q/%q, want detached/%q", s.State, s.Root, want)
	}
	if _, err := os.Stat(legacy); !os.IsNotExist(err) {
		t.Errorf("legacy view still present: %v", err)
	}
}

// The streamed case with content already at the mount path — exactly the
// shape a client has when a folder was declared before storage directories
// existed (the local copy of a streamed folder): content migrates into the
// storage directory and the folder ends up live over it.
func TestStorageBackedStreamedMigratesMountContent(t *testing.T) {
	storage := t.TempDir()
	mount := filepath.Join(t.TempDir(), "remote-docs")
	if err := os.MkdirAll(filepath.Join(mount, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(mount, "mine.txt"), "local before storage", time.Now())
	writeFile(t, filepath.Join(mount, "sub", "deep.txt"), "deeper", time.Now())

	n := mkStorageNode(t, "b", "a", "127.0.0.1:1", "127.0.0.1:2", mount, storage, false)
	fs := n.eng.folders[0]
	n.eng.prepare(fs)

	want := filepath.Join(storage, "docs")
	if got := readFile(t, filepath.Join(want, "mine.txt")); got != "local before storage" {
		t.Fatalf("migrated content = %q", got)
	}
	if got := readFile(t, filepath.Join(want, "sub", "deep.txt")); got != "deeper" {
		t.Fatalf("migrated nested content = %q", got)
	}
	if target, err := os.Readlink(mount); err != nil || target != want {
		t.Fatalf("mount = %q (%v), want a symlink to %q", target, err, want)
	}
	// The content is still available at the same path, now through the mount.
	if got := readFile(t, filepath.Join(mount, "mine.txt")); got != "local before storage" {
		t.Fatalf("content through the mount = %q", got)
	}
}

// A storage directory under the data directory has to be reachable from the
// mount: the daemon's own levels are traverse-only (0711) and the storage
// root carries the folder's resolved permissions — otherwise the owner gets
// "Permission denied" on ls through the mount, because the module's data dir
// is 0750.
func TestStorageChainIsTraversable(t *testing.T) {
	// Default storage (under the node's data dir) rather than an explicit one.
	n := mkNode(t, "b", "a", "127.0.0.1:1", "127.0.0.1:2", filepath.Join(t.TempDir(), "docs"), true)
	fs := n.eng.folders[0]
	fs.storage = filepath.Join(n.cfg.DataDir, "storage", "docs")
	n.eng.prepare(fs)

	base := filepath.Dir(filepath.Dir(fs.storage)) // the data dir
	for _, dir := range []string{base, filepath.Dir(fs.storage)} {
		fi, err := os.Stat(dir)
		if err != nil {
			t.Fatal(err)
		}
		if fi.Mode().Perm()&0o011 != 0o011 {
			t.Errorf("%s mode = %04o, want it traversable for others", dir, fi.Mode().Perm())
		}
	}
	fi, err := os.Stat(fs.storage)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm()&0o400 == 0 {
		t.Errorf("storage root mode = %04o, want the resolved permissions (owner-readable)", fi.Mode().Perm())
	}
	// And the mount resolves for the owner.
	if _, err := os.Stat(fs.mount); err != nil {
		t.Fatalf("mount does not resolve: %v", err)
	}
}

// A foreign symlink at a storage-backed folder's mount is refused, not
// replaced: the no-clobber rule holds for the new layout too.
func TestStorageMountRefusesForeignSymlink(t *testing.T) {
	storage := t.TempDir()
	mountParent := t.TempDir()
	mount := filepath.Join(mountParent, "docs")
	elsewhere := filepath.Join(mountParent, "elsewhere")
	if err := os.MkdirAll(elsewhere, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(elsewhere, mount); err != nil {
		t.Fatal(err)
	}

	n := mkStorageNode(t, "b", "a", "127.0.0.1:1", "127.0.0.1:2", mount, storage, false)
	n.eng.prepare(n.eng.folders[0])

	if s := n.eng.Snapshot()[0]; s.State != stateBusy || s.Busy != api.BusySymlink {
		t.Fatalf("state/busy = %q/%q, want busy/%q", s.State, s.Busy, api.BusySymlink)
	}
	if target, err := os.Readlink(mount); err != nil || target != elsewhere {
		t.Fatalf("foreign symlink was replaced: %q (%v)", target, err)
	}
}

// The incident: an empty view scanned against a journal full of entries from
// the mesh. An entry whose blob this machine never held must not be
// tombstoned — its absence says nothing — while a real local deletion (blob
// held, file gone) still tombstones.
func TestScanNeverTombstonesUnmaterialisedEntries(t *testing.T) {
	storage := t.TempDir()
	n := mkStorageNode(t, "b", "a", "127.0.0.1:1", "127.0.0.1:2", filepath.Join(t.TempDir(), "remote-docs"), storage, false)
	fs := n.eng.folders[0]
	n.eng.prepare(fs)

	// Known from the mesh, never fetched: the blob is not in the store.
	unknown := &Entry{Path: "from-the-mesh.txt", Blob: strings.Repeat("ab", 32), Size: 10, MTime: time.Now().UnixNano(), VV: map[string]uint64{"a": 1}}
	if err := fs.db.Put(unknown); err != nil {
		t.Fatal(err)
	}

	// Materialised once (blob in the store), then deleted on disk.
	f, err := n.eng.store.Temp()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("was here")); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	sum, err := store.SumFile(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	if err := n.eng.store.Adopt(f.Name(), sum); err != nil {
		t.Fatal(err)
	}
	local := &Entry{Path: "was-here.txt", Blob: sum, Size: 8, MTime: time.Now().UnixNano(), VV: map[string]uint64{"b": 1}}
	if err := fs.db.Put(local); err != nil {
		t.Fatal(err)
	}

	changed, err := n.eng.scan(fs, true)
	if err != nil {
		t.Fatal(err)
	}
	if got := fs.db.Get("from-the-mesh.txt"); got == nil || got.Deleted {
		t.Fatalf("unmaterialised entry tombstoned: %+v", got)
	}
	if got := fs.db.Get("was-here.txt"); got == nil || !got.Deleted {
		t.Fatalf("real local deletion not tombstoned: %+v", got)
	}
	for _, e := range changed {
		if e.Path == "from-the-mesh.txt" {
			t.Fatalf("tombstone broadcast for an unmaterialised entry: %+v", e)
		}
	}
}

// Upgrade with the mount on the scratchpad (the folder was detached): the
// legacy view's content still moves into the storage directory, so the new
// root starts with the folder's content instead of empty.
func TestLegacyViewMovesWhileDetached(t *testing.T) {
	storage := t.TempDir()
	mount := filepath.Join(t.TempDir(), "remote-docs")

	n := mkStorageNode(t, "b", "a", "127.0.0.1:1", "127.0.0.1:2", mount, storage, false)
	fs := n.eng.folders[0]

	legacy := filepath.Join(n.cfg.DataDir, "views", "docs")
	if err := os.MkdirAll(legacy, 0o755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(legacy, "old.txt"), "materialised before storage", time.Now())
	scratch := n.eng.detachedDir(fs)
	if err := os.MkdirAll(scratch, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(scratch, mount); err != nil { // detached at upgrade time
		t.Fatal(err)
	}

	n.eng.prepare(fs)

	if got := readFile(t, filepath.Join(storage, "docs", "old.txt")); got != "materialised before storage" {
		t.Fatalf("legacy content not moved while detached: %q", got)
	}
	if target, err := os.Readlink(mount); err != nil || target != filepath.Join(storage, "docs") {
		t.Fatalf("mount = %q (%v), want the storage root", target, err)
	}
}

// Status must answer while a folder is busy: a scan or a materialisation of
// tens of thousands of entries holds the folder's scan lock for minutes, and
// an endpoint that waits on it looks like a dead daemon.
func TestSnapshotNeverWaitsOnTheScanLock(t *testing.T) {
	mount := filepath.Join(t.TempDir(), "docs")
	n := mkNode(t, "b", "a", "127.0.0.1:1", "127.0.0.1:2", mount, true)
	fs := n.eng.folders[0]
	n.eng.prepare(fs)

	// What the last free moment published.
	want := n.eng.Snapshot()[0]

	fs.scanMu.Lock() // a long scan, in progress
	done := make(chan []api.FolderSync, 1)
	go func() { done <- n.eng.Snapshot() }()
	select {
	case got := <-done:
		if len(got) != 1 || got[0].State != want.State || got[0].Root != want.Root {
			t.Fatalf("snapshot during a scan = %+v, want the published values %+v", got, want)
		}
	case <-time.After(2 * time.Second):
		fs.scanMu.Unlock()
		t.Fatal("Snapshot waited on the scan lock")
	}
	fs.scanMu.Unlock()
}

// The grind has to be visible while it happens: local vs pending, scanning,
// and how many blobs are arriving — all readable without the scan lock.
func TestProgressIsVisibleWhileWorking(t *testing.T) {
	root := filepath.Join(t.TempDir(), "docs")
	mount := filepath.Join(t.TempDir(), "remote-docs")

	holder := mkNode(t, "a", "b", "127.0.0.1:1", "127.0.0.1:2", root, true)
	client := mkNode(t, "b", "a", "127.0.0.1:2", "127.0.0.1:1", mount, false)
	fs := client.eng.folders[0]
	client.eng.prepare(fs)

	// Known from the mesh, not fetched yet: pending, and nothing to do with
	// the scan lock being held.
	if err := fs.db.Put(&Entry{Path: "a.txt", Blob: strings.Repeat("ab", 32), Size: 4, MTime: time.Now().UnixNano(), VV: map[string]uint64{"a": 1}}); err != nil {
		t.Fatal(err)
	}
	fs.scanMu.Lock()
	s := client.eng.Snapshot()[0]
	fs.scanMu.Unlock()
	if s.Entries != 1 || s.Local != 0 || s.Pending != 1 {
		t.Fatalf("grind counts = entries %d local %d pending %d, want 1/0/1", s.Entries, s.Local, s.Pending)
	}

	// A scan reports what it found; afterwards the counts settle.
	writeFile(t, filepath.Join(fs.root, "here.txt"), "present", time.Now())
	if _, err := client.eng.scan(fs, true); err != nil {
		t.Fatal(err)
	}
	s = client.eng.Snapshot()[0]
	if s.Local != 1 || s.Scanning {
		t.Fatalf("after a scan: local %d scanning %v, want 1/false", s.Local, s.Scanning)
	}
	_ = holder
}

// The incident's exact shape: a streamed folder whose mirror is empty while
// the journal and the content store still remember the files (they were
// materialised earlier). Not live, that absence must never be read as a
// deletion — it cost 448 files on amanita once. Live, the same absence IS a
// deletion and has to propagate, or the mesh would never let you delete
// anything through a mount.
func TestStreamedFolderTombstonesOnlyWhileLive(t *testing.T) {
	storage := t.TempDir()
	n := mkStorageNode(t, "b", "a", "127.0.0.1:1", "127.0.0.1:2", filepath.Join(t.TempDir(), "remote-docs"), storage, false)
	fs := n.eng.folders[0]
	n.eng.prepare(fs)

	// Materialised once: the blob is in the store, the file is gone.
	f, err := n.eng.store.Temp()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("gone")); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	sum, err := store.SumFile(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	if err := n.eng.store.Adopt(f.Name(), sum); err != nil {
		t.Fatal(err)
	}
	if err := fs.db.Put(&Entry{Path: "gone.txt", Blob: sum, Size: 4, MTime: time.Now().UnixNano(), VV: map[string]uint64{"b": 1}}); err != nil {
		t.Fatal(err)
	}

	// Detached (not live): report what is here, never what is missing.
	if s := n.eng.Snapshot()[0]; s.State != stateDetached {
		t.Fatalf("state = %q, want detached for this case", s.State)
	}
	n.eng.scanOnce(fs)
	if got := fs.db.Get("gone.txt"); got == nil || got.Deleted {
		t.Fatalf("a detached scan tombstoned a materialised entry: %+v", got)
	}

	// Live: the same absence is a deletion.
	fs.scanMu.Lock()
	fs.state = stateAttached
	fs.scanMu.Unlock()
	n.eng.mu.Lock()
	n.eng.sends["a"] = func([]byte) error { return nil }
	n.eng.mu.Unlock()

	n.eng.scanOnce(fs)
	if got := fs.db.Get("gone.txt"); got == nil || !got.Deleted {
		t.Fatalf("a live scan did not propagate a real deletion: %+v", got)
	}
}

// The stale-dirent repair: an entry whose recorded size contradicts the file
// on disk (what a scan that hashed a file mid-write used to record) must be
// corrected in place, without a version bump — the poisoned entry otherwise
// made every later apply of that path look like an unsynced local change and
// park a false conflict next to it.
func TestScanRepairsAStaleSizeWithoutNewVersion(t *testing.T) {
	storage := t.TempDir()
	n := mkStorageNode(t, "b", "a", "127.0.0.1:1", "127.0.0.1:2", filepath.Join(t.TempDir(), "remote-docs"), storage, true)
	fs := n.eng.folders[0]
	n.eng.prepare(fs)

	writeFile(t, filepath.Join(fs.root, "shot.png"), "content", time.Now())
	sum, err := store.SumFile(filepath.Join(fs.root, "shot.png"))
	if err != nil {
		t.Fatal(err)
	}
	// The poison: right blob, wrong size, correct mtime.
	st, err := os.Stat(filepath.Join(fs.root, "shot.png"))
	if err != nil {
		t.Fatal(err)
	}
	if err := fs.db.Put(&Entry{Path: "shot.png", Blob: sum, Size: 0, MTime: st.ModTime().UnixNano(), Mode: 0o644, VV: map[string]uint64{"b": 3}}); err != nil {
		t.Fatal(err)
	}

	if _, err := n.eng.scan(fs, true); err != nil {
		t.Fatal(err)
	}
	got := fs.db.Get("shot.png")
	if got == nil || got.Size != int64(len("content")) {
		t.Fatalf("size not repaired: %+v", got)
	}
	if got.VV["b"] != 3 {
		t.Fatalf("repair bumped the version vector: %v", got.VV)
	}
	if got.Deleted {
		t.Fatalf("repair tombstoned the file: %+v", got)
	}
}
