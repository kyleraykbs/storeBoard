package syncer

import (
	"context"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"codeberg.org/kyleraykbs/storeboard/internal/config"
	"codeberg.org/kyleraykbs/storeboard/internal/transport"
)

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
		logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
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
	cfg := &config.Config{
		ConfigVersion: config.ConfigVersion,
		DataDir:       filepath.Join(dir, "data"),
		Socket:        filepath.Join(dir, "s.sock"),
		Peer:          name,
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
		logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
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
	waitFor(t, 10*time.Second, "file on B", func() bool {
		return exists(filepath.Join(rootB, "hello.txt"))
	})
	if got := readFile(t, filepath.Join(rootB, "hello.txt")); got != "one" {
		t.Fatalf("B content = %q", got)
	}

	// B edits it: A must converge on the newer content.
	time.Sleep(150 * time.Millisecond)
	writeFile(t, filepath.Join(rootB, "hello.txt"), "two", time.Now().Add(2*time.Second))
	waitFor(t, 10*time.Second, "edit on A", func() bool {
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
	waitFor(t, 10*time.Second, "delete propagated", func() bool {
		return !exists(filepath.Join(rootB, "hello.txt"))
	})
	waitFor(t, 5*time.Second, "trashed copy", func() bool {
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
		waitFor(t, 10*time.Second, "conflict convergence on "+n.name, func() bool {
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
	waitFor(t, 10*time.Second, "attached view", func() bool {
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
	waitFor(t, 15*time.Second, "detach", func() bool {
		return b.eng.Snapshot()[0].State == stateDetached
	})
	waitFor(t, 5*time.Second, "mount looks empty", func() bool {
		entries, err := os.ReadDir(mountB)
		return err == nil && len(entries) == 0
	})

	// Work written while offline.
	writeFile(t, filepath.Join(mountB, "offline.txt"), "kept", time.Now())

	// The holder comes back: the offline work merges into the live view and
	// propagates to the holder.
	a2 := mkNode(t, "a", "b", epA, epB, rootA, true)
	startNode(t, a2)

	waitFor(t, 15*time.Second, "reattach with both files", func() bool {
		if b.eng.Snapshot()[0].State != stateAttached {
			return false
		}
		return exists(filepath.Join(mountB, "offline.txt")) && exists(filepath.Join(mountB, "hello.txt"))
	})
	waitFor(t, 10*time.Second, "offline file synced to holder", func() bool {
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
