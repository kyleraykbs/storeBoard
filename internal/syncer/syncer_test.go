package syncer

import (
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
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
// trees, and a starved 50 ms scan tick can take seconds. The waits are
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
	log    *ringLog
}

// ringLog keeps the last few kilobytes of a node's log, so a failing test can
// print *why* it failed instead of just timing out.
type ringLog struct {
	mu  sync.Mutex
	buf []byte
}

func (r *ringLog) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.buf = append(r.buf, p...)
	if len(r.buf) > 16<<10 {
		r.buf = r.buf[len(r.buf)-(16<<10):]
	}
	return len(p), nil
}

func (r *ringLog) String() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return string(r.buf)
}

// lastLines trims a log to its final n lines: the beginning of a test is never
// where the story is.
func lastLines(s string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

// twoEndpoints returns two distinct loopback addresses the tests can hand to
// daemons. The listeners stay open while both are allocated: asking one at a
// time lets the kernel hand out the same port twice, which made one daemon
// dial itself.
func twoEndpoints(t *testing.T) (string, string) {
	t.Helper()
	for attempt := 0; attempt < 5; attempt++ {
		l1, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		l2, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			l1.Close()
			t.Fatal(err)
		}
		ep1, ep2 := l1.Addr().String(), l2.Addr().String()
		l1.Close()
		l2.Close()

		// Both must still be free, or the daemons will not be able to bind.
		p1, err1 := net.Listen("tcp", ep1)
		p2, err2 := net.Listen("tcp", ep2)
		if err1 == nil && err2 == nil {
			p1.Close()
			p2.Close()
			return ep1, ep2
		}
		if p1 != nil {
			p1.Close()
		}
		if p2 != nil {
			p2.Close()
		}
	}
	t.Fatal("could not find two free loopback ports")
	return "", ""
}

// start brings the engine and its transport up.
func startNode(t *testing.T, n *node) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	n.cancel = cancel
	if n.log == nil {
		n.log = &ringLog{}
	}
	logger := slog.New(slog.NewTextHandler(n.log, nil))
	if os.Getenv("STOREBOARD_TEST_LOG") != "" {
		logger = slog.New(slog.NewTextHandler(io.MultiWriter(os.Stderr, n.log), &slog.HandlerOptions{Level: slog.LevelDebug}))
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
	if n.t.Failed() && n.log != nil && n.log.String() != "" {
		n.t.Logf("node %q log (last lines):\n%s", n.name, lastLines(n.log.String(), 20))
	}
}

// newNode builds a two-peer config with the given store tree under a fresh
// root. Container is the directory the root lives in, so permission tests can
// control what the store inherits.
func newNode(t *testing.T, name, other, epSelf, epOther, container string, stores map[string]config.Store) *node {
	t.Helper()
	dir := t.TempDir()
	if container == "" {
		container = dir
	}
	cfg := &config.Config{
		ConfigVersion: config.ConfigVersion,
		Root:          filepath.Join(container, "Storeboard"),
		DataDir:       filepath.Join(dir, "data"),
		Socket:        filepath.Join(dir, "s.sock"),
		Peer:          name,
		Peers: map[string]config.Peer{
			name:  {Endpoints: []string{epSelf}},
			other: {Endpoints: []string{epOther}},
		},
		Groups: map[string]config.Group{
			"g": {Peers: []string{name, other}},
		},
		StoreTree:   stores,
		Permissions: map[string]config.Permissions{},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("test config: %v", err)
	}
	ring := &ringLog{}
	ng := &node{t: t, name: name, dir: dir, cfg: cfg, log: ring}
	ng.eng = newEngine(t, cfg, ring)
	return ng
}

func newEngine(t *testing.T, cfg *config.Config, ring *ringLog) *Engine {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(ring, &slog.HandlerOptions{Level: slog.LevelDebug}))
	if os.Getenv("STOREBOARD_TEST_LOG") != "" {
		logger = slog.New(slog.NewTextHandler(io.MultiWriter(os.Stderr, ring), &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	eng, err := New(cfg, logger)
	if err != nil {
		t.Fatal(err)
	}
	return eng
}

// mkNode is the common case: one local store "docs" in group g.
func mkNode(t *testing.T, name, other, epSelf, epOther string) *node {
	return newNode(t, name, other, epSelf, epOther, "", map[string]config.Store{
		"docs": {Group: "g"},
	})
}

// prepareAll does what Run does before its scanners start, so a test can
// exercise one node without racing the loop.
func (n *node) prepareAll() {
	n.eng.ensureRoot()
	for _, fs := range n.eng.stores {
		n.eng.prepareRoot(fs)
	}
	for _, fs := range n.eng.stores {
		n.eng.settleLinks(fs)
	}
}

func (n *node) store(name string) *StoreState { return n.eng.byName[name] }

func (n *node) root(name string) string { return n.eng.byName[name].root }

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
	a := mkNode(t, "a", "b", epA, epB)
	b := mkNode(t, "b", "a", epB, epA)
	startNode(t, a)
	startNode(t, b)

	// A creates a file: B must receive the content.
	writeFile(t, filepath.Join(a.root("docs"), "hello.txt"), "one", time.Now())
	waitFor(t, testWait, "file on B", func() bool {
		return exists(filepath.Join(b.root("docs"), "hello.txt"))
	})
	if got := readFile(t, filepath.Join(b.root("docs"), "hello.txt")); got != "one" {
		t.Fatalf("B content = %q", got)
	}

	// B edits it: A must converge on the newer content.
	time.Sleep(150 * time.Millisecond)
	writeFile(t, filepath.Join(b.root("docs"), "hello.txt"), "two", time.Now().Add(2*time.Second))
	waitFor(t, testWait, "edit on A", func() bool {
		data, err := os.ReadFile(filepath.Join(a.root("docs"), "hello.txt"))
		return err == nil && string(data) == "two"
	})

	// Nothing about a clean create->edit sequence is a conflict.
	for _, n := range []*node{a, b} {
		if names, _ := filepath.Glob(filepath.Join(n.root("docs"), "hello.txt.conflict-*")); len(names) != 0 {
			t.Fatalf("false conflict on %s: %v", n.name, names)
		}
	}

	// A deletes it: B's copy goes to the trash, not to nowhere.
	if err := os.Remove(filepath.Join(a.root("docs"), "hello.txt")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, testWait, "delete propagated", func() bool {
		return !exists(filepath.Join(b.root("docs"), "hello.txt"))
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
	a := mkNode(t, "a", "b", epA, epB)
	b := mkNode(t, "b", "a", epB, epA)

	// Both sides change the same path while disconnected; B's version is
	// newer, so it must win on both machines, and A's text must survive as
	// a conflict copy on both.
	base := time.Now().Add(-time.Hour)
	writeFile(t, filepath.Join(a.root("docs"), "note.txt"), "AAA", base)
	writeFile(t, filepath.Join(b.root("docs"), "note.txt"), "BBB", base.Add(time.Minute))

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
	for _, n := range []*node{a, b} {
		root := n.root("docs")
		waitFor(t, testWait, "conflict convergence on "+n.name, func() bool {
			return converged(root)
		})
	}
	if nameA, nameB := filepath.Base(conflictIn(a.root("docs"))), filepath.Base(conflictIn(b.root("docs"))); nameA != nameB {
		t.Fatalf("conflict names differ: %s vs %s", nameA, nameB)
	}

	snap := a.eng.Snapshot()
	if len(snap) != 1 || snap[0].Conflicts == 0 {
		t.Fatalf("snapshot = %+v, want a conflict counted", snap)
	}
}

// The nesting rule, made deliberate: a child store is a symlink inside its
// parent, so the parent's scan never sees the child's bytes. That is what
// makes nesting free instead of double-indexed.
func TestChildStoreIsALinkAndNeverIndexedByParent(t *testing.T) {
	n := newNode(t, "a", "b", "127.0.0.1:1", "127.0.0.1:2", "", map[string]config.Store{
		"pictures": {
			Group: "g",
			Stores: map[string]config.Store{
				"bulk": {Path: "Bulk"},
			},
		},
	})
	n.prepareAll()

	link := filepath.Join(n.root("pictures"), "Bulk")
	target, err := os.Readlink(link)
	if err != nil {
		t.Fatalf("child link: %v", err)
	}
	if target != n.root("bulk") {
		t.Fatalf("child link -> %q, want %q", target, n.root("bulk"))
	}

	writeFile(t, filepath.Join(n.root("bulk"), "big.bin"), "child", time.Now())
	writeFile(t, filepath.Join(n.root("pictures"), "small.jpg"), "parent", time.Now())

	if _, err := n.eng.scan(n.store("pictures"), true); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"Bulk", "Bulk/big.bin"} {
		if ent := n.store("pictures").db.Get(path); ent != nil {
			t.Fatalf("parent indexed %q: %+v", path, ent)
		}
	}
	if ent := n.store("pictures").db.Get("small.jpg"); ent == nil {
		t.Fatal("parent did not index its own file")
	}

	if _, err := n.eng.scan(n.store("bulk"), true); err != nil {
		t.Fatal(err)
	}
	if ent := n.store("bulk").db.Get("small.jpg"); ent != nil {
		t.Fatalf("child indexed the parent's file: %+v", ent)
	}
	if ent := n.store("bulk").db.Get("big.bin"); ent == nil {
		t.Fatal("child did not index its own file")
	}
}

func TestExternalSymlinkLinkedAndBusy(t *testing.T) {
	link := filepath.Join(t.TempDir(), "Documents")
	n := newNode(t, "a", "b", "127.0.0.1:1", "127.0.0.1:2", "", map[string]config.Store{
		"documents": {Group: "g", Symlinks: []string{link}},
	})

	// A path that does not exist yet: the daemon creates the link.
	n.prepareAll()
	target, err := os.Readlink(link)
	if err != nil {
		t.Fatalf("declared symlink: %v", err)
	}
	if target != n.root("documents") {
		t.Fatalf("link -> %q, want %q", target, n.root("documents"))
	}
	if s := n.eng.Snapshot()[0]; len(s.Symlinks) != 1 || s.Symlinks[0].State != api.SymlinkLinked {
		t.Fatalf("symlink status = %+v, want linked", s.Symlinks)
	}

	// A path that already holds the user's files: left alone, reported busy.
	busyPath := filepath.Join(t.TempDir(), "Old")
	writeFile(t, filepath.Join(busyPath, "keep.txt"), "mine", time.Now())
	m := newNode(t, "a", "b", "127.0.0.1:1", "127.0.0.1:2", "", map[string]config.Store{
		"documents": {Group: "g", Symlinks: []string{busyPath}},
	})
	m.prepareAll()
	if got := readFile(t, filepath.Join(busyPath, "keep.txt")); got != "mine" {
		t.Fatalf("content touched: %q", got)
	}
	s := m.eng.Snapshot()[0]
	if len(s.Symlinks) != 1 || s.Symlinks[0].State != api.SymlinkBusy || s.Symlinks[0].Busy != api.BusyContent {
		t.Fatalf("symlink status = %+v, want busy/path has content", s.Symlinks)
	}
}

func TestTakeoverMovesContentIntoTheStore(t *testing.T) {
	link := filepath.Join(t.TempDir(), "Documents")
	writeFile(t, filepath.Join(link, "a.txt"), "alpha", time.Now())
	writeFile(t, filepath.Join(link, "sub", "b.txt"), "beta", time.Now())

	n := newNode(t, "a", "b", "127.0.0.1:1", "127.0.0.1:2", "", map[string]config.Store{
		"documents": {Group: "g", Symlinks: []string{link}},
	})
	n.prepareAll()

	res, err := n.eng.Takeover("documents", link, os.Getuid())
	if err != nil {
		t.Fatalf("takeover: %v", err)
	}
	if res.Moved != 2 {
		t.Fatalf("moved = %d, want 2", res.Moved)
	}
	if got := readFile(t, filepath.Join(n.root("documents"), "a.txt")); got != "alpha" {
		t.Fatalf("moved content = %q", got)
	}
	if got := readFile(t, filepath.Join(n.root("documents"), "sub", "b.txt")); got != "beta" {
		t.Fatalf("moved nested content = %q", got)
	}
	if target, err := os.Readlink(link); err != nil || target != n.root("documents") {
		t.Fatalf("link = %q (%v), want the store root", target, err)
	}

	if _, err := n.eng.scan(n.store("documents"), true); err != nil {
		t.Fatal(err)
	}
	if ent := n.store("documents").db.Get("a.txt"); ent == nil {
		t.Fatal("taken-over file was not indexed")
	}

	// A declared foreign symlink is replaced, but its target is untouched.
	oldTarget := filepath.Join(t.TempDir(), "old-storeboard-storage")
	writeFile(t, filepath.Join(oldTarget, "old.txt"), "keep me", time.Now())
	foreign := filepath.Join(t.TempDir(), "Foreign")
	if err := os.Symlink(oldTarget, foreign); err != nil {
		t.Fatal(err)
	}
	m := newNode(t, "a", "b", "127.0.0.1:1", "127.0.0.1:2", "", map[string]config.Store{
		"documents": {Group: "g", Symlinks: []string{foreign}},
	})
	m.prepareAll()
	if target, _ := os.Readlink(foreign); target != m.root("documents") {
		t.Fatalf("declared symlink target = %q, want %q", target, m.root("documents"))
	}
	if got := readFile(t, filepath.Join(oldTarget, "old.txt")); got != "keep me" {
		t.Fatalf("old symlink target changed: %q", got)
	}
}

// A remote store keeps its canonical dir visible with no holder: there is no
// path to swap, only a liveness state.
func TestRemoteStoreStaysVisibleWithoutAHolder(t *testing.T) {
	n := newNode(t, "a", "b", "127.0.0.1:1", "127.0.0.1:2", "", map[string]config.Store{
		"docs": {Group: "g", Sync: config.SyncRemote},
	})
	n.prepareAll()

	writeFile(t, filepath.Join(n.root("docs"), "offline.txt"), "kept", time.Now())
	n.eng.scanOnce(n.store("docs"))

	s := n.eng.Snapshot()[0]
	if s.State != stateDetached {
		t.Fatalf("state = %q, want detached", s.State)
	}
	if got := readFile(t, filepath.Join(n.root("docs"), "offline.txt")); got != "kept" {
		t.Fatalf("content = %q", got)
	}
	if ent := n.store("docs").db.Get("offline.txt"); ent == nil || ent.Deleted {
		t.Fatalf("offline write not indexed: %+v", ent)
	}
}

// The incident's exact shape: a remote store whose mirror is empty while the
// journal and the content store still remember the files (they were
// materialised earlier). Not live, that absence must never be read as a
// deletion — it cost 448 files on amanita once. Live, the same absence IS a
// deletion and has to propagate.
func TestRemoteStoreTombstonesOnlyWhileLive(t *testing.T) {
	n := newNode(t, "b", "a", "127.0.0.1:1", "127.0.0.1:2", "", map[string]config.Store{
		"docs": {Group: "g", Sync: config.SyncRemote},
	})
	n.prepareAll()
	fs := n.store("docs")

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

func TestScanNeverTombstonesUnmaterialisedEntries(t *testing.T) {
	n := newNode(t, "b", "a", "127.0.0.1:1", "127.0.0.1:2", "", map[string]config.Store{
		"docs": {Group: "g", Sync: config.SyncRemote},
	})
	n.prepareAll()
	fs := n.store("docs")

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

// Status must answer while a store is busy: a scan or a materialisation of
// tens of thousands of entries holds the store's scan lock for minutes, and
// an endpoint that waits on it looks like a dead daemon.
func TestSnapshotNeverWaitsOnTheScanLock(t *testing.T) {
	n := mkNode(t, "b", "a", "127.0.0.1:1", "127.0.0.1:2")
	n.prepareAll()
	fs := n.store("docs")

	want := n.eng.Snapshot()[0]

	fs.scanMu.Lock() // a long scan, in progress
	done := make(chan []api.StoreSync, 1)
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
	n := newNode(t, "b", "a", "127.0.0.1:1", "127.0.0.1:2", "", map[string]config.Store{
		"docs": {Group: "g", Sync: config.SyncRemote},
	})
	n.prepareAll()
	fs := n.store("docs")

	if err := fs.db.Put(&Entry{Path: "a.txt", Blob: strings.Repeat("ab", 32), Size: 4, MTime: time.Now().UnixNano(), VV: map[string]uint64{"a": 1}}); err != nil {
		t.Fatal(err)
	}
	fs.scanMu.Lock()
	s := n.eng.Snapshot()[0]
	fs.scanMu.Unlock()
	if s.Entries != 1 || s.Local != 0 || s.Pending != 1 {
		t.Fatalf("grind counts = entries %d local %d pending %d, want 1/0/1", s.Entries, s.Local, s.Pending)
	}

	writeFile(t, filepath.Join(fs.root, "here.txt"), "present", time.Now())
	if _, err := n.eng.scan(fs, true); err != nil {
		t.Fatal(err)
	}
	s = n.eng.Snapshot()[0]
	if s.Local != 1 || s.Scanning {
		t.Fatalf("after a scan: local %d scanning %v, want 1/false", s.Local, s.Scanning)
	}
}

// The stale-dirent repair: an entry whose recorded size contradicts the file
// on disk must be corrected in place, without a version bump.
func TestScanRepairsAStaleSizeWithoutNewVersion(t *testing.T) {
	n := mkNode(t, "b", "a", "127.0.0.1:1", "127.0.0.1:2")
	n.prepareAll()
	fs := n.store("docs")

	writeFile(t, filepath.Join(fs.root, "shot.png"), "content", time.Now())
	sum, err := store.SumFile(filepath.Join(fs.root, "shot.png"))
	if err != nil {
		t.Fatal(err)
	}
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

func TestValidRelRejectsEscapes(t *testing.T) {
	for _, bad := range []string{"", "/abs", "../up", "a/../../b", "a/../..", "\x00"} {
		if validRel(bad) {
			t.Errorf("validRel(%q) = true, want false", bad)
		}
	}
	for _, ok := range []string{"a", "a/b", "a/b.txt", ".hidden"} {
		if !validRel(ok) {
			t.Errorf("validRel(%q) = false, want true", ok)
		}
	}
}

// Nothing declared: the store is permissioned exactly like the directory its
// canonical dir lives in.
func TestStoreInheritsContainerPermissions(t *testing.T) {
	container := t.TempDir()
	if err := os.Chmod(container, 0o750); err != nil {
		t.Fatal(err)
	}
	n := newNode(t, "b", "a", "127.0.0.1:1", "127.0.0.1:2", container, map[string]config.Store{
		"docs": {Group: "g"},
	})
	n.prepareAll()

	fs := n.store("docs")
	fs.scanMu.Lock()
	p := fs.perm
	fs.scanMu.Unlock()
	if p.uid != os.Getuid() || p.gid != os.Getgid() {
		t.Fatalf("owner = %d:%d, want the container's %d:%d", p.uid, p.gid, os.Getuid(), os.Getgid())
	}
	if p.dirMode != 0o750 {
		t.Fatalf("dirMode = %04o, want the container's 0750", p.dirMode)
	}
	fi, err := os.Stat(fs.root)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o750 {
		t.Fatalf("root mode = %04o, want 0750", fi.Mode().Perm())
	}
}

// Declared permissions merge field by field: machine entry > store chain >
// group default > container. An unresolvable name is a warning, not a
// failure — the inherited value stays.
func TestPermissionsOverrides(t *testing.T) {
	uname, gname := currentIDs(t)
	container := t.TempDir()
	if err := os.Chmod(container, 0o750); err != nil {
		t.Fatal(err)
	}
	n := newNode(t, "b", "a", "127.0.0.1:1", "127.0.0.1:2", container, map[string]config.Store{
		"docs": {Group: "g"},
	})
	g := n.cfg.Groups["g"]
	g.Permissions = &config.Permissions{Group: gname, Mode: "0770"}
	n.cfg.Groups["g"] = g
	n.cfg.Permissions = map[string]config.Permissions{"docs": {User: uname}}
	n.prepareAll()

	fs := n.store("docs")
	perm := func() perms {
		fs.scanMu.Lock()
		defer fs.scanMu.Unlock()
		return fs.perm
	}
	p := perm()
	if p.uid != os.Getuid() || p.gid != os.Getgid() || p.dirMode != 0o770 {
		t.Fatalf("perm = %d:%d %04o, want %d:%d 0770 (local user + group defaults)", p.uid, p.gid, p.dirMode, os.Getuid(), os.Getgid())
	}

	n.cfg.Permissions["docs"] = config.Permissions{User: uname, Mode: "0700"}
	n.prepareAll()
	if p := perm(); p.dirMode != 0o700 {
		t.Fatalf("dirMode = %04o, want the machine entry's 0700", p.dirMode)
	}

	n.cfg.Permissions["docs"] = config.Permissions{User: "definitely-not-a-user-xyz"}
	n.prepareAll()
	if p := perm(); p.uid != os.Getuid() {
		t.Fatalf("uid = %d, want the inherited %d when the name does not resolve", p.uid, os.Getuid())
	}
}

// Materialised files take the store's owner and their own synced mode; every
// directory created on the way takes the store's directory mode.
func TestMaterialisedEntriesTakeStorePermissions(t *testing.T) {
	container := t.TempDir()
	if err := os.Chmod(container, 0o750); err != nil {
		t.Fatal(err)
	}
	n := newNode(t, "b", "a", "127.0.0.1:1", "127.0.0.1:2", container, map[string]config.Store{
		"docs": {Group: "g"},
	})
	n.prepareAll()
	fs := n.store("docs")

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
		t.Errorf("file mtime = %d, want the entry's %d", fi.ModTime().UnixNano(), ent.MTime)
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
			t.Errorf("%s mode = %04o, want the store's 0750", d, fi.Mode().Perm())
		}
	}
}

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

// A store root on another filesystem (a pool, an external disk): a delete
// must still land in the trash, copying when rename cannot cross.
func TestTrashAcrossFilesystems(t *testing.T) {
	base, err := os.MkdirTemp("/dev/shm", "storeboard-xdev")
	if err != nil {
		t.Skipf("no second filesystem to cross with: %v", err)
	}
	defer os.RemoveAll(base)

	n := newNode(t, "b", "a", "127.0.0.1:1", "127.0.0.1:2", base, map[string]config.Store{
		"docs": {Group: "g"},
	})
	n.prepareAll()
	fs := n.store("docs")

	if sameDevice(fs.root, n.cfg.DataDir) {
		t.Skip("data dir and store root share a filesystem here; nothing to cross")
	}

	writeFile(t, filepath.Join(fs.root, "gone.txt"), "bye", time.Now())
	if err := n.eng.applyDelete(fs, &Entry{Path: "gone.txt"}); err != nil {
		t.Fatalf("applyDelete: %v", err)
	}
	if _, err := os.Stat(filepath.Join(fs.root, "gone.txt")); !os.IsNotExist(err) {
		t.Fatalf("file still in the store: %v", err)
	}
	if got := readFile(t, filepath.Join(n.cfg.DataDir, "trash", "docs", "gone.txt")); got != "bye" {
		t.Fatalf("trashed content = %q", got)
	}
}

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
