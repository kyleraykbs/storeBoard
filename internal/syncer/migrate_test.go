package syncer

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"codeberg.org/kyleraykbs/storeboard/internal/config"
)

// restartAs rebuilds a node's engine on the same data dir with the store's
// sync mode changed, as a config edit and a daemon restart would.
func restartAs(t *testing.T, n *node, store, mode string) {
	t.Helper()
	n.stop()
	n.eng.Close()
	s := n.cfg.StoreTree[store]
	s.Sync = mode
	n.cfg.StoreTree[store] = s
	n.eng = newEngine(t, n.cfg, n.log)
}

// Switching a replica to stream moves its files aside instead of mounting
// over them; each is then settled with the holder - retired when the holder
// has the bytes, uploaded when it does not - and nothing is lost.
func TestLocalToStreamMigratesFilesSafely(t *testing.T) {
	oldScan, oldIndex, oldMig := scanInterval, indexInterval, migrateEvery
	scanInterval, indexInterval, migrateEvery = 50*time.Millisecond, 100*time.Millisecond, 100*time.Millisecond
	defer func() { scanInterval, indexInterval, migrateEvery = oldScan, oldIndex, oldMig }()

	epA, epB := twoEndpoints(t)
	a := newNode(t, "a", "b", epA, epB, "", map[string]config.Store{"docs": {Group: "g"}})
	b := newNode(t, "b", "a", epB, epA, "", map[string]config.Store{"docs": {Group: "g"}})
	startNode(t, a)
	startNode(t, b)
	writeFile(t, filepath.Join(b.root("docs"), "shared.txt"), "both have this\n", time.Now())
	waitFor(t, testWait, "shared.txt on the holder", func() bool { return exists(filepath.Join(a.root("docs"), "shared.txt")) })

	// b's own file, written while a is away: only b has it when it switches.
	a.stop()
	writeFile(t, filepath.Join(b.root("docs"), "album", "only-b.txt"), "only on b\n", time.Now())
	waitFor(t, testWait, "b to record only-b.txt", func() bool { return b.store("docs").db.Get("album/only-b.txt") != nil })

	restartAs(t, b, "docs", config.SyncStream)
	startNode(t, b)
	waitPrepared(t, b.store("docs"))
	if !b.store("docs").mounted() {
		t.Skip("no FUSE mount available here")
	}
	// Readable through the mount from the staged copy, holder or not.
	if got := readFile(t, filepath.Join(b.root("docs"), "album", "only-b.txt")); got != "only on b\n" {
		t.Fatalf("staged file through the mount = %q", got)
	}

	a.eng = newEngine(t, a.cfg, a.log)
	startNode(t, a)
	waitFor(t, testWait, "only-b.txt uploaded to the holder", func() bool {
		got, err := os.ReadFile(filepath.Join(a.root("docs"), "album", "only-b.txt"))
		return err == nil && string(got) == "only on b\n"
	})
	waitFor(t, testWait, "the migration to finish", func() bool {
		n, _ := b.store("docs").migr.pending()
		_, err := os.Stat(manifestPath(b.cfg.DataDir, "docs"))
		return n == 0 && os.IsNotExist(err)
	})
	waitFor(t, testWait, "the upload queue to drain", func() bool {
		fs := b.store("docs")
		fs.queueMu.Lock()
		defer fs.queueMu.Unlock()
		return len(fs.queue) == 0
	})
	// The retired local copies are in the trash, not gone.
	if !exists(filepath.Join(b.cfg.DataDir, "trash", "docs", "shared.txt")) {
		t.Fatal("the retired local copy of shared.txt is not in the trash")
	}
	if got := readMode(b.cfg.DataDir, "docs"); got != config.SyncStream {
		t.Fatalf("recorded mode = %q", got)
	}
}

// Switching a streamed store to local writes its queued uploads into the
// canonical dir as the files they are.
func TestStreamToLocalKeepsQueuedWrites(t *testing.T) {
	n := newNode(t, "b", "a", "127.0.0.1:1", "127.0.0.1:2", "", map[string]config.Store{"docs": {Group: "g", Sync: config.SyncStream}})
	fs := n.store("docs")
	m := &storeMutator{e: n.eng, fs: fs}
	pf, err := m.Create("notes/new.txt", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pf.WriteAt([]byte("written offline"), 0); err != nil {
		t.Fatal(err)
	}
	if err := pf.Close(); err != nil {
		t.Fatal(err)
	}
	if err := writeMode(n.cfg.DataDir, "docs", config.SyncStream); err != nil {
		t.Fatal(err)
	}

	restartAs(t, n, "docs", config.SyncLocal)
	n.prepareAll()
	fs = n.store("docs")
	if fs.stream {
		t.Fatalf("store still streamed: %s", fs.modeNote)
	}
	if got := readFile(t, filepath.Join(fs.root, "notes", "new.txt")); got != "written offline" {
		t.Fatalf("queued write in the local copy = %q", got)
	}
	if ent := fs.db.Get("notes/new.txt"); ent == nil || !ent.Local {
		t.Fatalf("journal entry = %+v, want it marked local", ent)
	}
	if _, err := os.Stat(queueIndexPath(n.cfg.DataDir, "docs")); !os.IsNotExist(err) {
		t.Fatalf("queue still present: %v", err)
	}
	if got := readMode(n.cfg.DataDir, "docs"); got != config.SyncLocal {
		t.Fatalf("recorded mode = %q", got)
	}
	n.eng.scanOnce(fs)
	if ent := fs.db.Get("notes/new.txt"); ent == nil || ent.Deleted {
		t.Fatalf("scan after the switch changed the entry: %+v", ent)
	}
}

// A store whose contents would not fit stays streamed rather than filling
// the disk.
func TestStreamToLocalRefusesWhenItWouldNotFit(t *testing.T) {
	n := newNode(t, "b", "a", "127.0.0.1:1", "127.0.0.1:2", "", map[string]config.Store{"docs": {Group: "g", Sync: config.SyncStream}})
	fs := n.store("docs")
	if err := fs.db.Put(&Entry{Path: "huge.bin", Blob: strings.Repeat("ab", 32), Size: 1 << 62, VV: map[string]uint64{"a": 1}}); err != nil {
		t.Fatal(err)
	}
	if err := writeMode(n.cfg.DataDir, "docs", config.SyncStream); err != nil {
		t.Fatal(err)
	}
	restartAs(t, n, "docs", config.SyncLocal)
	fs = n.store("docs")
	if !fs.stream || fs.modeNote == "" {
		t.Fatalf("stream=%v note=%q, want it kept streamed with a reason", fs.stream, fs.modeNote)
	}
}
