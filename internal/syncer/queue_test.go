package syncer

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"codeberg.org/kyleraykbs/storeboard/internal/config"
)

// Writing at the canonical path on a streamed peer must put the bytes on the
// holder: the streamed machine keeps the queue entry, the holder keeps the
// file, and the file appears there without a whole-store mirror.
func TestStreamedStoreWritesThroughToTheHolder(t *testing.T) {
	old := scanInterval
	scanInterval = 50 * time.Millisecond
	defer func() { scanInterval = old }()

	epA, epB := twoEndpoints(t)
	a := newNode(t, "a", "b", epA, epB, "", map[string]config.Store{
		"docs": {Group: "g"},
	})
	b := newNode(t, "b", "a", epB, epA, "", map[string]config.Store{
		"docs": {Group: "g", Sync: config.SyncStream},
	})
	startNode(t, a)
	startNode(t, b)

	waitFor(t, testWait, "the peers to connect", func() bool {
		return len(b.eng.holdersFor(b.store("docs"))) > 0
	})
	waitPrepared(t, b.store("docs"))
	if !b.store("docs").mounted() {
		t.Skip("no FUSE mount available here")
	}

	const body = "written through the mount\n"
	path := filepath.Join(b.root("docs"), "notes.txt")
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write through the mount: %v", err)
	}

	waitFor(t, testWait, "the file on the holder", func() bool {
		return exists(filepath.Join(a.root("docs"), "notes.txt"))
	})
	if got := readFile(t, filepath.Join(a.root("docs"), "notes.txt")); got != body {
		t.Fatalf("holder content = %q, want %q", got, body)
	}
	// The streamed side keeps no copy once the holder has taken it.
	waitFor(t, testWait, "the queue to drain", func() bool {
		b.store("docs").scanMu.Lock()
		defer b.store("docs").scanMu.Unlock()
		return len(b.store("docs").queue) == 0
	})
	ent := b.store("docs").db.Get("notes.txt")
	if ent == nil || ent.Blob == "" {
		t.Fatalf("journal entry after write = %+v", ent)
	}
	if b.eng.store.Has(ent.Blob) {
		t.Fatal("the streamed peer kept the written bytes in its blob store")
	}
}

// With no holder the write is accepted and queued: the tree still shows it,
// and the bytes stay in the queue until a holder comes back.
func TestStreamedStoreQueuesWritesWhileOffline(t *testing.T) {
	n := newNode(t, "a", "b", "127.0.0.1:1", "127.0.0.1:2", "", map[string]config.Store{
		"docs": {Group: "g", Sync: config.SyncStream},
	})
	n.prepareAll()
	t.Cleanup(n.eng.Close)
	waitPrepared(t, n.store("docs"))
	if !n.store("docs").mounted() {
		t.Skip("no FUSE mount available here")
	}

	const body = "made while the holder is away\n"
	if err := os.WriteFile(filepath.Join(n.root("docs"), "offline.txt"), []byte(body), 0o644); err != nil {
		t.Fatalf("offline write: %v", err)
	}
	fs := n.store("docs")
	if ent := fs.db.Get("offline.txt"); ent == nil || ent.Deleted || ent.Blob == "" {
		t.Fatalf("offline write was not journalled: %+v", ent)
	}
	fs.scanMu.Lock()
	queued := len(fs.queue)
	fs.scanMu.Unlock()
	if queued != 1 {
		t.Fatalf("queued ops = %d, want 1", queued)
	}
	// The queue file is the durable copy: it survives a restart.
	if _, err := os.Stat(filepath.Join(n.cfg.DataDir, "queue", "docs.ndjson")); err != nil {
		t.Fatalf("queue index missing: %v", err)
	}
	// A read of queued content is served from the queue, not invented.
	ent := fs.db.Get("offline.txt")
	rc, err := n.eng.openEntryBlob(fs, ent)
	if err != nil {
		t.Fatalf("queued bytes are not readable: %v", err)
	}
	rc.Close()
}

// A write queued while no holder was reachable drains when one appears: the
// queue is replayed through the ordinary index exchange.
func TestStreamedStoreFlushesQueuedWritesOnReconnect(t *testing.T) {
	old := scanInterval
	scanInterval = 50 * time.Millisecond
	defer func() { scanInterval = old }()

	epA, epB := twoEndpoints(t)
	a := newNode(t, "a", "b", epA, epB, "", map[string]config.Store{
		"docs": {Group: "g"},
	})
	b := newNode(t, "b", "a", epB, epA, "", map[string]config.Store{
		"docs": {Group: "g", Sync: config.SyncStream},
	})
	startNode(t, b)
	waitPrepared(t, b.store("docs"))
	if !b.store("docs").mounted() {
		t.Skip("no FUSE mount available here")
	}

	const body = "queued before the holder was up\n"
	if err := os.WriteFile(filepath.Join(b.root("docs"), "later.txt"), []byte(body), 0o644); err != nil {
		t.Fatalf("offline write: %v", err)
	}
	b.store("docs").scanMu.Lock()
	queued := len(b.store("docs").queue)
	b.store("docs").scanMu.Unlock()
	if queued != 1 {
		t.Fatalf("queued ops before the holder existed = %d, want 1", queued)
	}

	startNode(t, a)
	waitFor(t, testWait, "the queued write on the holder", func() bool {
		return exists(filepath.Join(a.root("docs"), "later.txt"))
	})
	if got := readFile(t, filepath.Join(a.root("docs"), "later.txt")); got != body {
		t.Fatalf("holder content = %q, want %q", got, body)
	}
	waitFor(t, testWait, "the queue to drain", func() bool {
		b.store("docs").scanMu.Lock()
		defer b.store("docs").scanMu.Unlock()
		return len(b.store("docs").queue) == 0
	})
}

// Deleting through the mount removes the file on the holder, which keeps its
// copy in the trash rather than unlinking it.
func TestStreamedStoreDeleteReachesTheHolder(t *testing.T) {
	old := scanInterval
	scanInterval = 50 * time.Millisecond
	defer func() { scanInterval = old }()

	epA, epB := twoEndpoints(t)
	a := newNode(t, "a", "b", epA, epB, "", map[string]config.Store{
		"docs": {Group: "g"},
	})
	b := newNode(t, "b", "a", epB, epA, "", map[string]config.Store{
		"docs": {Group: "g", Sync: config.SyncStream},
	})
	startNode(t, a)
	startNode(t, b)
	waitPrepared(t, b.store("docs"))
	if !b.store("docs").mounted() {
		t.Skip("no FUSE mount available here")
	}

	writeFile(t, filepath.Join(a.root("docs"), "gone.txt"), "delete me", time.Now())
	waitFor(t, testWait, "the entry on the streamed peer", func() bool {
		return b.store("docs").db.Get("gone.txt") != nil
	})

	if err := os.Remove(filepath.Join(b.root("docs"), "gone.txt")); err != nil {
		t.Fatalf("delete through the mount: %v", err)
	}
	waitFor(t, testWait, "the deletion on the holder", func() bool {
		return !exists(filepath.Join(a.root("docs"), "gone.txt"))
	})
	waitFor(t, testWait, "the holder's trashed copy", func() bool {
		return exists(filepath.Join(a.cfg.DataDir, "trash", "docs", "gone.txt"))
	})
}

// A rename through the mount moves the file on the holder and in the journal.
func TestStreamedStoreRenameReachesTheHolder(t *testing.T) {
	old := scanInterval
	scanInterval = 50 * time.Millisecond
	defer func() { scanInterval = old }()

	epA, epB := twoEndpoints(t)
	a := newNode(t, "a", "b", epA, epB, "", map[string]config.Store{
		"docs": {Group: "g"},
	})
	b := newNode(t, "b", "a", epB, epA, "", map[string]config.Store{
		"docs": {Group: "g", Sync: config.SyncStream},
	})
	startNode(t, a)
	startNode(t, b)
	waitPrepared(t, b.store("docs"))
	if !b.store("docs").mounted() {
		t.Skip("no FUSE mount available here")
	}

	writeFile(t, filepath.Join(a.root("docs"), "before.txt"), "move me", time.Now())
	waitFor(t, testWait, "the entry on the streamed peer", func() bool {
		return b.store("docs").db.Get("before.txt") != nil
	})

	if err := os.Rename(filepath.Join(b.root("docs"), "before.txt"), filepath.Join(b.root("docs"), "after.txt")); err != nil {
		t.Fatalf("rename through the mount: %v", err)
	}
	waitFor(t, testWait, "the rename on the holder", func() bool {
		return exists(filepath.Join(a.root("docs"), "after.txt")) && !exists(filepath.Join(a.root("docs"), "before.txt"))
	})
	if got := readFile(t, filepath.Join(a.root("docs"), "after.txt")); got != "move me" {
		t.Fatalf("renamed content = %q", got)
	}
}

// waitPrepared blocks until the engine has run prepareRoot for a store, which
// is when a streamed store's mount exists (or has failed).
func waitPrepared(t *testing.T, fs *StoreState) {
	t.Helper()
	waitFor(t, testWait, "the store to be prepared", func() bool {
		fs.metaMu.Lock()
		defer fs.metaMu.Unlock()
		return fs.prepared
	})
}

// readonly refuses the write and leaves the tree untouched.
func TestStreamedStoreReadOnlyPolicyRefusesWrites(t *testing.T) {
	n := newNode(t, "a", "b", "127.0.0.1:1", "127.0.0.1:2", "", map[string]config.Store{
		"docs": {Group: "g", Sync: config.SyncStream, Offline: &config.OfflineQueue{Policy: config.StreamQueueReadOnly}},
	})
	n.prepareAll()
	t.Cleanup(n.eng.Close)
	waitPrepared(t, n.store("docs"))
	if !n.store("docs").mounted() {
		t.Skip("no FUSE mount available here")
	}
	if err := os.WriteFile(filepath.Join(n.root("docs"), "nope.txt"), []byte("x"), 0o644); err == nil {
		t.Fatal("a readonly streamed store accepted a write")
	}
	if ent := n.store("docs").db.Get("nope.txt"); ent != nil && !ent.Deleted {
		t.Fatalf("a refused write was journalled: %+v", ent)
	}
}

// A capped queue refuses the write that would cross its limit, before anything
// is journalled.
func TestStreamedStoreCappedQueueRefusesOverflow(t *testing.T) {
	n := newNode(t, "a", "b", "127.0.0.1:1", "127.0.0.1:2", "", map[string]config.Store{
		"docs": {Group: "g", Sync: config.SyncStream, Offline: &config.OfflineQueue{Policy: config.StreamQueueCapped, MaxBytes: 8}},
	})
	n.prepareAll()
	t.Cleanup(n.eng.Close)
	waitPrepared(t, n.store("docs"))
	if !n.store("docs").mounted() {
		t.Skip("no FUSE mount available here")
	}
	if err := os.WriteFile(filepath.Join(n.root("docs"), "big.txt"), []byte("far more than eight bytes"), 0o644); err == nil {
		t.Fatal("a capped queue accepted an over-limit write")
	}
	if ent := n.store("docs").db.Get("big.txt"); ent != nil && !ent.Deleted {
		t.Fatalf("refused write was journalled: %+v", ent)
	}
}

// cfgDir is where this node's durable state lives.
func (fs *StoreState) cfgDir(n *node) string { return n.cfg.DataDir }
