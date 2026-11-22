package syncer

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
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
		fs := b.store("docs")
		fs.queueMu.Lock()
		defer fs.queueMu.Unlock()
		return len(fs.queue) == 0
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
	// The queue op lands when the kernel delivers RELEASE, just after
	// close(2) returns.
	waitFor(t, testWait, "the offline write to be queued", func() bool {
		fs.queueMu.Lock()
		defer fs.queueMu.Unlock()
		return len(fs.queue) == 1
	})
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
	waitFor(t, testWait, "the write to be queued", func() bool {
		fs := b.store("docs")
		fs.queueMu.Lock()
		defer fs.queueMu.Unlock()
		return len(fs.queue) == 1
	})

	startNode(t, a)
	waitFor(t, testWait, "the queued write on the holder", func() bool {
		return exists(filepath.Join(a.root("docs"), "later.txt"))
	})
	if got := readFile(t, filepath.Join(a.root("docs"), "later.txt")); got != body {
		t.Fatalf("holder content = %q, want %q", got, body)
	}
	waitFor(t, testWait, "the queue to drain", func() bool {
		fs := b.store("docs")
		fs.queueMu.Lock()
		defer fs.queueMu.Unlock()
		return len(fs.queue) == 0
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

// versions and restore are answered by the holder: a streamed peer has no
// history, and its own path holds no bytes to restore from.
func TestStreamedStoreForwardsVersionsAndRestore(t *testing.T) {
	old := scanInterval
	scanInterval = 50 * time.Millisecond
	defer func() { scanInterval = old }()

	epA, epB := twoEndpoints(t)
	store := map[string]config.Store{"docs": {Group: "g"}}
	a := newNode(t, "a", "b", epA, epB, "", store)
	a.cfg.Backup = &config.Backup{Enable: true}
	if err := a.cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	a.eng = newEngine(t, a.cfg, a.log)

	b := newNode(t, "b", "a", epB, epA, "", map[string]config.Store{
		"docs": {Group: "g", Sync: config.SyncStream},
	})
	startNode(t, a)
	startNode(t, b)

	writeFile(t, filepath.Join(a.root("docs"), "doc.txt"), "first", time.Now())
	waitFor(t, testWait, "the entry on the streamed peer", func() bool {
		return b.store("docs").db.Get("doc.txt") != nil
	})
	writeFile(t, filepath.Join(a.root("docs"), "doc.txt"), "second", time.Now().Add(2*time.Second))
	waitFor(t, testWait, "the second version on the holder", func() bool {
		return a.store("docs").db.Get("doc.txt").Size == int64(len("second"))
	})

	vs, err := b.eng.Versions("docs", "doc.txt")
	if err != nil {
		t.Fatalf("versions through the holder: %v", err)
	}
	if len(vs.Versions) < 2 {
		t.Fatalf("holder reported %d versions, want at least 2: %+v", len(vs.Versions), vs)
	}

	// Restore the oldest recorded version back into the store.
	oldest := vs.Versions[len(vs.Versions)-1]
	res, err := b.eng.Restore("docs", 0, "doc.txt", oldest.Blob, time.Time{}, "")
	if err != nil {
		t.Fatalf("restore through the holder: %v", err)
	}
	if res.Blob != oldest.Blob {
		t.Fatalf("restored %q, want %q", res.Blob, oldest.Blob)
	}
	waitFor(t, testWait, "the restored content on the holder", func() bool {
		data, err := os.ReadFile(filepath.Join(a.root("docs"), "doc.txt"))
		return err == nil && string(data) == "first"
	})
}

// send on a streamed store is an ordinary write through the mount: the holder
// ends up with the file, the sender keeps no copy.
func TestStreamedStoreSendReachesTheHolder(t *testing.T) {
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
	waitFor(t, testWait, "the peers to connect", func() bool {
		return len(b.eng.holdersFor(b.store("docs"))) > 0
	})

	src := filepath.Join(t.TempDir(), "photo.jpg")
	if err := os.WriteFile(src, []byte("shared from the share sheet\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	res, err := b.eng.Send("docs", os.Getuid(), src, "Pictures/photo.jpg")
	if err != nil {
		t.Fatalf("send into a streamed store: %v", err)
	}
	if res.Path != "Pictures/photo.jpg" {
		t.Fatalf("send reported path %q", res.Path)
	}
	waitFor(t, testWait, "the sent file on the holder", func() bool {
		return exists(filepath.Join(a.root("docs"), "Pictures", "photo.jpg"))
	})
	if got := readFile(t, filepath.Join(a.root("docs"), "Pictures", "photo.jpg")); got != "shared from the share sheet\n" {
		t.Fatalf("holder content = %q", got)
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
	// The create is undone when the kernel delivers RELEASE, so the tree ends
	// up without the file rather than with an empty one.
	waitFor(t, testWait, "the refused file to leave the journal", func() bool {
		ent := n.store("docs").db.Get("big.txt")
		return ent == nil || ent.Deleted
	})
	n.store("docs").queueMu.Lock()
	queued := len(n.store("docs").queue)
	n.store("docs").queueMu.Unlock()
	if queued != 0 {
		t.Fatalf("a refused write queued %d ops", queued)
	}
}

// cfgDir is where this node's durable state lives.
func (fs *StoreState) cfgDir(n *node) string { return n.cfg.DataDir }

// A close(2) with nothing written must commit nothing: the kernel flushes
// every descriptor close, and a shell that dups its redirect target closes the
// original BEFORE it writes. Committing then journalled an empty file and made
// the write that followed fail with EIO.
func TestWriteHandleCommitsOnlyWhenDirty(t *testing.T) {
	n := newNode(t, "a", "b", "127.0.0.1:1", "127.0.0.1:2", "", map[string]config.Store{
		"docs": {Group: "g", Sync: config.SyncStream},
	})
	n.prepareAll()
	t.Cleanup(n.eng.Close)
	fs := n.store("docs")
	m := &storeMutator{e: n.eng, fs: fs}

	// An empty file exists already, so reopening it starts clean: there are
	// no bytes to fetch and nothing has changed yet.
	first, err := m.Create("notes.txt", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	queued := func() int {
		fs.queueMu.Lock()
		defer fs.queueMu.Unlock()
		return len(fs.queue)
	}
	if got := queued(); got != 1 {
		t.Fatalf("queued after creating an empty file = %d, want 1", got)
	}

	h, err := m.Open("notes.txt", false)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Close(); err != nil {
		t.Fatalf("flush with nothing written: %v", err)
	}
	if got := queued(); got != 1 {
		t.Fatalf("an unchanged flush queued another op: %d", got)
	}
	// The handle must still accept the write that follows that flush.
	if _, err := h.WriteAt([]byte("two"), 0); err != nil {
		t.Fatalf("write after an empty flush: %v", err)
	}
	if err := h.Close(); err != nil {
		t.Fatalf("close after writing: %v", err)
	}
	// The new version supersedes the unsent empty one: nobody will ever
	// ask for those bytes.
	if got := queued(); got != 1 {
		t.Fatalf("queued after the write = %d, want 1", got)
	}
	fs.queueMu.Lock()
	size := fs.queue[0].Size
	fs.queueMu.Unlock()
	if size != 3 {
		t.Fatalf("queued version has %d bytes, want 3", size)
	}
	if ent := fs.db.Get("notes.txt"); ent == nil || ent.Size != 3 {
		t.Fatalf("journal entry = %+v", ent)
	}
}

// Creating an empty file is a change even with no bytes written: `touch` must
// reach the mesh.
func TestCreateOfAnEmptyFileIsJournalled(t *testing.T) {
	n := newNode(t, "a", "b", "127.0.0.1:1", "127.0.0.1:2", "", map[string]config.Store{
		"docs": {Group: "g", Sync: config.SyncStream},
	})
	n.prepareAll()
	t.Cleanup(n.eng.Close)
	m := &storeMutator{e: n.eng, fs: n.store("docs")}
	h, err := m.Create("empty.txt", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	ent := n.store("docs").db.Get("empty.txt")
	if ent == nil || ent.Deleted || ent.Size != 0 || ent.Blob == "" {
		t.Fatalf("an empty file was not journalled: %+v", ent)
	}
}

// A viewer opens an image read-write to edit its metadata and then reads it.
// Refusing that read made the file look empty to the application while every
// other reader saw the real bytes.
func TestReadWriteOpenServesTheFilesBytes(t *testing.T) {
	n := newNode(t, "a", "b", "127.0.0.1:1", "127.0.0.1:2", "", map[string]config.Store{
		"docs": {Group: "g", Sync: config.SyncStream},
	})
	n.prepareAll()
	t.Cleanup(n.eng.Close)
	fs := n.store("docs")
	m := &storeMutator{e: n.eng, fs: fs}

	const body = "0123456789abcdef"
	h, err := m.Create("clip.bin", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.WriteAt([]byte(body), 0); err != nil {
		t.Fatal(err)
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen for writing: reads must still return the file.
	rw, err := m.Open("clip.bin", false)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4)
	if n, err := rw.ReadAt(buf, 0); err != nil || string(buf[:n]) != "0123" {
		t.Fatalf("read at 0 = %q (%v)", buf[:n], err)
	}
	buf = make([]byte, 6)
	if n, err := rw.ReadAt(buf, 10); err != nil || string(buf[:n]) != "abcdef" {
		t.Fatalf("read at 10 = %q (%v)", buf[:n], err)
	}
	// Overwrite the middle, then read across the seam: the written bytes and
	// the untouched ones must sit together correctly.
	if _, err := rw.WriteAt([]byte("XYZ"), 6); err != nil {
		t.Fatal(err)
	}
	all := make([]byte, len(body))
	if n, err := rw.ReadAt(all, 0); err != nil && err != io.EOF {
		t.Fatalf("full read: %v", err)
	} else if n != len(body) {
		t.Fatalf("full read returned %d bytes", n)
	}
	if want := "012345XYZ9abcdef"; string(all) != want {
		t.Fatalf("content = %q, want %q", all, want)
	}
	if err := rw.Close(); err != nil {
		t.Fatal(err)
	}
	if ent := fs.db.Get("clip.bin"); ent == nil || ent.Size != int64(len(body)) {
		t.Fatalf("journal entry after the edit = %+v", ent)
	}
}

// Opening a large file for writing must not pull it down: only the ranges
// written travel, and the commit assembles the whole file from the holder.
func TestReadWriteOpenDoesNotFetchUntilWritten(t *testing.T) {
	n := newNode(t, "a", "b", "127.0.0.1:1", "127.0.0.1:2", "", map[string]config.Store{
		"docs": {Group: "g", Sync: config.SyncStream},
	})
	n.prepareAll()
	t.Cleanup(n.eng.Close)
	fs := n.store("docs")
	m := &storeMutator{e: n.eng, fs: fs}

	// No holder is reachable, so any fetch would fail. Opening must succeed,
	// and so must a read of the untouched prefix when nothing was written.
	if err := fs.db.Put(&Entry{Path: "big.bin", Blob: "blob", Size: 1 << 20, VV: map[string]uint64{"a": 1}}); err != nil {
		t.Fatal(err)
	}
	rw, err := m.Open("big.bin", false)
	if err != nil {
		t.Fatalf("open for write: %v", err)
	}
	if err := rw.Close(); err != nil {
		t.Fatalf("closing without writing: %v", err)
	}
	fs.queueMu.Lock()
	queued := len(fs.queue)
	fs.queueMu.Unlock()
	if queued != 0 {
		t.Fatalf("an unmodified open queued %d ops", queued)
	}
	if ent := fs.db.Get("big.bin"); ent.Size != 1<<20 {
		t.Fatalf("journal entry changed: %+v", ent)
	}
}

func TestStreamedRenameMovesImpliedDirectorySubtree(t *testing.T) {
	n := newNode(t, "a", "b", "127.0.0.1:1", "127.0.0.1:2", "", map[string]config.Store{
		"docs": {Group: "g", Sync: config.SyncStream},
	})
	n.prepareAll()
	t.Cleanup(n.eng.Close)
	fs := n.store("docs")
	if err := fs.db.Put(&Entry{Path: "Adventures/one.txt", Blob: "one", Size: 3, VV: map[string]uint64{"a": 1}}); err != nil {
		t.Fatal(err)
	}
	if err := fs.db.Put(&Entry{Path: "Adventures/two.txt", Blob: "two", Size: 3, VV: map[string]uint64{"a": 2}}); err != nil {
		t.Fatal(err)
	}
	m := &storeMutator{e: n.eng, fs: fs}
	if err := m.Rename("Adventures", "Renamed"); err != nil {
		t.Fatalf("rename implied directory: %v", err)
	}
	for _, path := range []string{"Adventures/one.txt", "Adventures/two.txt"} {
		if ent := fs.db.Get(path); ent == nil || !ent.Deleted {
			t.Fatalf("old path not deleted: %s => %+v", path, ent)
		}
	}
	for _, path := range []string{"Renamed/one.txt", "Renamed/two.txt"} {
		if ent := fs.db.Get(path); ent == nil || ent.Deleted {
			t.Fatalf("new path missing: %s => %+v", path, ent)
		}
	}
}

func TestStreamedDirectoryRenameMovesHolderSubtree(t *testing.T) {
	old := scanInterval
	scanInterval = 50 * time.Millisecond
	defer func() { scanInterval = old }()
	epA, epB := twoEndpoints(t)
	a := newNode(t, "a", "b", epA, epB, "", map[string]config.Store{"docs": {Group: "g"}})
	b := newNode(t, "b", "a", epB, epA, "", map[string]config.Store{"docs": {Group: "g", Sync: config.SyncStream}})
	startNode(t, a)
	startNode(t, b)
	if !b.store("docs").mounted() {
		waitPrepared(t, b.store("docs"))
	}
	if !b.store("docs").mounted() {
		t.Skip("no FUSE mount available here")
	}
	writeFile(t, filepath.Join(a.root("docs"), "Adventures", "October", "one.jpg"), "one", time.Now())
	writeFile(t, filepath.Join(a.root("docs"), "Adventures", "October", "two.jpg"), "two", time.Now())
	waitFor(t, testWait, "subtree entries on streamed peer", func() bool {
		return b.store("docs").db.Get("Adventures/October/one.jpg") != nil && b.store("docs").db.Get("Adventures/October/two.jpg") != nil
	})
	if err := os.Rename(filepath.Join(b.root("docs"), "Adventures"), filepath.Join(b.root("docs"), "Renamed")); err != nil {
		t.Fatalf("rename directory through stream: %v", err)
	}
	waitFor(t, testWait, "renamed subtree on holder", func() bool {
		return exists(filepath.Join(a.root("docs"), "Renamed", "October", "one.jpg")) && exists(filepath.Join(a.root("docs"), "Renamed", "October", "two.jpg"))
	})
	for name, want := range map[string]string{"one.jpg": "one", "two.jpg": "two"} {
		if got := readFile(t, filepath.Join(a.root("docs"), "Renamed", "October", name)); got != want {
			t.Fatalf("holder %s = %q, want %q", name, got, want)
		}
	}
}

// GIO's atomic-save sequence is put(temp), then rename(temp, final). The
// holder must pull the pending bytes under final, not receive a rename for a
// temp path it never materialised.
func TestQueuedFileRenameMovesThePutDestination(t *testing.T) {
	n := newNode(t, "a", "b", "127.0.0.1:1", "127.0.0.1:2", "", map[string]config.Store{
		"docs": {Group: "g", Sync: config.SyncStream},
	})
	n.prepareAll()
	t.Cleanup(n.eng.Close)
	m := &storeMutator{e: n.eng, fs: n.store("docs")}
	pf, err := m.Create(".goutputstream-test", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pf.WriteAt([]byte("saved"), 0); err != nil {
		t.Fatal(err)
	}
	if err := pf.Close(); err != nil {
		t.Fatal(err)
	}
	if err := m.Rename(".goutputstream-test", "final.txt"); err != nil {
		t.Fatal(err)
	}
	fs := n.store("docs")
	fs.queueMu.Lock()
	defer fs.queueMu.Unlock()
	foundPut, foundTempPut := false, false
	for _, op := range fs.queue {
		if op.Op == opPut && op.Path == "final.txt" {
			foundPut = true
		}
		if op.Op == opPut && op.Path == ".goutputstream-test" {
			foundTempPut = true
		}
	}
	if !foundPut || foundTempPut {
		t.Fatalf("queue after GIO rename: %+v", fs.queue)
	}
}

// Every kind of queued op is retired once a holder's entry covers it. Deletes
// and new directories used to stay queued forever, re-sent on every flush.
func TestQueueRetiresDeletesAndDirectories(t *testing.T) {
	n := newNode(t, "a", "b", "127.0.0.1:1", "127.0.0.1:2", "", map[string]config.Store{
		"docs": {Group: "g", Sync: config.SyncStream},
	})
	t.Cleanup(n.eng.Close)
	fs := n.store("docs")
	m := &storeMutator{e: n.eng, fs: fs}
	if err := m.MakeDir("album", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := fs.db.Put(&Entry{Path: "old.txt", Blob: strings.Repeat("ab", 32), Size: 3, VV: map[string]uint64{"b": 1}}); err != nil {
		t.Fatal(err)
	}
	if err := m.Remove("old.txt"); err != nil {
		t.Fatal(err)
	}
	fs.queueMu.Lock()
	ops := append([]queueOp(nil), fs.queue...)
	fs.queueMu.Unlock()
	if len(ops) != 2 {
		t.Fatalf("queued %d ops, want 2", len(ops))
	}
	for _, op := range ops {
		rem := &Entry{Path: op.Path, Dir: op.Op == opMkdir, Deleted: op.Op == opDelete, VV: MergeVV(op.VV, map[string]uint64{"b": 7})}
		n.eng.ackQueue(fs, rem)
	}
	fs.queueMu.Lock()
	left := len(fs.queue)
	fs.queueMu.Unlock()
	if left != 0 {
		t.Fatalf("%d ops still queued after the holder covered them", left)
	}
}

// Replacing a file by rename (how GIO saves) queues the upload after the
// delete of what it replaces, with a version that dominates it.
func TestRenameOverExistingQueuesUploadAfterDelete(t *testing.T) {
	n := newNode(t, "a", "b", "127.0.0.1:1", "127.0.0.1:2", "", map[string]config.Store{
		"docs": {Group: "g", Sync: config.SyncStream},
	})
	t.Cleanup(n.eng.Close)
	fs := n.store("docs")
	m := &storeMutator{e: n.eng, fs: fs}
	if err := fs.db.Put(&Entry{Path: "photo.jpg", Blob: strings.Repeat("cd", 32), Size: 3, VV: map[string]uint64{"b": 4}}); err != nil {
		t.Fatal(err)
	}
	pf, err := m.Create(".goutputstream-XYZ", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pf.WriteAt([]byte("new"), 0); err != nil {
		t.Fatal(err)
	}
	if err := pf.Close(); err != nil {
		t.Fatal(err)
	}
	if err := m.Rename(".goutputstream-XYZ", "photo.jpg"); err != nil {
		t.Fatal(err)
	}
	fs.queueMu.Lock()
	ops := append([]queueOp(nil), fs.queue...)
	fs.queueMu.Unlock()
	var delAt, putAt = -1, -1
	for i, op := range ops {
		if op.Path == "photo.jpg" && op.Op == opDelete {
			delAt = i
		}
		if op.Path == "photo.jpg" && op.Op == opPut {
			putAt = i
		}
	}
	if delAt < 0 || putAt < 0 || putAt < delAt {
		t.Fatalf("queue order %+v: want the delete of photo.jpg before its upload", ops)
	}
	if !Dominates(ops[putAt].VV, map[string]uint64{"b": 4}) {
		t.Fatalf("upload vv %v does not dominate the replaced version", ops[putAt].VV)
	}
}

// A queue left by an older build - what the live BulkPictures store carried:
// uploads of editor temporary files whose bytes were gone, deletes of paths
// the holder never had, a directory renamed and then removed - drains once a
// holder is connected, instead of sitting at the same count forever.
func TestStuckLegacyQueueDrains(t *testing.T) {
	oldScan, oldIndex := scanInterval, indexInterval
	scanInterval, indexInterval = 50*time.Millisecond, 100*time.Millisecond
	defer func() { scanInterval, indexInterval = oldScan, oldIndex }()

	epA, epB := twoEndpoints(t)
	a := newNode(t, "a", "b", epA, epB, "", map[string]config.Store{
		"docs": {Group: "g"},
	})
	b := newNode(t, "b", "a", epB, epA, "", map[string]config.Store{
		"docs": {Group: "g", Sync: config.SyncStream},
	})
	vv := func(n uint64) map[string]uint64 { return map[string]uint64{"b": n} }
	tmp := "album/.goutputstream-ABC123"
	mv := func(from, to string, n uint64) renameMove {
		return renameMove{From: from, Old: &Entry{Path: from, Dir: true, VV: vv(n - 1)}, New: &Entry{Path: to, Dir: true, VV: vv(n)}}
	}
	legacy := []queueOp{
		{ID: "op1", Op: opDelete, Path: tmp, VV: vv(1)},
		{ID: "op2", Op: opPut, Path: tmp, Blob: strings.Repeat("ab", 32), Size: 6 << 20, VV: vv(2)}, // bytes long gone
		{ID: "op3", Op: opRmdir, Path: ".subtree-test/nested", VV: vv(3)},
		{ID: "op4", Op: opRmdir, Path: ".subtree-test", VV: vv(4)},
		{ID: "op5", Op: opRename, Path: "rename-check", To: "rename-check-moved", Dir: true, VV: vv(6),
			Moves: []renameMove{mv("rename-check", "rename-check-moved", 6)}},
		{ID: "op6", Op: opRmdir, Path: "rename-check-moved", VV: vv(7)},
		{ID: "op7", Op: opDelete, Path: tmp, VV: vv(8)},
	}
	var lines []byte
	for _, op := range legacy {
		line, err := json.Marshal(op)
		if err != nil {
			t.Fatal(err)
		}
		lines = append(append(lines, line...), '\n')
	}
	index := queueIndexPath(b.cfg.DataDir, "docs")
	if err := os.MkdirAll(filepath.Dir(index), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(index, lines, 0o600); err != nil {
		t.Fatal(err)
	}
	fs := b.store("docs")
	if err := b.eng.loadQueue(fs); err != nil { // as at startup
		t.Fatal(err)
	}
	if len(fs.queue) != 6 {
		t.Fatalf("loaded %d ops, want 6 (the upload with no bytes dropped)", len(fs.queue))
	}

	startNode(t, a)
	startNode(t, b)
	waitPrepared(t, b.store("docs"))
	waitFor(t, testWait, "the legacy queue to drain", func() bool {
		fs := b.store("docs")
		fs.queueMu.Lock()
		defer fs.queueMu.Unlock()
		return len(fs.queue) == 0 && fs.queuedBytes == 0
	})
	if ent := a.store("docs").db.Get(tmp); ent == nil || !ent.Deleted {
		t.Fatalf("holder entry for the temporary file = %+v, want a tombstone", ent)
	}
}

// A later put or delete supersedes an earlier upload of the same path - but
// not one a rename in between still needs: the holder moves the bytes it has.
func TestCompactQueue(t *testing.T) {
	put := func(id, path string) queueOp { return queueOp{ID: id, Op: opPut, Path: path, Size: 1} }
	del := func(id, path string) queueOp { return queueOp{ID: id, Op: opDelete, Path: path} }
	ren := func(id, from, to string) queueOp {
		return queueOp{ID: id, Op: opRename, Path: from, To: to, Moves: []renameMove{{From: from, New: &Entry{Path: to}}}}
	}
	ids := func(ops []queueOp) string {
		var out []string
		for _, op := range ops {
			out = append(out, op.ID)
		}
		return strings.Join(out, ",")
	}
	for _, tc := range []struct {
		name       string
		ops        []queueOp
		kept, drop string
	}{
		{"delete supersedes", []queueOp{put("1", "p"), del("2", "p")}, "2", "1"},
		{"put supersedes", []queueOp{put("1", "p"), put("2", "p")}, "2", "1"},
		{"rename keeps the bytes", []queueOp{put("1", "p"), ren("2", "p", "q"), del("3", "p")}, "1,2,3", ""},
		{"other paths untouched", []queueOp{put("1", "p"), del("2", "q")}, "1,2", ""},
		{"delete then put kept", []queueOp{del("1", "p"), put("2", "p")}, "1,2", ""},
	} {
		kept, dropped := compactQueue(tc.ops)
		if ids(kept) != tc.kept || ids(dropped) != tc.drop {
			t.Errorf("%s: kept %q dropped %q, want %q / %q", tc.name, ids(kept), ids(dropped), tc.kept, tc.drop)
		}
	}
}
