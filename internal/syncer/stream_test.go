package syncer

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"codeberg.org/kyleraykbs/storeboard/internal/config"
	"codeberg.org/kyleraykbs/storeboard/internal/streamfs"
)

// A streamed store keeps the journal and no bytes. A read is served by the
// holder over the ranged wire, so the caller gets exactly the window it asked
// for and this machine never grows a copy of the file.
func TestStreamedStoreReadsARangeFromItsHolder(t *testing.T) {
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

	const body = "0123456789abcdefghij"
	writeFile(t, filepath.Join(a.root("docs"), "clip.bin"), body, time.Now())

	waitFor(t, testWait, "journal entry on the streamed peer", func() bool {
		return b.store("docs").db.Get("clip.bin") != nil
	})
	ent := b.store("docs").db.Get("clip.bin")
	if ent.Deleted || ent.Blob == "" {
		t.Fatalf("journal entry did not arrive whole: %+v", ent)
	}
	// The bytes stayed on the holder: a streamed store records the entry and
	// fetches on demand, so the content store holds nothing.
	if b.eng.store.Has(ent.Blob) {
		t.Fatal("streamed store pulled the whole blob instead of reading a range")
	}

	fetch := &streamFetcher{e: b.eng, fs: b.store("docs")}
	got, err := fetch.ReadRange(context.Background(), streamfs.Entry{
		Path: "clip.bin",
		Blob: ent.Blob,
		Size: ent.Size,
	}, 4, 6)
	if err != nil {
		t.Fatalf("ranged read: %v", err)
	}
	if string(got) != body[4:10] {
		t.Fatalf("range = %q, want %q", got, body[4:10])
	}
	if b.eng.store.Has(ent.Blob) {
		t.Fatal("a ranged read left a whole-file copy behind")
	}
}

// The user-facing path: reading the file at its canonical path on a streamed
// peer goes through the mount, over the wire, to the holder.
func TestStreamedStoreServesTheFileThroughItsMount(t *testing.T) {
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

	const body = "photo bytes that live on the holder\n"
	writeFile(t, filepath.Join(a.root("docs"), "Pictures", "clip.jpg"), body, time.Now())

	waitFor(t, testWait, "journal entry on the streamed peer", func() bool {
		return b.store("docs").db.Get("Pictures/clip.jpg") != nil
	})
	if !b.store("docs").mounted() {
		t.Skip("no FUSE mount available here: the journal path is covered elsewhere")
	}

	got, err := os.ReadFile(filepath.Join(b.root("docs"), "Pictures", "clip.jpg"))
	if err != nil {
		t.Fatalf("read through the mount: %v", err)
	}
	if string(got) != body {
		t.Fatalf("content through the mount = %q, want %q", got, body)
	}
	if b.eng.store.Has(b.store("docs").db.Get("Pictures/clip.jpg").Blob) {
		t.Fatal("reading through the mount left a whole-file copy on the streamed peer")
	}
}

// A store with no reachable holder still presents its tree but cannot serve
// bytes: the read fails with something a caller can act on, and nothing is
// invented.
func TestStreamedStoreReadWithoutAHolderFails(t *testing.T) {
	n := newNode(t, "a", "b", "127.0.0.1:1", "127.0.0.1:2", "", map[string]config.Store{
		"docs": {Group: "g", Sync: config.SyncStream},
	})
	n.prepareAll()
	t.Cleanup(n.eng.Close)
	ent := &Entry{Path: "clip.bin", Blob: "deadbeef", Size: 10, Local: true}
	if err := n.eng.applyEntry(n.store("docs"), ent); err != nil {
		t.Fatal(err)
	}
	if got := n.store("docs").db.Get("clip.bin"); got == nil || got.Deleted {
		t.Fatal("the journal lost an entry for a store with no holder")
	}

	fetch := &streamFetcher{e: n.eng, fs: n.store("docs")}
	if _, err := fetch.ReadRange(context.Background(), streamfs.Entry{Path: "clip.bin", Blob: ent.Blob, Size: ent.Size}, 0, 4); err == nil {
		t.Fatal("a read with no holder answered successfully")
	}
	if n.eng.store.Has(ent.Blob) {
		t.Fatal("a failed read stored bytes")
	}
}

// The declared preference wins, then measured throughput, then the name: the
// order is what makes a pinned holder actually get the traffic.
func TestHoldersForPrefersTheDeclaredPeerThenThroughput(t *testing.T) {
	n := newNode(t, "a", "b", "127.0.0.1:1", "127.0.0.1:2", "", map[string]config.Store{
		"docs": {Group: "g", Sync: config.SyncStream, Preferred: "b"},
	})
	n.eng.mu.Lock()
	n.eng.sends["b"] = nil
	n.eng.sends["c"] = nil
	n.eng.mu.Unlock()
	n.eng.cfg.Peers["c"] = config.Peer{Endpoints: []string{"127.0.0.1:3"}}
	n.eng.cfg.Groups["g"] = config.Group{Peers: []string{"a", "b", "c"}}

	// c is faster, but b is declared.
	n.eng.noteRead("c", 4<<20, time.Millisecond)
	n.eng.noteRead("b", 1<<10, time.Second)

	fs := n.store("docs")
	if got := n.eng.holdersFor(fs); len(got) != 2 || got[0] != "b" || got[1] != "c" {
		t.Fatalf("holder order = %v, want [b c]", got)
	}

	// Without a preference, measured throughput decides.
	fs.preferred = ""
	if got := n.eng.holdersFor(fs); len(got) != 2 || got[0] != "c" {
		t.Fatalf("holder order without a preference = %v, want c first", got)
	}
}

// A streamed store never materialises what it learns: the entry is journal
// state, and the mount is the only place those bytes appear.
func TestStreamedApplyRecordsWithoutWritingBytes(t *testing.T) {
	n := newNode(t, "a", "b", "127.0.0.1:1", "127.0.0.1:2", "", map[string]config.Store{
		"docs": {Group: "g", Sync: config.SyncStream},
	})
	n.prepareAll()
	t.Cleanup(n.eng.Close)
	fs := n.store("docs")
	ent := &Entry{Path: "dir/clip.bin", Blob: "abc123", Size: 4, Mode: 0o644, VV: map[string]uint64{"b": 1}}
	if err := n.eng.applyEntry(fs, ent); err != nil {
		t.Fatal(err)
	}
	if got := fs.db.Get("dir/clip.bin"); got == nil || got.Blob != "abc123" {
		t.Fatalf("journal entry = %+v", got)
	}
	path := filepath.Join(fs.root, "dir", "clip.bin")
	if fs.mounted() {
		// The mount is the tree: the entry is visible with the journal's
		// size and no bytes of its own exist here.
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("mounted view lost the entry: %v", err)
		}
		if info.Size() != ent.Size {
			t.Fatalf("mounted size = %d, want %d", info.Size(), ent.Size)
		}
	} else if _, err := os.Stat(path); err == nil {
		t.Fatal("streamed apply wrote a file into the store's own path")
	}
	if n.eng.store.Has("abc123") {
		t.Fatal("streamed apply stored the blob")
	}
}
