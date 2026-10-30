package syncer

import (
	"context"
	"encoding/json"
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
	a := newNode(t, "a", "b", epA, epB, "", map[string]config.Store{"docs": {Group: "g"}})
	b := newNode(t, "b", "a", epB, epA, "", map[string]config.Store{"docs": {Group: "g", Sync: config.SyncStream}})
	startNode(t, a)
	startNode(t, b)

	const body = "0123456789abcdefghij"
	writeFile(t, filepath.Join(a.root("docs"), "clip.bin"), body, time.Now())
	waitFor(t, testWait, "journal entry on streamed peer", func() bool { return b.store("docs").db.Get("clip.bin") != nil })
	ent := b.store("docs").db.Get("clip.bin")
	if ent.Deleted || ent.Blob == "" {
		t.Fatalf("journal entry did not arrive whole: %+v", ent)
	}
	if b.eng.store.Has(ent.Blob) {
		t.Fatal("streamed store pulled the whole blob instead of reading a range")
	}
	fetch := &streamFetcher{e: b.eng, fs: b.store("docs")}
	got, err := fetch.ReadRange(context.Background(), streamfs.Entry{Path: "clip.bin", Blob: ent.Blob, Size: ent.Size}, 4, 6)
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

// The user-facing path: reading at the canonical path goes through FUSE, over
// the wire, to the holder.
func TestStreamedStoreServesTheFileThroughItsMount(t *testing.T) {
	old := scanInterval
	scanInterval = 50 * time.Millisecond
	defer func() { scanInterval = old }()
	epA, epB := twoEndpoints(t)
	a := newNode(t, "a", "b", epA, epB, "", map[string]config.Store{"docs": {Group: "g"}})
	b := newNode(t, "b", "a", epB, epA, "", map[string]config.Store{"docs": {Group: "g", Sync: config.SyncStream}})
	startNode(t, a)
	startNode(t, b)
	const body = "photo bytes that live on the holder\n"
	writeFile(t, filepath.Join(a.root("docs"), "Pictures", "clip.jpg"), body, time.Now())
	waitFor(t, testWait, "journal entry on streamed peer", func() bool { return b.store("docs").db.Get("Pictures/clip.jpg") != nil })
	waitPrepared(t, b.store("docs"))
	if !b.store("docs").mounted() {
		t.Skip("no FUSE mount available here")
	}
	got, err := os.ReadFile(filepath.Join(b.root("docs"), "Pictures", "clip.jpg"))
	if err != nil {
		t.Fatalf("read through mount: %v", err)
	}
	if string(got) != body {
		t.Fatalf("content = %q, want %q", got, body)
	}
	if b.eng.store.Has(b.store("docs").db.Get("Pictures/clip.jpg").Blob) {
		t.Fatal("read left a whole-file copy behind")
	}
}

func TestStreamedStoreReadWithoutAHolderFails(t *testing.T) {
	n := newNode(t, "a", "b", "127.0.0.1:1", "127.0.0.1:2", "", map[string]config.Store{"docs": {Group: "g", Sync: config.SyncStream}})
	n.prepareAll()
	t.Cleanup(n.eng.Close)
	ent := &Entry{Path: "clip.bin", Blob: "deadbeef", Size: 10, Local: true}
	if err := n.eng.applyEntry(n.store("docs"), ent); err != nil {
		t.Fatal(err)
	}
	if got := n.store("docs").db.Get("clip.bin"); got == nil || got.Deleted {
		t.Fatal("journal lost entry with no holder")
	}
	fetch := &streamFetcher{e: n.eng, fs: n.store("docs")}
	if _, err := fetch.ReadRange(context.Background(), streamfs.Entry{Path: "clip.bin", Blob: ent.Blob, Size: ent.Size}, 0, 4); err == nil {
		t.Fatal("read with no holder succeeded")
	}
}

func TestHoldersForPrefersTheDeclaredPeerThenThroughput(t *testing.T) {
	n := newNode(t, "a", "b", "127.0.0.1:1", "127.0.0.1:2", "", map[string]config.Store{"docs": {Group: "g", Sync: config.SyncStream, Preferred: "b"}})
	n.eng.mu.Lock()
	n.eng.sends["b"] = nil
	n.eng.sends["c"] = nil
	n.eng.mu.Unlock()
	n.eng.cfg.Peers["c"] = config.Peer{Endpoints: []string{"127.0.0.1:3"}}
	n.eng.cfg.Groups["g"] = config.Group{Peers: []string{"a", "b", "c"}}
	n.eng.noteRead("c", 4<<20, time.Millisecond)
	n.eng.noteRead("b", 1<<10, time.Second)
	fs := n.store("docs")
	if got := n.eng.holdersFor(fs); len(got) != 2 || got[0] != "b" || got[1] != "c" {
		t.Fatalf("holder order = %v, want [b c]", got)
	}
	fs.preferred = ""
	if got := n.eng.holdersFor(fs); len(got) != 2 || got[0] != "c" {
		t.Fatalf("holder order without preference = %v, want c first", got)
	}
}

func TestStreamedApplyRecordsWithoutWritingBytes(t *testing.T) {
	n := newNode(t, "a", "b", "127.0.0.1:1", "127.0.0.1:2", "", map[string]config.Store{"docs": {Group: "g", Sync: config.SyncStream}})
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
		info, err := os.Stat(path)
		if err != nil || info.Size() != ent.Size {
			t.Fatalf("mounted entry stat = %v, err=%v", info, err)
		}
	} else if _, err := os.Stat(path); err == nil {
		t.Fatal("streamed apply wrote a local file")
	}
	if n.eng.store.Has("abc123") {
		t.Fatal("streamed apply stored the blob")
	}
}

// Silent peers model a request whose reply is lost during a link replacement.
type silentSender struct{}

func (silentSender) Send([]byte) error     { return nil }
func (silentSender) SendBulk([]byte) error { return nil }

func TestRangedReadTimesOutWhenHolderStaysQuiet(t *testing.T) {
	n := newNode(t, "a", "b", "127.0.0.1:1", "127.0.0.1:2", "", map[string]config.Store{"docs": {Group: "g", Sync: config.SyncStream}})
	n.eng.mu.Lock()
	n.eng.sends["b"] = silentSender{}
	n.eng.mu.Unlock()
	old := readTimeout
	readTimeout = 120 * time.Millisecond
	defer func() { readTimeout = old }()
	start := time.Now()
	_, err := (&streamFetcher{e: n.eng, fs: n.store("docs")}).ReadRange(context.Background(), streamfs.Entry{Path: "x", Blob: "b", Size: 4}, 0, 4)
	if err == nil || time.Since(start) > time.Second {
		t.Fatalf("silent holder result err=%v duration=%s", err, time.Since(start))
	}
}

// A timed-out first attempt is retried against the replacement connection.
type reconnectingSender struct {
	e     *Engine
	calls int
}

func (s *reconnectingSender) Send(b []byte) error {
	s.calls++
	if s.calls == 2 {
		var fr frame
		if err := json.Unmarshal(b, &fr); err != nil {
			return err
		}
		s.e.deliverRead(&frame{Type: "readData", ID: fr.ID, Path: fr.Path, Data: []byte("data")})
	}
	return nil
}
func (*reconnectingSender) SendBulk([]byte) error { return nil }

func TestRangedReadRetriesAfterReconnect(t *testing.T) {
	n := newNode(t, "a", "b", "127.0.0.1:1", "127.0.0.1:2", "", map[string]config.Store{"docs": {Group: "g", Sync: config.SyncStream}})
	old := readTimeout
	readTimeout = 400 * time.Millisecond
	defer func() { readTimeout = old }()
	s := &reconnectingSender{e: n.eng}
	n.eng.mu.Lock()
	n.eng.sends["b"] = s
	n.eng.mu.Unlock()
	got, err := (&streamFetcher{e: n.eng, fs: n.store("docs")}).ReadRange(context.Background(), streamfs.Entry{Path: "x", Blob: "b", Size: 4}, 0, 4)
	if err != nil || string(got) != "data" || s.calls < 2 {
		t.Fatalf("retry result=%q err=%v sends=%d", got, err, s.calls)
	}
}
