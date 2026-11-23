package syncer

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"codeberg.org/kyleraykbs/storeboard/internal/config"
	"codeberg.org/kyleraykbs/storeboard/internal/store"
	"codeberg.org/kyleraykbs/storeboard/internal/streamfs"
)

func streamedNode(t *testing.T) (*node, *StoreState, *storeMutator) {
	t.Helper()
	n := newNode(t, "a", "b", "127.0.0.1:1", "127.0.0.1:2", "", map[string]config.Store{
		"docs": {Group: "g", Sync: config.SyncStream},
	})
	n.prepareAll()
	t.Cleanup(n.eng.Close)
	fs := n.store("docs")
	return n, fs, &storeMutator{e: n.eng, fs: fs}
}

func queueSnapshot(fs *StoreState) []queueOp {
	fs.queueMu.Lock()
	defer fs.queueMu.Unlock()
	return append([]queueOp(nil), fs.queue...)
}

func sumOf(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "f")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	sum, err := store.SumFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return sum
}

// GIO's atomic save renames its temporary file over the original before the
// kernel delivers RELEASE. The handle must commit under the new name: it used
// to find its old path tombstoned and throw the write away, leaving the
// destination journalled empty and the original deleted on the holder.
func TestRenameBeforeReleaseKeepsTheBytes(t *testing.T) {
	n, fs, m := streamedNode(t)
	if err := fs.db.Put(&Entry{Path: "photo.jpg", Blob: strings.Repeat("cd", 32), Size: 3, VV: map[string]uint64{"b": 4}}); err != nil {
		t.Fatal(err)
	}
	pf, err := m.Create(".goutputstream-ABC", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	const body = "the edited photo"
	if _, err := pf.WriteAt([]byte(body), 0); err != nil {
		t.Fatal(err)
	}
	if err := m.Rename(".goutputstream-ABC", "photo.jpg"); err != nil {
		t.Fatal(err)
	}
	if err := pf.Close(); err != nil {
		t.Fatal(err)
	}
	ent := fs.db.Get("photo.jpg")
	if ent == nil || ent.Deleted || ent.Size != int64(len(body)) || ent.Blob != sumOf(t, body) {
		t.Fatalf("destination after the save = %+v", ent)
	}
	if !Dominates(ent.VV, map[string]uint64{"b": 4}) {
		t.Fatalf("saved version %v does not replace the original", ent.VV)
	}
	var put *queueOp
	for _, op := range queueSnapshot(fs) {
		switch {
		case op.Op == opRename:
			t.Fatalf("a rename of a path only this handle had was queued: %+v", op)
		case op.Op == opPut && op.Path == "photo.jpg":
			put = &op
		case op.Op == opPut:
			t.Fatalf("unexpected upload %+v", op)
		}
	}
	if put == nil {
		t.Fatalf("no upload of the saved file: %+v", queueSnapshot(fs))
	}
	got, err := os.ReadFile(filepath.Join(queueBytesDir(n.cfg.DataDir, fs.name), put.ID))
	if err != nil || string(got) != body {
		t.Fatalf("queued bytes = %q (%v)", got, err)
	}
}

// A file created inside a directory that is renamed while the file is open
// lands under the directory's new name.
func TestDirectoryRenameMovesOpenHandle(t *testing.T) {
	_, fs, m := streamedNode(t)
	if err := m.MakeDir("album", 0o755); err != nil {
		t.Fatal(err)
	}
	pf, err := m.Create("album/new.txt", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pf.WriteAt([]byte("hello"), 0); err != nil {
		t.Fatal(err)
	}
	if err := m.Rename("album", "trip"); err != nil {
		t.Fatal(err)
	}
	if err := pf.Close(); err != nil {
		t.Fatal(err)
	}
	if ent := fs.db.Get("trip/new.txt"); ent == nil || ent.Deleted || ent.Size != 5 {
		t.Fatalf("moved file = %+v", ent)
	}
	if ent := fs.db.Get("album/new.txt"); ent != nil && !ent.Deleted {
		t.Fatalf("old path still live: %+v", ent)
	}
}

func countFDs(t *testing.T) int {
	t.Helper()
	ents, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Skip("no /proc/self/fd")
	}
	return len(ents)
}

func scratchFiles(t *testing.T, dir string) []string {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, ent := range ents {
		if strings.HasPrefix(ent.Name(), "new-") {
			out = append(out, ent.Name())
		}
	}
	return out
}

// Closing a write handle that wrote nothing, failed, or whose file was
// removed must close and remove its scratch file.
func TestUncommittedHandlesLeaveNoScratch(t *testing.T) {
	n, fs, m := streamedNode(t)
	n.eng.mu.Lock()
	n.eng.sends["b"] = silentSender{}
	n.eng.mu.Unlock()
	if err := fs.db.Put(&Entry{Path: "big.bin", Blob: strings.Repeat("ab", 32), Size: 10, VV: map[string]uint64{"b": 1}}); err != nil {
		t.Fatal(err)
	}
	dir := queueBytesDir(n.cfg.DataDir, fs.name)
	before := countFDs(t)
	for i := 0; i < 20; i++ {
		h, err := m.Open("big.bin", false)
		if err != nil {
			t.Fatal(err)
		}
		if err := h.Close(); err != nil {
			t.Fatal(err)
		}
	}
	// touch; rm before the release.
	h, err := m.Create("gone.txt", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.WriteAt([]byte("x"), 0); err != nil {
		t.Fatal(err)
	}
	if err := m.Remove("gone.txt"); err != nil {
		t.Fatal(err)
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	if left := scratchFiles(t, dir); len(left) != 0 {
		t.Fatalf("scratch files left behind: %v", left)
	}
	if after := countFDs(t); after > before+5 {
		t.Fatalf("descriptors grew from %d to %d", before, after)
	}
}

// With no holder connected, an in-place edit of a file whose bytes are not
// here is refused at open: its commit could only fail after the application
// was told the write succeeded.
func TestReadWriteOpenRefusedWithNoHolder(t *testing.T) {
	_, fs, m := streamedNode(t)
	if err := fs.db.Put(&Entry{Path: "big.bin", Blob: strings.Repeat("ab", 32), Size: 10, VV: map[string]uint64{"b": 1}}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Open("big.bin", false); !errors.Is(err, syscall.EROFS) {
		t.Fatalf("in-place open with no holder: %v, want EROFS", err)
	}
	h, err := m.Open("big.bin", true)
	if err != nil {
		t.Fatalf("truncating open needs no holder: %v", err)
	}
	h.Close()
}

// A queue write that fails (ENOSPC) must leave the journal and the queue as
// they were: the journal used to claim a version whose op was never written,
// and the next start deleted its bytes.
func TestFailedQueueWriteJournalsNothing(t *testing.T) {
	n, fs, m := streamedNode(t)
	index := queueIndexPath(n.cfg.DataDir, fs.name)
	_ = os.Remove(index)
	if err := os.MkdirAll(index, 0o700); err != nil { // an index that cannot be opened
		t.Fatal(err)
	}
	h, err := m.Create("a.txt", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.WriteAt([]byte("bytes"), 0); err != nil {
		t.Fatal(err)
	}
	if err := h.Close(); err == nil {
		t.Fatal("close succeeded with an unwritable queue")
	}
	if q := queueSnapshot(fs); len(q) != 0 {
		t.Fatalf("queue holds %+v", q)
	}
	if ent := fs.db.Get("a.txt"); ent != nil && ent.Size != 0 {
		t.Fatalf("journal claims the unqueued version: %+v", ent)
	}
}

// featureSender is a peer link that advertised features.
type featureSender struct {
	feats []string
	reply func(b []byte)
}

func (s featureSender) Send(b []byte) error {
	if s.reply != nil {
		s.reply(b)
	}
	return nil
}
func (featureSender) SendBulk([]byte) error         { return nil }
func (s featureSender) HasFeature(name string) bool { return slices.Contains(s.feats, name) }

// Another streamed peer records entries without their bytes. Its entry
// covering a queued upload is no acknowledgement: dropping the op deleted the
// only copy.
func TestStreamedPeerCannotAcknowledgeAnUpload(t *testing.T) {
	n, fs, m := streamedNode(t)
	n.eng.mu.Lock()
	n.eng.sends["b"] = featureSender{feats: []string{featureReplicaList}}
	n.eng.mu.Unlock()
	h, err := m.Create("a.txt", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.WriteAt([]byte("only copy"), 0); err != nil {
		t.Fatal(err)
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	op := queueSnapshot(fs)[0]
	rem := &Entry{Path: op.Path, Blob: op.Blob, Size: op.Size, VV: MergeVV(op.VV, map[string]uint64{"b": 1})}
	n.eng.ackQueue("b", fs, rem)
	if len(queueSnapshot(fs)) != 1 {
		t.Fatal("a streamed peer's entry retired the upload")
	}
	n.eng.mu.Lock()
	n.eng.sends["b"] = featureSender{feats: []string{featureReplicaList, featureReplicaPrefix + "docs"}}
	n.eng.mu.Unlock()
	n.eng.ackQueue("b", fs, rem)
	if len(queueSnapshot(fs)) != 0 {
		t.Fatal("the holder's entry did not retire the upload")
	}
}

func missingReply(e *Engine) func([]byte) {
	return func(b []byte) {
		var fr frame
		if json.Unmarshal(b, &fr) == nil && fr.Type == "read" {
			go e.deliverRead(&frame{Type: "readErr", ID: fr.ID, Path: fr.Path, Code: refuseMissing, Error: "no such entry"})
		}
	}
}

// A streamed peer's "missing" is no answer for the store: the read keeps
// trying until the deadline (the real holder may be reconnecting). A
// holder's "missing" is final at once.
func TestMissingFromStreamedPeerIsNotFinal(t *testing.T) {
	n, fs, _ := streamedNode(t)
	old := readTimeout
	readTimeout = 400 * time.Millisecond
	defer func() { readTimeout = old }()
	ent := streamfs.Entry{Path: "x", Blob: strings.Repeat("ab", 32), Size: 4}

	n.eng.mu.Lock()
	n.eng.sends["b"] = featureSender{feats: []string{featureReplicaList}, reply: missingReply(n.eng)}
	n.eng.mu.Unlock()
	start := time.Now()
	_, err := (&streamFetcher{e: n.eng, fs: fs}).ReadRange(context.Background(), ent, 0, 4)
	if err == nil || time.Since(start) < readTimeout/2 {
		t.Fatalf("streamed peer's refusal ended the read after %s (%v)", time.Since(start), err)
	}

	n.eng.mu.Lock()
	n.eng.sends["b"] = featureSender{feats: []string{featureReplicaList, featureReplicaPrefix + "docs"}, reply: missingReply(n.eng)}
	n.eng.mu.Unlock()
	start = time.Now()
	_, err = (&streamFetcher{e: n.eng, fs: fs}).ReadRange(context.Background(), ent, 0, 4)
	if !errors.Is(err, syscall.ENOENT) || time.Since(start) > readTimeout/2 {
		t.Fatalf("holder's refusal: %v after %s", err, time.Since(start))
	}
	var refusal *holderRefusal
	if _, err := n.eng.readRangeFrom(context.Background(), "b", fs, ent, 0, 4); !errors.As(err, &refusal) || refusal.peer != "b" {
		t.Fatalf("refusal names %+v, want peer b", refusal)
	}
}

// relinkSender loses the first read; the link then changes and the retry
// is answered.
type relinkSender struct {
	e     *Engine
	mu    sync.Mutex
	calls int
}

func (s *relinkSender) Send(b []byte) error {
	var fr frame
	if err := json.Unmarshal(b, &fr); err != nil || fr.Type != "read" {
		return nil
	}
	s.mu.Lock()
	s.calls++
	first := s.calls == 1
	s.mu.Unlock()
	if first {
		go func() {
			time.Sleep(50 * time.Millisecond)
			s.e.peerLinkChanged("b")
		}()
		return nil
	}
	go s.e.deliverRead(&frame{Type: "readData", ID: fr.ID, Data: []byte("data")})
	return nil
}
func (*relinkSender) SendBulk([]byte) error { return nil }

// A read in flight when its link is replaced is retried at once, not after
// the holder has been quiet for half of readTimeout (or readCap, while the
// new link is busy).
func TestReadInFlightOnAReplacedLinkRetriesAtOnce(t *testing.T) {
	n, fs, _ := streamedNode(t)
	old := readTimeout
	readTimeout = 20 * time.Second
	defer func() { readTimeout = old }()
	s := &relinkSender{e: n.eng}
	n.eng.mu.Lock()
	n.eng.sends["b"] = s
	n.eng.mu.Unlock()
	// Keep the holder "heard from" so only the link change can end the
	// first attempt.
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(10 * time.Millisecond):
				n.eng.heard("b")
			}
		}
	}()
	start := time.Now()
	got, err := (&streamFetcher{e: n.eng, fs: fs}).ReadRange(context.Background(), streamfs.Entry{Path: "x", Blob: strings.Repeat("ab", 32), Size: 4}, 0, 4)
	if err != nil || string(got) != "data" {
		t.Fatalf("read = %q, %v", got, err)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("the read waited %s on a link that was gone", took)
	}
}

// Read ids start at the clock, so a restarted reader never reuses an id a
// holder still has a cancel for; and a cancel older than cancelTTL is
// ignored.
func TestReadIDsAndCancelsSurviveRestart(t *testing.T) {
	n, _, _ := streamedNode(t)
	before := time.Now().UnixNano()
	n.eng.mu.Lock()
	id := n.eng.nextReadIDLocked()
	n.eng.mu.Unlock()
	if id <= before {
		t.Fatalf("first read id %d is not seeded from the clock", id)
	}
	n.eng.cancelMu.Lock()
	n.eng.readCancels[cancelKey{"b", 5}] = time.Now().Add(-2 * cancelTTL)
	n.eng.cancelMu.Unlock()
	if n.eng.readCancelled("b", 5) {
		t.Fatal("a stale cancel suppressed a read")
	}
}

// A stale GIO temporary whose upload no holder has taken holds the only copy
// of its bytes: cleanup must leave it until the upload lands.
func TestStaleAtomicCleanupKeepsUnuploadedBytes(t *testing.T) {
	n, fs, m := streamedNode(t)
	h, err := m.Create(".goutputstream-OLD", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.WriteAt([]byte("unsaved work"), 0); err != nil {
		t.Fatal(err)
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	ent := fs.db.Get(".goutputstream-OLD")
	n.eng.cleanupStaleAtomic(fs, ent)
	q := queueSnapshot(fs)
	if len(q) != 1 || q[0].Op != opPut {
		t.Fatalf("queue after cleanup = %+v", q)
	}
	if _, err := os.Stat(filepath.Join(queueBytesDir(n.cfg.DataDir, fs.name), q[0].ID)); err != nil {
		t.Fatalf("the only copy was removed: %v", err)
	}
	if cur := fs.db.Get(".goutputstream-OLD"); cur == nil || cur.Deleted {
		t.Fatalf("entry tombstoned before its bytes reached a holder: %+v", cur)
	}
}

// The queue file is appended to, not rewritten per op, and reads back to
// exactly the live queue: moved uploads, drops, and a torn tail included.
func TestQueueFileAppendsAndReplays(t *testing.T) {
	n, fs, m := streamedNode(t)
	for _, name := range []string{"one", "two", "three"} {
		h, err := m.Create(name+".txt", 0o644)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := h.WriteAt([]byte(name), 0); err != nil {
			t.Fatal(err)
		}
		if err := h.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := m.Rename("two.txt", "deux.txt"); err != nil {
		t.Fatal(err)
	}
	q := queueSnapshot(fs)
	var one queueOp
	for _, op := range q {
		if op.Path == "one.txt" && op.Op == opPut {
			one = op
		}
	}
	n.eng.ackQueue("b", fs, &Entry{Path: one.Path, Blob: one.Blob, VV: one.VV})
	want := queueSnapshot(fs)
	if len(want) != len(q)-1 {
		t.Fatalf("ack left %d of %d ops", len(want), len(q))
	}

	index := queueIndexPath(n.cfg.DataDir, fs.name)
	raw, err := os.ReadFile(index)
	if err != nil {
		t.Fatal(err)
	}
	if lines := strings.Count(string(raw), "\n"); lines <= len(want) {
		t.Fatalf("queue file has %d records for %d ops: it was rewritten, not appended to", lines, len(want))
	}
	// A crash tore the last append.
	f, err := os.OpenFile(index, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString(`{"id":"torn","op":"pu`)
	f.Close()

	reload := func() []queueOp {
		fs.queueMu.Lock()
		fs.queue, fs.queuedBytes = nil, 0
		fs.queueMu.Unlock()
		if err := n.eng.loadQueue(fs); err != nil {
			t.Fatal(err)
		}
		return queueSnapshot(fs)
	}
	sameOps := func(got, want []queueOp) {
		t.Helper()
		if len(got) != len(want) {
			t.Fatalf("replayed %d ops, want %d: %+v", len(got), len(want), got)
		}
		for i := range got {
			if got[i].ID != want[i].ID || got[i].Path != want[i].Path || got[i].Op != want[i].Op {
				t.Fatalf("op %d replayed as %+v, want %+v", i, got[i], want[i])
			}
		}
	}
	sameOps(reload(), want)
	// The torn tail was cut off, so the next append is whole.
	if err := m.MakeDir("after", 0o755); err != nil {
		t.Fatal(err)
	}
	want = queueSnapshot(fs)
	sameOps(reload(), want)
}

// A flush after a write sends only what the holder has not been sent; a new
// link gets everything again.
func TestFlushSendsOnlyNewOpsUntilTheLinkChanges(t *testing.T) {
	n, fs, _ := streamedNode(t)
	s := &recordingSender{}
	n.eng.mu.Lock()
	n.eng.sends["b"] = s
	n.eng.mu.Unlock()
	sentEntries := func() int {
		s.mu.Lock()
		defer s.mu.Unlock()
		total := 0
		for _, line := range s.control {
			var fr frame
			if json.Unmarshal([]byte(line), &fr) == nil && fr.Type == "index" {
				total += len(fr.Entries)
			}
		}
		s.control = nil
		return total
	}
	add := func(p string) {
		if err := n.eng.enqueueOp(fs, queueOp{ID: newOpID(n.eng), Op: opMkdir, Path: p, VV: map[string]uint64{"a": fs.db.Next()}}); err != nil {
			t.Fatal(err)
		}
	}
	add("one")
	n.eng.flushQueueOnce(fs)
	if got := sentEntries(); got != 1 {
		t.Fatalf("first flush sent %d entries", got)
	}
	add("two")
	n.eng.flushQueueOnce(fs)
	if got := sentEntries(); got != 1 {
		t.Fatalf("second flush sent %d entries, want only the new op", got)
	}
	n.eng.peerLinkChanged("b")
	n.eng.mu.Lock()
	n.eng.sends["b"] = s
	n.eng.mu.Unlock()
	n.eng.flushQueueOnce(fs)
	if got := sentEntries(); got != 2 {
		t.Fatalf("flush over a new link sent %d entries, want 2", got)
	}
}
