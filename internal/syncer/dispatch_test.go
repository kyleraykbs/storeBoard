package syncer

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"codeberg.org/kyleraykbs/storeboard/internal/config"
	"codeberg.org/kyleraykbs/storeboard/internal/store"
)

// A store whose index worker is stuck (here: behind a scan holding its lock)
// must never make the link wait. OnPeerFrame used to block once that store's
// queue was full, which stopped the link's read loop - and with it the
// replies every streamed read on this machine was waiting for. The frames
// that overflow are dropped and recovered by a digest once the store drains.
func TestBlockedStoreNeverBlocksTheLink(t *testing.T) {
	old := indexQueueBytes
	indexQueueBytes = 4 << 10
	defer func() { indexQueueBytes = old }()

	n := newNode(t, "a", "b", "127.0.0.1:1", "127.0.0.1:2", "", map[string]config.Store{
		"slow":  {Group: "g"},
		"quick": {Group: "g"},
	})
	n.prepareAll()
	t.Cleanup(n.eng.Close)
	slow := n.store("slow")
	writeFile(t, filepath.Join(slow.root, "here.txt"), "a file to offer", time.Now())
	if _, err := n.eng.scan(slow, true); err != nil {
		t.Fatal(err)
	}
	rec := &recordingSender{}
	n.eng.mu.Lock()
	n.eng.sends["b"] = rec
	n.eng.mu.Unlock()

	slow.scanMu.Lock()
	locked := true
	defer func() {
		if locked {
			slow.scanMu.Unlock()
		}
	}()

	// A delete of a file this side has: applying it takes the scan lock.
	loc := slow.db.Get("here.txt")
	if loc == nil {
		t.Fatal("scan did not journal the file")
	}
	stuck := func(i int) []byte {
		vv := map[string]uint64{"b": uint64(i + 1)}
		for p, v := range loc.VV {
			vv[p] = v
		}
		return mustFrame(t, frame{Type: "index", Store: "slow", Entries: []*Entry{{
			Path: "here.txt", Deleted: true, MTime: time.Now().UnixNano(), VV: vv,
		}}})
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 2000; i++ {
			n.eng.OnPeerFrame("b", stuck(i))
		}
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("OnPeerFrame blocked behind a busy store")
	}

	content := "arrives anyway"
	sum, err := store.Sum(strings.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	ent := &Entry{Path: "quick.txt", Blob: sum, Size: int64(len(content)),
		MTime: time.Now().UnixNano(), Mode: 0o644, VV: map[string]uint64{"b": 1}}
	n.eng.OnPeerFrame("b", mustFrame(t, frame{Type: "index", Store: "quick", Entries: []*Entry{ent}}))
	waitFor(t, 10*time.Second, "the quick store to request its blob", func() bool {
		return n.eng.isWanted("quick", ent.Path)
	})

	// Once the store drains, the peer gets a fresh offer of its index so
	// whatever was dropped is exchanged again.
	slow.scanMu.Unlock()
	locked = false
	waitFor(t, 20*time.Second, "a resync of the store that dropped frames", func() bool {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		for _, f := range rec.control {
			if strings.Contains(f, `"type":"index","store":"slow"`) && strings.Contains(f, "here.txt") {
				return true
			}
		}
		return false
	})
	if !strings.Contains(n.log.String(), "resyncing") {
		t.Fatal("no frames were dropped: the test did not fill the queue")
	}
}

// A dying link's disconnect, delivered after its replacement connected, must
// not remove the replacement's sender.
func TestStaleDisconnectKeepsTheLiveSender(t *testing.T) {
	n := mkNode(t, "a", "b", "127.0.0.1:1", "127.0.0.1:2")
	t.Cleanup(n.eng.Close)
	s1, s2 := &recordingSender{}, &recordingSender{}
	n.eng.OnPeerConnected("b", s1)
	n.eng.OnPeerConnected("b", s2)
	n.eng.OnPeerDisconnected("b", s1)
	if got := n.eng.senderFor("b"); got != s2 {
		t.Fatalf("sender after a stale disconnect = %v, want the live one", got)
	}
	n.eng.OnPeerDisconnected("b", s2)
	if got := n.eng.senderFor("b"); got != nil {
		t.Fatalf("sender after the live link went = %v, want none", got)
	}
}

// A reconnect stops transfers running on the old link - they must not carry
// on over the new one with a gap in the middle - and makes wants pointed at
// the peer due again at once.
func TestReconnectCancelsSendsAndReasksWants(t *testing.T) {
	n := mkNode(t, "a", "b", "127.0.0.1:1", "127.0.0.1:2")
	n.prepareAll()
	t.Cleanup(n.eng.Close)
	fs := n.store("docs")
	writeFile(t, filepath.Join(fs.root, "big.bin"), strings.Repeat("z", 4*store.ChunkSize), time.Now())
	if _, err := n.eng.scan(fs, true); err != nil {
		t.Fatal(err)
	}

	release := make(chan struct{})
	old := &recordingSender{block: release}
	n.eng.OnPeerConnected("b", old)
	n.eng.handleWant("b", &frame{Type: "want", Store: "docs", Path: "big.bin"})
	waitFor(t, 5*time.Second, "the transfer to start", func() bool {
		active, _ := n.eng.sendingCount()
		return active == 1
	})

	want := &pending{entry: &Entry{Path: "theirs.txt", Blob: strings.Repeat("2", 64)}, peer: "b", tried: []string{"b"}, asked: time.Now()}
	n.eng.addWant("docs", want)

	replacement := &recordingSender{}
	n.eng.OnPeerConnected("b", replacement)
	close(release)
	waitFor(t, 5*time.Second, "the old transfer to stop", func() bool {
		active, queued := n.eng.sendingCount()
		return active == 0 && queued == 0
	})
	replacement.mu.Lock()
	moved := len(replacement.bulk)
	replacement.mu.Unlock()
	if moved != 0 {
		t.Fatalf("%d chunks of the old link's transfer went out on the new link", moved)
	}
	old.mu.Lock()
	sentOld := len(old.bulk)
	old.mu.Unlock()
	if sentOld > 1 {
		t.Fatalf("the cancelled transfer kept sending: %d chunks", sentOld)
	}

	n.eng.mu.Lock()
	asked := want.asked
	n.eng.mu.Unlock()
	if !asked.IsZero() {
		t.Fatal("a want pointed at the reconnected peer was not made due again")
	}
}

// A want is only consumed by bytes that were applied: a blobEnd with no
// transfer behind it, or bytes that fail verification, leave it in place.
func TestUnappliedTransfersKeepTheirWants(t *testing.T) {
	n := mkNode(t, "a", "b", "127.0.0.1:1", "127.0.0.1:2")
	n.prepareAll()
	t.Cleanup(n.eng.Close)
	content := "the real bytes"
	sum, err := store.Sum(strings.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	ent := &Entry{Path: "x.txt", Blob: sum, Size: int64(len(content)), Mode: 0o644,
		MTime: time.Now().UnixNano(), VV: map[string]uint64{"b": 1}}
	want := &pending{entry: ent, peer: "b", tried: []string{"b"}, asked: time.Now()}
	n.eng.addWant("docs", want)

	// The start of the transfer was lost.
	n.eng.handleBlobEnd("b", &frame{Type: "blobEnd", Store: "docs", Path: ent.Path, Blob: sum, Entry: ent})
	n.eng.finishWG.Wait()
	if !n.eng.isWanted("docs", ent.Path) {
		t.Fatal("a blobEnd with no transfer consumed the want")
	}

	// The bytes are wrong.
	n.eng.handleBlob("b", &frame{Type: "blob", Store: "docs", Path: ent.Path, Blob: sum, Seq: 0, Data: []byte("not the bytes")})
	n.eng.handleBlobEnd("b", &frame{Type: "blobEnd", Store: "docs", Path: ent.Path, Blob: sum, Entry: ent})
	n.eng.finishWG.Wait()
	if !n.eng.isWanted("docs", ent.Path) {
		t.Fatal("a transfer that failed verification consumed the want")
	}

	// The right bytes are applied and consume it.
	n.eng.handleBlob("b", &frame{Type: "blob", Store: "docs", Path: ent.Path, Blob: sum, Seq: 0, Data: []byte(content)})
	n.eng.handleBlobEnd("b", &frame{Type: "blobEnd", Store: "docs", Path: ent.Path, Blob: sum, Entry: ent})
	n.eng.finishWG.Wait()
	if n.eng.isWanted("docs", ent.Path) {
		t.Fatal("an applied transfer left its want behind")
	}
}

// Once Close has cancelled the transfers, a want still being handled must not
// start a new one: its sendWG.Add would race Close's Wait.
func TestNoTransferStartsAfterClose(t *testing.T) {
	n := mkNode(t, "a", "b", "127.0.0.1:1", "127.0.0.1:2")
	n.prepareAll()
	t.Cleanup(n.eng.Close)
	fs := n.store("docs")
	writeFile(t, filepath.Join(fs.root, "f.txt"), "bytes", time.Now())
	if _, err := n.eng.scan(fs, true); err != nil {
		t.Fatal(err)
	}
	n.eng.mu.Lock()
	n.eng.sends["b"] = &recordingSender{}
	n.eng.mu.Unlock()
	n.eng.cancelSends()
	n.eng.handleWant("b", &frame{Type: "want", Store: "docs", Path: "f.txt"})
	if active, queued := n.eng.sendingCount(); active != 0 || queued != 0 {
		t.Fatalf("after close: %d running, %d queued transfers", active, queued)
	}
}

// Link callbacks run on the transport's goroutines, inside its shutdown
// wait: they must not wait for a streamed store's scan lock.
func TestPeerCallbacksDoNotWaitForScans(t *testing.T) {
	n := newNode(t, "a", "b", "127.0.0.1:1", "127.0.0.1:2", "", map[string]config.Store{
		"docs": {Group: "g", Sync: config.SyncStream},
	})
	t.Cleanup(n.eng.Close)
	fs := n.store("docs")
	fs.scanMu.Lock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		n.eng.OnPeerConnected("b", nopSender{})
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		fs.scanMu.Unlock()
		t.Fatal("OnPeerConnected waited for the store's scan lock")
	}
	fs.scanMu.Unlock()
	waitFor(t, 10*time.Second, "the store to go live", func() bool {
		return fs.publish().State == stateAttached
	})
}
