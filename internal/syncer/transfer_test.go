package syncer

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"codeberg.org/kyleraykbs/storeboard/internal/store"
)

// pathGateSender holds bulk frames for one path until released, and records
// every frame that gets through.
type pathGateSender struct {
	mu     sync.Mutex
	frames []frame
	gate   string
	open   chan struct{}
}

func (s *pathGateSender) record(b []byte) {
	var fr frame
	_ = json.Unmarshal(b, &fr)
	s.mu.Lock()
	s.frames = append(s.frames, fr)
	s.mu.Unlock()
}

func (s *pathGateSender) Send(b []byte) error { s.record(b); return nil }
func (s *pathGateSender) SendBulk(b []byte) error {
	var fr frame
	_ = json.Unmarshal(b, &fr)
	if fr.Path == s.gate {
		<-s.open
	}
	s.record(b)
	return nil
}

func (s *pathGateSender) ended(path string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, fr := range s.frames {
		if fr.Type == "blobEnd" && fr.Path == path {
			return true
		}
	}
	return false
}

// A transfer that takes forever must not hold back the files queued behind
// it. Wants used to be served one at a time on the store's want worker, so a
// 20 GiB video stopped every other file in the store until it was done.
func TestLargeTransferDoesNotBlockSmallOnes(t *testing.T) {
	n := mkNode(t, "a", "b", "127.0.0.1:1", "127.0.0.1:2")
	n.prepareAll()
	defer n.eng.Close()
	fs := n.store("docs")

	big := filepath.Join(fs.root, "big.bin")
	if err := os.WriteFile(big, make([]byte, largeTransfer+1), 0o644); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(fs.root, "small.txt"), "small", time.Now())
	if _, err := n.eng.scan(fs, true); err != nil {
		t.Fatal(err)
	}

	gate := &pathGateSender{gate: "big.bin", open: make(chan struct{})}
	n.eng.mu.Lock()
	n.eng.sends["b"] = gate
	n.eng.mu.Unlock()

	n.eng.handleWant("b", &frame{Type: "want", Store: "docs", Path: "big.bin", Blob: fs.db.Get("big.bin").Blob})
	n.eng.handleWant("b", &frame{Type: "want", Store: "docs", Path: "small.txt", Blob: fs.db.Get("small.txt").Blob})
	waitFor(t, 5*time.Second, "the small file to finish while the large one is stuck", func() bool { return gate.ended("small.txt") })
	if gate.ended("big.bin") {
		t.Fatal("the gated transfer finished early")
	}
	close(gate.open)
	waitFor(t, 10*time.Second, "the large file to finish once released", func() bool { return gate.ended("big.bin") })
}

// A want names a version, and the holder serves exactly those bytes from
// wherever it keeps them - here a conflict loser whose path has since moved
// on to another version. Asking by path sent the newer bytes instead, and the
// loser was never fetched.
func TestWantIsServedByContentNotPath(t *testing.T) {
	n := mkNode(t, "a", "b", "127.0.0.1:1", "127.0.0.1:2")
	n.prepareAll()
	defer n.eng.Close()
	fs := n.store("docs")

	writeFile(t, filepath.Join(fs.root, "photo.jpg"), "first version", time.Now().Add(-time.Hour))
	if _, err := n.eng.scan(fs, true); err != nil {
		t.Fatal(err)
	}
	old := fs.db.Get("photo.jpg").Blob
	// The first version moves aside under another name, and the path gets
	// new content.
	if err := os.Rename(filepath.Join(fs.root, "photo.jpg"), filepath.Join(fs.root, "kept.jpg")); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(fs.root, "photo.jpg"), "second version", time.Now())
	if _, err := n.eng.scan(fs, true); err != nil {
		t.Fatal(err)
	}
	if fs.db.Get("photo.jpg").Blob == old {
		t.Fatal("scan did not record the new version")
	}

	rec := &pathGateSender{}
	n.eng.mu.Lock()
	n.eng.sends["b"] = rec
	n.eng.mu.Unlock()
	n.eng.handleWant("b", &frame{Type: "want", Store: "docs", Path: "photo.jpg", Blob: old})
	n.eng.sendWG.Wait()

	var got strings.Builder
	var end *frame
	for _, fr := range rec.frames {
		switch fr.Type {
		case "blob":
			if fr.Blob != old {
				t.Fatalf("chunk named blob %s, want %s", fr.Blob, old)
			}
			got.Write(fr.Data)
		case "blobEnd":
			end = &fr
		}
	}
	if got.String() != "first version" {
		t.Fatalf("served %q, want the version that was asked for", got.String())
	}
	if end == nil || end.Blob != old || end.Entry.Size != int64(len("first version")) {
		t.Fatalf("blobEnd = %+v", end)
	}
}

// A holder that has no copy says so, and with nobody else to ask the want is
// dropped rather than left to wait out its refresh.
func TestRefusedWantIsDropped(t *testing.T) {
	n := mkNode(t, "a", "b", "127.0.0.1:1", "127.0.0.1:2")
	n.prepareAll()
	defer n.eng.Close()

	sum, _ := store.Sum(strings.NewReader("nobody has this"))
	ent := &Entry{Path: "gone.bin", Blob: sum, Size: 15, VV: map[string]uint64{"b": 1}}
	rec := &pathGateSender{}
	n.eng.mu.Lock()
	n.eng.sends["b"] = rec
	n.eng.mu.Unlock()
	n.eng.request("b", n.store("docs"), ent, &pending{entry: ent})
	if !n.eng.isWanted("docs", "gone.bin") {
		t.Fatal("request recorded no want")
	}

	// The holder's side of it: a want for bytes it does not have.
	holder := mkNode(t, "b", "a", "127.0.0.1:3", "127.0.0.1:4")
	holder.prepareAll()
	defer holder.eng.Close()
	back := &pathGateSender{}
	holder.eng.mu.Lock()
	holder.eng.sends["a"] = back
	holder.eng.mu.Unlock()
	holder.eng.handleWant("a", &frame{Type: "want", Store: "docs", Path: "gone.bin", Blob: sum})
	holder.eng.sendWG.Wait()
	var refusal *frame
	for _, fr := range back.frames {
		if fr.Type == "wantErr" {
			refusal = &fr
		}
	}
	if refusal == nil || refusal.Code != wantMissing || refusal.Blob != sum {
		t.Fatalf("holder answered %+v, want a wantErr", back.frames)
	}

	n.eng.handleWantErr("b", refusal)
	if n.eng.isWanted("docs", "gone.bin") {
		t.Fatal("a refused want with no other holder was kept")
	}
}

// Bytes are verified as they arrive. A transfer whose stream does not hash to
// the version asked for is refused without being applied.
func TestCorruptTransferIsRefused(t *testing.T) {
	n := mkNode(t, "a", "b", "127.0.0.1:1", "127.0.0.1:2")
	n.prepareAll()
	defer n.eng.Close()
	fs := n.store("docs")

	sum, _ := store.Sum(strings.NewReader("the real bytes"))
	ent := &Entry{Path: "x.bin", Blob: sum, Size: 14, MTime: time.Now().UnixNano(), VV: map[string]uint64{"b": 1}}
	n.eng.addWant("docs", &pending{entry: ent})
	n.eng.handleBlob("b", &frame{Type: "blob", Store: "docs", Path: "x.bin", Blob: sum, Seq: 0, Data: []byte("forged bytes!!")})
	n.eng.handleBlobEnd("b", &frame{Type: "blobEnd", Store: "docs", Path: "x.bin", Blob: sum, Entry: ent})
	n.eng.finishWG.Wait()
	if exists(filepath.Join(fs.root, "x.bin")) {
		t.Fatal("bytes that failed verification were applied")
	}
	if n.eng.store.Has(sum) {
		t.Fatal("bytes that failed verification were stored")
	}
}

// An edit concurrent with a local deletion wins, and is fetched with a vector
// that covers the deletion: fetched with the peer's own vector, it was refused
// on arrival as older than the tombstone, re-requested on the next exchange,
// and refused again, forever.
func TestEditConcurrentWithDeleteIsFetchedToWin(t *testing.T) {
	n := mkNode(t, "a", "b", "127.0.0.1:1", "127.0.0.1:2")
	n.prepareAll()
	defer n.eng.Close()
	fs := n.store("docs")
	if err := fs.db.Put(&Entry{Path: "x.txt", Deleted: true, VV: map[string]uint64{"a": 2}}); err != nil {
		t.Fatal(err)
	}
	sum, _ := store.Sum(strings.NewReader("edited"))
	rem := &Entry{Path: "x.txt", Blob: sum, Size: 6, MTime: time.Now().UnixNano(), Mode: 0o644, VV: map[string]uint64{"a": 1, "b": 1}}
	rec := &pathGateSender{}
	n.eng.mu.Lock()
	n.eng.sends["b"] = rec
	n.eng.mu.Unlock()
	n.eng.handleIndex("b", &frame{Type: "index", Store: "docs", Entries: []*Entry{rem}})

	n.eng.mu.Lock()
	p := n.eng.wantLocked("docs", "x.txt", sum)
	n.eng.mu.Unlock()
	if p == nil {
		t.Fatal("the edit was not fetched")
	}
	if !Dominates(p.entry.VV, fs.db.Get("x.txt").VV) {
		t.Fatalf("fetched with %v, which cannot supersede the tombstone %v", p.entry.VV, fs.db.Get("x.txt").VV)
	}

	// And it lands.
	n.eng.handleBlob("b", &frame{Type: "blob", Store: "docs", Path: "x.txt", Blob: sum, Seq: 0, Data: []byte("edited")})
	n.eng.handleBlobEnd("b", &frame{Type: "blobEnd", Store: "docs", Path: "x.txt", Blob: sum, Entry: rem})
	n.eng.finishWG.Wait()
	if got := readFile(t, filepath.Join(fs.root, "x.txt")); got != "edited" {
		t.Fatalf("x.txt = %q", got)
	}
}
