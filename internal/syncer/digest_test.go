package syncer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// pipeSender delivers frames straight into another engine, as a link that
// advertised the digest feature would, and counts the index entries sent.
type pipeSender struct {
	to      *Engine
	from    string
	mu      sync.Mutex
	entries int
	frames  map[string]int
}

func (p *pipeSender) HasFeature(name string) bool { return name == featureDigest }

func (p *pipeSender) deliver(b []byte) error {
	b = bytes.TrimSpace(b)
	var fr frame
	if err := json.Unmarshal(b, &fr); err == nil {
		p.mu.Lock()
		if p.frames == nil {
			p.frames = map[string]int{}
		}
		p.frames[fr.Type]++
		if fr.Type == "index" {
			p.entries += len(fr.Entries)
		}
		p.mu.Unlock()
	}
	p.to.OnPeerFrame(p.from, b)
	return nil
}

func (p *pipeSender) Send(b []byte) error     { return p.deliver(b) }
func (p *pipeSender) SendBulk(b []byte) error { return p.deliver(b) }

func (p *pipeSender) count() (int, map[string]int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := map[string]int{}
	for k, v := range p.frames {
		out[k] = v
	}
	return p.entries, out
}

func (p *pipeSender) reset() {
	p.mu.Lock()
	p.entries, p.frames = 0, nil
	p.mu.Unlock()
}

func pipePair(t *testing.T) (a, b *node, ab, ba *pipeSender) {
	a = mkNode(t, "a", "b", "127.0.0.1:1", "127.0.0.1:2")
	b = mkNode(t, "b", "a", "127.0.0.1:2", "127.0.0.1:1")
	a.prepareAll()
	b.prepareAll()
	t.Cleanup(func() { a.eng.Close(); b.eng.Close() })
	ab = &pipeSender{to: b.eng, from: "a"}
	ba = &pipeSender{to: a.eng, from: "b"}
	a.eng.mu.Lock()
	a.eng.sends["b"] = ab
	a.eng.mu.Unlock()
	b.eng.mu.Lock()
	b.eng.sends["a"] = ba
	b.eng.mu.Unlock()
	return a, b, ab, ba
}

// Peers that agree prove it with one frame each way at most; a difference
// costs the entries near it, not the whole store.
func TestDigestExchangesOnlyWhatDiffers(t *testing.T) {
	a, b, ab, ba := pipePair(t)
	fsA := a.store("docs")
	const files = 300
	for i := range files {
		writeFile(t, filepath.Join(fsA.root, fmt.Sprintf("dir%d/file%03d.txt", i%7, i)), fmt.Sprintf("content %d", i), time.Now())
	}
	if _, err := a.eng.scan(fsA, true); err != nil {
		t.Fatal(err)
	}

	// First contact: b knows nothing, so every bucket differs and b pulls
	// the lot through the ordinary index rules.
	if err := a.eng.sendDigestTo("b", fsA); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, "b to materialise every file", func() bool {
		return b.store("docs").db.LiveCount() == files && b.eng.wantCount() == 0
	})
	for i := range files {
		p := filepath.Join(b.root("docs"), fmt.Sprintf("dir%d/file%03d.txt", i%7, i))
		if got := readFile(t, p); got != fmt.Sprintf("content %d", i) {
			t.Fatalf("%s = %q", p, got)
		}
	}

	// In agreement: a digest and nothing else.
	time.Sleep(100 * time.Millisecond)
	ab.reset()
	ba.reset()
	if err := a.eng.sendDigestTo("b", fsA); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	if n, frames := ba.count(); n != 0 || frames["pull"] != 0 {
		t.Fatalf("agreeing peers exchanged %d entries (%v)", n, frames)
	}

	// One new file: only its bucket travels.
	writeFile(t, filepath.Join(fsA.root, "late.txt"), "late", time.Now())
	if _, err := a.eng.scan(fsA, true); err != nil {
		t.Fatal(err)
	}
	ab.reset()
	ba.reset()
	if err := a.eng.sendDigestTo("b", fsA); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, "b to fetch the new file", func() bool {
		return b.store("docs").db.Get("late.txt") != nil && b.eng.wantCount() == 0
	})
	sent, _ := ab.count()
	back, _ := ba.count()
	if sent+back > files/4 {
		t.Fatalf("one new file cost %d entries out of %d", sent+back, files+1)
	}
}

// A replica that knows a file but never got its bytes is not in agreement,
// even when the journals match: the exchange retries the fetch, as the
// periodic full index used to.
func TestDigestRetriesAnUnmaterialisedFile(t *testing.T) {
	a, b, _, _ := pipePair(t)
	fsA := a.store("docs")
	writeFile(t, filepath.Join(fsA.root, "photo.jpg"), "bytes", time.Now())
	if _, err := a.eng.scan(fsA, true); err != nil {
		t.Fatal(err)
	}
	// b has the entry but not the file - a want that was lost.
	ent := fsA.db.Get("photo.jpg")
	ent.Local = false
	if err := b.store("docs").db.Put(ent); err != nil {
		t.Fatal(err)
	}
	if err := a.eng.sendDigestTo("b", fsA); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, "b to fetch the missing bytes", func() bool {
		return exists(filepath.Join(b.root("docs"), "photo.jpg"))
	})
}

func TestDigestBits(t *testing.T) {
	for _, tc := range []struct{ n, want int }{{0, 4}, {100, 4}, {1000, 6}, {100000, 12}, {10000000, 12}} {
		if got := digestBits(tc.n); got != tc.want {
			t.Errorf("digestBits(%d) = %d, want %d", tc.n, got, tc.want)
		}
	}
}
