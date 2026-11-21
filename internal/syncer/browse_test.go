package syncer

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"codeberg.org/kyleraykbs/storeboard/internal/config"
)

// A client with no mount browses a streamed store from its journal and reads
// a file's bytes from the holder, a range at a time.
func TestBrowseAndReadAStreamedStore(t *testing.T) {
	old := scanInterval
	scanInterval = 50 * time.Millisecond
	defer func() { scanInterval = old }()

	epA, epB := twoEndpoints(t)
	a := newNode(t, "a", "b", epA, epB, "", map[string]config.Store{"docs": {Group: "g"}})
	b := newNode(t, "b", "a", epB, epA, "", map[string]config.Store{"docs": {Group: "g", Sync: config.SyncStream}})
	startNode(t, a)
	startNode(t, b)

	const body = "0123456789abcdefghij"
	writeFile(t, filepath.Join(a.root("docs"), "Pictures", "clip.bin"), body, time.Now())
	waitFor(t, testWait, "journal entry on streamed peer", func() bool { return b.store("docs").db.Get("Pictures/clip.bin") != nil })

	top, err := b.eng.Browse("docs", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(top) != 1 || top[0].Name != "Pictures" || !top[0].Dir {
		t.Fatalf("root = %+v, want one Pictures directory", top)
	}
	kids, err := b.eng.Browse("docs", "Pictures")
	if err != nil {
		t.Fatal(err)
	}
	if len(kids) != 1 || kids[0].Path != "Pictures/clip.bin" || kids[0].Size != int64(len(body)) || kids[0].Local {
		t.Fatalf("Pictures = %+v", kids)
	}
	got, err := b.eng.ReadAt(context.Background(), "docs", "Pictures/clip.bin", 4, 100)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != body[4:] {
		t.Fatalf("read = %q, want %q", got, body[4:])
	}
	if b.eng.store.Has(kids[0].Blob) {
		t.Fatal("reading left a whole-file copy behind")
	}
	if _, err := b.eng.LocalPath("docs", "Pictures/clip.bin"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("LocalPath on a streamed store = %v, want ErrNotFound", err)
	}
	if _, err := b.eng.Browse("docs", "../etc"); err == nil {
		t.Fatal("browse accepted an escaping path")
	}

	// The holder's own replica browses too, and reads from disk.
	waitFor(t, testWait, "holder journal", func() bool { return a.store("docs").db.Get("Pictures/clip.bin") != nil })
	st, err := a.eng.Stat("docs", "Pictures/clip.bin")
	if err != nil || !st.Local {
		t.Fatalf("holder stat = %+v, %v", st, err)
	}
	p, err := a.eng.LocalPath("docs", "Pictures/clip.bin")
	if err != nil || p != filepath.Join(a.root("docs"), "Pictures", "clip.bin") {
		t.Fatalf("LocalPath = %q, %v", p, err)
	}
	got, err = a.eng.ReadAt(context.Background(), "docs", "Pictures/clip.bin", 0, 4)
	if err != nil || string(got) != "0123" {
		t.Fatalf("holder read = %q, %v", got, err)
	}
}
