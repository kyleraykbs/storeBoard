package mobile

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"codeberg.org/kyleraykbs/storeboard/pkg/api"
)

// The embedded engine end to end: start it from JSON, send a file (the share
// sheet's path), see it in the status and the history, and restore a version
// — all through the same calls the Android app makes.
func TestEmbeddedDaemon(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "root")
	documents := filepath.Join(root, "documents")
	cfg := `{
  "configVersion": 2,
  "root": "` + root + `",
  "peer": "phone",
  "dataDir": "` + filepath.Join(dir, "data") + `",
  "socket": "` + filepath.Join(dir, "unused.sock") + `",
  "groups": { "kyle": { "peers": ["phone"] } },
  "stores": { "documents": { "group": "kyle" } },
  "backup": { "enable": true, "keep": "1y" }
}`

	d, err := Start(cfg, filepath.Join(dir, "storeboard.log"))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer d.Stop()

	// A file to share, standing in for a camera-roll photo.
	src := filepath.Join(dir, "IMG_0001.jpg")
	if err := os.WriteFile(src, []byte("pretend this is a photo"), 0o644); err != nil {
		t.Fatal(err)
	}

	raw, err := d.Send("documents", src, "Camera/IMG_0001.jpg")
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	var sent api.SendResult
	if err := json.Unmarshal([]byte(raw), &sent); err != nil {
		t.Fatal(err)
	}
	if sent.Path != "Camera/IMG_0001.jpg" || sent.Parked != "" {
		t.Fatalf("send result = %+v", sent)
	}
	if got, err := os.ReadFile(filepath.Join(documents, "Camera", "IMG_0001.jpg")); err != nil || string(got) != "pretend this is a photo" {
		t.Fatalf("sent file = %q, %v", got, err)
	}

	// Status: the folder is there and the file is indexed.
	raw, err = d.Status()
	if err != nil {
		t.Fatal(err)
	}
	var st api.Status
	if err := json.Unmarshal([]byte(raw), &st); err != nil {
		t.Fatal(err)
	}
	if st.Peer != "phone" || len(st.Stores) != 1 || st.Stores[0].Name != "documents" {
		t.Fatalf("status = %+v", st)
	}
	if len(st.Sync) != 1 || st.Sync[0].Entries == 0 {
		t.Fatalf("sync section = %+v, want the sent file indexed", st.Sync)
	}

	// History: one version, restore a copy of it elsewhere.
	raw, err = d.Versions("documents", "Camera/IMG_0001.jpg")
	if err != nil {
		t.Fatalf("versions: %v", err)
	}
	var vs api.Versions
	if err := json.Unmarshal([]byte(raw), &vs); err != nil {
		t.Fatal(err)
	}
	if len(vs.Versions) != 1 || vs.Versions[0].Blob == "" {
		t.Fatalf("versions = %+v, want one recorded version", vs.Versions)
	}

	out := filepath.Join(dir, "restored.jpg")
	raw, err = d.Restore("documents", "Camera/IMG_0001.jpg", "", "", out)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	var res api.RestoreResult
	if err := json.Unmarshal([]byte(raw), &res); err != nil {
		t.Fatal(err)
	}
	if !res.Copied {
		t.Fatalf("restore result = %+v, want a copy", res)
	}
	if got, err := os.ReadFile(out); err != nil || string(got) != "pretend this is a photo" {
		t.Fatalf("restored file = %q, %v", got, err)
	}
}

// Sending onto a taken name keeps both: the existing file is parked, and the
// sender is told where.
func TestEmbeddedSendNeverOverwrites(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "root")
	documents := filepath.Join(root, "documents")
	cfg := `{
  "configVersion": 2,
  "root": "` + root + `",
  "peer": "phone",
  "dataDir": "` + filepath.Join(dir, "data") + `",
  "socket": "` + filepath.Join(dir, "unused.sock") + `",
  "groups": {"kyle": {"peers": ["phone"]}},
  "stores": {"documents": {"group": "kyle"}}
}`
	d, err := Start(cfg, "")
	if err != nil {
		t.Fatal(err)
	}
	defer d.Stop()

	first := filepath.Join(dir, "a.txt")
	if err := os.WriteFile(first, []byte("first"), 0o644); err != nil {
		t.Fatal(err)
	}
	second := filepath.Join(dir, "b.txt")
	if err := os.WriteFile(second, []byte("second"), 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := d.Send("documents", first, "same.txt"); err != nil {
		t.Fatal(err)
	}
	raw, err := d.Send("documents", second, "same.txt")
	if err != nil {
		t.Fatal(err)
	}
	var sent api.SendResult
	if err := json.Unmarshal([]byte(raw), &sent); err != nil {
		t.Fatal(err)
	}
	if sent.Parked == "" {
		t.Fatalf("send result = %+v, want the existing file parked", sent)
	}
	if got, err := os.ReadFile(filepath.Join(documents, "same.txt")); err != nil || string(got) != "second" {
		t.Fatalf("destination = %q, %v; want the new content", got, err)
	}
	if got, err := os.ReadFile(filepath.Join(documents, sent.Parked)); err != nil || string(got) != "first" {
		t.Fatalf("parked copy = %q, %v; want the old content kept", got, err)
	}
}

// One engine per data directory: a second Start (a service restarted while
// the old engine still drains) is refused, Stop releases the directory, and
// a second Stop is harmless.
func TestEmbeddedEngineOwnsItsDataDir(t *testing.T) {
	dir := t.TempDir()
	cfg := `{
  "configVersion": 2,
  "root": "` + filepath.Join(dir, "root") + `",
  "peer": "phone",
  "dataDir": "` + filepath.Join(dir, "data") + `",
  "socket": "` + filepath.Join(dir, "unused.sock") + `",
  "groups": {"kyle": {"peers": ["phone"]}},
  "stores": {"documents": {"group": "kyle"}}
}`
	d, err := Start(cfg, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Start(cfg, ""); err == nil || !strings.Contains(err.Error(), "already running") {
		t.Fatalf("second Start = %v, want a refusal", err)
	}

	// Send copies: the share sheet's cache file is still there for the app
	// to delete.
	src := filepath.Join(dir, "shared.txt")
	if err := os.WriteFile(src, []byte("shared"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Send("documents", src, ""); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(src); err != nil || string(got) != "shared" {
		t.Fatalf("source after send = %q, %v", got, err)
	}

	if err := d.Stop(); err != nil {
		t.Fatal(err)
	}
	if err := d.Stop(); err != nil {
		t.Fatalf("second Stop: %v", err)
	}
	again, err := Start(cfg, "")
	if err != nil {
		t.Fatalf("Start after Stop: %v", err)
	}
	again.Stop()
}

// The document provider's calls: a store browses as folders, a file stats,
// opens by its own path when its bytes are here, and reads by range; and an
// unknown store or path, or an escaping one, is an error rather than an
// empty folder.
func TestEmbeddedBrowse(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "root")
	cfg := `{
  "configVersion": 2,
  "root": "` + root + `",
  "peer": "phone",
  "dataDir": "` + filepath.Join(dir, "data") + `",
  "socket": "` + filepath.Join(dir, "unused.sock") + `",
  "groups": {"kyle": {"peers": ["phone"]}},
  "stores": {"documents": {"group": "kyle"}}
}`
	d, err := Start(cfg, "")
	if err != nil {
		t.Fatal(err)
	}
	defer d.Stop()

	// An empty store is an empty folder, not null: the provider parses it as
	// an array.
	raw, err := d.Browse("documents", "")
	if err != nil {
		t.Fatal(err)
	}
	if raw != "[]" {
		t.Fatalf("empty store = %s, want []", raw)
	}

	const body = "0123456789abcdefghij"
	src := filepath.Join(dir, "clip.bin")
	if err := os.WriteFile(src, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Send("documents", src, "Pictures/clip.bin"); err != nil {
		t.Fatal(err)
	}

	raw, err = d.Browse("documents", "")
	if err != nil {
		t.Fatal(err)
	}
	var top []api.BrowseEntry
	if err := json.Unmarshal([]byte(raw), &top); err != nil {
		t.Fatal(err)
	}
	if len(top) != 1 || top[0].Name != "Pictures" || !top[0].Dir {
		t.Fatalf("root = %+v, want one Pictures directory", top)
	}
	raw, err = d.Browse("documents", "Pictures")
	if err != nil {
		t.Fatal(err)
	}
	var kids []api.BrowseEntry
	if err := json.Unmarshal([]byte(raw), &kids); err != nil {
		t.Fatal(err)
	}
	if len(kids) != 1 || kids[0].Path != "Pictures/clip.bin" || kids[0].Size != int64(len(body)) || !kids[0].Local {
		t.Fatalf("Pictures = %+v", kids)
	}

	raw, err = d.Stat("documents", "Pictures/clip.bin")
	if err != nil {
		t.Fatal(err)
	}
	var st api.BrowseEntry
	if err := json.Unmarshal([]byte(raw), &st); err != nil {
		t.Fatal(err)
	}
	if st.Name != "clip.bin" || st.Dir || st.Size != int64(len(body)) || st.MTime.IsZero() {
		t.Fatalf("stat = %+v", st)
	}
	raw, err = d.Stat("documents", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(raw), &st); err != nil || !st.Dir {
		t.Fatalf("stat of the store root = %s, %v; want a directory", raw, err)
	}

	p, err := d.LocalPath("documents", "Pictures/clip.bin")
	if err != nil || p != filepath.Join(root, "documents", "Pictures", "clip.bin") {
		t.Fatalf("LocalPath = %q, %v", p, err)
	}

	got, err := d.ReadAt("documents", "Pictures/clip.bin", 4, 6)
	if err != nil || string(got) != body[4:10] {
		t.Fatalf("ReadAt = %q, %v; want %q", got, err, body[4:10])
	}
	got, err = d.ReadAt("documents", "Pictures/clip.bin", 15, 100)
	if err != nil || string(got) != body[15:] {
		t.Fatalf("ReadAt across the end = %q, %v; want %q", got, err, body[15:])
	}
	got, err = d.ReadAt("documents", "Pictures/clip.bin", int64(len(body)), 10)
	if err != nil || len(got) != 0 {
		t.Fatalf("ReadAt at the end = %q, %v; want nothing", got, err)
	}

	for _, c := range []struct{ name, store, path string }{
		{"unknown store", "nope", ""},
		{"unknown path", "documents", "Pictures/missing.bin"},
		{"escaping path", "documents", "../data"},
	} {
		if _, err := d.Stat(c.store, c.path); err == nil {
			t.Errorf("Stat %s: no error", c.name)
		}
		if _, err := d.Browse(c.store, c.path); err == nil {
			t.Errorf("Browse %s: no error", c.name)
		}
	}
	if _, err := d.Browse("documents", "Pictures/clip.bin"); err == nil {
		t.Error("Browse of a file: no error")
	}
	if _, err := d.LocalPath("documents", ""); err == nil {
		t.Error("LocalPath of the store root: no error")
	}
	if _, err := d.ReadAt("documents", "Pictures/missing.bin", 0, 1); err == nil {
		t.Error("ReadAt of a missing file: no error")
	}
}

// A document provider outlives the engine it calls: the service can stop
// while the Files app still holds a folder open. Every browse call after
// Stop is a plain error, never a read of closed journals — and so is a call
// on no engine at all.
func TestEmbeddedBrowseAfterStop(t *testing.T) {
	dir := t.TempDir()
	cfg := `{
  "configVersion": 2,
  "root": "` + filepath.Join(dir, "root") + `",
  "peer": "phone",
  "dataDir": "` + filepath.Join(dir, "data") + `",
  "socket": "` + filepath.Join(dir, "unused.sock") + `",
  "groups": {"kyle": {"peers": ["phone"]}},
  "stores": {"documents": {"group": "kyle"}}
}`
	d, err := Start(cfg, "")
	if err != nil {
		t.Fatal(err)
	}
	src := filepath.Join(dir, "a.txt")
	if err := os.WriteFile(src, []byte("a"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Send("documents", src, "a.txt"); err != nil {
		t.Fatal(err)
	}

	// Calls racing Stop either finish or are refused; none may crash.
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 200 {
				_, _ = d.Browse("documents", "")
				_, _ = d.ReadAt("documents", "a.txt", 0, 1)
			}
		}()
	}
	if err := d.Stop(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()

	for _, d := range []*Daemon{d, nil} {
		if _, err := d.Browse("documents", ""); !errors.Is(err, errStopped) {
			t.Errorf("Browse = %v, want errStopped", err)
		}
		if _, err := d.Stat("documents", "a.txt"); !errors.Is(err, errStopped) {
			t.Errorf("Stat = %v, want errStopped", err)
		}
		if _, err := d.LocalPath("documents", "a.txt"); !errors.Is(err, errStopped) {
			t.Errorf("LocalPath = %v, want errStopped", err)
		}
		if _, err := d.ReadAt("documents", "a.txt", 0, 1); !errors.Is(err, errStopped) {
			t.Errorf("ReadAt = %v, want errStopped", err)
		}
	}
}

// A phone has no FUSE: a streamed store is served from its journal, and a
// file sent into it (a camera backup) goes through the mutator the mount
// would use, journalled and queued for a holder.
func TestEmbeddedSendIntoStreamedStore(t *testing.T) {
	dir := t.TempDir()
	cfg := `{
  "configVersion": 2,
  "root": "` + filepath.Join(dir, "root") + `",
  "peer": "phone",
  "dataDir": "` + filepath.Join(dir, "data") + `",
  "socket": "` + filepath.Join(dir, "unused.sock") + `",
  "peers": {"phone": {}, "amanita": {"endpoints": ["127.0.0.1:1"]}},
  "groups": {"kyle": {"peers": ["phone", "amanita"]}},
  "stores": {"Pictures": {"group": "kyle", "sync": "stream", "holders": ["amanita"]}}
}`
	d, err := Start(cfg, "")
	if err != nil {
		t.Fatal(err)
	}
	defer d.Stop()
	src := filepath.Join(dir, "IMG_0001.jpg")
	if err := os.WriteFile(src, []byte("a picture"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"Phone/IMG_0001.jpg", "Phone/IMG_0001.jpg"} {
		if _, err := d.Send("Pictures", src, name); err != nil {
			t.Fatal(err)
		}
	}
	// Through a mount (where FUSE exists) the kernel's release lands a moment
	// after the write returns; the journal-only path records at once.
	var raw string
	deadline := time.Now().Add(5 * time.Second)
	for {
		raw, err = d.Browse("Pictures", "Phone")
		if err != nil {
			t.Fatal(err)
		}
		var kids []api.BrowseEntry
		if err := json.Unmarshal([]byte(raw), &kids); err != nil {
			t.Fatal(err)
		}
		// The second send of the same name parks the first, never
		// overwrites.
		if len(kids) == 2 && kids[0].Size == 9 && kids[1].Size == 9 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Phone = %s", raw)
		}
		time.Sleep(50 * time.Millisecond)
	}
	got, err := d.ReadAt("Pictures", "Phone/IMG_0001.jpg", 0, 100)
	if err != nil || string(got) != "a picture" {
		t.Fatalf("ReadAt = %q, %v", got, err)
	}
}
