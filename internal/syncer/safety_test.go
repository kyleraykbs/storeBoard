package syncer

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"codeberg.org/kyleraykbs/storeboard/internal/store"
)

// capSender records what the engine sends.
type capSender struct {
	mu  sync.Mutex
	out [][]byte
}

func (c *capSender) Send(b []byte) error {
	c.mu.Lock()
	c.out = append(c.out, append([]byte(nil), b...))
	c.mu.Unlock()
	return nil
}
func (c *capSender) SendBulk(b []byte) error { return c.Send(b) }

// storeBytes puts content into a node's content store and returns its sum.
func storeBytes(t *testing.T, e *Engine, content string) string {
	t.Helper()
	p := mkBlob(t, e, content)
	sum, err := store.SumFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.store.Adopt(p, sum); err != nil {
		t.Fatal(err)
	}
	return sum
}

func skipIfRoot(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads every file")
	}
}

// A conflict whose loser cannot be parked must not let the winner in: the
// local edit would be overwritten with no copy anywhere.
func TestConflictParkFailureKeepsLocalEdit(t *testing.T) {
	skipIfRoot(t)
	n := mkNode(t, "b", "a", "127.0.0.1:1", "127.0.0.1:2")
	defer n.eng.Close()
	n.prepareAll()
	eng, fs := n.eng, n.store("docs")
	p := filepath.Join(fs.root, "a.txt")
	writeFile(t, p, "LOCAL EDIT", time.Now().Add(-time.Hour))
	if _, err := eng.scan(fs, true); err != nil {
		t.Fatal(err)
	}
	loc := fs.db.Get("a.txt")
	sum := strings.Repeat("ab", 32)
	rem := &Entry{Path: "a.txt", Blob: sum, Size: 6, MTime: time.Now().UnixNano(), Mode: 0o644, VV: map[string]uint64{"a": 1}}
	if winner(loc, rem) != rem {
		t.Fatal("test setup: remote should win")
	}
	if err := os.Chmod(p, 0); err != nil { // parking cannot hash it
		t.Fatal(err)
	}
	defer os.Chmod(p, 0o644)
	eng.resolveConflict("a", fs, loc, rem)
	eng.mu.Lock()
	want := eng.wantLocked("docs", "a.txt", wantSlot(sum, false))
	eng.mu.Unlock()
	if want != nil {
		t.Fatal("the winner is still wanted though the local version could not be parked")
	}
	os.Chmod(p, 0o644)
	if got := readFile(t, p); got != "LOCAL EDIT" {
		t.Fatalf("local edit = %q", got)
	}
}

// Parking is a rename: it must work with no room in the content store.
func TestParkNeedsNoContentStore(t *testing.T) {
	n := mkNode(t, "b", "a", "127.0.0.1:1", "127.0.0.1:2")
	defer n.eng.Close()
	n.prepareAll()
	eng, fs := n.eng, n.store("docs")
	writeFile(t, filepath.Join(fs.root, "a.txt"), "mine", time.Now())
	if _, err := eng.scan(fs, true); err != nil {
		t.Fatal(err)
	}
	// A content store that cannot take anything.
	storeDir := filepath.Join(eng.dataDir, "store")
	if err := os.Chmod(storeDir, 0o500); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(storeDir, 0o700)
	ent, err := eng.saveLocalConflict(fs, fs.db.Get("a.txt"))
	if err != nil || ent == nil {
		t.Fatalf("park: %v %v", ent, err)
	}
	if got := readFile(t, filepath.Join(fs.root, ent.Path)); got != "mine" {
		t.Fatalf("parked copy = %q", got)
	}
}

// The conflict loser's bytes must stay servable after the winner (with the
// merged vector) overwrote them: the winner's machine asks for them as its
// conflict copy, possibly later.
func TestConflictLoserServableAfterOverwrite(t *testing.T) {
	b := mkNode(t, "b", "a", "127.0.0.1:2", "127.0.0.1:1")
	defer b.eng.Close()
	b.prepareAll()
	fs := b.store("docs")
	b.eng.mu.Lock()
	b.eng.sends["a"] = &capSender{}
	b.eng.mu.Unlock()
	path := filepath.Join(fs.root, "f.txt")
	if err := os.WriteFile(path, []byte("B's concurrent edit"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := b.eng.scan(fs, true); err != nil {
		t.Fatal(err)
	}
	locB := fs.db.Get("f.txt")
	sumA := storeBytes(t, b.eng, "A's winning edit")
	upd := &Entry{Path: "f.txt", Blob: sumA, Size: 16, MTime: locB.MTime + 1e9, Mode: 0o644, VV: MergeVV(locB.VV, map[string]uint64{"a": 1})}
	b.eng.handleIndex("a", &frame{Type: "index", Store: "docs", Entries: []*Entry{upd}})
	b.eng.mu.Lock()
	p := b.eng.wantLocked("docs", "f.txt", sumA)
	b.eng.mu.Unlock()
	if p == nil {
		t.Fatal("no want for a's version")
	}
	b.eng.applyWants(fs, []*pending{p})
	if got := readFile(t, path); got != "A's winning edit" {
		t.Fatalf("winner not applied: %q", got)
	}
	f, _, err := b.eng.openBlobFor(fs, "f.txt", locB.Blob)
	if err != nil {
		t.Fatalf("b's concurrent edit is gone everywhere: %v", err)
	}
	got, _ := readAllClose(f)
	if got != "B's concurrent edit" {
		t.Fatalf("served %q", got)
	}
	// And the collector leaves it for a while.
	old := recentBlob
	recentBlob = 0
	defer func() { recentBlob = old }()
	b.eng.Collect()
	if !b.eng.store.Has(locB.Blob) {
		t.Fatal("the displaced version was collected at once")
	}
}

func readAllClose(f *os.File) (string, error) {
	defer f.Close()
	var sb strings.Builder
	buf := make([]byte, 4096)
	for {
		n, err := f.Read(buf)
		sb.Write(buf[:n])
		if err != nil {
			return sb.String(), nil
		}
	}
}

// A deleted file stays in the trash for the window, however old it is.
func TestTrashCountsFromDeletion(t *testing.T) {
	n := mkNode(t, "b", "a", "127.0.0.1:1", "127.0.0.1:2")
	defer n.eng.Close()
	n.prepareAll()
	eng, fs := n.eng, n.store("docs")
	old := time.Now().Add(-2 * 365 * 24 * time.Hour)
	writeFile(t, filepath.Join(fs.root, "old.jpg"), "photo from 2019", old)
	if _, err := eng.scan(fs, true); err != nil {
		t.Fatal(err)
	}
	if err := eng.applyDelete(fs, fs.db.Get("old.jpg")); err != nil {
		t.Fatal(err)
	}
	tr := filepath.Join(eng.dataDir, "trash", "docs", "old.jpg")
	eng.pruneOnce(time.Now())
	if !exists(tr) {
		t.Fatal("an old photo deleted just now was purged from the trash")
	}
	info, err := os.Stat(tr)
	if err != nil || !info.ModTime().Equal(time.Unix(0, old.UnixNano())) {
		t.Fatalf("trashed file lost its mtime: %v %v", info.ModTime(), err)
	}
	eng.pruneOnce(time.Now().Add(DefaultKeep + 24*time.Hour))
	if exists(tr) {
		t.Fatal("trash past the window survived")
	}
}

// A root that comes back holding one new file is still not the library.
func TestMassDeleteGuardSurvivesOneNewFile(t *testing.T) {
	n := mkNode(t, "b", "a", "127.0.0.1:1", "127.0.0.1:2")
	defer n.eng.Close()
	n.prepareAll()
	eng, fs := n.eng, n.store("docs")
	for i := 0; i < 60; i++ {
		writeFile(t, filepath.Join(fs.root, "lib", fmt.Sprintf("%03d.jpg", i)), fmt.Sprintf("x%d", i), time.Now().Add(-time.Hour))
	}
	if _, err := eng.scan(fs, true); err != nil {
		t.Fatal(err)
	}
	// The disk goes away (its directory is not the one at the path any
	// more); the recreated root receives one new file from a peer.
	if err := os.Rename(fs.root, fs.root+".gone"); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(fs.root, "new-from-peer.txt"), "hi", time.Now())
	changed, err := eng.scan(fs, true)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range changed {
		if c.Deleted {
			t.Fatalf("%s tombstoned by a root that is not the library", c.Path)
		}
	}
	if !fs.rootEmpty.Load() {
		t.Fatal("root not flagged unavailable")
	}
	// The disk is back: scans resume normally.
	os.RemoveAll(fs.root)
	if err := os.Rename(fs.root+".gone", fs.root); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.scan(fs, true); err != nil {
		t.Fatal(err)
	}
	if fs.rootEmpty.Load() {
		t.Fatal("root still flagged after it came back")
	}
	// A real mass deletion in the same directory goes through.
	if err := os.RemoveAll(filepath.Join(fs.root, "lib")); err != nil {
		t.Fatal(err)
	}
	changed, err = eng.scan(fs, true)
	if err != nil {
		t.Fatal(err)
	}
	tombs := 0
	for _, c := range changed {
		if c.Deleted && !c.Dir {
			tombs++
		}
	}
	if tombs != 60 {
		t.Fatalf("real deletion in the same root: %d tombstones, want 60", tombs)
	}
}

// The newest version of a file is kept however old it is.
func TestKeepMaskKeepsOldCurrentVersion(t *testing.T) {
	now := time.Now()
	vs := []Version{
		{Time: now.Add(-500 * 24 * time.Hour).UnixNano(), Blob: "a"},
		{Time: now.Add(-400 * 24 * time.Hour).UnixNano(), Blob: "b"},
	}
	m := keepMask(vs, now, DefaultKeep)
	if !m[1] {
		t.Fatal("current version dropped")
	}
	if m[0] {
		t.Fatal("superseded version past the window kept")
	}
	// A deletion record is not a current file: it expires.
	del := []Version{{Time: now.Add(-400 * 24 * time.Hour).UnixNano(), Blob: "a", Deleted: true}}
	if keepMask(del, now, DefaultKeep)[0] {
		t.Fatal("deletion record past the window kept")
	}
}

func TestBackupKeepsUnchangedFile(t *testing.T) {
	old := recentBlob
	recentBlob = 0
	defer func() { recentBlob = old }()
	n := mkBackupNode(t, "", "")
	defer n.eng.Close()
	eng, fs := n.eng, n.store("docs")
	writeFile(t, filepath.Join(fs.root, "keep.txt"), "precious", time.Now())
	if _, err := eng.scan(fs, true); err != nil {
		t.Fatal(err)
	}
	ent := fs.db.Get("keep.txt")
	fs.hist.Prune(time.Now().Add(400 * 24 * time.Hour))
	eng.Collect()
	if !eng.store.Has(ent.Blob) || len(fs.hist.Versions("keep.txt")) != 1 {
		t.Fatal("an unchanged file lost its only version")
	}
}

// A file written while it is hashed is left for the next pass.
func TestScanSkipsFileChangingWhileHashed(t *testing.T) {
	n := mkNode(t, "b", "a", "127.0.0.1:1", "127.0.0.1:2")
	defer n.eng.Close()
	n.prepareAll()
	eng, fs := n.eng, n.store("docs")
	p := filepath.Join(fs.root, "growing.bin")
	writeFile(t, p, "part", time.Now().Add(-time.Minute))
	scanHashed = func(path string) {
		if path == p {
			f, _ := os.OpenFile(p, os.O_APPEND|os.O_WRONLY, 0)
			f.WriteString(" and more")
			f.Close()
		}
	}
	defer func() { scanHashed = nil }()
	if _, err := eng.scan(fs, true); err != nil {
		t.Fatal(err)
	}
	if ent := fs.db.Get("growing.bin"); ent != nil {
		t.Fatalf("recorded mid-write: %+v", ent)
	}
	scanHashed = nil
	if _, err := eng.scan(fs, true); err != nil {
		t.Fatal(err)
	}
	ent := fs.db.Get("growing.bin")
	want, _ := store.SumFile(p)
	if ent == nil || ent.Blob != want {
		t.Fatalf("not recorded once still: %+v", ent)
	}
}

// A failed append is cut back, so the records after it survive a restart.
func TestJournalShortWriteDoesNotEatNextRecord(t *testing.T) {
	p := filepath.Join(t.TempDir(), "j.ndjson")
	d, err := OpenFileDB(p, "me")
	if err != nil {
		t.Fatal(err)
	}
	d.Put(&Entry{Path: "a", Blob: "1", VV: map[string]uint64{"me": 1}})
	// What a write that ran out of space part-way leaves behind.
	half := []byte(`{"entry":{"path":"b","bl`)
	d.mu.Lock()
	n, _ := d.f.Write(half)
	d.cutTornLocked(n)
	d.mu.Unlock()
	d.Put(&Entry{Path: "c", Blob: "3", VV: map[string]uint64{"me": 3}})
	// And one whose cut failed: the next record starts on a fresh line.
	d.mu.Lock()
	n, _ = d.f.Write(half)
	d.torn, d.off = true, d.off+int64(n)
	d.mu.Unlock()
	d.Put(&Entry{Path: "d", Blob: "4", VV: map[string]uint64{"me": 4}})
	d.Close()
	d2, err := OpenFileDB(p, "me")
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close()
	for _, k := range []string{"a", "c", "d"} {
		if d2.Get(k) == nil {
			t.Fatalf("%s lost after a torn write", k)
		}
	}
	if d2.corrupt != 1 || !exists(p+".corrupt") {
		t.Fatalf("corrupt lines = %d, copy kept = %v", d2.corrupt, exists(p+".corrupt"))
	}
}

// A full data disk must not stop the journal from opening.
func TestJournalOpensWhenCheckpointFails(t *testing.T) {
	skipIfRoot(t)
	dir := t.TempDir()
	p := filepath.Join(dir, "j.ndjson")
	d, err := OpenFileDB(p, "me")
	if err != nil {
		t.Fatal(err)
	}
	d.Put(&Entry{Path: "a", Blob: "1", VV: map[string]uint64{"me": 1}})
	d.Close()
	// The checkpoint's scratch file cannot be created.
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(dir, 0o700)
	d2, err := OpenFileDB(p, "me")
	if err != nil {
		t.Fatalf("journal did not open: %v", err)
	}
	if d2.Get("a") == nil {
		t.Fatal("state lost")
	}
	if err := d2.Put(&Entry{Path: "b", Blob: "2", VV: map[string]uint64{"me": 2}}); err != nil {
		t.Fatal(err)
	}
	d2.Close()
	os.Chmod(dir, 0o700)
	d3, err := OpenFileDB(p, "me")
	if err != nil {
		t.Fatal(err)
	}
	defer d3.Close()
	if d3.Get("a") == nil || d3.Get("b") == nil {
		t.Fatal("appends to the uncompacted log were lost")
	}

	// The history opens the same way.
	hp := filepath.Join(dir, "h.ndjson")
	h, err := OpenHistory(hp, 0)
	if err != nil {
		t.Fatal(err)
	}
	h.Record("x", Version{Time: 1, Blob: "1"})
	h.Close()
	os.Chmod(dir, 0o500)
	h2, err := OpenHistory(hp, 0)
	if err != nil {
		t.Fatalf("history did not open: %v", err)
	}
	h2.Close()
}

// Tombstones of scratch files expire; others stay.
func TestScratchTombstonesExpire(t *testing.T) {
	p := filepath.Join(t.TempDir(), "j.ndjson")
	d, err := OpenFileDB(p, "me")
	if err != nil {
		t.Fatal(err)
	}
	d.Put(&Entry{Path: "dir/.goutputstream-ABC123", Deleted: true, VV: map[string]uint64{"me": 1}})
	d.Put(&Entry{Path: "photo.jpg", Deleted: true, VV: map[string]uint64{"me": 2}})
	d.Put(&Entry{Path: ".goutputstream-LIVE", Blob: "x", VV: map[string]uint64{"me": 3}})
	if ent := d.Get("photo.jpg"); ent.DeletedAt == 0 {
		t.Fatal("tombstone not stamped")
	}
	if n, _ := d.ExpireTombstones(time.Now(), scratchTombstoneAge, isScratchPath); n != 0 {
		t.Fatalf("fresh tombstones expired: %d", n)
	}
	n, err := d.ExpireTombstones(time.Now().Add(8*24*time.Hour), scratchTombstoneAge, isScratchPath)
	if err != nil || n != 1 {
		t.Fatalf("expired %d (%v), want 1", n, err)
	}
	d.Close()
	d2, err := OpenFileDB(p, "me")
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close()
	if d2.Get("dir/.goutputstream-ABC123") != nil {
		t.Fatal("expired tombstone came back")
	}
	if d2.Get("photo.jpg") == nil || d2.Get(".goutputstream-LIVE") == nil {
		t.Fatal("dropped an entry it must keep")
	}
	if d2.counter != 3 {
		t.Fatalf("counter = %d", d2.counter)
	}
}

// A forwarded verb gives up when its holder goes away.
func TestVerbRoundTripGivesUpOnDisconnect(t *testing.T) {
	n := mkNode(t, "b", "a", "127.0.0.1:1", "127.0.0.1:2")
	defer n.eng.Close()
	eng, fs := n.eng, n.store("docs")
	eng.mu.Lock()
	eng.sends["a"] = &capSender{}
	eng.mu.Unlock()
	done := make(chan error, 1)
	go func() {
		_, err := eng.verbRoundTrip("a", fs, verbsRequest{Action: "versions"})
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	eng.mu.Lock()
	delete(eng.sends, "a")
	eng.mu.Unlock()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("no error")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("verb round trip hung after the holder disconnected")
	}
}

// Scratch files a crash left in a store root are swept.
func TestScanSweepsStaleScratch(t *testing.T) {
	n := mkNode(t, "b", "a", "127.0.0.1:1", "127.0.0.1:2")
	defer n.eng.Close()
	n.prepareAll()
	eng, fs := n.eng, n.store("docs")
	stale := filepath.Join(fs.root, tmpPrefix+"123")
	writeFile(t, stale, "junk", time.Now().Add(-48*time.Hour))
	if _, err := eng.scan(fs, true); err != nil {
		t.Fatal(err)
	}
	if !exists(stale) {
		t.Fatal("a scratch file touched just now was swept")
	}
	old := scratchMaxAge
	scratchMaxAge = 0
	defer func() { scratchMaxAge = old }()
	time.Sleep(10 * time.Millisecond)
	if _, err := eng.scan(fs, true); err != nil {
		t.Fatal(err)
	}
	if exists(stale) {
		t.Fatal("stale scratch file left in the store root")
	}
}

// Restore must not write over an edit the scanner has not recorded.
func TestRestoreParksUnscannedEdit(t *testing.T) {
	n := mkBackupNode(t, "", "")
	defer n.eng.Close()
	eng, fs := n.eng, n.store("docs")
	p := filepath.Join(fs.root, "note.txt")
	writeFile(t, p, "v1", time.Now().Add(-2*time.Hour))
	if _, err := eng.scan(fs, true); err != nil {
		t.Fatal(err)
	}
	v1 := fs.db.Get("note.txt").Blob
	writeFile(t, p, "v2", time.Now().Add(-time.Hour))
	if _, err := eng.scan(fs, true); err != nil {
		t.Fatal(err)
	}
	writeFile(t, p, "unscanned v3", time.Now())
	if _, err := eng.Restore("docs", 0, "note.txt", v1, time.Time{}, ""); err != nil {
		t.Fatal(err)
	}
	if got := readFile(t, p); got != "v1" {
		t.Fatalf("restored %q", got)
	}
	sum3 := sumOfString("unscanned v3")
	if got := readFile(t, filepath.Join(fs.root, conflictPath("note.txt", sum3))); got != "unscanned v3" {
		t.Fatalf("unscanned edit not parked: %q", got)
	}
}

func sumOfString(s string) string {
	sum, _ := store.Sum(strings.NewReader(s))
	return sum
}

// An unreadable unsynced change refuses the update instead of being
// overwritten.
func TestApplyRefusesUnreadableChange(t *testing.T) {
	skipIfRoot(t)
	n := mkNode(t, "b", "a", "127.0.0.1:1", "127.0.0.1:2")
	defer n.eng.Close()
	n.prepareAll()
	eng, fs := n.eng, n.store("docs")
	p := filepath.Join(fs.root, "a.txt")
	writeFile(t, p, "v1", time.Now().Add(-time.Hour))
	if _, err := eng.scan(fs, true); err != nil {
		t.Fatal(err)
	}
	loc := fs.db.Get("a.txt")
	writeFile(t, p, "unsynced edit", time.Now())
	os.Chmod(p, 0)
	defer os.Chmod(p, 0o644)
	sum := storeBytes(t, eng, "remote")
	upd := &Entry{Path: "a.txt", Blob: sum, Size: 6, MTime: time.Now().UnixNano(), Mode: 0o644, VV: MergeVV(loc.VV, map[string]uint64{"a": 1})}
	if err := eng.applyEntry(fs, upd); err == nil {
		t.Fatal("applied over an unreadable unsynced change")
	}
	os.Chmod(p, 0o644)
	if got := readFile(t, p); got != "unsynced edit" {
		t.Fatalf("file = %q", got)
	}
}

// An edit made while an update is being copied in is parked, not renamed over.
func TestApplyParksEditDuringCopy(t *testing.T) {
	n := mkNode(t, "b", "a", "127.0.0.1:1", "127.0.0.1:2")
	defer n.eng.Close()
	n.prepareAll()
	eng, fs := n.eng, n.store("docs")
	p := filepath.Join(fs.root, "a.txt")
	writeFile(t, p, "v1", time.Now().Add(-time.Hour))
	if _, err := eng.scan(fs, true); err != nil {
		t.Fatal(err)
	}
	loc := fs.db.Get("a.txt")
	sum := storeBytes(t, eng, "remote v2")
	upd := &Entry{Path: "a.txt", Blob: sum, Size: 9, MTime: time.Now().UnixNano(), Mode: 0o644, VV: MergeVV(loc.VV, map[string]uint64{"a": 1})}
	beforePlace = func(dst string) {
		if dst == p {
			writeFile(t, p, "edit during copy", time.Now())
		}
	}
	defer func() { beforePlace = nil }()
	if err := eng.applyEntry(fs, upd); err != nil {
		t.Fatal(err)
	}
	if got := readFile(t, p); got != "remote v2" {
		t.Fatalf("file = %q", got)
	}
	if got := readFile(t, filepath.Join(fs.root, conflictPath("a.txt", sumOfString("edit during copy")))); got != "edit during copy" {
		t.Fatalf("edit not parked: %q", got)
	}
}

// Abandoned incoming transfers are reaped.
func TestReapIncoming(t *testing.T) {
	n := mkNode(t, "b", "a", "127.0.0.1:1", "127.0.0.1:2")
	defer n.eng.Close()
	eng := n.eng
	f, err := eng.store.Temp()
	if err != nil {
		t.Fatal(err)
	}
	f2, _ := eng.store.Temp()
	eng.mu.Lock()
	eng.incoming["old"] = &incomingBlob{store: "docs", path: "x", f: f, last: time.Now().Add(-wantRefresh - time.Minute)}
	eng.incoming["live"] = &incomingBlob{store: "docs", path: "y", f: f2, last: time.Now()}
	eng.mu.Unlock()
	if got := eng.reapIncoming(time.Now()); got != 1 {
		t.Fatalf("reaped %d", got)
	}
	if exists(f.Name()) || !exists(f2.Name()) {
		t.Fatal("wrong transfer reaped")
	}
	f2.Close()
}

// No room: nothing is asked for, and an unasked transfer is not accepted.
func TestNoFetchWithoutSpace(t *testing.T) {
	n := mkNode(t, "b", "a", "127.0.0.1:1", "127.0.0.1:2")
	defer n.eng.Close()
	n.prepareAll()
	eng, fs := n.eng, n.store("docs")
	cs := &capSender{}
	eng.mu.Lock()
	eng.sends["a"] = cs
	eng.mu.Unlock()
	old := diskReserve
	diskReserve = 1 << 62
	defer func() { diskReserve = old }()
	rem := &Entry{Path: "big.bin", Blob: strings.Repeat("ab", 32), Size: 10, MTime: 1, VV: map[string]uint64{"a": 1}}
	eng.handleIndex("a", &frame{Type: "index", Store: "docs", Entries: []*Entry{rem}})
	if eng.wantCount() != 0 {
		t.Fatal("wanted a file there is no room for")
	}
	cs.mu.Lock()
	for _, l := range cs.out {
		var fr frame
		_ = json.Unmarshal(l, &fr)
		if fr.Type == "want" {
			t.Fatal("sent a want with no room")
		}
	}
	cs.mu.Unlock()
	// Backed off: room again does not mean asking again at once.
	diskReserve = old
	eng.handleIndex("a", &frame{Type: "index", Store: "docs", Entries: []*Entry{rem}})
	if eng.wantCount() != 0 {
		t.Fatal("a backed-off fetch was asked for again at once")
	}
	if !eng.fetchBackedOff(fs, "big.bin") {
		t.Fatal("not backed off")
	}
}

func (c *capSender) take() [][]byte { c.mu.Lock(); o := c.out; c.out = nil; c.mu.Unlock(); return o }

// A file tombstone that dominates a directory entry is a deletion of the
// directory, not a file clashing with it: the exchange must settle.
func TestFileTombstoneVsDirConverges(t *testing.T) {
	a := mkNode(t, "a", "b", "127.0.0.1:1", "127.0.0.1:2")
	b := mkNode(t, "b", "a", "127.0.0.1:2", "127.0.0.1:1")
	a.prepareAll()
	b.prepareAll()
	if err := a.store("docs").db.Put(&Entry{Path: "p", Dir: true, VV: map[string]uint64{"a": 1}}); err != nil {
		t.Fatal(err)
	}
	if err := b.store("docs").db.Put(&Entry{Path: "p", Deleted: true, VV: map[string]uint64{"a": 1, "b": 4}}); err != nil {
		t.Fatal(err)
	}
	sa, sb := &capSender{}, &capSender{}
	a.eng.mu.Lock()
	a.eng.sends["b"] = sa
	a.eng.mu.Unlock()
	b.eng.mu.Lock()
	b.eng.sends["a"] = sb
	b.eng.mu.Unlock()

	// b offers its entry to a.
	line, _ := json.Marshal(frame{Type: "index", Store: "docs", Entries: []*Entry{b.store("docs").db.Get("p")}})
	var fr frame
	_ = json.Unmarshal(line, &fr)
	a.eng.handleIndex("b", &fr)
	rounds := 0
	for ; rounds < 50; rounds++ {
		fromA := sa.take()
		fromB := sb.take()
		if len(fromA) == 0 && len(fromB) == 0 {
			break
		}
		for _, l := range fromA {
			var f frame
			_ = json.Unmarshal(l, &f)
			b.eng.handleIndex("a", &f)
		}
		for _, l := range fromB {
			var f frame
			_ = json.Unmarshal(l, &f)
			a.eng.handleIndex("b", &f)
		}
	}
	t.Logf("a: %+v  b: %+v", a.store("docs").db.Get("p"), b.store("docs").db.Get("p"))
	if rounds == 50 {
		t.Fatalf("index exchange never settles: still ping-ponging after %d rounds", rounds)
	}
}
