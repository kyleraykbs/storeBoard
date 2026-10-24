package syncer

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"codeberg.org/kyleraykbs/storeboard/internal/config"
	"codeberg.org/kyleraykbs/storeboard/internal/store"
)

// The thinning schedule, as a table: hours inside a day, days inside a week,
// weeks inside a month, months inside a year, years beyond — and the newest
// version of every file, always.
func TestKeepMaskThinsBySchedule(t *testing.T) {
	now := time.Now()
	at := func(age time.Duration) Version { return Version{Time: now.Add(-age).UnixNano()} }
	h := time.Hour
	d := 24 * h

	versions := []Version{
		at(400 * d), // older than the default year: dropped
		at(370 * d), // dropped (past 1y)
		at(300 * d), // the newest in its year bucket: kept
		at(200 * d), // dropped: same year bucket as 300d, older
		at(60 * d),  // month bucket m2: kept
		at(40 * d),  // m1: kept
		at(20 * d),  // w2: kept
		at(10 * d),  // w1: kept
		at(6 * d),   // d6: kept
		at(3 * d),   // d3: kept
		at(30 * h),  // d1: kept
		at(5 * h),   // h5: kept
		at(5 * h),   // h5 again: same hour bucket, newer kept above
		at(1 * h),   // h1: kept
		at(0),       // the current version: always kept
	}
	mask := keepMask(versions, now, DefaultKeep)

	if !mask[len(versions)-1] {
		t.Fatal("the current version must always be kept")
	}
	if mask[0] || mask[1] {
		t.Fatal("versions past the deadline must be dropped")
	}
	if mask[11] && mask[12] {
		t.Fatal("two versions from the same hour bucket were both kept")
	}
	for _, i := range []int{2, 4, 5, 6, 7, 8, 9, 10, 13, 14} {
		if !mask[i] {
			t.Errorf("version %d (age %v) should be kept", i, time.Duration(now.UnixNano()-versions[i].Time))
		}
	}
}

// The deadline is the one Kyle asked for: nothing sticks around past it,
// whatever the schedule would say.
func TestKeepMaskHonoursTheDeadline(t *testing.T) {
	now := time.Now()
	keep := 90 * 24 * time.Hour
	versions := []Version{
		{Time: now.Add(-100 * 24 * time.Hour).UnixNano()}, // past 90d
		{Time: now.Add(-89 * 24 * time.Hour).UnixNano()},  // inside
		{Time: now.UnixNano()},
	}
	mask := keepMask(versions, now, keep)
	if mask[0] {
		t.Fatal("a version older than the keep window survived")
	}
	if !mask[1] || !mask[2] {
		t.Fatal("versions inside the window were dropped")
	}
}

// mkBackupNode builds a node whose engine was constructed with history on.
func mkBackupNode(t *testing.T, syncType, keep string) *node {
	t.Helper()
	n := newNode(t, "b", "a", "127.0.0.1:1", "127.0.0.1:2", "", map[string]config.Store{
		"docs": {Group: "g", Sync: syncType},
	})
	n.eng.Close()
	n.cfg.Backup = &config.Backup{Enable: true, Keep: keep}
	n.eng = newEngine(t, n.cfg, n.log)
	n.prepareAll()
	return n
}

// The whole point of a version log: a file's history survives its changes and
// its deletion, and a restore brings any of it back.
func TestHistoryRestoreRoundTrip(t *testing.T) {
	n := mkBackupNode(t, config.SyncLocal, "")
	eng, fs := n.eng, n.store("docs")

	// v1, then v2: two versions, and the first is recoverable.
	writeFile(t, filepath.Join(fs.root, "note.txt"), "first", time.Now().Add(-2*time.Hour))
	if _, err := eng.scan(fs, true); err != nil {
		t.Fatal(err)
	}
	firstBlob, err := store.SumFile(filepath.Join(fs.root, "note.txt"))
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(fs.root, "note.txt"), "second", time.Now())
	if _, err := eng.scan(fs, true); err != nil {
		t.Fatal(err)
	}

	versions := fs.hist.Versions("note.txt")
	if len(versions) != 2 {
		t.Fatalf("recorded %d versions, want 2: %+v", len(versions), versions)
	}

	if _, err := eng.Restore(fs.name, os.Getuid(), "note.txt", firstBlob, time.Time{}, ""); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if got := readFile(t, filepath.Join(fs.root, "note.txt")); got != "first" {
		t.Fatalf("content after restore = %q, want the first version", got)
	}
	if v := fs.hist.Versions("note.txt"); len(v) < 3 {
		t.Fatalf("restore did not add a version: %+v", v)
	}

	// Delete it, then restore: the deletion is recorded with the content it
	// removed, so a bare restore brings the file back.
	if err := os.Remove(filepath.Join(fs.root, "note.txt")); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.scan(fs, true); err != nil {
		t.Fatal(err)
	}
	if got := fs.db.Get("note.txt"); got == nil || !got.Deleted {
		t.Fatalf("entry not tombstoned: %+v", got)
	}
	if _, err := eng.Restore(fs.name, os.Getuid(), "note.txt", "", time.Time{}, ""); err != nil {
		t.Fatalf("restore after delete: %v", err)
	}
	if got := readFile(t, filepath.Join(fs.root, "note.txt")); got != "first" {
		t.Fatalf("content after delete+restore = %q", got)
	}
	if e := fs.db.Get("note.txt"); e == nil || e.Deleted {
		t.Fatalf("restored entry still tombstoned: %+v", e)
	}

	// --to writes beside the store and leaves it alone.
	out := filepath.Join(t.TempDir(), "copy.txt")
	res, err := eng.Restore(fs.name, os.Getuid(), "note.txt", firstBlob, time.Time{}, out)
	if err != nil || !res.Copied {
		t.Fatalf("copy restore = %+v, %v", res, err)
	}
	if got := readFile(t, out); got != "first" {
		t.Fatalf("copied content = %q", got)
	}
	eng.Close()
}

// A scan that re-observes an unchanged file must not grow the log: the
// history records transitions, not passes.
func TestHistoryRecordsTransitionsOnly(t *testing.T) {
	n := mkBackupNode(t, config.SyncLocal, "")
	eng, fs := n.eng, n.store("docs")

	writeFile(t, filepath.Join(fs.root, "note.txt"), "steady", time.Now())
	for range 4 {
		if _, err := eng.scan(fs, true); err != nil {
			t.Fatal(err)
		}
	}
	if v := fs.hist.Versions("note.txt"); len(v) != 1 {
		t.Fatalf("recorded %d versions for one unchanged file: %+v", len(v), v)
	}
	eng.Close()
}

// Retention drops old versions from the log, and the collector then frees the
// content only if nothing else references it — never a journal's, never an
// orphan journal's, never something a peer may be mid-transfer with.
func TestCollectFreesOnlyUnreferencedContent(t *testing.T) {
	old := recentBlob
	recentBlob = 0
	defer func() { recentBlob = old }()

	n := mkBackupNode(t, config.SyncLocal, "1h")
	eng, fs := n.eng, n.store("docs")

	// One version, aged past the window, with its content in the store.
	writeFile(t, filepath.Join(fs.root, "old.txt"), "ancient", time.Now().Add(-10*time.Hour))
	if _, err := eng.scan(fs, true); err != nil {
		t.Fatal(err)
	}
	oldBlob, err := store.SumFile(filepath.Join(fs.root, "old.txt"))
	if err != nil {
		t.Fatal(err)
	}
	// Age the recorded version past the keep window.
	fs.hist.mu.Lock()
	vs := fs.hist.byPath["old.txt"]
	vs[len(vs)-1].Time = time.Now().Add(-9 * time.Hour).UnixNano()
	fs.hist.byPath["old.txt"] = vs
	fs.hist.mu.Unlock()

	// A blob only an orphan journal references (a store's data outliving its
	// config entry) must survive.
	orphan := strings.Repeat("cd", 32)
	if err := writeFakeJournal(filepath.Join(eng.dataDir, "journal", "gone.ndjson"), "keep.txt", orphan); err != nil {
		t.Fatal(err)
	}
	if err := eng.store.Adopt(mkBlob(t, eng, "orphan bytes"), orphan); err != nil {
		t.Fatal(err)
	}

	eng.pruneOnce(time.Now())
	eng.Collect()

	if !eng.store.Has(orphan) {
		t.Fatal("collector freed a blob an orphan journal references")
	}
	live, err := store.SumFile(filepath.Join(fs.root, "old.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if !eng.store.Has(live) {
		t.Fatal("collector freed a blob the journal references")
	}
	// And a blob nobody references is gone.
	stranded := strings.Repeat("ef", 32)
	if err := eng.store.Adopt(mkBlob(t, eng, "nobody wants this"), stranded); err != nil {
		t.Fatal(err)
	}
	if removed, _ := eng.Collect(); removed == 0 {
		t.Fatal("collector freed nothing, though a blob was unreferenced")
	}
	if eng.store.Has(stranded) {
		t.Fatal("unreferenced blob survived the collector")
	}
	if !eng.store.Has(oldBlob) {
		t.Fatal("collector freed the file's own content")
	}
	eng.Close()
}

// mkBlob writes content into a temp file and returns its path, for adopting.
func mkBlob(t *testing.T, e *Engine, content string) string {
	t.Helper()
	f, err := e.store.Temp()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(content); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return f.Name()
}

// writeFakeJournal writes a minimal journal file referencing one blob.
func writeFakeJournal(path, entry, blob string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	line := `{"entry":{"path":"` + entry + `","blob":"` + blob + `","size":1,"mtime":1}}` + "\n"
	return os.WriteFile(path, []byte(line), 0o600)
}

// The deadline applies to what deletes left behind, too.
func TestTrashObeysTheDeadline(t *testing.T) {
	n := mkBackupNode(t, config.SyncLocal, "1h")
	eng, fs := n.eng, n.store("docs")

	trash := filepath.Join(eng.dataDir, "trash", fs.name)
	writeFile(t, filepath.Join(trash, "old.txt"), "long gone", time.Now().Add(-2*time.Hour))
	writeFile(t, filepath.Join(trash, "recent.txt"), "just deleted", time.Now())

	eng.pruneOnce(time.Now())

	if exists(filepath.Join(trash, "old.txt")) {
		t.Fatal("a trashed file past the deadline survived")
	}
	if !exists(filepath.Join(trash, "recent.txt")) {
		t.Fatal("a fresh trashed file was dropped")
	}
	eng.Close()
}

// A machine that keeps a remote store keeps no versions of it: the history
// belongs to whoever holds the content. This is what makes one group-level
// backup declaration correct on every machine.
func TestRemoteStoreIgnoresBackup(t *testing.T) {
	remote := mkBackupNode(t, config.SyncRemote, "")
	if fs := remote.store("docs"); fs.hist != nil {
		t.Fatal("a remote store kept a history")
	}
	remote.eng.Close()

	local := mkBackupNode(t, config.SyncLocal, "")
	if fs := local.store("docs"); fs.hist == nil {
		t.Fatal("a local store kept no history")
	}
	local.eng.Close()
}
