package syncer

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"codeberg.org/kyleraykbs/storeboard/internal/config"
	"codeberg.org/kyleraykbs/storeboard/internal/store"
	"codeberg.org/kyleraykbs/storeboard/pkg/api"
)

func noRecentBlobWindow(t *testing.T) {
	prev := recentBlob
	recentBlob = 0
	t.Cleanup(func() { recentBlob = prev })
}

func blobExists(n *node, sum string) bool {
	_, err := os.Stat(filepath.Join(n.cfg.DataDir, "store", sum[:2], sum[2:4], sum))
	return err == nil
}

func TestTrashKeepFollowsConfig(t *testing.T) {
	n := mkNode(t, "a", "b", "127.0.0.1:1", "127.0.0.1:2")
	if got := n.store("docs").trashWindow(); got != DefaultKeep {
		t.Fatalf("default trash window = %v, want a year", got)
	}
	n.eng.Close()
	n.cfg.Trash = &config.Trash{Keep: "1w"}
	n.eng = newEngine(t, n.cfg, n.log)
	if got := n.store("docs").trashWindow(); got != 7*24*time.Hour {
		t.Fatalf("trash window = %v, want a week", got)
	}
	n.cfg.Trash = &config.Trash{Keep: "soon"}
	if err := n.cfg.Validate(); err == nil {
		t.Fatal("a bad trash.keep passed validation")
	}
}

func TestPruneTrashUsesTrashWindow(t *testing.T) {
	n := mkNode(t, "a", "b", "127.0.0.1:1", "127.0.0.1:2")
	n.prepareAll()
	fs := n.store("docs")
	fs.trashKeep = time.Hour
	dir := filepath.Join(n.cfg.DataDir, "trash", "docs", "sub")
	writeFile(t, filepath.Join(dir, "old.txt"), "old", time.Now())
	writeFile(t, filepath.Join(dir, "new.txt"), "new", time.Now())
	// The ctime cannot be set back; judge "now" two hours ahead instead and
	// keep new.txt by giving it a future mtime (trashTime takes the later).
	if err := os.Chtimes(filepath.Join(dir, "new.txt"), time.Now(), time.Now().Add(3*time.Hour)); err != nil {
		t.Fatal(err)
	}
	n.eng.pruneTrash(fs, time.Now().Add(2*time.Hour))
	if _, err := os.Stat(filepath.Join(dir, "old.txt")); !os.IsNotExist(err) {
		t.Fatal("a file past the trash window was kept")
	}
	if _, err := os.Stat(filepath.Join(dir, "new.txt")); err != nil {
		t.Fatal("a file inside the trash window was removed")
	}
}

func TestCleanupTrashDryRunThenReal(t *testing.T) {
	n := mkNode(t, "a", "b", "127.0.0.1:1", "127.0.0.1:2")
	n.prepareAll()
	trash := filepath.Join(n.cfg.DataDir, "trash", "docs")
	writeFile(t, filepath.Join(trash, "a", "x.bin"), "12345", time.Now())
	writeFile(t, filepath.Join(trash, "y.bin"), "123", time.Now())

	res, err := n.eng.RunCleanup(api.CleanupRequest{What: CleanupTrash, DryRun: true}, os.Getuid())
	if err != nil {
		t.Fatal(err)
	}
	if res.TrashFiles != 2 || res.TrashBytes != 8 {
		t.Fatalf("dry run = %+v", res)
	}
	if _, err := os.Stat(filepath.Join(trash, "y.bin")); err != nil {
		t.Fatal("a dry run removed a file")
	}

	// --older-than spares what is younger.
	res, err = n.eng.RunCleanup(api.CleanupRequest{What: CleanupTrash, OlderThan: "1d"}, os.Getuid())
	if err != nil || res.TrashFiles != 0 {
		t.Fatalf("older-than cleanup = %+v, %v", res, err)
	}

	res, err = n.eng.RunCleanup(api.CleanupRequest{What: CleanupTrash, Stores: []string{"docs"}}, os.Getuid())
	if err != nil || res.TrashFiles != 2 {
		t.Fatalf("cleanup = %+v, %v", res, err)
	}
	if _, err := os.Stat(filepath.Join(trash, "a")); !os.IsNotExist(err) {
		t.Fatal("an emptied trash directory was left behind")
	}

	if _, err := n.eng.RunCleanup(api.CleanupRequest{What: CleanupTrash, Stores: []string{"nope"}}, os.Getuid()); err == nil {
		t.Fatal("an unknown store was accepted")
	}
}

func TestCleanupTrashNeedsOwner(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root owns everything")
	}
	n := mkNode(t, "a", "b", "127.0.0.1:1", "127.0.0.1:2")
	n.prepareAll()
	_, err := n.eng.RunCleanup(api.CleanupRequest{What: CleanupTrash, Stores: []string{"docs"}}, os.Getuid()+12345)
	if err == nil {
		t.Fatal("a stranger emptied a store's trash")
	}
}

func TestCleanupHistoryKeepsCurrentUnlessAsked(t *testing.T) {
	noRecentBlobWindow(t)
	n := mkBackupNode(t, config.SyncLocal, "")
	eng, fs := n.eng, n.store("docs")
	path := filepath.Join(fs.root, "note.txt")

	writeFile(t, path, "first version", time.Now().Add(-2*time.Hour))
	if _, err := eng.scan(fs, true); err != nil {
		t.Fatal(err)
	}
	first, _ := store.SumFile(path)
	writeFile(t, path, "second version", time.Now())
	if _, err := eng.scan(fs, true); err != nil {
		t.Fatal(err)
	}
	second, _ := store.SumFile(path)
	if !blobExists(n, first) || !blobExists(n, second) {
		t.Fatal("test setup: history blobs missing")
	}

	// Dry run: the old version and its blob are what would go.
	res, err := eng.RunCleanup(api.CleanupRequest{What: CleanupHistory, DryRun: true}, os.Getuid())
	if err != nil {
		t.Fatal(err)
	}
	if res.Versions != 1 || res.Blobs != 1 {
		t.Fatalf("dry run = %+v, want 1 version and 1 blob", res)
	}
	if len(fs.hist.Versions("note.txt")) != 2 || !blobExists(n, first) {
		t.Fatal("a dry run changed something")
	}

	res, err = eng.RunCleanup(api.CleanupRequest{What: CleanupHistory}, os.Getuid())
	if err != nil {
		t.Fatal(err)
	}
	if res.Versions != 1 || len(fs.hist.Versions("note.txt")) != 1 {
		t.Fatalf("cleanup = %+v, versions left %+v", res, fs.hist.Versions("note.txt"))
	}
	if blobExists(n, first) {
		t.Fatal("the dropped version's blob was kept")
	}
	if !blobExists(n, second) {
		t.Fatal("the current version's backup copy went without --current")
	}

	// --current drops the backup copy; the file itself stays.
	res, err = eng.RunCleanup(api.CleanupRequest{What: CleanupHistory, Current: true}, os.Getuid())
	if err != nil || res.Versions != 1 {
		t.Fatalf("cleanup --current = %+v, %v", res, err)
	}
	if blobExists(n, second) {
		t.Fatal("--current kept the backup copy")
	}
	if got := readFile(t, path); got != "second version" {
		t.Fatalf("the file itself changed: %q", got)
	}
	// And it survives a restart: the log on disk was rewritten.
	eng.Close()
	n.eng = newEngine(t, n.cfg, n.log)
	if v := n.store("docs").hist.Versions("note.txt"); len(v) != 0 {
		t.Fatalf("versions came back after a restart: %+v", v)
	}
}

func TestCleanupHistoryOlderThan(t *testing.T) {
	now := time.Now()
	vs := []Version{
		{Time: now.Add(-48 * time.Hour).UnixNano()},
		{Time: now.Add(-time.Hour).UnixNano()},
		{Time: now.Add(-30 * time.Minute).UnixNano()},
	}
	mask := dropMask(vs, now.Add(-24*time.Hour), false)
	if !mask[0] || mask[1] || mask[2] {
		t.Fatalf("mask = %v, want only the 2-day-old version", mask)
	}
	// A deletion record is not a current file.
	gone := []Version{{Time: now.Add(-48 * time.Hour).UnixNano(), Deleted: true}}
	if !dropMask(gone, time.Time{}, false)[0] {
		t.Fatal("a deletion record was kept as current")
	}
}

func TestCleanupOrphans(t *testing.T) {
	noRecentBlobWindow(t)
	n := mkNode(t, "a", "b", "127.0.0.1:1", "127.0.0.1:2")
	n.prepareAll()
	d := n.cfg.DataDir

	// An orphan whose journal names a blob only it uses, plus its folder.
	blobDir := filepath.Join(d, "store", "ab", "cd")
	sum := "abcd" + strings.Repeat("0", 60)
	writeFile(t, filepath.Join(blobDir, sum), "orphan bytes", time.Now().Add(-time.Hour))
	writeFile(t, filepath.Join(d, "journal", "old.ndjson"), `{"entry":{"path":"f","blob":"`+sum+`","size":12}}`+"\n", time.Now())
	writeFile(t, filepath.Join(d, "trash", "old", "t.txt"), "trash", time.Now())
	writeFile(t, filepath.Join(n.cfg.Root, "old", "mine.txt"), "user file", time.Now())

	if _, err := n.eng.RunCleanup(api.CleanupRequest{What: CleanupOrphans}, 1000); err == nil && os.Getuid() != 0 {
		t.Fatal("a non-root caller forgot a store")
	}
	if _, err := n.eng.RunCleanup(api.CleanupRequest{What: CleanupOrphans, Stores: []string{"docs"}}, 0); err == nil {
		t.Fatal("a configured store was forgotten")
	}
	if _, err := n.eng.RunCleanup(api.CleanupRequest{What: CleanupOrphans, Stores: []string{"../etc"}}, 0); err == nil {
		t.Fatal("a path was accepted as a store name")
	}

	res, err := n.eng.RunCleanup(api.CleanupRequest{What: CleanupOrphans, DryRun: true}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Items) != 1 || res.Items[0].Store != "old" || res.Blobs != 1 {
		t.Fatalf("dry run = %+v", res)
	}
	if !strings.Contains(res.Items[0].Note, "left in place") {
		t.Fatalf("the user's folder was not called out: %+v", res.Items[0])
	}

	// Unsent writes block it without --force.
	writeFile(t, filepath.Join(queueBytesDir(d, "old"), "1"), "unsent", time.Now())
	if _, err := n.eng.RunCleanup(api.CleanupRequest{What: CleanupOrphans}, 0); err == nil || !strings.Contains(err.Error(), "unsent") {
		t.Fatalf("unsent writes did not block: %v", err)
	}

	if _, err := n.eng.RunCleanup(api.CleanupRequest{What: CleanupOrphans, Force: true}, 0); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{filepath.Join(d, "journal", "old.ndjson"), filepath.Join(d, "trash", "old"), queueBytesDir(d, "old"), filepath.Join(blobDir, sum)} {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Fatalf("%s survived", p)
		}
	}
	if got := readFile(t, filepath.Join(n.cfg.Root, "old", "mine.txt")); got != "user file" {
		t.Fatal("the user's folder was touched")
	}
}

// Status counts each store's trash and versions, attributes content to what
// alone keeps it, and changes nothing.
func TestCleanupStatus(t *testing.T) {
	n := mkBackupNode(t, config.SyncLocal, "")
	eng, fs := n.eng, n.store("docs")
	path := filepath.Join(fs.root, "note.txt")
	writeFile(t, path, "first version", time.Now().Add(-2*time.Hour))
	if _, err := eng.scan(fs, true); err != nil {
		t.Fatal(err)
	}
	writeFile(t, path, "second!", time.Now())
	if _, err := eng.scan(fs, true); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(n.cfg.DataDir, "trash", "docs", "gone.txt"), "12345", time.Now())

	st, err := eng.CleanupStatus()
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Stores) != 1 {
		t.Fatalf("stores = %+v", st.Stores)
	}
	s := st.Stores[0]
	if s.TrashFiles != 1 || s.TrashBytes != 5 || s.TrashNextExpiry == "" {
		t.Fatalf("trash = %+v", s)
	}
	if s.OldVersions != 1 || s.OldVersionBytes != int64(len("first version")) {
		t.Fatalf("old versions = %+v", s)
	}
	if s.CurrentCopies != 1 || s.CurrentCopyBytes != int64(len("second!")) {
		t.Fatalf("current copies = %+v", s)
	}
	if len(fs.hist.Versions("note.txt")) != 2 {
		t.Fatal("status changed the history")
	}
	if _, err := os.Stat(filepath.Join(n.cfg.DataDir, "trash", "docs", "gone.txt")); err != nil {
		t.Fatal("status changed the trash")
	}
	if _, err := eng.RunCleanup(api.CleanupRequest{}, os.Getuid()); err == nil {
		t.Fatal("a cleanup without a kind ran")
	}
}
