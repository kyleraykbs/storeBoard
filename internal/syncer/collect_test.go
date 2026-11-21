package syncer

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"codeberg.org/kyleraykbs/storeboard/internal/config"
)

// restoreFixture is a backed-up store holding one recorded version.
func restoreFixture(t *testing.T) (*node, *StoreState) {
	t.Helper()
	n := mkBackupNode(t, config.SyncLocal, "")
	fs := n.store("docs")
	writeFile(t, filepath.Join(fs.root, "note.txt"), "kept", time.Now().Add(-time.Hour))
	if _, err := n.eng.scan(fs, true); err != nil {
		t.Fatal(err)
	}
	return n, fs
}

// restore --to is written by a root daemon on behalf of whoever asked, so it
// lands only where the caller could have written it: an absolute, clean path,
// in an existing directory they own, never through or onto something else.
func TestRestoreToStaysWhereTheCallerMayWrite(t *testing.T) {
	n, _ := restoreFixture(t)
	eng := n.eng
	uid := os.Getuid()
	defer eng.Close()

	mine := t.TempDir()
	good := filepath.Join(mine, "copy.txt")
	res, err := eng.Restore("docs", uid, "note.txt", "", time.Time{}, good)
	if err != nil || !res.Copied {
		t.Fatalf("restore into my own directory = %+v, %v", res, err)
	}
	if got := readFile(t, good); got != "kept" {
		t.Fatalf("copy = %q", got)
	}
	// Replacing my own file is fine too.
	if _, err := eng.Restore("docs", uid, "note.txt", "", time.Time{}, good); err != nil {
		t.Fatalf("restore over my own file: %v", err)
	}

	for _, bad := range []string{"copy.txt", mine + "/./copy.txt", mine + "/x/../copy.txt"} {
		if _, err := eng.Restore("docs", uid, "note.txt", "", time.Time{}, bad); err == nil || !strings.Contains(err.Error(), "absolute, clean") {
			t.Fatalf("restore --to %q = %v, want a refusal", bad, err)
		}
	}

	// A missing parent is not created: restore makes the file, nothing else.
	deep := filepath.Join(mine, "not", "there", "copy.txt")
	if _, err := eng.Restore("docs", uid, "note.txt", "", time.Time{}, deep); err == nil {
		t.Fatal("restore into a missing directory succeeded")
	}
	if exists(filepath.Join(mine, "not")) {
		t.Fatal("restore created directories outside the store")
	}

	// A symlink at the destination is neither followed nor replaced.
	target := filepath.Join(mine, "target.txt")
	writeFile(t, target, "untouched", time.Now())
	link := filepath.Join(mine, "link.txt")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.Restore("docs", uid, "note.txt", "", time.Time{}, link); err == nil || !strings.Contains(err.Error(), "not a regular file") {
		t.Fatalf("restore onto a symlink = %v, want a refusal", err)
	}
	if got := readFile(t, target); got != "untouched" {
		t.Fatalf("the symlink's target was written: %q", got)
	}

	if uid == 0 {
		t.Skip("running as root: every directory is the caller's")
	}
	// A directory owned by somebody else — "/" is root's — is refused, and
	// its ownership and mode are left exactly as they were.
	foreign := "/storeboard-restore-must-not-exist"
	before, err := os.Stat("/")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := eng.Restore("docs", uid, "note.txt", "", time.Time{}, foreign); err == nil || !strings.Contains(err.Error(), "directory you own") {
		t.Fatalf("restore into / = %v, want an ownership refusal", err)
	}
	if exists(foreign) {
		t.Fatal("a refused restore wrote its file")
	}
	after, _ := os.Stat("/")
	if before.Mode() != after.Mode() {
		t.Fatalf("restore changed /'s mode: %v -> %v", before.Mode(), after.Mode())
	}
}

// A restore is a local edit on top of the current version, wherever that
// version came from: its vector must dominate the current one, or the next
// exchange with the peer that wrote it sees two concurrent edits and the
// restore can lose.
func TestRestoreDominatesAVersionFromAnotherPeer(t *testing.T) {
	n, fs := restoreFixture(t)
	defer n.eng.Close()

	cur := fs.db.Get("note.txt").clone()
	cur.VV = MergeVV(cur.VV, map[string]uint64{"a": 1000})
	if err := fs.db.Put(cur); err != nil {
		t.Fatal(err)
	}
	if _, err := n.eng.Restore("docs", os.Getuid(), "note.txt", "", time.Time{}, ""); err != nil {
		t.Fatal(err)
	}
	restored := fs.db.Get("note.txt")
	if !Dominates(restored.VV, cur.VV) || Concurrent(restored.VV, cur.VV) || restored.VV[n.eng.me] <= cur.VV[n.eng.me] {
		t.Fatalf("restored VV %v does not strictly dominate the current %v", restored.VV, cur.VV)
	}
}
