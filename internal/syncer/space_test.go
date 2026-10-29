package syncer

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSpaceReportsOrphansAndCleanupLeavesThemByDefault(t *testing.T) {
	n := mkNode(t, "a", "b", "127.0.0.1:1", "127.0.0.1:2")
	n.prepareAll()
	orphanRoot := filepath.Join(n.cfg.Root, "old-store")
	if err := os.MkdirAll(orphanRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(orphanRoot, "old.txt"), []byte("keep"), 0o644); err != nil {
		t.Fatal(err)
	}
	journal := filepath.Join(n.cfg.DataDir, "journal", "old-store.ndjson")
	if err := os.WriteFile(journal, []byte(`{"entry":{"path":"old.txt","blob":""}}
`), 0o600); err != nil {
		t.Fatal(err)
	}

	space := n.eng.Space()
	found := false
	for _, ent := range space.Entries {
		if strings.HasSuffix(ent.Path, "old-store") && !ent.Configured {
			found = true
		}
	}
	if !found {
		t.Fatalf("space report omitted orphan store: %+v", space)
	}
	res := n.eng.Cleanup(false)
	if len(res.OrphanStores) == 0 {
		t.Fatal("cleanup did not report orphan stores")
	}
	if _, err := os.Stat(orphanRoot); err != nil {
		t.Fatalf("default cleanup removed orphan data: %v", err)
	}
}

func TestCleanupRemoveOrphansIsExplicit(t *testing.T) {
	n := mkNode(t, "a", "b", "127.0.0.1:1", "127.0.0.1:2")
	n.prepareAll()
	orphanRoot := filepath.Join(n.cfg.Root, "old-store")
	if err := os.MkdirAll(orphanRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(orphanRoot, "old.txt"), []byte("remove"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(n.cfg.DataDir, "journal", "old-store.ndjson"), []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	result := n.eng.Cleanup(true)
	if len(result.OrphanStores) == 0 {
		t.Fatal("cleanup did not report the preserved orphan")
	}
	if _, err := os.Stat(orphanRoot); err != nil {
		t.Fatalf("cleanup removed orphan data without implementation: %v", err)
	}
}

func TestRootRepairAddsTraversalBits(t *testing.T) {
	n := mkNode(t, "a", "b", "127.0.0.1:1", "127.0.0.1:2")
	n.prepareAll()
	fs := n.store("docs")
	root := n.cfg.Root
	if err := os.Chmod(root, 0o600); err != nil {
		t.Fatal(err)
	}
	n.eng.healRoot(fs)
	info, err := os.Stat(root)
	if err != nil {
		t.Fatalf("root is missing: %v", err)
	}
	if info.Mode().Perm()&0o100 == 0 {
		t.Fatalf("root still lacks owner-execute: %v", info.Mode())
	}
}

func TestLockedRootLeavesSyncUsable(t *testing.T) {
	n := mkNode(t, "a", "b", "127.0.0.1:1", "127.0.0.1:2")
	n.prepareAll()
	fs := n.store("docs")
	root := n.cfg.Root
	if err := os.Chmod(root, 0o750); err != nil {
		t.Fatal(err)
	}
	n.eng.healRoot(fs)
	info, err := os.Stat(root)
	if err != nil {
		t.Fatalf("root is missing: %v", err)
	}
	if info.Mode().Perm()&0o100 == 0 {
		t.Fatalf("root lost owner traversal: %v", info.Mode())
	}
	writeFile(t, filepath.Join(fs.root, "after.txt"), "sync", time.Now())
	if _, err := n.eng.scan(fs, true); err != nil {
		t.Fatal(err)
	}
}
