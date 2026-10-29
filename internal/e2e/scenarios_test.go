package e2e

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

// --- helpers ---------------------------------------------------------------

// waitForContent waits until a file holds exactly body.
func waitForContent(t *testing.T, what, path, body string) {
	t.Helper()
	waitFor(t, what, func() bool {
		got, err := os.ReadFile(path)
		return err == nil && string(got) == body
	})
}

// waitForAbsent waits until path no longer exists.
func waitForAbsent(t *testing.T, what, path string) {
	t.Helper()
	waitFor(t, what, func() bool {
		_, err := os.Stat(path)
		return os.IsNotExist(err)
	})
}

// conflictCopies lists the conflict copies parked beside base.
func conflictCopies(t *testing.T, dir, base string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var out []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), base+".conflict-") {
			out = append(out, e.Name())
		}
	}
	sort.Strings(out)
	return out
}

// treeOfRel lists every file and directory under dir, relative to it. It is
// the comparison a person would make by hand after a sync.
func treeOfRel(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil || path == dir {
			return nil
		}
		rel, rerr := filepath.Rel(dir, path)
		if rerr != nil {
			return rerr
		}
		switch {
		case d.IsDir():
			out = append(out, "d "+rel)
		case d.Type()&os.ModeSymlink != 0:
			target, _ := os.Readlink(path)
			out = append(out, "l "+rel+" -> "+target)
		default:
			out = append(out, "f "+rel)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", dir, err)
	}
	sort.Strings(out)
	return out
}

// waitForSameTree waits until both roots hold the same layout.
func waitForSameTree(t *testing.T, what, a, b string) {
	t.Helper()
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		if equalStrings(treeOfRel(t, a), treeOfRel(t, b)) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("%s: trees did not converge\nfirst:  %v\nsecond: %v", what, treeOfRel(t, a), treeOfRel(t, b))
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// --- edits and deletions ---------------------------------------------------

// A later edit on one machine replaces the earlier content everywhere, with no
// conflict copy: an ordinary edit is not a conflict.
func TestEditPropagatesNewerContent(t *testing.T) {
	a, b := startMesh(t, mesh{
		storesA: map[string]storeDecl{"docs": {Group: "g"}},
		storesB: map[string]storeDecl{"docs": {Group: "g"}},
	})

	path := filepath.Join(a.storeDir("docs"), "note.txt")
	write(t, path, "first\n")
	waitForContent(t, "first content on bravo", filepath.Join(b.storeDir("docs"), "note.txt"), "first\n")

	write(t, path, "second\n")
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		if got, err := os.ReadFile(filepath.Join(b.storeDir("docs"), "note.txt")); err == nil && string(got) == "second\n" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if got, err := os.ReadFile(filepath.Join(b.storeDir("docs"), "note.txt")); err != nil || string(got) != "second\n" {
		t.Fatalf("bravo holds %q (%v)\nalpha journal:\n%s\nbravo journal:\n%s\nalpha log:\n%s\nbravo log:\n%s",
			string(got), err, journalOf(a, "docs"), journalOf(b, "docs"), interestingLog(a), interestingLog(b))
	}

	if copies := conflictCopies(t, b.storeDir("docs"), "note.txt"); len(copies) != 0 {
		t.Fatalf("a plain edit produced conflict copies: %v", copies)
	}
}

// Deleting a file removes it on the peer and keeps the bytes in that peer's
// trash, so the deletion can be undone.
func TestDeleteMovesFileToTrashOnPeer(t *testing.T) {
	a, b := startMesh(t, mesh{
		storesA: map[string]storeDecl{"docs": {Group: "g"}},
		storesB: map[string]storeDecl{"docs": {Group: "g"}},
	})

	const body = "content worth keeping\n"
	write(t, filepath.Join(a.storeDir("docs"), "gone.txt"), body)
	waitForContent(t, "file on bravo", filepath.Join(b.storeDir("docs"), "gone.txt"), body)

	if err := os.Remove(filepath.Join(a.storeDir("docs"), "gone.txt")); err != nil {
		t.Fatal(err)
	}
	waitForAbsent(t, "file removed on bravo", filepath.Join(b.storeDir("docs"), "gone.txt"))
	waitForContent(t, "bytes kept in bravo's trash",
		filepath.Join(b.dataDir, "trash", "docs", "gone.txt"), body)
}

// Deleting and then recreating a path converges on the new content.
func TestRecreateAfterDeleteConverges(t *testing.T) {
	a, b := startMesh(t, mesh{
		storesA: map[string]storeDecl{"docs": {Group: "g"}},
		storesB: map[string]storeDecl{"docs": {Group: "g"}},
	})

	path := filepath.Join(a.storeDir("docs"), "again.txt")
	write(t, path, "one\n")
	waitForContent(t, "first version on bravo", filepath.Join(b.storeDir("docs"), "again.txt"), "one\n")

	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	waitForAbsent(t, "deletion on bravo", filepath.Join(b.storeDir("docs"), "again.txt"))

	write(t, path, "two\n")
	waitForContent(t, "recreated content on bravo", filepath.Join(b.storeDir("docs"), "again.txt"), "two\n")
}

// Removing a directory tree on one machine removes it on the peer, files and
// directories alike.
func TestDirectoryTreeDeletionPropagates(t *testing.T) {
	a, b := startMesh(t, mesh{
		storesA: map[string]storeDecl{"docs": {Group: "g"}},
		storesB: map[string]storeDecl{"docs": {Group: "g"}},
	})

	root := filepath.Join(a.storeDir("docs"), "session")
	write(t, filepath.Join(root, "a", "one.txt"), "one\n")
	write(t, filepath.Join(root, "b", "two.txt"), "two\n")
	write(t, filepath.Join(root, "top.txt"), "top\n")
	waitForContent(t, "tree on bravo", filepath.Join(b.storeDir("docs"), "session", "b", "two.txt"), "two\n")

	if err := os.RemoveAll(root); err != nil {
		t.Fatal(err)
	}
	waitForAbsent(t, "the whole tree on bravo", filepath.Join(b.storeDir("docs"), "session"))
}

// A rename has no wire verb: it arrives as a deletion of the old name and a
// creation of the new one, and both machines end up agreeing.
func TestRenameArrivesAsDeleteAndCreate(t *testing.T) {
	a, b := startMesh(t, mesh{
		storesA: map[string]storeDecl{"docs": {Group: "g"}},
		storesB: map[string]storeDecl{"docs": {Group: "g"}},
	})

	const body = "renamed content\n"
	oldPath := filepath.Join(a.storeDir("docs"), "old.txt")
	write(t, oldPath, body)
	waitForContent(t, "old name on bravo", filepath.Join(b.storeDir("docs"), "old.txt"), body)

	if err := os.Rename(oldPath, filepath.Join(a.storeDir("docs"), "new.txt")); err != nil {
		t.Fatal(err)
	}
	waitForContent(t, "new name on bravo", filepath.Join(b.storeDir("docs"), "new.txt"), body)
	waitForAbsent(t, "old name removed on bravo", filepath.Join(b.storeDir("docs"), "old.txt"))
}

// --- conflicts -------------------------------------------------------------

// Concurrent different edits of the same path keep both versions: one at the
// path, the other parked as a deterministic conflict copy, identically on both
// machines.
func TestConcurrentEditsConflictAndConverge(t *testing.T) {
	n := startNetwork(t, map[string]map[string]storeDecl{
		"alpha": {"docs": {Group: "g"}},
		"bravo": {"docs": {Group: "g"}},
	})
	a, b := n.peer("alpha"), n.peer("bravo")

	const base = "shared.txt"
	write(t, filepath.Join(a.storeDir("docs"), base), "base\n")
	waitForContent(t, "base on bravo", filepath.Join(b.storeDir("docs"), base), "base\n")

	// Both sides edit while the link is down, so neither saw the other.
	n.stop("bravo")
	write(t, filepath.Join(a.storeDir("docs"), base), "from alpha\n")
	write(t, filepath.Join(b.storeDir("docs"), base), "from bravo\n")
	n.start("bravo")

	waitForSameTree(t, "converged conflict trees", a.storeDir("docs"), b.storeDir("docs"))
	waitFor(t, "one conflict copy on each side", func() bool {
		return len(conflictCopies(t, a.storeDir("docs"), base)) == 1 &&
			len(conflictCopies(t, b.storeDir("docs"), base)) == 1
	})

	// Neither version is lost: the path holds one, the copy the other.
	contents := map[string]bool{}
	for _, path := range []string{
		filepath.Join(a.storeDir("docs"), base),
		filepath.Join(a.storeDir("docs"), conflictCopies(t, a.storeDir("docs"), base)[0]),
	} {
		contents[read(t, path)] = true
	}
	if !contents["from alpha\n"] || !contents["from bravo\n"] {
		t.Fatalf("a version was lost: %v", contents)
	}
}

// Two machines creating the same path with different bytes is a conflict too,
// not a silent overwrite.
func TestConcurrentCreatesConflict(t *testing.T) {
	n := startNetwork(t, map[string]map[string]storeDecl{
		"alpha": {"docs": {Group: "g"}},
		"bravo": {"docs": {Group: "g"}},
	})

	const base = "same-name.txt"
	n.stop("bravo")
	write(t, filepath.Join(n.peer("alpha").storeDir("docs"), base), "alpha bytes\n")
	write(t, filepath.Join(n.peer("bravo").storeDir("docs"), base), "bravo bytes\n")
	n.start("bravo")

	a, b := n.peer("alpha"), n.peer("bravo")
	waitForSameTree(t, "converged create conflict", a.storeDir("docs"), b.storeDir("docs"))
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		if len(conflictCopies(t, a.storeDir("docs"), base)) == 1 &&
			len(conflictCopies(t, b.storeDir("docs"), base)) == 1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if ca, cb := conflictCopies(t, a.storeDir("docs"), base), conflictCopies(t, b.storeDir("docs"), base); len(ca) != 1 || len(cb) != 1 {
		t.Fatalf("conflict copies: alpha=%v bravo=%v\nalpha journal:\n%s\nbravo journal:\n%s",
			ca, cb, journalOf(a, "docs"), journalOf(b, "docs"))
	}
	if got := read(t, filepath.Join(a.storeDir("docs"), conflictCopies(t, a.storeDir("docs"), base)[0])); got != "bravo bytes\n" && got != "alpha bytes\n" {
		t.Fatalf("conflict copy holds %q", got)
	}
}

// The same edit made independently on both machines is not a conflict: the
// content agrees, so the versions merge and no copy is parked.
func TestIdenticalConcurrentEditsMergeWithoutConflict(t *testing.T) {
	n := startNetwork(t, map[string]map[string]storeDecl{
		"alpha": {"docs": {Group: "g"}},
		"bravo": {"docs": {Group: "g"}},
	})

	base := "agreed.txt"
	write(t, filepath.Join(n.peer("alpha").storeDir("docs"), base), "base\n")
	waitForContent(t, "base on bravo", filepath.Join(n.peer("bravo").storeDir("docs"), base), "base\n")

	n.stop("bravo")
	write(t, filepath.Join(n.peer("alpha").storeDir("docs"), base), "same conclusion\n")
	write(t, filepath.Join(n.peer("bravo").storeDir("docs"), base), "same conclusion\n")
	n.start("bravo")

	a, b := n.peer("alpha"), n.peer("bravo")
	waitForSameTree(t, "converged identical edit", a.storeDir("docs"), b.storeDir("docs"))
	if copies := conflictCopies(t, b.storeDir("docs"), base); len(copies) != 0 {
		t.Fatalf("identical edits produced conflict copies: %v", copies)
	}
	if copies := conflictCopies(t, a.storeDir("docs"), base); len(copies) != 0 {
		t.Fatalf("identical edits produced conflict copies: %v", copies)
	}
}

// A deletion that races an edit must not destroy the edited file: the data
// that still exists is parked as a conflict copy, and both machines retain it.
func TestConcurrentDeleteAndEditKeepsTheFile(t *testing.T) {
	n := startNetwork(t, map[string]map[string]storeDecl{
		"alpha": {"docs": {Group: "g"}},
		"bravo": {"docs": {Group: "g"}},
	})
	const base = "contested.txt"
	write(t, filepath.Join(n.peer("alpha").storeDir("docs"), base), "original\n")
	waitForContent(t, "base on bravo", filepath.Join(n.peer("bravo").storeDir("docs"), base), "original\n")
	n.stop("bravo")
	if err := os.Remove(filepath.Join(n.peer("alpha").storeDir("docs"), base)); err != nil {
		t.Fatal(err)
	}
	const edited = "edited while alpha deleted\n"
	write(t, filepath.Join(n.peer("bravo").storeDir("docs"), base), edited)
	n.start("bravo")
	a, b := n.peer("alpha"), n.peer("bravo")
	waitFor(t, "the edited conflict copy on both", func() bool {
		for _, p := range []*peer{a, b} {
			copies := conflictCopies(t, p.storeDir("docs"), base)
			if len(copies) != 1 || read(t, filepath.Join(p.storeDir("docs"), copies[0])) != edited {
				return false
			}
		}
		return true
	})
	waitForSameTree(t, "converged delete/edit race", a.storeDir("docs"), b.storeDir("docs"))
}

// A conflict copy is ordinary content: a third machine receives it too.
func TestConflictCopyReachesThirdPeer(t *testing.T) {
	n := startNetwork(t, map[string]map[string]storeDecl{
		"alpha": {"docs": {Group: "g"}},
		"bravo": {"docs": {Group: "g"}},
		"third": {"docs": {Group: "g"}},
	})

	const base = "three-way.txt"
	write(t, filepath.Join(n.peer("alpha").storeDir("docs"), base), "base\n")
	waitForContent(t, "base on third", filepath.Join(n.peer("third").storeDir("docs"), base), "base\n")

	n.stop("bravo")
	write(t, filepath.Join(n.peer("alpha").storeDir("docs"), base), "alpha wins\n")
	write(t, filepath.Join(n.peer("bravo").storeDir("docs"), base), "bravo wins\n")
	n.start("bravo")

	for _, name := range []string{"alpha", "bravo", "third"} {
		p := n.peer(name)
		waitFor(t, "conflict copy on "+name, func() bool {
			return len(conflictCopies(t, p.storeDir("docs"), base)) == 1
		})
	}
	// The copy arriving is not the end of the story: the winner has to land
	// on the third machine too.
	for _, name := range []string{"bravo", "third"} {
		waitForSameTree(t, name+" matches alpha", n.peer("alpha").storeDir("docs"), n.peer(name).storeDir("docs"))
	}
}

// --- structural overlap ----------------------------------------------------

// A path that is a file on one machine and a directory after a local change is
// replaced cleanly, with the old bytes kept aside.
func TestFileReplacedByDirectoryConverges(t *testing.T) {
	a, b := startMesh(t, mesh{
		storesA: map[string]storeDecl{"docs": {Group: "g"}},
		storesB: map[string]storeDecl{"docs": {Group: "g"}},
	})

	target := filepath.Join(a.storeDir("docs"), "item")
	write(t, target, "was a file\n")
	waitForContent(t, "file on bravo", filepath.Join(b.storeDir("docs"), "item"), "was a file\n")

	if err := os.Remove(target); err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(target, "child.txt"), "now a directory\n")

	waitFor(t, "bravo sees the directory", func() bool {
		info, err := os.Stat(filepath.Join(b.storeDir("docs"), "item"))
		return err == nil && info.IsDir()
	})
	waitForContent(t, "the child on bravo", filepath.Join(b.storeDir("docs"), "item", "child.txt"), "now a directory\n")
}

// Names are bytes: spaces, unicode and a leading dot all sync unchanged.
func TestUnicodeSpacesAndDotfilesSync(t *testing.T) {
	a, b := startMesh(t, mesh{
		storesA: map[string]storeDecl{"docs": {Group: "g"}},
		storesB: map[string]storeDecl{"docs": {Group: "g"}},
	})

	cases := map[string]string{
		"a file with spaces.txt":                "spaces\n",
		"ünïcode-ファイル.txt":                      "unicode\n",
		".hidden":                               "dotfile\n",
		"nested dir/deep ünï/another file 🌱.md": "emoji\n",
	}
	for name, body := range cases {
		write(t, filepath.Join(a.storeDir("docs"), name), body)
		waitForContent(t, name+" on bravo", filepath.Join(b.storeDir("docs"), name), body)
	}
	waitForSameTree(t, "converged odd names", a.storeDir("docs"), b.storeDir("docs"))
}

// Deep nesting is just paths: a file thirty levels down arrives with all its
// parents.
func TestDeepNestingSyncs(t *testing.T) {
	a, b := startMesh(t, mesh{
		storesA: map[string]storeDecl{"docs": {Group: "g"}},
		storesB: map[string]storeDecl{"docs": {Group: "g"}},
	})

	dir := a.storeDir("docs")
	for i := range 30 {
		dir = filepath.Join(dir, fmt.Sprintf("level%02d", i))
	}
	write(t, filepath.Join(dir, "bottom.txt"), "deep\n")

	waitFor(t, "the deep file on bravo", func() bool {
		rel, err := filepath.Rel(a.storeDir("docs"), dir)
		if err != nil {
			return false
		}
		_, err = os.Stat(filepath.Join(b.storeDir("docs"), rel, "bottom.txt"))
		return err == nil
	})
}

// Symlinks inside a store are not followed and not synced: following them
// would drag an outside tree into the mesh under the link's name.
func TestSymlinkInStoreIsNotFollowed(t *testing.T) {
	a, b := startMesh(t, mesh{
		storesA: map[string]storeDecl{"docs": {Group: "g"}},
		storesB: map[string]storeDecl{"docs": {Group: "g"}},
	})

	outside := t.TempDir()
	outsideFile := filepath.Join(outside, "secret.txt")
	write(t, outsideFile, "not for the mesh\n")

	link := filepath.Join(a.storeDir("docs"), "shortcut")
	if err := os.Symlink(outside, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	// A real file beside it proves the store is otherwise live.
	write(t, filepath.Join(a.storeDir("docs"), "real.txt"), "real\n")
	waitForContent(t, "the real file on bravo", filepath.Join(b.storeDir("docs"), "real.txt"), "real\n")

	time.Sleep(2 * time.Second) // let a few scans run
	if _, err := os.Stat(filepath.Join(b.storeDir("docs"), "shortcut")); !os.IsNotExist(err) {
		t.Fatalf("the symlink itself was synced")
	}
	if _, err := os.Stat(filepath.Join(b.storeDir("docs"), "shortcut", "secret.txt")); !os.IsNotExist(err) {
		t.Fatalf("content behind the symlink was pulled into the mesh")
	}
}

// --- outages and restarts --------------------------------------------------

// Edits made on both machines while they cannot see each other resolve when
// the link returns, leaving identical trees and no lost writes.
func TestOfflineEditsConvergeOnReconnect(t *testing.T) {
	n := startNetwork(t, map[string]map[string]storeDecl{
		"alpha": {"docs": {Group: "g"}},
		"bravo": {"docs": {Group: "g"}},
	})

	a, b := n.peer("alpha"), n.peer("bravo")
	write(t, filepath.Join(a.storeDir("docs"), "kept.txt"), "base\n")
	waitForContent(t, "base on bravo", filepath.Join(b.storeDir("docs"), "kept.txt"), "base\n")

	n.stop("bravo")
	write(t, filepath.Join(a.storeDir("docs"), "alpha-only.txt"), "made while down\n")
	write(t, filepath.Join(b.storeDir("docs"), "bravo-only.txt"), "made while down\n")
	write(t, filepath.Join(b.storeDir("docs"), "kept.txt"), "bravo edit\n")
	n.start("bravo")

	waitForContent(t, "alpha's offline file on bravo", filepath.Join(b.storeDir("docs"), "alpha-only.txt"), "made while down\n")
	waitForContent(t, "bravo's offline file on alpha", filepath.Join(a.storeDir("docs"), "bravo-only.txt"), "made while down\n")
	waitForSameTree(t, "converged after the outage", a.storeDir("docs"), b.storeDir("docs"))
}

// A deletion made while a peer is down is applied when it returns.
func TestDeletionWhilePeerIsDownAppliesOnRestart(t *testing.T) {
	n := startNetwork(t, map[string]map[string]storeDecl{
		"alpha": {"docs": {Group: "g"}},
		"bravo": {"docs": {Group: "g"}},
	})

	a, b := n.peer("alpha"), n.peer("bravo")
	write(t, filepath.Join(a.storeDir("docs"), "doomed.txt"), "bye\n")
	write(t, filepath.Join(a.storeDir("docs"), "stays.txt"), "hi\n")
	waitForContent(t, "both files on bravo", filepath.Join(b.storeDir("docs"), "doomed.txt"), "bye\n")

	n.stop("bravo")
	if err := os.Remove(filepath.Join(a.storeDir("docs"), "doomed.txt")); err != nil {
		t.Fatal(err)
	}
	n.start("bravo")

	waitForAbsent(t, "the deletion applied after restart", filepath.Join(b.storeDir("docs"), "doomed.txt"))
	if got := read(t, filepath.Join(b.storeDir("docs"), "stays.txt")); got != "hi\n" {
		t.Fatalf("unrelated file changed: %q", got)
	}
}

// Every peer in a three-machine group ends up with the same tree, no matter
// which machine the content started on.
func TestThreePeersConverge(t *testing.T) {
	n := startNetwork(t, map[string]map[string]storeDecl{
		"alpha": {"docs": {Group: "g"}},
		"bravo": {"docs": {Group: "g"}},
		"third": {"docs": {Group: "g"}},
	})

	write(t, filepath.Join(n.peer("alpha").storeDir("docs"), "from-alpha.txt"), "a\n")
	write(t, filepath.Join(n.peer("bravo").storeDir("docs"), "from-bravo.txt"), "b\n")
	write(t, filepath.Join(n.peer("third").storeDir("docs"), "from-third.txt"), "c\n")

	want := []string{"f from-alpha.txt", "f from-bravo.txt", "f from-third.txt"}
	for _, name := range n.order {
		p := n.peer(name)
		waitFor(t, name+" to hold all three files", func() bool {
			got := treeOfRel(t, p.storeDir("docs"))
			return len(got) == len(want) && equalStrings(got, want)
		})
	}
}

// The send verb never overwrites: a taken name parks the existing file beside
// it under the deterministic conflict name and reports where.
func TestSendVerbParksAnExistingFile(t *testing.T) {
	n := startNetwork(t, map[string]map[string]storeDecl{
		"alpha": {"docs": {Group: "g"}},
		"bravo": {"docs": {Group: "g"}},
	})
	a, b := n.peer("alpha"), n.peer("bravo")

	first := filepath.Join(t.TempDir(), "upload.txt")
	write(t, first, "first upload\n")
	if out := cliRun(t, a, "send", "docs", first, "--as", "shared/upload.txt", "--json"); !strings.Contains(out, "upload.txt") {
		t.Fatalf("first send: %s", out)
	}

	second := filepath.Join(t.TempDir(), "upload.txt")
	write(t, second, "second upload\n")
	out := cliRun(t, a, "send", "docs", second, "--as", "shared/upload.txt", "--json")
	var res struct {
		Path   string `json:"path"`
		Parked string `json:"parked"`
	}
	if err := json.Unmarshal([]byte(out), &res); err != nil {
		t.Fatalf("send json: %v\n%s", err, out)
	}
	if res.Parked == "" {
		t.Fatalf("the taken name was overwritten instead of parked: %s", out)
	}
	if got := read(t, filepath.Join(a.storeDir("docs"), "shared", "upload.txt")); got != "second upload\n" {
		t.Fatalf("path holds %q", got)
	}
	if got := read(t, filepath.Join(a.storeDir("docs"), res.Parked)); got != "first upload\n" {
		t.Fatalf("parked copy holds %q", got)
	}

	// Both files reach the peer.
	waitForContent(t, "the new upload on bravo", filepath.Join(b.storeDir("docs"), "shared", "upload.txt"), "second upload\n")
	waitForContent(t, "the parked copy on bravo", filepath.Join(b.storeDir("docs"), res.Parked), "first upload\n")
}

// Backup keeps the version a deletion removed, and restoring it puts the file
// back on this machine and on its peers.
func TestBackupRestoreBringsFileBackToEveryPeer(t *testing.T) {
	n := startNetworkWith(t, map[string]map[string]storeDecl{
		"alpha": {"docs": {Group: "g"}},
		"bravo": {"docs": {Group: "g"}},
	}, map[string]any{"backup": map[string]any{"enable": true, "keep": "30d"}})
	a, b := n.peer("alpha"), n.peer("bravo")

	const body = "content a restore should bring back\n"
	write(t, filepath.Join(a.storeDir("docs"), "precious.txt"), body)
	waitForContent(t, "the file on bravo", filepath.Join(b.storeDir("docs"), "precious.txt"), body)

	if err := os.Remove(filepath.Join(a.storeDir("docs"), "precious.txt")); err != nil {
		t.Fatal(err)
	}
	waitForAbsent(t, "the deletion on bravo", filepath.Join(b.storeDir("docs"), "precious.txt"))

	// The peer that applied the deletion still has the version it removed.
	list := cliRun(t, b, "backup", "list", "docs", "precious.txt", "--json")
	if !strings.Contains(list, "precious.txt") {
		t.Fatalf("bravo's history has no record of the file:\n%s", list)
	}

	cliRun(t, b, "backup", "restore", "docs", "precious.txt", "--json")
	waitForContent(t, "the restored file on bravo", filepath.Join(b.storeDir("docs"), "precious.txt"), body)
	waitForContent(t, "the restored file back on alpha", filepath.Join(a.storeDir("docs"), "precious.txt"), body)
}

// A file edited on a machine that mounts the store as a remote stream still
// reaches the holder: remote means "the bytes live elsewhere", not "read-only".
func TestRemoteStoreEditReachesHolder(t *testing.T) {
	n := startNetwork(t, map[string]map[string]storeDecl{
		"alpha": {"media": {Group: "g", Sync: "local"}},
		"bravo": {"media": {Group: "g", Sync: "remote"}},
	})
	a, b := n.peer("alpha"), n.peer("bravo")

	waitFor(t, "bravo to attach the remote store", func() bool {
		return statusOf(t, b).syncState("media") == "attached"
	})
	write(t, filepath.Join(a.storeDir("media"), "clip.txt"), "from the holder\n")
	waitForContent(t, "the file on the remote peer", filepath.Join(b.storeDir("media"), "clip.txt"), "from the holder\n")

	write(t, filepath.Join(b.storeDir("media"), "clip.txt"), "edited on the remote peer\n")
	waitForContent(t, "the edit at the holder", filepath.Join(a.storeDir("media"), "clip.txt"), "edited on the remote peer\n")
}

// Concurrent file-versus-directory at one path converges without losing the
// directory's content: the two machines that disagree settle on one tree.
func TestConcurrentFileAndDirectoryConverge(t *testing.T) {
	n := startNetwork(t, map[string]map[string]storeDecl{
		"alpha": {"docs": {Group: "g"}},
		"bravo": {"docs": {Group: "g"}},
	})
	a, b := n.peer("alpha"), n.peer("bravo")

	n.stop("bravo")
	write(t, filepath.Join(a.storeDir("docs"), "shape"), "alpha made a file\n")
	write(t, filepath.Join(b.storeDir("docs"), "shape", "child.txt"), "bravo made a directory\n")
	n.start("bravo")

	// Whatever the winner, both sides must agree and nothing may vanish
	// without a copy: the loser is parked as a conflict entry.
	waitForSameTree(t, "converged file/directory clash", a.storeDir("docs"), b.storeDir("docs"))
	entries := treeOfRel(t, a.storeDir("docs"))
	if len(entries) < 2 {
		t.Fatalf("content disappeared in the clash: %v", entries)
	}
}

// Two machines creating the same empty directory independently is agreement,
// not a clash: both keep it and the trees match.
func TestConcurrentEmptyDirectoryCreatesConverge(t *testing.T) {
	n := startNetwork(t, map[string]map[string]storeDecl{
		"alpha": {"docs": {Group: "g"}},
		"bravo": {"docs": {Group: "g"}},
	})

	n.stop("bravo")
	for _, name := range []string{"alpha", "bravo"} {
		if err := os.MkdirAll(filepath.Join(n.peer(name).storeDir("docs"), "shared-empty"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	n.start("bravo")

	a, b := n.peer("alpha"), n.peer("bravo")
	waitForSameTree(t, "converged empty directory", a.storeDir("docs"), b.storeDir("docs"))
	for _, p := range []*peer{a, b} {
		if info, err := os.Stat(filepath.Join(p.storeDir("docs"), "shared-empty")); err != nil || !info.IsDir() {
			t.Fatalf("%s lost the shared empty directory: %v", p.name, err)
		}
	}
}

// A daemon killed without shutting down keeps its files and converges again
// after it is restarted.
func TestCrashAndRestartKeepsDataAndResumesSync(t *testing.T) {
	n := startNetwork(t, map[string]map[string]storeDecl{
		"alpha": {"docs": {Group: "g"}},
		"bravo": {"docs": {Group: "g"}},
	})
	a, b := n.peer("alpha"), n.peer("bravo")

	write(t, filepath.Join(a.storeDir("docs"), "before.txt"), "written before the crash\n")
	waitForContent(t, "the file on bravo", filepath.Join(b.storeDir("docs"), "before.txt"), "written before the crash\n")

	n.kill("alpha")
	write(t, filepath.Join(b.storeDir("docs"), "during.txt"), "written while alpha was dead\n")
	write(t, filepath.Join(a.storeDir("docs"), "offline.txt"), "written while alpha was dead\n")
	n.start("alpha")

	waitForContent(t, "the peer's file after the crash", filepath.Join(a.storeDir("docs"), "during.txt"), "written while alpha was dead\n")
	waitForContent(t, "the local file after the crash", filepath.Join(b.storeDir("docs"), "offline.txt"), "written while alpha was dead\n")
	if got := read(t, filepath.Join(a.storeDir("docs"), "before.txt")); got != "written before the crash\n" {
		t.Fatalf("pre-crash file damaged: %q", got)
	}
	waitForSameTree(t, "converged after the crash", a.storeDir("docs"), b.storeDir("docs"))
}

// --- storage bloat ---------------------------------------------------------

// fileSizes lists every file under dir with its size, for failure messages.
func fileSizes(t *testing.T, dir string) string {
	t.Helper()
	var out []string
	_ = filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		info, ierr := d.Info()
		if ierr != nil {
			return nil
		}
		rel, _ := filepath.Rel(dir, path)
		out = append(out, fmt.Sprintf("%d %s", info.Size(), rel))
		return nil
	})
	sort.Strings(out)
	return strings.Join(out, "\n")
}

// dirSize sums the bytes under a directory, for growth assertions.
func dirSize(t *testing.T, dir string) int64 {
	t.Helper()
	var total int64
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, ierr := d.Info()
		if ierr == nil {
			total += info.Size()
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", dir, err)
	}
	return total
}

// A replica stores each file once: the tree holds it, and the content store
// does not keep a second copy. This is the doubling that made a 200 GiB tree
// take 400 GiB per machine.
func TestReplicaStoresEachFileOnce(t *testing.T) {
	n := startNetwork(t, map[string]map[string]storeDecl{
		"alpha": {"docs": {Group: "g"}},
		"bravo": {"docs": {Group: "g"}},
	})
	a, b := n.peer("alpha"), n.peer("bravo")

	// A megabyte, so an accidental second copy is obvious in the numbers.
	body := strings.Repeat("payload ", 128<<10)
	write(t, filepath.Join(a.storeDir("docs"), "blob.txt"), body)
	waitForContent(t, "the file on bravo", filepath.Join(b.storeDir("docs"), "blob.txt"), body)

	for _, p := range []*peer{a, b} {
		waitFor(t, p.name+" to release the redundant blob", func() bool {
			return dirSize(t, filepath.Join(p.dataDir, "store")) < int64(len(body))
		})
	}
}

// An idle mesh must not grow its state. Journals are append-only, so a
// repeated write of an unchanged entry would add disk forever; index resends
// happen every second in this harness, which makes the check sharp.
func TestIdleMeshDoesNotGrowState(t *testing.T) {
	n := startNetwork(t, map[string]map[string]storeDecl{
		"alpha": {"docs": {Group: "g"}},
		"bravo": {"docs": {Group: "g"}},
	})
	a, b := n.peer("alpha"), n.peer("bravo")

	write(t, filepath.Join(a.storeDir("docs"), "steady.txt"), "unchanged\n")
	waitForContent(t, "the file on bravo", filepath.Join(b.storeDir("docs"), "steady.txt"), "unchanged\n")
	time.Sleep(2 * time.Second) // let the first pass settle

	sizes := func() (int64, int64) {
		data := dirSize(t, a.dataDir)
		journal := int64(0)
		for _, name := range []string{"docs.ndjson"} {
			if info, err := os.Stat(filepath.Join(a.dataDir, "journal", name)); err == nil {
				journal = info.Size()
			}
		}
		return data, journal
	}
	dataBefore, journalBefore := sizes()

	time.Sleep(5 * time.Second) // several scans and index resends with no change

	dataAfter, journalAfter := sizes()
	if journalAfter != journalBefore {
		t.Fatalf("the journal grew from %d to %d bytes while nothing changed", journalBefore, journalAfter)
	}
	if dataAfter != dataBefore {
		t.Fatalf("state grew from %d to %d bytes while nothing changed; files:\n%s",
			dataBefore, dataAfter, fileSizes(t, a.dataDir))
	}
}

// A peer can still be served when the blob is gone, because the file is the
// copy. This is what lets a replica drop its content store at all.
func TestPeerIsServedFromTheTreeWhenBlobIsGone(t *testing.T) {
	n := startNetwork(t, map[string]map[string]storeDecl{
		"alpha": {"docs": {Group: "g"}},
		"bravo": {"docs": {Group: "g"}},
	}, "alpha")
	a, b := n.peer("alpha"), n.peer("bravo")

	const body = "served without a blob\n"
	write(t, filepath.Join(a.storeDir("docs"), "source.txt"), body)
	waitFor(t, "alpha to record the file", func() bool {
		return strings.Contains(journalOf(a, "docs"), "source.txt")
	})
	waitFor(t, "alpha to release its blob", func() bool {
		return dirSize(t, filepath.Join(a.dataDir, "store")) == 0
	})

	// The only peer arrives after the bytes left the content store.
	n.start("bravo")
	waitForContent(t, "the file on the late peer", filepath.Join(b.storeDir("docs"), "source.txt"), body)
}
