package syncer

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Send copies: the file the user shared is still where they left it. It used
// to be renamed into the store, which emptied the caller's directory — and,
// with a root daemon, could take any file the daemon could reach.
func TestSendCopiesAndLeavesTheSource(t *testing.T) {
	n := mkNode(t, "b", "a", "127.0.0.1:1", "127.0.0.1:2")
	n.prepareAll()
	fs := n.store("docs")

	src := filepath.Join(t.TempDir(), "photo.jpg")
	when := time.Now().Add(-48 * time.Hour).Truncate(time.Second)
	writeFile(t, src, "the original", when)

	res, err := n.eng.Send("docs", os.Getuid(), src, "")
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if res.Path != "photo.jpg" || res.Size != int64(len("the original")) {
		t.Fatalf("send result = %+v", res)
	}
	if got := readFile(t, src); got != "the original" {
		t.Fatalf("source after send = %q, want it untouched", got)
	}
	dst := filepath.Join(fs.root, "photo.jpg")
	if got := readFile(t, dst); got != "the original" {
		t.Fatalf("store copy = %q", got)
	}
	if info, err := os.Stat(dst); err != nil || !info.ModTime().Equal(when) {
		t.Fatalf("store copy mtime = %v (%v), want the source's %v", info.ModTime(), err, when)
	}
	if ent := fs.db.Get("photo.jpg"); ent == nil || ent.Deleted {
		t.Fatalf("sent file not indexed: %+v", ent)
	}
	// No scratch file is left beside it.
	entries, _ := os.ReadDir(fs.root)
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), tmpPrefix) {
			t.Fatalf("scratch file left in the store: %s", e.Name())
		}
	}
}

// The daemon resolves nothing relative to its own working directory, never
// follows a symlink it is handed, and reads only the caller's own files.
func TestSendRefusesWhatTheCallerDoesNotOwn(t *testing.T) {
	n := mkNode(t, "b", "a", "127.0.0.1:1", "127.0.0.1:2")
	n.prepareAll()
	dir := t.TempDir()
	real := filepath.Join(dir, "real.txt")
	writeFile(t, real, "mine", time.Now())

	if _, err := n.eng.Send("docs", os.Getuid(), "real.txt", ""); err == nil || !strings.Contains(err.Error(), "absolute") {
		t.Fatalf("relative source = %v, want a refusal", err)
	}

	link := filepath.Join(dir, "link.txt")
	if err := os.Symlink(real, link); err != nil {
		t.Fatal(err)
	}
	if _, err := n.eng.Send("docs", os.Getuid(), link, ""); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("symlink source = %v, want a refusal", err)
	}
	if !exists(link) || readFile(t, real) != "mine" {
		t.Fatal("a refused send touched the source")
	}

	if os.Getuid() == 0 {
		t.Skip("running as root: every file is the caller's")
	}
	// A file owned by somebody else: root's, the case that matters.
	var foreign string
	for _, p := range []string{"/etc/passwd", "/etc/hostname", "/etc/os-release"} {
		info, err := os.Lstat(p)
		if err != nil || !info.Mode().IsRegular() {
			continue
		}
		if st, ok := info.Sys().(*syscall.Stat_t); ok && int(st.Uid) != os.Getuid() {
			foreign = p
			break
		}
	}
	if foreign == "" {
		t.Skip("no readable regular file owned by another user")
	}
	if _, err := n.eng.Send("docs", os.Getuid(), foreign, "stolen.txt"); err == nil || !strings.Contains(err.Error(), "only its owner or root") {
		t.Fatalf("send of %s = %v, want an ownership refusal", foreign, err)
	}
	if exists(filepath.Join(n.store("docs").root, "stolen.txt")) {
		t.Fatal("a refused send wrote into the store")
	}
}
