package streamfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
)

// fuseAvailable reports whether this machine can mount a userspace filesystem
// at all. The daemon's deployment requires it; a test host without it skips.
func fuseAvailable() bool {
	if _, err := os.Stat("/dev/fuse"); err != nil {
		return false
	}
	for _, name := range []string{"fusermount3", "fusermount"} {
		if _, err := lookPath(name); err == nil {
			return true
		}
	}
	return false
}

func lookPath(name string) (string, error) {
	for _, dir := range []string{"/run/wrappers/bin", "/usr/bin", "/bin", "/nix/var/nix/profiles/default/bin"} {
		p := filepath.Join(dir, name)
		if info, err := os.Stat(p); err == nil && !info.IsDir() {
			return p, nil
		}
	}
	return "", errors.New("not found")
}

// memoryFetcher serves entries from a map, standing in for the engine's
// network fetch.
type memoryFetcher struct {
	bodies map[string]string
	opened []string
}

func (m *memoryFetcher) ReadRange(_ context.Context, entry Entry, offset, length int64) ([]byte, error) {
	m.opened = append(m.opened, entry.Blob)
	body, ok := m.bodies[entry.Blob]
	if !ok {
		return nil, os.ErrNotExist
	}
	if offset < 0 || offset > int64(len(body)) || length < 0 || offset+length > int64(len(body)) {
		return nil, syscall.EINVAL
	}
	return []byte(body[offset : offset+length]), nil
}

// The mount presents the journal's tree: directories synthesised from deep
// paths, sizes from entries, and file bytes fetched on open — never stored.
func TestMountServesJournalTree(t *testing.T) {
	if !fuseAvailable() {
		t.Skip("no /dev/fuse or fusermount on this host")
	}
	const body = "photo bytes that live on the holder\n"
	tree := NewTree(map[string]Entry{
		"Walks & Daily/October 2026/Walk 4/IMG_5639.JPG": {Blob: "blob-jpg", Size: int64(len(body)), Mode: 0o644, MTime: time.Unix(1700000000, 0)},
		"notes.txt": {Blob: "blob-txt", Size: 5, Mode: 0o644},
		"album":     {Dir: true, Mode: 0o755},
	})
	fetcher := &memoryFetcher{bodies: map[string]string{"blob-jpg": body, "blob-txt": "hello"}}

	mnt := filepath.Join(t.TempDir(), "mnt")
	if err := os.Mkdir(mnt, 0o755); err != nil {
		t.Fatal(err)
	}
	mounted, err := Mount(mnt, tree, Options{Fetcher: fetcher})
	if err != nil {
		t.Fatalf("mount: %v", err)
	}
	t.Cleanup(func() { _ = mounted.Unmount() })

	entries, err := os.ReadDir(mnt)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	for _, want := range []string{"Walks & Daily", "notes.txt", "album"} {
		found := false
		for _, name := range names {
			if name == want {
				found = true
			}
		}
		if !found {
			t.Fatalf("root listing %v is missing %q", names, want)
		}
	}

	// Reads happen in at least one explicit range and write nothing locally.
	deep, err := openDeep(t, mnt)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4)
	if _, err := deep.ReadAt(buf, 2); err != nil {
		t.Fatal(err)
	}
	if string(buf) != body[2:6] {
		t.Fatalf("ranged read = %q", buf)
	}
	if err := deep.Close(); err != nil {
		t.Fatal(err)
	}
	if len(fetcher.opened) == 0 {
		t.Fatal("no ranged fetch happened")
	}
	if err := mounted.Unmount(); err != nil {
		t.Fatalf("unmount: %v", err)
	}
	leftovers, err := os.ReadDir(mnt)
	if err != nil {
		t.Fatalf("readdir after unmount: %v", err)
	}
	if len(leftovers) != 0 {
		t.Fatalf("%d files were written behind the mount", len(leftovers))
	}
}

func openDeep(t *testing.T, mount string) (*os.File, error) {
	t.Helper()
	return os.Open(filepath.Join(mount, "Walks & Daily", "October 2026", "Walk 4", "IMG_5639.JPG"))
}

func TestSeekUsesByteRanges(t *testing.T) {
	if !fuseAvailable() {
		t.Skip("no /dev/fuse or fusermount on this host")
	}
	tree := NewTree(map[string]Entry{"big.bin": {Blob: "big", Size: 1024, Mode: 0o644}})
	fetcher := &memoryFetcher{bodies: map[string]string{"big": strings.Repeat("x", 1024)}}
	mnt := filepath.Join(t.TempDir(), "mnt")
	if err := os.Mkdir(mnt, 0o755); err != nil {
		t.Fatal(err)
	}
	mounted, err := Mount(mnt, tree, Options{Fetcher: fetcher})
	if err != nil {
		t.Fatalf("mount: %v", err)
	}
	t.Cleanup(func() { _ = mounted.Unmount() })
	f, err := os.Open(filepath.Join(mnt, "big.bin"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	middle := make([]byte, 8)
	if _, err := f.ReadAt(middle, 100); err != nil && !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	if string(middle) != strings.Repeat("x", 8) {
		t.Fatalf("seeking read = %q", middle)
	}
}

// A read that the holder cannot satisfy fails with a code the caller can act
// on, not a silent empty file.
func TestMissingBlobReportsNoSuchFile(t *testing.T) {
	if !fuseAvailable() {
		t.Skip("no /dev/fuse or fusermount on this host")
	}
	tree := NewTree(map[string]Entry{"gone.txt": {Blob: "absent", Size: 3, Mode: 0o644}})
	fetcher := &memoryFetcher{bodies: map[string]string{}}
	mnt := filepath.Join(t.TempDir(), "mnt")
	if err := os.Mkdir(mnt, 0o755); err != nil {
		t.Fatal(err)
	}
	mounted, err := Mount(mnt, tree, Options{Fetcher: fetcher})
	if err != nil {
		t.Fatalf("mount: %v", err)
	}
	t.Cleanup(func() { _ = mounted.Unmount() })

	_, err = os.ReadFile(filepath.Join(mnt, "gone.txt"))
	if err == nil {
		t.Fatal("reading a file with no holder copy succeeded")
	}
	if !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("err = %v, want ENOENT", err)
	}
}

// Writes are refused until write-through exists: silently accepting them
// would create a shadow tree the user believes is in the store.
func TestWriteIsRefused(t *testing.T) {
	if !fuseAvailable() {
		t.Skip("no /dev/fuse or fusermount on this host")
	}
	tree := NewTree(map[string]Entry{"file.txt": {Blob: "b", Size: 1, Mode: 0o644}})
	mnt := filepath.Join(t.TempDir(), "mnt")
	if err := os.Mkdir(mnt, 0o755); err != nil {
		t.Fatal(err)
	}
	mounted, err := Mount(mnt, tree, Options{Fetcher: &memoryFetcher{bodies: map[string]string{"b": "x"}}})
	if err != nil {
		t.Fatalf("mount: %v", err)
	}
	t.Cleanup(func() { _ = mounted.Unmount() })

	if err := os.WriteFile(filepath.Join(mnt, "file.txt"), []byte("new"), 0o644); err == nil {
		t.Fatal("write succeeded on a streamed store")
	}
	if err := os.WriteFile(filepath.Join(mnt, "new.txt"), []byte("new"), 0o644); err == nil {
		t.Fatal("creating a file succeeded on a streamed store")
	}
}

// A journal replaced under a mounted filesystem is visible to the next lookup:
// the tree is the source of truth, not a cache of it.
func TestTreeReplacementIsVisible(t *testing.T) {
	if !fuseAvailable() {
		t.Skip("no /dev/fuse or fusermount on this host")
	}
	tree := NewTree(map[string]Entry{"one.txt": {Blob: "1", Size: 1, Mode: 0o644}})
	mnt := filepath.Join(t.TempDir(), "mnt")
	if err := os.Mkdir(mnt, 0o755); err != nil {
		t.Fatal(err)
	}
	mounted, err := Mount(mnt, tree, Options{Fetcher: &memoryFetcher{bodies: map[string]string{"1": "a", "2": "b"}}})
	if err != nil {
		t.Fatalf("mount: %v", err)
	}
	t.Cleanup(func() { _ = mounted.Unmount() })

	if _, err := os.Stat(filepath.Join(mnt, "one.txt")); err != nil {
		t.Fatalf("initial entry missing: %v", err)
	}
	tree.Replace(map[string]Entry{"two.txt": {Blob: "2", Size: 1, Mode: 0o644}})
	if _, err := os.Stat(filepath.Join(mnt, "two.txt")); err != nil {
		t.Fatalf("replaced entry missing: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(mnt, "two.txt"))
	if err != nil || string(got) != "b" {
		t.Fatalf("content = %q (%v)", got, err)
	}
}

// Empty stores still mount: a streamed store with no journal entries is an
// empty directory, not a failure.
func TestEmptyTreeMounts(t *testing.T) {
	if !fuseAvailable() {
		t.Skip("no /dev/fuse or fusermount on this host")
	}
	mnt := filepath.Join(t.TempDir(), "mnt")
	if err := os.Mkdir(mnt, 0o755); err != nil {
		t.Fatal(err)
	}
	mounted, err := Mount(mnt, NewTree(nil), Options{Fetcher: &memoryFetcher{}})
	if err != nil {
		t.Fatalf("mount: %v", err)
	}
	t.Cleanup(func() { _ = mounted.Unmount() })
	entries, err := os.ReadDir(mnt)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("empty tree listed %d entries", len(entries))
	}
	fmt.Fprint(io.Discard, "")
}

// fakePending records what the mount did to a file being written.
type fakePending struct {
	body   []byte
	size   int64
	synced bool
	closed int
}

func (f *fakePending) WriteAt(p []byte, off int64) (int, error) {
	if f.closed > 0 {
		return 0, os.ErrClosed
	}
	f.body = append(f.body, p...)
	return len(p), nil
}

func (f *fakePending) Truncate(size int64) error {
	if f.closed > 0 {
		return os.ErrClosed
	}
	f.size = size
	return nil
}

func (f *fakePending) Sync() error { f.synced = true; return nil }

func (f *fakePending) Close() error { f.closed++; return nil }

// Truncate reaches the mutator when no write handle is open, which is what
// `truncate(2)` and open(O_TRUNC) on an existing file do.
func TestSetattrTruncatesThroughTheMutator(t *testing.T) {
	pending := &fakePending{}
	m := &recordingMutator{pending: pending}
	f := &fileNode{fsys: &FS{mutator: m}, path: "notes.txt", entry: Entry{Path: "notes.txt", Size: 40}}

	in := &fuse.SetAttrIn{}
	in.Valid = fuse.FATTR_SIZE
	in.Size = 7
	out := &fuse.AttrOut{}
	if errno := f.Setattr(context.Background(), nil, in, out); errno != 0 {
		t.Fatalf("setattr: %v", errno)
	}
	if m.truncated != 7 {
		t.Fatalf("truncate(%d), want 7", m.truncated)
	}
	if f.entry.Size != 7 || out.Size != 7 {
		t.Fatalf("sizes after truncate: entry=%d out=%d", f.entry.Size, out.Size)
	}
}

// recordingMutator captures the calls the mount makes.
type recordingMutator struct {
	pending   *fakePending
	truncated int64
}

func (m *recordingMutator) Create(string, uint32) (PendingFile, error) { return m.pending, nil }
func (m *recordingMutator) Open(string, bool) (PendingFile, error)     { return m.pending, nil }
func (m *recordingMutator) Truncate(_ string, size int64) error        { m.truncated = size; return nil }
func (m *recordingMutator) Remove(string) error                        { return nil }
func (m *recordingMutator) MakeDir(string, uint32) error               { return nil }
func (m *recordingMutator) RemoveDir(string) error                     { return nil }
func (m *recordingMutator) Rename(string, string) error                { return nil }
