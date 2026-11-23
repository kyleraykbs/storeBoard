package streamfs

import (
	"context"
	"errors"
	"strings"
	"sync"
	"syscall"
	"testing"
)

// failOnceFetcher fails the first fetch of each offset, then serves it.
type failOnceFetcher struct {
	mu     sync.Mutex
	body   string
	failed map[int64]bool
	calls  int
}

func (f *failOnceFetcher) ReadRange(_ context.Context, _ Entry, offset, length int64) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if !f.failed[offset] {
		f.failed[offset] = true
		return nil, errors.New("link dropped")
	}
	return []byte(f.body[offset : offset+length]), nil
}

// A readahead block that failed while nobody needed it (the link blinked) must
// not become the reader's EIO when it gets there: it is fetched once more.
func TestFailedReadaheadIsRefetchedWhenNeeded(t *testing.T) {
	body := strings.Repeat("x", 2*raBlockSize)
	f := &failOnceFetcher{body: body, failed: map[int64]bool{}}
	r := newReadahead(f, Entry{Path: "a", Blob: "b", Size: int64(len(body))}, nil)
	defer r.close()

	r.mu.Lock()
	b := r.startLocked(1)
	r.mu.Unlock()
	<-b.done
	if b.err == nil {
		t.Fatal("the speculative fetch was meant to fail")
	}
	dest := make([]byte, 4096)
	res, errno := r.read(context.Background(), dest, raBlockSize)
	if errno != 0 {
		t.Fatalf("read of a block whose readahead failed: %v", errno)
	}
	if got, _ := res.Bytes(nil); len(got) != len(dest) {
		t.Fatalf("read %d bytes, want %d", len(got), len(dest))
	}
}

// A block this read started itself and that failed is the answer: no retry.
func TestFailedUrgentReadIsNotRetried(t *testing.T) {
	body := strings.Repeat("x", raBlockSize)
	f := &failOnceFetcher{body: body, failed: map[int64]bool{}}
	r := newReadahead(f, Entry{Path: "a", Blob: "b", Size: int64(len(body))}, nil)
	defer r.close()
	if _, errno := r.read(context.Background(), make([]byte, 10), 0); errno == 0 {
		t.Fatal("a failed fetch for this read should fail it")
	}
	if f.calls != 1 {
		t.Fatalf("%d fetches, want 1", f.calls)
	}
}

type countingRename struct{ n int }

func (m *countingRename) Create(string, uint32) (PendingFile, error) { return nil, nil }
func (m *countingRename) Open(string, bool) (PendingFile, error)     { return nil, nil }
func (m *countingRename) Truncate(string, int64) error               { return nil }
func (m *countingRename) Remove(string) error                        { return nil }
func (m *countingRename) MakeDir(string, uint32) error               { return nil }
func (m *countingRename) RemoveDir(string) error                     { return nil }
func (m *countingRename) Rename(string, string) error                { m.n++; return nil }

// RENAME_EXCHANGE swaps; doing a plain move instead deleted the destination.
// RENAME_NOREPLACE must not replace what the tree already has.
func TestRenameHonoursFlags(t *testing.T) {
	m := &countingRename{}
	fsys := &FS{mutator: m, tree: NewTree(map[string]Entry{
		"a.txt": {Blob: "1", Size: 1},
		"b.txt": {Blob: "2", Size: 1},
	})}
	root := &dirNode{fsys: fsys}
	if errno := root.Rename(context.Background(), "a.txt", root, "b.txt", renameExchange); errno != syscall.EINVAL {
		t.Fatalf("RENAME_EXCHANGE = %v, want EINVAL", errno)
	}
	if errno := root.Rename(context.Background(), "a.txt", root, "b.txt", renameNoReplace); errno != syscall.EEXIST {
		t.Fatalf("RENAME_NOREPLACE onto an existing file = %v, want EEXIST", errno)
	}
	if m.n != 0 {
		t.Fatalf("the mutator was asked to rename %d times", m.n)
	}
}
