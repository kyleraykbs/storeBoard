package streamfs

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
)

// Reopening a file - a viewer decoding the picture a thumbnailer just read -
// is served from the shared cache, not fetched from the holder again.
func TestReopenIsServedFromTheCache(t *testing.T) {
	body := strings.Repeat("x", raBlockSize+1000)
	f := &memoryFetcher{bodies: map[string]string{"blob1": body}}
	cache := newBlockCache(defaultCacheBytes)
	entry := Entry{Path: "a.jpg", Blob: "blob1", Size: int64(len(body))}
	read := func() string {
		r := newReadahead(f, entry, cache)
		defer r.close()
		dest := make([]byte, len(body))
		got := 0
		for got < len(body) {
			res, errno := r.read(context.Background(), dest[got:], int64(got))
			if errno != 0 {
				t.Fatalf("read at %d: %v", got, errno)
			}
			b, _ := res.Bytes(nil)
			if len(b) == 0 {
				t.Fatalf("short read at %d", got)
			}
			got += len(b)
		}
		return string(dest)
	}
	if read() != body {
		t.Fatal("first read returned the wrong bytes")
	}
	f.mu.Lock()
	first := len(f.opened)
	f.mu.Unlock()
	if read() != body {
		t.Fatal("second read returned the wrong bytes")
	}
	f.mu.Lock()
	second := len(f.opened)
	f.mu.Unlock()
	if first == 0 || second != first {
		t.Fatalf("fetches: %d on the first open, %d after the second; want the second served from cache", first, second)
	}
}

func TestBlockCacheEvictsOldest(t *testing.T) {
	c := newBlockCache(10)
	c.put("a", 0, []byte("12345"))
	c.put("b", 0, []byte("12345"))
	c.get("a", 0) // a is now the most recent
	c.put("c", 0, []byte("12345"))
	if _, ok := c.get("b", 0); ok {
		t.Fatal("least recently used block survived")
	}
	if _, ok := c.get("a", 0); !ok {
		t.Fatal("recently used block was evicted")
	}
}

func TestUrgencyDefaultsToUrgent(t *testing.T) {
	if !Urgent(context.Background()) {
		t.Fatal("a fetch with no hint must be urgent")
	}
	var flag atomic.Bool
	ctx := withUrgency(context.Background(), &flag)
	if Urgent(ctx) {
		t.Fatal("unflagged readahead reported urgent")
	}
	flag.Store(true)
	if !Urgent(ctx) {
		t.Fatal("flag change not seen")
	}
}
