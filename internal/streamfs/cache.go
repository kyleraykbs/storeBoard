package streamfs

import (
	"container/list"
	"context"
	"sync"
	"sync/atomic"
)

// blockCache keeps recently read blocks in memory, shared by every handle of
// one mount. Opening a photo is rarely one read: a viewer probes the header,
// a thumbnailer reads it, then the viewer opens it again to decode, and each
// open used to fetch every byte from the holder again. Over a slow link that
// tripled the wait for a picture.
//
// Blocks are keyed by blob - the hash of the file's content - so a cached
// block can never be stale: a changed file is a different blob.
type blockCache struct {
	mu    sync.Mutex
	max   int64
	size  int64
	order *list.List // front is most recently used
	items map[blockKey]*list.Element
}

type blockKey struct {
	blob  string
	block int64
}

type cached struct {
	key  blockKey
	data []byte
}

// defaultCacheBytes bounds the shared block cache: a few dozen photos, or the
// last few seconds of a video, never the store.
const defaultCacheBytes = 192 << 20

func newBlockCache(max int64) *blockCache {
	if max <= 0 {
		return nil
	}
	return &blockCache{max: max, order: list.New(), items: map[blockKey]*list.Element{}}
}

func (c *blockCache) get(blob string, block int64) ([]byte, bool) {
	if c == nil || blob == "" {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.items[blockKey{blob, block}]
	if !ok {
		return nil, false
	}
	c.order.MoveToFront(el)
	return el.Value.(*cached).data, true
}

func (c *blockCache) put(blob string, block int64, data []byte) {
	if c == nil || blob == "" || int64(len(data)) > c.max {
		return
	}
	key := blockKey{blob, block}
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[key]; ok {
		c.order.MoveToFront(el)
		return
	}
	c.items[key] = c.order.PushFront(&cached{key: key, data: data})
	c.size += int64(len(data))
	for c.size > c.max {
		el := c.order.Back()
		old := el.Value.(*cached)
		c.order.Remove(el)
		delete(c.items, old.key)
		c.size -= int64(len(old.data))
	}
}

// Urgency. A readahead block is fetched before anyone needs it; once a reader
// is blocked on it, it is urgent. The fetcher learns which through the
// context, and can let urgent fetches go first when it has to queue them -
// otherwise the block a viewer is waiting on sits behind speculative reads
// of a file it already moved past.
type urgencyKey struct{}

func withUrgency(ctx context.Context, flag *atomic.Bool) context.Context {
	return context.WithValue(ctx, urgencyKey{}, flag)
}

// Urgent reports whether a reader is blocked on the fetch ctx belongs to
// right now. It can change from false to true while the fetch waits. A fetch
// that carries no hint is urgent.
func Urgent(ctx context.Context) bool {
	flag, ok := ctx.Value(urgencyKey{}).(*atomic.Bool)
	return !ok || flag.Load()
}
