package syncer

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Streamed reads over a slow link.
//
// Every open file keeps readahead in flight, and an image viewer opens
// several at once (the picture, its neighbours, thumbnails). Over a link
// that moves a megabyte a second that was many seconds of replies queued at
// the holder, and a read near the back of the queue hit its deadline before
// its reply had even started - the photo stopped loading half drawn - while
// the holder went on sending every reply nobody was waiting for any more,
// and the retries queued behind them. The more a viewer asked, the less it
// got.
//
// So: each holder gets a bounded number of reads in flight (readGate), and
// a read someone is blocked on goes before readahead nobody needs yet. A
// read is given up only when the holder has gone quiet (readStalled), not
// because the link is slow. A read given up on is cancelled at the holder
// (readCancel), which drops it instead of sending it anyway.

// readSlots bounds the ranged reads in flight to one holder: about 3 MiB of
// 256 KiB blocks, a couple of seconds of queue on a slow link and still
// enough to fill a fast one at WAN round trips.
const readSlots = 12

// readCap bounds one read however steadily the holder is sending other
// replies: a request the holder lost is not waited on forever.
var readCap = 2 * time.Minute

// readGate admits reads to one holder, urgent ones first.
type readGate struct {
	mu      sync.Mutex
	free    int
	waiters []*readWaiter
}

type readWaiter struct {
	ready  chan struct{}
	urgent func() bool
}

func newReadGate(slots int) *readGate { return &readGate{free: slots} }

// acquire waits for a slot. urgent is consulted whenever a slot frees, so a
// readahead block that a reader starts waiting on moves to the front.
func (g *readGate) acquire(ctx context.Context, urgent func() bool) error {
	g.mu.Lock()
	if g.free > 0 && len(g.waiters) == 0 {
		g.free--
		g.mu.Unlock()
		return nil
	}
	w := &readWaiter{ready: make(chan struct{}), urgent: urgent}
	g.waiters = append(g.waiters, w)
	g.mu.Unlock()
	select {
	case <-w.ready:
		return nil
	case <-ctx.Done():
		g.mu.Lock()
		for i, other := range g.waiters {
			if other == w {
				g.waiters = append(g.waiters[:i], g.waiters[i+1:]...)
				g.mu.Unlock()
				return ctx.Err()
			}
		}
		g.mu.Unlock()
		// Granted as we gave up: hand the slot on.
		g.release()
		return ctx.Err()
	}
}

// release hands the slot to the first urgent waiter, else the oldest.
func (g *readGate) release() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.waiters) == 0 {
		g.free++
		return
	}
	pick := 0
	for i, w := range g.waiters {
		if w.urgent() {
			pick = i
			break
		}
	}
	w := g.waiters[pick]
	g.waiters = append(g.waiters[:pick], g.waiters[pick+1:]...)
	close(w.ready)
}

func (e *Engine) readGateFor(peer string) *readGate {
	e.mu.Lock()
	defer e.mu.Unlock()
	g := e.readGates[peer]
	if g == nil {
		g = newReadGate(readSlots)
		e.readGates[peer] = g
	}
	return g
}

// heard records that a peer sent something. Called for every frame off the
// link, so it is one atomic store once the peer is known.
func (e *Engine) heard(peer string) {
	now := time.Now().UnixNano()
	if v, ok := e.lastHeard.Load(peer); ok {
		v.(*atomic.Int64).Store(now)
		return
	}
	v := new(atomic.Int64)
	v.Store(now)
	if prev, loaded := e.lastHeard.LoadOrStore(peer, v); loaded {
		prev.(*atomic.Int64).Store(now)
	}
}

func (e *Engine) lastHeardFrom(peer string) time.Time {
	if v, ok := e.lastHeard.Load(peer); ok {
		return time.Unix(0, v.(*atomic.Int64).Load())
	}
	return time.Time{}
}

// readStalled reports whether a read sent at sent should be given up: the
// holder has sent nothing at all for half of readTimeout (leaving the other
// half to retry on a replacement link), or the read has waited readCap
// regardless.
func (e *Engine) readStalled(peer string, sent time.Time) bool {
	now := time.Now()
	if now.Sub(sent) > readCap {
		return true
	}
	last := e.lastHeardFrom(peer)
	if last.Before(sent) {
		last = sent
	}
	return now.Sub(last) > readTimeout/2
}

// featureReadCancel is advertised by holders that drop a read the reader
// cancelled. Anyone else is simply not told.
const featureReadCancel = "readcancel"

// cancelRead tells a holder a read is no longer wanted.
func (e *Engine) cancelRead(peer, store string, id int64) {
	e.mu.Lock()
	send := e.sends[peer]
	e.mu.Unlock()
	if send == nil || !peerHasFeature(send, featureReadCancel) {
		return
	}
	_ = e.sendTo(peer, frame{Type: "readCancel", Store: store, ID: id})
}

// Holder side: cancelled read ids, dropped when the read comes up or after a
// minute (a cancel can arrive after the read was already served).
type cancelKey struct {
	peer string
	id   int64
}

// cancelTTL is how long a cancel is kept. A cancel either arrives while its
// read is still queued here - within seconds - or after it was served, when
// it matters to nobody. Keeping one longer let it match an unrelated read: a
// reader that restarted used the same ids again.
const cancelTTL = time.Minute

func (e *Engine) noteReadCancel(peer string, id int64) {
	e.cancelMu.Lock()
	defer e.cancelMu.Unlock()
	now := time.Now()
	if len(e.readCancels) > 1024 || now.Sub(e.sx.cancelPruned) > cancelTTL {
		e.sx.cancelPruned = now
		for k, at := range e.readCancels {
			if now.Sub(at) > cancelTTL {
				delete(e.readCancels, k)
			}
		}
	}
	e.readCancels[cancelKey{peer, id}] = now
}

// readCancelled reports, once, whether a read was cancelled before it was
// served.
func (e *Engine) readCancelled(peer string, id int64) bool {
	e.cancelMu.Lock()
	defer e.cancelMu.Unlock()
	k := cancelKey{peer, id}
	if at, ok := e.readCancels[k]; ok {
		delete(e.readCancels, k)
		return time.Since(at) <= cancelTTL
	}
	return false
}
