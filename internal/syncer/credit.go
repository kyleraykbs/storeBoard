package syncer

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"codeberg.org/kyleraykbs/storeboard/internal/transport"
)

// Flow control for blob chunks.
//
// A link is one TCP stream, and its read loop hands each blob chunk to a
// worker that writes it to disk. When the workers fall behind - a holder
// filing thirty thousand deletes into its trash, a disk busy elsewhere - the
// read loop waits for room, stops reading the socket, and everything the
// sender wrote after the chunks waits with them: index frames, and the
// request of a streamed read someone is watching a video through. A copy
// into a streamed folder could stall a video's first bytes for minutes.
//
// So a sender keeps at most blobWindow bytes of chunks unprocessed at a
// receiver that can say when it has processed them (featureBlobCredit):
// past that it waits, and the link stays free for everything else. The
// receiver returns credit as its workers finish frames, a few at a time.

const featureBlobCredit = "blobcredit"

// blobWindow is the chunk bytes in flight to one receiver: enough to keep a
// fast link full at a WAN round trip, few enough that the receiver's queue
// can always take them without making its read loop wait.
const blobWindow = 8 << 20

// creditMinFrame is what one frame costs at least. The window is bytes, and
// a copy of a node_modules tree is tens of thousands of files of a few
// hundred bytes: 8 MiB of those was thousands of frames, more than the
// receiver's queue held, and its read loop waited again. Charging small
// frames as this much bounds the frames in flight too (blobWindow /
// creditMinFrame).
const creditMinFrame = 4 << 10

func frameCost(n int) int64 {
	if n < creditMinFrame {
		return creditMinFrame
	}
	return int64(n)
}

// creditBatch is how much a receiver acknowledges at once, unless its queue
// just ran dry (then it says so at once: the sender may be waiting).
const creditBatch = 1 << 20

// creditStall gives up waiting for credit that never comes back (a receiver
// that lost track of it): the transfer then carries on as before credit
// existed rather than hanging.
var creditStall = 2 * time.Minute

// blobWorkHook, when set (tests only), runs before a blob worker handles a
// frame: a way to make a receiver's disk slow.
var blobWorkHook atomic.Pointer[func(e *Engine, pf peerFrame)]

type credit struct {
	mu   sync.Mutex
	out  int64
	wake chan struct{}
	last time.Time // when credit last came back, or the first take
}

func (e *Engine) creditFor(send transport.Sender) *credit {
	e.creditMu.Lock()
	defer e.creditMu.Unlock()
	c := e.credits[send]
	if c == nil {
		c = &credit{wake: make(chan struct{}), last: time.Now()}
		e.credits[send] = c
	}
	return c
}

// forgetCredit drops a link's accounting when the link goes.
func (e *Engine) forgetCredit(send transport.Sender) {
	// Only links that keep a window are ever keys (and those are pointers:
	// a test's fake sender need not be comparable).
	if send == nil || !peerHasFeature(send, featureBlobCredit) {
		return
	}
	e.creditMu.Lock()
	delete(e.credits, send)
	e.creditMu.Unlock()
}

// take waits until n more bytes fit in the window. One frame always fits
// an empty window, however large.
func (c *credit) take(ctx context.Context, n int64) error {
	for {
		c.mu.Lock()
		if c.out == 0 || c.out+n <= blobWindow {
			if c.out == 0 {
				c.last = time.Now()
			}
			c.out += n
			c.mu.Unlock()
			return nil
		}
		if time.Since(c.last) > creditStall {
			// Nothing came back for minutes while frames were out: the
			// receiver is not counting. Carry on unmetered.
			c.out = 0
			c.mu.Unlock()
			continue
		}
		wake := c.wake
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wake:
		case <-time.After(creditStall / 4):
		}
	}
}

func (c *credit) grant(n int64) {
	c.mu.Lock()
	c.out -= n
	if c.out < 0 {
		c.out = 0
	}
	c.last = time.Now()
	close(c.wake)
	c.wake = make(chan struct{})
	c.mu.Unlock()
}

// sendBlobFrame writes a blob or blobEnd frame of a transfer, within the
// receiver's window when it keeps one.
func (e *Engine) sendBlobFrame(ctx context.Context, send transport.Sender, fr frame, bulk bool) error {
	b, err := json.Marshal(fr)
	if err != nil {
		return err
	}
	if peerHasFeature(send, featureBlobCredit) {
		if err := e.creditFor(send).take(ctx, frameCost(len(b))); err != nil {
			return err
		}
	}
	b = append(b, '\n')
	if bulk {
		return send.SendBulk(b)
	}
	return send.Send(b)
}

// gotCredit is a receiver's acknowledgement of processed chunk bytes.
func (e *Engine) gotCredit(peer string, n int64) {
	e.mu.Lock()
	send := e.sends[peer]
	e.mu.Unlock()
	if send == nil || n <= 0 || !peerHasFeature(send, featureBlobCredit) {
		return
	}
	e.creditMu.Lock()
	c := e.credits[send]
	e.creditMu.Unlock()
	if c != nil {
		c.grant(n)
	}
}

// returnCredit notes that a worker processed a peer's chunk frame of size
// bytes, and acknowledges once enough has built up or the queue is empty.
func (e *Engine) returnCredit(peer string, size int, queueEmpty bool) {
	e.creditMu.Lock()
	e.owed[peer] += frameCost(size)
	owed := e.owed[peer]
	if owed < creditBatch && !queueEmpty {
		e.creditMu.Unlock()
		return
	}
	delete(e.owed, peer)
	e.creditMu.Unlock()
	e.mu.Lock()
	send := e.sends[peer]
	e.mu.Unlock()
	if send == nil || !peerHasFeature(send, featureBlobCredit) {
		return
	}
	_ = e.sendTo(peer, frame{Type: "blobCredit", Length: owed})
}

// peerMetered reports whether a peer's current link keeps a blob window.
func (e *Engine) peerMetered(peer string) bool {
	e.mu.Lock()
	send := e.sends[peer]
	e.mu.Unlock()
	return send != nil && peerHasFeature(send, featureBlobCredit)
}

// blobQueue is one blob worker's backlog. Its length is bounded by the
// senders' windows for peers that keep one, and by blobFrameQueue (waiting
// for room) for those that do not.
type blobQueue struct {
	mu     sync.Mutex
	items  []peerFrame
	notify chan struct{} // a frame arrived
	space  chan struct{} // a frame left
}

func newBlobQueue() *blobQueue {
	return &blobQueue{notify: make(chan struct{}, 1), space: make(chan struct{}, 1)}
}

func (q *blobQueue) push(pf peerFrame, bounded bool, closed <-chan struct{}) {
	for {
		q.mu.Lock()
		if !bounded || len(q.items) < blobFrameQueue {
			q.items = append(q.items, pf)
			q.mu.Unlock()
			select {
			case q.notify <- struct{}{}:
			default:
			}
			return
		}
		q.mu.Unlock()
		select {
		case <-q.space:
		case <-closed:
			return
		}
	}
}

func (q *blobQueue) pop(closed <-chan struct{}) (peerFrame, bool) {
	for {
		q.mu.Lock()
		if len(q.items) > 0 {
			pf := q.items[0]
			q.items[0] = peerFrame{}
			q.items = q.items[1:]
			if len(q.items) == 0 {
				q.items = nil
			}
			q.mu.Unlock()
			select {
			case q.space <- struct{}{}:
			default:
			}
			return pf, true
		}
		q.mu.Unlock()
		select {
		case <-q.notify:
		case <-closed:
			return peerFrame{}, false
		}
	}
}

func (q *blobQueue) len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}
