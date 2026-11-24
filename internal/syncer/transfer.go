package syncer

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"slices"
	"sync"
	"time"

	"codeberg.org/kyleraykbs/storeboard/internal/store"
	"codeberg.org/kyleraykbs/storeboard/internal/transport"
)

// Transfers are addressed by content. A want names the store, the path it is
// for, and the blob - the exact version. The holder serves those bytes from
// wherever it has them: its content store, the file at that path, or any other
// file in the store with the same hash. Asking by path alone meant a conflict
// loser whose path had moved on could never be fetched, and a holder that had
// moved past the asked-for version quietly sent a different one.

// conflictSlot keys a conflict-copy want apart from a plain update of the
// same blob at the same path, so neither overwrites the other.
func wantSlot(blob string, conflict bool) string {
	if conflict {
		return "conflict:" + blob
	}
	return blob
}

func (p *pending) slot() string {
	if p.entry == nil {
		return wantSlot("", p.conflict)
	}
	return wantSlot(p.entry.Blob, p.conflict)
}

// wantLocked returns the want for one version of a path. Callers hold e.mu.
func (e *Engine) wantLocked(store, path, slot string) *pending {
	return e.wants[wantKey(store, path)][slot]
}

func (e *Engine) setWantLocked(store, path string, p *pending) {
	key := wantKey(store, path)
	set := e.wants[key]
	if set == nil {
		set = map[string]*pending{}
		e.wants[key] = set
	}
	set[p.slot()] = p
}

// takeWantLocked removes and returns one want.
func (e *Engine) takeWantLocked(store, path, slot string) *pending {
	key := wantKey(store, path)
	set := e.wants[key]
	p := set[slot]
	if p == nil {
		return nil
	}
	delete(set, slot)
	if len(set) == 0 {
		delete(e.wants, key)
	}
	return p
}

// wantedLocked reports whether this side asked for these bytes: blob "" (an
// older sender that does not say) matches any want for the path.
func (e *Engine) wantedLocked(store, path, blob string) bool {
	set := e.wants[wantKey(store, path)]
	if blob == "" {
		return len(set) > 0
	}
	return set[wantSlot(blob, false)] != nil || set[wantSlot(blob, true)] != nil
}

// dropSupersededLocked forgets plain wants for older versions of a path once
// a newer one is asked for: their bytes would only be discarded on arrival.
func (e *Engine) dropSupersededLocked(store, path, blob string) {
	set := e.wants[wantKey(store, path)]
	for slot, p := range set {
		if !p.conflict && p.entry != nil && p.entry.Blob != blob {
			delete(set, slot)
		}
	}
	if len(set) == 0 {
		delete(e.wants, wantKey(store, path))
	}
}

// awaitingUpdate reports whether a new version of the path is on its way
// (a plain want, not a conflict copy, which lands under another name).
func (e *Engine) awaitingUpdate(store, path string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, p := range e.wants[wantKey(store, path)] {
		if !p.conflict {
			return true
		}
	}
	return false
}

// wantCount is the number of versions this side is waiting for.
func (e *Engine) wantCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	n := 0
	for _, set := range e.wants {
		n += len(set)
	}
	return n
}

// Want refusals. A holder that predates them stays silent instead, and the
// want is retried after wantRefresh.
const (
	wantMissing = "missing" // the holder has no copy of this blob
)

// handleWantErr moves a want the holder could not serve on to the next peer
// that might have the bytes. Content addressing is what makes that possible:
// any peer holding the blob can answer, not only the one that announced it.
func (e *Engine) handleWantErr(peer string, fr *frame) {
	fs := e.byName[fr.Store]
	if fs == nil || !validRel(fr.Path) || !store.ValidSum(fr.Blob) {
		return
	}
	candidates := e.holdersFor(fs)
	type retry struct {
		to   string
		slot string
	}
	var retries []retry
	e.mu.Lock()
	for _, conflict := range []bool{false, true} {
		slot := wantSlot(fr.Blob, conflict)
		p := e.wantLocked(fs.name, fr.Path, slot)
		if p == nil || p.peer != peer {
			continue
		}
		next := ""
		for _, c := range candidates {
			if !slices.Contains(p.tried, c) {
				next = c
				break
			}
		}
		if next == "" {
			// Nobody reachable has it. The next index exchange asks again,
			// by which time a holder may have it.
			e.takeWantLocked(fs.name, fr.Path, slot)
			continue
		}
		p.peer = next
		p.tried = append(p.tried, next)
		p.asked = time.Now()
		retries = append(retries, retry{to: next, slot: slot})
	}
	e.mu.Unlock()
	if len(retries) == 0 {
		e.logger.Info("no reachable peer holds the wanted version", "store", fs.name, "path", fr.Path, "blob", fr.Blob, "refused", peer)
		return
	}
	for _, r := range retries {
		if err := e.sendTo(r.to, frame{Type: "want", Store: fs.name, Path: fr.Path, Blob: fr.Blob}); err != nil {
			e.logger.Debug("want", "peer", r.to, "error", err)
		}
	}
}

// openBlobFor finds the bytes of one version: the content store, the queue,
// the file at path, or any other file in the store holding the same content.
// blob "" means whatever the path holds now (a want from an older peer). It
// returns the open file and an entry describing the bytes.
func (e *Engine) openBlobFor(fs *StoreState, path, blob string) (*os.File, *Entry, error) {
	cur := fs.db.Get(path)
	if blob == "" {
		if cur == nil || cur.Deleted || cur.Dir || cur.Blob == "" {
			return nil, nil, os.ErrNotExist
		}
		f, err := e.openEntryBlob(fs, cur)
		return f, cur, err
	}
	if !store.ValidSum(blob) {
		return nil, nil, store.ErrInvalidSum
	}
	if cur != nil && !cur.Deleted && cur.Blob == blob {
		if f, err := e.openEntryBlob(fs, cur); err == nil {
			return f, cur, nil
		}
	}
	if e.store.Has(blob) {
		if f, err := e.store.Open(blob); err == nil {
			if info, err := f.Stat(); err == nil {
				return f, &Entry{Path: path, Blob: blob, Size: info.Size()}, nil
			}
			f.Close()
		}
	}
	if qp, ok := e.queuedBlob(fs, blob); ok {
		if f, err := os.Open(qp); err == nil {
			if info, err := f.Stat(); err == nil {
				return f, &Entry{Path: path, Blob: blob, Size: info.Size()}, nil
			}
			f.Close()
		}
	}
	// Renamed, or parked as a conflict copy, since the want went out.
	for _, ent := range fs.db.WithBlob(blob) {
		if ent.Path == path {
			continue
		}
		if f, err := e.openEntryBlob(fs, ent); err == nil {
			out := ent.clone()
			out.Path = path
			return f, out, nil
		}
	}
	return nil, nil, os.ErrNotExist
}

// --- the sending side --------------------------------------------------------

// Outgoing transfers run on their own goroutines, a few per peer, in two
// lanes. A want used to be served inline on the store's want worker: a 20 GiB
// video held every other want for that store - and every rename and verb -
// until the last byte left. Large files now share their own lane, and small
// ones always have slots of their own, so a big transfer slows the rest down
// but never stops it.
const (
	largeTransfer  = 16 << 20
	sendSmallSlots = 4
	sendLargeSlots = 2
)

type sendJob struct {
	key            string
	peer           string
	fs             *StoreState
	path, blob     string
	offset, length int64
	large          bool
	ctx            context.Context
	cancel         context.CancelFunc
}

// peerSends is one peer's transfer queue. Guarded by Engine.sendMu.
type peerSends struct {
	small, large   []*sendJob
	queued, active map[string]*sendJob
	nSmall, nLarge int
}

func sendKey(store, path, blob string) string { return store + "\x00" + path + "\x00" + blob }

// handleWant queues a transfer. It never reads the file itself: finding and
// verifying the bytes can mean hashing a large file, which belongs on the
// transfer's goroutine, not on the worker every want for the store shares.
func (e *Engine) handleWant(peer string, fr *frame) {
	fs := e.byName[fr.Store]
	if fs == nil || !validRel(fr.Path) || !e.storeSharedWith(fs, peer) {
		return
	}
	if fr.Blob != "" && !store.ValidSum(fr.Blob) {
		return
	}
	size := int64(-1)
	if cur := fs.db.Get(fr.Path); cur != nil && !cur.Deleted && (fr.Blob == "" || cur.Blob == fr.Blob) {
		size = cur.Size
	} else if fr.Blob != "" {
		if s, ok := e.storedSize(fr.Blob); ok {
			size = s
		}
	} else {
		return // an older peer asking for a path this side no longer has
	}
	if fr.Length > 0 {
		size = fr.Length
	}
	ctx, cancel := context.WithCancel(context.Background())
	j := &sendJob{
		key:    sendKey(fs.name, fr.Path, fr.Blob),
		peer:   peer,
		fs:     fs,
		path:   fr.Path,
		blob:   fr.Blob,
		offset: fr.Offset,
		length: fr.Length,
		large:  size < 0 || size > largeTransfer,
		ctx:    ctx,
		cancel: cancel,
	}
	e.sendMu.Lock()
	if e.sendsClosed {
		e.sendMu.Unlock()
		cancel()
		return
	}
	ps := e.sendq[peer]
	if ps == nil {
		ps = &peerSends{queued: map[string]*sendJob{}, active: map[string]*sendJob{}}
		e.sendq[peer] = ps
	}
	if ps.queued[j.key] != nil {
		// Already waiting for a slot; the repeat is a reader that has not
		// seen a byte yet, and it will.
		e.sendMu.Unlock()
		cancel()
		return
	}
	if old := ps.active[j.key]; old != nil {
		// The reader asked again for a transfer that is running: it stopped
		// seeing chunks (a dropped link ate them). Start over, first in line,
		// so it is not queued behind everything the restart displaced.
		old.cancel()
		ps.queued[j.key] = j
		if j.large {
			ps.large = append([]*sendJob{j}, ps.large...)
		} else {
			ps.small = append([]*sendJob{j}, ps.small...)
		}
	} else {
		ps.queued[j.key] = j
		if j.large {
			ps.large = append(ps.large, j)
		} else {
			ps.small = append(ps.small, j)
		}
	}
	e.startSendsLocked(peer, ps)
	e.sendMu.Unlock()
}

// startSendsLocked starts whatever the free slots allow, in queue order.
// Callers hold sendMu. Nothing starts once Close has begun: sendWG.Add must
// never race the Wait.
func (e *Engine) startSendsLocked(peer string, ps *peerSends) {
	if e.sendsClosed {
		return
	}
	start := func(j *sendJob) {
		delete(ps.queued, j.key)
		ps.active[j.key] = j
		e.sendWG.Add(1)
		go func() {
			defer e.sendWG.Done()
			e.runSend(j)
			e.sendMu.Lock()
			if ps.active[j.key] == j {
				delete(ps.active, j.key)
			}
			if j.large {
				ps.nLarge--
			} else {
				ps.nSmall--
			}
			e.startSendsLocked(peer, ps)
			e.sendMu.Unlock()
		}()
	}
	fill := func(lane []*sendJob, n *int, slots int) []*sendJob {
		kept := lane[:0]
		for _, j := range lane {
			if *n >= slots || ps.active[j.key] != nil {
				// No slot, or a cancelled run of the same transfer is still
				// winding down: two runs interleaved would garble its
				// chunks. Its end frees the way and calls back here.
				kept = append(kept, j)
				continue
			}
			*n++
			start(j)
		}
		return kept
	}
	ps.small = fill(ps.small, &ps.nSmall, sendSmallSlots)
	ps.large = fill(ps.large, &ps.nLarge, sendLargeSlots)
}

// cancelSends stops every transfer, queued or running, and refuses new ones:
// it is Close's, and after it sendWG only goes down.
func (e *Engine) cancelSends() {
	e.sendMu.Lock()
	e.sendsClosed = true
	for _, ps := range e.sendq {
		ps.cancelLocked()
	}
	e.sendMu.Unlock()
}

// cancelPeerSends stops one peer's transfers, queued or running, when its
// link goes or is replaced. A running one would otherwise carry on over the
// next link, missing the chunks the old one lost: the receiver abandons a
// transfer at the first gap and silently drops the rest - gigabytes of it for
// a large file. The peer asks again on the new link.
func (e *Engine) cancelPeerSends(peer string) {
	e.sendMu.Lock()
	if ps := e.sendq[peer]; ps != nil {
		ps.cancelLocked()
	}
	e.sendMu.Unlock()
}

// cancelLocked cancels every job. A running one stays in active until it
// winds down, so a repeat of it waits rather than interleaving. Callers hold
// Engine.sendMu.
func (ps *peerSends) cancelLocked() {
	for _, j := range ps.active {
		j.cancel()
	}
	for _, j := range ps.queued {
		j.cancel()
	}
	ps.small, ps.large = nil, nil
	ps.queued = map[string]*sendJob{}
}

// senderFor is the peer's sender right now, or nil.
func (e *Engine) senderFor(peer string) transport.Sender {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.sends[peer]
}

// sendOn writes one frame to a given link: bulk for blob chunks, priority for
// everything else.
func sendOn(send transport.Sender, fr frame, bulk bool) error {
	b, err := json.Marshal(fr)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	if bulk {
		return send.SendBulk(b)
	}
	return send.Send(b)
}

// sendingCount is the number of transfers running or queued, per peer.
func (e *Engine) sendingCount() (active, queued int) {
	e.sendMu.Lock()
	defer e.sendMu.Unlock()
	for _, ps := range e.sendq {
		active += len(ps.active)
		queued += len(ps.queued)
	}
	return active, queued
}

func (e *Engine) runSend(j *sendJob) {
	if j.ctx.Err() != nil {
		return
	}
	// The whole transfer goes out on one link. Looking the sender up per
	// chunk moved a running transfer onto a replacement link mid-file, with
	// whatever the dead one swallowed missing from the middle.
	send := e.senderFor(j.peer)
	if send == nil {
		return
	}
	fs := j.fs
	blob, ent, err := e.openBlobFor(fs, j.path, j.blob)
	if err != nil {
		e.logger.Info("no readable copy for want", "store", fs.name, "path", j.path, "blob", j.blob, "peer", j.peer, "error", err)
		if j.blob != "" {
			_ = sendOn(send, frame{Type: "wantErr", Store: fs.name, Path: j.path, Blob: j.blob, Code: wantMissing, Error: "no copy here"}, false)
		}
		return
	}
	defer blob.Close()

	offset, length := j.offset, j.length
	if offset < 0 || length < 0 || offset > ent.Size || (length > 0 && offset+length > ent.Size) {
		e.logger.Warn("ranged want out of bounds", "store", fs.name, "path", j.path, "offset", offset, "length", length, "size", ent.Size)
		return
	}
	if length == 0 {
		length = ent.Size - offset
	}
	// A section reader seeks; discarding the prefix cost the whole offset.
	src := io.NewSectionReader(blob, offset, length)
	buf := make([]byte, store.ChunkSize)
	seq := 0
	for {
		if j.ctx.Err() != nil {
			return
		}
		n, rerr := src.Read(buf)
		if n > 0 {
			// Bulk: index frames for every store overtake a transfer in
			// progress instead of waiting behind gigabytes of video.
			if err := e.sendBlobFrame(j.ctx, send, frame{Type: "blob", Store: fs.name, Path: j.path, Blob: ent.Blob, Seq: seq, Data: buf[:n]}, true); err != nil {
				return
			}
			seq++
		}
		if rerr == io.EOF {
			// A zero-byte blob yields no data chunk at all, so the receiver
			// never opened a transfer and dropped the blobEnd on the floor.
			// Send the empty first chunk to open the transfer.
			if seq == 0 {
				if err := e.sendBlobFrame(j.ctx, send, frame{Type: "blob", Store: fs.name, Path: j.path, Blob: ent.Blob, Seq: 0}, true); err != nil {
					return
				}
			}
			break
		}
		if rerr != nil {
			e.logger.Warn("read for transfer", "store", fs.name, "path", j.path, "error", rerr)
			return
		}
	}
	if j.ctx.Err() != nil {
		return
	}
	if err := e.sendBlobFrame(j.ctx, send, frame{Type: "blobEnd", Store: fs.name, Path: j.path, Blob: ent.Blob, Entry: ent, Offset: j.offset, Length: j.length}, false); err != nil {
		e.logger.Debug("blobEnd", "peer", j.peer, "error", err)
	}
}

// --- the receiving side ------------------------------------------------------

// blobShards is how many workers take incoming chunks. A transfer's chunks
// always land on the same worker, in order; different transfers spread out,
// so one slow disk write does not hold every other file arriving.
const blobShards = 4

// finishSlots bounds the completed transfers being verified and applied at
// once. Finishing runs off the chunk workers: it fsyncs the file and writes
// it into the tree, and a chunk worker stuck doing that for a 20 GiB file
// backed the link up until the sender's write deadline dropped it.
const finishSlots = 4

// finishBacklog bounds the completed transfers waiting for a finish slot. The
// chunk worker used to wait for the slot itself, so four slow applies (each
// behind a long scan's lock) stopped every transfer on the shard and, behind
// it, the link's reads. Waiting transfers hold a file descriptor each, so
// they are bounded too; past this the chunk worker waits after all.
const finishBacklog = 256

// restoreWants puts back wants a finished transfer could not satisfy (the
// bytes failed verification, could not be stored), due to be asked again at
// the next exchange. A want registered since, or a newer version of the path
// wanted since, takes precedence and the old one stays gone.
func (e *Engine) restoreWants(fs *StoreState, path string, wants []*pending) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, p := range wants {
		if e.wantLocked(fs.name, path, p.slot()) != nil {
			continue
		}
		if !p.conflict && p.entry != nil {
			superseded := false
			for _, other := range e.wants[wantKey(fs.name, path)] {
				if !other.conflict && other.entry != nil && other.entry.Blob != p.entry.Blob {
					superseded = true
				}
			}
			if superseded {
				continue
			}
		}
		p.asked = time.Time{}
		e.setWantLocked(fs.name, path, p)
	}
}

// resetWantsLocked makes the wants for one version of a path due again
// without removing them. Callers hold e.mu.
func (e *Engine) resetWantsLocked(store, path, blob string) {
	for _, conflict := range []bool{false, true} {
		if p := e.wantLocked(store, path, wantSlot(blob, conflict)); p != nil {
			p.asked = time.Time{}
		}
	}
}

func blobShard(peer string, fr *frame) int {
	var h uint32 = 2166136261
	for _, s := range []string{peer, fr.Store, fr.Path, fr.Blob} {
		for i := 0; i < len(s); i++ {
			h ^= uint32(s[i])
			h *= 16777619
		}
		h *= 16777619 // separator: ("ab","c") and ("a","bc") differ
	}
	return int(h % blobShards)
}

// lockBlob serialises finishing transfers of one blob. Two can be in flight
// at once - the same bytes for two paths, or a conflict copy beside its
// update - and both adopt into, then move out of, the same store path: the
// second one found nothing there.
func (e *Engine) lockBlob(sum string) (unlock func()) {
	e.blobLockMu.Lock()
	l := e.blobLocks[sum]
	if l == nil {
		l = &blobLock{}
		e.blobLocks[sum] = l
	}
	l.refs++
	e.blobLockMu.Unlock()
	l.mu.Lock()
	return func() {
		l.mu.Unlock()
		e.blobLockMu.Lock()
		if l.refs--; l.refs == 0 {
			delete(e.blobLocks, sum)
		}
		e.blobLockMu.Unlock()
	}
}

type blobLock struct {
	mu   sync.Mutex
	refs int
}
