package syncer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"sort"
	"syscall"
	"time"

	"codeberg.org/kyleraykbs/storeboard/internal/config"
	"codeberg.org/kyleraykbs/storeboard/internal/streamfs"
)

// readTimeout is how long a ranged read waits on a holder that has gone
// quiet - sent nothing at all - before giving up, and how long a read waits
// for any holder to be connected. Without it a peer that stops answering (a
// link replaced by the cross-dial tie-break, a holder that hung) leaves the
// caller's read blocked forever. It is not a bound on a slow transfer: that
// used to fail reads on a busy slow link, half way through a photo. Tests
// shorten it.
var readTimeout = 20 * time.Second

// maxReadChunk bounds one ranged read. A FUSE read is at most a few hundred
// kilobytes, so this only stops a caller from asking one frame to carry a
// whole file.
const maxReadChunk = 1 << 20

// readReply is one answer to a ranged read.
type readReply struct {
	data []byte
	err  error
}

// holderRate is the throughput this machine measured against one holder. It
// is only ever evidence from real transfers: a peer that has never answered
// has no rate, and therefore no claim to be preferred.
type holderRate struct {
	bytes int64
	nanos int64
}

func (r holderRate) rate() float64 {
	if r.nanos <= 0 {
		return 0
	}
	return float64(r.bytes) / (float64(r.nanos) / float64(time.Second))
}

// streamFetcher serves one streamed store's reads. It is per store because a
// streamfs.Entry carries a path inside that store, not the store's name.
type streamFetcher struct {
	e  *Engine
	fs *StoreState
}

// holderRefusal is a holder's definite answer that it cannot serve a range:
// asking it again will not help, unlike a timeout or a dropped link.
type holderRefusal struct {
	peer string
	code string
	msg  string
}

func (r *holderRefusal) Error() string { return r.peer + ": " + r.msg }

// Refusal codes on readErr frames. A holder that predates them sends none.
const (
	refuseStale   = "stale"   // the holder does not have the version the reader opened
	refuseMissing = "missing" // the holder has no such entry
)

// ReadRange retries a failed range after a holder reconnects. The peer link
// can briefly drop while the transport redials; returning a short read or
// giving up after one connection turns that transient into a truncated photo.
// When every reachable peer refuses outright there is nothing to wait for,
// and the reader gets an answer at once rather than after the deadline.
func (f *streamFetcher) ReadRange(ctx context.Context, entry streamfs.Entry, offset, length int64) ([]byte, error) {
	if entry.Dir || entry.Blob == "" {
		return nil, fmt.Errorf("%s: no content to read", entry.Path)
	}
	if length <= 0 {
		return nil, nil
	}
	if length > maxReadChunk {
		length = maxReadChunk
	}
	// A version this machine wrote and has not handed over yet is only in
	// the queue: no holder has those bytes, and asking for them by path would
	// serve the previous version under the new size.
	if data, ok, err := f.e.readQueued(f.fs, entry.Blob, offset, length); ok {
		return data, err
	}
	data, err := f.readRange(ctx, entry, offset, length)
	if err != nil && ctx.Err() == nil {
		// Said once per failed range: a read that fails is a photo drawn
		// half way in someone's viewer, and it used to leave no trace.
		f.e.logger.Warn("streamed read failed", "store", f.fs.name, "path", entry.Path, "offset", offset, "length", length, "error", err)
	}
	return data, err
}

func (f *streamFetcher) readRange(ctx context.Context, entry streamfs.Entry, offset, length int64) ([]byte, error) {
	// The deadline bounds waiting for a holder to be there at all. A read
	// that reached one is bounded by the holder going quiet instead
	// (readStalled): a slow link is not a dead one.
	deadline := time.Now().Add(readTimeout)
	var firstErr error
	for {
		peers := f.e.holdersFor(f.fs)
		refused := 0
		var refusal *holderRefusal
		for _, peer := range peers {
			data, err := f.e.readRangeFrom(ctx, peer, f.fs, entry, offset, length)
			if err == nil && int64(len(data)) != length {
				err = fmt.Errorf("%s: holder %s returned %d of %d requested bytes", entry.Path, peer, len(data), length)
			}
			if err == nil {
				return data, nil
			}
			if errors.As(err, &refusal) {
				refused++
			}
			if firstErr == nil {
				firstErr = err
			}
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
		}
		if len(peers) > 0 && refused == len(peers) {
			switch refusal.code {
			case refuseStale:
				return nil, fmt.Errorf("%s: no holder has the version that was opened: %w", entry.Path, syscall.ESTALE)
			case refuseMissing:
				return nil, fmt.Errorf("%s: no holder has this file: %w", entry.Path, syscall.ENOENT)
			}
			return nil, fmt.Errorf("%s: %w", entry.Path, firstErr)
		}
		if !time.Now().Before(deadline) {
			if firstErr == nil {
				firstErr = fmt.Errorf("%s: no holder reachable for store %q", entry.Path, f.fs.name)
			}
			return nil, firstErr
		}
		// The link can be down for a short reconnect window. Retry the same
		// range against the new connection, bounded by the overall deadline.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-f.e.closed:
			return nil, errors.New("engine closed")
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// readQueued serves a range of a blob that sits in this store's offline
// queue. ok reports whether the queue held the blob at all.
func (e *Engine) readQueued(fs *StoreState, sum string, offset, length int64) ([]byte, bool, error) {
	path, ok := e.queuedBlob(fs, sum)
	if !ok {
		return nil, false, nil
	}
	f, err := os.Open(path)
	if err != nil {
		// Handed over and dropped between the lookup and the open: the
		// holder has it now.
		return nil, false, nil
	}
	defer f.Close()
	buf := make([]byte, length)
	n, err := f.ReadAt(buf, offset)
	if err != nil && err != io.EOF {
		return nil, true, err
	}
	return buf[:n], true, nil
}

// holdersFor orders the peers that could serve this store: the declared
// preference first, then measured throughput, then name. Connected peers in
// the store's group are the candidate set - the wire is the only proof a peer
// actually holds the bytes, and a failed read moves on to the next one.
func (e *Engine) holdersFor(fs *StoreState) []string {
	members := e.cfg.Groups[fs.group].Peers
	e.mu.Lock()
	out := make([]string, 0, len(e.sends))
	for peer := range e.sends {
		if peer != e.me && slices.Contains(members, peer) {
			out = append(out, peer)
		}
	}
	rates := make(map[string]float64, len(out))
	for _, peer := range out {
		rates[peer] = e.holderRates[peer].rate()
	}
	e.mu.Unlock()

	preferred := fs.preferred
	sort.SliceStable(out, func(i, j int) bool {
		if (out[i] == preferred) != (out[j] == preferred) {
			return out[i] == preferred
		}
		if rates[out[i]] != rates[out[j]] {
			return rates[out[i]] > rates[out[j]]
		}
		return out[i] < out[j]
	})
	return out
}

// noteRead records throughput evidence for one successful ranged read.
func (e *Engine) noteRead(peer string, n int, took time.Duration) {
	if n <= 0 || took <= 0 {
		return
	}
	e.mu.Lock()
	r := e.holderRates[peer]
	r.bytes += int64(n)
	r.nanos += took.Nanoseconds()
	e.holderRates[peer] = r
	e.mu.Unlock()
}

// readRangeFrom asks one peer for a window of one version and waits for its
// answer.
func (e *Engine) readRangeFrom(ctx context.Context, peer string, fs *StoreState, entry streamfs.Entry, offset, length int64) ([]byte, error) {
	gate := e.readGateFor(peer)
	if err := gate.acquire(ctx, func() bool { return streamfs.Urgent(ctx) }); err != nil {
		return nil, err
	}
	defer gate.release()

	ch := make(chan readReply, 1)
	e.mu.Lock()
	e.readSeq++
	id := e.readSeq
	e.reads[id] = ch
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		delete(e.reads, id)
		e.mu.Unlock()
	}()

	start := time.Now()
	fr := frame{Type: "read", ID: id, Store: fs.name, Path: entry.Path, Blob: entry.Blob, Offset: offset, Length: length}
	if err := e.sendTo(peer, fr); err != nil {
		return nil, err
	}
	tick := time.NewTicker(min(time.Second, max(readTimeout/4, time.Millisecond)))
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			e.cancelRead(peer, fs.name, id)
			return nil, ctx.Err()
		case <-e.closed:
			return nil, errors.New("engine closed")
		case <-tick.C:
			if e.readStalled(peer, start) {
				e.cancelRead(peer, fs.name, id)
				return nil, fmt.Errorf("%s: holder %s stopped answering", entry.Path, peer)
			}
		case r := <-ch:
			if r.err != nil {
				return nil, r.err
			}
			e.noteRead(peer, len(r.data), time.Since(start))
			return r.data, nil
		}
	}
}

// deliverRead hands a reply to the read that is waiting for it. An unknown id
// is a read this side already gave up on.
func (e *Engine) deliverRead(fr *frame) {
	e.mu.Lock()
	ch := e.reads[fr.ID]
	delete(e.reads, fr.ID)
	e.mu.Unlock()
	if ch == nil {
		return
	}
	if fr.Error != "" {
		ch <- readReply{err: &holderRefusal{peer: fr.Path, code: fr.Code, msg: fr.Error}}
		return
	}
	ch <- readReply{data: fr.Data}
}

// readWorkers bounds how many ranged reads one store serves at once. Serving
// them one at a time made every reader of a store - a thumbnailer opening a
// folder, a video seeking - wait behind every other.
const readWorkers = 8

// handleRead serves one ranged read from this machine's copy: the content
// store when it holds the blob, else the canonical file.
func (e *Engine) handleRead(peer string, fr *frame) {
	if e.readCancelled(peer, fr.ID) {
		return // the reader gave up while this waited its turn
	}
	fs := e.byName[fr.Store]
	if fs == nil || !validRel(fr.Path) || !e.storeSharedWith(fs, peer) {
		e.replyReadErr(peer, fr, "", "unknown store or unsafe path")
		return
	}
	ent := fs.db.Get(fr.Path)
	var blob *os.File
	if fr.Blob != "" && (ent == nil || ent.Deleted || ent.Blob != fr.Blob) {
		// The reader opened a version this machine has moved past, renamed,
		// or not reached yet. Serving today's bytes under that version's
		// size splices two files together inside one open; serve the version
		// it asked for from wherever it is kept, else say so.
		f, held, err := e.openBlobFor(fs, fr.Path, fr.Blob)
		if err != nil {
			if ent == nil || ent.Deleted {
				e.replyReadErr(peer, fr, refuseMissing, "no such entry")
			} else {
				e.replyReadErr(peer, fr, refuseStale, "version not held here")
			}
			return
		}
		blob, ent = f, held
	} else if ent == nil || ent.Deleted || ent.Dir {
		e.replyReadErr(peer, fr, refuseMissing, "no such entry")
		return
	}
	if fr.Offset < 0 || fr.Length <= 0 || fr.Offset > ent.Size {
		if blob != nil {
			blob.Close()
		}
		e.replyReadErr(peer, fr, "", "range out of bounds")
		return
	}
	length := min(fr.Length, ent.Size-fr.Offset, maxReadChunk)

	if blob == nil {
		f, err := e.openEntryBlob(fs, ent)
		if err != nil {
			e.replyReadErr(peer, fr, refuseMissing, "no readable copy here")
			return
		}
		blob = f
	}
	defer blob.Close()
	// ReadAt, not a skip: discarding the prefix made every range of a large
	// file cost its offset, so the end of a 20 GiB video read 20 GiB per
	// window.
	buf := make([]byte, length)
	n, err := blob.ReadAt(buf, fr.Offset)
	if int64(n) < length {
		e.replyReadErr(peer, fr, "", fmt.Sprintf("short holder file: read %d of %d bytes (%v)", n, length, err))
		return
	}
	if err := e.sendInteractiveTo(peer, frame{Type: "readData", ID: fr.ID, Store: fs.name, Path: fr.Path, Offset: fr.Offset, Data: buf}); err != nil {
		e.logger.Debug("read reply", "peer", peer, "store", fs.name, "path", fr.Path, "error", err)
	}
}

// storedSize reports the size of a blob the content store holds.
func (e *Engine) storedSize(sum string) (int64, bool) {
	p := e.store.BlobPath(sum)
	if p == "" {
		return 0, false
	}
	info, err := os.Stat(p)
	if err != nil || !info.Mode().IsRegular() {
		return 0, false
	}
	return info.Size(), true
}

func (e *Engine) replyReadErr(peer string, fr *frame, code, msg string) {
	if err := e.sendTo(peer, frame{Type: "readErr", ID: fr.ID, Store: fr.Store, Path: fr.Path, Code: code, Error: msg}); err != nil {
		e.logger.Debug("read error reply", "peer", peer, "error", err)
	}
}

// streamEntry is a journal entry as the mounted view sees it.
func streamEntry(ent *Entry) streamfs.Entry {
	out := streamfs.Entry{
		Path: ent.Path,
		Dir:  ent.Dir,
		Blob: ent.Blob,
		Size: ent.Size,
		Mode: ent.Mode,
	}
	if ent.MTime != 0 {
		out.MTime = time.Unix(0, ent.MTime)
	}
	return out
}

// observeTree keeps a streamed store's view in step with its journal, one
// path at a time. Rebuilding the whole view on every journal write made the
// first sync of a large store quadratic: each of a hundred thousand incoming
// entries rebuilt and diffed a hundred-thousand-entry tree.
func (fs *StoreState) observeTree() {
	fs.tree = streamfs.NewTree(nil)
	fs.db.Observe(func(ent *Entry) {
		if ent.Deleted {
			fs.tree.Delete(ent.Path)
			return
		}
		fs.tree.Set(streamEntry(ent))
	})
}

// mountStream mounts the store's canonical dir as the journal-backed view.
// A mount failure is not fatal: the journal is still the truth, and the
// daemon keeps syncing metadata while the paths stay plain directories. A
// mount that died underneath us - detached by hand, or its connection aborted
// - is noticed here and replaced, rather than leaving the store's folder empty
// until the daemon restarts.
func (e *Engine) mountStream(fs *StoreState) {
	if fs.mount != nil {
		if fs.mount.Alive() {
			return
		}
		e.logger.Warn("streamed mount went away; mounting it again", "store", fs.name, "path", fs.root)
		fs.mount = nil
		fs.mountUp.Store(false)
	}
	if fstype := mountFSType(fs.root); fstype != "" {
		if fstype != "fuse.storeboard" {
			e.logger.Warn("streamed store path is another filesystem's mount; leaving it alone",
				"store", fs.name, "path", fs.root, "fstype", fstype)
			return
		}
		// Our own mount, left behind by a daemon that died without releasing
		// it. Its server is gone, so no reader can use it: detach and mount
		// fresh rather than serving a dead filesystem forever.
		if err := syscall.Unmount(fs.root, syscall.MNT_DETACH); err != nil {
			e.logger.Error("detach a stale streamed mount", "store", fs.name, "path", fs.root, "error", err)
			return
		}
		e.logger.Info("detached a stale streamed mount", "store", fs.name, "path", fs.root)
	}
	if err := os.MkdirAll(fs.root, 0o755); err != nil {
		e.logger.Error("create streamed mountpoint", "store", fs.name, "path", fs.root, "error", err)
		return
	}
	mounted, err := streamfs.Mount(fs.root, fs.tree, streamfs.Options{
		Fetcher: &streamFetcher{e: e, fs: fs},
		Mutator: &storeMutator{e: e, fs: fs},
		UID:     uint32(max(fs.perm.uid, 0)),
		GID:     uint32(max(fs.perm.gid, 0)),
		Space:   func() (uint64, uint64) { return e.streamSpace(fs) },
	})
	if err != nil {
		e.logger.Error("mount streamed store; serving the journal only",
			"store", fs.name, "path", fs.root, "error", err)
		return
	}
	fs.mount = mounted
	fs.mountUp.Store(true)
	e.logger.Info("streamed store mounted", "store", fs.name, "path", fs.root, "entries", fs.db.LiveCount())
}

// streamSpace is what a write through the mount can use: the queue's
// filesystem, narrowed to what a capped queue has left. Writes land in the
// queue first, so that is the space that runs out.
func (e *Engine) streamSpace(fs *StoreState) (total, free uint64) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(e.dataDir, &st); err == nil {
		total = st.Blocks * uint64(st.Bsize)
		free = st.Bavail * uint64(st.Bsize)
	}
	switch e.offlinePolicy(fs) {
	case config.StreamQueueReadOnly:
		free = 0
	case config.StreamQueueCapped:
		if fs.offline != nil {
			fs.queueMu.Lock()
			left := max(fs.offline.MaxBytes-fs.queuedBytes, 0)
			fs.queueMu.Unlock()
			free = min(free, uint64(left))
			total = min(total, uint64(fs.offline.MaxBytes))
		}
	}
	return total, free
}

// unmountStream detaches a mounted store. Called once, at shutdown.
func (e *Engine) unmountStream(fs *StoreState) {
	fs.scanMu.Lock()
	defer fs.scanMu.Unlock()
	if fs.mount == nil {
		return
	}
	if err := fs.mount.Unmount(); err != nil {
		e.logger.Warn("unmount streamed store", "store", fs.name, "path", fs.root, "error", err)
	}
	fs.mount = nil
	fs.mountUp.Store(false)
}

// streamRootDir reports whether the canonical path is storeboard's own mount
// rather than a directory the user owns.
func (fs *StoreState) mounted() bool { return fs.mountUp.Load() }
