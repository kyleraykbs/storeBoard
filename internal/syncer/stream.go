package syncer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"sort"
	"time"

	"codeberg.org/kyleraykbs/storeboard/internal/streamfs"
)

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

// ReadRange fetches a byte window from the fastest reachable holder, trying
// the next one when a holder fails mid-read. A store with no holder reachable
// reports ENOENT: the tree exists, the bytes do not.
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
	peers := f.e.holdersFor(f.fs)
	if len(peers) == 0 {
		return nil, fmt.Errorf("no holder reachable for store %q: %w", f.fs.name, os.ErrNotExist)
	}
	var firstErr error
	for _, peer := range peers {
		data, err := f.e.readRangeFrom(ctx, peer, f.fs, entry.Path, offset, length)
		if err == nil {
			return data, nil
		}
		if firstErr == nil {
			firstErr = err
		}
	}
	return nil, firstErr
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

// readRangeFrom asks one peer for a window and waits for its answer.
func (e *Engine) readRangeFrom(ctx context.Context, peer string, fs *StoreState, path string, offset, length int64) ([]byte, error) {
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
	if err := e.sendTo(peer, frame{Type: "read", ID: id, Store: fs.name, Path: path, Offset: offset, Length: length}); err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-e.closed:
		return nil, errors.New("engine closed")
	case r := <-ch:
		if r.err != nil {
			return nil, r.err
		}
		e.noteRead(peer, len(r.data), time.Since(start))
		return r.data, nil
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
		ch <- readReply{err: fmt.Errorf("%s: %s", fr.Path, fr.Error)}
		return
	}
	ch <- readReply{data: fr.Data}
}

// handleRead serves one ranged read from this machine's copy: the content
// store when it holds the blob, else the canonical file, hashed first so a
// peer never receives bytes that do not match the entry it asked for.
func (e *Engine) handleRead(peer string, fr *frame) {
	fs := e.byName[fr.Store]
	if fs == nil || !validRel(fr.Path) {
		e.replyReadErr(peer, fr, "unknown store or unsafe path")
		return
	}
	ent := fs.db.Get(fr.Path)
	if ent == nil || ent.Deleted || ent.Dir {
		e.replyReadErr(peer, fr, "no such entry")
		return
	}
	if fr.Offset < 0 || fr.Length <= 0 || fr.Offset > ent.Size {
		e.replyReadErr(peer, fr, "range out of bounds")
		return
	}
	length := fr.Length
	if rest := ent.Size - fr.Offset; length > rest {
		length = rest
	}
	if length > maxReadChunk {
		length = maxReadChunk
	}

	blob, err := e.openEntryBlob(fs, ent)
	if err != nil {
		e.replyReadErr(peer, fr, "no readable copy here")
		return
	}
	defer blob.Close()
	if fr.Offset > 0 {
		if _, err := io.CopyN(io.Discard, blob, fr.Offset); err != nil {
			e.replyReadErr(peer, fr, "cannot seek")
			return
		}
	}
	buf := make([]byte, length)
	n, err := io.ReadFull(blob, buf)
	if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
		e.replyReadErr(peer, fr, "read failed")
		return
	}
	if err := e.sendBulkTo(peer, frame{Type: "readData", ID: fr.ID, Store: fs.name, Path: fr.Path, Offset: fr.Offset, Data: buf[:n]}); err != nil {
		e.logger.Debug("read reply", "peer", peer, "store", fs.name, "path", fr.Path, "error", err)
	}
}

func (e *Engine) replyReadErr(peer string, fr *frame, msg string) {
	if err := e.sendTo(peer, frame{Type: "readErr", ID: fr.ID, Store: fr.Store, Path: fr.Path, Error: msg}); err != nil {
		e.logger.Debug("read error reply", "peer", peer, "error", err)
	}
}

// treeEntries is the journal as the mounted view sees it: live entries only,
// with the deletion records and byte-holding fields the filesystem needs.
func treeEntries(fs *StoreState) map[string]streamfs.Entry {
	out := map[string]streamfs.Entry{}
	for _, ent := range fs.db.All() {
		if ent.Deleted {
			continue
		}
		e := streamfs.Entry{
			Path: ent.Path,
			Dir:  ent.Dir,
			Blob: ent.Blob,
			Size: ent.Size,
			Mode: ent.Mode,
		}
		if ent.MTime != 0 {
			e.MTime = time.Unix(0, ent.MTime)
		}
		out[ent.Path] = e
	}
	return out
}

// refreshTree republishes the journal to the mounted view. It is a no-op for
// every store that is not mounted.
func (e *Engine) refreshTree(fs *StoreState) {
	if fs.mount == nil {
		return
	}
	fs.mount.Replace(treeEntries(fs))
}

// mountStream mounts the store's canonical dir as the journal-backed view.
// A mount failure is not fatal: the journal is still the truth, and the
// daemon keeps syncing metadata while the paths stay plain directories.
func (e *Engine) mountStream(fs *StoreState) {
	if fs.mount != nil {
		return
	}
	if fstype := mountFSType(fs.root); fstype != "" {
		e.logger.Warn("streamed store path is already a mount; leaving it alone",
			"store", fs.name, "path", fs.root, "fstype", fstype)
		return
	}
	if err := os.MkdirAll(fs.root, 0o755); err != nil {
		e.logger.Error("create streamed mountpoint", "store", fs.name, "path", fs.root, "error", err)
		return
	}
	mounted, err := streamfs.Mount(fs.root, streamfs.NewTree(treeEntries(fs)), streamfs.Options{
		Fetcher: &streamFetcher{e: e, fs: fs},
		Mutator: &storeMutator{e: e, fs: fs},
		UID:     uint32(max(fs.perm.uid, 0)),
		GID:     uint32(max(fs.perm.gid, 0)),
	})
	if err != nil {
		e.logger.Error("mount streamed store; serving the journal only",
			"store", fs.name, "path", fs.root, "error", err)
		return
	}
	fs.mount = mounted
	e.logger.Info("streamed store mounted", "store", fs.name, "path", fs.root)
}

// unmountStream detaches a mounted store. Called once, at shutdown.
func (e *Engine) unmountStream(fs *StoreState) {
	if fs.mount == nil {
		return
	}
	if err := fs.mount.Unmount(); err != nil {
		e.logger.Warn("unmount streamed store", "store", fs.name, "path", fs.root, "error", err)
	}
	fs.mount = nil
}

// streamRootDir reports whether the canonical path is storeboard's own mount
// rather than a directory the user owns.
func (fs *StoreState) mounted() bool { return fs.mount != nil }
