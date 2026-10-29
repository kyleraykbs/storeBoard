package syncer

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"codeberg.org/kyleraykbs/storeboard/internal/store"
)

type frame struct {
	Type  string `json:"type"`
	Store string `json:"store,omitempty"`
	Path  string `json:"path,omitempty"`
	// To is the destination of a rename queued on a streamed store.
	To      string   `json:"to,omitempty"`
	Seq     int      `json:"seq,omitempty"`
	Data    []byte   `json:"data,omitempty"`
	Entry   *Entry   `json:"entry,omitempty"`
	Entries []*Entry `json:"entries,omitempty"`
	// Offset and Length scope a read to a byte window. A ranged read lets a
	// streamed file seek without refetching a 20 GiB video from 0.
	Offset int64 `json:"offset,omitempty"`
	Length int64 `json:"length,omitempty"`
	// ID correlates a ranged read with its answer. Reads are concurrent and
	// multiplexed over the one connection, so the reply cannot rely on order.
	ID int64 `json:"id,omitempty"`
	// Error carries a holder's refusal for a read it could not serve.
	Error string `json:"error,omitempty"`
	// Action names a forwarded verb (versions, restore).
	Action string `json:"action,omitempty"`
}

// pending is a blob this side asked for: either a plain update to apply, or
// a conflict copy to park beside the winner.
type pending struct {
	entry        *Entry
	conflict     bool
	conflictName string
}

// incomingBlob is a transfer in progress. next is the chunk sequence this
// side expects: a sender that restarts a transfer begins again at 0, and
// appending its bytes onto the partial file would produce a blob that can
// never hash correctly, so the file is reset instead of extended.
type incomingBlob struct {
	store string
	f     *os.File
	entry *Entry
	next  int
}

func wantKey(store, path string) string { return store + "\x00" + path }
func incomeKey(peer, store, path string) string {
	return peer + "\x00" + store + "\x00" + path
}

// validRel rejects paths that could escape a store root.
func validRel(p string) bool {
	if p == "" || strings.HasPrefix(p, "/") || strings.Contains(p, "\x00") {
		return false
	}
	clean := filepath.Clean(p)
	return clean == p && clean != ".." && !strings.HasPrefix(clean, "../")
}

func (e *Engine) sendTo(peer string, fr frame) error {
	b, err := json.Marshal(fr)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	e.mu.Lock()
	send := e.sends[peer]
	e.mu.Unlock()
	if send == nil {
		return fmt.Errorf("peer %q not connected", peer)
	}
	return send.Send(b)
}

// sendBulkTo writes a blob chunk: it yields the socket to control frames
// between chunks so a long transfer cannot starve the rest of the mesh.
func (e *Engine) sendBulkTo(peer string, fr frame) error {
	b, err := json.Marshal(fr)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	e.mu.Lock()
	send := e.sends[peer]
	e.mu.Unlock()
	if send == nil {
		return fmt.Errorf("peer %q not connected", peer)
	}
	return send.SendBulk(b)
}

func (e *Engine) sendIndexTo(peer string, fs *StoreState, entries []*Entry) error {
	if len(entries) == 0 {
		return nil
	}
	return e.sendTo(peer, frame{Type: "index", Store: fs.name, Entries: entries})
}

// --- receiving -------------------------------------------------------------

// needsMaterialization reports whether the canonical file is missing or stale.
// The tree is the materialisation: a released blob does not mean the file is
// absent, and treating it that way made a replica re-fetch its own files from
// its peers on every index exchange.
func (e *Engine) needsMaterialization(fs *StoreState, ent *Entry) bool {
	if ent.Deleted || ent.Dir {
		return false
	}
	if fs.stream {
		// Nothing is ever materialised here, and the canonical path is our
		// own mount: statting it from the frame handlers would re-enter the
		// filesystem to ask a question the journal already answers.
		return false
	}
	path := filepath.Join(fs.root, ent.Path)
	info, err := os.Stat(path)
	return err != nil || !info.Mode().IsRegular() || info.Size() != ent.Size || info.ModTime().UnixNano() != ent.MTime
}

// --- receiving -------------------------------------------------------------
// handleIndex folds a peer's entries into the journal. The version-vector
// rules are the whole sync contract:
//
//	remote dominates  → fetch it
//	local dominates   → push ours
//	concurrent        → deterministic winner keeps the path, loser becomes a
//	                    conflict copy on both sides (never silently lost)
//	equal content     → merge vectors and move on
func (e *Engine) handleIndex(peer string, fr *frame) {
	fs := e.byName[fr.Store]
	if fs == nil {
		return // a store this machine does not carry
	}
	for _, rem := range fr.Entries {
		select {
		case <-e.closed:
			return
		default:
		}
		if rem == nil || !validRel(rem.Path) {
			continue
		}
		// Local is this machine's own knowledge; a peer's claim about it means
		// nothing and would let a remote entry masquerade as evidence that a
		// file existed here.
		rem = rem.clone()
		rem.Local = false
		loc := fs.db.Get(rem.Path)
		e.ackQueue(fs, rem)

		if rem.Dir {
			e.handleDirEntry(peer, fs, rem, loc)
			continue
		}
		if loc != nil && !loc.Deleted && loc.Dir {
			// A directory and a file cannot share a path. The directory wins,
			// deterministically, because entries may live beneath it that
			// cannot exist if the path becomes a file; a file, by contrast,
			// can be parked whole beside it. Both machines compute the same
			// winner, so the clash converges instead of ping-ponging.
			_ = e.sendIndexTo(peer, fs, []*Entry{loc})
			continue
		}

		switch {
		case rem.Deleted:
			if loc == nil || loc.Deleted {
				continue
			}
			if Dominates(rem.VV, loc.VV) {
				if err := e.applyDelete(fs, loc); err != nil {
					// The file is still on disk: tombstoning it now would
					// make the scanner resurrect it later, so keep the live
					// entry and push it back instead.
					e.logger.Warn("delete not applied; keeping the local file", "store", fs.name, "path", loc.Path, "error", err)
					_ = e.sendIndexTo(peer, fs, []*Entry{loc})
					continue
				}
				merged := MergeVV(loc.VV, rem.VV)
				_ = fs.db.Put(&Entry{Path: rem.Path, Deleted: true, VV: merged})
			} else {
				e.sendIndexTo(peer, fs, []*Entry{loc})
			}

		case loc == nil || loc.Deleted:
			// A tombstone is an ordinary entry for ordering purposes. A peer
			// that has not heard about the deletion yet keeps resending the
			// live entry; fetching it back resurrected the file the user had
			// just removed. If our tombstone dominates, say so instead.
			if loc != nil && loc.Deleted && Dominates(loc.VV, rem.VV) && !Dominates(rem.VV, loc.VV) {
				if err := e.sendIndexTo(peer, fs, []*Entry{loc}); err != nil {
					e.logger.Debug("push delete", "peer", peer, "error", err)
				}
				continue
			}
			e.request(peer, fs, rem, &pending{entry: rem})

		case Dominates(rem.VV, loc.VV) && !Dominates(loc.VV, rem.VV):
			e.request(peer, fs, rem, &pending{entry: rem})

		case Dominates(loc.VV, rem.VV) && !Dominates(rem.VV, loc.VV):
			if err := e.sendIndexTo(peer, fs, []*Entry{loc}); err != nil {
				e.logger.Debug("push", "peer", peer, "error", err)
			}

		default:
			// Equal journal entries can still be unmaterialised locally: an
			// index may have arrived before its blob, or a previous collector
			// may have removed content while the journal remained. Status counts
			// filesystem entries, so request the bytes whenever the canonical
			// file or content-store blob is absent/stale.
			if loc.Blob == rem.Blob {
				if e.needsMaterialization(fs, rem) {
					if !fs.streamed {
						// A replica owns its tree. Judge the entry by what is on
						// disk, not by whether the content store still holds a
						// copy: a replica releases that copy once the file is
						// materialised, so its presence says nothing.
						if !e.pathMissing(fs, rem.Path) {
							// The file holds different bytes than the journal:
							// a local edit the next scan will record. Fetching
							// would park it as a conflict and put the old
							// content back.
							continue
						}
						if loc.Local {
							// It was here and is gone: a deletion, which the
							// peer has not heard about yet. Fetching it back
							// resurrected files the user had just removed.
							fs.scanMu.Lock()
							tomb := e.tombstoneEntry(fs, loc)
							if tomb != nil {
								e.pruneEmptyParents(fs, rem.Path)
							}
							fs.scanMu.Unlock()
							if tomb != nil {
								e.broadcastIndex(fs, []*Entry{tomb})
							}
							continue
						}
					}
					e.request(peer, fs, rem, &pending{entry: rem})
					continue
				}
				merged := MergeVV(loc.VV, rem.VV)
				upd := loc.clone()
				upd.VV = merged
				_ = fs.db.Put(upd)
				continue
			}
			e.resolveConflict(peer, fs, loc, rem)
		}
	}
}

// handleDirEntry folds a peer's directory entry into the journal. Directories
// carry no bytes, so none of the blob machinery applies: the entry's existence
// is the content, and the version vector decides who is right.
func (e *Engine) handleDirEntry(peer string, fs *StoreState, rem, loc *Entry) {
	switch {
	case rem.Deleted:
		if loc == nil || loc.Deleted {
			return
		}
		if Dominates(rem.VV, loc.VV) {
			if err := e.applyDelete(fs, loc); err != nil {
				e.logger.Warn("directory delete not applied", "store", fs.name, "path", loc.Path, "error", err)
				_ = e.sendIndexTo(peer, fs, []*Entry{loc})
				return
			}
			merged := MergeVV(loc.VV, rem.VV)
			_ = fs.db.Put(&Entry{Path: rem.Path, Dir: true, Deleted: true, VV: merged})
			return
		}
		_ = e.sendIndexTo(peer, fs, []*Entry{loc})

	case loc == nil || loc.Deleted:
		if loc != nil && loc.Deleted && Dominates(loc.VV, rem.VV) && !Dominates(rem.VV, loc.VV) {
			// Our removal dominates a peer's stale directory entry.
			_ = e.sendIndexTo(peer, fs, []*Entry{loc})
			return
		}
		if err := e.applyEntry(fs, rem); err != nil {
			e.logger.Warn("create directory", "store", fs.name, "path", rem.Path, "error", err)
		}

	case !loc.Dir:
		// A file holds this path here. The directory wins, and the file is
		// parked under its conflict name so its bytes survive; that is the
		// same rule every peer applies, so the clash cannot ping-pong.
		if copyEnt := e.saveLocalConflict(fs, loc); copyEnt != nil {
			e.broadcastIndex(fs, []*Entry{copyEnt})
		}
		if err := e.applyEntry(fs, rem); err != nil {
			e.logger.Warn("replace file with directory", "store", fs.name, "path", rem.Path, "error", err)
		}

	case Dominates(rem.VV, loc.VV) && !Dominates(loc.VV, rem.VV):
		if err := e.applyEntry(fs, rem); err != nil {
			e.logger.Warn("create directory", "store", fs.name, "path", rem.Path, "error", err)
		}

	case Dominates(loc.VV, rem.VV) && !Dominates(rem.VV, loc.VV):
		_ = e.sendIndexTo(peer, fs, []*Entry{loc})

	default:
		// Same version: the directory exists on both sides, so only a missing
		// one needs recreating (a mount that came back empty, say).
		if info, err := os.Stat(filepath.Join(fs.root, rem.Path)); err != nil || !info.IsDir() {
			if err := e.applyEntry(fs, rem); err != nil {
				e.logger.Warn("create directory", "store", fs.name, "path", rem.Path, "error", err)
			}
		}
		merged := MergeVV(loc.VV, rem.VV)
		upd := loc.clone()
		upd.VV = merged
		_ = fs.db.Put(upd)
	}
}

// resolveConflict keeps exactly one of the two contents at the path (both
// sides compute the same winner) and preserves the other as a
// `.conflict-<hash8>` copy, indexed like any other file so it spreads.
func (e *Engine) resolveConflict(peer string, fs *StoreState, loc, rem *Entry) {
	fs.addConflict()
	win := winner(loc, rem)
	merged := MergeVV(loc.VV, rem.VV)

	if win == loc {
		// Ours stays; fetch theirs as a conflict copy.
		name := conflictPath(rem.Path, rem.Blob)
		e.request(peer, fs, rem, &pending{entry: rem, conflict: true, conflictName: name})
		upd := loc.clone()
		upd.VV = merged
		_ = fs.db.Put(upd)
		e.broadcastIndex(fs, []*Entry{upd})
		e.logger.Warn("conflict: kept local version, remote saved as copy",
			"store", fs.name, "path", loc.Path, "copy", name, "peer", peer)
		return
	}

	// Theirs wins: park a copy of ours, then fetch theirs with a merged
	// vector so the pair cannot re-conflict.
	if copyEnt := e.saveLocalConflict(fs, loc); copyEnt != nil {
		e.broadcastIndex(fs, []*Entry{copyEnt})
	}
	winnerEntry := rem.clone()
	winnerEntry.VV = merged
	e.request(peer, fs, rem, &pending{entry: winnerEntry})
	e.logger.Warn("conflict: kept remote version, local saved as copy",
		"store", fs.name, "path", loc.Path, "peer", peer)
}

// saveLocalConflict moves the current local content aside under a
// deterministic conflict name and indexes it as a new file.
func (e *Engine) saveLocalConflict(fs *StoreState, loc *Entry) *Entry {
	fs.scanMu.Lock()
	defer fs.scanMu.Unlock()
	return e.saveLocalConflictLocked(fs, loc)
}

// saveLocalConflictLocked is saveLocalConflict for callers already holding
// scanMu (applyEntry, which parks a file that a directory is about to replace).
func (e *Engine) saveLocalConflictLocked(fs *StoreState, loc *Entry) *Entry {
	if fs.stream {
		// There is no local file to park: the bytes live on the holder, and
		// the loser stays reachable through its entry.
		return nil
	}
	src := filepath.Join(fs.root, loc.Path)
	sum, err := store.SumFile(src)
	if err != nil {
		return nil
	}
	if !e.store.Has(sum) {
		stored, aerr := e.adoptFile(src, sum)
		if aerr != nil {
			return nil
		}
		sum = stored
	}
	name := conflictPath(loc.Path, sum)
	dst := filepath.Join(fs.root, name)
	if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
		return nil
	}
	info, serr := os.Stat(src)
	if serr != nil {
		return nil
	}
	if err := os.Rename(src, dst); err != nil {
		return nil
	}
	e.chownPath(fs, dst)
	ent := &Entry{
		Path:  name,
		Blob:  sum,
		Size:  info.Size(),
		MTime: info.ModTime().UnixNano(),
		Mode:  uint32(info.Mode().Perm()),
		VV:    map[string]uint64{e.me: fs.db.Next()},
		Local: true,
	}
	_ = fs.db.Put(ent)
	return ent
}

// openEntryBlob opens the bytes of an entry, preferring the content store and
// falling back to the canonical file, which is hashed first so a peer never
// receives bytes that do not match the entry it asked for.
func (e *Engine) openEntryBlob(fs *StoreState, ent *Entry) (io.ReadCloser, error) {
	if ent.Blob != "" && e.store.Has(ent.Blob) {
		return e.store.Open(ent.Blob)
	}
	// A write this machine made but has not handed to a holder yet lives in
	// the queue: that is the only copy, so a holder pulling it reads here.
	if path, ok := e.queuedBlob(fs, ent.Blob); ok {
		return os.Open(path)
	}
	if fs.stream {
		// This store's canonical path is our own mount: statting it from
		// here would re-enter the filesystem. Bytes exist only in the blob
		// store or the queue, and neither had them.
		return nil, os.ErrNotExist
	}
	path := filepath.Join(fs.root, ent.Path)
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%s is not a regular file", ent.Path)
	}
	sum, err := store.SumFile(path)
	if err != nil {
		return nil, err
	}
	if sum != ent.Blob || info.Size() != ent.Size {
		return nil, fmt.Errorf("%s does not match the entry (%s)", ent.Path, sum)
	}
	return os.Open(path)
}

// wantsBlob reports whether the content store must keep a copy of a file: a
// kept history version needs those bytes, and a streamed store has no
// authoritative tree of its own. A plain replica does not — its canonical file
// is the copy, and the blob is redundant.
func (e *Engine) wantsBlob(fs *StoreState, path, sum string) bool {
	// With history on, every version this machine records has to keep its
	// bytes, and the version being recorded right now is one of them.
	//
	// A streamed store is not special: the tree it materialises IS the copy,
	// and a blob beside it is doubling just the same. What differs is only
	// what absence means, which the scanner and the collector already handle
	// by keeping blobs for entries with no file on disk.
	return fs.hist != nil
}

// releaseBlob removes the content-store copy of a file that is materialised
// here and does not need the bytes kept for history. Half the point of a
// content-addressed store is sharing bytes between peers; keeping a second
// copy of every file on every replica is not sharing, it is doubling.
func (e *Engine) releaseBlob(fs *StoreState, ent *Entry) {
	if ent == nil || ent.Blob == "" || ent.Dir {
		return
	}
	if e.wantsBlob(fs, ent.Path, ent.Blob) {
		return
	}
	path := filepath.Join(fs.root, ent.Path)
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() != ent.Size || info.ModTime().UnixNano() != ent.MTime {
		return // no verifiable copy on disk: the blob is the only one
	}
	if err := e.store.Remove(ent.Blob); err != nil {
		e.logger.Warn("release blob", "store", fs.name, "path", ent.Path, "error", err)
	}
}

// pathMissing reports whether a store's canonical file is absent. Used to tell
// "the bytes never arrived" from "the file was deleted here".
func (e *Engine) pathMissing(fs *StoreState, path string) bool {
	_, err := os.Stat(filepath.Join(fs.root, path))
	return os.IsNotExist(err)
}

// request asks the peer for a blob and remembers what to do with it. A
// streamed store never fetches: it records the entry and reads ranges from a
// holder on demand, which is the whole point of the mount.
func (e *Engine) request(peer string, fs *StoreState, rem *Entry, p *pending) {
	if fs.stream {
		_ = e.applyEntry(fs, rem)
		return
	}
	e.mu.Lock()
	e.wants[wantKey(fs.name, rem.Path)] = p
	e.mu.Unlock()
	if err := e.sendTo(peer, frame{Type: "want", Store: fs.name, Path: rem.Path}); err != nil {
		e.logger.Debug("want", "peer", peer, "error", err)
	}
}

// requestRange asks the peer for a byte window of a blob. Offset and Length
// mirror what a FUSE read asks for, so seeking a streamed file refetches only
// the bytes the caller actually read.
func (e *Engine) requestRange(peer string, fs *StoreState, rem *Entry, p *pending, offset, length int64) {
	e.mu.Lock()
	e.wants[wantKey(fs.name, rem.Path)] = p
	e.mu.Unlock()
	if err := e.sendTo(peer, frame{Type: "want", Store: fs.name, Path: rem.Path, Offset: offset, Length: length}); err != nil {
		e.logger.Debug("want", "peer", peer, "error", err)
	}
}

// handleRename applies a rename queued on a streamed peer: the holder moves
// its own file and journals both paths, so every other peer converges on the
// same tree.
func (e *Engine) handleRename(peer string, fr *frame) {
	fs := e.byName[fr.Store]
	if fs == nil || !validRel(fr.Path) || !validRel(fr.To) {
		return
	}
	if fs.stream {
		// A streamed peer has no tree of its own to rename; the journal
		// records it locally already.
		return
	}
	fs.scanMu.Lock()
	defer fs.scanMu.Unlock()
	ent := fs.db.Get(fr.Path)
	if ent == nil || ent.Deleted {
		return
	}
	from := filepath.Join(fs.root, ent.Path)
	to := filepath.Join(fs.root, fr.To)
	if err := e.ensureDir(fs, filepath.Dir(to)); err != nil {
		e.logger.Warn("rename: create parent", "store", fs.name, "path", fr.To, "error", err)
		return
	}
	if err := os.Rename(from, to); err != nil {
		e.logger.Warn("rename on holder", "store", fs.name, "path", fr.Path, "to", fr.To, "error", err)
		return
	}
	e.chownPath(fs, to)

	vv := fr.Entry.VV
	if len(vv) == 0 {
		vv = map[string]uint64{e.me: fs.db.Next()}
	}
	old := &Entry{Path: ent.Path, Deleted: true, Dir: ent.Dir, VV: vv}
	moved := ent.clone()
	moved.Path = fr.To
	moved.VV = vv
	_ = fs.db.Put(old)
	_ = fs.db.Put(moved)
	e.broadcastIndex(fs, []*Entry{old, moved})
}

// --- serving ---------------------------------------------------------------

// handleWant streams a stored blob in chunks.
func (e *Engine) handleWant(peer string, fr *frame) {
	fs := e.byName[fr.Store]
	if fs == nil || !validRel(fr.Path) {
		return
	}
	ent := fs.db.Get(fr.Path)
	if ent == nil || ent.Deleted || ent.Blob == "" {
		return
	}
	blob, err := e.openEntryBlob(fs, ent)
	if err != nil {
		e.logger.Warn("no readable copy for want", "store", fs.name, "path", fr.Path, "blob", ent.Blob, "error", err)
		return
	}
	defer blob.Close()

	offset, length := fr.Offset, fr.Length
	if offset < 0 || length < 0 || offset > ent.Size || (length > 0 && offset+length > ent.Size) {
		e.logger.Warn("ranged want out of bounds", "store", fs.name, "path", fr.Path, "offset", offset, "length", length, "size", ent.Size)
		return
	}
	if _, err := io.CopyN(io.Discard, blob, offset); err != nil && err != io.EOF {
		return
	}
	if length > 0 {
		blob = struct {
			io.Reader
			io.Closer
		}{io.LimitReader(blob, length), blob}
	}

	buf := make([]byte, store.ChunkSize)
	seq := 0
	for {
		n, rerr := blob.Read(buf)
		if n > 0 {
			// Bulk: index frames for every store overtake a transfer in
			// progress instead of waiting behind gigabytes of video.
			if err := e.sendBulkTo(peer, frame{Type: "blob", Store: fs.name, Path: fr.Path, Seq: seq, Data: buf[:n]}); err != nil {
				return
			}
			seq++
		}
		if rerr == io.EOF {
			// A zero-byte blob yields no data chunk at all, so the receiver
			// never opened a transfer and dropped the blobEnd on the floor:
			// empty files, and the directories holding them, never
			// materialised. Send the empty first chunk to open the transfer.
			if seq == 0 {
				if err := e.sendBulkTo(peer, frame{Type: "blob", Store: fs.name, Path: fr.Path, Seq: 0}); err != nil {
					return
				}
			}
			break
		}
		if rerr != nil {
			return
		}
	}
	if err := e.sendTo(peer, frame{Type: "blobEnd", Store: fs.name, Path: fr.Path, Entry: ent, Offset: fr.Offset, Length: fr.Length}); err != nil {
		e.logger.Debug("blobEnd", "peer", peer, "error", err)
	}
}

// handleBlob collects incoming chunks into a temp file. Chunk sequence is
// honoured: seq 0 restarts the file (a re-sent transfer must not append onto
// the bytes of the abandoned one), and any other out-of-order chunk abandons
// the transfer rather than writing a blob that cannot hash correctly. Before
// this, a sender that reconnected mid-file made the receiver append a second
// copy, fail verification, re-request, and loop forever — 37 GB over the wire
// for a 105-entry store.
func (e *Engine) handleBlob(peer string, fr *frame) {
	fs := e.byName[fr.Store]
	if fs == nil || !validRel(fr.Path) {
		return
	}
	key := incomeKey(peer, fr.Store, fr.Path)

	e.mu.Lock()
	inc := e.incoming[key]
	if inc == nil {
		if fr.Seq != 0 {
			// Mid-stream chunk with no transfer open: the start was dropped,
			// so there is nothing this can validly extend.
			e.mu.Unlock()
			return
		}
		f, err := e.store.Temp()
		if err != nil {
			e.mu.Unlock()
			return
		}
		inc = &incomingBlob{store: fr.Store, f: f}
		e.incoming[key] = inc
	}
	e.mu.Unlock()

	if fr.Seq != inc.next {
		if fr.Seq != 0 {
			e.logger.Warn("out-of-order blob chunk; abandoning the transfer",
				"store", fs.name, "path", fr.Path, "peer", peer, "want", inc.next, "got", fr.Seq)
			e.dropIncoming(key, inc)
			return
		}
		// The sender started over: truncate rather than append.
		if err := inc.f.Truncate(0); err != nil {
			e.dropIncoming(key, inc)
			return
		}
		if _, err := inc.f.Seek(0, io.SeekStart); err != nil {
			e.dropIncoming(key, inc)
			return
		}
		inc.next = 0
	}

	if len(fr.Data) > 0 {
		if _, err := inc.f.Write(fr.Data); err != nil {
			e.dropIncoming(key, inc)
			return
		}
	}
	inc.next = fr.Seq + 1
}

// handleBlobEnd verifies, stores and applies a completed blob.
func (e *Engine) handleBlobEnd(peer string, fr *frame) {
	fs := e.byName[fr.Store]
	if fs == nil {
		return
	}
	key := incomeKey(peer, fr.Store, fr.Path)

	e.mu.Lock()
	inc := e.incoming[key]
	delete(e.incoming, key)
	want := e.wants[wantKey(fr.Store, fr.Path)]
	delete(e.wants, wantKey(fr.Store, fr.Path))
	e.mu.Unlock()
	if inc == nil {
		// No transfer was opened. That is legitimate only for a zero-byte
		// file whose sender never sent a chunk: apply it directly rather than
		// dropping the want and leaving an empty file missing forever.
		if ent := fr.Entry; ent != nil && !ent.Deleted && ent.Size == 0 && ent.Blob != "" {
			if err := e.ensureEmptyBlob(ent.Blob); err != nil {
				e.logger.Warn("empty blob", "store", fs.name, "path", ent.Path, "error", err)
				return
			}
			if err := e.applyEntry(fs, ent); err != nil {
				e.logger.Warn("apply empty entry", "store", fs.name, "path", ent.Path, "error", err)
			}
		}
		return
	}
	defer os.Remove(inc.f.Name())

	// The sender's entry describes the bytes that actually arrived; the
	// remembered want only says what to do with them. Trusting a stale want's
	// blob made every arrival fail verification against a hash the sender was
	// never transmitting, re-request, and loop. Verify against the sender,
	// then require the want (when there is one) to agree before applying.
	wantEntry := fr.Entry
	if wantEntry == nil || wantEntry.Blob == "" {
		inc.f.Close()
		return
	}
	if want != nil && want.entry != nil && want.entry.Blob != wantEntry.Blob {
		// A newer version superseded what this side asked for: keep the bytes
		// out of the journal and let the next index exchange request the
		// current one.
		e.logger.Info("blob superseded before it arrived; dropping it",
			"store", fs.name, "path", fr.Path, "peer", peer,
			"asked", want.entry.Blob, "got", wantEntry.Blob)
		inc.f.Close()
		return
	}

	if err := inc.f.Sync(); err != nil {
		inc.f.Close()
		return
	}
	if err := inc.f.Close(); err != nil {
		return
	}
	sum, err := store.SumFile(inc.f.Name())
	if err != nil || sum != wantEntry.Blob {
		e.logger.Warn("blob failed verification", "store", fs.name, "path", fr.Path, "want", wantEntry.Blob, "got", sum)
		return
	}
	if err := e.store.Adopt(inc.f.Name(), sum); err != nil {
		e.logger.Error("adopt blob", "error", err)
		return
	}

	// The blobs agree, so the want's entry carries the same content with the
	// merged version vector conflict resolution computed; prefer it so a
	// resolved pair cannot re-conflict on the next exchange.
	applyEnt := wantEntry
	if want != nil && want.entry != nil {
		applyEnt = want.entry
	}
	if want != nil && want.conflict {
		e.writeConflictCopy(fs, want, applyEnt)
		return
	}
	if err := e.applyEntry(fs, applyEnt); err != nil {
		e.logger.Warn("apply entry", "store", fs.name, "path", applyEnt.Path, "error", err)
		return
	}
	// Fan the applied version out at once. The peer that sent the bytes is
	// usually a streamed store waiting for proof the holder took them; it
	// retires its queue entry on that echo rather than on the next
	// half-minute resend.
	e.broadcastIndex(fs, []*Entry{applyEnt})
}

// ensureEmptyBlob stores the zero-byte blob under sum, verifying the name is
// really the hash of nothing before adopting it. It exists for peers that
// send no chunk at all for an empty file.
func (e *Engine) ensureEmptyBlob(sum string) error {
	if e.store.Has(sum) {
		return nil
	}
	f, err := e.store.Temp()
	if err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	defer os.Remove(f.Name())
	got, err := store.SumFile(f.Name())
	if err != nil {
		return err
	}
	if got != sum {
		return fmt.Errorf("sum %q is not the empty blob", sum)
	}
	return e.store.Adopt(f.Name(), sum)
}

func (e *Engine) dropIncoming(key string, inc *incomingBlob) {
	inc.f.Close()
	os.Remove(inc.f.Name())
	e.mu.Lock()
	delete(e.incoming, key)
	e.mu.Unlock()
}

// writeConflictCopy parks a fetched loser version under its conflict name.
func (e *Engine) writeConflictCopy(fs *StoreState, want *pending, ent *Entry) {
	if fs.stream {
		// The loser's bytes stay on the holder. Recording the copy as an
		// entry is enough: reading it fetches the range that is wanted.
		fs.queueMu.Lock()
		copyEnt := &Entry{
			Path:  want.conflictName,
			Blob:  ent.Blob,
			Size:  ent.Size,
			MTime: ent.MTime,
			Mode:  ent.Mode,
			VV:    map[string]uint64{e.me: fs.db.Next()},
		}
		_ = fs.db.Put(copyEnt)
		fs.queueMu.Unlock()
		e.refreshTree(fs)
		e.broadcastIndex(fs, []*Entry{copyEnt})
		return
	}
	fs.scanMu.Lock()
	defer fs.scanMu.Unlock()
	dst := filepath.Join(fs.root, want.conflictName)
	if err := e.writeBlobTo(fs, dst, ent.Blob, ent.Mode, ent.MTime); err != nil {
		e.logger.Warn("write conflict copy", "store", fs.name, "path", want.conflictName, "error", err)
		return
	}
	copyEnt := &Entry{
		Path:  want.conflictName,
		Blob:  ent.Blob,
		Size:  ent.Size,
		MTime: ent.MTime,
		Mode:  ent.Mode,
		VV:    map[string]uint64{e.me: fs.db.Next()},
		Local: true,
	}
	_ = fs.db.Put(copyEnt)
	e.broadcastIndex(fs, []*Entry{copyEnt})
}

// applyEntry writes a fetched version into the store and records it. If
// the file on disk has unsynced local changes (changed since the last
// scan), those are preserved as a conflict copy first — a remote update
// never destroys data the mesh has not seen.
func (e *Engine) applyEntry(fs *StoreState, ent *Entry) error {
	if !validRel(ent.Path) {
		return fmt.Errorf("unsafe path %q", ent.Path)
	}
	if fs.stream {
		// A streamed store keeps no bytes of its own: the entry is the
		// journal record, and the mounted view presents it. Writing here
		// would materialise a full copy of content the user asked this
		// machine not to hold.
		fs.scanMu.Lock()
		err := fs.db.Put(ent)
		fs.scanMu.Unlock()
		e.refreshTree(fs)
		return err
	}
	// Held across the write so a mount transition cannot swap the root out
	// from under a file in flight.
	fs.scanMu.Lock()
	defer fs.scanMu.Unlock()
	dst := filepath.Join(fs.root, ent.Path)

	if ent.Dir {
		// A directory carries no bytes: its existence is the whole entry.
		if info, err := os.Stat(dst); err == nil && !info.IsDir() {
			return fmt.Errorf("%s exists and is not a directory", ent.Path)
		}
		if err := e.ensureDir(fs, dst); err != nil {
			return err
		}
		return fs.db.Put(ent)
	}

	// A path that must become a directory can be blocked by a file entry from
	// another machine — the two machines disagree about the shape. The
	// directory wins (the same rule handleDirEntry applies), and the file is
	// parked so its bytes survive. Without this the child could never be
	// written and the clash would never converge.
	if err := e.unblockParents(fs, ent.Path); err != nil {
		return err
	}

	if info, err := os.Stat(dst); err == nil {
		loc := fs.db.Get(ent.Path)
		if loc == nil || loc.Size != info.Size() || loc.MTime != info.ModTime().UnixNano() {
			sum, herr := store.SumFile(dst)
			if herr == nil && sum != ent.Blob {
				if !e.store.Has(sum) {
					if stored, aerr := e.adoptFile(dst, sum); aerr == nil {
						sum = stored
					}
				}
				name := conflictPath(ent.Path, sum)
				parked := filepath.Join(fs.root, name)
				if rerr := os.Rename(dst, parked); rerr == nil {
					e.chownPath(fs, parked)
					parkEnt := &Entry{
						Path:  name,
						Blob:  sum,
						Size:  info.Size(),
						MTime: info.ModTime().UnixNano(),
						Mode:  uint32(info.Mode().Perm()),
						VV:    map[string]uint64{e.me: fs.db.Next()},
					}
					_ = fs.db.Put(parkEnt)
					e.broadcastIndex(fs, []*Entry{parkEnt})
					e.logger.Warn("unsynced local change preserved as conflict copy",
						"store", fs.name, "path", ent.Path, "copy", name)
				}
			}
		}
	}

	if err := e.writeBlobTo(fs, dst, ent.Blob, ent.Mode, ent.MTime); err != nil {
		return err
	}
	// The file is now here: from now on its absence means a deletion rather
	// than "the bytes never arrived".
	ent.Local = true
	_ = fs.db.Put(ent)
	e.record(fs, ent)
	// The tree holds these bytes; keeping the content-store copy as well
	// would store every file twice on this machine.
	e.releaseBlob(fs, ent)
	return nil
}

// unblockParents frees every ancestor of path that a peer recorded as a file,
// parking that file under its conflict name first. Callers hold scanMu.
func (e *Engine) unblockParents(fs *StoreState, path string) error {
	if fs.stream {
		// The tree is the journal, not directories on disk: nothing can be
		// blocked, and there is no file here to move aside.
		return nil
	}
	for dir := filepath.Dir(path); dir != "." && dir != string(filepath.Separator) && dir != ""; dir = filepath.Dir(dir) {
		loc := fs.db.Get(dir)
		if loc == nil || loc.Deleted || loc.Dir {
			continue
		}
		var parked string
		if copyEnt := e.saveLocalConflictLocked(fs, loc); copyEnt != nil {
			parked = copyEnt.Path
			e.broadcastIndex(fs, []*Entry{copyEnt})
		}
		if tomb := e.tombstoneEntry(fs, loc); tomb != nil {
			e.broadcastIndex(fs, []*Entry{tomb})
		}
		// Whatever was on disk is either parked or gone; make sure the path
		// can hold a directory now.
		_ = os.Remove(filepath.Join(fs.root, dir))
		e.logger.Info("file replaced by a directory", "store", fs.name, "path", dir, "parked", parked)
	}
	return nil
}

// writeBlobTo materialises a stored blob at dst: temp file, fsync, rename —
// then mode, mtime and owner are set (DESIGN invariant 3). The mtime is the
// entry's, not the write's: a materialised file must look exactly like the
// one it mirrors, both for the tools that sort by it (photos, videos) and
// for the scanner, whose fast path is "size and mtime unchanged".
func (e *Engine) writeBlobTo(fs *StoreState, dst, sum string, mode uint32, mtime int64) error {
	blob, err := e.store.Open(sum)
	if err != nil {
		return err
	}
	defer blob.Close()

	if err := e.ensureDir(fs, filepath.Dir(dst)); err != nil {
		return err
	}
	// Unique scratch name: two applies for the same path (a repeated want,
	// say) must not share a temp file.
	f, err := os.CreateTemp(filepath.Dir(dst), tmpPrefix+"*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	if _, err := io.Copy(f, blob); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, dst); err != nil {
		os.Remove(tmp)
		return err
	}
	perm := os.FileMode(mode)
	if perm == 0 {
		perm = 0o644
	}
	_ = os.Chmod(dst, perm.Perm())
	e.chownPath(fs, dst)
	if mtime != 0 {
		when := time.Unix(0, mtime)
		if err := os.Chtimes(dst, when, when); err != nil {
			e.logger.Warn("set mtime", "store", fs.name, "path", dst, "error", err)
		}
	}
	return nil
}

// applyDelete moves a locally present file to the trash (nothing is ever
// destroyed) before the tombstone is recorded by the caller. It reports an
// error when the file could not be moved, so the caller can keep the entry
// alive instead of tombstoning a file that is still on disk.
func (e *Engine) applyDelete(fs *StoreState, loc *Entry) error {
	if fs.stream {
		// A streamed store has no local copy to move to the trash, and its
		// canonical path is this daemon's own mount: touching it from here
		// would re-enter the filesystem while holding the scan lock and
		// deadlock against the mount's own handler.
		return nil
	}
	fs.scanMu.Lock()
	defer fs.scanMu.Unlock()
	src := filepath.Join(fs.root, loc.Path)
	info, err := os.Stat(src)
	if err != nil {
		return nil // already gone
	}
	if loc.Dir || info.IsDir() {
		// A directory is only removed when nothing is left in it. A peer that
		// still holds content under it keeps the tree; its own deletions
		// prune it in turn.
		if err := os.Remove(src); err != nil {
			if errors.Is(err, syscall.ENOTEMPTY) || errors.Is(err, syscall.EEXIST) || os.IsNotExist(err) {
				return nil
			}
			return err
		}
		e.logger.Info("removed directory", "store", fs.name, "path", loc.Path)
		e.pruneEmptyParents(fs, loc.Path)
		return nil
	}
	// A delete only removes the version the journal knows. If the file on disk
	// has moved on since the last scan, that is an edit the mesh has not seen,
	// and a tombstone that merely dominates the old version must not throw it
	// away: park it as a conflict copy first, exactly as applyEntry does for a
	// remote update. Without this, deleting on one machine while another edits
	// lost the edit whenever the delete won the race to the scanner.
	// Only compare when the journal actually recorded a version; an entry with
	// no size and no mtime describes nothing to compare against.
	recorded := loc.Size != 0 || loc.MTime != 0
	if recorded && (info.Size() != loc.Size || info.ModTime().UnixNano() != loc.MTime) {
		if copyEnt := e.saveLocalConflictLocked(fs, loc); copyEnt != nil {
			e.broadcastIndex(fs, []*Entry{copyEnt})
			// The rename moved the file; the path is free and the caller's
			// tombstone stands. Nothing left to trash.
			return nil
		}
	}
	trash := filepath.Join(e.dataDir, "trash", fs.name)
	dst := filepath.Join(trash, loc.Path)
	if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
		return err
	}
	if err := moveOrCopy(src, dst, info.Mode()); err != nil {
		return err
	}
	// Record the version that is leaving: a restore undoes exactly this.
	gone := loc.clone()
	gone.Deleted = true
	e.record(fs, gone)
	e.logger.Info("deleted (moved to trash)", "store", fs.name, "path", loc.Path)
	// The file has left this directory; if that emptied it, the directory
	// goes too — unless it is one the user asked to keep.
	e.pruneEmptyParents(fs, loc.Path)
	return nil
}

// moveOrCopy renames src onto dst, falling back to copy-then-remove when the
// rename cannot cross filesystems. The trash lives under the one data
// directory while store roots may sit on other mounts (a pool, an external
// disk); without the fallback a delete fails silently, the caller still
// records the tombstone, and the scanner then resurrects the file it was
// told to delete — a loop that never converges.
//
// A name already taken in the trash is not a failure: the trash holds
// content the store no longer has, and two deletions of the same path are
// ordinary. Refusing made applyDelete fail forever, so the entry was never
// tombstoned and the store retried the same doomed delete on every index
// exchange, starving everything behind it. The existing copy is parked under
// a numbered sibling and both are kept.
func moveOrCopy(src, dst string, mode os.FileMode) error {
	dst = freeTrashName(dst)
	if err := os.Rename(src, dst); err == nil {
		return nil
	} else if !errors.Is(err, syscall.EXDEV) {
		return err
	}
	if mode&os.ModeSymlink != 0 {
		// A copy that followed the link would turn it into its target's
		// contents; recreate the link itself.
		target, err := os.Readlink(src)
		if err != nil {
			return err
		}
		if err := os.Symlink(target, dst); err != nil {
			return err
		}
		return os.Remove(src)
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode.Perm())
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		os.Remove(dst)
		return err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		os.Remove(dst)
		return err
	}
	if err := out.Close(); err != nil {
		os.Remove(dst)
		return err
	}
	return os.Remove(src)
}

// freeTrashName returns dst, or the first unused numbered sibling when dst is
// taken. Deleting the same path twice must not be able to fail.
func freeTrashName(dst string) string {
	if _, err := os.Lstat(dst); err != nil {
		return dst
	}
	for i := 1; ; i++ {
		alt := dst + ".storeboard-deleted-" + strconv.Itoa(i)
		if _, err := os.Lstat(alt); err != nil {
			return alt
		}
	}
}
