package syncer

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"codeberg.org/kyleraykbs/storeboard/internal/store"
	"codeberg.org/kyleraykbs/storeboard/internal/transport"
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
	// Error carries a holder's refusal for a read it could not serve, and
	// Code classifies it (see refuseStale) so the reader knows not to retry.
	Error string `json:"error,omitempty"`
	Code  string `json:"code,omitempty"`
	// Blob pins a ranged read to the version the reader opened.
	Blob string `json:"blob,omitempty"`
	// Action names a forwarded verb (versions, restore).
	Action string `json:"action,omitempty"`
	// Moves carries per-entry journal records for an atomic subtree rename.
	Moves []renameMove `json:"moves,omitempty"`
	// Bits, Sums and Buckets carry an index digest and the buckets a peer
	// wants entries for (see digest.go).
	Bits    int    `json:"bits,omitempty"`
	Sums    []byte `json:"sums,omitempty"`
	Buckets []int  `json:"buckets,omitempty"`
}

// pending is a blob this side asked for: either a plain update to apply, or
// a conflict copy to park beside the winner.
type pending struct {
	entry        *Entry
	conflict     bool
	conflictName string
	// asked is when the want went out; a want for the same version is not
	// repeated while it is fresh (see request).
	asked time.Time
	// peer is who was asked; tried is everyone asked so far, so a refusal
	// moves on to a peer that has not refused yet (see handleWantErr).
	peer  string
	tried []string
}

// wantRefresh is how long a want stands before the same version may be asked
// for again. Every index frame used to re-send it: with full indexes every
// 30 s, a transfer longer than that was restarted from byte zero, once per
// peer holding the file, and never finished.
const wantRefresh = 2 * time.Minute

// incomingBlob is a transfer in progress. next is the chunk sequence this
// side expects: a sender that restarts a transfer begins again at 0, and
// appending its bytes onto the partial file would produce a blob that can
// never hash correctly, so the file is reset instead of extended.
//
// The bytes are hashed as they arrive, so a finished transfer is verified
// without reading it back: re-hashing a 20 GiB file took minutes on the one
// worker every incoming chunk shared.
type incomingBlob struct {
	store string
	path  string
	blob  string    // as the sender named it; "" from an older sender
	last  time.Time // guarded by Engine.mu
	f     *os.File
	hash  hash.Hash
	next  int
}

func wantKey(store, path string) string { return store + "\x00" + path }
func incomeKey(peer, store, path, blob string) string {
	return peer + "\x00" + store + "\x00" + path + "\x00" + blob
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

// sendInteractiveTo sends a frame someone is waiting on - a streamed read's
// reply - ahead of bulk sync traffic where the link supports it.
func (e *Engine) sendInteractiveTo(peer string, fr frame) error {
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
	if is, ok := send.(transport.InteractiveSender); ok {
		return is.SendInteractive(b)
	}
	return send.SendBulk(b)
}

// indexChunk bounds the entries one index frame carries. A whole store's
// index used to be one line - tens of megabytes for a photo library - which a
// slow link could not write inside the 30 s write deadline; the half-written
// line then corrupted the frame after it.
const indexChunk = 2000

func (e *Engine) sendIndexTo(peer string, fs *StoreState, entries []*Entry) error {
	for len(entries) > 0 {
		n := min(len(entries), indexChunk)
		if err := e.sendTo(peer, frame{Type: "index", Store: fs.name, Entries: entries[:n]}); err != nil {
			return err
		}
		entries = entries[n:]
	}
	return nil
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
	if !e.storeSharedWith(fs, peer) {
		// Group membership is the authorization: a peer outside the store's
		// group has no say over its tree.
		return
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
		if !rem.Dir && !rem.Deleted && !store.ValidSum(rem.Blob) {
			// A live file names its bytes by their hash. Anything else is
			// not a version this machine can fetch, verify or serve.
			continue
		}
		// Local is this machine's own knowledge; a peer's claim about it means
		// nothing and would let a remote entry masquerade as evidence that a
		// file existed here.
		rem = rem.clone()
		rem.Local = false
		loc := fs.db.Get(rem.Path)
		e.ackQueue(peer, fs, rem)

		if rem.Dir {
			e.handleDirEntry(peer, fs, rem, loc)
			continue
		}
		if loc != nil && !loc.Deleted && loc.Dir && !rem.Deleted {
			// A directory and a file cannot share a path. The directory wins,
			// deterministically, because entries may live beneath it that
			// cannot exist if the path becomes a file; a file, by contrast,
			// can be parked whole beside it. Both machines compute the same
			// winner, so the clash converges instead of ping-ponging.
			//
			// A tombstone is not a file claiming the path: it is a deletion,
			// ordered by its vector like any other (below). Answering one
			// with the directory made a peer whose tombstone dominates it
			// answer with the tombstone, forever.
			_ = e.sendIndexTo(peer, fs, []*Entry{loc})
			continue
		}

		switch {
		case rem.Deleted:
			if loc == nil || loc.Deleted {
				e.foldTombstone(fs, rem, loc)
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
				_ = fs.db.PutIf(loc, &Entry{Path: rem.Path, Deleted: true, VV: merged})
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
			if loc != nil && loc.Deleted && !Dominates(rem.VV, loc.VV) {
				// An edit concurrent with a deletion: the edit wins, so
				// nothing is lost, and it carries the deletion's history.
				// Fetching it with the peer's own vector was refused on
				// arrival as not superseding the tombstone - on every
				// exchange, forever.
				win := rem.clone()
				win.VV = MergeVV(loc.VV, rem.VV)
				e.request(peer, fs, rem, &pending{entry: win})
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
					if !fs.stream {
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
						if loc.Local && fs.rootEmpty.Load() {
							// The root looks unmounted (see scan): this is
							// not a deletion, and the file is not wanted
							// back into a directory that is not the store.
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
				_ = fs.db.PutIf(loc, upd)
				continue
			}
			e.resolveConflict(peer, fs, loc, rem)
		}
	}
}

// foldTombstone records a peer's deletion of a path this side has no live
// entry for. There is nothing to delete, but the deletion still has to be
// remembered: it is what this side echoes back, and a streamed store's queued
// delete is acknowledged only by an entry that covers it. Ignoring it - the
// old rule - left every delete of a file the holder never received (an
// editor's temporary file, a test directory) queued on the writer forever.
// It also carries the deletion on to a third peer that still has the file.
func (e *Engine) foldTombstone(fs *StoreState, rem, loc *Entry) {
	if loc != nil && Dominates(loc.VV, rem.VV) {
		return // already known
	}
	var vv map[string]uint64
	if loc != nil {
		vv = loc.VV
	}
	vv = MergeVV(vv, rem.VV)
	if err := fs.db.PutIf(loc, &Entry{Path: rem.Path, Dir: rem.Dir, Deleted: true, VV: vv}); err != nil {
		e.logger.Debug("record tombstone", "store", fs.name, "path", rem.Path, "error", err)
	}
}

// handleDirEntry folds a peer's directory entry into the journal. Directories
// carry no bytes, so none of the blob machinery applies: the entry's existence
// is the content, and the version vector decides who is right.
func (e *Engine) handleDirEntry(peer string, fs *StoreState, rem, loc *Entry) {
	switch {
	case rem.Deleted:
		if loc == nil || loc.Deleted {
			e.foldTombstone(fs, rem, loc)
			return
		}
		if Dominates(rem.VV, loc.VV) {
			if err := e.applyDelete(fs, loc); err != nil {
				e.logger.Warn("directory delete not applied", "store", fs.name, "path", loc.Path, "error", err)
				_ = e.sendIndexTo(peer, fs, []*Entry{loc})
				return
			}
			merged := MergeVV(loc.VV, rem.VV)
			_ = fs.db.PutIf(loc, &Entry{Path: rem.Path, Dir: true, Deleted: true, VV: merged})
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
		copyEnt, err := e.saveLocalConflict(fs, loc)
		if err != nil {
			// The file is still at the path: replacing it would lose it.
			e.logger.Warn("file not parked; the directory waits", "store", fs.name, "path", rem.Path, "error", err)
			return
		}
		if copyEnt != nil {
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
		// one needs recreating (a mount that came back empty, say). A
		// streamed store's root is our own mount: statting it from the index
		// worker re-enters the filesystem, and the journal already answers.
		if fs.stream {
			// Nothing to recreate: the view is the journal.
		} else if info, err := os.Stat(filepath.Join(fs.root, rem.Path)); err != nil || !info.IsDir() {
			if err := e.applyEntry(fs, rem); err != nil {
				e.logger.Warn("create directory", "store", fs.name, "path", rem.Path, "error", err)
			}
		}
		merged := MergeVV(loc.VV, rem.VV)
		upd := loc.clone()
		upd.VV = merged
		_ = fs.db.PutIf(loc, upd)
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
		if fs.db.PutIf(loc, upd) != nil {
			return // the path moved on while this was decided
		}
		e.broadcastIndex(fs, []*Entry{upd})
		e.logger.Warn("conflict: kept local version, remote saved as copy",
			"store", fs.name, "path", loc.Path, "copy", name, "peer", peer)
		return
	}

	// Theirs wins: park a copy of ours, then fetch theirs with a merged
	// vector so the pair cannot re-conflict. The want is recorded before
	// the file moves aside: a scan in between would otherwise read the
	// parked file as a deletion (see awaitingUpdate), and it is sent after,
	// so the winner cannot land before ours is safe.
	winnerEntry := rem.clone()
	winnerEntry.VV = merged
	if fs.stream {
		e.request(peer, fs, rem, &pending{entry: winnerEntry})
		e.logger.Warn("conflict: kept remote version, local saved as copy",
			"store", fs.name, "path", loc.Path, "peer", peer)
		return
	}
	if !e.canFetch(fs, rem) {
		// No room for the winner: leave both as they are. Parking ours now
		// would leave the path empty until the bytes could arrive.
		return
	}
	send := e.registerWant(peer, fs, rem, &pending{entry: winnerEntry})
	copyEnt, perr := e.saveLocalConflict(fs, loc)
	if perr != nil {
		// Ours is still at the path and could not be moved aside: the winner
		// must not land on it. Drop the want; the next exchange tries again.
		e.mu.Lock()
		e.takeWantLocked(fs.name, rem.Path, wantSlot(rem.Blob, false))
		e.mu.Unlock()
		e.logger.Warn("conflict: local version could not be parked; not applying the remote one yet",
			"store", fs.name, "path", loc.Path, "peer", peer, "error", perr)
		return
	}
	if copyEnt != nil {
		e.broadcastIndex(fs, []*Entry{copyEnt})
	}
	if send {
		e.sendWant(peer, fs, rem)
	}
	e.logger.Warn("conflict: kept remote version, local saved as copy",
		"store", fs.name, "path", loc.Path, "peer", peer)
}

// saveLocalConflict moves the current local content aside under a
// deterministic conflict name and indexes it as a new file. It reports
// (nil, nil) when there was no file to move, and an error when there was one
// and it could not be moved: the caller must then not write over the path.
func (e *Engine) saveLocalConflict(fs *StoreState, loc *Entry) (*Entry, error) {
	fs.scanMu.Lock()
	defer fs.scanMu.Unlock()
	return e.saveLocalConflictLocked(fs, loc)
}

// saveLocalConflictLocked is saveLocalConflict for callers already holding
// scanMu (applyEntry, which parks a file that a directory is about to replace).
// Parking is a rename inside the tree (parkLocked): it used to copy the file
// into the content store first, so a full data disk made it fail - and the
// remote winner was then written over the local edit regardless.
func (e *Engine) saveLocalConflictLocked(fs *StoreState, loc *Entry) (*Entry, error) {
	if fs.stream {
		// There is no local file to park: the bytes live on the holder, and
		// the loser stays reachable through its entry.
		return nil, nil
	}
	return e.parkLocked(fs, loc.Path)
}

// openEntryBlob opens the bytes of an entry, preferring the content store and
// falling back to the canonical file, which must still be the version the
// journal describes so a peer never receives bytes that do not match the
// entry it asked for.
func (e *Engine) openEntryBlob(fs *StoreState, ent *Entry) (*os.File, error) {
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
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if !info.Mode().IsRegular() {
		f.Close()
		return nil, fmt.Errorf("%s is not a regular file", ent.Path)
	}
	// A file whose size and mtime are the ones the journal recorded is the
	// version the journal describes: that is the same evidence the scanner
	// accepts, and the scanner is what wrote the entry (or applyEntry, which
	// stamps the mtime). Anything else is hashed. Hashing unconditionally
	// cost a full read of a 20 GiB video before its first range could be
	// served, which outlasted the reader's deadline.
	if ent.Blob != "" && (info.Size() != ent.Size || info.ModTime().UnixNano() != ent.MTime) &&
		!e.verifiedRecently(path, info, ent.Blob) {
		sum, err := store.Sum(f)
		if err != nil {
			f.Close()
			return nil, err
		}
		if sum != ent.Blob {
			f.Close()
			return nil, fmt.Errorf("%s does not match the entry (%s)", ent.Path, sum)
		}
		e.rememberVerified(path, info, ent.Blob)
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			f.Close()
			return nil, err
		}
	}
	return f, nil
}

// verifiedFile is one confirmed canonical file: a match between the bytes on
// disk and the journal's blob, kept until the file's size or mtime moves.
type verifiedFile struct {
	size  int64
	mtime int64
	blob  string
	at    time.Time
}

// verifiedLately reports whether this file was hashed against this blob
// recently enough to serve ranges without hashing it again.
func (e *Engine) verifiedRecently(path string, info os.FileInfo, blob string) bool {
	e.mu.Lock()
	v, ok := e.verified[path]
	e.mu.Unlock()
	if !ok {
		return false
	}
	if v.blob != blob || v.size != info.Size() || v.mtime != info.ModTime().UnixNano() {
		return false
	}
	return time.Since(v.at) < 5*time.Minute
}

func (e *Engine) rememberVerified(path string, info os.FileInfo, blob string) {
	e.mu.Lock()
	if len(e.verified) >= 4096 {
		// A cache, not a record: start over rather than grow forever.
		e.verified = map[string]verifiedFile{}
	}
	e.verified[path] = verifiedFile{
		size:  info.Size(),
		mtime: info.ModTime().UnixNano(),
		blob:  blob,
		at:    time.Now(),
	}
	e.mu.Unlock()
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
//
// The want names the exact version, so whichever peer serves it sends those
// bytes and no others, and a refusal can move on to another peer.
func (e *Engine) request(peer string, fs *StoreState, rem *Entry, p *pending) {
	if fs.stream {
		_ = e.applyEntry(fs, rem)
		return
	}
	if !e.canFetch(fs, rem) {
		// No room, or a recent attempt ran out of it: asking again would
		// download the file only to fail writing it, on every exchange.
		return
	}
	if e.registerWant(peer, fs, rem, p) {
		e.sendWant(peer, fs, rem)
	}
}

func (e *Engine) sendWant(peer string, fs *StoreState, rem *Entry) {
	if err := e.sendTo(peer, frame{Type: "want", Store: fs.name, Path: rem.Path, Blob: rem.Blob}); err != nil {
		e.logger.Debug("want", "peer", peer, "error", err)
	}
}

// registerWant records a want, reporting whether it still has to be sent
// (false: the same version is already on its way).
func (e *Engine) registerWant(peer string, fs *StoreState, rem *Entry, p *pending) bool {
	if p.entry == nil || p.entry.Blob != rem.Blob {
		return false
	}
	slot := p.slot()
	e.mu.Lock()
	if prev := e.wantLocked(fs.name, rem.Path, slot); prev != nil &&
		(time.Since(prev.asked) < wantRefresh || e.receivingLocked(fs.name, rem.Path)) {
		// Already asked, and the answer is on its way. The entry to apply
		// may have moved on (a merged vector), so keep the newer one.
		prev.entry = p.entry
		if p.conflictName != "" {
			prev.conflictName = p.conflictName
		}
		e.mu.Unlock()
		return false
	}
	if !p.conflict {
		e.dropSupersededLocked(fs.name, rem.Path, rem.Blob)
	}
	p.asked = time.Now()
	p.peer = peer
	p.tried = []string{peer}
	e.setWantLocked(fs.name, rem.Path, p)
	e.mu.Unlock()
	return true
}

// receivingLocked reports whether a transfer of this path is arriving and
// moved recently. Callers hold e.mu.
func (e *Engine) receivingLocked(store, path string) bool {
	for _, inc := range e.incoming {
		if inc.store == store && inc.path == path && time.Since(inc.last) < wantRefresh {
			return true
		}
	}
	return false
}

// handleRename applies a rename queued on a streamed peer: the holder moves
// its own file and journals both paths, so every other peer converges on the
// same tree.
func (e *Engine) handleRename(peer string, fr *frame) {
	fs := e.byName[fr.Store]
	if fs == nil || !validRel(fr.Path) || !validRel(fr.To) || len(fr.Moves) == 0 || !e.storeSharedWith(fs, peer) {
		return
	}
	if fs.stream {
		return
	}
	fs.scanMu.Lock()
	defer fs.scanMu.Unlock()

	// Only the shape of a move comes off the wire: which paths, and the
	// sender's version. What the entry holds is this machine's own record.
	// Trusting the sender's entry let it name any blob - including a path
	// outside the content store - and then ask for its bytes.
	type move struct{ old, moved *Entry }
	moves := make([]move, 0, len(fr.Moves))
	for _, mv := range fr.Moves {
		if mv.New == nil || !validRel(mv.From) || !validRel(mv.New.Path) {
			continue
		}
		var suffix string
		switch {
		case mv.From == fr.Path:
		case strings.HasPrefix(mv.From, fr.Path+"/"):
			suffix = mv.From[len(fr.Path):]
		default:
			continue
		}
		if mv.New.Path != fr.To+suffix {
			continue
		}
		loc := fs.db.Get(mv.From)
		if loc == nil || loc.Deleted {
			continue
		}
		vv := MergeVV(loc.VV, mv.New.VV)
		moved := loc.clone()
		moved.Path = mv.New.Path
		moved.VV = vv
		moves = append(moves, move{
			old:   &Entry{Path: mv.From, Deleted: true, Dir: loc.Dir, VV: vv},
			moved: moved,
		})
	}
	if len(moves) == 0 {
		return
	}

	from := filepath.Join(fs.root, fr.Path)
	to := filepath.Join(fs.root, fr.To)
	if err := e.ensureDir(fs, filepath.Dir(to)); err != nil {
		e.logger.Warn("rename: create parent", "store", fs.name, "path", fr.To, "error", err)
		return
	}
	if _, err := os.Lstat(to); err == nil {
		e.logger.Warn("rename destination exists", "store", fs.name, "from", fr.Path, "to", fr.To)
		return
	} else if !os.IsNotExist(err) {
		e.logger.Warn("inspect rename destination", "store", fs.name, "to", fr.To, "error", err)
		return
	}
	if err := os.Rename(from, to); err != nil {
		e.logger.Warn("rename on holder", "store", fs.name, "path", fr.Path, "to", fr.To, "error", err)
		return
	}
	e.chownPath(fs, to)
	changed := make([]*Entry, 0, len(moves)*2)
	for _, mv := range moves {
		if err := fs.db.Put(mv.old); err != nil {
			e.logger.Warn("journal rename tombstone", "store", fs.name, "path", mv.old.Path, "error", err)
			return
		}
		if err := fs.db.Put(mv.moved); err != nil {
			e.logger.Warn("journal renamed entry", "store", fs.name, "path", mv.moved.Path, "error", err)
			return
		}
		changed = append(changed, mv.old, mv.moved)
	}
	go e.broadcastIndex(fs, changed)
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
	if fs == nil || !validRel(fr.Path) || !e.storeSharedWith(fs, peer) {
		return
	}
	if fr.Blob != "" && !store.ValidSum(fr.Blob) {
		return
	}
	key := incomeKey(peer, fr.Store, fr.Path, fr.Blob)

	e.mu.Lock()
	inc := e.incoming[key]
	if inc == nil {
		if !e.wantedLocked(fr.Store, fr.Path, fr.Blob) {
			// Nobody asked: an unrequested transfer is at best a duplicate
			// answer and at worst a peer filling this disk.
			e.mu.Unlock()
			return
		}
		if fr.Seq != 0 {
			// Mid-stream chunk with no transfer open: the start was dropped,
			// so there is nothing this can validly extend.
			e.mu.Unlock()
			return
		}
		// Accepting a transfer that cannot fit only fills the disk and then
		// fails; refuse it up front and back the want off (see canFetch).
		size := int64(-1)
		for _, conflict := range []bool{false, true} {
			if p := e.wantLocked(fr.Store, fr.Path, wantSlot(fr.Blob, conflict)); p != nil && p.entry != nil && p.entry.Size > size {
				size = p.entry.Size
			}
		}
		if ok, why := e.roomFor(fs, size); !ok {
			e.dropWantsLocked(fr.Store, fr.Path, fr.Blob)
			e.mu.Unlock()
			e.backOffFetch(fs, fr.Path, why)
			return
		}
		f, err := e.store.Temp()
		if err != nil {
			e.mu.Unlock()
			if isNoSpace(err) {
				e.dropWants(fr.Store, fr.Path, fr.Blob)
				e.backOffFetch(fs, fr.Path, err.Error())
			}
			return
		}
		inc = &incomingBlob{store: fr.Store, path: fr.Path, blob: fr.Blob, f: f, hash: sha256.New(), last: time.Now()}
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
		inc.hash.Reset()
		inc.next = 0
	}

	if len(fr.Data) > 0 {
		if _, err := inc.f.Write(fr.Data); err != nil {
			e.dropIncoming(key, inc)
			if isNoSpace(err) {
				// Out of space mid-file: asking again at the next exchange
				// would download it all again and fail the same way.
				e.dropWants(fr.Store, fr.Path, fr.Blob)
				e.backOffFetch(fs, fr.Path, err.Error())
			}
			return
		}
		inc.hash.Write(fr.Data)
	}
	inc.next = fr.Seq + 1
	e.mu.Lock()
	inc.last = time.Now()
	e.mu.Unlock()
}

// handleBlobEnd closes a transfer and hands it to a finisher, which verifies,
// stores and applies it off the chunk worker.
func (e *Engine) handleBlobEnd(peer string, fr *frame) {
	fs := e.byName[fr.Store]
	if fs == nil || !validRel(fr.Path) {
		return
	}
	// The version is the sender's name for the bytes; an older sender only
	// says it in the entry.
	blob := fr.Blob
	if blob == "" && fr.Entry != nil {
		blob = fr.Entry.Blob
	}
	key := incomeKey(peer, fr.Store, fr.Path, fr.Blob)

	e.mu.Lock()
	inc := e.incoming[key]
	delete(e.incoming, key)
	if inc == nil && !emptyBlobEnd(fr, blob) {
		// No transfer is open, so there are no bytes to apply: the start was
		// lost, or the transfer was abandoned at a gap. The want is not
		// answered - it is due again at the next exchange, not consumed.
		if store.ValidSum(blob) {
			e.resetWantsLocked(fr.Store, fr.Path, blob)
		}
		e.mu.Unlock()
		return
	}
	var wants []*pending
	if store.ValidSum(blob) {
		for _, conflict := range []bool{false, true} {
			if p := e.takeWantLocked(fr.Store, fr.Path, wantSlot(blob, conflict)); p != nil {
				wants = append(wants, p)
			}
		}
	}
	e.mu.Unlock()
	if len(wants) == 0 {
		// A blobEnd nobody is waiting for - a second peer's answer to a want
		// the first already satisfied, a version superseded since, or one
		// never asked for - must not be applied: it carries no version check,
		// and used to overwrite whatever the path held by then (a bare
		// zero-byte one truncated any file).
		if inc != nil {
			inc.f.Close()
			os.Remove(inc.f.Name())
		}
		return
	}
	// The chunk worker waits only for room in the backlog; the finish slot
	// is waited for off it (see finishBacklog).
	discard := func() {
		if inc != nil {
			inc.f.Close()
			os.Remove(inc.f.Name())
		}
	}
	select {
	case e.finishQueue <- struct{}{}:
	case <-e.closed:
		discard()
		return
	}
	e.finishWG.Add(1)
	go func() {
		defer e.finishWG.Done()
		defer func() { <-e.finishQueue }()
		select {
		case e.finishSem <- struct{}{}:
		case <-e.closed:
			discard()
			return
		}
		defer func() { <-e.finishSem }()
		e.finishBlob(peer, fs, fr, blob, inc, wants)
	}()
}

// emptyBlobEnd reports a blobEnd that legitimately has no transfer behind it:
// a zero-byte file from a sender that sent no chunk at all.
func emptyBlobEnd(fr *frame, blob string) bool {
	ent := fr.Entry
	return ent != nil && !ent.Deleted && ent.Size == 0 && ent.Blob == blob
}

// finishBlob verifies a completed transfer against the version that was
// asked for, adopts it into the content store, and applies every want it
// satisfies: the update itself, a conflict copy, or both.
func (e *Engine) finishBlob(peer string, fs *StoreState, fr *frame, blob string, inc *incomingBlob, wants []*pending) {
	defer e.lockBlob(blob)()
	if inc == nil {
		// No transfer was opened. That is legitimate only for a zero-byte
		// file whose sender never sent a chunk: apply it directly rather than
		// dropping the want and leaving an empty file missing forever.
		if emptyBlobEnd(fr, blob) {
			if err := e.ensureEmptyBlob(blob); err != nil {
				e.logger.Warn("empty blob", "store", fs.name, "path", fr.Path, "error", err)
				e.restoreWants(fs, fr.Path, wants)
				return
			}
			e.applyWants(fs, wants)
			return
		}
		e.restoreWants(fs, fr.Path, wants)
		return
	}
	defer os.Remove(inc.f.Name())

	// A transfer that fails here never applied anything: its wants go back,
	// due again, instead of being consumed by bytes that were never used.
	sum := hex.EncodeToString(inc.hash.Sum(nil))
	if sum != blob {
		inc.f.Close()
		e.logger.Warn("blob failed verification", "store", fs.name, "path", fr.Path, "peer", peer, "want", blob, "got", sum)
		e.restoreWants(fs, fr.Path, wants)
		return
	}
	if err := inc.f.Sync(); err != nil {
		inc.f.Close()
		e.failedFinish(fs, fr.Path, wants, err)
		return
	}
	if err := inc.f.Close(); err != nil {
		e.failedFinish(fs, fr.Path, wants, err)
		return
	}
	if err := e.store.Adopt(inc.f.Name(), sum); err != nil {
		e.logger.Error("adopt blob", "error", err)
		e.restoreWants(fs, fr.Path, wants)
		return
	}
	e.applyWants(fs, wants)
}

// failedFinish handles a transfer whose bytes could not be made durable: a
// full disk pauses fetching the path (asking again at once would only fill it
// again); anything else puts the wants back, due again.
func (e *Engine) failedFinish(fs *StoreState, path string, wants []*pending, err error) {
	if isNoSpace(err) {
		e.backOffFetch(fs, path, err.Error())
		return
	}
	e.restoreWants(fs, path, wants)
}

// applyWants applies a verified, stored blob to what asked for it. A
// conflict copy goes first: it copies the blob, and applying the update may
// move the blob into the tree.
func (e *Engine) applyWants(fs *StoreState, wants []*pending) {
	slices.SortFunc(wants, func(a, b *pending) int {
		switch {
		case a.conflict == b.conflict:
			return 0
		case a.conflict:
			return -1
		}
		return 1
	})
	for _, want := range wants {
		if want.conflict {
			e.writeConflictCopy(fs, want, want.entry)
			continue
		}
		if err := e.applyEntry(fs, want.entry); err != nil {
			e.logger.Warn("apply entry", "store", fs.name, "path", want.entry.Path, "error", err)
			if isNoSpace(err) {
				e.backOffFetch(fs, want.entry.Path, err.Error())
			}
			continue
		}
		// Fan the applied version out at once. The peer that sent the bytes
		// is usually a streamed store waiting for proof the holder took them;
		// it retires its queue entry on that echo rather than on the next
		// resend.
		e.broadcastIndex(fs, []*Entry{want.entry})
	}
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
	if e.incoming[key] == inc {
		delete(e.incoming, key)
	}
	e.mu.Unlock()
}

// dropWants forgets both wants (update and conflict copy) for one version.
func (e *Engine) dropWants(store, path, blob string) {
	e.mu.Lock()
	e.dropWantsLocked(store, path, blob)
	e.mu.Unlock()
}

func (e *Engine) dropWantsLocked(store, path, blob string) {
	if blob == "" {
		delete(e.wants, wantKey(store, path))
		return
	}
	for _, conflict := range []bool{false, true} {
		e.takeWantLocked(store, path, wantSlot(blob, conflict))
	}
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
		e.broadcastIndex(fs, []*Entry{copyEnt})
		return
	}
	fs.scanMu.Lock()
	defer fs.scanMu.Unlock()
	dst := filepath.Join(fs.root, want.conflictName)
	if err := e.writeBlobTo(fs, dst, ent.Blob, ent.Mode, ent.MTime, nil); err != nil {
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
		defer fs.scanMu.Unlock()
		// Decided on an entry read earlier; a local change since (a rename
		// that tombstoned the path) supersedes it, and writing it anyway
		// put the file back at its old name.
		if cur := fs.db.Get(ent.Path); cur != nil && Dominates(cur.VV, ent.VV) && !Dominates(ent.VV, cur.VV) {
			return nil
		}
		return fs.db.Put(ent)
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
	// The want went out some time ago. If a newer (or concurrent) version
	// was recorded here since - a local edit the scanner picked up while the
	// bytes were in flight - the arriving one must not land over it. The
	// next index exchange orders the two properly, with a conflict copy if
	// they really are concurrent.
	if cur := fs.db.Get(ent.Path); cur != nil && !Dominates(ent.VV, cur.VV) {
		return fmt.Errorf("%s: the arriving version does not supersede the one recorded here; leaving it to the next exchange", ent.Path)
	}

	if err := e.unblockParents(fs, ent.Path); err != nil {
		return err
	}

	// What is at the path now decides what may happen to it. A file the
	// journal does not describe is an edit the mesh has not seen: it is parked
	// first, and if it cannot even be read the apply is refused - hashing
	// failed used to fall through and overwrite it. A file the journal does
	// describe is the version being superseded; its bytes go into the content
	// store rather than away (fileState.keep), because a peer that resolved a
	// conflict against it may still be about to ask for them.
	before := statFile(dst)
	if before.exists {
		loc := fs.db.Get(ent.Path)
		if loc == nil || loc.Deleted || loc.Size != before.size || loc.MTime != before.mtime {
			sum, herr := store.SumFile(dst)
			if herr != nil {
				return fmt.Errorf("%s: cannot read the file this update would replace: %w", ent.Path, herr)
			}
			if sum != ent.Blob {
				parkEnt, perr := e.parkLocked(fs, ent.Path)
				if perr != nil {
					// Overwriting now would destroy the only copy of an
					// edit the mesh has not seen.
					return fmt.Errorf("%s: cannot park an unsynced local change: %w", ent.Path, perr)
				}
				if parkEnt != nil {
					e.broadcastIndex(fs, []*Entry{parkEnt})
					e.logger.Warn("unsynced local change preserved as conflict copy",
						"store", fs.name, "path", ent.Path, "copy", parkEnt.Path)
				}
				before = statFile(dst)
			}
		} else if loc.Local && !loc.Dir && loc.Blob != "" && loc.Blob != ent.Blob && store.ValidSum(loc.Blob) {
			before.keep = loc.Blob
		}
	}

	// When the content-store copy would be released right after the write
	// (no history wants it), move it into the tree instead of copying: the
	// same end state, without reading and writing a 20 GiB file under the
	// scan lock.
	consume := !e.wantsBlob(fs, ent.Path, ent.Blob)
	if err := e.placeBlob(fs, dst, ent.Blob, ent.Mode, ent.MTime, consume, &before); err != nil {
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
		copyEnt, perr := e.saveLocalConflictLocked(fs, loc)
		if copyEnt != nil {
			parked = copyEnt.Path
			e.broadcastIndex(fs, []*Entry{copyEnt})
		} else if info, err := os.Lstat(filepath.Join(fs.root, dir)); err == nil && info.Mode().IsRegular() {
			// The file is still there and could not be parked. Removing it
			// would delete the user's bytes.
			return fmt.Errorf("%s: cannot park the file a directory must replace: %v", dir, perr)
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
// for the scanner, whose fast path is "size and mtime unchanged". before, when
// set, is what the caller saw at dst (see placeBlob).
func (e *Engine) writeBlobTo(fs *StoreState, dst, sum string, mode uint32, mtime int64, before *fileState) error {
	return e.placeBlob(fs, dst, sum, mode, mtime, false, before)
}

// beforePlace, when set (tests), runs after the new bytes are staged and
// before the path is re-checked and replaced.
var beforePlace func(dst string)

// placeBlob is writeBlobTo, optionally taking the content-store copy instead
// of copying it (consume). Taking it is a rename when the store and the tree
// share a filesystem; otherwise it falls back to the copy.
//
// before is what the caller saw at dst when it decided to replace it. Copying
// a large blob takes a while, and a file written at dst meanwhile is an edit
// nobody has seen: dst is checked again just before the final rename, and a
// change is parked as a conflict copy instead of being renamed over. When
// before.keep is set the displaced version goes into the content store.
func (e *Engine) placeBlob(fs *StoreState, dst, sum string, mode uint32, mtime int64, consume bool, before *fileState) error {
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
	moved := false
	// abort undoes the scratch file; a content-store copy taken into it goes
	// back rather than being lost.
	abort := func(err error) error {
		if moved {
			if rerr := os.Rename(tmp, e.store.BlobPath(sum)); rerr != nil {
				os.Remove(tmp)
			}
		} else {
			os.Remove(tmp)
		}
		return err
	}
	if consume {
		f.Close()
		if err := os.Rename(e.store.BlobPath(sum), tmp); err == nil {
			moved = true
		} else if f, err = os.OpenFile(tmp, os.O_WRONLY|os.O_TRUNC, 0o600); err != nil {
			os.Remove(tmp)
			return err
		}
	}
	if !moved {
		blob, err := e.store.Open(sum)
		if err != nil {
			f.Close()
			os.Remove(tmp)
			return err
		}
		// Between two files io.Copy is copy_file_range: in-kernel, and a
		// reflink on filesystems that share extents.
		_, err = io.Copy(f, blob)
		blob.Close()
		if err != nil {
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
	}
	// Mode, owner and mtime go on before the rename, so the file never
	// appears at its path looking like anything but what it mirrors.
	perm := os.FileMode(mode)
	if perm == 0 {
		perm = 0o644
	}
	_ = os.Chmod(tmp, perm.Perm())
	e.chownPath(fs, tmp)
	if mtime != 0 {
		when := time.Unix(0, mtime)
		if err := os.Chtimes(tmp, when, when); err != nil {
			e.logger.Warn("set mtime", "store", fs.name, "path", dst, "error", err)
		}
	}
	if beforePlace != nil {
		beforePlace(dst)
	}
	displaced := false
	if before != nil {
		rel, rerr := filepath.Rel(fs.root, dst)
		if rerr != nil {
			return abort(rerr)
		}
		switch {
		case !before.same(dst):
			// Written while the bytes were being copied: an edit. Park it.
			parkEnt, perr := e.parkLocked(fs, rel)
			if perr != nil {
				return abort(fmt.Errorf("%s changed while the update was copied, and could not be parked: %w", rel, perr))
			}
			if parkEnt != nil {
				e.broadcastIndex(fs, []*Entry{parkEnt})
				e.logger.Warn("edit made during an update preserved as conflict copy",
					"store", fs.name, "path", rel, "copy", parkEnt.Path)
			}
		case before.exists && before.keep != "" && !e.store.Has(before.keep):
			if kerr := e.keepDisplacedLocked(fs, dst, *before); kerr != nil {
				return abort(fmt.Errorf("%s: keeping the replaced version: %w", rel, kerr))
			}
			displaced = !statFile(dst).exists
		}
	}
	if err := os.Rename(tmp, dst); err != nil {
		if displaced {
			// The path is empty and the old version sits in the store: put
			// it back, or the next scan reads this failure as a deletion.
			_ = os.Rename(e.store.BlobPath(before.keep), dst)
		}
		return abort(err)
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
		copyEnt, perr := e.saveLocalConflictLocked(fs, loc)
		if perr != nil {
			// An edit the mesh has not seen, and it cannot be moved aside:
			// keep it, and keep the entry alive.
			return fmt.Errorf("%s: cannot park an unsynced local change: %w", loc.Path, perr)
		}
		if copyEnt != nil {
			e.broadcastIndex(fs, []*Entry{copyEnt})
			// The rename moved the file; the path is free and the caller's
			// tombstone stands. Nothing left to trash.
			return nil
		}
	}
	trash := filepath.Join(e.dataDir, "trash", fs.name)
	dst := filepath.Join(trash, loc.Path)
	e.trashMu.RLock()
	if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
		e.trashMu.RUnlock()
		return err
	}
	dst = freeTrashName(dst)
	err = moveOrCopy(src, dst, info.Mode())
	e.trashMu.RUnlock()
	if err != nil {
		return err
	}
	if info.Mode().IsRegular() {
		// Stamp the deletion time: setting the times moves the file's ctime
		// to now, which is what pruneTrash expires by (trashTime). The mtime
		// is kept as it was, so a file taken back out of the trash still
		// sorts by when it was written.
		_ = os.Chtimes(dst, time.Now(), info.ModTime())
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
