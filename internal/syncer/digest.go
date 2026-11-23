package syncer

import (
	"crypto/sha256"
	"encoding/binary"
	"math/bits"
	"slices"
	"sort"

	"codeberg.org/kyleraykbs/storeboard/internal/transport"
)

// Index digests. Two peers that already agree used to prove it by sending each
// other every entry they had - on every reconnect, and again every ten minutes
// - and walking every entry they received. For a photo library that is
// megabytes per peer for no change at all.
//
// Instead one side sends a digest: the journal split into buckets by path,
// each bucket summarised as the sum of its entries' hashes. The other side
// compares against its own, sends its entries for the buckets that differ, and
// asks for the sender's entries for the same buckets. Agreement costs one
// small frame; a difference costs the entries near it, not the whole store.
// Both sides then run the ordinary index rules over what they received.

// featureDigest is advertised in the hello by peers that understand digest
// and pull frames. Anyone else gets full indexes, as before.
const featureDigest = "digest"

// Features is what this engine advertises to peers (transport.FeatureHandler).
func (e *Engine) Features() []string {
	return append([]string{featureDigest, featureReadCancel}, e.replicaFeatures()...)
}

var _ transport.FeatureHandler = (*Engine)(nil)

func peerHasDigest(send transport.Sender) bool { return peerHasFeature(send, featureDigest) }

func peerHasFeature(send transport.Sender, name string) bool {
	fs, ok := send.(transport.FeatureSender)
	return ok && fs.HasFeature(name)
}

func (e *Engine) peerSupportsDigest(peer string) bool {
	e.mu.Lock()
	send := e.sends[peer]
	e.mu.Unlock()
	return send != nil && peerHasDigest(send)
}

const (
	minDigestBits = 4
	maxDigestBits = 12
)

// digestBits sizes the digest for a journal of n entries: about sixteen
// entries a bucket, between 16 and 4096 buckets (64 KiB of sums at most).
func digestBits(n int) int {
	b := bits.Len(uint(n / 16))
	return min(max(b, minDigestBits), maxDigestBits)
}

func bucketOf(path string, nbits int) int {
	var h uint64 = 14695981039346656037
	for i := 0; i < len(path); i++ {
		h ^= uint64(path[i])
		h *= 1099511628211
	}
	return int(h >> (64 - nbits))
}

// entrySum hashes the parts of an entry both peers must agree on. Local,
// mtime, mode and size are left out: Local is this machine's own business,
// and the rest follow from the blob and the version vector - two peers whose
// only difference is an mtime the index rules would never reconcile must not
// look different forever.
//
// unsettled marks an entry this side has not finished with: a replica that
// knows a file but has not received its bytes, or a streamed store with the
// change still queued for a holder. Its hash then differs from the peer's, so
// the bucket is exchanged and the ordinary rules retry the fetch or retire the
// queued op - the work the periodic full index used to trigger.
func (e *Engine) entrySum(ent *Entry, unsettled bool) uint64 {
	h := sha256.New()
	var buf [8]byte
	write := func(s string) {
		binary.BigEndian.PutUint64(buf[:], uint64(len(s)))
		h.Write(buf[:])
		h.Write([]byte(s))
	}
	write(ent.Path)
	write(ent.Blob)
	flags := byte(0)
	if ent.Deleted {
		flags |= 1
	}
	if ent.Dir {
		flags |= 2
	}
	h.Write([]byte{flags})
	peers := make([]string, 0, len(ent.VV))
	for p := range ent.VV {
		peers = append(peers, p)
	}
	sort.Strings(peers)
	for _, p := range peers {
		write(p)
		binary.BigEndian.PutUint64(buf[:], ent.VV[p])
		h.Write(buf[:])
	}
	if unsettled {
		write("unsettled:" + e.me)
	}
	return binary.BigEndian.Uint64(h.Sum(nil))
}

// queuedPaths is every path a streamed store's queue still has an op for.
func queuedPaths(fs *StoreState) map[string]bool {
	fs.queueMu.Lock()
	defer fs.queueMu.Unlock()
	queued := make(map[string]bool, len(fs.queue))
	for _, op := range fs.queue {
		queued[op.Path] = true
		if op.To != "" {
			queued[op.To] = true
		}
	}
	return queued
}

// digest summarises a journal into 1<<nbits bucket sums.
//
// An entry this side has not finished with is perturbed (see entrySum). For
// a streamed store that is every path with an op still queued - including
// paths the journal no longer has an entry for at all (a temporary file
// written and removed): their bucket must differ too, or the holder's answer
// that would acknowledge the op is never asked for.
func (e *Engine) digest(fs *StoreState, entries []*Entry, nbits int) []uint64 {
	sums := make([]uint64, 1<<nbits)
	if !fs.stream {
		for _, ent := range entries {
			unsettled := !ent.Deleted && !ent.Dir && !ent.Local
			sums[bucketOf(ent.Path, nbits)] += e.entrySum(ent, unsettled)
		}
		return sums
	}
	queued := queuedPaths(fs)
	for _, ent := range entries {
		sums[bucketOf(ent.Path, nbits)] += e.entrySum(ent, queued[ent.Path])
		delete(queued, ent.Path)
	}
	for path := range queued {
		sums[bucketOf(path, nbits)] += e.entrySum(&Entry{Path: path}, true)
	}
	return sums
}

func encodeSums(sums []uint64) []byte {
	out := make([]byte, 8*len(sums))
	for i, s := range sums {
		binary.BigEndian.PutUint64(out[8*i:], s)
	}
	return out
}

// sendDigestTo offers a peer this side's digest of one store.
func (e *Engine) sendDigestTo(peer string, fs *StoreState) error {
	entries := fs.db.All()
	nbits := digestBits(len(entries))
	return e.sendTo(peer, frame{Type: "digest", Store: fs.name, Bits: nbits, Sums: encodeSums(e.digest(fs, entries, nbits))})
}

// handleDigest compares a peer's digest with this side's, sends the entries
// in every bucket that differs, and asks the peer for its entries in the same
// buckets.
func (e *Engine) handleDigest(peer string, fr *frame) {
	fs := e.byName[fr.Store]
	if fs == nil || !e.storeSharedWith(fs, peer) {
		return
	}
	if fr.Bits < minDigestBits || fr.Bits > maxDigestBits || len(fr.Sums) != 8<<fr.Bits {
		// Not a digest this side can compare: fall back to the whole index,
		// which is always correct.
		_ = e.sendIndexTo(peer, fs, fs.db.All())
		return
	}
	entries := fs.db.All()
	mine := e.digest(fs, entries, fr.Bits)
	var differ []int
	for i, s := range mine {
		if binary.BigEndian.Uint64(fr.Sums[8*i:]) != s {
			differ = append(differ, i)
		}
	}
	if len(differ) == 0 {
		return
	}
	if err := e.sendIndexTo(peer, fs, entriesInBuckets(entries, fr.Bits, differ)); err != nil {
		e.logger.Debug("digest reply", "peer", peer, "store", fs.name, "error", err)
		return
	}
	if err := e.sendTo(peer, frame{Type: "pull", Store: fs.name, Bits: fr.Bits, Buckets: differ}); err != nil {
		e.logger.Debug("pull", "peer", peer, "store", fs.name, "error", err)
	}
	e.logger.Debug("index digest differs", "peer", peer, "store", fs.name, "buckets", len(differ), "of", len(mine))
}

// handlePull sends this side's entries in the buckets a peer asked for.
func (e *Engine) handlePull(peer string, fr *frame) {
	fs := e.byName[fr.Store]
	if fs == nil || !e.storeSharedWith(fs, peer) || fr.Bits < minDigestBits || fr.Bits > maxDigestBits {
		return
	}
	var buckets []int
	for _, b := range fr.Buckets {
		if b >= 0 && b < 1<<fr.Bits {
			buckets = append(buckets, b)
		}
	}
	if len(buckets) == 0 {
		return
	}
	if err := e.sendIndexTo(peer, fs, entriesInBuckets(fs.db.All(), fr.Bits, buckets)); err != nil {
		e.logger.Debug("pull reply", "peer", peer, "store", fs.name, "error", err)
	}
}

func entriesInBuckets(entries []*Entry, nbits int, buckets []int) []*Entry {
	want := make([]bool, 1<<nbits)
	for _, b := range buckets {
		want[b] = true
	}
	var out []*Entry
	for _, ent := range entries {
		if want[bucketOf(ent.Path, nbits)] {
			out = append(out, ent)
		}
	}
	return out
}

// offerIndex brings one peer up to date on one store: a digest to a peer that
// understands one, the whole index to any other.
func (e *Engine) offerIndex(peer string, fs *StoreState) error {
	if e.peerSupportsDigest(peer) {
		return e.sendDigestTo(peer, fs)
	}
	return e.sendIndexTo(peer, fs, fs.db.All())
}

// digestInitiator reports whether this side opens the digest exchange with a
// newly connected peer. One side is enough - the exchange is symmetric - and
// both opening it would send every differing bucket twice.
func (e *Engine) digestInitiator(peer string) bool {
	return slices.Min([]string{e.me, peer}) == e.me
}
