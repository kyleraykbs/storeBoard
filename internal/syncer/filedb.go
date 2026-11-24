package syncer

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

// Entry is one file's journal record: where it is in a store, what bytes it
// holds, and the version vector that orders it against other peers'
// versions (DESIGN invariant 5).
type Entry struct {
	Path    string            `json:"path"`
	Blob    string            `json:"blob,omitempty"`
	Size    int64             `json:"size"`
	MTime   int64             `json:"mtime"` // unix nanoseconds
	Mode    uint32            `json:"mode"`
	VV      map[string]uint64 `json:"vv,omitempty"`
	Deleted bool              `json:"deleted,omitempty"`
	// Dir marks a directory with no files beneath it. A directory that holds
	// files needs no entry: the file paths imply it, and applying one creates
	// the parents. Only an otherwise invisible directory — one the user just
	// made, or one holding nothing but empty directories — is recorded, so it
	// exists on every peer instead of being silently local.
	Dir bool `json:"dir,omitempty"`
	// Local records that this machine materialised the file. It is evidence a
	// deletion can be trusted: a file that used to be here and is now gone is
	// a deletion, while an entry this machine never held may simply have
	// arrived before its bytes. It is never trusted off the wire — a peer's
	// entries are decoded with it cleared.
	Local bool `json:"local,omitempty"`
	// DeletedAt is when a tombstone was first recorded (unix nanoseconds),
	// stamped by the journal itself and carried with the tombstone. It is
	// what lets a tombstone ever be dropped (ExpireTombstones); it plays no
	// part in ordering or in the digest.
	DeletedAt int64 `json:"deletedAt,omitempty"`
}

func (e *Entry) clone() *Entry {
	if e == nil {
		return nil
	}
	out := *e
	if e.VV != nil {
		out.VV = make(map[string]uint64, len(e.VV))
		for k, v := range e.VV {
			out.VV[k] = v
		}
	}
	return &out
}

type record struct {
	Entry *Entry `json:"entry,omitempty"`
}

// FileDB is a store's journal: an append-only NDJSON log replayed into an
// in-memory index at open (then checkpointed, so the log never grows past
// one session's changes), with one version-vector counter for this machine.
type FileDB struct {
	mu      sync.Mutex
	me      string
	logPath string
	f       *os.File
	entries map[string]*Entry
	counter uint64
	closed  bool
	// observe, when set, sees every change to the in-memory state, in order.
	// It runs under the journal lock, so it must be cheap and must never
	// call back into the journal.
	observe func(*Entry)
	// gen counts changes to the in-memory state.
	gen uint64
	// off is the log's length after the last whole record. A write that
	// fails part-way (ENOSPC) is cut back to it, or the torn half would glue
	// itself to the next record and both would be lost on replay. torn is set
	// when even that failed: the next record then starts on a fresh line.
	off  int64
	torn bool
	// corrupt counts unreadable lines found at open.
	corrupt int
}

// Gen is a counter that moves whenever the journal's state changes.
func (d *FileDB) Gen() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.gen
}

// Observe hands fn every current entry, then every later change, with no gap
// between the two: a view built this way can never miss an update that raced
// its construction. fn runs under the journal lock.
func (d *FileDB) Observe(fn func(*Entry)) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, ent := range d.entries {
		fn(ent.clone())
	}
	d.observe = fn
}

// OpenFileDB replays the log at path (absent: a fresh journal) and rewrites
// it compacted.
//
// Unreadable lines are counted and logged, and the original log is kept
// beside the journal as <path>.corrupt before the checkpoint replaces it: a
// torn final line after a crash is expected, anything else is evidence worth
// keeping. A checkpoint that cannot be written (a full disk) is not fatal:
// the journal opens uncompacted, appending to the log as it is.
func OpenFileDB(path, me string) (*FileDB, error) {
	d := &FileDB{me: me, logPath: path, entries: map[string]*Entry{}}

	now := time.Now().UnixNano()
	if f, err := os.Open(path); err == nil {
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 0, 64*1024), 64*1024*1024)
		for sc.Scan() {
			line := sc.Bytes()
			if len(line) == 0 {
				continue
			}
			var r record
			if err := json.Unmarshal(line, &r); err != nil {
				d.corrupt++
				continue
			}
			if r.Entry != nil {
				if r.Entry.Deleted && r.Entry.DeletedAt == 0 {
					// A tombstone from before deletion times were kept: its
					// age is unknown, so it counts from now.
					r.Entry.DeletedAt = now
				}
				d.entries[r.Entry.Path] = r.Entry
			}
		}
		f.Close()
		if err := sc.Err(); err != nil {
			// The checkpoint below rewrites the journal from what was read:
			// carrying on after a read error would truncate it for good.
			return nil, fmt.Errorf("journal %s: %w", path, err)
		}
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("journal: %w", err)
	}

	for _, e := range d.entries {
		if c := e.VV[me]; c > d.counter {
			d.counter = c
		}
	}
	if d.corrupt > 0 {
		keepCorrupt(path)
		slog.Warn("journal had unreadable lines; they were skipped and the original kept",
			"journal", path, "lines", d.corrupt, "copy", path+".corrupt")
	}

	if err := d.checkpointLocked(); err != nil {
		// A journal that cannot be compacted can still be used: the log on
		// disk is intact, and appending to it is what it is for. Refusing
		// to start left a full data disk with no daemon to free it.
		slog.Warn("journal checkpoint failed; continuing with the uncompacted log", "journal", path, "error", err)
		f, oerr := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		if oerr != nil {
			return nil, fmt.Errorf("journal: %w (checkpoint: %v)", oerr, err)
		}
		d.f = f
		d.off, d.torn = logTail(f)
	}
	return d, nil
}

// keepCorrupt keeps a copy of a log that had unreadable lines, so the
// checkpoint that drops them does not drop the evidence with them. A hard
// link costs nothing; a filesystem without them gets a copy.
func keepCorrupt(path string) {
	dst := path + ".corrupt"
	os.Remove(dst)
	if os.Link(path, dst) == nil {
		return
	}
	b, err := os.ReadFile(path)
	if err == nil {
		_ = os.WriteFile(dst, b, 0o600)
	}
}

// logTail reports an append handle's length and whether the log ends part-way
// through a line.
func logTail(f *os.File) (int64, bool) {
	info, err := f.Stat()
	if err != nil || info.Size() == 0 {
		return 0, false
	}
	var last [1]byte
	if _, err := f.ReadAt(last[:], info.Size()-1); err != nil {
		// Opened write-only: read it separately.
		if r, oerr := os.Open(f.Name()); oerr == nil {
			_, err = r.ReadAt(last[:], info.Size()-1)
			r.Close()
		}
		if err != nil {
			return info.Size(), true
		}
	}
	return info.Size(), last[0] != '\n'
}

// checkpointLocked rewrites the log as exactly the current state and reopens
// it for appending. On failure the old log and handle are left as they were
// and the scratch file is removed.
func (d *FileDB) checkpointLocked() error {
	tmp := d.logPath + ".new"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("journal: %w", err)
	}
	fail := func(err error) error {
		f.Close()
		os.Remove(tmp)
		return err
	}
	w := bufio.NewWriter(f)
	var size int64
	for _, p := range d.pathsLocked() {
		b, err := json.Marshal(record{Entry: d.entries[p]})
		if err != nil {
			return fail(err)
		}
		w.Write(b)
		w.WriteByte('\n')
		size += int64(len(b)) + 1
	}
	if err := w.Flush(); err != nil {
		return fail(err)
	}
	if err := f.Sync(); err != nil {
		return fail(err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, d.logPath); err != nil {
		os.Remove(tmp)
		return err
	}
	nf, err := os.OpenFile(d.logPath, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("journal: %w", err)
	}
	if d.f != nil {
		d.f.Close()
	}
	d.f = nf
	d.off, d.torn = size, false
	return nil
}

// ExpireTombstones drops tombstones older than age whose path match accepts,
// and compacts the log when it dropped any. It reports how many went.
//
// Only paths the caller can vouch for are dropped. A tombstone is what stops a
// peer that never heard of a deletion from bringing the file back, and proving
// every peer has heard would mean knowing every peer's version vector; for an
// editor's scratch file the worst a late peer can do is bring back a temporary
// file, while every one of those names otherwise stayed in the journal, the
// index and the digest forever.
func (d *FileDB) ExpireTombstones(now time.Time, age time.Duration, match func(string) bool) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return 0, nil
	}
	cutoff := now.Add(-age).UnixNano()
	n := 0
	for p, ent := range d.entries {
		if ent.Deleted && ent.DeletedAt != 0 && ent.DeletedAt < cutoff && match(p) {
			delete(d.entries, p)
			n++
		}
	}
	if n == 0 {
		return 0, nil
	}
	d.gen++
	return n, d.checkpointLocked()
}

func (d *FileDB) pathsLocked() []string {
	out := make([]string, 0, len(d.entries))
	for p := range d.entries {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

// Next returns the next version-vector counter value for a peer (this
// machine in practice).
func (d *FileDB) Next() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.counter++
	return d.counter
}

// Put records an entry (replacing any previous version of the path).
func (d *FileDB) Put(e *Entry) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.putLocked(e)
}

func (d *FileDB) putLocked(e *Entry) error {
	// Rewriting a journal line for a state that did not change is pure bloat.
	// Peers exchange their full index on every reconnect and resend it
	// periodically, and each of those passes used to append a line per path:
	// an idle mesh grew journals by megabytes a minute, and a store with a
	// few thousand files collected gigabytes of identical lines.
	prev := d.entries[e.Path]
	if prev != nil && sameEntry(prev, e) {
		return nil
	}
	e = e.clone()
	if e.Deleted {
		// The first time a path is known deleted is its deletion time; a
		// re-record of the same tombstone (a merged vector) keeps it.
		if prev != nil && prev.Deleted && prev.DeletedAt != 0 && (e.DeletedAt == 0 || prev.DeletedAt < e.DeletedAt) {
			e.DeletedAt = prev.DeletedAt
		}
		if e.DeletedAt == 0 || e.DeletedAt > time.Now().UnixNano() {
			e.DeletedAt = time.Now().UnixNano()
		}
	} else {
		e.DeletedAt = 0
	}
	d.entries[e.Path] = e.clone()
	d.gen++
	// This machine's counter must stay ahead of every version carrying its
	// name - including ones it learns back from peers after its own journal
	// was lost (a reinstalled phone). A counter that restarted at 1 made
	// every new local edit look older than the last one, and peers pushed
	// the old version back over it.
	if c := e.VV[d.me]; c > d.counter {
		d.counter = c
	}
	if d.observe != nil {
		d.observe(e.clone())
	}
	if d.closed {
		// Shutdown raced a scan: the in-memory state is updated, the log
		// line is the only casualty, and the next start replays whatever
		// was durably written.
		return nil
	}
	b, err := json.Marshal(record{Entry: e})
	if err != nil {
		return err
	}
	b = append(b, '\n')
	if d.torn {
		b = append([]byte{'\n'}, b...)
	}
	n, err := d.f.Write(b)
	if err != nil {
		// Cut the torn half off, or it joins the next record into one
		// unreadable line. The in-memory state stands; the next checkpoint
		// writes it out whole.
		d.cutTornLocked(n)
		return err
	}
	d.off += int64(n)
	d.torn = false
	return nil
}

// cutTornLocked undoes the n bytes a failed append left behind.
func (d *FileDB) cutTornLocked(n int) {
	if n <= 0 {
		return
	}
	if err := d.f.Truncate(d.off); err != nil {
		d.torn = true
		d.off += int64(n)
	}
}

// sameEntry reports whether two entries record the same state: same content,
// same shape, same version vector. Timestamps are part of the state (the
// scanner's fast path is size and mtime), so they are compared too.
func sameEntry(a, b *Entry) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.Blob != b.Blob || a.Size != b.Size || a.MTime != b.MTime ||
		a.Mode != b.Mode || a.Deleted != b.Deleted || a.Dir != b.Dir || a.Local != b.Local {
		return false
	}
	if len(a.VV) != len(b.VV) {
		return false
	}
	for k, v := range a.VV {
		if b.VV[k] != v {
			return false
		}
	}
	return true
}

// errEntryChanged is PutIf's answer when the entry moved on.
var errEntryChanged = errors.New("journal entry changed since it was read")

// PutIf writes e only if the entry at its path is still expect (nil: still
// absent). A decision taken on an entry read earlier - a peer's index
// handled while a local rename ran - must not write over what happened in
// between: that put renamed files back at their old paths.
func (d *FileDB) PutIf(expect, e *Entry) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !sameEntry(d.entries[e.Path], expect) {
		return errEntryChanged
	}
	return d.putLocked(e)
}

// Get returns a copy of the entry, or nil.
func (d *FileDB) Get(path string) *Entry {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.entries[path].clone()
}

// All returns every entry, sorted by path.
func (d *FileDB) All() []*Entry {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]*Entry, 0, len(d.entries))
	for _, p := range d.pathsLocked() {
		out = append(out, d.entries[p].clone())
	}
	return out
}

// Under returns the entries at path or beneath it, sorted. It scans without
// copying the entries it skips, which matters on a large store where a rename
// in a FUSE handler used to clone and sort the whole journal.
func (d *FileDB) Under(path string) []*Entry {
	prefix := path + "/"
	d.mu.Lock()
	var out []*Entry
	for p, ent := range d.entries {
		if p == path || strings.HasPrefix(p, prefix) {
			out = append(out, ent.clone())
		}
	}
	d.mu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out
}

// WithBlob returns the live entries holding this content, sorted by path.
func (d *FileDB) WithBlob(blob string) []*Entry {
	d.mu.Lock()
	var out []*Entry
	for _, ent := range d.entries {
		if ent.Blob == blob && !ent.Deleted && !ent.Dir {
			out = append(out, ent.clone())
		}
	}
	d.mu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out
}

// Count is the number of tracked paths (tombstones included).
func (d *FileDB) Count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.entries)
}

// LiveCount returns the number of non-deleted paths currently known. Tombstones
// remain in Count for conflict ordering and history, but are not files that a
// replica can materialise.
func (d *FileDB) LiveCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	count := 0
	for _, ent := range d.entries {
		if !ent.Deleted && !ent.Dir {
			count++
		}
	}
	return count
}

// Sync flushes the journal to disk; the scanner calls it once per batch.
func (d *FileDB) Sync() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil
	}
	return d.f.Sync()
}

// Close closes the log. Later writes are dropped rather than erroring,
// because cancellation and shutdown can race a scan by design.
func (d *FileDB) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil
	}
	d.closed = true
	return d.f.Close()
}

// Dominates reports whether a is a strict-or-equal descendant of b (a saw
// everything b saw, and possibly more).
func Dominates(a, b map[string]uint64) bool {
	for k, v := range b {
		if a[k] < v {
			return false
		}
	}
	return true
}

// Concurrent reports whether neither version vector dominates the other:
// both sides changed the file since their common ancestor.
func Concurrent(a, b map[string]uint64) bool {
	return !Dominates(a, b) && !Dominates(b, a)
}

// MergeVV is the join of two version vectors.
func MergeVV(a, b map[string]uint64) map[string]uint64 {
	out := make(map[string]uint64, len(a)+len(b))
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		if v > out[k] {
			out[k] = v
		}
	}
	return out
}

// winner picks the canonical content for a concurrent pair, deterministically
// on both sides: newer mtime wins, ties break on the blob hash so every
// replica computes the same answer and the conflict converges.
func winner(local, remote *Entry) *Entry {
	if local.MTime != remote.MTime {
		if local.MTime > remote.MTime {
			return local
		}
		return remote
	}
	if local.Blob <= remote.Blob {
		return local
	}
	return remote
}
