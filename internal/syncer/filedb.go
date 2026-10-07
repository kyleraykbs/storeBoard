package syncer

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
)

// Entry is one file's journal record: where it is in a folder, what bytes it
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

// FileDB is a folder's journal: an append-only NDJSON log replayed into an
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
}

// OpenFileDB replays the log at path (absent: a fresh journal) and rewrites
// it compacted.
func OpenFileDB(path, me string) (*FileDB, error) {
	d := &FileDB{me: me, logPath: path, entries: map[string]*Entry{}}

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
				// A torn final line after a crash is expected; anything
				// else would have been written whole (one append per entry).
				continue
			}
			if r.Entry != nil {
				d.entries[r.Entry.Path] = r.Entry
			}
		}
		f.Close()
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("journal: %w", err)
	}

	for _, e := range d.entries {
		if c := e.VV[me]; c > d.counter {
			d.counter = c
		}
	}

	// Checkpoint: the log becomes exactly the current state.
	tmp := path + ".new"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, fmt.Errorf("journal: %w", err)
	}
	w := bufio.NewWriter(f)
	for _, p := range d.pathsLocked() {
		b, err := json.Marshal(record{Entry: d.entries[p]})
		if err != nil {
			f.Close()
			return nil, err
		}
		w.Write(b)
		w.WriteByte('\n')
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return nil, err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return nil, err
	}
	if err := f.Close(); err != nil {
		return nil, err
	}
	if err := os.Rename(tmp, path); err != nil {
		return nil, err
	}

	f, err = os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("journal: %w", err)
	}
	d.f = f
	return d, nil
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
	d.entries[e.Path] = e.clone()
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
	if _, err := d.f.Write(b); err != nil {
		return err
	}
	return nil
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

// Count is the number of tracked paths (tombstones included).
func (d *FileDB) Count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.entries)
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
