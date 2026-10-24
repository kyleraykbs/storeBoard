package syncer

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// Version is one recorded version of one file: the bytes (a blob in the
// content store, which is append-only, so the bytes are already there), when
// this machine observed it, and what it looked like.
type Version struct {
	Time    int64  `json:"time"` // unix nanoseconds: when we recorded it
	Blob    string `json:"blob,omitempty"`
	Size    int64  `json:"size"`
	MTime   int64  `json:"mtime,omitempty"`
	Mode    uint32 `json:"mode,omitempty"`
	Deleted bool   `json:"deleted,omitempty"`
}

// History is a store's version log: an append-only NDJSON file under the
// data directory, replayed into memory at open (then checkpointed, like the
// journals) and rewritten only when retention prunes it.
//
// Retention is the thinning schedule: the newest version of each hour for a
// day, of each day for a week, of each week for a month, of each month for a
// year, and of each year beyond that — and nothing older than the keep
// window at all. The current version of a file is never dropped.
type History struct {
	mu      sync.Mutex
	path    string
	f       *os.File
	byPath  map[string][]Version
	keep    time.Duration
	closed  bool
	pruned  int
	totalIn int
}

// OpenHistory replays the log at path (absent: a fresh history) and rewrites
// it compacted, exactly as the journals do.
func OpenHistory(path string, keep time.Duration) (*History, error) {
	if keep <= 0 {
		keep = DefaultKeep
	}
	h := &History{path: path, byPath: map[string][]Version{}, keep: keep}

	if f, err := os.Open(path); err == nil {
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
		for sc.Scan() {
			line := sc.Bytes()
			if len(line) == 0 {
				continue
			}
			var rec struct {
				Path    string  `json:"path"`
				Version Version `json:"version"`
			}
			if err := json.Unmarshal(line, &rec); err != nil {
				continue // a torn final line after a crash, as with the journals
			}
			h.byPath[rec.Path] = append(h.byPath[rec.Path], rec.Version)
		}
		f.Close()
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("history: %w", err)
	}

	if err := h.rewriteLocked(); err != nil {
		return nil, err
	}
	return h, nil
}

// DefaultKeep is the retention window when a machine declares none.
const DefaultKeep = 365 * 24 * time.Hour

// Record appends a version of path, unless it duplicates the last one (same
// blob and same deletion state): a scan that re-observes a file must not add
// a line, or the log would grow with every pass.
func (h *History) Record(path string, v Version) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	if last := h.byPath[path]; len(last) > 0 {
		prev := last[len(last)-1]
		if prev.Blob == v.Blob && prev.Deleted == v.Deleted {
			return
		}
	}
	h.byPath[path] = append(h.byPath[path], v)
	h.totalIn++
	h.appendLocked(path, v)
}

// Versions returns the recorded versions of path, newest first.
func (h *History) Versions(path string) []Version {
	h.mu.Lock()
	defer h.mu.Unlock()
	vs := h.byPath[path]
	out := make([]Version, len(vs))
	for i, v := range vs {
		out[len(vs)-1-i] = v
	}
	return out
}

// Prune applies the retention schedule and returns the blobs that no longer
// appear in any kept version (the caller may collect them, after checking
// nothing else references them). The newest version of every file is always
// kept, and so is anything inside the keep window's schedule.
func (h *History) Prune(now time.Time) []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil
	}

	var orphaned []string
	kept := map[string][]Version{}
	keptBlobs := map[string]bool{}
	for path, versions := range h.byPath {
		keepIdx := keepMask(versions, now, h.keep)
		var out []Version
		for i, v := range versions {
			if keepIdx[i] {
				out = append(out, v)
				if v.Blob != "" {
					keptBlobs[v.Blob] = true
				}
			}
		}
		if len(out) > 0 {
			kept[path] = out
		}
		h.pruned += len(versions) - len(out)
	}
	for _, versions := range h.byPath {
		for _, v := range versions {
			if v.Blob != "" && !keptBlobs[v.Blob] {
				orphaned = append(orphaned, v.Blob)
			}
		}
	}
	h.byPath = kept
	if err := h.rewriteLocked(); err != nil {
		// The in-memory view is pruned either way; the log will be rewritten
		// on the next Prune. Reporting nothing keeps the caller from acting
		// on a prune that did not reach disk.
		return nil
	}
	return orphaned
}

// keepMask decides, for a file's versions in chronological order, which to
// keep: the newest of each hour for a day, of each day for a week, of each
// week for a month, of each month for a year, of each year beyond — and
// nothing older than keep. The last version is always kept.
func keepMask(versions []Version, now time.Time, keep time.Duration) []bool {
	out := make([]bool, len(versions))
	if len(versions) == 0 {
		return out
	}
	out[len(versions)-1] = true // the current version, always
	seen := map[string]bool{}
	// Walk newest to oldest: the first version of each bucket survives.
	for i := len(versions) - 1; i >= 0; i-- {
		v := versions[i]
		age := now.Sub(time.Unix(0, v.Time))
		switch {
		case age < 0:
			out[i] = true // clock skew: keep rather than throw away
		case age > keep:
			out[i] = false // past the deadline, whatever the schedule says
		default:
			key := bucketKey(age)
			if !seen[key] {
				seen[key] = true
				out[i] = true
			}
		}
	}
	return out
}

// bucketKey names the retention bucket an age falls in. Finer buckets apply
// close in (hours, then days, weeks, months), coarser ones far out (years).
func bucketKey(age time.Duration) string {
	h := int64(age.Hours())
	switch {
	case h < 24:
		return fmt.Sprintf("h%d", h)
	case h < 24*7:
		return fmt.Sprintf("d%d", h/24)
	case h < 24*28:
		return fmt.Sprintf("w%d", h/(24*7))
	case h < 24*365:
		return fmt.Sprintf("m%d", h/(24*30))
	default:
		return fmt.Sprintf("y%d", h/(24*365))
	}
}

// rewriteLocked rewrites the log from the in-memory view: temp file, fsync,
// rename — the same discipline as every other write.
func (h *History) rewriteLocked() error {
	if err := os.MkdirAll(filepath.Dir(h.path), 0o700); err != nil {
		return err
	}
	tmp := h.path + ".new"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	w := bufio.NewWriterSize(f, 1<<16)
	paths := make([]string, 0, len(h.byPath))
	for p := range h.byPath {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	for _, p := range paths {
		for _, v := range h.byPath[p] {
			line, err := json.Marshal(struct {
				Path    string  `json:"path"`
				Version Version `json:"version"`
			}{p, v})
			if err != nil {
				continue
			}
			w.Write(line)
			w.WriteByte('\n')
		}
	}
	if err := w.Flush(); err != nil {
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
	if err := os.Rename(tmp, h.path); err != nil {
		os.Remove(tmp)
		return err
	}
	return nil
}

func (h *History) appendLocked(path string, v Version) {
	if h.f == nil {
		if err := os.MkdirAll(filepath.Dir(h.path), 0o700); err != nil {
			return
		}
		f, err := os.OpenFile(h.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			return
		}
		h.f = f
	}
	line, err := json.Marshal(struct {
		Path    string  `json:"path"`
		Version Version `json:"version"`
	}{path, v})
	if err != nil {
		return
	}
	h.f.Write(append(line, '\n'))
}

// Close flushes and releases the log.
func (h *History) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil
	}
	h.closed = true
	if h.f == nil {
		return nil
	}
	err := h.f.Close()
	h.f = nil
	return err
}

// VersionOf builds a version record from a journal entry.
func VersionOf(e *Entry, at time.Time) Version {
	return Version{
		Time:    at.UnixNano(),
		Blob:    e.Blob,
		Size:    e.Size,
		MTime:   e.MTime,
		Mode:    e.Mode,
		Deleted: e.Deleted,
	}
}

// record notes a version transition for a store that keeps history. The
// journal already holds the current state; this is the time series on top.
func (e *Engine) record(fs *StoreState, ent *Entry) {
	if fs.hist == nil || ent == nil {
		return
	}
	fs.hist.Record(ent.Path, VersionOf(ent, time.Now()))
}

// Paths lists the files the history knows, for the collector's reference set.
func (h *History) Paths() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]string, 0, len(h.byPath))
	for p := range h.byPath {
		out = append(out, p)
	}
	return out
}
