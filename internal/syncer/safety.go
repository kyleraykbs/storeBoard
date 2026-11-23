package syncer

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"codeberg.org/kyleraykbs/storeboard/internal/store"
)

// safetyState is the replica's data-safety bookkeeping. The zero value is
// ready to use.
type safetyState struct {
	mu sync.Mutex
	// backoff holds paths whose fetch is paused until the given time: a
	// transfer that failed for lack of space would otherwise be asked for,
	// downloaded and thrown away again on every index exchange, forever.
	backoff map[string]time.Time
}

// diskReserve is the free space a transfer must leave behind, on the data
// disk and on the store's own. Tests shrink it.
var diskReserve int64 = 64 << 20

// spaceBackoff is how long a fetch that did not fit waits before it is tried
// again.
var spaceBackoff = 10 * time.Minute

// isNoSpace reports whether err is a full disk or an exhausted quota.
func isNoSpace(err error) bool {
	return errors.Is(err, syscall.ENOSPC) || errors.Is(err, syscall.EDQUOT)
}

// fsFree is the space an unprivileged writer can still use at path, and
// the filesystem's id, so two paths on one disk are counted once.
func fsFree(path string) (int64, [2]int32, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, [2]int32{}, err
	}
	return int64(st.Bavail) * int64(st.Bsize), st.Fsid.X__val, nil
}

// roomFor reports whether a version of size bytes can be received and placed
// without filling either disk it touches: the data directory (where the
// transfer is staged) and the store root (where it lands). A path that cannot
// be measured is not a reason to refuse.
func (e *Engine) roomFor(fs *StoreState, size int64) (bool, string) {
	if size < 0 {
		size = 0
	}
	need := size + diskReserve
	var seen [][2]int32
	for _, p := range []string{filepath.Join(e.dataDir, "tmp"), fs.root} {
		free, id, err := fsFree(p)
		if err != nil || slices.Contains(seen, id) {
			// Unmeasurable, or the same disk as before: the staged copy is
			// renamed into place there, so it is counted once.
			continue
		}
		seen = append(seen, id)
		if free < need {
			return false, fmt.Sprintf("%s has %d bytes free, %d needed", p, free, need)
		}
	}
	return true, ""
}

func backoffKey(fs *StoreState, path string) string { return fs.name + "\x00" + path }

// fetchBackedOff reports whether fetching path is paused.
func (e *Engine) fetchBackedOff(fs *StoreState, path string) bool {
	e.safety.mu.Lock()
	defer e.safety.mu.Unlock()
	until, ok := e.safety.backoff[backoffKey(fs, path)]
	if !ok {
		return false
	}
	if time.Now().After(until) {
		delete(e.safety.backoff, backoffKey(fs, path))
		return false
	}
	return true
}

// backOffFetch pauses fetching path for spaceBackoff.
func (e *Engine) backOffFetch(fs *StoreState, path, why string) {
	e.safety.mu.Lock()
	if e.safety.backoff == nil {
		e.safety.backoff = map[string]time.Time{}
	}
	if len(e.safety.backoff) > 65536 {
		// Bounded: a forgotten pause only costs one more attempt.
		now := time.Now()
		for k, t := range e.safety.backoff {
			if now.After(t) {
				delete(e.safety.backoff, k)
			}
		}
	}
	e.safety.backoff[backoffKey(fs, path)] = time.Now().Add(spaceBackoff)
	e.safety.mu.Unlock()
	e.logger.Warn("not enough disk space to fetch; retrying later", "store", fs.name, "path", path, "why", why, "retryIn", spaceBackoff)
}

// canFetch reports whether a version may be asked for now: it is not backed
// off, and there is room for it.
func (e *Engine) canFetch(fs *StoreState, ent *Entry) bool {
	if fs.stream || ent == nil {
		return true
	}
	if e.fetchBackedOff(fs, ent.Path) {
		return false
	}
	if ok, why := e.roomFor(fs, ent.Size); !ok {
		e.backOffFetch(fs, ent.Path, why)
		return false
	}
	return true
}

// fileState is what a writer saw at a path before replacing it. Re-checking it
// just before the final rename is how a write made while the new bytes were
// being copied is told apart from the version the caller decided to replace.
type fileState struct {
	exists bool
	size   int64
	mtime  int64
	// keep names the blob the file holds when the journal vouches for it. The
	// displaced file then goes into the content store instead of away: a
	// peer may still want exactly that version (a conflict copy of it, most
	// often), and it would otherwise exist nowhere.
	keep string
}

func statFile(path string) fileState {
	info, err := os.Lstat(path)
	if err != nil {
		return fileState{}
	}
	return fileState{exists: true, size: info.Size(), mtime: info.ModTime().UnixNano()}
}

// same reports whether the path still looks the way it did.
func (s fileState) same(path string) bool {
	now := statFile(path)
	return now.exists == s.exists && (!s.exists || (now.size == s.size && now.mtime == s.mtime))
}

// parkLocked moves the file at rel aside under its conflict name and journals
// the copy. Parking is a rename inside the tree: it needs no space and no copy
// into the content store, so it works on a full disk. It reports (nil, nil)
// when there is nothing at rel to park. Callers hold scanMu.
func (e *Engine) parkLocked(fs *StoreState, rel string) (*Entry, error) {
	if fs.stream {
		return nil, nil
	}
	src := filepath.Join(fs.root, rel)
	before, err := os.Lstat(src)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !before.Mode().IsRegular() {
		return nil, fmt.Errorf("%s is not a regular file", rel)
	}
	sum, err := store.SumFile(src)
	if err != nil {
		return nil, fmt.Errorf("hash %s: %w", rel, err)
	}
	name := conflictPath(rel, sum)
	dst := filepath.Join(fs.root, name)
	if err := os.Rename(src, dst); err != nil {
		return nil, fmt.Errorf("park %s: %w", rel, err)
	}
	info, err := os.Lstat(dst)
	if err != nil {
		info = before
	} else if info.Size() != before.Size() || !info.ModTime().Equal(before.ModTime()) {
		// Written while it was hashed: describe what was actually parked.
		if s, herr := store.SumFile(dst); herr == nil {
			sum = s
		}
	}
	e.chownPath(fs, dst)
	var seen map[string]uint64
	if prev := fs.db.Get(name); prev != nil {
		seen = prev.VV
	}
	ent := &Entry{
		Path:  name,
		Blob:  sum,
		Size:  info.Size(),
		MTime: info.ModTime().UnixNano(),
		Mode:  uint32(info.Mode().Perm()),
		VV:    MergeVV(seen, map[string]uint64{e.me: fs.db.Next()}),
		Local: true,
	}
	if err := fs.db.Put(ent); err != nil {
		e.logger.Warn("journal conflict copy", "store", fs.name, "path", name, "error", err)
	}
	return ent, nil
}

// displacedGrace is how long a displaced version stays in the content store
// for a peer that may still ask for it. It is written as the blob's mtime
// (now + grace): the collector keeps anything that new, and the stamp
// survives a restart.
var displacedGrace = time.Hour

// keepDisplacedLocked moves the file at dst - a version the journal vouches
// for, about to be replaced - into the content store under before.keep. It
// fails rather than drop bytes it cannot account for: an edit that appeared
// meanwhile is parked instead. Callers hold scanMu.
func (e *Engine) keepDisplacedLocked(fs *StoreState, dst string, before fileState) error {
	hold, err := e.store.Temp()
	if err != nil {
		return err
	}
	holdPath := hold.Name()
	hold.Close()
	rel, _ := filepath.Rel(fs.root, dst)
	if err := os.Rename(dst, holdPath); err == nil {
		if !before.same(holdPath) {
			// Written to between the check and the move: that is an edit,
			// not the version the journal names. Put it back and park it.
			if rerr := os.Rename(holdPath, dst); rerr != nil {
				return fmt.Errorf("%s changed while being replaced and could not be put back: %w", rel, rerr)
			}
			_, perr := e.parkLocked(fs, rel)
			return perr
		}
		when := time.Now().Add(displacedGrace)
		_ = os.Chtimes(holdPath, when, when)
		if aerr := e.store.Adopt(holdPath, before.keep); aerr != nil {
			if rerr := os.Rename(holdPath, dst); rerr != nil {
				return fmt.Errorf("keep displaced %s: %w (and putting it back: %v)", rel, aerr, rerr)
			}
			return aerr
		}
		return nil
	} else if !errors.Is(err, syscall.EXDEV) {
		os.Remove(holdPath)
		return err
	}
	os.Remove(holdPath)
	// The tree is on another disk: copy, hashing while copying.
	stored, err := e.adoptFile(dst, before.keep)
	if err != nil {
		return err
	}
	if stored != before.keep {
		// Not the journal's version after all: an edit. Park it.
		_, perr := e.parkLocked(fs, rel)
		return perr
	}
	when := time.Now().Add(displacedGrace)
	_ = os.Chtimes(e.store.BlobPath(stored), when, when)
	return nil
}

// reapIncoming abandons transfers that have not moved for wantRefresh: the
// sender went away mid-file, and the open descriptor and partial temp file
// would otherwise stay until a restart.
func (e *Engine) reapIncoming(now time.Time) int {
	var dead []*incomingBlob
	e.mu.Lock()
	for key, inc := range e.incoming {
		if inc.last.IsZero() || now.Sub(inc.last) <= wantRefresh {
			continue
		}
		delete(e.incoming, key)
		dead = append(dead, inc)
	}
	e.mu.Unlock()
	for _, inc := range dead {
		inc.f.Close()
		os.Remove(inc.f.Name())
		e.logger.Info("abandoned a stalled incoming transfer", "store", inc.store, "path", inc.path)
	}
	return len(dead)
}

// scratchTombstoneAge is how long a tombstone of an editor's or the daemon's
// own scratch file is kept.
var scratchTombstoneAge = 7 * 24 * time.Hour

// isScratchPath reports whether a path is a temporary file an editor or the
// daemon writes and renames away: its tombstone protects nothing worth a
// journal line forever.
func isScratchPath(p string) bool {
	base := filepath.Base(p)
	return strings.HasPrefix(base, ".goutputstream-") || strings.HasPrefix(base, tmpPrefix)
}

// rootID is a store root's identity: the device, inode and (where the
// filesystem records it) birth time of the directory. A disk that failed to
// mount leaves a different directory at the path - the bare mountpoint, or one
// recreated beneath it - and that is the one fact that tells a missing library
// from a deleted one. The birth time tells a directory recreated on the same
// disk apart even when the inode number is reused.
type rootID struct {
	dev, ino uint64
	birth    int64
}

func (r rootID) String() string {
	return strconv.FormatUint(r.dev, 10) + " " + strconv.FormatUint(r.ino, 10) + " " + strconv.FormatInt(r.birth, 10)
}

func statRootID(root string) (rootID, bool) {
	var stx unix.Statx_t
	if err := unix.Statx(unix.AT_FDCWD, root, 0, unix.STATX_INO|unix.STATX_BTIME, &stx); err != nil {
		return rootID{}, false
	}
	id := rootID{dev: unix.Mkdev(stx.Dev_major, stx.Dev_minor), ino: stx.Ino}
	if stx.Mask&unix.STATX_BTIME != 0 {
		id.birth = stx.Btime.Sec*1e9 + int64(stx.Btime.Nsec)
	}
	return id, true
}

func (e *Engine) rootIDPath(fs *StoreState) string {
	return filepath.Join(e.dataDir, "roots", fs.name)
}

// recordedRootID is the identity the root had when a scan last found its
// files where they belong.
func (e *Engine) recordedRootID(fs *StoreState) (rootID, bool) {
	b, err := os.ReadFile(e.rootIDPath(fs))
	if err != nil {
		return rootID{}, false
	}
	parts := strings.Fields(string(b))
	if len(parts) != 3 {
		return rootID{}, false
	}
	dev, err1 := strconv.ParseUint(parts[0], 10, 64)
	ino, err2 := strconv.ParseUint(parts[1], 10, 64)
	birth, err3 := strconv.ParseInt(parts[2], 10, 64)
	if err1 != nil || err2 != nil || err3 != nil {
		return rootID{}, false
	}
	return rootID{dev: dev, ino: ino, birth: birth}, true
}

func (e *Engine) recordRootID(fs *StoreState, id rootID) {
	if cur, ok := e.recordedRootID(fs); ok && cur == id {
		return
	}
	p := e.rootIDPath(fs)
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		return
	}
	tmp := p + ".new"
	if err := os.WriteFile(tmp, []byte(id.String()+"\n"), 0o600); err != nil {
		os.Remove(tmp)
		return
	}
	if err := os.Rename(tmp, p); err != nil {
		os.Remove(tmp)
	}
}

// scratchMaxAge is how old a leftover scratch file in a store root must be
// before the scanner removes it: older than any write the daemon makes.
var scratchMaxAge = time.Hour

// staleScratch reports whether a scratch file was abandoned: neither its
// contents (mtime) nor its metadata (ctime: a chmod, chtimes or rename) moved
// for scratchMaxAge. The daemon stamps a scratch file with the version's own
// mtime before renaming it into place, so mtime alone says nothing.
func staleScratch(info os.FileInfo, now time.Time) bool {
	last := info.ModTime()
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		if c := time.Unix(st.Ctim.Sec, st.Ctim.Nsec); c.After(last) {
			last = c
		}
	}
	return now.Sub(last) > scratchMaxAge
}

// trashTime is when a file entered the trash: its ctime, which the move (and
// the stamp applyDelete puts on it) set, or its mtime if that is later. A
// file's own mtime is when it was last written - years ago for an old photo -
// and expiring by it purged a fresh deletion at the next prune.
func trashTime(info os.FileInfo) time.Time {
	t := info.ModTime()
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		if c := time.Unix(st.Ctim.Sec, st.Ctim.Nsec); c.After(t) {
			t = c
		}
	}
	return t
}
