package syncer

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"codeberg.org/kyleraykbs/storeboard/internal/gatehouse"
	"codeberg.org/kyleraykbs/storeboard/internal/store"
	"codeberg.org/kyleraykbs/storeboard/pkg/api"
)

// Send copies a local file into one of this machine's stores, which is how a
// share sheet puts a photo on the mesh: the copy is an ordinary local change,
// so the scanner indexes it, the history records it, and the peers get it.
// dst names the destination inside the store (a relative path); empty uses
// the source's base name. Nothing is ever overwritten — a name already taken
// is parked under the deterministic conflict name first, and the caller is
// told where it landed.
//
// uid is the requesting user, from the socket's peer credentials: root and
// the store's owner may send, nobody else — and the source must be the
// caller's own file. The daemon runs as root and its socket is open to every
// local user, so a path is only a request: without the ownership check any
// user could have had the daemon read /etc/shadow into a store they own.
//
// The source is copied, never moved. Send used to rename it into the store,
// which removed the user's file from wherever it was — and, run as root, would
// have removed any file the daemon could reach.
func (e *Engine) Send(name string, uid int, src, dst string) (api.SendResult, error) {
	fs := e.byName[name]
	if fs == nil {
		return api.SendResult{}, fmt.Errorf("unknown store %q", name)
	}
	if err := e.checkOwner(fs, uid, gatehouse.Write, "send into"); err != nil {
		return api.SendResult{}, err
	}
	// The daemon's working directory is its data dir, not the caller's: a
	// relative path would name a file the user never meant. The CLI makes the
	// path absolute before it asks.
	if !filepath.IsAbs(src) {
		return api.SendResult{}, fmt.Errorf("source %q must be an absolute path", src)
	}
	info, err := os.Lstat(src)
	if err != nil {
		return api.SendResult{}, fmt.Errorf("source: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return api.SendResult{}, fmt.Errorf("%s is a symlink; send the file it points to", src)
	}
	if !info.Mode().IsRegular() {
		return api.SendResult{}, fmt.Errorf("%s is not a regular file (directories are not sent yet)", src)
	}
	in, info, err := openSendSource(src, uid)
	if err != nil {
		return api.SendResult{}, err
	}
	defer in.Close()
	if dst == "" {
		dst = filepath.Base(src)
	}
	if !validRel(dst) {
		return api.SendResult{}, fmt.Errorf("unsafe destination %q", dst)
	}

	if fs.stream {
		return e.sendStreamed(fs, in, src, dst, info)
	}

	fs.scanMu.Lock()
	defer fs.scanMu.Unlock()

	target := filepath.Join(fs.root, dst)
	if err := e.ensureDir(fs, filepath.Dir(target)); err != nil {
		return api.SendResult{}, err
	}
	parked := ""
	if _, serr := os.Lstat(target); serr == nil {
		// Taken: keep both. Parking the existing file keeps this a pure
		// addition from the sender's point of view.
		sum, herr := store.SumFile(target)
		if herr != nil {
			return api.SendResult{}, fmt.Errorf("destination %s exists and cannot be hashed: %w", dst, herr)
		}
		parked = conflictPath(dst, sum)
		if err := os.Rename(target, filepath.Join(fs.root, parked)); err != nil {
			return api.SendResult{}, fmt.Errorf("park existing %s: %w", dst, err)
		}
		if ent := fs.db.Get(dst); ent != nil {
			moved := ent.clone()
			moved.Path = parked
			// The parked name may already have a history (a tombstone, an
			// earlier copy); the new version must dominate it, not race it.
			var seen map[string]uint64
			if prev := fs.db.Get(parked); prev != nil {
				seen = prev.VV
			}
			moved.VV = MergeVV(seen, map[string]uint64{e.me: fs.db.Next()})
			_ = fs.db.Put(moved)
			e.record(fs, moved)
		}
	}
	if err := e.copyIntoStore(fs, in, target, info); err != nil {
		return api.SendResult{}, err
	}

	// Index it now rather than waiting for the next scan: the sender wants to
	// know it is on the mesh, not that it will be in fifteen seconds. A scan
	// of just-sent content is a walk of that store, which is cheap next to
	// the transfer the caller just made.
	fs.scanMu.Unlock()
	e.scanOnce(fs)
	fs.scanMu.Lock()

	e.logger.Info("sent a file into the store", "store", name, "path", dst, "from", src)
	return api.SendResult{Store: name, Path: dst, Parked: parked, Size: info.Size()}, nil
}

// sendStreamed copies a file into a streamed store through its mount. The
// mount's own write path hashes, journals and queues the file, so this holds
// no lock the FUSE handlers need: taking the scan lock here deadlocked the
// send against the close that records it.
func (e *Engine) sendStreamed(fs *StoreState, in *os.File, src, dst string, info os.FileInfo) (api.SendResult, error) {
	if !fs.mountUp.Load() {
		return e.sendStreamedDirect(fs, in, src, dst, info)
	}
	target := filepath.Join(fs.root, dst)
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return api.SendResult{}, err
	}
	parked := ""
	if _, err := os.Lstat(target); err == nil {
		// Taken: park the existing file under its content-derived name. Both
		// paths are ordinary writes through the mount, so the move is
		// journalled and queued like any other change.
		sum, herr := store.SumFile(target)
		if herr != nil {
			return api.SendResult{}, fmt.Errorf("destination %s exists and cannot be hashed: %w", dst, herr)
		}
		parked = conflictPath(dst, sum)
		if err := os.Rename(target, filepath.Join(fs.root, parked)); err != nil {
			return api.SendResult{}, fmt.Errorf("park existing %s: %w", dst, err)
		}
	}
	if err := writeThroughMount(in, target, info); err != nil {
		return api.SendResult{}, err
	}
	// The queue owns the bytes until a holder takes them; make sure it is
	// flushed now rather than at the next tick.
	e.flushQueue(fs)
	e.logger.Info("sent a file into the streamed store", "store", fs.name, "path", dst, "from", src)
	return api.SendResult{Store: fs.name, Path: dst, Parked: parked, Size: info.Size()}, nil
}

// openSendSource opens the file a caller asked to send and proves it is
// theirs. The Lstat in Send is the readable refusal; this is the one that
// holds: the path is opened without following a symlink and without blocking
// on a fifo swapped in after the check, and the ownership test is made on the
// descriptor actually read, so a rename between check and open cannot point
// the daemon at somebody else's file.
func openSendSource(src string, uid int) (*os.File, os.FileInfo, error) {
	f, err := os.OpenFile(src, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, nil, fmt.Errorf("source: %w", err)
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, nil, fmt.Errorf("source: %w", err)
	}
	if !info.Mode().IsRegular() {
		f.Close()
		return nil, nil, fmt.Errorf("%s is not a regular file", src)
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		f.Close()
		return nil, nil, fmt.Errorf("%s: cannot read the file's owner", src)
	}
	if uid != 0 && int(st.Uid) != uid {
		f.Close()
		return nil, nil, fmt.Errorf("%s belongs to uid %d: only its owner or root may send it", src, st.Uid)
	}
	return f, info, nil
}

// copyIntoStore writes the sender's bytes at target the way every other write
// into a store happens (DESIGN invariant 3): a scratch file beside the target,
// fsync, then rename — so a crash leaves either nothing or the whole file,
// never a truncated photo the scanner would index and sync. The mtime is the
// source's, so a shared photo sorts by when it was taken, not when it was sent.
func (e *Engine) copyIntoStore(fs *StoreState, in *os.File, target string, info os.FileInfo) error {
	f, err := os.CreateTemp(filepath.Dir(target), tmpPrefix+"*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	fail := func(err error) error {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if _, err := io.Copy(f, in); err != nil {
		return fail(fmt.Errorf("copy into the store: %w", err))
	}
	if err := f.Chmod(info.Mode().Perm()); err != nil {
		return fail(err)
	}
	if err := f.Sync(); err != nil {
		return fail(err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	e.chownPath(fs, tmp)
	if err := os.Chtimes(tmp, info.ModTime(), info.ModTime()); err != nil {
		e.logger.Warn("send: set mtime", "store", fs.name, "path", target, "error", err)
	}
	if err := os.Rename(tmp, target); err != nil {
		os.Remove(tmp)
		return err
	}
	return nil
}

// writeThroughMount copies the sender's bytes into a streamed store. The mount
// is the store's write path: it hashes, journals and queues on RELEASE, so a
// plain exclusive create is the whole protocol, and a scratch name would only
// be journalled and renamed by the mount for nothing.
func writeThroughMount(in *os.File, target string, info os.FileInfo) error {
	out, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, info.Mode().Perm())
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		os.Remove(target)
		return fmt.Errorf("copy into the store: %w", err)
	}
	if err := out.Sync(); err != nil {
		out.Close()
		os.Remove(target)
		return err
	}
	return out.Close()
}

// sendStreamedDirect is a send into a streamed store with no mount: a phone,
// where apps get no FUSE, serves streamed stores from the journal only. The
// write goes through the same mutator the mount would call, so it is
// journalled and queued exactly as a write through the mount is.
func (e *Engine) sendStreamedDirect(fs *StoreState, in *os.File, src, dst string, info os.FileInfo) (api.SendResult, error) {
	m := &storeMutator{e: e, fs: fs}
	parked := ""
	if ent := fs.db.Get(dst); ent != nil && !ent.Deleted && !ent.Dir {
		parked = conflictPath(dst, ent.Blob)
		if err := m.Rename(dst, parked); err != nil {
			return api.SendResult{}, fmt.Errorf("park existing %s: %w", dst, err)
		}
	}
	pf, err := m.Create(dst, uint32(info.Mode().Perm()))
	if err != nil {
		return api.SendResult{}, err
	}
	buf := make([]byte, 1<<20)
	var off int64
	for {
		n, rerr := in.Read(buf)
		if n > 0 {
			if _, werr := pf.WriteAt(buf[:n], off); werr != nil {
				pf.Close()
				return api.SendResult{}, werr
			}
			off += int64(n)
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			pf.Close()
			return api.SendResult{}, rerr
		}
	}
	if err := pf.Sync(); err != nil {
		pf.Close()
		return api.SendResult{}, err
	}
	if err := pf.Close(); err != nil {
		return api.SendResult{}, err
	}
	e.flushQueue(fs)
	e.logger.Info("sent a file into the streamed store", "store", fs.name, "path", dst, "from", src, "mount", false)
	return api.SendResult{Store: fs.name, Path: dst, Parked: parked, Size: off}, nil
}
