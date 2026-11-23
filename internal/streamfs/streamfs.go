// Package streamfs presents a store as a mounted filesystem without keeping
// its bytes on this machine.
//
// The journal is the whole truth about the tree: directories and file names
// come from it, so the layout exists even when no holder answers. File
// contents are fetched per open, through a Fetcher the sync engine provides —
// there is no disk cache, because the point of a streamed store is that this
// machine stores metadata and not photographs. An open handle keeps a small,
// bounded in-memory readahead window so a sequential reader is not paced by
// one network round trip per kernel request, and the mount keeps a bounded
// in-memory cache of recent blocks so reopening a file does not fetch it
// again.
package streamfs

import (
	"context"
	"errors"
	"hash/fnv"
	"io"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// Entry is one path the journal knows: a directory, or a file with the blob
// that holds its bytes.
type Entry struct {
	Path  string
	Dir   bool
	Blob  string
	Size  int64
	MTime time.Time
	Mode  uint32
}

// Fetcher opens one byte range of an entry. A streamed filesystem issues one
// request per window, so it never needs a local file-sized cache.
type Fetcher interface {
	ReadRange(ctx context.Context, entry Entry, offset, length int64) ([]byte, error)
}

// stableIno derives an inode number from a path and its kind, so one path
// keeps one identity for the life of the mount. go-fuse otherwise hands out a
// fresh number per Lookup, and a file manager - which keys selections,
// refresh and "is this the same folder" on (mount, inode) - sees the whole
// tree churn on every read. `ls` never notices; Nautilus falls apart.
//
// The kind is part of the identity: go-fuse returns an already-registered
// inode for a known number, so a path that changes from a file into a
// directory must not reuse the file's number, or the kernel would be handed
// a regular-file inode for a directory.
func stableIno(p string, dir bool) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(p))
	if dir {
		// No journal path ends in "/", so this cannot collide with a file.
		_, _ = h.Write([]byte{'/'})
	}
	v := h.Sum64()
	// 1 is the root and 0 means "unassigned" to go-fuse.
	if v < 2 {
		v += 2
	}
	return v
}

func cleanPath(p string) string {
	clean := strings.TrimPrefix(path.Clean("/"+p), "/")
	if clean == "." {
		return ""
	}
	return clean
}

func parentOf(p string) string {
	if i := strings.LastIndexByte(p, '/'); i >= 0 {
		return p[:i]
	}
	return ""
}

func baseOf(p string) string {
	return p[strings.LastIndexByte(p, '/')+1:]
}

func joinPath(dir, name string) string {
	if dir == "" {
		return name
	}
	return dir + "/" + name
}

// Tree is the journal's view. It is indexed by directory, so a lookup or a
// listing costs the size of one directory rather than the size of the store:
// a photo library with a hundred thousand entries used to scan every one of
// them for each folder a file manager opened.
//
// It can be replaced wholesale (at mount) or updated one path at a time (as
// the journal changes), and tells the mount which paths moved so the kernel's
// cached names can be dropped.
type Tree struct {
	mu sync.RWMutex
	// entries holds the explicit journal records: files, and directories
	// that would otherwise be invisible.
	entries map[string]Entry
	// under counts the explicit entries at or below each path; a path with a
	// count exists, as a directory when nothing explicit says otherwise.
	under map[string]int
	// kids names each directory's immediate children.
	kids map[string]map[string]struct{}

	notifyMu sync.Mutex
	notify   func(paths []string)
}

// NewTree builds a tree from a path→entry map.
func NewTree(entries map[string]Entry) *Tree {
	t := &Tree{}
	t.Replace(entries)
	return t
}

func (t *Tree) setNotify(fn func([]string)) {
	t.notifyMu.Lock()
	t.notify = fn
	t.notifyMu.Unlock()
}

func (t *Tree) changed(paths []string) {
	if len(paths) == 0 {
		return
	}
	t.notifyMu.Lock()
	fn := t.notify
	t.notifyMu.Unlock()
	if fn != nil {
		fn(paths)
	}
}

// addLocked records one explicit entry and returns the paths whose state
// changed: the entry itself and any directory it brought into existence.
func (t *Tree) addLocked(p string, e Entry, changed []string) []string {
	e.Path = p
	prev, existed := t.entries[p]
	t.entries[p] = e
	if existed {
		if prev != e {
			changed = append(changed, p)
		}
		return changed
	}
	changed = append(changed, p)
	for a := p; a != ""; a = parentOf(a) {
		t.under[a]++
		if t.under[a] == 1 {
			par := parentOf(a)
			m := t.kids[par]
			if m == nil {
				m = map[string]struct{}{}
				t.kids[par] = m
			}
			m[baseOf(a)] = struct{}{}
			if a != p {
				changed = append(changed, a)
			}
		}
	}
	return changed
}

// removeLocked drops one explicit entry and returns the paths whose state
// changed: the entry and every implied directory that is now empty.
func (t *Tree) removeLocked(p string, changed []string) []string {
	if _, ok := t.entries[p]; !ok {
		return changed
	}
	delete(t.entries, p)
	changed = append(changed, p)
	for a := p; a != ""; a = parentOf(a) {
		t.under[a]--
		if t.under[a] > 0 {
			continue
		}
		delete(t.under, a)
		par := parentOf(a)
		if m := t.kids[par]; m != nil {
			delete(m, baseOf(a))
			if len(m) == 0 {
				delete(t.kids, par)
			}
		}
		if a != p {
			changed = append(changed, a)
		}
	}
	return changed
}

// Replace installs a new snapshot of the journal.
func (t *Tree) Replace(entries map[string]Entry) {
	next := make(map[string]Entry, len(entries))
	for p, e := range entries {
		clean := cleanPath(p)
		if clean == "" {
			continue
		}
		e.Path = clean
		next[clean] = e
	}
	t.mu.Lock()
	var changed []string
	for p := range t.entries {
		if _, keep := next[p]; !keep {
			changed = t.removeLocked(p, changed)
		}
	}
	if t.entries == nil {
		t.entries = make(map[string]Entry, len(next))
		t.under = make(map[string]int, len(next))
		t.kids = map[string]map[string]struct{}{}
	}
	for p, e := range next {
		changed = t.addLocked(p, e, changed)
	}
	t.mu.Unlock()
	t.changed(changed)
}

// Set records or updates one path.
func (t *Tree) Set(e Entry) {
	p := cleanPath(e.Path)
	if p == "" {
		return
	}
	t.mu.Lock()
	changed := t.addLocked(p, e, nil)
	t.mu.Unlock()
	t.changed(changed)
}

// Delete forgets one path. Implied parent directories that held nothing else
// disappear with it.
func (t *Tree) Delete(p string) {
	p = cleanPath(p)
	if p == "" {
		return
	}
	t.mu.Lock()
	changed := t.removeLocked(p, nil)
	t.mu.Unlock()
	t.changed(changed)
}

// resolveLocked is what a path is: its explicit entry, or a directory implied
// by deeper entries. A file entry with entries beneath it reads as the
// directory - the same rule the engine applies when the two clash.
func (t *Tree) resolveLocked(p string) (Entry, bool) {
	if p == "" {
		return Entry{Dir: true, Mode: 0o755}, true
	}
	e, explicit := t.entries[p]
	n := t.under[p]
	if explicit && (e.Dir || n <= 1) {
		return e, true
	}
	if n > 0 {
		return Entry{Path: p, Dir: true, Mode: 0o755}, true
	}
	return Entry{}, false
}

// Get returns what one path is: an explicit entry or an implied directory.
func (t *Tree) Get(p string) (Entry, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.resolveLocked(p)
}

type dirChild struct {
	name  string
	entry Entry
}

// children lists the immediate children of a directory, sorted by name.
func (t *Tree) children(dir string) []dirChild {
	t.mu.RLock()
	defer t.mu.RUnlock()
	names := t.kids[dir]
	out := make([]dirChild, 0, len(names))
	for name := range names {
		if e, ok := t.resolveLocked(joinPath(dir, name)); ok {
			out = append(out, dirChild{name: name, entry: e})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
	return out
}

// Children lists the immediate children of a directory, sorted by name. Each
// child carries its full path; an implied directory is reported as one.
func (t *Tree) Children(dir string) []Entry {
	kids := t.children(dir)
	out := make([]Entry, 0, len(kids))
	for _, c := range kids {
		e := c.entry
		e.Path = joinPath(dir, c.name)
		out = append(out, e)
	}
	return out
}

// hasChildren reports whether a directory has anything beneath it.
func (t *Tree) hasChildren(dir string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.kids[dir]) > 0
}

type dirNode struct {
	fs.Inode
	fsys *FS
	mu   sync.Mutex
	path string
}

func (d *dirNode) getPath() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.path
}

var _ = (fs.NodeOnAdder)((*dirNode)(nil))
var _ = (fs.NodeOnForgetter)((*dirNode)(nil))

// OnAdd registers this directory's inode so a journal change can invalidate
// the exact parent/name entries the kernel may have cached.
func (d *dirNode) OnAdd(ctx context.Context) {
	p := d.getPath()
	d.fsys.mu.Lock()
	d.fsys.dirs[p] = d.EmbeddedInode()
	d.fsys.mu.Unlock()
}

// OnForget drops the registration once the kernel no longer holds the inode.
func (d *dirNode) OnForget() {
	p := d.getPath()
	d.fsys.mu.Lock()
	if d.fsys.dirs[p] == d.EmbeddedInode() {
		delete(d.fsys.dirs, p)
	}
	d.fsys.mu.Unlock()
}

var _ = (fs.NodeLookuper)((*dirNode)(nil))
var _ = (fs.NodeReaddirer)((*dirNode)(nil))
var _ = (fs.NodeGetattrer)((*dirNode)(nil))
var _ = (fs.NodeStatfser)((*dirNode)(nil))

// Statfs supplies the filesystem name limit Nautilus uses to validate a
// new folder name before it sends MKDIR, and the space a write can use. An
// unimplemented FUSE statfs makes GIO report a zero/unknown NAME_MAX, so every
// non-empty name appears "too long" in the New Folder dialog; a zero free
// count makes a file manager refuse a copy for lack of space.
func (d *dirNode) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	const bsize = 4096
	out.Bsize = bsize
	out.Frsize = bsize
	out.NameLen = 255
	if d.fsys.space != nil {
		total, free := d.fsys.space()
		if free > total {
			total = free
		}
		out.Blocks = total / bsize
		out.Bfree = free / bsize
		out.Bavail = free / bsize
	}
	out.Files = 1 << 32
	out.Ffree = 1 << 31
	return 0
}

// Lookup resolves one child name into an inode.
func (d *dirNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	child := joinPath(d.getPath(), name)
	entry, ok := d.fsys.tree.Get(child)
	if !ok {
		return nil, syscall.ENOENT
	}
	out.SetEntryTimeout(time.Second)
	// File sizes and mtimes can change when a journal update arrives; keep
	// directory entries briefly, but always ask Getattr for live attributes.
	out.SetAttrTimeout(0)
	if in := d.GetChild(name); in != nil {
		// The kernel still holds this name, so keep the identity and just
		// refresh what the journal says about it - unless the path changed
		// kind, which needs a new inode.
		switch n := in.Operations().(type) {
		case *fileNode:
			if !entry.Dir {
				n.update(child, entry)
				n.fill(&out.Attr, entry)
				return in, 0
			}
		case *dirNode:
			if entry.Dir {
				n.mu.Lock()
				n.path = child
				n.mu.Unlock()
				d.fsys.fillDir(&out.Attr)
				return in, 0
			}
		}
	}
	if entry.Dir {
		d.fsys.fillDir(&out.Attr)
		return d.NewInode(ctx, &dirNode{fsys: d.fsys, path: child}, fs.StableAttr{Mode: fuse.S_IFDIR, Ino: stableIno(child, true)}), 0
	}
	node := &fileNode{fsys: d.fsys, path: child, entry: entry}
	node.fill(&out.Attr, entry)
	return d.NewInode(ctx, node, fs.StableAttr{Mode: fuse.S_IFREG, Ino: stableIno(child, false)}), 0
}

// Readdir lists the children of this directory.
func (d *dirNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	dir := d.getPath()
	children := d.fsys.tree.children(dir)
	out := make([]fuse.DirEntry, 0, len(children))
	for _, c := range children {
		mode := uint32(fuse.S_IFREG)
		if c.entry.Dir {
			mode = fuse.S_IFDIR
		}
		out = append(out, fuse.DirEntry{
			Name: c.name,
			Mode: mode,
			Ino:  stableIno(joinPath(dir, c.name), c.entry.Dir),
		})
	}
	return fs.NewListDirStream(out), 0
}

// Getattr reports a directory's attributes.
func (d *dirNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	d.fsys.fillDir(&out.Attr)
	return 0
}

func (f *FS) fillDir(out *fuse.Attr) {
	out.Mode = fuse.S_IFDIR | 0o755
	out.Uid = f.uid
	out.Gid = f.gid
}

var _ = (fs.NodeCreater)((*dirNode)(nil))
var _ = (fs.NodeMkdirer)((*dirNode)(nil))
var _ = (fs.NodeUnlinker)((*dirNode)(nil))
var _ = (fs.NodeRmdirer)((*dirNode)(nil))
var _ = (fs.NodeRenamer)((*dirNode)(nil))

// Create makes a new file and hands back the handle that takes its bytes.
func (d *dirNode) Create(ctx context.Context, name string, flags, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	if d.fsys.mutator == nil {
		return nil, nil, 0, syscall.EROFS
	}
	child := joinPath(d.getPath(), name)
	pending, err := d.fsys.mutator.Create(child, mode&0o7777)
	if err != nil {
		return nil, nil, 0, errnoFor(err)
	}
	entry := Entry{Path: child, Mode: mode & 0o7777}
	node := &fileNode{fsys: d.fsys, path: child, entry: entry}
	node.fill(&out.Attr, entry)
	inode := d.NewInode(ctx, node, fs.StableAttr{Mode: fuse.S_IFREG, Ino: stableIno(child, false)})
	h := &fileHandle{fsys: d.fsys, entry: entry, write: pending}
	return inode, h, 0, 0
}

// Mkdir records an empty directory, which only exists because the journal
// says so.
func (d *dirNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if d.fsys.mutator == nil {
		return nil, syscall.EROFS
	}
	child := joinPath(d.getPath(), name)
	if err := d.fsys.mutator.MakeDir(child, mode&0o7777); err != nil {
		return nil, errnoFor(err)
	}
	d.fsys.fillDir(&out.Attr)
	return d.NewInode(ctx, &dirNode{fsys: d.fsys, path: child}, fs.StableAttr{Mode: fuse.S_IFDIR, Ino: stableIno(child, true)}), 0
}

// Unlink removes a file: the journal records the tombstone and the holder is
// told, so the deletion travels like any other.
func (d *dirNode) Unlink(ctx context.Context, name string) syscall.Errno {
	if d.fsys.mutator == nil {
		return syscall.EROFS
	}
	if err := d.fsys.mutator.Remove(joinPath(d.getPath(), name)); err != nil {
		return errnoFor(err)
	}
	return 0
}

// Rmdir removes an empty directory.
func (d *dirNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	if d.fsys.mutator == nil {
		return syscall.EROFS
	}
	if err := d.fsys.mutator.RemoveDir(joinPath(d.getPath(), name)); err != nil {
		return errnoFor(err)
	}
	return 0
}

// Rename moves a path. The daemon performs it on both the journal and, when a
// holder is reachable, the holder's own tree.
//
// Nothing here may ask the kernel to invalidate an entry: the kernel holds
// both parent directories locked for the whole rename, and an invalidation
// waits for that lock - a rename inside any subdirectory hung forever that
// way. The kernel moves its own dentry when the rename succeeds.
func (d *dirNode) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	if d.fsys.mutator == nil {
		return syscall.EROFS
	}
	parent, ok := newParent.(*dirNode)
	if !ok {
		return syscall.EXDEV
	}
	oldPath := joinPath(d.getPath(), name)
	newPath := joinPath(parent.getPath(), newName)
	child := d.GetChild(name)
	if err := d.fsys.mutator.Rename(oldPath, newPath); err != nil {
		return errnoFor(err)
	}
	// The kernel has already moved its dentry. The inode objects below it
	// still hold the old absolute paths, so a read straight after the rename
	// resolves against names that no longer exist. Rewrite the whole moved
	// subtree before returning.
	d.fsys.repath(child, oldPath, newPath)
	return 0
}

// fileNode is a file whose bytes come from a holder.
type fileNode struct {
	fs.Inode
	fsys  *FS
	mu    sync.Mutex
	path  string
	entry Entry
}

func (f *fileNode) snapshot() (string, Entry) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.path, f.entry
}

func (f *fileNode) update(p string, e Entry) {
	f.mu.Lock()
	f.path = p
	f.entry = e
	f.mu.Unlock()
}

// current refreshes the node from the journal and returns what it now says.
// The inode may have survived a journal replacement, and the kernel can ask
// for attributes of a cached dentry without looking it up again.
func (f *fileNode) current() (string, Entry) {
	p, e := f.snapshot()
	if f.fsys.tree == nil {
		return p, e
	}
	if cur, ok := f.fsys.tree.Get(p); ok && !cur.Dir {
		f.mu.Lock()
		if f.path == p {
			f.entry = cur
		}
		f.mu.Unlock()
		e = cur
	}
	return p, e
}

func (f *fileNode) fill(out *fuse.Attr, e Entry) {
	mode := e.Mode
	if mode == 0 {
		mode = 0o644
	}
	out.Mode = fuse.S_IFREG | mode&0o777
	out.Size = uint64(e.Size)
	out.Uid = f.fsys.uid
	out.Gid = f.fsys.gid
	if !e.MTime.IsZero() {
		mt := e.MTime
		out.SetTimes(nil, &mt, nil)
	}
}

var _ = (fs.NodeOpener)((*fileNode)(nil))
var _ = (fs.NodeGetattrer)((*fileNode)(nil))
var _ = (fs.NodeSetattrer)((*fileNode)(nil))

// Setattr honours truncate(2) and open(O_TRUNC), which is how a shell
// redirect or `cp` resets a file. Mode and times are not the filesystem's to
// decide - a store's permissions and mtimes come from the journal - so they
// are accepted and ignored rather than refused.
func (f *fileNode) Setattr(ctx context.Context, h fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	p, e := f.current()
	size, ok := in.GetSize()
	if !ok {
		if fh, isOurs := h.(*fileHandle); isOurs && fh.write != nil {
			e.Size = fh.getSize()
		}
		f.fill(&out.Attr, e)
		return 0
	}
	if fh, isOurs := h.(*fileHandle); isOurs && fh.write != nil {
		if err := fh.write.Truncate(int64(size)); err != nil {
			return errnoFor(err)
		}
		fh.setSize(int64(size))
	} else {
		if f.fsys.mutator == nil {
			return syscall.EROFS
		}
		if err := f.fsys.mutator.Truncate(p, int64(size)); err != nil {
			return errnoFor(err)
		}
	}
	f.mu.Lock()
	f.entry.Size = int64(size)
	f.mu.Unlock()
	e.Size = int64(size)
	f.fill(&out.Attr, e)
	return 0
}

// Getattr reports the size the journal recorded - or, while the file is open
// for writing, the size the write has grown it to. go-fuse asks the node, not
// the handle, whenever the node answers, so the node has to look.
func (f *fileNode) Getattr(ctx context.Context, h fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	_, e := f.current()
	if fh, isOurs := h.(*fileHandle); isOurs && fh.write != nil {
		e.Size = fh.getSize()
	}
	f.fill(&out.Attr, e)
	return 0
}

func (f *fileNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	wantWrite := flags&uint32(os.O_WRONLY|os.O_RDWR) != 0
	if f.fsys.fetcher == nil && !wantWrite {
		return nil, 0, syscall.ENOSYS
	}
	// Open whatever the journal says now: a handle reads one version from
	// start to end, even if a newer one arrives while it is open.
	p, entry := f.current()
	h := &fileHandle{fsys: f.fsys, entry: entry, size: entry.Size}
	if wantWrite {
		if f.fsys.mutator == nil {
			return nil, 0, syscall.EROFS
		}
		w, err := f.fsys.mutator.Open(p, flags&uint32(os.O_TRUNC) != 0)
		if err != nil {
			return nil, 0, errnoFor(err)
		}
		h.write = w
		if flags&uint32(os.O_TRUNC) != 0 {
			h.size = 0
		}
		return h, 0, 0
	}
	h.ra = newReadahead(f.fsys.fetcher, entry, f.fsys.cache)
	return h, 0, 0
}

type fileHandle struct {
	fsys  *FS
	entry Entry
	write PendingFile
	ra    *readahead

	mu   sync.Mutex
	size int64
}

func (h *fileHandle) getSize() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.size
}

func (h *fileHandle) setSize(n int64) {
	h.mu.Lock()
	h.size = n
	h.mu.Unlock()
}

var _ = (fs.FileReader)((*fileHandle)(nil))
var _ = (fs.FileWriter)((*fileHandle)(nil))
var _ = (fs.FileFlusher)((*fileHandle)(nil))
var _ = (fs.FileFsyncer)((*fileHandle)(nil))
var _ = (fs.FileGetattrer)((*fileHandle)(nil))
var _ = (fs.FileReleaser)((*fileHandle)(nil))

func (h *fileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	if h.write == nil {
		if h.ra == nil {
			return nil, syscall.EBADF
		}
		return h.ra.read(ctx, dest, off)
	}
	size := h.getSize()
	if off >= size {
		return fuse.ReadResultData(nil), 0
	}
	want := int64(len(dest))
	if rest := size - off; want > rest {
		want = rest
	}
	// The handle may already have overwritten part of the file: read
	// through it, so a writer sees its own bytes and the holder's
	// everywhere else.
	n, err := h.write.ReadAt(dest[:want], off)
	if err != nil && err != io.EOF {
		return nil, errnoFor(err)
	}
	return fuse.ReadResultData(dest[:n]), 0
}

func (h *fileHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	if h.write == nil {
		return 0, syscall.EBADF
	}
	n, err := h.write.WriteAt(data, off)
	if err != nil {
		if errors.Is(err, syscall.ENOSPC) {
			return uint32(n), syscall.ENOSPC
		}
		return uint32(n), syscall.EIO
	}
	h.mu.Lock()
	if end := off + int64(n); end > h.size {
		h.size = end
	}
	h.mu.Unlock()
	return uint32(n), 0
}

// Getattr on the handle reports the size this open has grown to: a caller
// that stats an open file mid-write must not see the size it had at open.
func (h *fileHandle) Getattr(ctx context.Context, out *fuse.AttrOut) syscall.Errno {
	mode := h.entry.Mode
	if mode == 0 {
		mode = 0o644
	}
	out.Mode = fuse.S_IFREG | mode&0o777
	out.Size = uint64(h.getSize())
	out.Uid = h.fsys.uid
	out.Gid = h.fsys.gid
	return 0
}

// Flush is sent on every close(2) of a descriptor that refers to the file, not
// once per open - a shell that dups its redirect target closes the original
// before it writes. It must therefore never finalise: the file is committed in
// Release, and a policy that refuses the data refuses it at write(2), which is
// where the caller can still act on it.
func (h *fileHandle) Flush(ctx context.Context) syscall.Errno {
	if h.write == nil {
		return 0
	}
	if err := h.write.Sync(); err != nil {
		return errnoFor(err)
	}
	return 0
}

// Fsync pushes the written bytes to the queue file. The bytes are on this
// machine's disk already; the holder receives them when the file is flushed.
func (h *fileHandle) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	return h.Flush(ctx)
}

func (h *fileHandle) Release(ctx context.Context) syscall.Errno {
	if h.ra != nil {
		h.ra.close()
	}
	if h.write == nil {
		return 0
	}
	if err := h.write.Close(); err != nil {
		return errnoFor(err)
	}
	return 0
}

// Readahead. A kernel read is at most 128 KiB, and a holder is a network round
// trip away: fetching exactly what each request asks for paces a sequential
// reader - a thumbnailer, a video player, `cp` - at one round trip per 128 KiB.
// A handle instead fetches aligned windows, and while the reader keeps going
// forward it keeps a growing number of windows in flight ahead of it. Memory
// is bounded per handle; nothing touches the disk.
const (
	raBlockSize = 256 << 10
	// raMaxAhead bounds how many windows a sequential reader keeps in flight.
	raMaxAhead = 8
)

type raBlock struct {
	done chan struct{}
	data []byte
	err  error
	// urgent is set once a reader waits on the block (see Urgent).
	urgent atomic.Bool
}

type readahead struct {
	fetcher Fetcher
	entry   Entry
	cache   *blockCache
	ctx     context.Context
	cancel  context.CancelFunc

	mu     sync.Mutex
	blocks map[int64]*raBlock
	next   int64 // offset just past the previous read
	ahead  int   // windows to fetch beyond the current read
}

func newReadahead(f Fetcher, entry Entry, cache *blockCache) *readahead {
	ctx, cancel := context.WithCancel(context.Background())
	return &readahead{fetcher: f, entry: entry, cache: cache, ctx: ctx, cancel: cancel, blocks: map[int64]*raBlock{}}
}

func (r *readahead) close() { r.cancel() }

// startLocked returns block k, starting its fetch if nobody has. Callers
// hold r.mu.
func (r *readahead) startLocked(k int64) *raBlock {
	if b := r.blocks[k]; b != nil {
		return b
	}
	off := k * raBlockSize
	if off >= r.entry.Size {
		return nil
	}
	n := min(int64(raBlockSize), r.entry.Size-off)
	b := &raBlock{done: make(chan struct{})}
	r.blocks[k] = b
	if data, ok := r.cache.get(r.entry.Blob, k); ok && int64(len(data)) == n {
		b.data = data
		close(b.done)
		return b
	}
	go func() {
		data, err := r.fetcher.ReadRange(withUrgency(r.ctx, &b.urgent), r.entry, off, n)
		if err == nil && int64(len(data)) != n {
			err = io.ErrUnexpectedEOF
		}
		if err == nil {
			r.cache.put(r.entry.Blob, k, data)
		}
		b.data, b.err = data, err
		close(b.done)
	}()
	return b
}

func (r *readahead) read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	size := r.entry.Size
	if off >= size || len(dest) == 0 {
		return fuse.ReadResultData(nil), 0
	}
	want := min(int64(len(dest)), size-off)
	first := off / raBlockSize
	last := (off + want - 1) / raBlockSize

	r.mu.Lock()
	// The kernel issues its own readahead concurrently and slightly out of
	// order, so "sequential" means the reader landed where we predicted, not
	// only exactly where the last read ended.
	_, predicted := r.blocks[first]
	if off == r.next || predicted {
		r.ahead = min(max(r.ahead*2, 1), raMaxAhead)
	} else {
		r.ahead = 0
	}
	needed := make([]*raBlock, 0, last-first+1)
	for k := first; k <= last; k++ {
		b := r.startLocked(k)
		if b != nil {
			b.urgent.Store(true)
		}
		needed = append(needed, b)
	}
	for k := last + 1; k <= last+int64(r.ahead); k++ {
		r.startLocked(k)
	}
	r.next = off + want
	// Keep only the window around the reader: the block before it (the
	// kernel's reads straddle block boundaries) through the readahead edge.
	for k := range r.blocks {
		if k < first-1 || k > last+int64(r.ahead) {
			delete(r.blocks, k)
		}
	}
	r.mu.Unlock()

	for i, b := range needed {
		if b == nil {
			return nil, syscall.EIO
		}
		select {
		case <-b.done:
		case <-ctx.Done():
			return nil, syscall.EINTR
		}
		k := first + int64(i)
		if b.err != nil {
			// Forget the failure so a retry asks again (a holder may have
			// failed over in the meantime).
			r.mu.Lock()
			if r.blocks[k] == b {
				delete(r.blocks, k)
			}
			r.mu.Unlock()
			return nil, errnoFor(b.err)
		}
		start := k * raBlockSize
		lo := max(off, start)
		hi := min(off+want, start+int64(len(b.data)))
		if hi > lo {
			copy(dest[lo-off:hi-off], b.data[lo-start:hi-start])
		}
	}
	return fuse.ReadResultData(dest[:want]), 0
}

// Mutator persists the changes a writable mount makes. The engine implements
// it: a mount with no mutator is read-only.
type Mutator interface {
	// Create starts a new file at path. The returned handle receives the
	// bytes and finalises them when closed.
	Create(path string, mode uint32) (PendingFile, error)
	// Open starts writing an existing file. Truncate decides whether the
	// holder's current bytes are kept as the starting point.
	Open(path string, truncate bool) (PendingFile, error)
	// Truncate sets a file's length. Growing pads with zeroes; shrinking
	// keeps the first size bytes.
	Truncate(path string, size int64) error
	Remove(path string) error
	MakeDir(path string, mode uint32) error
	RemoveDir(path string) error
	Rename(from, to string) error
}

// PendingFile is one file being written through the mount. Close is where the
// bytes are hashed, journalled and queued; it must be idempotent.
//
// ReadAt is not optional: a viewer opens an image read-write to edit its
// metadata, then reads it. Refusing that read made the file look empty
// ("Empty document (application/x-zerosize)") even though every other reader
// saw the real bytes.
type PendingFile interface {
	ReadAt(p []byte, off int64) (int, error)
	WriteAt(p []byte, off int64) (int, error)
	Truncate(size int64) error
	Sync() error
	Close() error
}

// Options configures one mount. A nil Mutator makes it read-only; UID and GID
// are the owner the kernel checks with default_permissions, which is the
// store's resolved owner rather than the daemon's. Space, when set, reports
// the total and free bytes a write through the mount can use.
type Options struct {
	Fetcher Fetcher
	Mutator Mutator
	UID     uint32
	GID     uint32
	Space   func() (total, free uint64)
	// CacheBytes bounds the in-memory block cache shared by every open
	// file (0: the default; negative: none).
	CacheBytes int64
}

// maxPendingInvalidations bounds the kernel invalidations waiting to be sent.
// Past it, changes are left to expire on their own: entries are cached for a
// second and attributes not at all, so an invalidation only speeds things up.
const maxPendingInvalidations = 8192

func cacheBytes(n int64) int64 {
	if n == 0 {
		return defaultCacheBytes
	}
	return n
}

// FS is one mounted streamed store.
type FS struct {
	tree       *Tree
	fetcher    Fetcher
	mutator    Mutator
	uid        uint32
	gid        uint32
	space      func() (total, free uint64)
	cache      *blockCache
	server     *fuse.Server
	mountpoint string

	mu   sync.Mutex
	dirs map[string]*fs.Inode

	invalMu   sync.Mutex
	inval     map[string]struct{}
	invalKick chan struct{}

	// done closes when the kernel connection ends, however it ends: our
	// Unmount, `fusermount -u` by hand, or an aborted connection.
	done chan struct{}
}

// Mount serves tree at mountpoint until Unmount is called. The mountpoint must
// exist and be empty; the caller owns its lifetime.
func Mount(mountpoint string, tree *Tree, opts Options) (*FS, error) {
	f := &FS{
		tree:       tree,
		fetcher:    opts.Fetcher,
		mutator:    opts.Mutator,
		uid:        opts.UID,
		gid:        opts.GID,
		space:      opts.Space,
		cache:      newBlockCache(cacheBytes(opts.CacheBytes)),
		mountpoint: mountpoint,
		dirs:       map[string]*fs.Inode{},
		inval:      map[string]struct{}{},
		invalKick:  make(chan struct{}, 1),
		done:       make(chan struct{}),
	}
	root := &dirNode{fsys: f}
	serverOpts := &fs.Options{
		MountOptions: fuse.MountOptions{
			// The daemon runs as root but the files belong to the store's
			// user, so let the kernel enforce the modes reported above
			// instead of trusting whoever mounted it. allow_other is what
			// lets that user reach the mount at all: without it FUSE admits
			// only the mounting process, and every other uid gets EACCES
			// before default_permissions is ever consulted.
			Options: []string{"default_permissions"},
			// allow_other needs root (or user_allow_other in /etc/fuse.conf);
			// the daemon mounts as root, and a non-root mount is a test or a
			// single-user run where the mounter is the only caller anyway.
			AllowOther:    os.Geteuid() == 0,
			FsName:        "storeboard",
			Name:          "storeboard",
			DisableXAttrs: true,
			// Larger requests mean fewer round trips through the daemon for
			// a big copy into or out of the mount.
			MaxWrite: 1 << 20,
			// STOREBOARD_FUSE_DEBUG logs every operation go-fuse serves.
			Debug: os.Getenv("STOREBOARD_FUSE_DEBUG") != "",
			// A root daemon mounts directly. NixOS has no /bin/fusermount for
			// go-fuse to fall back to, and the unit already carries
			// CAP_SYS_ADMIN for exactly this; a non-root daemon keeps the
			// fusermount path.
			DirectMount: os.Geteuid() == 0,
		},
	}

	server, err := fs.Mount(mountpoint, root, serverOpts)
	if err != nil {
		return nil, err
	}
	f.server = server
	go func() {
		server.Wait()
		close(f.done)
	}()
	tree.setNotify(f.queueInvalidation)
	go f.invalidator()
	return f, nil
}

// Replace updates the journal-backed view while the mount remains attached.
// Existing open handles keep the version they opened; new lookups see this
// snapshot.
func (f *FS) Replace(entries map[string]Entry) {
	if f == nil || f.tree == nil {
		return
	}
	f.tree.Replace(entries)
}

// Set records or updates one path in the mounted view.
func (f *FS) Set(e Entry) {
	if f == nil || f.tree == nil {
		return
	}
	f.tree.Set(e)
}

// Delete removes one path from the mounted view.
func (f *FS) Delete(p string) {
	if f == nil || f.tree == nil {
		return
	}
	f.tree.Delete(p)
}

// queueInvalidation hands changed paths to the invalidator. It never blocks
// and never talks to the kernel itself: it is called from inside FUSE
// handlers (a create journals its file), and the kernel holds the parent
// directory's lock for the whole request - an invalidation sent from there
// waits on that lock forever.
func (f *FS) queueInvalidation(paths []string) {
	f.invalMu.Lock()
	for _, p := range paths {
		if len(f.inval) >= maxPendingInvalidations {
			break
		}
		f.inval[p] = struct{}{}
	}
	f.invalMu.Unlock()
	select {
	case f.invalKick <- struct{}{}:
	default:
	}
}

// invalidator sends queued invalidations from its own goroutine, where waiting
// for a directory lock only delays the invalidation.
func (f *FS) invalidator() {
	for {
		select {
		case <-f.done:
			return
		case <-f.invalKick:
		}
		f.invalMu.Lock()
		batch := f.inval
		f.inval = map[string]struct{}{}
		f.invalMu.Unlock()
		for p := range batch {
			select {
			case <-f.done:
				return
			default:
			}
			f.invalidateNow(p)
		}
	}
}

// invalidateNow asks the kernel to drop one cached parent/name dentry.
func (f *FS) invalidateNow(p string) {
	if p == "" {
		return
	}
	f.mu.Lock()
	dir := f.dirs[parentOf(p)]
	f.mu.Unlock()
	if dir != nil {
		_ = dir.NotifyEntry(baseOf(p))
	}
}

// repath rewrites the absolute paths held by the inodes under a renamed
// entry. The kernel moves its dentry; these node fields do not move with it.
func (f *FS) repath(inode *fs.Inode, from, to string) {
	if inode == nil {
		return
	}
	moved := func(old string) string {
		if old == from {
			return to
		}
		if strings.HasPrefix(old, from+"/") {
			return to + old[len(from):]
		}
		return old
	}
	switch n := inode.Operations().(type) {
	case *dirNode:
		n.mu.Lock()
		old := n.path
		n.path = moved(old)
		next := n.path
		n.mu.Unlock()
		f.mu.Lock()
		if f.dirs[old] == inode {
			delete(f.dirs, old)
		}
		f.dirs[next] = inode
		f.mu.Unlock()
	case *fileNode:
		n.mu.Lock()
		n.path = moved(n.path)
		n.entry.Path = n.path
		n.mu.Unlock()
	}
	for _, child := range inode.Children() {
		f.repath(child, from, to)
	}
}

// Alive reports whether the kernel connection is still being served. A mount
// someone detached by hand stays "mounted" as far as the engine's pointer is
// concerned; this is how it notices.
func (f *FS) Alive() bool {
	select {
	case <-f.done:
		return false
	default:
		return true
	}
}

// Wait blocks until the filesystem is unmounted.
func (f *FS) Wait() { <-f.done }

// Unmount detaches the filesystem. A mount that is busy - a shell sitting in
// it, a viewer holding a file open - is detached lazily instead of being left
// behind as a dead endpoint ("Transport endpoint is not connected") that only
// the next daemon start would clean up.
func (f *FS) Unmount() error {
	err := f.server.Unmount()
	if err != nil && f.Alive() {
		if derr := syscall.Unmount(f.mountpoint, syscall.MNT_DETACH); derr == nil {
			err = nil
		}
	}
	return err
}

// errnoFor maps fetch failures onto what a caller can act on: a missing blob
// is ENOENT, a holder that went away is ESTALE (retry after failover), and
// anything else is EIO.
func errnoFor(err error) syscall.Errno {
	var errno syscall.Errno
	switch {
	case err == nil:
		return 0
	case errors.As(err, &errno):
		return errno
	case errors.Is(err, os.ErrPermission):
		return syscall.EROFS
	case errors.Is(err, os.ErrExist):
		return syscall.EEXIST
	case errors.Is(err, os.ErrNotExist):
		return syscall.ENOENT
	case errors.Is(err, syscall.ESTALE), errors.Is(err, io.ErrUnexpectedEOF):
		return syscall.ESTALE
	case errors.Is(err, context.Canceled):
		return syscall.EINTR
	default:
		return syscall.EIO
	}
}
