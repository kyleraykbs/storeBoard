// Package streamfs presents a store as a mounted filesystem without keeping
// its bytes on this machine.
//
// The journal is the whole truth about the tree: directories and file names
// come from it, so the layout exists even when no holder answers. File
// contents are fetched per open, through a Fetcher the sync engine provides —
// there is no read cache, because the point of a streamed store is that this
// machine stores metadata and not photographs.
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
// request per read, so it never needs a local file-sized cache.
type Fetcher interface {
	ReadRange(ctx context.Context, entry Entry, offset, length int64) ([]byte, error)
}

// stableIno derives an inode number from a path, so one path keeps one
// identity for the life of the mount. go-fuse otherwise hands out a fresh
// number per Lookup, and a file manager - which keys selections, refresh and
// "is this the same folder" on (mount, inode) - sees the whole tree churn on
// every read. `ls` never notices; Nautilus falls apart.
func stableIno(p string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(p))
	v := h.Sum64()
	// 1 is the root and 0 means "unassigned" to go-fuse.
	if v < 2 {
		v += 2
	}
	return v
}

// Tree is the journal's view, replaced wholesale as the journal changes. Reads
// take a snapshot, so a lookup never blocks behind an update.
type Tree struct {
	mu      sync.RWMutex
	entries map[string]Entry
}

// NewTree builds a tree from a path→entry map.
func NewTree(entries map[string]Entry) *Tree {
	t := &Tree{}
	t.Replace(entries)
	return t
}

// Replace installs a new snapshot of the journal.
func (t *Tree) Replace(entries map[string]Entry) {
	next := make(map[string]Entry, len(entries))
	for p, e := range entries {
		clean := strings.TrimPrefix(path.Clean("/"+p), "/")
		if clean == "" || clean == "." {
			continue
		}
		next[clean] = e
	}
	t.mu.Lock()
	t.entries = next
	t.mu.Unlock()
}

// Get returns one entry.
func (t *Tree) Get(p string) (Entry, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	e, ok := t.entries[p]
	return e, ok
}

// children lists the immediate children of a directory. A deeper path implies
// its parent directories, so a store whose journal holds only files still
// presents a navigable tree.
func (t *Tree) children(dir string) map[string]Entry {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := map[string]Entry{}
	prefix := ""
	if dir != "" {
		prefix = dir + "/"
	}
	for p, e := range t.entries {
		if !strings.HasPrefix(p, prefix) {
			continue
		}
		rest := p[len(prefix):]
		name, tail, nested := strings.Cut(rest, "/")
		if name == "" {
			continue
		}
		if nested && tail != "" {
			if _, seen := out[name]; !seen {
				out[name] = Entry{Dir: true, Mode: 0o755}
			}
			continue
		}
		out[name] = e
	}
	return out
}

// dirNode is a directory backed by the tree.
type dirNode struct {
	fs.Inode
	fsys *FS
	path string
}

var _ = (fs.NodeLookuper)((*dirNode)(nil))
var _ = (fs.NodeReaddirer)((*dirNode)(nil))
var _ = (fs.NodeGetattrer)((*dirNode)(nil))

// Lookup resolves one child name into an inode.
func (d *dirNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	child := name
	if d.path != "" {
		child = d.path + "/" + name
	}
	entry, ok := d.fsys.tree.Get(child)
	if !ok {
		// A name that only exists as a prefix of deeper entries is a directory.
		if len(d.fsys.tree.children(child)) > 0 {
			entry = Entry{Dir: true, Mode: 0o755}
		} else {
			return nil, syscall.ENOENT
		}
	}
	if in := d.GetChild(name); in != nil {
		// The kernel still holds this name, so keep the identity and just
		// refresh what the journal says about it.
		switch n := in.Operations().(type) {
		case *fileNode:
			n.entry = entry
		case *dirNode:
			n.path = child
		}
		return in, 0
	}
	if entry.Dir {
		return d.NewInode(ctx, &dirNode{fsys: d.fsys, path: child}, fs.StableAttr{Mode: fuse.S_IFDIR, Ino: stableIno(child)}), 0
	}
	return d.NewInode(ctx, &fileNode{fsys: d.fsys, path: child, entry: entry}, fs.StableAttr{Mode: fuse.S_IFREG, Ino: stableIno(child)}), 0
}

// Readdir lists the children of this directory.
func (d *dirNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	children := d.fsys.tree.children(d.path)
	names := make([]string, 0, len(children))
	for name := range children {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]fuse.DirEntry, 0, len(names))
	for _, name := range names {
		mode := uint32(fuse.S_IFREG)
		if children[name].Dir {
			mode = fuse.S_IFDIR
		}
		out = append(out, fuse.DirEntry{Name: name, Mode: mode})
	}
	return fs.NewListDirStream(out), 0
}

// Getattr reports a directory's attributes.
func (d *dirNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = fuse.S_IFDIR | 0o755
	d.fsys.owner(out)
	return 0
}

// owner stamps the store's uid and gid, which is what default_permissions
// compares a caller against.
func (f *FS) owner(out *fuse.AttrOut) {
	out.Uid = f.uid
	out.Gid = f.gid
}

var _ = (fs.NodeCreater)((*dirNode)(nil))
var _ = (fs.NodeMkdirer)((*dirNode)(nil))
var _ = (fs.NodeUnlinker)((*dirNode)(nil))
var _ = (fs.NodeRmdirer)((*dirNode)(nil))
var _ = (fs.NodeRenamer)((*dirNode)(nil))

// childPath joins a name onto this directory's path.
func (d *dirNode) childPath(name string) string {
	if d.path == "" {
		return name
	}
	return d.path + "/" + name
}

// Create makes a new file and hands back the handle that takes its bytes.
func (d *dirNode) Create(ctx context.Context, name string, flags, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	if d.fsys.mutator == nil {
		return nil, nil, 0, syscall.EROFS
	}
	child := d.childPath(name)
	pending, err := d.fsys.mutator.Create(child, mode&0o7777)
	if err != nil {
		return nil, nil, 0, errnoFor(err)
	}
	entry := Entry{Path: child, Mode: mode & 0o7777}
	node := &fileNode{fsys: d.fsys, path: child, entry: entry}
	inode := d.NewInode(ctx, node, fs.StableAttr{Mode: fuse.S_IFREG, Ino: stableIno(child)})
	h := &fileHandle{fsys: d.fsys, entry: entry, write: pending}
	return inode, h, 0, 0
}

// Mkdir records an empty directory, which only exists because the journal
// says so.
func (d *dirNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if d.fsys.mutator == nil {
		return nil, syscall.EROFS
	}
	child := d.childPath(name)
	if err := d.fsys.mutator.MakeDir(child, mode&0o7777); err != nil {
		return nil, errnoFor(err)
	}
	return d.NewInode(ctx, &dirNode{fsys: d.fsys, path: child}, fs.StableAttr{Mode: fuse.S_IFDIR, Ino: stableIno(child)}), 0
}

// Unlink removes a file: the journal records the tombstone and the holder is
// told, so the deletion travels like any other.
func (d *dirNode) Unlink(ctx context.Context, name string) syscall.Errno {
	if d.fsys.mutator == nil {
		return syscall.EROFS
	}
	if err := d.fsys.mutator.Remove(d.childPath(name)); err != nil {
		return errnoFor(err)
	}
	return 0
}

// Rmdir removes an empty directory.
func (d *dirNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	if d.fsys.mutator == nil {
		return syscall.EROFS
	}
	if err := d.fsys.mutator.RemoveDir(d.childPath(name)); err != nil {
		return errnoFor(err)
	}
	return 0
}

// Rename moves a path. The daemon performs it on both the journal and, when a
// holder is reachable, the holder's own tree.
func (d *dirNode) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	if d.fsys.mutator == nil {
		return syscall.EROFS
	}
	parent, ok := newParent.(*dirNode)
	if !ok {
		return syscall.EXDEV
	}
	if err := d.fsys.mutator.Rename(d.childPath(name), parent.childPath(newName)); err != nil {
		return errnoFor(err)
	}
	return 0
}

// fileNode is a file whose bytes come from a holder.
type fileNode struct {
	fs.Inode
	fsys  *FS
	path  string
	entry Entry
}

var _ = (fs.NodeOpener)((*fileNode)(nil))
var _ = (fs.NodeGetattrer)((*fileNode)(nil))
var _ = (fs.NodeSetattrer)((*fileNode)(nil))

// Setattr honours truncate(2) and open(O_TRUNC), which is how a shell
// redirect or `cp` resets a file. Mode and times are not the filesystem's to
// decide - a store's permissions and mtimes come from the journal - so they
// are accepted and ignored rather than refused.
func (f *fileNode) Setattr(ctx context.Context, h fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	size, ok := in.GetSize()
	if !ok {
		if out != nil {
			out.Mode = fuse.S_IFREG | 0o644
			out.Size = uint64(f.entry.Size)
		}
		return 0
	}
	if h != nil {
		if fh, isOurs := h.(*fileHandle); isOurs && fh.write != nil {
			if err := fh.write.Truncate(int64(size)); err != nil {
				return errnoFor(err)
			}
			fh.size = int64(size)
			f.entry.Size = int64(size)
			if out != nil {
				out.Mode = fuse.S_IFREG | 0o644
				out.Size = size
			}
			return 0
		}
	}
	if f.fsys.mutator == nil {
		return syscall.EROFS
	}
	if err := f.fsys.mutator.Truncate(f.path, int64(size)); err != nil {
		return errnoFor(err)
	}
	f.entry.Size = int64(size)
	if out != nil {
		out.Mode = fuse.S_IFREG | 0o644
		out.Size = size
	}
	return 0
}

// Getattr reports the size the journal recorded.
func (f *fileNode) Getattr(ctx context.Context, h fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	mode := f.entry.Mode
	if mode == 0 {
		mode = 0o644
	}
	out.Mode = fuse.S_IFREG | mode&0o777
	out.Size = uint64(f.entry.Size)
	f.fsys.owner(out)
	if !f.entry.MTime.IsZero() {
		mt := f.entry.MTime
		out.SetTimes(nil, &mt, nil)
	}
	return 0
}

func (f *fileNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	wantWrite := flags&uint32(os.O_WRONLY|os.O_RDWR) != 0
	if f.fsys.fetcher == nil && !wantWrite {
		return nil, 0, syscall.ENOSYS
	}
	h := &fileHandle{fsys: f.fsys, entry: f.entry, size: f.entry.Size}
	if wantWrite {
		if f.fsys.mutator == nil {
			return nil, 0, syscall.EROFS
		}
		w, err := f.fsys.mutator.Open(f.path, flags&uint32(os.O_TRUNC) != 0)
		if err != nil {
			return nil, 0, errnoFor(err)
		}
		h.write = w
		if flags&uint32(os.O_TRUNC) != 0 {
			h.size = 0
		}
	}
	return h, 0, 0
}

type fileHandle struct {
	fsys  *FS
	entry Entry
	size  int64
	write PendingFile
}

var _ = (fs.FileReader)((*fileHandle)(nil))
var _ = (fs.FileWriter)((*fileHandle)(nil))
var _ = (fs.FileFlusher)((*fileHandle)(nil))
var _ = (fs.FileFsyncer)((*fileHandle)(nil))
var _ = (fs.FileGetattrer)((*fileHandle)(nil))
var _ = (fs.FileReleaser)((*fileHandle)(nil))

func (h *fileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	if off >= h.size {
		return fuse.ReadResultData(nil), 0
	}
	want := int64(len(dest))
	if rest := h.size - off; want > rest {
		want = rest
	}
	if h.write != nil {
		// The handle may already have overwritten part of the file: read
		// through it, so a writer sees its own bytes and the holder's
		// everywhere else.
		buf := make([]byte, want)
		n, err := h.write.ReadAt(buf, off)
		if err != nil && err != io.EOF {
			return nil, errnoFor(err)
		}
		return fuse.ReadResultData(buf[:n]), 0
	}
	buf, err := h.fsys.fetcher.ReadRange(ctx, h.entry, off, want)
	if err != nil {
		return nil, errnoFor(err)
	}
	return fuse.ReadResultData(buf), 0
}

func (h *fileHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	if h.write == nil {
		return 0, syscall.EROFS
	}
	n, err := h.write.WriteAt(data, off)
	if err != nil {
		return uint32(n), syscall.EIO
	}
	if end := off + int64(n); end > h.size {
		h.size = end
	}
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
	out.Size = uint64(h.size)
	h.fsys.owner(out)
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
	if h.write == nil {
		return 0
	}
	if err := h.write.Sync(); err != nil {
		return errnoFor(err)
	}
	return 0
}

func (h *fileHandle) Release(ctx context.Context) syscall.Errno {
	if h.write == nil {
		return 0
	}
	if err := h.write.Close(); err != nil {
		return errnoFor(err)
	}
	return 0
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
// store's resolved owner rather than the daemon's.
type Options struct {
	Fetcher Fetcher
	Mutator Mutator
	UID     uint32
	GID     uint32
}

// FS is one mounted streamed store.
type FS struct {
	tree    *Tree
	fetcher Fetcher
	mutator Mutator
	uid     uint32
	gid     uint32
	server  *fuse.Server
}

// Mount serves tree at mountpoint until Unmount is called. The mountpoint must
// exist and be empty; the caller owns its lifetime.
func Mount(mountpoint string, tree *Tree, opts Options) (*FS, error) {
	f := &FS{tree: tree, fetcher: opts.Fetcher, mutator: opts.Mutator, uid: opts.UID, gid: opts.GID}
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
	return f, nil
}

// Replace updates the journal-backed view while the mount remains attached.
// Existing open handles keep their current range fetch; new lookups see this
// snapshot.
func (f *FS) Replace(entries map[string]Entry) {
	if f != nil && f.tree != nil {
		f.tree.Replace(entries)
	}
}

// Wait blocks until the filesystem is unmounted.
func (f *FS) Wait() { f.server.Wait() }

// Unmount detaches the filesystem.
func (f *FS) Unmount() error { return f.server.Unmount() }

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
	default:
		return syscall.EIO
	}
}
