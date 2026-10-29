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
	if entry.Dir {
		return d.NewInode(ctx, &dirNode{fsys: d.fsys, path: child}, fs.StableAttr{Mode: fuse.S_IFDIR}), 0
	}
	return d.NewInode(ctx, &fileNode{fsys: d.fsys, path: child, entry: entry}, fs.StableAttr{Mode: fuse.S_IFREG}), 0
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

// Getattr reports the size the journal recorded.
func (f *fileNode) Getattr(ctx context.Context, h fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	mode := f.entry.Mode
	if mode == 0 {
		mode = 0o644
	}
	out.Mode = fuse.S_IFREG | mode&0o777
	out.Size = uint64(f.entry.Size)
	if !f.entry.MTime.IsZero() {
		mt := f.entry.MTime
		out.SetTimes(nil, &mt, nil)
	}
	return 0
}

func (f *fileNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	if flags&uint32(os.O_WRONLY|os.O_RDWR) != 0 {
		return nil, 0, syscall.EROFS
	}
	if f.fsys.fetcher == nil {
		return nil, 0, syscall.ENOSYS
	}
	return &fileHandle{fsys: f.fsys, entry: f.entry, size: f.entry.Size}, 0, 0
}

type fileHandle struct {
	fsys  *FS
	entry Entry
	size  int64
}

var _ = (fs.FileReader)((*fileHandle)(nil))
var _ = (fs.FileReleaser)((*fileHandle)(nil))

func (h *fileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	if off >= h.size {
		return fuse.ReadResultData(nil), 0
	}
	want := int64(len(dest))
	if rest := h.size - off; want > rest {
		want = rest
	}
	buf, err := h.fsys.fetcher.ReadRange(ctx, h.entry, off, want)
	if err != nil {
		return nil, errnoFor(err)
	}
	return fuse.ReadResultData(buf), 0
}

func (h *fileHandle) Release(ctx context.Context) syscall.Errno {
	return 0
}

// FS is one mounted streamed store.
type FS struct {
	tree    *Tree
	fetcher Fetcher
	server  *fuse.Server
}

// Mount serves tree at mountpoint until Unmount is called. The mountpoint must
// exist and be empty; the caller owns its lifetime.
func Mount(mountpoint string, tree *Tree, fetcher Fetcher) (*FS, error) {
	f := &FS{tree: tree, fetcher: fetcher}
	root := &dirNode{fsys: f}
	opts := &fs.Options{
		MountOptions: fuse.MountOptions{
			// The daemon runs as root but the files belong to the store's
			// user, so let the kernel enforce the modes reported above
			// instead of trusting whoever mounted it.
			Options:       []string{"default_permissions"},
			FsName:        "storeboard",
			Name:          "storeboard",
			DisableXAttrs: true,
		},
	}
	server, err := fs.Mount(mountpoint, root, opts)
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
	switch {
	case err == nil:
		return 0
	case errors.Is(err, os.ErrNotExist):
		return syscall.ENOENT
	case errors.Is(err, syscall.ESTALE), errors.Is(err, io.ErrUnexpectedEOF):
		return syscall.ESTALE
	default:
		return syscall.EIO
	}
}
