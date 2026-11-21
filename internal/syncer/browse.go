package syncer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"codeberg.org/kyleraykbs/storeboard/internal/streamfs"
	"codeberg.org/kyleraykbs/storeboard/pkg/api"
)

// Browsing is the journal seen as folders, for a client with no mount: the
// phone's document provider lists a store and reads ranges of its files the
// way the FUSE mount does on a desktop. It works for every kind of store; a
// file whose bytes are not on this machine is read from a holder on demand.

// ErrNotFound reports a store or path the journal does not know.
var ErrNotFound = errors.New("not found")

// view is the store's journal as a directory index. A streamed store keeps
// one for its mount; any other store builds it the first time it is browsed,
// and from then on it follows the journal like the mount's does.
func (fs *StoreState) view() *streamfs.Tree {
	if fs.stream {
		return fs.tree
	}
	fs.browseOnce.Do(fs.observeTree)
	return fs.tree
}

func (e *Engine) browseStore(name string) (*StoreState, error) {
	fs := e.byName[name]
	if fs == nil {
		return nil, fmt.Errorf("store %q: %w", name, ErrNotFound)
	}
	return fs, nil
}

// browseRel validates a store-relative path; "" is the store's root.
func browseRel(p string) (string, error) {
	if p == "" || p == "." || p == "/" {
		return "", nil
	}
	if !validRel(p) {
		return "", fmt.Errorf("unsafe path %q", p)
	}
	return p, nil
}

func (e *Engine) browseEntry(fs *StoreState, ent streamfs.Entry) api.BrowseEntry {
	out := api.BrowseEntry{
		Name: filepath.Base(ent.Path),
		Path: ent.Path,
		Dir:  ent.Dir,
		Size: ent.Size,
		Blob: ent.Blob,
	}
	if ent.Dir {
		out.Size = 0
		out.Blob = ""
	}
	if !ent.MTime.IsZero() {
		out.MTime = ent.MTime.UTC()
	}
	if !fs.stream && !ent.Dir {
		if j := fs.db.Get(ent.Path); j != nil && j.Local {
			out.Local = true
		}
	}
	return out
}

// Browse lists the immediate children of dir in a store ("" is its root).
func (e *Engine) Browse(store, dir string) ([]api.BrowseEntry, error) {
	fs, err := e.browseStore(store)
	if err != nil {
		return nil, err
	}
	rel, err := browseRel(dir)
	if err != nil {
		return nil, err
	}
	tree := fs.view()
	if rel != "" {
		ent, ok := tree.Get(rel)
		if !ok {
			return nil, fmt.Errorf("%s: %w", rel, ErrNotFound)
		}
		if !ent.Dir {
			return nil, fmt.Errorf("%s: not a directory", rel)
		}
	}
	kids := tree.Children(rel)
	out := make([]api.BrowseEntry, 0, len(kids))
	for _, k := range kids {
		out = append(out, e.browseEntry(fs, k))
	}
	return out, nil
}

// Stat describes one path in a store ("" is its root).
func (e *Engine) Stat(store, path string) (api.BrowseEntry, error) {
	fs, err := e.browseStore(store)
	if err != nil {
		return api.BrowseEntry{}, err
	}
	rel, err := browseRel(path)
	if err != nil {
		return api.BrowseEntry{}, err
	}
	ent, ok := fs.view().Get(rel)
	if !ok {
		return api.BrowseEntry{}, fmt.Errorf("%s: %w", rel, ErrNotFound)
	}
	ent.Path = rel
	return e.browseEntry(fs, ent), nil
}

// LocalPath is the file's own path on this machine, when its bytes are here
// and current: a caller can then open it directly (and write it - the scanner
// records the change) instead of reading it through ReadAt. A streamed store
// never has one.
func (e *Engine) LocalPath(store, path string) (string, error) {
	fs, err := e.browseStore(store)
	if err != nil {
		return "", err
	}
	rel, err := browseRel(path)
	if err != nil || rel == "" {
		return "", fmt.Errorf("unsafe path %q", path)
	}
	if fs.stream {
		return "", fmt.Errorf("%s: streamed store keeps no local copy: %w", rel, ErrNotFound)
	}
	ent := fs.db.Get(rel)
	if ent == nil || ent.Deleted || ent.Dir {
		return "", fmt.Errorf("%s: %w", rel, ErrNotFound)
	}
	abs := filepath.Join(fs.root, rel)
	info, err := os.Lstat(abs)
	if err != nil || !info.Mode().IsRegular() {
		return "", fmt.Errorf("%s: not on this machine: %w", rel, ErrNotFound)
	}
	return abs, nil
}

// browseTimeout bounds one ReadAt that has to go to a holder.
var browseTimeout = 60 * time.Second

// ReadAt reads up to length bytes of a file at offset. Bytes on this machine
// are read from disk; anything else is fetched from a holder in ranges, so a
// phone can open a 20 GiB video without storing it. A read at or past the end
// returns no bytes and no error.
func (e *Engine) ReadAt(ctx context.Context, store, path string, offset, length int64) ([]byte, error) {
	fs, err := e.browseStore(store)
	if err != nil {
		return nil, err
	}
	rel, err := browseRel(path)
	if err != nil || rel == "" {
		return nil, fmt.Errorf("unsafe path %q", path)
	}
	ent := fs.db.Get(rel)
	if ent == nil || ent.Deleted || ent.Dir {
		return nil, fmt.Errorf("%s: %w", rel, ErrNotFound)
	}
	if offset < 0 || length < 0 {
		return nil, fmt.Errorf("negative range")
	}
	if offset >= ent.Size || length == 0 {
		return []byte{}, nil
	}
	length = min(length, ent.Size-offset)

	if !fs.stream {
		if data, ok := e.readLocal(fs, ent, offset, length); ok {
			return data, nil
		}
	}
	ctx, cancel := context.WithTimeout(ctx, browseTimeout)
	defer cancel()
	fetch := &streamFetcher{e: e, fs: fs}
	out := make([]byte, 0, length)
	for int64(len(out)) < length {
		pos := offset + int64(len(out))
		want := min(length-int64(len(out)), int64(maxReadChunk))
		data, err := fetch.ReadRange(ctx, streamEntry(ent), pos, want)
		if err != nil {
			return nil, err
		}
		if len(data) == 0 {
			break
		}
		out = append(out, data...)
	}
	return out, nil
}

// readLocal reads a range from the canonical file, when that file is still
// the version the journal describes (the scanner's own evidence: size and
// mtime). ok is false when the bytes are not here.
func (e *Engine) readLocal(fs *StoreState, ent *Entry, offset, length int64) ([]byte, bool) {
	f, err := os.Open(filepath.Join(fs.root, ent.Path))
	if err != nil {
		return nil, false
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() != ent.Size || info.ModTime().UnixNano() != ent.MTime {
		return nil, false
	}
	buf := make([]byte, length)
	n, err := f.ReadAt(buf, offset)
	if err != nil && err != io.EOF {
		return nil, false
	}
	return buf[:n], true
}
