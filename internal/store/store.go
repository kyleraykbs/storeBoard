// Package store is storeboard's content-addressed blob store: every file the
// mesh knows about lives here once, keyed by its sha256, so replicas share
// bytes and integrity is checkable. Nothing is ever deleted from the store —
// deletion propagates in the journal, not in the bytes (DESIGN invariant 2).
package store

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// ChunkSize is the blob-frame payload size on the wire.
const ChunkSize = 256 * 1024

// Store is the blob pile plus its scratch space, all under one data dir.
type Store struct {
	root string
	tmp  string
}

// Open prepares store/tmp directories under dataDir.
func Open(dataDir string) (*Store, error) {
	s := &Store{
		root: filepath.Join(dataDir, "store"),
		tmp:  filepath.Join(dataDir, "tmp"),
	}
	for _, d := range []string{s.root, s.tmp} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return nil, fmt.Errorf("store: %w", err)
		}
	}
	return s, nil
}

// BlobPath is where a blob with this sum lives.
func (s *Store) BlobPath(sum string) string {
	if len(sum) < 5 {
		return filepath.Join(s.root, "invalid", sum)
	}
	return filepath.Join(s.root, sum[:2], sum[2:4], sum)
}

// Has reports whether the blob is already stored.
func (s *Store) Has(sum string) bool {
	_, err := os.Stat(s.BlobPath(sum))
	return err == nil
}

// Temp creates a scratch file on the same filesystem as the store, so
// adopting it is a rename.
func (s *Store) Temp() (*os.File, error) {
	return os.CreateTemp(s.tmp, "blob-*")
}

// Open reads a stored blob.
func (s *Store) Open(sum string) (io.ReadCloser, error) {
	return os.Open(s.BlobPath(sum))
}

// Adopt moves a fully written temp file into the store under sum. The
// rename is atomic; the parent directories are fsynced so a crash cannot
// leave a blob that the journal already references but the store lacks.
func (s *Store) Adopt(tmpPath, sum string) error {
	dst := s.BlobPath(sum)
	if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, dst); err != nil {
		return err
	}
	return syncDir(filepath.Dir(dst))
}

// Sum hashes a reader (hex sha256).
func Sum(r io.Reader) (string, error) {
	h := sha256.New()
	if _, err := io.Copy(h, r); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// SumFile hashes a file on disk.
func SumFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	return Sum(f)
}

// syncDir fsyncs a directory (its entries), which is what makes a rename
// crash-durable.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		// Directory fsync is unsupported on some filesystems; the rename
		// still succeeded, so this is not fatal.
		if os.IsPermission(err) {
			return nil
		}
		return err
	}
	return nil
}
