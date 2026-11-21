// Package store is storeboard's content-addressed blob store: every file the
// mesh knows about lives here once, keyed by its sha256, so replicas share
// bytes and integrity is checkable. Nothing is ever deleted from the store —
// deletion propagates in the journal, not in the bytes (DESIGN invariant 2).
package store

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
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

// ErrInvalidSum refuses a blob name that is not a sha256 in lowercase hex.
var ErrInvalidSum = errors.New("store: not a sha256 blob name")

// ValidSum reports whether sum can name a blob: exactly 64 lowercase hex
// digits. Sums arrive from peers; anything else would be joined into a path,
// and "../../etc/shadow" is not a hash.
func ValidSum(sum string) bool {
	if len(sum) != sha256.Size*2 {
		return false
	}
	for i := 0; i < len(sum); i++ {
		c := sum[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// BlobPath is where a blob with this sum lives, or "" for a sum that cannot
// name a blob.
func (s *Store) BlobPath(sum string) string {
	if !ValidSum(sum) {
		return ""
	}
	return filepath.Join(s.root, sum[:2], sum[2:4], sum)
}

// Has reports whether the blob is already stored.
func (s *Store) Has(sum string) bool {
	p := s.BlobPath(sum)
	if p == "" {
		return false
	}
	info, err := os.Stat(p)
	return err == nil && info.Mode().IsRegular()
}

// Temp creates a scratch file on the same filesystem as the store, so
// adopting it is a rename.
func (s *Store) Temp() (*os.File, error) {
	return os.CreateTemp(s.tmp, "blob-*")
}

// Open reads a stored blob.
func (s *Store) Open(sum string) (*os.File, error) {
	p := s.BlobPath(sum)
	if p == "" {
		return nil, ErrInvalidSum
	}
	return os.Open(p)
}

// Remove deletes a stored blob. Callers only do this when the bytes exist
// somewhere else on this machine (a materialised file, or a kept version), so
// removal never destroys the last copy.
func (s *Store) Remove(sum string) error {
	p := s.BlobPath(sum)
	if p == "" {
		return nil
	}
	err := os.Remove(p)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

// Adopt moves a fully written temp file into the store under sum. The
// rename is atomic; the parent directories are fsynced so a crash cannot
// leave a blob that the journal already references but the store lacks.
func (s *Store) Adopt(tmpPath, sum string) error {
	dst := s.BlobPath(sum)
	if dst == "" {
		return ErrInvalidSum
	}
	leaf := filepath.Dir(dst)
	mid := filepath.Dir(leaf)
	// The fan-out directories are created on first use. A directory entry
	// is only durable once its parent is fsynced, so a new one is synced
	// into its parent, or a crash could lose a blob the journal references.
	_, midErr := os.Stat(mid)
	_, leafErr := os.Stat(leaf)
	if err := os.MkdirAll(leaf, 0o700); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, dst); err != nil {
		return err
	}
	if err := syncDir(leaf); err != nil {
		return err
	}
	if leafErr != nil {
		if err := syncDir(mid); err != nil {
			return err
		}
	}
	if midErr != nil {
		if err := syncDir(s.root); err != nil {
			return err
		}
	}
	return nil
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
		if os.IsPermission(err) || errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.ENOTSUP) {
			return nil
		}
		return err
	}
	return nil
}
