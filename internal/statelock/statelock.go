// Package statelock makes a data directory single-owner: one engine at a time
// may hold its journals, its content store and its history.
//
// Two engines on one data directory append to the same journals with two
// different in-memory views of them, each collects blobs the other still
// references, and each one's checkpoint truncates what the other just wrote.
// The socket used to be the only guard, and it guards the wrong thing: two
// configs that share a dataDir but name different sockets (a system unit and a
// user unit, an app restarted while its old engine is still draining) started
// side by side. The lock is on the state itself.
package statelock

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// FileName is the lock file inside the data directory. Its content is the
// pid of the holder, for a human reading it; the lock itself is the flock.
const FileName = "lock"

// Lock is a held data-directory lock. The kernel drops it when the process
// exits, however it exits, so a crash never leaves a stale lock behind.
type Lock struct {
	f *os.File
}

// Acquire takes the data directory's lock without waiting. A directory held
// by another live engine is an error naming it; nothing is touched.
func Acquire(dataDir string) (*Lock, error) {
	path := filepath.Join(dataDir, FileName)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open lock %s: %w", path, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("another storeboard engine is already running on data directory %s (%s is locked); stop it first", dataDir, path)
		}
		return nil, fmt.Errorf("lock %s: %w", path, err)
	}
	// Best-effort: who holds it, for whoever looks.
	if err := f.Truncate(0); err == nil {
		fmt.Fprintf(f, "%d\n", os.Getpid())
	}
	return &Lock{f: f}, nil
}

// Release drops the lock. The file stays: removing it would let a racing
// Acquire lock a fresh inode while another still holds the old one.
func (l *Lock) Release() error {
	if l == nil || l.f == nil {
		return nil
	}
	err := l.f.Close()
	l.f = nil
	return err
}
