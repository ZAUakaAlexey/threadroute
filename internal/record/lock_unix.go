//go:build unix

package record

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// lockFileName is the lock file in the output directory. It is not a
// segment, so readers and retention ignore it.
const lockFileName = ".lock"

// ErrDirLocked is returned by OpenWriter when another Writer, in this or
// another process, holds the output directory.
var ErrDirLocked = errors.New("output directory is used by another record writer")

// lockDir takes an exclusive flock on dir/.lock without waiting. The lock is
// held until the returned file is closed, and the kernel releases it when
// the process dies, so a crash never leaves a stale lock.
func lockDir(dir string) (*os.File, error) {
	path := filepath.Join(dir, lockFileName)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, filePerm)
	if err != nil {
		return nil, fmt.Errorf("open lock file %s: %w", path, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("%w: %s is locked; only one writer may use a directory, because a second one would delete the open segment of the first", ErrDirLocked, path)
		}
		return nil, fmt.Errorf("lock %s: %w", path, err)
	}
	return f, nil
}

// unlockDir releases the lock. Closing the file releases the flock.
func unlockDir(f *os.File) error {
	if f == nil {
		return nil
	}
	return f.Close()
}
