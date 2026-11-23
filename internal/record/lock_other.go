//go:build !unix

package record

import (
	"errors"
	"os"
)

const lockFileName = ".lock"

// ErrDirLocked is returned by OpenWriter when another Writer holds the
// output directory.
var ErrDirLocked = errors.New("output directory is used by another record writer")

// lockDir is not supported outside Unix. The project runs on Linux and
// macOS only.
func lockDir(string) (*os.File, error) {
	return nil, errors.New("directory lock is only supported on Unix")
}

func unlockDir(f *os.File) error {
	if f == nil {
		return nil
	}
	return f.Close()
}
