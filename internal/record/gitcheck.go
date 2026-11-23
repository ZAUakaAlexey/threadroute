package record

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// ErrInsideGitTree is returned when the output directory is inside a git
// working tree.
var ErrInsideGitTree = errors.New("output directory is inside a git working tree")

// CheckNotInGitTree returns an error wrapping ErrInsideGitTree if dir, or
// any parent of it, contains a .git entry (a directory, or a file in a
// worktree or submodule). Recordings hold real post text and author IDs and
// must never be stored in a repository (decision 0004).
//
// dir does not have to exist. Both the path as given and the path with
// symbolic links resolved are checked.
func CheckNotInGitTree(dir string) error {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return fmt.Errorf("check git working tree: %w", err)
	}
	starts := []string{abs}
	if real, err := resolveExisting(abs); err != nil {
		return fmt.Errorf("check git working tree: %w", err)
	} else if real != abs {
		starts = append(starts, real)
	}
	for _, start := range starts {
		if root := findGitRoot(start); root != "" {
			return fmt.Errorf("%w: %s is inside %s; recordings hold real post text and author IDs and must stay outside any repository (decision 0004), so choose a directory such as $HOME/threadroute-recordings",
				ErrInsideGitTree, dir, root)
		}
	}
	return nil
}

// resolveExisting resolves symbolic links in the nearest existing ancestor
// of path and appends the parts that do not exist yet.
func resolveExisting(path string) (string, error) {
	existing, rest := path, ""
	for {
		if _, err := os.Lstat(existing); err == nil {
			break
		} else if !errors.Is(err, fs.ErrNotExist) {
			return "", err
		}
		parent := filepath.Dir(existing)
		if parent == existing {
			return path, nil
		}
		rest = filepath.Join(filepath.Base(existing), rest)
		existing = parent
	}
	real, err := filepath.EvalSymlinks(existing)
	if err != nil {
		return "", err
	}
	return filepath.Join(real, rest), nil
}

// findGitRoot walks up from dir and returns the first directory that holds a
// .git entry, or "" if there is none. Any error from Lstat (the entry does
// not exist, a part of the path is not a directory yet) means "no .git
// here", and the walk goes on to the parent.
func findGitRoot(dir string) string {
	for d := dir; ; {
		if _, err := os.Lstat(filepath.Join(d, ".git")); err == nil {
			return d
		}
		parent := filepath.Dir(d)
		if parent == d {
			return ""
		}
		d = parent
	}
}
