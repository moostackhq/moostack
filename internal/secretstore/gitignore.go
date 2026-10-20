package secretstore

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
)

// IgnoreLockFile adds LockFileName to the .gitignore in dir, the cluster
// directory, creating the .gitignore if there is none, so that the lock
// file Save leaves next to the store isn't committed. It reports whether
// it changed the file; a line that is already there is left alone.
func IgnoreLockFile(dir string) (changed bool, err error) {
	path := filepath.Join(dir, ".gitignore")
	data, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	line := []byte("/" + LockFileName)
	for _, l := range bytes.Split(data, []byte("\n")) {
		if l = bytes.TrimSpace(l); bytes.Equal(l, line) || bytes.Equal(l, line[1:]) {
			return false, nil
		}
	}
	if len(data) > 0 && !bytes.HasSuffix(data, []byte("\n")) {
		data = append(data, '\n')
	}
	data = append(append(data, line...), '\n')
	return true, os.WriteFile(path, data, 0o644)
}
