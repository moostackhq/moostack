// Package buildid names a build of a node component by its files' SHA-256
// sums, as a release's SHA256SUMS gives them, so that two builds of one
// version are told apart.
package buildid

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"maps"
	"os"
	"slices"
	"sync"
	"time"
)

// FromSums is the build of a component whose files, by name, have the
// hex SHA-256 sums sums.
func FromSums(sums map[string]string) string {
	h := sha256.New()
	for _, name := range slices.Sorted(maps.Keys(sums)) {
		fmt.Fprintf(h, "%s %s\n", name, sums[name])
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil))
}

// Cache tells the build of files on disk, hashing a file again only once
// it has changed. Its zero value is ready to use.
type Cache struct {
	mu     sync.Mutex
	hashed map[string]hashed
	// hashing are the files being hashed in the background, by path.
	hashing map[string]bool
}

// hashed is a file's sum, as of its size, modification time and inode.
type hashed struct {
	size  int64
	mtime time.Time
	inode uint64
	sum   string
}

// Of is the build of the files at the paths, by the names the release
// gives them.
func (c *Cache) Of(paths map[string]string) (string, error) {
	sums := map[string]string{}
	for name, path := range paths {
		sum, err := c.sum(path)
		if err != nil {
			return "", err
		}
		sums[name] = sum
	}
	return FromSums(sums), nil
}

// Peek is Of, if each file at the paths is hashed already and unchanged
// since, and else empty, while it hashes the others in the background,
// so that a caller that asks often, as a status call does, never waits
// for a large file to be hashed.
func (c *Cache) Peek(paths map[string]string) string {
	sums := map[string]string{}
	known := true
	for name, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			return ""
		}
		c.mu.Lock()
		h, ok := c.hashed[path]
		if ok && h.size == info.Size() && h.mtime.Equal(info.ModTime()) && h.inode == inode(info) {
			sums[name] = h.sum
		} else {
			known = false
			if !c.hashing[path] {
				if c.hashing == nil {
					c.hashing = map[string]bool{}
				}
				c.hashing[path] = true
				go func() {
					c.sum(path)
					c.mu.Lock()
					delete(c.hashing, path)
					c.mu.Unlock()
				}()
			}
		}
		c.mu.Unlock()
	}
	if !known {
		return ""
	}
	return FromSums(sums)
}

// sum is the hex SHA-256 of the file at path.
func (c *Cache) sum(path string) (string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	key := hashed{size: info.Size(), mtime: info.ModTime(), inode: inode(info)}
	c.mu.Lock()
	h, ok := c.hashed[path]
	c.mu.Unlock()
	if ok && h.size == key.size && h.mtime.Equal(key.mtime) && h.inode == key.inode {
		return h.sum, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	s := sha256.New()
	if _, err := io.Copy(s, f); err != nil {
		return "", err
	}
	key.sum = hex.EncodeToString(s.Sum(nil))
	c.mu.Lock()
	if c.hashed == nil {
		c.hashed = map[string]hashed{}
	}
	c.hashed[path] = key
	c.mu.Unlock()
	return key.sum, nil
}
