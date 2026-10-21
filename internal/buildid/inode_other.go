//go:build !unix

package buildid

import "os"

// inode is 0 where the system has no inodes: the size and the
// modification time tell a change.
func inode(os.FileInfo) uint64 { return 0 }
