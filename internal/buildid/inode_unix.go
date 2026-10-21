//go:build unix

package buildid

import (
	"os"
	"syscall"
)

// inode is the inode of the file info describes, which a rename over it
// changes.
func inode(info os.FileInfo) uint64 {
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		return uint64(st.Ino)
	}
	return 0
}
