//go:build unix

package sshconfig

import (
	"io/fs"
	"os"
	"syscall"
)

// securePermissions reports whether a config file may be read, as OpenSSH
// checks it: owned by root or the user running ssh, and writable by neither
// its group nor others.
func securePermissions(info fs.FileInfo) bool {
	if info.Mode().Perm()&0o022 != 0 {
		return false
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	return !ok || st.Uid == 0 || int(st.Uid) == os.Getuid()
}
