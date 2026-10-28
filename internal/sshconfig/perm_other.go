//go:build !unix

package sshconfig

import "io/fs"

// securePermissions accepts every file where Unix owners and modes don't
// apply.
func securePermissions(fs.FileInfo) bool { return true }
