// Package buildinfo is what the build stamps into Moostack's programs.
package buildinfo

// Version is the Moostack release the program belongs to. The release
// build sets it with -ldflags "-X"; it is empty in other builds.
var Version string
