package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestMooctlBuildsForEveryPlatform(t *testing.T) {
	if testing.Short() {
		t.Skip("builds mooctl four times")
	}
	for _, platform := range [][2]string{{"darwin", "arm64"}, {"darwin", "amd64"}, {"linux", "arm64"}, {"linux", "amd64"}} {
		t.Run(platform[0]+"/"+platform[1], func(t *testing.T) {
			cmd := exec.Command("go", "build", "-o", filepath.Join(t.TempDir(), "mooctl"), ".")
			cmd.Env = append(os.Environ(), "GOOS="+platform[0], "GOARCH="+platform[1], "CGO_ENABLED=0")
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Errorf("%v\n%s", err, out)
			}
		})
	}
}
