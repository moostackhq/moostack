package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestConsoleBuildsForEveryPlatform(t *testing.T) {
	if testing.Short() {
		t.Skip("builds console twice")
	}
	for _, arch := range []string{"arm64", "amd64"} {
		t.Run("linux/"+arch, func(t *testing.T) {
			cmd := exec.Command("go", "build", "-o", filepath.Join(t.TempDir(), "console"), ".")
			cmd.Env = append(os.Environ(), "GOOS=linux", "GOARCH="+arch, "CGO_ENABLED=0")
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Errorf("%v\n%s", err, out)
			}
		})
	}
}
