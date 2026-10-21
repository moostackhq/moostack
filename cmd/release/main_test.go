package main

import (
	"context"
	"os"
	"strings"
	"testing"
)

func TestBuildRefusesAPreviousReleaseThatIsntAReleaseVersion(t *testing.T) {
	out := t.TempDir()
	for _, previous := range []string{"../0.9.0", "..", ".hidden", "0.9 0"} {
		err := build(context.Background(), "1.0.0", previous, out, false)
		if err == nil || !strings.Contains(err.Error(), "isn't a release version") {
			t.Errorf("build with -previous %q: %v", previous, err)
		}
	}
	if left, _ := os.ReadDir(out); len(left) != 0 {
		t.Errorf("the refused builds wrote %v", left)
	}
}

func TestTheReleaseNamesTheReleaseBefore(t *testing.T) {
	if m := manifest("1.0.0", "0.9.0", nil); m.Version != "1.0.0" || m.Previous != "0.9.0" {
		t.Errorf("the manifest is for %s after %q, want 1.0.0 after 0.9.0", m.Version, m.Previous)
	}
	if m := manifest("1.0.0", "", nil); m.Previous != "" {
		t.Errorf("the first release's manifest names %q before it, want none", m.Previous)
	}
}

func TestTheProgramsAreStampedWithTheReleaseAndTheOneBefore(t *testing.T) {
	flags := ldflags("1.0.0", "0.9.0")
	for _, want := range []string{
		"-X github.com/moostackhq/moostack/internal/buildinfo.Version=1.0.0",
		"-X github.com/moostackhq/moostack/internal/buildinfo.Previous=0.9.0",
	} {
		if !strings.Contains(flags, want) {
			t.Errorf("the ldflags %q don't contain %q", flags, want)
		}
	}
}
