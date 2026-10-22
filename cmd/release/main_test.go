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

func TestBuildRefusesAReleaseVersionShapedLikeADevelopmentBuilds(t *testing.T) {
	out := t.TempDir()
	for _, args := range [][2]string{{"0123456789ab", ""}, {"0123456789ab-dirty", ""}, {"1.0.0", "0123456789ab"}} {
		err := build(context.Background(), args[0], args[1], out, false)
		if err == nil || !strings.Contains(err.Error(), "a development build's") {
			t.Errorf("build of %s after %q: %v", args[0], args[1], err)
		}
	}
	if left, _ := os.ReadDir(out); len(left) != 0 {
		t.Errorf("the refused builds wrote %v", left)
	}
}
