package release_test

import (
	"strings"
	"testing"

	"github.com/moostackhq/moostack/internal/release"
)

func TestTheProgramsAreStampedWithTheReleaseAndTheOneBefore(t *testing.T) {
	flags := release.Ldflags("1.0.0", "0.9.0")
	for _, want := range []string{
		"-X github.com/moostackhq/moostack/internal/buildinfo.Version=1.0.0",
		"-X github.com/moostackhq/moostack/internal/buildinfo.Previous=0.9.0",
	} {
		if !strings.Contains(flags, want) {
			t.Errorf("the ldflags %q don't contain %q", flags, want)
		}
	}
}
