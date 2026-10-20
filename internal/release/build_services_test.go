package release_test

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/moostackhq/moostack/internal/release"
)

func TestBuildServicesBuildsWhereItReadsWhateverCargoIsConfiguredWith(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the cluster services with cargo")
	}
	// CARGO_BUILD_TARGET_DIR is cargo's config build.target-dir.
	for _, variable := range []string{"CARGO_TARGET_DIR", "CARGO_BUILD_TARGET_DIR"} {
		t.Run(variable, func(t *testing.T) {
			elsewhere := t.TempDir()
			t.Setenv(variable, elsewhere)
			if _, err := release.BuildServices(t.Context(), filepath.Join("..", ".."), runtime.GOARCH, "1.0.0", t.TempDir()); err != nil {
				t.Fatal(err)
			}
			if entries, _ := os.ReadDir(elsewhere); len(entries) > 0 {
				t.Errorf("cargo built into %s, not where BuildServices reads the binaries", variable)
			}
		})
	}
}
