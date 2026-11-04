//go:build linux

package nodetest

import (
	"testing"

	"github.com/moostackhq/moostack/internal/ociimage"
)

// largeFile is the data of img's file called large.
func largeFile(t *testing.T, img ociimage.Image) []byte {
	t.Helper()
	for _, f := range img.Files {
		if f.Name == "large" {
			return f.Data
		}
	}
	t.Fatal("a slow image has no large file")
	return nil
}

// Slow images share one large file, of 1 GiB, whatever else they are.
func TestSlowImagesShareTheirLargeFile(t *testing.T) {
	a := largeFile(t, Image{Name: "example.test/a:1", Slow: true}.of(nil))
	b := largeFile(t, Image{Name: "example.test/b:1", API: "a.Service", Slow: true}.of(nil))
	if len(a) != 1<<30 {
		t.Errorf("the large file has %d bytes, want 1 GiB", len(a))
	}
	if &a[0] != &b[0] {
		t.Error("two slow images have a large file each")
	}
}
