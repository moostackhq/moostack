package ociimage_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"math/rand/v2"
	"runtime"
	"testing"

	"github.com/moostackhq/moostack/internal/ociimage"
)

// An image of several files, large enough for gzip to end several
// blocks, makes the archive of the recorded SHA-256.
func TestAnImagesArchiveHasTheRecordedSHA256(t *testing.T) {
	data := make([]byte, 3<<20)
	for i := range data {
		data[i] = byte(i * i >> 7)
	}
	img := ociimage.Image{
		Name:         "moostack.local/golden:1.0.0",
		Architecture: "amd64",
		Entrypoint:   []string{"/program", "-flag"},
		User:         "nobody",
		Files: []ociimage.File{
			{Name: "program", Mode: 0o755, Data: data},
			{Name: "etc/config", Mode: 0o644, Data: []byte("a file\n")},
			{Name: "empty", Mode: 0o600},
			{Name: "zeros", Mode: 0o644, Data: make([]byte, 1<<20)},
		},
	}
	sum := sha256.Sum256(write(t, img))
	if got, want := hex.EncodeToString(sum[:]), "16f94dd9f2cf21fd4c73c9d9ce63dfd0061ee333daf0b29443c1a7b7394e3e58"; got != want {
		t.Errorf("the archive's SHA-256 is %s, want %s", got, want)
	}
}

// Writing an image of a file that compresses well allocates far less
// than the file holds: nothing keeps the uncompressed layer.
func TestWritingAnImageHoldsNoUncompressedLayer(t *testing.T) {
	const size = 64 << 20
	img := ociimage.Image{Name: "moostack.local/large:1.0.0", Architecture: "amd64", Files: []ociimage.File{{Name: "large", Mode: 0o644, Data: make([]byte, size)}}}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	if err := ociimage.Write(io.Discard, img); err != nil {
		t.Fatal(err)
	}
	runtime.ReadMemStats(&after)
	if got := after.TotalAlloc - before.TotalAlloc; got > size/2 {
		t.Errorf("writing an image of %d bytes allocated %d bytes", size, got)
	}
}

// layerOf is the one layer of archive, uncompressed, and its diff ID.
func layerOf(t *testing.T, archive []byte) ([]byte, string) {
	t.Helper()
	blobs, index := read(t, archive)
	var manifest struct {
		Config struct{ Digest string }
		Layers []struct{ Digest string }
	}
	unmarshal(t, blobs[index.Manifests[0].Digest], &manifest)
	var config struct {
		RootFS struct {
			DiffIDs []string `json:"diff_ids"`
		} `json:"rootfs"`
	}
	unmarshal(t, blobs[manifest.Config.Digest], &config)
	gz, err := gzip.NewReader(bytes.NewReader(blobs[manifest.Layers[0].Digest]))
	if err != nil {
		t.Fatal(err)
	}
	layer, err := io.ReadAll(gz)
	if err != nil {
		t.Fatal(err)
	}
	return layer, config.RootFS.DiffIDs[0]
}

// checkLayer checks that img's archive has a layer of img's files, in
// order, under the diff ID of that layer.
func checkLayer(t *testing.T, img ociimage.Image) {
	t.Helper()
	layer, diffID := layerOf(t, write(t, img))
	sum := sha256.Sum256(layer)
	if want := "sha256:" + hex.EncodeToString(sum[:]); diffID != want {
		t.Errorf("the diff ID is %s, the layer's digest %s", diffID, want)
	}
	tr := tar.NewReader(bytes.NewReader(layer))
	for _, f := range img.Files {
		h, err := tr.Next()
		if err != nil {
			t.Fatalf("reading the layer at %s: %v", f.Name, err)
		}
		data, err := io.ReadAll(tr)
		if err != nil || h.Name != f.Name || !bytes.Equal(data, f.Data) {
			t.Errorf("the layer has %s of %d bytes, %v, want %s of %d", h.Name, len(data), err, f.Name, len(f.Data))
		}
	}
	if _, err := tr.Next(); err != io.EOF {
		t.Errorf("the layer has more than the image's files: %v", err)
	}
}

// Every image's layer holds its files, under the digest of what was
// compressed.
func TestEveryImagesLayerHoldsItsFilesUnderItsDiffID(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	for i := range 200 {
		img := ociimage.Image{Name: "moostack.local/a:1", Architecture: "amd64"}
		for j := range r.IntN(5) {
			data := make([]byte, []int{0, 1, 511, 512, 513, 65535, 65536, 200000}[r.IntN(8)])
			for k := range data {
				data[k] = byte(r.IntN(1 + i%256))
			}
			img.Files = append(img.Files, ociimage.File{Name: "f" + string(rune('a'+j)), Mode: 0o644, Data: data})
		}
		checkLayer(t, img)
	}
}

// A file tar can't write fails the write.
func TestAFileTarRefusesFailsTheWrite(t *testing.T) {
	img := ociimage.Image{Name: "moostack.local/a:1", Architecture: "amd64", Files: []ociimage.File{{Name: "a\x00b", Mode: 0o644}}}
	if err := ociimage.Write(io.Discard, img); err == nil {
		t.Error("an image of a file with a NUL in its name was written")
	}
}

func FuzzWriteKeepsTheLayer(f *testing.F) {
	f.Add("a", []byte("data"), []byte{})
	f.Add("dir/b", make([]byte, 70000), []byte("x"))
	f.Fuzz(func(t *testing.T, name string, a, b []byte) {
		if name == "" || bytes.ContainsAny([]byte(name), "\x00") {
			t.Skip()
		}
		checkLayer(t, ociimage.Image{Name: "moostack.local/a:1", Architecture: "amd64", Files: []ociimage.File{{Name: name, Mode: 0o644, Data: a}, {Name: name + "2", Mode: 0o644, Data: b}}})
	})
}

func BenchmarkWrite(b *testing.B) {
	data := make([]byte, 16<<20)
	for i := range data {
		data[i] = byte(i * i >> 7)
	}
	img := ociimage.Image{Name: "moostack.local/a:1", Architecture: "amd64", Files: []ociimage.File{{Name: "program", Mode: 0o755, Data: data}}}
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	for b.Loop() {
		if err := ociimage.Write(io.Discard, img); err != nil {
			b.Fatal(err)
		}
	}
}
