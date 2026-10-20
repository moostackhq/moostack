package ociimage_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/moostackhq/moostack/internal/ociimage"
)

var image = ociimage.Image{
	Name:         "moostack.local/registry:1.2.3",
	Architecture: "arm64",
	Entrypoint:   []string{"/registry"},
	Files:        []ociimage.File{{Name: "registry", Mode: 0o755, Data: []byte("#!binary\n")}},
}

func TestAnImageIsAnOCILayoutThatNamesItself(t *testing.T) {
	blobs, index := read(t, write(t, image))
	if len(index.Manifests) != 1 {
		t.Fatalf("the index has %d manifests, want 1", len(index.Manifests))
	}
	m := index.Manifests[0]
	if got := m.Annotations["io.containerd.image.name"]; got != "moostack.local/registry:1.2.3" {
		t.Errorf("the image is named %q for containerd", got)
	}
	if m.MediaType != "application/vnd.oci.image.manifest.v1+json" {
		t.Errorf("the manifest's media type is %s", m.MediaType)
	}
	var manifest struct {
		Config descriptor   `json:"config"`
		Layers []descriptor `json:"layers"`
	}
	unmarshal(t, blob(t, blobs, m), &manifest)
	var config struct {
		Architecture string `json:"architecture"`
		OS           string `json:"os"`
		Config       struct {
			Entrypoint []string `json:"Entrypoint"`
		} `json:"config"`
		RootFS struct {
			Type    string   `json:"type"`
			DiffIDs []string `json:"diff_ids"`
		} `json:"rootfs"`
	}
	unmarshal(t, blob(t, blobs, manifest.Config), &config)
	if config.Architecture != "arm64" || config.OS != "linux" || fmt.Sprint(config.Config.Entrypoint) != "[/registry]" {
		t.Errorf("the config is for %s/%s with entrypoint %v", config.OS, config.Architecture, config.Config.Entrypoint)
	}
	if len(manifest.Layers) != 1 || len(config.RootFS.DiffIDs) != 1 {
		t.Fatalf("%d layers and %d diff IDs, want 1 each", len(manifest.Layers), len(config.RootFS.DiffIDs))
	}
	if manifest.Layers[0].MediaType != "application/vnd.oci.image.layer.v1.tar+gzip" {
		t.Errorf("the layer's media type is %s", manifest.Layers[0].MediaType)
	}
	gz, err := gzip.NewReader(bytes.NewReader(blob(t, blobs, manifest.Layers[0])))
	if err != nil {
		t.Fatal(err)
	}
	layer, err := io.ReadAll(gz)
	if err != nil {
		t.Fatal(err)
	}
	if got := digest(layer); got != config.RootFS.DiffIDs[0] {
		t.Errorf("the layer's diff ID is %s, want %s", config.RootFS.DiffIDs[0], got)
	}
	files := untar(t, layer)
	f, ok := files["registry"]
	if !ok || string(f.data) != "#!binary\n" || f.mode != 0o755 {
		t.Errorf("the layer has %v, want registry with mode 755", files)
	}
}

func TestAnImageIsTheSameEveryTime(t *testing.T) {
	if !bytes.Equal(write(t, image), write(t, image)) {
		t.Error("two archives of the same image differ")
	}
}

func TestWriteFileWritesTheImagesArchiveReadableByAll(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.tar")
	if err := ociimage.WriteFile(path, image); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, write(t, image)) {
		t.Error("the file isn't the image's archive")
	}
	if info, err := os.Stat(path); err != nil || info.Mode().Perm() != 0o644 {
		t.Errorf("the file is %v, %v, want mode 0644", info, err)
	}
}

type descriptor struct {
	MediaType   string            `json:"mediaType"`
	Digest      string            `json:"digest"`
	Size        int64             `json:"size"`
	Annotations map[string]string `json:"annotations"`
}

type entry struct {
	mode int64
	data []byte
}

func write(t *testing.T, img ociimage.Image) []byte {
	t.Helper()
	var b bytes.Buffer
	if err := ociimage.Write(&b, img); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

// read is an archive's blobs by digest, and its index, having checked
// its oci-layout file.
func read(t *testing.T, archive []byte) (map[string][]byte, struct{ Manifests []descriptor }) {
	t.Helper()
	files := untar(t, archive)
	var layout struct {
		Version string `json:"imageLayoutVersion"`
	}
	unmarshal(t, files["oci-layout"].data, &layout)
	if layout.Version != "1.0.0" {
		t.Errorf("the image layout's version is %q", layout.Version)
	}
	var index struct{ Manifests []descriptor }
	unmarshal(t, files["index.json"].data, &index)
	blobs := map[string][]byte{}
	for name, f := range files {
		if hex, ok := strings.CutPrefix(name, "blobs/sha256/"); ok {
			blobs["sha256:"+hex] = f.data
		}
	}
	return blobs, index
}

// blob is the blob d describes, checked against its digest and size.
func blob(t *testing.T, blobs map[string][]byte, d descriptor) []byte {
	t.Helper()
	b, ok := blobs[d.Digest]
	if !ok {
		t.Fatalf("no blob %s", d.Digest)
	}
	if digest(b) != d.Digest || int64(len(b)) != d.Size {
		t.Fatalf("blob %s has digest %s and size %d, want size %d", d.Digest, digest(b), len(b), d.Size)
	}
	return b
}

func untar(t *testing.T, data []byte) map[string]entry {
	t.Helper()
	files := map[string]entry{}
	r := tar.NewReader(bytes.NewReader(data))
	for {
		h, err := r.Next()
		if err == io.EOF {
			return files
		}
		if err != nil {
			t.Fatal(err)
		}
		b, err := io.ReadAll(r)
		if err != nil {
			t.Fatal(err)
		}
		files[h.Name] = entry{h.Mode, b}
	}
}

func unmarshal(t *testing.T, data []byte, v any) {
	t.Helper()
	if err := json.Unmarshal(data, v); err != nil {
		t.Fatalf("%v: %s", err, data)
	}
}

func digest(b []byte) string {
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}
