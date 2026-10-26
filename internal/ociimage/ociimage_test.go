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

func TestTheDigestOfAnArchiveIsItsImagesAndNamesItsBuild(t *testing.T) {
	digest := func(img ociimage.Image) string {
		t.Helper()
		var archive bytes.Buffer
		if err := ociimage.Write(&archive, img); err != nil {
			t.Fatal(err)
		}
		d, err := ociimage.Digest(archive.Bytes())
		if err != nil {
			t.Fatal(err)
		}
		return d
	}
	img := ociimage.Image{Name: "moostack.local/dns:1.0.0", Architecture: "amd64", Entrypoint: []string{"/dns"}, Files: []ociimage.File{{Name: "dns", Mode: 0o755, Data: []byte("one build")}}}
	first := digest(img)
	if !strings.HasPrefix(first, "sha256:") || len(first) != len("sha256:")+64 {
		t.Errorf("the digest is %q, want sha256: and 64 hex digits", first)
	}
	if again := digest(img); again != first {
		t.Errorf("the same image has the digests %s and %s", first, again)
	}
	img.Files[0].Data = []byte("another build")
	if other := digest(img); other == first {
		t.Errorf("another build under the same name has the same digest, %s", first)
	}
	if _, err := ociimage.Digest([]byte("not an archive")); err == nil {
		t.Error("Digest of something that isn't an archive succeeded")
	}
}

// An image's config names its user only if the image has one, so that an
// image without one is as it was before images had users.
func TestAnImagesConfigNamesItsUserIfItHasOne(t *testing.T) {
	for user, want := range map[string]string{"": `{"Entrypoint":["/registry"]}`, "1000:1001": `{"Entrypoint":["/registry"],"User":"1000:1001"}`} {
		img := image
		img.User = user
		blobs, index := read(t, write(t, img))
		var manifest struct {
			Config descriptor `json:"config"`
		}
		unmarshal(t, blob(t, blobs, index.Manifests[0]), &manifest)
		var config struct {
			Config json.RawMessage `json:"config"`
		}
		unmarshal(t, blob(t, blobs, manifest.Config), &config)
		if string(config.Config) != want {
			t.Errorf("an image of the user %q has the config %s, want %s", user, config.Config, want)
		}
	}
}

// ReadDigest takes the archive's index.json as containerd's import does:
// by its cleaned path, a regular file. It refuses an archive of two,
// whose import takes another than the first, and one without any.
func TestReadDigestTakesTheIndexAsContainerdsImportDoes(t *testing.T) {
	var archive bytes.Buffer
	if err := ociimage.Write(&archive, image); err != nil {
		t.Fatal(err)
	}
	want, err := ociimage.Digest(archive.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	var files []tarFile
	r := tar.NewReader(bytes.NewReader(archive.Bytes()))
	for {
		h, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(r)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, tarFile{h.Name, data})
	}
	rename := func(from, to string) []tarFile {
		var out []tarFile
		for _, f := range files {
			if f.name == from {
				f.name = to
			}
			out = append(out, f)
		}
		return out
	}
	index := rename("", "")[slicesIndex(files, "index.json")]
	for name, c := range map[string]struct {
		files []tarFile
		want  string
	}{
		"./index.json":         {rename("index.json", "./index.json"), want},
		"two index.json":       {append(rename("", ""), index), "more than one index.json"},
		"no index.json":        {rename("index.json", "other.json"), "has no index.json"},
		"index.json/ dir":      {append(rename("index.json", "other.json"), tarFile{"index.json/", nil}), "has no index.json"},
		"no oci-layout":        {rename("oci-layout", "other"), "isn't an OCI image layout of version 1.0.0"},
		"oci-layout 2.0.0":     {append(rename("", ""), tarFile{"oci-layout", []byte(`{"imageLayoutVersion":"2.0.0"}`)}), "its oci-layout says \"2.0.0\""},
		"a later empty layout": {append(rename("", ""), tarFile{"oci-layout", []byte(`{}`)}), want},
	} {
		got, err := ociimage.ReadDigest(bytes.NewReader(writeTar(t, c.files)))
		if err != nil && !strings.Contains(err.Error(), c.want) || err == nil && got != c.want {
			t.Errorf("ReadDigest of an archive with %s = %q, %v, want %q", name, got, err, c.want)
		}
	}
}

type tarFile struct {
	name string
	data []byte
}

func slicesIndex(files []tarFile, name string) int {
	for i, f := range files {
		if f.name == name {
			return i
		}
	}
	return -1
}

// writeTar is the tar of files, in order; a name ending in / is a
// directory.
func writeTar(t *testing.T, files []tarFile) []byte {
	t.Helper()
	var out bytes.Buffer
	w := tar.NewWriter(&out)
	for _, f := range files {
		h := &tar.Header{Name: f.name, Mode: 0o644, Size: int64(len(f.data)), Typeflag: tar.TypeReg}
		if strings.HasSuffix(f.name, "/") {
			h.Typeflag, h.Mode = tar.TypeDir, 0o755
		}
		if err := w.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write(f.data); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}
