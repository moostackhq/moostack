//go:build linux

package agent_test

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"maps"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/containerd/containerd/v2/pkg/namespaces"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/moostackhq/moostack/internal/hostlayout"
	"github.com/moostackhq/moostack/internal/nodetest"
)

// withPlatform is the image layout archive data with the platform
// architecture set on its index's manifest, as buildx writes it.
func withPlatform(t *testing.T, data []byte, architecture string) []byte {
	t.Helper()
	var out bytes.Buffer
	r, w := tar.NewReader(bytes.NewReader(data)), tar.NewWriter(&out)
	for {
		h, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(r)
		if err != nil {
			t.Fatal(err)
		}
		if h.Name == "index.json" {
			var index map[string]any
			if err := json.Unmarshal(body, &index); err != nil {
				t.Fatal(err)
			}
			for _, m := range index["manifests"].([]any) {
				m.(map[string]any)["platform"] = map[string]string{"architecture": architecture, "os": "linux"}
			}
			if body, err = json.Marshal(index); err != nil {
				t.Fatal(err)
			}
			h.Size = int64(len(body))
		}
		if err := w.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write(body); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

// An image for another architecture is refused at its import, rather
// than once its process starts, whether the archive's index says its
// platform, as buildx writes it, or only its config does. The image of
// its name that the node had stays as it was.
func TestImportImageRefusesAnImageForAnotherPlatform(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	lease := hold(t, n)
	name := "moostack.local/web:1"
	own := nodetest.Image{Name: name}
	if err := importImage(t, n, lease, own.Archive(t)); err != nil {
		t.Fatalf("importing an image for this node: %v", err)
	}
	ctx := namespaces.WithNamespace(t.Context(), hostlayout.ContainerdNamespace)
	before, err := n.Containerd(t).ImageService().Get(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	other := nodetest.Image{Name: name, Architecture: otherArch()}.Archive(t)
	says := "the image is for linux/" + otherArch() + ", not for this node's linux/" + runtime.GOARCH
	ownBuild := nodetest.Image{Name: name, Rebuild: 3}.Archive(t)
	for what, archive := range map[string][]byte{
		"its config alone":            other,
		"its index and its config":    withPlatform(t, other, otherArch()),
		"its config, not its index's": withPlatform(t, other, runtime.GOARCH),
		"its index, not its config's": withPlatform(t, ownBuild, otherArch()),
	} {
		err := importImage(t, n, lease, archive)
		if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), says) {
			t.Errorf("ImportImage of an image for %s by %s: %v, want FAILED_PRECONDITION saying %q", otherArch(), what, err, says)
		}
		after, err := n.Containerd(t).ImageService().Get(ctx, name)
		if err != nil || after.Target.Digest != before.Target.Digest {
			t.Errorf("a refused import by %s left %s at %v, %v, want it as it was, %s", what, name, after.Target.Digest, err, before.Target.Digest)
		}
	}
	// An index that says this node's platform, of an image that is, is
	// imported, as buildx writes it.
	rebuilt := nodetest.Image{Name: name, Rebuild: 1}
	if err := importImage(t, n, lease, withPlatform(t, rebuilt.Archive(t), runtime.GOARCH)); err != nil {
		t.Errorf("ImportImage of an image for this node, its index saying so: %v", err)
	}
}

// withPlatform sets the platform in the index of the archive it makes.
func TestWithPlatformSetsTheIndexsPlatform(t *testing.T) {
	data := withPlatform(t, nodetest.Image{Name: "moostack.local/web:1"}.Archive(t), "arm64")
	files := archiveFiles(t, data)
	var index struct {
		Manifests []struct{ Platform map[string]string }
	}
	if err := json.Unmarshal(files["index.json"], &index); err != nil || len(index.Manifests) != 1 || index.Manifests[0].Platform["architecture"] != "arm64" {
		t.Errorf("withPlatform made the index %s: %v", files["index.json"], err)
	}
}

// archiveFiles are the files of the tar data, by name.
func archiveFiles(t *testing.T, data []byte) map[string][]byte {
	t.Helper()
	files := map[string][]byte{}
	r := tar.NewReader(bytes.NewReader(data))
	for {
		h, err := r.Next()
		if err == io.EOF {
			return files
		}
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(r)
		if err != nil {
			t.Fatal(err)
		}
		files[h.Name] = body
	}
}

// writeArchive is the tar of files, by name, in the order of names.
func writeArchive(t *testing.T, files map[string][]byte, names []string) []byte {
	t.Helper()
	var out bytes.Buffer
	w := tar.NewWriter(&out)
	for _, name := range names {
		if err := w.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(files[name])), Typeflag: tar.TypeReg}); err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write(files[name]); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

// platformed is an image of archive, its index's one manifest, said in
// an index to be for architecture.
type platformed struct {
	archive      []byte
	architecture string
}

// merged is one archive of the images of each of imgs, its index naming
// name: if nested, one entry, an index of the images, each said to be
// for its architecture; and otherwise an entry of each image.
func merged(t *testing.T, name string, nested bool, imgs ...platformed) []byte {
	t.Helper()
	files := map[string][]byte{}
	var manifests []map[string]any
	for _, img := range imgs {
		f := archiveFiles(t, img.archive)
		var index struct{ Manifests []map[string]any }
		if err := json.Unmarshal(f["index.json"], &index); err != nil || len(index.Manifests) != 1 {
			t.Fatalf("an image's index: %v", err)
		}
		for n, data := range f {
			if strings.HasPrefix(n, "blobs/") {
				files[n] = data
			}
		}
		m := index.Manifests[0]
		m["platform"] = map[string]string{"architecture": img.architecture, "os": "linux"}
		delete(m, "annotations")
		manifests = append(manifests, m)
	}
	annotations := map[string]string{"io.containerd.image.name": name}
	top := manifests
	if nested {
		blob, err := json.Marshal(map[string]any{"schemaVersion": 2, "mediaType": "application/vnd.oci.image.index.v1+json", "manifests": manifests})
		if err != nil {
			t.Fatal(err)
		}
		d := godigestFromBytes(blob)
		files["blobs/sha256/"+strings.TrimPrefix(d, "sha256:")] = blob
		top = []map[string]any{{"mediaType": "application/vnd.oci.image.index.v1+json", "digest": d, "size": len(blob), "annotations": annotations}}
	} else {
		for _, m := range top {
			m["annotations"] = annotations
		}
	}
	index, err := json.Marshal(map[string]any{"schemaVersion": 2, "manifests": top})
	if err != nil {
		t.Fatal(err)
	}
	files["index.json"] = index
	files["oci-layout"] = []byte(`{"imageLayoutVersion":"1.0.0"}`)
	var names []string
	for n := range files {
		names = append(names, n)
	}
	slices.Sort(names)
	return writeArchive(t, files, names)
}

func godigestFromBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// An archive of several images, or an index of them, is refused unless
// each image it names is for the node: an index of images must have one
// for the node, whose config says so too.
func TestImportImageChecksEveryImageOfAnArchive(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	lease := hold(t, n)
	own := nodetest.Image{Name: "moostack.local/a:1"}.Archive(t)
	ownAgain := nodetest.Image{Name: "moostack.local/a:1", Rebuild: 1}.Archive(t)
	other := nodetest.Image{Name: "moostack.local/a:1", Rebuild: 2, Architecture: otherArch()}.Archive(t)
	for what, c := range map[string]struct {
		archive []byte
		says    string
	}{
		"an index without the node's platform": {merged(t, "moostack.local/nested:1", true, platformed{other, otherArch()}),
			"it has no image for this node's linux/" + runtime.GOARCH},
		"an index whose entry for the node is another's": {merged(t, "moostack.local/nested:2", true, platformed{other, runtime.GOARCH}),
			"the image is for linux/" + otherArch()},
		"two entries, one for another platform": {merged(t, "moostack.local/two:1", false, platformed{own, runtime.GOARCH}, platformed{other, otherArch()}),
			"the image is for linux/" + otherArch()},
	} {
		err := importImage(t, n, lease, c.archive)
		if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), c.says) {
			t.Errorf("ImportImage of %s: %v, want FAILED_PRECONDITION saying %q", what, err, c.says)
		}
	}
	for what, archive := range map[string][]byte{
		"an index with the node's platform among others": merged(t, "moostack.local/nested:3", true, platformed{other, otherArch()}, platformed{ownAgain, runtime.GOARCH}),
		"two entries for the node":                       merged(t, "moostack.local/two:2", false, platformed{own, runtime.GOARCH}, platformed{ownAgain, runtime.GOARCH}),
	} {
		if err := importImage(t, n, lease, archive); err != nil {
			t.Errorf("ImportImage of %s: %v", what, err)
		}
	}
}

// An archive whose images can't be read is refused as an invalid
// archive, saying what is missing, rather than for the platform.
func TestImportImageRefusesAnArchiveItCantReadSayingWhy(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	lease := hold(t, n)
	files := archiveFiles(t, nodetest.Image{Name: "moostack.local/web:1"}.Archive(t))
	var index struct{ Manifests []struct{ Digest string } }
	if err := json.Unmarshal(files["index.json"], &index); err != nil {
		t.Fatal(err)
	}
	manifest := "blobs/sha256/" + strings.TrimPrefix(index.Manifests[0].Digest, "sha256:")
	var names []string
	for name := range files {
		if name != manifest {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	err := importImage(t, n, lease, writeArchive(t, files, names))
	if status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "importing the image: reading the archive's image: the archive has no "+manifest) ||
		strings.Contains(err.Error(), "rpc error: code = NotFound") {
		t.Errorf("ImportImage of an archive without its manifest: %v, want INVALID_ARGUMENT saying the manifest is missing", err)
	}
	// An index entry of no size, which containerd takes for no image.
	var whole map[string]any
	if err := json.Unmarshal(files["index.json"], &whole); err != nil {
		t.Fatal(err)
	}
	whole["manifests"].([]any)[0].(map[string]any)["size"] = 0
	zero, err := json.Marshal(whole)
	if err != nil {
		t.Fatal(err)
	}
	withZero := maps.Clone(files)
	withZero["index.json"] = zero
	all := slices.Sorted(maps.Keys(withZero))
	if err := importImage(t, n, lease, writeArchive(t, withZero, all)); status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "not the 0 its descriptor says") {
		t.Errorf("ImportImage of an index entry of no size: %v, want INVALID_ARGUMENT", err)
	}
	delete(files, "index.json")
	names = slices.DeleteFunc(names, func(n string) bool { return n == "index.json" })
	if err := importImage(t, n, lease, writeArchive(t, files, names)); status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "the archive has no index.json") {
		t.Errorf("ImportImage of an archive without index.json: %v", err)
	}
}

// An archive without an OCI image layout, which containerd's import reads
// in Docker's format, by its manifest.json, is refused: an index.json of
// an image for the node can't vouch for the images its manifest.json
// names, such as one for another architecture under a name the node has.
func TestImportImageRefusesAnArchiveThatIsntAnOCIImageLayout(t *testing.T) {
	n := nodetest.Start(t)
	n.EventuallyHealthy(t)
	lease := hold(t, n)
	name := "moostack.local/web:1"
	if err := importImage(t, n, lease, nodetest.Image{Name: name}.Archive(t)); err != nil {
		t.Fatal(err)
	}
	ctx := namespaces.WithNamespace(t.Context(), hostlayout.ContainerdNamespace)
	before, err := n.Containerd(t).ImageService().Get(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	decoy := archiveFiles(t, nodetest.Image{Name: "moostack.local/decoy:1", Rebuild: 6}.Archive(t))
	other := archiveFiles(t, nodetest.Image{Name: name, Rebuild: 5, Architecture: otherArch()}.Archive(t))
	paths := func(files map[string][]byte) (config, layer string) {
		var idx struct{ Manifests []struct{ Digest string } }
		json.Unmarshal(files["index.json"], &idx)
		var m struct {
			Config struct{ Digest string }
			Layers []struct{ Digest string }
		}
		json.Unmarshal(files["blobs/sha256/"+strings.TrimPrefix(idx.Manifests[0].Digest, "sha256:")], &m)
		return "blobs/sha256/" + strings.TrimPrefix(m.Config.Digest, "sha256:"), "blobs/sha256/" + strings.TrimPrefix(m.Layers[0].Digest, "sha256:")
	}
	dc, dl := paths(decoy)
	oc, ol := paths(other)
	files := map[string][]byte{"index.json": decoy["index.json"]}
	for _, f := range []map[string][]byte{decoy, other} {
		for k, v := range f {
			if strings.HasPrefix(k, "blobs/") {
				files[k] = v
			}
		}
	}
	mf, _ := json.Marshal([]map[string]any{
		{"Config": dc, "RepoTags": []string{"moostack.local/decoy:1"}, "Layers": []string{dl}},
		{"Config": oc, "RepoTags": []string{name}, "Layers": []string{ol}},
	})
	files["manifest.json"] = mf
	var names []string
	for k := range files {
		names = append(names, k)
	}
	slices.Sort(names)
	err = importImage(t, n, lease, writeArchive(t, files, names))
	if status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "the archive isn't an OCI image layout of version 1.0.0") {
		t.Errorf("ImportImage of an archive in Docker's format: %v, want it refused as no OCI image layout", err)
	}
	if after, err := n.Containerd(t).ImageService().Get(ctx, name); err != nil || after.Target.Digest != before.Target.Digest {
		t.Errorf("a refused archive in Docker's format left %s at %v, %v, want it as it was, %s", name, after.Target.Digest, err, before.Target.Digest)
	}
	if hasImage(t, n, "moostack.local/decoy:1") {
		t.Error("a refused archive left its decoy image")
	}
	// A layout of another version is refused too.
	files = archiveFiles(t, nodetest.Image{Name: "moostack.local/web:2"}.Archive(t))
	files["oci-layout"] = []byte(`{"imageLayoutVersion":"2.0.0"}`)
	names = names[:0]
	for k := range files {
		names = append(names, k)
	}
	slices.Sort(names)
	if err := importImage(t, n, lease, writeArchive(t, files, names)); status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), `its oci-layout says "2.0.0"`) {
		t.Errorf("ImportImage of a layout of version 2.0.0: %v", err)
	}
}
