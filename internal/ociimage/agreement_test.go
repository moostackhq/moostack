package ociimage_test

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"testing"

	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/images/archive"
	"github.com/containerd/containerd/v2/plugins/content/local"

	"github.com/moostackhq/moostack/internal/ociimage"
)

// ReadDigest and containerd's import agree on every archive: a digest
// ReadDigest reads is that of the one image of the index containerd's
// import takes, so that what a deploy decides by is what the node keeps.
func FuzzReadDigestAgreesWithContainerdsImport(f *testing.F) {
	var written bytes.Buffer
	if err := ociimage.Write(&written, image); err != nil {
		f.Fatal(err)
	}
	f.Add(written.Bytes())
	// The same archive as Docker's format, without its oci-layout and
	// with a manifest.json, which containerd reads instead.
	var docker bytes.Buffer
	tw, tr := tar.NewWriter(&docker), tar.NewReader(bytes.NewReader(written.Bytes()))
	for {
		h, err := tr.Next()
		if err != nil {
			break
		}
		data, _ := io.ReadAll(tr)
		if h.Name == "oci-layout" {
			h.Name, data = "manifest.json", []byte(`[{"Config":"blobs/sha256/x","RepoTags":["example.test/web:1"],"Layers":[]}]`)
			h.Size = int64(len(data))
		}
		tw.WriteHeader(h)
		tw.Write(data)
	}
	tw.Close()
	f.Add(docker.Bytes())
	f.Add([]byte("not an archive"))
	f.Fuzz(func(t *testing.T, data []byte) {
		digest, err := ociimage.ReadDigest(bytes.NewReader(data))
		if err != nil {
			return
		}
		store, err := local.NewStore(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		index, err := archive.ImportIndex(ctx, store, bytes.NewReader(data))
		if err != nil {
			return
		}
		blob, err := content.ReadBlob(ctx, store, index)
		if err != nil {
			t.Fatalf("containerd's import's index: %v", err)
		}
		var idx struct{ Manifests []struct{ Digest string } }
		if err := json.Unmarshal(blob, &idx); err != nil || len(idx.Manifests) != 1 || idx.Manifests[0].Digest != digest {
			t.Fatalf("ReadDigest read %s of an archive whose index containerd takes as %s", digest, blob)
		}
	})
}
