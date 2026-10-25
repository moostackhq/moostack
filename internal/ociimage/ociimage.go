// Package ociimage writes container images as OCI image layout archives,
// which containerd imports without a registry. An image is a single
// layer of files.
package ociimage

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
)

// Image is an image of one layer.
type Image struct {
	// Name is the reference containerd imports the image under, such as
	// moostack.local/registry:1.0.0.
	Name string
	// Architecture is amd64 or arm64.
	Architecture string
	// Entrypoint is the command the image's container runs.
	Entrypoint []string
	// User is the user the image's container runs as, root if empty.
	User string
	// Files are the layer's files, at the root of the image.
	Files []File
}

// File is a regular file in an image.
type File struct {
	// Name is the file's name, at the root of the image.
	Name string
	// Mode is its permission bits.
	Mode fs.FileMode
	// Data is its content.
	Data []byte
}

const (
	// The media types of an image's manifest, config, layer and index,
	// as OCI names them.
	manifestType = "application/vnd.oci.image.manifest.v1+json"
	configType   = "application/vnd.oci.image.config.v1+json"
	layerType    = "application/vnd.oci.image.layer.v1.tar+gzip"
	indexType    = "application/vnd.oci.image.index.v1+json"
	// nameAnnotation is the annotation containerd names an imported
	// image by.
	nameAnnotation = "io.containerd.image.name"
)

// descriptor points at a blob of the image by its digest, as OCI
// descriptors do.
type descriptor struct {
	MediaType   string            `json:"mediaType"`
	Digest      string            `json:"digest"`
	Size        int               `json:"size"`
	Annotations map[string]string `json:"annotations,omitempty"`
}

// WriteFile writes img's archive to the file at path, readable by all.
func WriteFile(path string, img Image) error {
	var archive bytes.Buffer
	if err := Write(&archive, img); err != nil {
		return err
	}
	return os.WriteFile(path, archive.Bytes(), 0o644)
}

// Write writes img to w as an OCI image layout archive. The same image
// always makes the same archive: every time in it is zero.
func Write(w io.Writer, img Image) error {
	var layer bytes.Buffer
	if err := writeTar(&layer, img.Files); err != nil {
		return err
	}
	var compressed bytes.Buffer
	gz := gzip.NewWriter(&compressed)
	if _, err := gz.Write(layer.Bytes()); err != nil {
		return err
	}
	if err := gz.Close(); err != nil {
		return err
	}
	runConfig := map[string]any{"Entrypoint": img.Entrypoint}
	if img.User != "" {
		runConfig["User"] = img.User
	}
	config, err := json.Marshal(map[string]any{
		"architecture": img.Architecture,
		"os":           "linux",
		"config":       runConfig,
		"rootfs":       map[string]any{"type": "layers", "diff_ids": []string{digest(layer.Bytes())}},
	})
	if err != nil {
		return err
	}
	manifest, err := json.Marshal(map[string]any{
		"schemaVersion": 2,
		"mediaType":     manifestType,
		"config":        describe(configType, config),
		"layers":        []descriptor{describe(layerType, compressed.Bytes())},
	})
	if err != nil {
		return err
	}
	m := describe(manifestType, manifest)
	m.Annotations = map[string]string{nameAnnotation: img.Name}
	index, err := json.Marshal(map[string]any{
		"schemaVersion": 2,
		"mediaType":     indexType,
		"manifests":     []descriptor{m},
	})
	if err != nil {
		return err
	}
	files := []File{
		{Name: "oci-layout", Mode: 0o644, Data: []byte(`{"imageLayoutVersion":"1.0.0"}`)},
		{Name: "index.json", Mode: 0o644, Data: index},
	}
	for _, b := range [][]byte{config, manifest, compressed.Bytes()} {
		files = append(files, File{Name: "blobs/sha256/" + digest(b)[len("sha256:"):], Mode: 0o644, Data: b})
	}
	return writeTar(w, files)
}

// writeTar writes files to w as a tar archive.
func writeTar(w io.Writer, files []File) error {
	t := tar.NewWriter(w)
	for _, f := range files {
		h := &tar.Header{Name: f.Name, Mode: int64(f.Mode.Perm()), Size: int64(len(f.Data)), Typeflag: tar.TypeReg, Format: tar.FormatPAX}
		if err := t.WriteHeader(h); err != nil {
			return err
		}
		if _, err := t.Write(f.Data); err != nil {
			return err
		}
	}
	return t.Close()
}

// describe is the descriptor of the blob b, of mediaType.
func describe(mediaType string, b []byte) descriptor {
	return descriptor{MediaType: mediaType, Digest: digest(b), Size: len(b)}
}

// digest is the OCI digest of b, by sha256.
func digest(b []byte) string {
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// Digest is the digest of the image in the OCI image layout archive
// archive, an image of one manifest as Write writes, which containerd
// knows the image by once imported. Another build of an image, under the
// same name, has another.
func Digest(archive []byte) (string, error) {
	r := tar.NewReader(bytes.NewReader(archive))
	for {
		h, err := r.Next()
		if err == io.EOF {
			return "", errors.New("the archive has no index.json")
		}
		if err != nil {
			return "", err
		}
		if h.Name != "index.json" {
			continue
		}
		var index struct{ Manifests []descriptor }
		if err := json.NewDecoder(r).Decode(&index); err != nil {
			return "", fmt.Errorf("index.json: %w", err)
		}
		if len(index.Manifests) != 1 {
			return "", fmt.Errorf("index.json lists %d images, not one", len(index.Manifests))
		}
		return index.Manifests[0].Digest, nil
	}
}
