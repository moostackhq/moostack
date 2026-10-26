//go:build linux

package agent

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/errdefs"
	"github.com/containerd/platforms"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// checkPlatforms fails with FAILED_PRECONDITION unless each image that
// the index of the OCI image layout archive r lists is for node, the
// node's platform: by the platform its index entry says, if it says one,
// and by its config. An entry that is an index of images must have one
// for node, as a pull's must. containerd's import checks only an entry's
// platform, and only if it says one, so an image for another
// architecture would otherwise import, and fail only once its process
// starts. It fails otherwise, without a code, for an archive whose
// images it can't read, and for one that isn't an OCI image layout,
// which containerd would read by another index, its manifest.json. It
// reads the archive's headers once, within ctx, and its metadata alone.
func checkPlatforms(ctx context.Context, r io.ReadSeeker, node platforms.MatchComparer) error {
	blobs, err := readArchive(ctx, r)
	if err != nil {
		return err
	}
	data, err := blobs.index()
	if err != nil {
		return err
	}
	var idx ocispec.Index
	if err := json.Unmarshal(data, &idx); err != nil {
		return fmt.Errorf("reading the archive's index.json: %w", err)
	}
	for _, m := range idx.Manifests {
		if err := ctx.Err(); err != nil {
			return err
		}
		if m.Platform != nil {
			if p := platforms.Normalize(*m.Platform); !node.Match(p) {
				return status.Error(codes.FailedPrecondition, (&platformError{image: p}).Error())
			}
		}
		manifest, err := images.Manifest(ctx, blobs, m, node)
		if err != nil {
			if errdefs.IsNotFound(err) && strings.Contains(err.Error(), "no match for platform in manifest") {
				return status.Errorf(codes.FailedPrecondition, "it has no image for this node's %s: %v", platforms.DefaultString(), err)
			}
			return fmt.Errorf("reading the archive's image: %w", err)
		}
		data, err := content.ReadBlob(ctx, blobs, manifest.Config)
		if err != nil {
			return fmt.Errorf("reading the archive's image's config: %w", err)
		}
		var config ocispec.Image
		if err := json.Unmarshal(data, &config); err != nil {
			return fmt.Errorf("reading the archive's image's config: %w", err)
		}
		if p := platforms.Normalize(config.Platform); !node.Match(p) {
			return status.Error(codes.FailedPrecondition, (&platformError{image: p}).Error())
		}
	}
	return nil
}

// maxMetadata bounds a file of an archive's metadata that checkPlatforms
// reads: its layout, its index, a manifest or a config.
const maxMetadata = 4 << 20

// maxEntries bounds how many entries an archive may have, which an
// image's few blobs are far from.
const maxEntries = 100_000

// archiveFile is where a regular file of an archive is in it, and how
// large it is.
type archiveFile struct{ offset, size int64 }

// archiveBlobs are the regular files of an OCI image layout archive, a
// tar that r reads, by their cleaned paths, as containerd's import reads
// them: a content.Provider of the archive's blobs.
type archiveBlobs struct {
	// ctx ends what is read of r: a read fails once it ended.
	ctx context.Context
	r   io.ReadSeeker
	// first and last are the first and the last file of each path.
	first, last map[string]archiveFile
	// layouts are the oci-layout files, in order.
	layouts []archiveFile
}

// readArchive walks the headers of the tar r reads once, seeking past
// the files' contents, and returns where its regular files are. It
// fails once ctx ends, and for an archive of more than maxEntries.
func readArchive(ctx context.Context, r io.ReadSeeker) (archiveBlobs, error) {
	a := archiveBlobs{ctx: ctx, r: r, first: map[string]archiveFile{}, last: map[string]archiveFile{}}
	if _, err := r.Seek(0, io.SeekStart); err != nil {
		return a, err
	}
	// The context is checked at each read too, as one entry may be read
	// through any number of extended headers.
	tr := tar.NewReader(stoppable{ctx, r})
	for entries := 0; ; entries++ {
		if err := ctx.Err(); err != nil {
			return a, err
		}
		if entries > maxEntries {
			return a, fmt.Errorf("the archive has more than %d entries", maxEntries)
		}
		h, err := tr.Next()
		if err == io.EOF {
			return a, nil
		}
		if err != nil {
			return a, fmt.Errorf("reading the archive: %w", err)
		}
		if h.Typeflag != tar.TypeReg {
			continue
		}
		// The tar reader reads no further than a header: the file's
		// content starts where it stands. A sparse file's content isn't
		// that file, and its digest or its JSON then fails to read.
		offset, err := r.Seek(0, io.SeekCurrent)
		if err != nil {
			return a, err
		}
		f := archiveFile{offset: offset, size: h.Size}
		name := path.Clean(h.Name)
		if _, ok := a.first[name]; !ok {
			a.first[name] = f
		}
		a.last[name] = f
		if name == ocispec.ImageLayoutFile {
			a.layouts = append(a.layouts, f)
		}
	}
}

// stoppable is a reader whose reads fail once ctx ends, and which seeks
// as r does, as the tar reader seeks past the files' contents.
type stoppable struct {
	ctx context.Context
	r   io.ReadSeeker
}

func (s stoppable) Read(p []byte) (int, error) {
	if err := s.ctx.Err(); err != nil {
		return 0, err
	}
	return s.r.Read(p)
}

func (s stoppable) Seek(offset int64, whence int) (int64, error) {
	return s.r.Seek(offset, whence)
}

// index is the archive's index.json, the last one of that name, as
// containerd's import takes it. It fails unless the archive is an OCI
// image layout of the version containerd reads one by: containerd takes
// an archive without one as a Docker archive, by its manifest.json.
func (a archiveBlobs) index() ([]byte, error) {
	// containerd decodes each oci-layout in turn into one layout.
	var layout ocispec.ImageLayout
	for _, f := range a.layouts {
		data, err := a.read(ocispec.ImageLayoutFile, f)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(data, &layout); err != nil {
			return nil, fmt.Errorf("reading the archive's %s: %w", ocispec.ImageLayoutFile, err)
		}
	}
	if layout.Version != ocispec.ImageLayoutVersion {
		return nil, fmt.Errorf("the archive isn't an OCI image layout of version %s: its %s says %q", ocispec.ImageLayoutVersion, ocispec.ImageLayoutFile, layout.Version)
	}
	f, ok := a.last[ocispec.ImageIndexFile]
	if !ok {
		return nil, errors.New("the archive has no index.json")
	}
	return a.read(ocispec.ImageIndexFile, f)
}

// read is the content of the archive's file f, called name, at most
// maxMetadata.
func (a archiveBlobs) read(name string, f archiveFile) ([]byte, error) {
	if f.size > maxMetadata {
		return nil, fmt.Errorf("the archive's %s is %d bytes, more than %d", name, f.size, maxMetadata)
	}
	if err := a.ctx.Err(); err != nil {
		return nil, err
	}
	if _, err := a.r.Seek(f.offset, io.SeekStart); err != nil {
		return nil, err
	}
	data := make([]byte, f.size)
	if _, err := io.ReadFull(stoppable{a.ctx, a.r}, data); err != nil {
		return nil, fmt.Errorf("reading the archive's %s: %w", name, err)
	}
	return data, nil
}

func (a archiveBlobs) ReaderAt(_ context.Context, desc ocispec.Descriptor) (content.ReaderAt, error) {
	if err := desc.Digest.Validate(); err != nil {
		return nil, fmt.Errorf("the archive's descriptor: %w", err)
	}
	name := path.Join("blobs", desc.Digest.Algorithm().String(), desc.Digest.Encoded())
	f, ok := a.first[name]
	if !ok {
		return nil, fmt.Errorf("the archive has no %s: %w", name, errdefs.ErrNotFound)
	}
	data, err := a.read(name, f)
	if err != nil {
		return nil, err
	}
	// What is read is what the descriptor names, as containerd's import
	// verifies of what it writes, and of the size it says, as containerd
	// requires of an image's.
	if desc.Digest.Algorithm().FromBytes(data) != desc.Digest {
		return nil, fmt.Errorf("the archive's blob %s doesn't match its digest", desc.Digest)
	}
	if int64(len(data)) != desc.Size {
		return nil, fmt.Errorf("the archive's blob %s is %d bytes, not the %d its descriptor says", desc.Digest, len(data), desc.Size)
	}
	return blobReader{bytes.NewReader(data)}, nil
}

// blobReader is a blob read whole, as a content.ReaderAt.
type blobReader struct{ *bytes.Reader }

func (blobReader) Close() error { return nil }
