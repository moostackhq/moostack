// Package registry is the cluster's registry: its name and port, the
// scripts that install and remove zot on the control plane, and the push
// of an app's built images, as OCI image layouts, one per architecture,
// as one image index.
package registry

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
	"github.com/google/go-containerregistry/pkg/v1/types"
)

// Push pushes the images in layouts, by architecture, each an OCI image
// layout's directory or an archive of one, as one index named for app, to the registry at host, which dial connects to. It
// returns the index's tag: its digest's hex. progress, if not nil, is
// called as the push goes on, with the bytes pushed or found there
// already, and the bytes of the whole push.
func Push(ctx context.Context, layouts map[string]string, host, app string, dial Dial, progress func(complete, total int64)) (string, error) {
	idx := mutate.IndexMediaType(empty.Index, types.OCIImageIndex)
	arches := make([]string, 0, len(layouts))
	for arch := range layouts {
		arches = append(arches, arch)
	}
	slices.Sort(arches)
	for _, arch := range arches {
		img, err := readLayout(layouts[arch], arch)
		if err != nil {
			return "", fmt.Errorf("the linux/%s image: %w", arch, err)
		}
		idx = mutate.AppendManifests(idx, mutate.IndexAddendum{
			Add:        img,
			Descriptor: v1.Descriptor{Platform: &v1.Platform{OS: "linux", Architecture: arch}},
		})
	}
	digest, err := idx.Digest()
	if err != nil {
		return "", err
	}
	tag := digest.Hex
	ref, err := name.NewTag(host+"/"+app+":"+tag, name.Insecure)
	if err != nil {
		return "", err
	}
	opts := []remote.Option{remote.WithContext(ctx), remote.WithTransport(httpTransport(dial))}
	reported := make(chan struct{})
	var total int64
	if progress == nil {
		close(reported)
	} else {
		if total, err = pushSize(idx); err != nil {
			return "", err
		}
		// WriteIndex closes updates when it returns.
		updates := make(chan v1.Update, 64)
		opts = append(opts, remote.WithProgress(updates))
		go func() {
			defer close(reported)
			for u := range updates {
				if u.Error == nil {
					progress(u.Complete, total)
				}
			}
		}()
	}
	err = remote.WriteIndex(ref, idx, opts...)
	<-reported
	if err != nil {
		return "", fmt.Errorf("pushing to the cluster's registry at %s: %w", host, err)
	}
	if progress != nil {
		// An image the registry had already was never counted.
		progress(total, total)
	}
	return tag, nil
}

// pushSize is the size of idx with its images: its manifests, configs
// and layers, as stored.
func pushSize(idx v1.ImageIndex) (int64, error) {
	total, err := idx.Size()
	if err != nil {
		return 0, err
	}
	m, err := idx.IndexManifest()
	if err != nil {
		return 0, err
	}
	for _, d := range m.Manifests {
		img, err := idx.Image(d.Digest)
		if err != nil {
			return 0, err
		}
		im, err := img.Manifest()
		if err != nil {
			return 0, err
		}
		total += d.Size + im.Config.Size
		for _, l := range im.Layers {
			total += l.Size
		}
	}
	return total, nil
}

// Tag gives the image of digest, in the repository repo of the registry
// at host, the tag as well.
func Tag(ctx context.Context, host, repo, digest, tag string, dial Dial) error {
	ref, err := name.NewDigest(host+"/"+repo+"@sha256:"+digest, name.Insecure)
	if err != nil {
		return err
	}
	opts := []remote.Option{remote.WithContext(ctx), remote.WithTransport(httpTransport(dial))}
	desc, err := remote.Get(ref, opts...)
	if err == nil {
		err = remote.Tag(ref.Context().Tag(tag), desc, opts...)
	}
	if err != nil {
		return fmt.Errorf("tagging %s in the cluster's registry at %s: %w", repo, host, err)
	}
	return nil
}

// Dial connects to the registry at addr.
type Dial func(ctx context.Context, addr string) (net.Conn, error)

// dialTimeout bounds connecting to the registry, and a TLS handshake
// with it, and responseTimeout its answer to a request once sent, and
// any stall of a connection while a request or answer is under way.
var (
	dialTimeout     = 30 * time.Second
	responseTimeout = 2 * time.Minute
)

// httpTransport is an HTTP transport to the registry through dial.
func httpTransport(dial Dial) *http.Transport {
	return &http.Transport{
		DialContext: func(ctx context.Context, _, addr string) (net.Conn, error) {
			ctx, cancel := context.WithTimeout(ctx, dialTimeout)
			defer cancel()
			c, err := dial(ctx, addr)
			if err != nil {
				return nil, err
			}
			return watch(c, responseTimeout), nil
		},
		TLSHandshakeTimeout:   dialTimeout,
		ResponseHeaderTimeout: responseTimeout,
	}
}

// Remove deletes every image of app from the registry at host, which
// dial connects to: those of the repository app, and of each repository
// app/<container>. An app with no images there is no error.
func Remove(ctx context.Context, host, app string, dial Dial) error {
	reg, err := name.NewRegistry(host, name.Insecure)
	if err != nil {
		return err
	}
	opts := []remote.Option{remote.WithContext(ctx), remote.WithTransport(httpTransport(dial))}
	repos, err := remote.Catalog(ctx, reg, opts...)
	if err != nil {
		return fmt.Errorf("listing the cluster's registry: %w", err)
	}
	for _, r := range repos {
		if r != app && !strings.HasPrefix(r, app+"/") {
			continue
		}
		if err := removeRepo(reg.Repo(r), opts); err != nil {
			return err
		}
	}
	return nil
}

// removeRepo deletes every image of repo.
func removeRepo(repo name.Repository, opts []remote.Option) error {
	tags, err := remote.List(repo, opts...)
	var terr *transport.Error
	if errors.As(err, &terr) && terr.StatusCode == http.StatusNotFound {
		return nil
	}
	if err != nil {
		return fmt.Errorf("listing %s's images in the cluster's registry: %w", repo.RepositoryStr(), err)
	}
	for _, t := range tags {
		err := remote.Delete(repo.Tag(t), opts...)
		if errors.As(err, &terr) && (terr.StatusCode == http.StatusMethodNotAllowed || terr.StatusCode == http.StatusBadRequest) {
			var desc *v1.Descriptor
			if desc, err = remote.Head(repo.Tag(t), opts...); err == nil {
				err = remote.Delete(repo.Digest(desc.Digest.String()), opts...)
			}
		}
		if err != nil {
			return fmt.Errorf("deleting %s:%s from the cluster's registry: %w", repo.RepositoryStr(), t, err)
		}
	}
	return nil
}

// readLayout is the image for arch in the OCI image layout at path, a
// directory, or an archive, which it extracts beside itself.
func readLayout(path, arch string) (v1.Image, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	dir := path
	if !info.IsDir() {
		if dir, err = os.MkdirTemp(filepath.Dir(path), "layout-"); err != nil {
			return nil, err
		}
		if err := untar(path, dir); err != nil {
			return nil, err
		}
	}
	idx, err := layout.ImageIndexFromPath(dir)
	if err != nil {
		return nil, err
	}
	return findImage(idx, arch)
}

// findImage is the image in idx, or in an index it holds, for arch, or its
// only image.
func findImage(idx v1.ImageIndex, arch string) (v1.Image, error) {
	m, err := idx.IndexManifest()
	if err != nil {
		return nil, err
	}
	var images []v1.Hash
	for _, d := range m.Manifests {
		switch {
		case d.MediaType.IsIndex():
			child, err := idx.ImageIndex(d.Digest)
			if err != nil {
				return nil, err
			}
			if img, err := findImage(child, arch); err == nil {
				return img, nil
			}
		case d.MediaType.IsImage():
			if d.Platform != nil && d.Platform.Architecture == arch {
				return idx.Image(d.Digest)
			}
			if d.Platform == nil || d.Platform.Architecture != "unknown" {
				images = append(images, d.Digest)
			}
		}
	}
	if len(images) == 1 {
		return idx.Image(images[0])
	}
	return nil, errors.New("the layout has no image for it")
}

// untar extracts the tar at path into dir.
func untar(path, dir string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	tr := tar.NewReader(f)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		clean := filepath.Clean(filepath.FromSlash(h.Name))
		if filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
			return fmt.Errorf("the archive has a file outside it, %s", h.Name)
		}
		target := filepath.Join(dir, clean)
		switch h.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o755); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			out, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
			if err != nil {
				return err
			}
			_, err = io.Copy(out, tr)
			if cerr := out.Close(); err == nil {
				err = cerr
			}
			if err != nil {
				return err
			}
		}
	}
}

// stallConn is a connection that closes itself once a read or write has
// made no progress for its timeout.
type stallConn struct {
	net.Conn
	timeout time.Duration
	last    atomic.Int64
	pending atomic.Int32
	stop    chan struct{}
	once    sync.Once
}

// watch is c, closed once a read or write on it stalls for timeout.
func watch(c net.Conn, timeout time.Duration) net.Conn {
	s := &stallConn{Conn: c, timeout: timeout, stop: make(chan struct{})}
	s.last.Store(time.Now().UnixNano())
	go func() {
		t := time.NewTicker(timeout / 4)
		defer t.Stop()
		for {
			select {
			case <-s.stop:
				return
			case <-t.C:
				if s.pending.Load() > 0 && time.Since(time.Unix(0, s.last.Load())) > timeout {
					s.Close()
					return
				}
			}
		}
	}()
	return s
}

func (s *stallConn) Read(p []byte) (int, error) {
	s.pending.Add(1)
	defer s.pending.Add(-1)
	n, err := s.Conn.Read(p)
	if n > 0 {
		s.last.Store(time.Now().UnixNano())
	}
	return n, err
}

func (s *stallConn) Write(p []byte) (int, error) {
	s.pending.Add(1)
	defer s.pending.Add(-1)
	s.last.Store(time.Now().UnixNano())
	n, err := s.Conn.Write(p)
	if n > 0 {
		s.last.Store(time.Now().UnixNano())
	}
	return n, err
}

func (s *stallConn) Close() error {
	s.once.Do(func() { close(s.stop) })
	return s.Conn.Close()
}
