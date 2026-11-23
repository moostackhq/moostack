package registry_test

import (
	"archive/tar"
	"context"
	"io/fs"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/go-containerregistry/pkg/name"
	ggcrregistry "github.com/google/go-containerregistry/pkg/registry"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/google/go-containerregistry/pkg/v1/random"
	"github.com/google/go-containerregistry/pkg/v1/remote"

	"github.com/moostackhq/moostack/internal/registry"
)

// archive writes an OCI image layout archive of a random image, as
// buildx writes one, and returns its path and the image's digest.
func archive(t *testing.T, dir, arch string) (string, string) {
	t.Helper()
	img, err := random.Image(256, 1)
	if err != nil {
		t.Fatal(err)
	}
	lay := filepath.Join(dir, arch+"-layout")
	p, err := layout.Write(lay, empty.Index)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.AppendImage(img); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, arch+".tar")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	tw := tar.NewWriter(f)
	err = filepath.WalkDir(lay, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(lay, p)
		data, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		if err := tw.WriteHeader(&tar.Header{Name: filepath.ToSlash(rel), Mode: 0o644, Size: int64(len(data)), Typeflag: tar.TypeReg}); err != nil {
			return err
		}
		_, err = tw.Write(data)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	d, _ := img.Digest()
	return path, d.String()
}

func TestPushMakesOneIndexOfEveryArchitecture(t *testing.T) {
	server := httptest.NewServer(ggcrregistry.New())
	defer server.Close()
	dir := t.TempDir()
	amd, amdDigest := archive(t, dir, "amd64")
	arm, armDigest := archive(t, dir, "arm64")

	// The registry is named as nodes see it; the dial reaches it.
	dial := func(ctx context.Context, addr string) (net.Conn, error) {
		if addr != "10.0.0.2:5000" {
			t.Errorf("dialed %s", addr)
		}
		return (&net.Dialer{}).DialContext(ctx, "tcp", server.Listener.Addr().String())
	}
	tag, err := registry.Push(t.Context(), map[string]string{"amd64": amd, "arm64": arm}, "10.0.0.2:5000", "web", dial, nil)
	if err != nil {
		t.Fatal(err)
	}

	ref, err := name.NewTag(server.Listener.Addr().String()+"/web:"+tag, name.Insecure)
	if err != nil {
		t.Fatal(err)
	}
	idx, err := remote.Index(ref)
	if err != nil {
		t.Fatal(err)
	}
	if d, _ := idx.Digest(); d.Hex != tag {
		t.Errorf("tag %s isn't the index's digest %s", tag, d.Hex)
	}
	m, err := idx.IndexManifest()
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, d := range m.Manifests {
		got[d.Platform.OS+"/"+d.Platform.Architecture] = d.Digest.String()
	}
	if got["linux/amd64"] != amdDigest || got["linux/arm64"] != armDigest || len(got) != 2 {
		t.Errorf("index = %v", got)
	}
}

func TestRemoveDeletesEveryImageOfTheApp(t *testing.T) {
	server := httptest.NewServer(ggcrregistry.New())
	defer server.Close()
	dir := t.TempDir()
	amd, _ := archive(t, dir, "amd64")
	dial := func(ctx context.Context, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", server.Listener.Addr().String())
	}
	host := server.Listener.Addr().String()
	if _, err := registry.Push(t.Context(), map[string]string{"amd64": amd}, host, "web", dial, nil); err != nil {
		t.Fatal(err)
	}
	other, _ := archive(t, t.TempDir(), "amd64")
	if _, err := registry.Push(t.Context(), map[string]string{"amd64": other}, host, "other", dial, nil); err != nil {
		t.Fatal(err)
	}
	if err := registry.Remove(t.Context(), host, "web", dial); err != nil {
		t.Fatal(err)
	}
	repo, _ := name.NewRepository(host+"/web", name.Insecure)
	if tags, _ := remote.List(repo); len(tags) > 0 {
		for _, tag := range tags {
			if _, err := remote.Head(repo.Tag(tag)); err == nil {
				t.Errorf("web:%s is still there", tag)
			}
		}
	}
	otherRepo, _ := name.NewRepository(host+"/other", name.Insecure)
	if tags, err := remote.List(otherRepo); err != nil || len(tags) != 1 {
		t.Errorf("another app's images: %v, %v", tags, err)
	}
	if err := registry.Remove(t.Context(), host, "never", dial); err != nil {
		t.Errorf("an app with no images: %v", err)
	}
}

func TestPushToARegistryThatNeverAnswersEnds(t *testing.T) {
	defer registry.SetTimeouts(time.Second, time.Second)()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			defer c.Close()
		}
	}()
	amd, _ := archive(t, t.TempDir(), "amd64")
	dial := func(ctx context.Context, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", l.Addr().String())
	}
	done := make(chan error, 1)
	go func() {
		_, err := registry.Push(context.Background(), map[string]string{"amd64": amd}, "10.0.0.2:5000", "web", dial, nil)
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "10.0.0.2:5000") {
			t.Errorf("Push = %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Push to a silent registry didn't end")
	}
}

func TestPushReportsItsProgressUpToTheWholePush(t *testing.T) {
	server := httptest.NewServer(ggcrregistry.New())
	defer server.Close()
	dir := t.TempDir()
	amd, _ := archive(t, dir, "amd64")
	arm, _ := archive(t, dir, "arm64")
	dial := func(ctx context.Context, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", server.Listener.Addr().String())
	}
	host := server.Listener.Addr().String()
	push := func(archives map[string]string) {
		t.Helper()
		var complete, total int64
		calls := 0
		// Push has made every call by the time it returns.
		tag, err := registry.Push(t.Context(), archives, host, "web", dial, func(c, tot int64) {
			if c < complete {
				t.Errorf("progress went back from %d to %d", complete, c)
			}
			if calls > 0 && tot != total {
				t.Errorf("the whole push went from %d to %d bytes", total, tot)
			}
			complete, total, calls = c, tot, calls+1
		})
		if err != nil {
			t.Fatal(err)
		}
		ref, _ := name.NewTag(host+"/web:"+tag, name.Insecure)
		idx, err := remote.Index(ref)
		if err != nil {
			t.Fatal(err)
		}
		want, _ := idx.Size()
		m, _ := idx.IndexManifest()
		for _, d := range m.Manifests {
			img, err := idx.Image(d.Digest)
			if err != nil {
				t.Fatal(err)
			}
			want += d.Size
			im, _ := img.Manifest()
			want += im.Config.Size
			for _, l := range im.Layers {
				want += l.Size
			}
		}
		if total != want {
			t.Errorf("the whole push is %d bytes, not the %d of the index, its manifests, configs and layers", total, want)
		}
		if complete != total {
			t.Errorf("the push ended at %d of %d bytes", complete, total)
		}
	}
	both := map[string]string{"amd64": amd, "arm64": arm}
	push(both)
	// One image the registry has already, and then both.
	other, _ := archive(t, t.TempDir(), "arm64")
	push(map[string]string{"amd64": amd, "arm64": other})
	push(both)
}

func TestPushReadsALayoutsDirectoryAsItsArchive(t *testing.T) {
	server := httptest.NewServer(ggcrregistry.New())
	defer server.Close()
	dir := t.TempDir()
	tarred, digest := archive(t, dir, "amd64")
	dial := func(ctx context.Context, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", server.Listener.Addr().String())
	}
	host := server.Listener.Addr().String()
	fromArchive, err := registry.Push(t.Context(), map[string]string{"amd64": tarred}, host, "tarred", dial, nil)
	if err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	fromDir, err := registry.Push(t.Context(), map[string]string{"amd64": filepath.Join(dir, "amd64-layout")}, host, "web", dial, nil)
	if err != nil {
		t.Fatal(err)
	}
	if fromDir != fromArchive {
		t.Errorf("the directory's index is %s, its archive's %s", fromDir, fromArchive)
	}
	ref, _ := name.NewTag(host+"/web:"+fromDir, name.Insecure)
	idx, err := remote.Index(ref)
	if err != nil {
		t.Fatal(err)
	}
	if m, err := idx.IndexManifest(); err != nil || len(m.Manifests) != 1 || m.Manifests[0].Digest.String() != digest {
		t.Errorf("the index lists %+v, %v, want the image %s", m, err, digest)
	}
	// A directory is read where it is, with nothing extracted beside it.
	if after, _ := os.ReadDir(dir); len(after) != len(before) {
		t.Errorf("the push of a directory left %d entries beside it, of %d before", len(after), len(before))
	}
}
