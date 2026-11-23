package registry_test

import (
	"archive/tar"
	"bytes"
	"context"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	ggcrregistry "github.com/google/go-containerregistry/pkg/registry"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/google/go-containerregistry/pkg/v1/random"

	"github.com/moostackhq/moostack/internal/registry"
)

func bigArchive(t *testing.T, size int64) string {
	t.Helper()
	img, err := random.Image(size, 1)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	lay := filepath.Join(dir, "layout")
	p, err := layout.Write(lay, empty.Index)
	if err == nil {
		err = p.AppendImage(img)
	}
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "amd64.tar")
	f, _ := os.Create(path)
	defer f.Close()
	tw := tar.NewWriter(f)
	filepath.WalkDir(lay, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(lay, p)
		data, _ := os.ReadFile(p)
		tw.WriteHeader(&tar.Header{Name: filepath.ToSlash(rel), Mode: 0o644, Size: int64(len(data)), Typeflag: tar.TypeReg})
		tw.Write(data)
		return nil
	})
	tw.Close()
	return path
}

// A push to a registry that stops reading a blob's upload partway ends.
func TestAPushThatStallsMidUploadEnds(t *testing.T) {
	defer registry.SetTimeouts(time.Second, time.Second)()
	reg := ggcrregistry.New()
	block := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPatch || r.Method == http.MethodPut {
			head, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
			if len(head) == 1<<20 {
				<-block
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(head))
			r.ContentLength = int64(len(head))
		}
		reg.ServeHTTP(w, r)
	}))
	defer server.Close()
	defer close(block)
	server.Config.SetKeepAlivesEnabled(false)
	amd := bigArchive(t, 64<<20)
	dial := func(ctx context.Context, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", server.Listener.Addr().String())
	}
	done := make(chan error, 1)
	go func() {
		_, err := registry.Push(context.Background(), map[string]string{"amd64": amd}, server.Listener.Addr().String(), "web", dial, nil)
		done <- err
	}()
	select {
	case err := <-done:
		t.Logf("Push ended: %v", err)
	case <-time.After(20 * time.Second):
		t.Fatal("Push to a registry that stopped reading the upload didn't end in 20 s (timeouts 1 s)")
	}
}
