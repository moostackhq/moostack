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
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/containerd/platforms"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/moostackhq/moostack/internal/ociimage"
)

// Whatever archive it is sent, the platform check ends, and either takes
// it, refuses it with FAILED_PRECONDITION for its platform, or fails
// without a code, as an archive it can't read.
func FuzzCheckPlatforms(f *testing.F) {
	for _, arch := range []string{"amd64", "arm64"} {
		var b bytes.Buffer
		if err := ociimage.Write(&b, ociimage.Image{Name: "example.test/web:1", Architecture: arch, Entrypoint: []string{"/web"}, Files: []ociimage.File{{Name: "web", Mode: 0o755, Data: []byte("web")}}}); err != nil {
			f.Fatal(err)
		}
		f.Add(b.Bytes())
	}
	f.Add([]byte("not an archive"))
	node := platforms.Only(platforms.MustParse("linux/amd64"))
	f.Fuzz(func(t *testing.T, data []byte) {
		err := checkPlatforms(context.Background(), bytes.NewReader(data), node)
		if s, ok := status.FromError(err); err != nil && ok && s.Code() != codes.FailedPrecondition {
			t.Fatalf("checkPlatforms = %v, a code other than FAILED_PRECONDITION", err)
		}
	})
}

// costlyArchive is an archive of one image for linux/amd64 whose index
// lists it m times, padded with e empty files ahead of it.
func costlyArchive(t *testing.T, m, e int) []byte {
	t.Helper()
	var b bytes.Buffer
	if err := ociimage.Write(&b, ociimage.Image{Name: "example.test/web:1", Architecture: "amd64", Entrypoint: []string{"/web"}}); err != nil {
		t.Fatal(err)
	}
	r := tar.NewReader(&b)
	files := map[string][]byte{}
	var names []string
	for {
		h, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		data, _ := io.ReadAll(r)
		files[h.Name] = data
		names = append(names, h.Name)
	}
	var idx map[string]any
	if err := json.Unmarshal(files["index.json"], &idx); err != nil {
		t.Fatal(err)
	}
	one := idx["manifests"].([]any)[0]
	manifests := make([]any, m)
	for i := range manifests {
		manifests[i] = one
	}
	idx["manifests"] = manifests
	index, err := json.Marshal(idx)
	if err != nil {
		t.Fatal(err)
	}
	files["index.json"] = index
	var out bytes.Buffer
	w := tar.NewWriter(&out)
	write := func(name string, data []byte) {
		if err := w.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(data)), Typeflag: tar.TypeReg}); err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write(data); err != nil {
			t.Fatal(err)
		}
	}
	for i := range e {
		write(fmt.Sprintf("pad/%d", i), nil)
	}
	for _, n := range names {
		write(n, files[n])
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

// The check reads an archive's headers once, whatever its index lists,
// stops once its call ends, and refuses an archive of too many entries,
// so that no archive holds the node's changes for long.
func TestThePlatformCheckIsBoundedAndStops(t *testing.T) {
	node := platforms.Only(platforms.MustParse("linux/amd64"))
	costly := costlyArchive(t, 1000, 10_000)
	start := time.Now()
	if err := checkPlatforms(context.Background(), bytes.NewReader(costly), node); err != nil {
		t.Fatalf("checkPlatforms of an index of 1000 entries among 10000 files: %v", err)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Errorf("checkPlatforms of an index of 1000 entries among 10000 files took %s", took)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := checkPlatforms(ctx, bytes.NewReader(costly), node); !errors.Is(err, context.Canceled) {
		t.Errorf("checkPlatforms once its call ended = %v, want it stopped", err)
	}
	if err := checkPlatforms(context.Background(), bytes.NewReader(costlyArchive(t, 1, maxEntries+1)), node); err == nil || !strings.Contains(err.Error(), "more than") {
		t.Errorf("checkPlatforms of an archive of more than %d entries = %v", maxEntries, err)
	}
}

// endlessHeaders reads an endless chain of extended headers, as one
// entry that never ends.
type endlessHeaders struct {
	pair []byte
	at   int64
}

func newEndlessHeaders() *endlessHeaders {
	data := []byte("20 comment=xxxxxxxx\n")
	h := make([]byte, 512)
	copy(h, "pax")
	copy(h[100:], "0000644\x00")
	copy(h[124:], fmt.Sprintf("%011o\x00", len(data)))
	copy(h[136:], "00000000000\x00")
	h[156] = 'x'
	copy(h[257:], "ustar\x0000")
	copy(h[148:], "        ")
	var sum int
	for _, c := range h {
		sum += int(c)
	}
	copy(h[148:], fmt.Sprintf("%06o\x00 ", sum))
	body := make([]byte, 512)
	copy(body, data)
	return &endlessHeaders{pair: append(h, body...)}
}

func (e *endlessHeaders) Read(p []byte) (int, error) {
	n := 0
	for n < len(p) {
		c := copy(p[n:], e.pair[e.at%int64(len(e.pair)):])
		n += c
		e.at += int64(c)
	}
	return n, nil
}

func (e *endlessHeaders) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		e.at = offset
	case io.SeekCurrent:
		e.at += offset
	default:
		return 0, errors.New("an endless archive has no end")
	}
	return e.at, nil
}

// An entry read through extended headers that never end stops once the
// check's call ends, as entries do.
func TestThePlatformCheckStopsWithinAnEntry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- checkPlatforms(ctx, newEndlessHeaders(), platforms.Only(platforms.MustParse("linux/amd64")))
	}()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("checkPlatforms of endless extended headers = %v, want it stopped", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("checkPlatforms of endless extended headers didn't stop once its call ended")
	}
}

// countingReader counts the bytes read through it.
type countingReader struct {
	io.ReadSeeker
	read int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.ReadSeeker.Read(p)
	c.read += int64(n)
	return n, err
}

// The check seeks past the contents of the archive's files, reading its
// headers and its metadata alone, however large its layers are.
func TestThePlatformCheckSeeksPastTheLayers(t *testing.T) {
	// Bytes that don't compress, so that the layer is as large.
	large := make([]byte, 64<<20)
	rand.NewChaCha8([32]byte{}).Read(large)
	var b bytes.Buffer
	if err := ociimage.Write(&b, ociimage.Image{Name: "example.test/web:1", Architecture: "amd64", Entrypoint: []string{"/web"},
		Files: []ociimage.File{{Name: "large", Mode: 0o644, Data: large}}}); err != nil {
		t.Fatal(err)
	}
	r := &countingReader{ReadSeeker: bytes.NewReader(b.Bytes())}
	if err := checkPlatforms(context.Background(), r, platforms.Only(platforms.MustParse("linux/amd64"))); err != nil {
		t.Fatal(err)
	}
	if r.read > 1<<20 {
		t.Errorf("checkPlatforms read %d bytes of an archive of %d, want its headers and metadata alone", r.read, b.Len())
	}
}

// cancelAtRead is a reader that ends its call as the check first seeks
// to a file's content, once it walked the archive's headers.
type cancelAtRead struct {
	io.ReadSeeker
	cancel func()
}

func (c cancelAtRead) Seek(offset int64, whence int) (int64, error) {
	if whence == io.SeekStart && offset > 0 {
		c.cancel()
	}
	return c.ReadSeeker.Seek(offset, whence)
}

// A call that ends once the archive's headers were walked stops the
// check before it reads the archive's metadata, or goes on through the
// images its index lists.
func TestThePlatformCheckStopsWhileItReadsTheImages(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := cancelAtRead{ReadSeeker: bytes.NewReader(costlyArchive(t, 1000, 0)), cancel: cancel}
	err := checkPlatforms(ctx, r, platforms.Only(platforms.MustParse("linux/amd64")))
	if !errors.Is(err, context.Canceled) {
		t.Errorf("checkPlatforms once its call ended while it read the images = %v, want it stopped", err)
	}
}
