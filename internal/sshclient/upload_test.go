package sshclient_test

import (
	"errors"
	"io"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"time"

	"github.com/moostackhq/moostack/internal/sshclient"
)

func TestUpload(t *testing.T) {
	d, h := startNode(t, "op")
	ctx := testContext(t)
	c, err := sshclient.Dial(ctx, d.node(), h.options())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	d.exec(t, "sh", "-c", "mkdir /srv/up && echo old > '/srv/up/it'\\''s.conf'")
	path := "/srv/up/it's.conf"

	if err := c.Upload(ctx, path, strings.NewReader("new\n"), 0o640, "nosudo:op"); err != nil {
		t.Fatal(err)
	}
	if got, want := d.exec(t, "stat", "-c", "%a %U:%G", path), "640 nosudo:op\n"; got != want {
		t.Errorf("mode and owner = %q, want %q", got, want)
	}
	if got := d.exec(t, "cat", path); got != "new\n" {
		t.Errorf("content = %q, want %q", got, "new\n")
	}

	t.Run("a failed upload leaves the file as it was", func(t *testing.T) {
		if err := c.Upload(ctx, path, strings.NewReader("newer\n"), 0o600, "nobody-here"); err == nil {
			t.Fatal("Upload with an unknown owner succeeded")
		}
		if got := d.exec(t, "cat", path); got != "new\n" {
			t.Errorf("content = %q, want %q", got, "new\n")
		}
		if got := d.exec(t, "ls", "-A", "/srv/up"); got != "it's.conf\n" {
			t.Errorf("the directory holds %q, want only the file", got)
		}
	})

	t.Run("a reader that fails partway leaves the file as it was", func(t *testing.T) {
		// Longer than the trailer that marks a complete upload.
		partial := strings.Repeat("partial ", 32)
		diskGone := errors.New("disk gone")
		r := io.MultiReader(strings.NewReader(partial), iotest.ErrReader(diskGone))
		if err := c.Upload(ctx, path, r, 0o600, "root"); !errors.Is(err, diskGone) {
			t.Fatalf("err = %v, want the reader's error", err)
		}
		if got := d.exec(t, "cat", path); got != "new\n" {
			t.Errorf("content = %q, want %q", got, "new\n")
		}
		if got := d.exec(t, "ls", "-A", "/srv/up"); got != "it's.conf\n" {
			t.Errorf("the directory holds %q, want only the file", got)
		}
	})

	t.Run("no read of the reader starts once Upload returns", func(t *testing.T) {
		// mktemp fails, so the node never reads the input.
		r := &endlessReader{}
		if err := c.Upload(ctx, "/nonexistent/file", r, 0o600, "root"); err == nil {
			t.Fatal("Upload into a missing directory succeeded")
		}
		n := r.reads.Load()
		time.Sleep(200 * time.Millisecond)
		if got := r.reads.Load(); got != n {
			t.Errorf("%d more reads of the reader started after Upload returned", got-n)
		}
	})

	t.Run("a relative path may start with -", func(t *testing.T) {
		if err := c.Upload(ctx, "-config", strings.NewReader("dash\n"), 0o600, "root"); err != nil {
			t.Fatal(err)
		}
		if got := d.exec(t, "cat", "/home/op/-config"); got != "dash\n" {
			t.Errorf("content = %q, want %q", got, "dash\n")
		}
	})

	t.Run("a relative directory may start with -", func(t *testing.T) {
		d.exec(t, "mkdir", "/home/op/-dir")
		if err := c.Upload(ctx, "-dir/-file", strings.NewReader("dash\n"), 0o600, "root"); err != nil {
			t.Fatal(err)
		}
		if got := d.exec(t, "cat", "/home/op/-dir/-file"); got != "dash\n" {
			t.Errorf("content = %q, want %q", got, "dash\n")
		}
	})

	t.Run("an empty path is refused", func(t *testing.T) {
		if err := c.Upload(ctx, "", strings.NewReader("x"), 0o600, "root"); err == nil {
			t.Error("Upload to an empty path succeeded")
		}
		if got := d.exec(t, "sh", "-c", "ls -A /home/op | grep upload || true"); got != "" {
			t.Errorf("the upload left %q behind", got)
		}
	})

	t.Run("a directory isn't replaced", func(t *testing.T) {
		if err := c.Upload(ctx, "/srv/up", strings.NewReader("x"), 0o600, "root"); err == nil {
			t.Error("Upload over a directory succeeded")
		}
		if got := d.exec(t, "sh", "-c", "ls -A /srv/up | grep upload || true"); got != "" {
			t.Errorf("the upload left %q in the directory", got)
		}
	})

	t.Run("a path that isn't clean is refused", func(t *testing.T) {
		// The shell resolves .. after following a link, so the temporary
		// file would be on another filesystem than the target.
		d.exec(t, "sh", "-c", "mkdir -p /mnt/sub && ln -s /mnt/sub /srv/up/link")
		if err := c.Upload(ctx, "/srv/up/link/../config", strings.NewReader("x"), 0o600, "root"); err == nil {
			t.Error("Upload to a path with .. succeeded")
		}
	})

	t.Run("a relative path may start with ..", func(t *testing.T) {
		if err := c.Upload(ctx, "../op/dotdot", strings.NewReader("up\n"), 0o600, "root"); err != nil {
			t.Fatal(err)
		}
		if got := d.exec(t, "cat", "/home/op/dotdot"); got != "up\n" {
			t.Errorf("content = %q, want %q", got, "up\n")
		}
	})

	t.Run("a symlink to a directory is refused", func(t *testing.T) {
		d.exec(t, "sh", "-c", "mkdir -p /mnt/dir && ln -s /mnt/dir /srv/up/dirlink")
		if err := c.Upload(ctx, "/srv/up/dirlink", strings.NewReader("file\n"), 0o600, "root"); err == nil {
			t.Fatal("Upload over a symlink to a directory succeeded")
		}
		if got := d.exec(t, "sh", "-c", "[ -L /srv/up/dirlink ] && echo link; ls -A /mnt/dir"); got != "link\n" {
			t.Errorf("got %q, want the link kept and the directory left empty", got)
		}
	})

	t.Run("a symlink to a file is replaced", func(t *testing.T) {
		d.exec(t, "sh", "-c", "echo old > /mnt/target && ln -s /mnt/target /srv/up/filelink")
		if err := c.Upload(ctx, "/srv/up/filelink", strings.NewReader("new\n"), 0o600, "root"); err != nil {
			t.Fatal(err)
		}
		if got := d.exec(t, "sh", "-c", "[ -L /srv/up/filelink ] && echo link; cat /srv/up/filelink /mnt/target"); got != "new\nold\n" {
			t.Errorf("got %q, want the link replaced and its target left as it was", got)
		}
	})

	t.Run("an empty file uploads", func(t *testing.T) {
		if err := c.Upload(ctx, "/srv/up/empty", strings.NewReader(""), 0o644, "root"); err != nil {
			t.Fatal(err)
		}
		if got := d.exec(t, "stat", "-c", "%s", "/srv/up/empty"); got != "0\n" {
			t.Errorf("size = %q, want 0", got)
		}
	})
}

// TestEachUploadHasItsOwnTrailer checks that the trailer marking a
// complete upload differs every time, so no content can end in it.
func TestEachUploadHasItsOwnTrailer(t *testing.T) {
	s, p, n := inProcessPool(t)
	ctx := testContext(t)
	c, err := p.Client(ctx, n)
	if err != nil {
		t.Fatal(err)
	}
	// The directory doesn't exist, so the uploads fail before writing.
	for range 2 {
		c.Upload(ctx, "/nonexistent/file", strings.NewReader("x"), 0o600, "root")
	}
	trailer := regexp.MustCompile(`upload complete (\S+) %020d`)
	var nonces []string
	for _, cmd := range s.ranCommands() {
		if m := trailer.FindStringSubmatch(cmd); m != nil {
			nonces = append(nonces, m[1])
		}
	}
	if len(nonces) != 2 || nonces[0] == nonces[1] || len(nonces[0]) < 16 {
		t.Errorf("the uploads' trailers hold %q, want two different long nonces", nonces)
	}
}

// endlessReader gives zeros forever, slowly, and counts the reads that
// have started.
type endlessReader struct {
	reads atomic.Int64
}

func (r *endlessReader) Read(p []byte) (int, error) {
	r.reads.Add(1)
	time.Sleep(50 * time.Millisecond)
	clear(p)
	return len(p), nil
}
