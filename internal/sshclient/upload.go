package sshclient

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"strings"
	"sync"
)

// Upload writes r to file on the node with the permission bits of mode,
// owned by owner, which is user or user:group. It writes a temporary file
// next to file and renames it, so file is either as it was or complete.
// A symlink at file is replaced, not written through. A directory is
// refused, even one that appears during the upload, and so is a symlink to
// one, which BusyBox's mv refuses and GNU's would replace. file must be a
// path to a file that path.Clean leaves as it is, so .. may only lead it.
// A relative file is relative to the user's home directory. As for Run,
// no read of r starts once Upload returns, though one under way may
// finish.
func (c *Client) Upload(ctx context.Context, file string, r io.Reader, mode fs.FileMode, owner string) error {
	// The temporary file goes in path.Dir(file). On a path that isn't
	// clean, that drops a .. after a symlink without following the link,
	// as the shell does, so it could land on another filesystem, where mv
	// can't rename atomically. path.Clean turns an empty path into ".".
	if base := path.Base(file); path.Clean(file) != file || base == "." || base == ".." || base == "/" {
		return fmt.Errorf("uploading %q: not a clean path to a file", file)
	}
	target := notOption(file)
	content := &countingReader{r: r}
	t := newTrailer()
	script := fmt.Sprintf(`set -e
if [ -d %s ]; then
	echo "it is a directory" >&2
	exit 1
fi
tmp=$(mktemp %s)
trap 'rm -f "$tmp"' EXIT
cat > "$tmp"
size=$(($(wc -c < "$tmp") - %d))
if [ "$size" -lt 0 ] || [ "$(tail -c %d "$tmp")" != "$(printf %s "$size")" ]; then
	echo "the upload was cut short" >&2
	exit 1
fi
truncate -s "$size" "$tmp"
sync "$tmp"
chown -- %s "$tmp"
chmod %o "$tmp"
mv -fT "$tmp" %s
trap - EXIT`, shellQuote(target), shellQuote(notOption(path.Join(path.Dir(file), ".upload.XXXXXX"))), t.size(), t.size(),
		shellQuote(t.shellFormat()), shellQuote(owner), mode.Perm(), shellQuote(target))
	res, err := c.Run(ctx, script, io.MultiReader(content, &lazyReader{open: func() io.Reader {
		return strings.NewReader(t.after(content.n))
	}}))
	if failure := content.failure(); failure != nil {
		err = errors.Join(failure, err)
	}
	if err != nil {
		return fmt.Errorf("uploading %s: %w", file, err)
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("uploading %s: %s", file, bytes.TrimSpace(res.Stderr))
	}
	return nil
}

// notOption is the path p, written so that no command takes it for an
// option: a relative path starts with ./.
func notOption(p string) string {
	if path.IsAbs(p) {
		return p
	}
	return "./" + p
}

// trailer follows an upload's content, with the content's length. Only a
// reader that ends without an error gets one, so the node can tell a
// complete upload from one cut short: the session ends the remote input
// the same way either way. Each upload's trailer holds a random nonce, so
// that no content can end in it.
type trailer struct {
	nonce string
}

// newTrailer makes the trailer for one upload, with a new nonce.
func newTrailer() trailer { return trailer{nonce: rand.Text()} }

// after is the trailer that follows n bytes of content. The nonce holds
// no %, so shellFormat is a Go format too.
func (t trailer) after(n int64) string { return fmt.Sprintf(t.shellFormat(), n) + "\n" }

// size is the trailer's length in bytes, whatever the content's length.
func (t trailer) size() int { return len(t.after(0)) }

// shellFormat is a format for the shell's printf that prints the trailer
// for the length it is given, less the final newline, as $(...) strips it.
func (t trailer) shellFormat() string { return "\nupload complete " + t.nonce + " %020d" }

// countingReader counts the bytes read from r, and keeps the error that
// ended it, other than io.EOF. Its error can be read while a read is
// under way.
type countingReader struct {
	r  io.Reader
	n  int64
	mu sync.Mutex
	// err is guarded by mu.
	err error
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	if err != nil && err != io.EOF {
		c.mu.Lock()
		c.err = err
		c.mu.Unlock()
	}
	return n, err
}

// failure is the error that ended the reads, if any.
func (c *countingReader) failure() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

// lazyReader reads from the reader open returns, opened on the first read.
type lazyReader struct {
	open func() io.Reader
	r    io.Reader
}

func (l *lazyReader) Read(p []byte) (int, error) {
	if l.r == nil {
		l.r = l.open()
	}
	return l.r.Read(p)
}
