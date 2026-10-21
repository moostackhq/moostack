package release

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"slices"
	"time"

	"github.com/moostackhq/moostack/internal/hostlayout"
)

// ContainerdVersion is the containerd that releases pin.
const ContainerdVersion = "2.4.1"

// containerdSHA256 are the hex SHA-256 sums of containerd's static
// tarballs, by architecture, as containerd published them.
var containerdSHA256 = map[string]string{
	"amd64": "02ad3a7e80d7d2c018c7134d5eca7e301283db6895f100b957fb52ad8e5a30fd",
	"arm64": "d3aba9347650505df79858bb9cbf1be52080fd8145f72dd9308d4936f487fb74",
}

// ContainerdFiles are the binaries a node needs from containerd's
// tarball.
var ContainerdFiles = hostlayout.ContainerdFiles

// FetchContainerd writes ContainerdFiles for arch into dir. It downloads
// containerd's static tarball once into cacheDir, and checks it against
// its pinned checksum every time.
func FetchContainerd(ctx context.Context, arch, cacheDir, dir string) error {
	sum, ok := containerdSHA256[arch]
	if !ok {
		return fmt.Errorf("no containerd for %s", arch)
	}
	name := fmt.Sprintf("containerd-static-%s-linux-%s.tar.gz", ContainerdVersion, arch)
	tarball := filepath.Join(cacheDir, name)
	if err := checkSum(tarball, sum); err != nil {
		url := fmt.Sprintf("https://github.com/containerd/containerd/releases/download/v%s/%s", ContainerdVersion, name)
		if err := download(ctx, http.DefaultClient, url, tarball); err != nil {
			return err
		}
		if err := checkSum(tarball, sum); err != nil {
			return err
		}
	}
	return extract(tarball, dir, ContainerdFiles)
}

// checkSum checks that the file at path has the hex SHA-256 sum.
func checkSum(path, sum string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return err
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != sum {
		return fmt.Errorf("%s has SHA-256 %s, want %s", path, got, sum)
	}
	return nil
}

// stallTimeout is how long a download may go without data before it
// gives up, and maxFileSize how large a downloaded file may be, more than
// any file of a release.
var (
	stallTimeout       = 30 * time.Second
	maxFileSize  int64 = 4 << 30
)

// download writes what rawURL serves to path, getting it with client. It
// gives up once stallTimeout passes without data, or the file grows
// larger than maxFileSize.
func download(ctx context.Context, client *http.Client, rawURL, path string) error {
	stalled := fmt.Errorf("no data for %s", stallTimeout)
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	timer := time.AfterFunc(stallTimeout, func() { cancel(stalled) })
	defer timer.Stop()
	// why is the error a download failed with: the stall, if it was.
	why := func(err error) error {
		if context.Cause(ctx) == stalled {
			return stalled
		}
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		// The error names the URL it failed at, which may be a
		// redirect's, whole.
		var urlErr *url.Error
		if errors.As(err, &urlErr) {
			urlErr.URL = redacted(urlErr.URL)
		}
		return fmt.Errorf("GET %s: %w", redacted(rawURL), why(err))
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: %s", redacted(rawURL), resp.Status)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	body := &progress{r: io.LimitReader(resp.Body, maxFileSize+1), timer: timer}
	_, err = io.Copy(tmp, body)
	if err == nil && body.n > maxFileSize {
		err = fmt.Errorf("larger than %d bytes", maxFileSize)
	}
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return fmt.Errorf("GET %s: %w", redacted(rawURL), why(err))
	}
	return os.Rename(tmp.Name(), path)
}

// progress reads r, counting the bytes in n, and restarts timer, the
// stall timer, whenever data arrives.
type progress struct {
	r     io.Reader
	n     int64
	timer *time.Timer
}

func (p *progress) Read(b []byte) (int, error) {
	n, err := p.r.Read(b)
	if n > 0 {
		p.n += int64(n)
		p.timer.Reset(stallTimeout)
	}
	return n, err
}

// extract writes the regular files of the tarball whose base names are
// in names into dir, executable.
func extract(tarball, dir string, names []string) error {
	f, err := os.Open(tarball)
	if err != nil {
		return err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	found := map[string]bool{}
	r := tar.NewReader(gz)
	for {
		h, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("%s: %w", tarball, err)
		}
		name := path.Base(h.Name)
		if h.Typeflag != tar.TypeReg || !slices.Contains(names, name) {
			continue
		}
		if err := writeFile(filepath.Join(dir, name), r, 0o755); err != nil {
			return err
		}
		found[name] = true
	}
	for _, name := range names {
		if !found[name] {
			return fmt.Errorf("%s has no %s", tarball, name)
		}
	}
	return nil
}

func writeFile(path string, r io.Reader, mode os.FileMode) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	_, err = io.Copy(f, r)
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	return err
}
