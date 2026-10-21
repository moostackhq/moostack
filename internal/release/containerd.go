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
	"os"
	"path"
	"path/filepath"
	"slices"

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
		if err := download(ctx, url, tarball); err != nil {
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

// download writes what url serves to path.
func download(ctx context.Context, url, path string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: %s", url, resp.Status)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	_, err = io.Copy(tmp, resp.Body)
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return fmt.Errorf("GET %s: %w", url, err)
	}
	return os.Rename(tmp.Name(), path)
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
