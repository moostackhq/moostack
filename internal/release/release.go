// Package release fetches the files of a Moostack release.
package release

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

// URL is where the files of the release built from main are.
const URL = "https://github.com/moostackhq/moostack/releases/download/edge"

// Fetch downloads the file name of the release at url into dir, checks
// it against the release's sha256sums.txt, and returns its path. A file
// that doesn't match is removed.
func Fetch(ctx context.Context, client *http.Client, url, name, dir string) (string, error) {
	sums, err := get(ctx, client, url+"/sha256sums.txt")
	if err != nil {
		return "", err
	}
	defer sums.Close()
	listed, err := io.ReadAll(io.LimitReader(sums, 1<<20))
	if err != nil {
		return "", fmt.Errorf("downloading %s/sha256sums.txt: %w", url, err)
	}
	want := ""
	for line := range strings.Lines(string(listed)) {
		if sum, file, ok := strings.Cut(strings.TrimSpace(line), "  "); ok && file == name {
			want = sum
		}
	}
	if want == "" {
		return "", fmt.Errorf("%s/sha256sums.txt doesn't list %s", url, name)
	}

	content, err := get(ctx, client, url+"/"+name)
	if err != nil {
		return "", err
	}
	defer content.Close()
	path := filepath.Join(dir, name)
	file, err := os.Create(path)
	if err != nil {
		return "", err
	}
	sum := sha256.New()
	_, err = io.Copy(io.MultiWriter(file, sum), content)
	if cerr := file.Close(); err == nil {
		err = cerr
	}
	if err == nil && hex.EncodeToString(sum.Sum(nil)) != want {
		err = fmt.Errorf("it doesn't match its checksum in sha256sums.txt")
	}
	if err != nil {
		os.Remove(path)
		return "", fmt.Errorf("downloading %s/%s: %w", url, name, err)
	}
	return path, nil
}

// get is the body of url.
func get(ctx context.Context, client *http.Client, url string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	res, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("downloading %s: %w", url, err)
	}
	if res.StatusCode != http.StatusOK {
		res.Body.Close()
		return nil, fmt.Errorf("downloading %s: %s", url, res.Status)
	}
	return res.Body, nil
}
