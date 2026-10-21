package release

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/moostackhq/moostack/internal/filelock"
)

// waitNotice is how long a download waits for another before it says so.
var waitNotice = time.Second

// Download opens the release with version in the release source at the
// https URL source, laid out as a release source directory is, after
// downloading it into cache, unless cache has it already, whole and
// matching its checksums. cache holds a release source directory for
// each source, so that two sources' builds of one version are kept
// apart. It checks every file against the release's checksums as it
// downloads it, and keeps nothing of a release with a file that doesn't
// match or can't be downloaded, so that cache holds only whole releases,
// each downloaded once. One download into cache runs at a time, and it
// removes what a download that was killed left. say, unless nil, is told
// when it downloads, and when it waits for another download a while.
func Download(ctx context.Context, client *http.Client, source, cache, version string, say func(format string, args ...any)) (*Release, error) {
	if say == nil {
		say = func(string, ...any) {}
	}
	if err := CheckVersion(version); err != nil {
		return nil, err
	}
	if !strings.HasPrefix(source, "https://") {
		return nil, fmt.Errorf("the release source %s is neither a directory nor an https URL", redacted(source))
	}
	key := sha256.Sum256([]byte(strings.TrimRight(source, "/")))
	cache = filepath.Join(cache, hex.EncodeToString(key[:8]))
	if err := os.MkdirAll(cache, 0o755); err != nil {
		return nil, err
	}
	// The lock is beside the cache, so that the cache holds releases
	// only.
	waiting := time.AfterFunc(waitNotice, func() { say("Waiting for another mooctl's download from %s", redacted(source)) })
	unlock, err := filelock.LockContext(ctx, filepath.Clean(cache)+".lock")
	waiting.Stop()
	if err != nil {
		return nil, err
	}
	defer unlock()
	// Holding the lock, no other download runs, so a download directory
	// is one that a killed mooctl left.
	leftovers, err := filepath.Glob(filepath.Join(cache, ".download-*"))
	if err != nil {
		return nil, err
	}
	for _, l := range leftovers {
		if err := os.RemoveAll(l); err != nil {
			return nil, err
		}
	}
	if r, err := Open(cache, version); err == nil && r.whole() == nil {
		return r, nil
	}
	// A release in the cache that isn't whole, as after a power loss
	// once it was moved there, is downloaded again.
	if err := os.RemoveAll(filepath.Join(cache, version)); err != nil {
		return nil, err
	}
	say("Downloading release %s from %s", version, redacted(source))
	// The release is downloaded beside the cache's releases, and moved
	// among them only once it is whole.
	tmp, err := os.MkdirTemp(cache, ".download-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmp)
	dir := filepath.Join(tmp, version)
	if client == nil {
		client = http.DefaultClient
	}
	// A redirect away from https would take the files, and the checksums
	// that check them, from where anyone on the path can change both.
	https := *client
	https.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if req.URL.Scheme != "https" {
			return fmt.Errorf("refusing a redirect to %s, which isn't https", req.URL.Redacted())
		}
		if client.CheckRedirect != nil {
			return client.CheckRedirect(req, via)
		}
		if len(via) >= 10 {
			return errors.New("stopped after 10 redirects")
		}
		return nil
	}
	get := func(p, sum string) error {
		u, err := url.JoinPath(source, version, p)
		if err != nil {
			return err
		}
		to := filepath.Join(dir, filepath.FromSlash(p))
		if err := download(ctx, &https, u, to); err != nil {
			return fmt.Errorf("release %s: %w", version, err)
		}
		if sum != "" && checkSum(to, sum) != nil {
			return fmt.Errorf("release %s: %s doesn't match its checksum", version, p)
		}
		return nil
	}
	for _, name := range []string{manifestName, sumsName} {
		if err := get(name, ""); err != nil {
			return nil, err
		}
	}
	r, err := Open(tmp, version)
	if err != nil {
		return nil, err
	}
	for _, f := range r.files() {
		for _, arch := range Architectures {
			p := filePath(arch, f.name)
			if err := get(p, r.sums[p]); err != nil {
				return nil, err
			}
		}
	}
	if err := os.Rename(dir, filepath.Join(cache, version)); err != nil {
		return nil, err
	}
	return Open(cache, version)
}

// whole fails unless every file of r, for every architecture, is there
// and matches its checksum.
func (r *Release) whole() error {
	for _, f := range r.files() {
		for _, arch := range Architectures {
			p := filePath(arch, f.name)
			if err := checkSum(filepath.Join(r.dir, filepath.FromSlash(p)), r.sums[p]); err != nil {
				return err
			}
		}
	}
	return nil
}

// redacted is the URL rawURL with any password in it replaced, for
// errors to show.
func redacted(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "a URL that doesn't parse"
	}
	return u.Redacted()
}
