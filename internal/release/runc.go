package release

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
)

// RuncVersion is the runc that releases pin, the one containerd's
// release was tested with. containerd's shim runs each container with
// it. It is a component of its own, whose one file is runc.
const RuncVersion = "1.5.1"

// runcSHA256 are the hex SHA-256 sums of runc's static binaries, by
// architecture, as runc published them.
var runcSHA256 = map[string]string{
	"amd64": "177df879d50c913eb205e898d5c1c05a18f574053c0ce5524c471208eaf06f6f",
	"arm64": "ca70e7dbd6616ca782a59b5d3ac86909123fdaa9fa3f89dcf29051c70eee7ce9",
}

// FetchRunc writes runc for arch into dir. It downloads runc's static
// binary once into cacheDir, and checks it against its pinned checksum
// every time.
func FetchRunc(ctx context.Context, arch, cacheDir, dir string) error {
	sum, ok := runcSHA256[arch]
	if !ok {
		return fmt.Errorf("no runc for %s", arch)
	}
	cached := filepath.Join(cacheDir, fmt.Sprintf("runc-%s.%s", RuncVersion, arch))
	if err := checkSum(cached, sum); err != nil {
		url := fmt.Sprintf("https://github.com/opencontainers/runc/releases/download/v%s/runc.%s", RuncVersion, arch)
		if err := download(ctx, http.DefaultClient, url, cached); err != nil {
			return err
		}
		if err := checkSum(cached, sum); err != nil {
			return err
		}
	}
	data, err := os.ReadFile(cached)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "runc"), data, 0o755)
}
