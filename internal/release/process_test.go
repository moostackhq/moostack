package release_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/moostackhq/moostack/internal/release"
)

// downloadEnv, when set, makes the test binary a process that downloads
// release 1.0.0 into the cache it names, from the source in
// downloadSourceEnv, trusting the certificate in downloadCAEnv, and
// exits, as a mooctl of its own would.
const (
	downloadEnv       = "MOOSTACK_TEST_DOWNLOAD_CACHE"
	downloadSourceEnv = "MOOSTACK_TEST_DOWNLOAD_SOURCE"
	downloadCAEnv     = "MOOSTACK_TEST_DOWNLOAD_CA"
)

func TestMain(m *testing.M) {
	if cache := os.Getenv(downloadEnv); cache != "" {
		if err := downloadAsAProcess(cache); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func downloadAsAProcess(cache string) error {
	data, err := os.ReadFile(os.Getenv(downloadCAEnv))
	if err != nil {
		return err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(data) {
		return fmt.Errorf("no certificate in %s", os.Getenv(downloadCAEnv))
	}
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots}}}
	r, err := release.Download(context.Background(), client, os.Getenv(downloadSourceEnv), cache, "1.0.0", nil)
	if err != nil {
		return err
	}
	_, err = r.ReadFile("arm64", "shim")
	return err
}

func TestMooctlsDownloadingOneReleaseAtOnceTakeTurns(t *testing.T) {
	s := serve(t, writeRelease(t))
	ca := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: s.Certificate().Raw}), 0o644); err != nil {
		t.Fatal(err)
	}
	cache := t.TempDir()
	var cmds []*exec.Cmd
	for range 4 {
		cmd := exec.Command(os.Args[0])
		cmd.Env = append(os.Environ(), downloadEnv+"="+cache, downloadSourceEnv+"="+s.URL, downloadCAEnv+"="+ca)
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		cmds = append(cmds, cmd)
	}
	for i, cmd := range cmds {
		if err := cmd.Wait(); err != nil {
			t.Errorf("download %d: %v", i, err)
		}
	}
	if left := cached(t, cache); len(left) != 1 || filepath.Base(left[0]) != "1.0.0" {
		t.Errorf("the cache holds %v, want release 1.0.0 only", left)
	}
	for _, p := range []string{"/1.0.0/manifest.yml", "/1.0.0/linux-arm64/shim"} {
		if n := s.count(p); n != 1 {
			t.Errorf("%s was downloaded %d times by the four processes, want once", p, n)
		}
	}
}
