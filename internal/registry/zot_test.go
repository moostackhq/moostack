package registry_test

import (
	"net/netip"
	"os/exec"
	"regexp"
	"strings"
	"testing"

	"github.com/moostackhq/moostack/internal/registry"
)

func TestZotsScriptsParse(t *testing.T) {
	scripts := map[string]string{
		"zot":       registry.ZotScript("amd64", netip.MustParseAddr("10.0.0.2")),
		"uninstall": registry.ZotUninstallScript,
	}
	for _, sh := range []string{"sh", "bash", "dash"} {
		if _, err := exec.LookPath(sh); err != nil {
			continue
		}
		for name, script := range scripts {
			if out, err := exec.Command(sh, "-n", "-c", script).CombinedOutput(); err != nil {
				t.Errorf("%s: %s doesn't parse: %v\n%s", sh, name, err, out)
			}
		}
	}
}

func TestZotKeepsTheLastImagesAndRestarts(t *testing.T) {
	got := registry.ZotScript("amd64", netip.MustParseAddr("10.0.0.2"))
	for _, want := range []string{`"mostRecentlyPushedCount":3`, `"patterns":["live"]`, "RestartSec=5", "StartLimitIntervalSec=0", `"readTimeout":"30m"`} {
		if !strings.Contains(got, want) {
			t.Errorf("lacks %s:\n%s", want, got)
		}
	}
}

func TestZotsUninstallDoesntWaitForItself(t *testing.T) {
	m := regexp.MustCompile(`pgrep -f '([^']+)'`).FindStringSubmatch(registry.ZotUninstallScript)
	if m == nil {
		t.Fatalf("no pgrep in\n%s", registry.ZotUninstallScript)
	}
	if regexp.MustCompile(m[1]).MatchString(registry.ZotUninstallScript) {
		t.Errorf("the script's own command line matches its pgrep pattern %q", m[1])
	}
}

func TestZotsDownloadIsBounded(t *testing.T) {
	got := registry.ZotScript("amd64", netip.MustParseAddr("10.0.0.2"))
	if want := "curl -sfL --retry 3 --max-time 600 --speed-limit 10240 --speed-time 60 -o /usr/local/bin/zot.download "; !strings.Contains(got, want) {
		t.Errorf("lacks %s:\n%s", want, got)
	}
}

func TestZotListensOnTheRegistrysPortAtThePrivateAddress(t *testing.T) {
	got := registry.ZotScript("amd64", netip.MustParseAddr("10.0.0.2"))
	for _, want := range []string{`"address":"10.0.0.2","port":"5000"`, "http://10.0.0.2:5000/v2/"} {
		if !strings.Contains(got, want) {
			t.Errorf("lacks %s:\n%s", want, got)
		}
	}
}

func TestAnImageIsNamedAsNodesPullIt(t *testing.T) {
	if got, want := registry.Image("shop/web", "0a1b2c"), "moostack.local/shop/web@sha256:0a1b2c"; got != want {
		t.Errorf("Image = %s, want %s", got, want)
	}
}

func TestZotsInstallStartsWithItsDownload(t *testing.T) {
	got := registry.ZotScript("amd64", netip.MustParseAddr("10.0.0.2"))
	if !strings.HasPrefix(got, "set -e\ncurl ") {
		t.Errorf("the script starts:\n%s", got)
	}
}

func TestZotsUninstallWaitsForARunningInstall(t *testing.T) {
	m := regexp.MustCompile(`pgrep -f '([^']+)'`).FindStringSubmatch(registry.ZotUninstallScript)
	if install := registry.ZotScript("amd64", netip.MustParseAddr("10.0.0.2")); m == nil || !regexp.MustCompile(m[1]).MatchString(install) {
		t.Errorf("the uninstall's pgrep pattern %q doesn't match the install's command line:\n%s", m, install)
	}
}
