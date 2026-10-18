package bootstrap

import (
	"strings"
	"testing"
)

func TestUnitFilesRunTheInstalledBinaries(t *testing.T) {
	want := map[string][]string{
		"/etc/systemd/system/moostack-containerd.service": {
			"ExecStart=/opt/moostack/bin/containerd --config /etc/moostack/containerd.toml",
			"Environment=PATH=/opt/moostack/bin:",
		},
		"/etc/systemd/system/moostack-agent.service": {
			"Wants=moostack-containerd.service",
			"After=moostack-containerd.service",
			"ExecStart=/opt/moostack/bin/agent serve",
		},
	}
	if len(unitFiles) != len(want) {
		t.Fatalf("there are %d unit files, want %d", len(unitFiles), len(want))
	}
	for _, u := range unitFiles {
		lines, ok := want[u.path]
		if !ok {
			t.Errorf("an unexpected unit file %s", u.path)
			continue
		}
		for _, line := range lines {
			if !strings.Contains(u.content, "\n"+line) {
				t.Errorf("%s has no line starting %q:\n%s", u.path, line, u.content)
			}
		}
	}
}
