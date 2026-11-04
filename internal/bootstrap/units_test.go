package bootstrap

import (
	"os"
	"strings"
	"testing"
)

func TestUnitFilesRunTheInstalledBinaries(t *testing.T) {
	want := map[string][]string{
		"/etc/systemd/system/moostack-containerd.service": {
			"ExecStart=/opt/moostack/bin/containerd --config /etc/moostack/containerd.toml",
			"Environment=PATH=/opt/moostack/bin:",
		},
		// The updater doesn't depend on the agent or containerd, so it
		// runs whatever state they are in.
		"/etc/systemd/system/moostack-updater.service": {
			"ExecStart=/opt/moostack/bin/updater serve",
			"Restart=always",
		},
		"/etc/systemd/system/moostack-agent.service": {
			"Wants=moostack-containerd.service",
			"After=moostack-containerd.service",
			"ExecStart=/opt/moostack/bin/agent serve",
			// An agent that can't open its ports keeps trying until it can.
			"Restart=always",
			"StartLimitIntervalSec=0",
		},
	}
	agent, err := os.ReadFile("units/moostack-agent.service")
	if err != nil {
		t.Fatal(err)
	}
	if limit, service := strings.Index(string(agent), "StartLimitIntervalSec="), strings.Index(string(agent), "[Service]"); limit < 0 || limit > service {
		t.Error("StartLimitIntervalSec isn't in the agent unit's [Unit], the only section systemd reads it from")
	}
	if len(unitFiles) != len(want) {
		t.Fatalf("there are %d unit files, want %d", len(unitFiles), len(want))
	}
	for _, u := range unitFiles {
		// Every unit dependency is set in [Unit], which for the updater
		// holds only its description. systemd merges a repeated [Unit]
		// into the first, so there must be only one.
		if u.path == "/etc/systemd/system/moostack-updater.service" &&
			(!strings.HasPrefix(u.content, "[Unit]\nDescription=Moostack updater\n\n[") || strings.Count(u.content, "[Unit]") != 1) {
			t.Errorf("the updater's unit has more than a description in [Unit]:\n%s", u.content)
		}
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
