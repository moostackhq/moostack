package cli

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// chartK3s is a k3s whose kubectl answers the chart scripts from the
// files in its directory: generation, job (uid|succeeded|failed), and
// fail, which makes every call fail.
func chartK3s(t *testing.T, state map[string]string) []string {
	t.Helper()
	dir := t.TempDir()
	for name, v := range state {
		os.WriteFile(filepath.Join(dir, name), []byte(v), 0o644)
	}
	script := `#!/bin/sh
d=$(dirname "$0")
[ -f "$d/fail" ] && { echo "the server is unavailable" >&2; exit 1; }
case "$*" in
*"get helmchart"*) [ -f "$d/generation" ] && cat "$d/generation" ;;
*"get job"*"succeeded"*) [ -f "$d/job" ] && cat "$d/job" ;;
*"get job"*) [ -f "$d/job" ] && cut -d'|' -f1 "$d/job" | tr -d '\n' ;;
*logs*) echo "helm: install failed" ;;
esac
exit 0
`
	os.WriteFile(filepath.Join(dir, "k3s"), []byte(script), 0o755)
	return []string{"PATH=" + dir + ":/usr/bin:/bin"}
}

func runChartScript(t *testing.T, env []string, script string) (string, error) {
	t.Helper()
	cmd := exec.Command("sh", "-c", script)
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	return strings.TrimSpace(string(out)), err
}

func TestTheChartStateKeepsItsFieldsApart(t *testing.T) {
	for _, c := range []struct {
		state map[string]string
		want  string
	}{
		{map[string]string{"generation": "2", "job": "uid-1|1|"}, "2|uid-1"},
		{map[string]string{"job": "uid-1|1|"}, "|uid-1"},
		{map[string]string{"generation": "2"}, "2|"},
		{map[string]string{}, "|"},
	} {
		out, err := runChartScript(t, chartK3s(t, c.state), chartStateScript("web"))
		if err != nil || out != c.want {
			t.Errorf("%v: %q, %v; want %q", c.state, out, err, c.want)
		}
	}
	if _, err := runChartScript(t, chartK3s(t, map[string]string{"fail": "1"}), chartStateScript("web")); err == nil {
		t.Error("a failed read passed")
	}
}

func TestTheChartWaitTellsAFailedPodFromSuccess(t *testing.T) {
	before := chartState{generation: "1", job: "old"}
	for _, c := range []struct {
		name, job string
		ok        bool
		says      string
	}{
		{"the new job succeeded", "new|1|", true, ""},
		{"the new job succeeded after a failed pod", "new|1|1", true, ""},
		{"one failed pod, still running", "new||1", false, "isn't installed"},
		{"three failed pods", "new||3", false, "helm: install failed"},
		{"only the old job", "old|1|", false, "isn't installed"},
	} {
		env := chartK3s(t, map[string]string{"generation": "2", "job": c.job})
		out, err := runChartScript(t, env, waitChartScript("web", before, 2))
		if (err == nil) != c.ok || !strings.Contains(out, c.says) {
			t.Errorf("%s: %q, %v", c.name, out, err)
		}
	}
	// An unchanged chart waits for the job that was there.
	env := chartK3s(t, map[string]string{"generation": "1", "job": "old|1|"})
	if out, err := runChartScript(t, env, waitChartScript("web", before, 2)); err != nil {
		t.Errorf("an unchanged chart: %q, %v", out, err)
	}
}
