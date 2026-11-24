package cli

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"testing"
)

// logsOn runs mooctl logs with args on prod under ctx, and returns what
// it printed on each stream.
func logsOn(ctx context.Context, c *fakeCluster, home string, args ...string) (stdout, stderr string, err error) {
	var out, errs bytes.Buffer
	env := fakeEnvFor(c, home, nil)
	env.Stdout, env.Stderr = &out, &errs
	err = New(env).Run(ctx, append([]string{"mooctl", "-c", "prod", "logs"}, args...))
	return out.String(), errs.String(), err
}

// logging is prod with the app web deployed from the directory the test
// is in afterwards, and its logs.
func logging(t *testing.T) (*fakeCluster, string) {
	t.Helper()
	c, home := deployingProd(t)
	t.Chdir(manifestsApp(t, webDeployment))
	mustRun(t, c, home, "deploy")
	c.orch.Logged = map[string]string{"web": "[pod/web-1/web] a log line\n[pod/web-2/web] another\n"}
	return c, home
}

func TestLogsPrintsTheAppsLogsAsAskedFor(t *testing.T) {
	c, home := logging(t)
	const lines = "[pod/web-1/web] a log line\n[pod/web-2/web] another\n"
	for _, tc := range []struct {
		args         []string
		call, stderr string
	}{
		{nil, "Logs web, tail 100, follow false", ""},
		{[]string{"web"}, "Logs web, tail 100, follow false", ""},
		{[]string{"--tail", "5"}, "Logs web, tail 5, follow false", ""},
		{[]string{"--tail", "0"}, "Logs web, tail 0, follow false", ""},
		{[]string{"--tail", "-1", "web"}, "Logs web, tail -1, follow false", ""},
		{[]string{"-f", "web"}, "Logs web, tail 100, follow true", "Stopped: the pods followed have ended.\n"},
		{[]string{"--follow", "--tail", "1"}, "Logs web, tail 1, follow true", "Stopped: the pods followed have ended.\n"},
	} {
		before := len(c.orch.made(""))
		stdout, stderr, err := logsOn(t.Context(), c, home, tc.args...)
		if err != nil || stdout != lines || stderr != tc.stderr {
			t.Errorf("logs %q: %v, printed %q on stdout and %q on stderr, want the lines and %q", tc.args, err, stdout, stderr, tc.stderr)
		}
		if len(tc.args) == 2 && tc.args[0] == "-f" {
			golden(t, "logs-follow.stdout.golden", stdout)
			golden(t, "logs-follow.stderr.golden", stderr)
		}
		if calls := c.orch.made("")[before:]; !slices.Equal(calls, []string{tc.call}) {
			t.Errorf("logs %q made the calls %q, want %q", tc.args, calls, tc.call)
		}
	}
	if c.locked {
		t.Error("logs took the operation lock")
	}
}

func TestLogsRefusals(t *testing.T) {
	c, home := logging(t)
	charted := t.TempDir()
	for _, tc := range []struct {
		name string
		args []string
		want string
		// calls is how many calls to the orchestrator the command makes.
		calls int
	}{
		{"an app that isn't there", []string{"other"}, "there is no app other on prod", 1},
		{"a name that isn't an app's", []string{"Web;rm"}, `"Web;rm" may hold only lowercase letters, digits and hyphens`, 0},
		{"two apps", []string{"web", "other"}, "mooctl logs takes 1 argument, not 2; see --help", 0},
		{"a tail below -1", []string{"--tail", "-2"}, "--tail -2: it is a number of lines, or -1 for all", 0},
	} {
		before := len(c.orch.made(""))
		stdout, _, err := logsOn(t.Context(), c, home, tc.args...)
		if err == nil || err.Error() != tc.want {
			t.Errorf("logs of %s: %v, want %q", tc.name, err, tc.want)
		}
		if calls := c.orch.made("")[before:]; stdout != "" || len(calls) != tc.calls {
			t.Errorf("logs of %s printed %q and made the calls %q, want %d", tc.name, stdout, calls, tc.calls)
		}
	}

	// An app without replicas, followed or not.
	delete(c.orch.Logged, "web")
	for _, args := range [][]string{nil, {"-f", "web"}} {
		if stdout, stderr, err := logsOn(t.Context(), c, home, args...); err == nil || err.Error() != "web has no pods" || stdout != "" || stderr != "" {
			t.Errorf("logs %q of an app without replicas: %v, printed %q and %q", args, err, stdout, stderr)
		}
	}

	t.Chdir(charted)
	if _, _, err := logsOn(t.Context(), c, home); err == nil || err.Error() != "which app? name it, or run mooctl logs in its directory" {
		t.Errorf("logs with no app named: %v", err)
	}

	c.orch.Fail("Logs", errors.New("listing web's pods: the connection is lost"))
	if _, stderr, err := logsOn(t.Context(), c, home, "-f", "web"); err == nil || err.Error() != "listing web's pods: the connection is lost" || stderr != "" {
		t.Errorf("logs that fail: %v, printed %q on stderr", err, stderr)
	}
}

func TestLogsInterruptedExitsWithoutAnError(t *testing.T) {
	c, home := logging(t)
	for _, follow := range [][]string{{"-f"}, nil} {
		ctx, interrupt := context.WithCancel(t.Context())
		c.orch.onLogs = interrupt
		stdout, stderr, err := logsOn(ctx, c, home, follow...)
		if err != nil || stdout != "" || stderr != "" {
			t.Errorf("logs %q interrupted: %v, printed %q on stdout and %q on stderr, want nothing", follow, err, stdout, stderr)
		}
	}
}
