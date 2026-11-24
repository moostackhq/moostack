package cli

import (
	"bytes"
	"context"
	"errors"
	"net/netip"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

// consoleOn runs mooctl console on c, and returns standard output,
// standard error, the URLs the browser was asked to open and the
// command's error. A non-nil browser is the browser's failure.
func consoleOn(t *testing.T, c *fakeCluster, home string, browser error) (stdout, stderr string, opened []string, err error) {
	t.Helper()
	var out, errOut bytes.Buffer
	env := fakeEnvFor(c, home, &out)
	env.Stderr = &errOut
	env.Browser = func(url string) error {
		opened = append(opened, url)
		return browser
	}
	err = New(env).Run(t.Context(), []string{"mooctl", "-c", "prod", "console"})
	return out.String(), errOut.String(), opened, err
}

func TestConsolePrintsTheURLAndATokenAndOpensTheBrowser(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	stdout, stderr, opened, err := consoleOn(t, c, home, nil)
	if err != nil {
		t.Fatalf("mooctl console: %v\n%s", err, stderr)
	}
	if want := "URL: https://192.0.2.10/console/login\nToken: " + c.tokens[0] + "\n"; stdout != want {
		t.Errorf("standard output is %q, want %q", stdout, want)
	}
	if len(opened) != 1 || opened[0] != "https://192.0.2.10/console/login" {
		t.Errorf("the browser was asked to open %q", opened)
	}
	if len(c.ingressDials) != 1 || c.ingressDials[0] != "192.0.2.10:443" {
		t.Errorf("the ingress was connected to at %q, want once at 192.0.2.10:443", c.ingressDials)
	}
	if strings.Contains(stderr, c.tokens[0]) {
		t.Errorf("standard error holds the token:\n%s", stderr)
	}
	if stderr != "" {
		t.Errorf("standard error is %q for a console that answers at once", stderr)
	}
}

func TestEachConsoleGetsAnotherToken(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	first, _, _, _ := consoleOn(t, c, home, nil)
	second, _, _, _ := consoleOn(t, c, home, nil)
	if first == second || len(c.tokens) != 2 {
		t.Errorf("two runs printed %q and %q, with %d tokens issued", first, second, len(c.tokens))
	}
}

func TestConsoleOfANodeWithoutAPublicAddressIsAtItsPrivateOne(t *testing.T) {
	c, home := fakeProd(t)
	c.hosts["root@h1"].public = netip.Addr{}
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	stdout, _, opened, err := consoleOn(t, c, home, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(stdout, "URL: https://10.0.0.2/console/login\n") || len(opened) != 1 || opened[0] != "https://10.0.0.2/console/login" {
		t.Errorf("standard output is %q, and the browser opened %q", stdout, opened)
	}
}

func TestConsoleWaitsUntilTheLoginPageAnswers(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	synctest.Test(t, func(t *testing.T) {
		c.consolePageAfter = 3
		start := time.Now()
		stdout, stderr, opened, err := consoleOn(t, c, home, nil)
		if err != nil {
			t.Fatalf("mooctl console: %v\n%s", err, stderr)
		}
		if stderr != "Waiting for the console...\n" {
			t.Errorf("standard error is %q", stderr)
		}
		if len(opened) != 1 || !strings.Contains(stdout, "Token: ") {
			t.Errorf("standard output is %q, and the browser opened %q", stdout, opened)
		}
		if took := time.Since(start); took != 6*time.Second {
			t.Errorf("it waited %v for a page that answered on the fourth try, want 6s", took)
		}
	})
}

func TestConsoleGivesUpAfter2MinutesOnAnIngressThatStalls(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	synctest.Test(t, func(t *testing.T) {
		c.consolePageAfter, c.consolePageStalls = 2, true
		start := time.Now()
		stdout, _, opened, err := consoleOn(t, c, home, nil)
		if err == nil || !strings.HasPrefix(err.Error(), "the console doesn't answer at https://192.0.2.10/console/login: ") {
			t.Errorf("mooctl console: %v", err)
		}
		if stdout != "" || len(opened) != 0 || len(c.tokens) != 0 {
			t.Errorf("standard output is %q, the browser opened %q, and %d tokens were issued", stdout, opened, len(c.tokens))
		}
		if took := time.Since(start); took != 2*time.Minute {
			t.Errorf("it gave up after %v, want 2m", took)
		}
	})
}

func TestConsoleGivesUpOnALoginPageThatDoesntAnswerIn2Minutes(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	synctest.Test(t, func(t *testing.T) {
		c.consolePageAfter = 1 << 30
		start := time.Now()
		stdout, _, opened, err := consoleOn(t, c, home, nil)
		if err == nil || err.Error() != "the console doesn't answer at https://192.0.2.10/console/login: it answered 404" {
			t.Errorf("mooctl console: %v", err)
		}
		if stdout != "" || len(opened) != 0 || len(c.tokens) != 0 {
			t.Errorf("standard output is %q, the browser opened %q, and %d tokens were issued", stdout, opened, len(c.tokens))
		}
		if took := time.Since(start); took != 2*time.Minute {
			t.Errorf("it gave up after %v, want 2m", took)
		}
	})
}

func TestABrowserThatCantBeOpenedIsNoError(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	stdout, stderr, _, err := consoleOn(t, c, home, errors.New("exec: \"xdg-open\": executable file not found in $PATH"))
	if err != nil {
		t.Errorf("mooctl console without a browser: %v", err)
	}
	if !strings.HasPrefix(stdout, "URL: https://192.0.2.10/console/login\nToken: ") || stderr != "" {
		t.Errorf("standard output is %q and standard error %q", stdout, stderr)
	}
}

func TestConsoleRefusesAClusterWithoutNodesAndAnArgument(t *testing.T) {
	c, home := fakeProd(t)
	if _, _, _, err := consoleOn(t, c, home, nil); err == nil || !strings.Contains(err.Error(), "cluster prod has no nodes yet") {
		t.Errorf("mooctl console on a cluster without nodes: %v", err)
	}
	if _, err := onFake(t, c, home, "console", "more"); err == nil {
		t.Error("mooctl console with an argument: no error")
	}
}

// The console's container isn't running: it starts, or it was removed.
func TestConsoleWaitsForAConsoleThatIsntRunningAndIssuesNoTokenMeanwhile(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	image := c.consoleRuns
	c.stopConsole()
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		stdout, stderr, opened, err := consoleOn(t, c, home, nil)
		if err == nil || err.Error() != "the console doesn't answer at https://192.0.2.10/console/login: it answered 404" {
			t.Errorf("mooctl console on a cluster whose console never runs: %v", err)
		}
		if stdout != "" || stderr != "Waiting for the console...\n" || len(opened) != 0 || len(c.orch.made("ConsoleToken")) != 0 {
			t.Errorf("it printed %q, said %q, opened %q and asked for %d tokens", stdout, stderr, opened, len(c.orch.made("ConsoleToken")))
		}
		if took := time.Since(start); took != 2*time.Minute {
			t.Errorf("it gave up after %v, want 2m", took)
		}
	})
	synctest.Test(t, func(t *testing.T) {
		go func() {
			time.Sleep(30 * time.Second)
			c.orch.RunConsole(t.Context(), image, nil)
		}()
		start := time.Now()
		stdout, _, opened, err := consoleOn(t, c, home, nil)
		if err != nil || !strings.Contains(stdout, "Token: ") || len(opened) != 1 {
			t.Errorf("mooctl console on a cluster whose console starts after 30 seconds: %v, printed %q, opened %q", err, stdout, opened)
		}
		if took := time.Since(start); took < 30*time.Second || took > 34*time.Second {
			t.Errorf("it took %v, want the 30 seconds the console took to start", took)
		}
	})
}

func TestTheConsolesURLIsItsLoginPageAtTheAddress(t *testing.T) {
	for address, want := range map[string]string{
		"203.0.113.7": "https://203.0.113.7/console/login",
		"2001:db8::7": "https://[2001:db8::7]/console/login",
	} {
		if got := consoleURL(netip.MustParseAddr(address)); got != want {
			t.Errorf("the console's URL at %s is %q, want %q", address, got, want)
		}
	}
}

func TestConsoleFailsWhenATokenCantBeRead(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	c.orch.Fail("ConsoleToken", errors.New("the console isn't running"))
	stdout, _, opened, err := consoleOn(t, c, home, nil)
	if err == nil || err.Error() != "getting a login token: the console isn't running" || stdout != "" || len(opened) != 0 {
		t.Errorf("mooctl console without a token: %v, printed %q, opened %q", err, stdout, opened)
	}
	if got := c.orch.made(""); !slices.Equal(got[len(got)-1:], []string{"ConsoleToken"}) || c.orch.hosts != 1 {
		t.Errorf("the calls to the orchestrator ended with %q, through %d control plane connections; want ConsoleToken alone, through one", got, c.orch.hosts)
	}
}

func TestConsoleInterruptedWhileItWaitsFailsWithTheCauseEveryTime(t *testing.T) {
	c, home := fakeProd(t)
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	c.stopConsole()
	for range 20 {
		ctx, cancel := context.WithCancelCause(t.Context())
		var out bytes.Buffer
		env := fakeEnvFor(c, home, &out)
		env.Stderr = &lineHook{on: "Waiting for the console...", do: func() { cancel(errors.New("interrupted")) }}
		err := New(env).Run(ctx, []string{"mooctl", "-c", "prod", "console"})
		if err == nil || err.Error() != "interrupted" || out.Len() != 0 {
			t.Fatalf("mooctl console interrupted while it waits: %v, printed %q; want the interrupt alone", err, out.String())
		}
	}
}

// lineHook is a writer that calls do when it is written a line that
// starts with on.
type lineHook struct {
	on string
	do func()
}

func (h *lineHook) Write(p []byte) (int, error) {
	if strings.HasPrefix(string(p), h.on) {
		h.do()
	}
	return len(p), nil
}
