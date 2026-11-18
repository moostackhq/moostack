package cli

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/moostackhq/moostack/internal/k3s"
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
	c.hosts["root@h1"].addrs = []string{"ens10 10.0.0.2/32"}
	c.hosts["root@h1"].routes = []string{"ens10"}
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

func TestConsoleRefusesAClusterWithoutNodesOrWithoutAConsole(t *testing.T) {
	c, home := fakeProd(t)
	if _, _, _, err := consoleOn(t, c, home, nil); err == nil || !strings.Contains(err.Error(), "cluster prod has no nodes yet") {
		t.Errorf("mooctl console on a cluster without nodes: %v", err)
	}
	mustRun(t, c, home, "cluster", "node-add", "root@h1")
	delete(c.hosts["root@h1"].files, k3s.AutoDeployFile("console"))
	stdout, _, opened, err := consoleOn(t, c, home, nil)
	if err == nil || !strings.Contains(err.Error(), "cluster prod has no console") {
		t.Errorf("mooctl console on a cluster without a console: %v", err)
	}
	if stdout != "" || len(opened) != 0 {
		t.Errorf("standard output is %q, and the browser opened %q", stdout, opened)
	}
	if _, err := onFake(t, c, home, "console", "more"); err == nil {
		t.Error("mooctl console with an argument: no error")
	}
}
