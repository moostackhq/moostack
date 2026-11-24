package cli

import (
	"cmp"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"os/exec"
	"runtime"
	"time"

	"github.com/urfave/cli/v3"

	"github.com/moostackhq/moostack/internal/interrupt"
)

// consoleCommand is mooctl console.
func consoleCommand(env Env) *cli.Command {
	return &cli.Command{
		Name:   "console",
		Usage:  "print a login token of the cluster's console and open its login page in the browser",
		Action: func(ctx context.Context, cmd *cli.Command) error { return console(ctx, env, cmd) },
	}
}

// consoleTimeout is how long the console's login page gets to answer.
const consoleTimeout = 2 * time.Minute

// console is mooctl console.
func console(ctx context.Context, env Env, cmd *cli.Command) error {
	if err := wantArgs(cmd, 0); err != nil {
		return err
	}
	ctx, stop := interrupt.Context(ctx, env.Exit)
	defer stop()
	d, err := openCluster(env, cmd)
	if err != nil {
		return err
	}
	cp, ok, err := d.ControlPlane()
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("cluster %s has no nodes yet; add one with mooctl cluster node-add", d.Cluster.Ref)
	}
	cpClient, err := dialNode(ctx, env, cp)
	if err != nil {
		return err
	}
	defer cpClient.Close()
	address := cp.PrivateAddress
	if cp.PublicAddress.IsValid() {
		address = cp.PublicAddress
	}
	url := consoleURL(address)
	ingress := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			DialContext:       func(ctx context.Context, _, addr string) (net.Conn, error) { return cpClient.DialTCP(ctx, addr) },
			TLSClientConfig:   &tls.Config{InsecureSkipVerify: true},
			DisableKeepAlives: true,
		},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	waitCtx, cancel := context.WithTimeout(ctx, consoleTimeout)
	defer cancel()
	for waiting, last := false, error(nil); ; {
		err := loginPageAnswers(waitCtx, ingress, url)
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		if waitCtx.Err() == nil {
			last = err
		}
		if !waiting {
			say(env, "Waiting for the console...")
			waiting = true
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-waitCtx.Done():
			if ctx.Err() != nil {
				return context.Cause(ctx)
			}
			return fmt.Errorf("the console doesn't answer at %s: %w", url, cmp.Or(last, err))
		case <-time.After(2 * time.Second):
		}
	}
	token, err := orchestratorThrough(env, cpClient).ConsoleToken(ctx)
	if err != nil {
		return fmt.Errorf("getting a login token: %w", err)
	}
	fmt.Fprintf(env.Stdout, "URL: %s\nToken: %s\n", url, token)
	if env.Browser != nil {
		env.Browser(url)
	}
	return nil
}

// consoleURL is the URL of the console's login page at address, one of
// the control plane node's.
func consoleURL(address netip.Addr) string {
	host := address.String()
	if address.Is6() {
		host = "[" + host + "]"
	}
	return "https://" + host + "/console/login"
}

// loginPageAnswers asks for the console's login page at url, and fails
// unless the answer is 200.
func loginPageAnswers(ctx context.Context, client *http.Client, url string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("it answered %d", res.StatusCode)
	}
	return nil
}

// openBrowser opens url in the browser of the operator's desktop.
func openBrowser(url string) error {
	opener := "xdg-open"
	if runtime.GOOS == "darwin" {
		opener = "open"
	}
	return exec.Command(opener, url).Start()
}
