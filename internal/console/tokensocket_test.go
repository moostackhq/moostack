package console_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/moostackhq/moostack/internal/console"
)

// serve starts a console over cluster on a free port and on the token
// socket at socket. It returns the console's URL and a function that
// stops it and returns what Serve returned.
func serve(t *testing.T, socket string) (base string, stop func() error) {
	t.Helper()
	tokens, err := console.ListenTokenSocket(socket)
	if err != nil {
		t.Fatal(err)
	}
	web, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- console.New(cluster).Serve(ctx, web, tokens) }()
	stopped := false
	stop = func() error {
		if stopped {
			return nil
		}
		stopped = true
		cancel()
		select {
		case err := <-done:
			return err
		case <-time.After(30 * time.Second):
			t.Fatal("Serve didn't return within 30 seconds of being stopped")
			return nil
		}
	}
	t.Cleanup(func() { stop() })
	return "http://" + web.Addr().String(), stop
}

var client = &http.Client{
	CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	Timeout:       30 * time.Second,
}

func requestToken(t *testing.T, socket string) string {
	t.Helper()
	token, err := console.RequestToken(context.Background(), socket)
	if err != nil {
		t.Fatal(err)
	}
	return token
}

func TestATokenFromTheSocketLogsABrowserInOverHTTPS(t *testing.T) {
	socket := socketPath(t)
	tokens, err := console.ListenTokenSocket(socket)
	if err != nil {
		t.Fatal(err)
	}
	c := console.New(cluster)
	go c.Serve(t.Context(), newMemListener(), tokens)
	https := httptest.NewTLSServer(c)
	defer https.Close()
	browser := https.Client()
	if browser.Jar, err = cookiejar.New(nil); err != nil {
		t.Fatal(err)
	}
	// visit returns where a request ends after its redirects, and the
	// page there.
	visit := func(res *http.Response, err error) (string, string) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		body, err := io.ReadAll(res.Body)
		if err != nil {
			t.Fatal(err)
		}
		return res.Request.URL.Path, string(body)
	}

	if at, _ := visit(browser.Get(https.URL + "/console/")); at != "/console/login" {
		t.Errorf("without a session /console/ ends at %s, want the login page", at)
	}
	token := requestToken(t, socket)
	if at, body := visit(browser.PostForm(https.URL+"/console/login", url.Values{"token": {token}})); at != "/console/" || !strings.Contains(body, "<td>coredns</td>") {
		t.Errorf("the login ends at %s, want the containers page:\n%s", at, body)
	}
	if at, _ := visit(browser.Get(https.URL + "/console/")); at != "/console/" {
		t.Errorf("with the session /console/ ends at %s", at)
	}
	if at, _ := visit(browser.PostForm(https.URL+"/console/logout", nil)); at != "/console/login" {
		t.Errorf("logout ends at %s, want the login page", at)
	}
	if at, _ := visit(browser.Get(https.URL + "/console/")); at != "/console/login" {
		t.Errorf("after logout /console/ ends at %s, want the login page", at)
	}
	if at, body := visit(browser.PostForm(https.URL+"/console/login", url.Values{"token": {token}})); at != "/console/login" || !strings.Contains(body, "The token wasn&#39;t accepted.") {
		t.Errorf("the used token ends at %s:\n%s", at, body)
	}
}

func TestEachRequestOnTheSocketGetsAnotherToken(t *testing.T) {
	socket := socketPath(t)
	serve(t, socket)
	if first, second := requestToken(t, socket), requestToken(t, socket); first == second {
		t.Errorf("the socket gave %q twice", first)
	}
}

func TestTheSocketIsForItsOwnerOnly(t *testing.T) {
	socket := socketPath(t)
	serve(t, socket)
	info, err := os.Stat(socket)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Type() != os.ModeSocket || info.Mode().Perm() != 0o600 {
		t.Errorf("the socket is %v, want a socket of mode 0600", info.Mode())
	}
}

func TestASocketFileLeftBehindIsReplaced(t *testing.T) {
	socket := socketPath(t)
	left, err := net.ListenUnix("unix", &net.UnixAddr{Name: socket, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	left.SetUnlinkOnClose(false)
	left.Close()

	serve(t, socket)
	requestToken(t, socket)
}

func TestWithoutAConsoleATokenRequestSaysSo(t *testing.T) {
	socket := socketPath(t)
	token, err := console.RequestToken(context.Background(), socket)
	if err == nil || !strings.Contains(err.Error(), "no console is listening at "+socket) || token != "" {
		t.Errorf("RequestToken without a console: %q, %v, want an error that no console is listening at the socket", token, err)
	}
}

func TestStoppingEndsServeAndRemovesTheSocket(t *testing.T) {
	socket := socketPath(t)
	base, stop := serve(t, socket)
	if err := stop(); err != nil {
		t.Errorf("Serve returned %v", err)
	}
	if _, err := os.Stat(socket); !os.IsNotExist(err) {
		t.Errorf("the socket is still there: %v", err)
	}
	if _, err := client.Get(base + "/console/login"); err == nil {
		t.Error("the console still answers")
	}
}

func TestATokenFromTheSocketIsNotLogged(t *testing.T) {
	written := recordOutput(t)
	socket := socketPath(t)
	_, stop := serve(t, socket)
	token := requestToken(t, socket)
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(written(), token) {
		t.Errorf("the log or the output holds the token:\n%s", written())
	}
}

func TestASecondConsoleOnTheSocketIsRefusedAndLeavesIt(t *testing.T) {
	socket := socketPath(t)
	serve(t, socket)
	second, err := console.ListenTokenSocket(socket)
	if err == nil {
		second.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "a console is listening at "+socket) {
		t.Errorf("listening on a console's socket: %v, want an error that a console is listening there", err)
	}
	requestToken(t, socket)
}

// failsOnce is a listener whose first Accept fails.
type failsOnce struct {
	net.Listener
	failed atomic.Bool
}

func (l *failsOnce) Accept() (net.Conn, error) {
	if l.failed.CompareAndSwap(false, true) {
		return nil, errors.New("accept: too many open files")
	}
	return l.Listener.Accept()
}

func TestAFailedAcceptOnTheSocketDoesntStopTheConsole(t *testing.T) {
	recordOutput(t)
	socket := socketPath(t)
	tokens, err := console.ListenTokenSocket(socket)
	if err != nil {
		t.Fatal(err)
	}
	web, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- console.New(cluster).Serve(ctx, web, &failsOnce{Listener: tokens}) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Serve returned %v", err)
		}
	}()

	requestToken(t, socket)
	res, err := client.Get("http://" + web.Addr().String() + "/console/login")
	if err != nil {
		t.Fatalf("the pages after a failed accept: %v", err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Errorf("the login page after a failed accept: status %d", res.StatusCode)
	}
}

func TestTheSocketsPathMustBeAbsolute(t *testing.T) {
	for _, path := range []string{"", "@console", "console.sock", "./console.sock"} {
		l, err := console.ListenTokenSocket(path)
		if err == nil {
			l.Close()
		}
		if err == nil || !strings.Contains(err.Error(), "isn't absolute") {
			t.Errorf("ListenTokenSocket(%q): %v, want a refusal of a path that isn't absolute", path, err)
		}
	}
}

func TestASocketPathThatCantBeUsedIsRefused(t *testing.T) {
	dir := filepath.Dir(socketPath(t))
	full := filepath.Join(dir, "full")
	if err := os.MkdirAll(filepath.Join(full, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(dir, "file")
	if err := os.WriteFile(file, []byte("not a socket"), 0o600); err != nil {
		t.Fatal(err)
	}
	for what, path := range map[string]string{
		"an ordinary file":             file,
		"a directory that isn't empty": full,
		"a directory that is missing":  filepath.Join(dir, "missing", "console.sock"),
		"a path too long for a socket": filepath.Join(dir, strings.Repeat("x", 200)),
	} {
		l, err := console.ListenTokenSocket(path)
		if err == nil {
			l.Close()
			t.Errorf("%s: no error", what)
		}
	}
	if kept, err := os.ReadFile(file); err != nil || string(kept) != "not a socket" {
		t.Errorf("the ordinary file is %q, %v", kept, err)
	}
	if _, err := os.Stat(filepath.Join(full, "sub")); err != nil {
		t.Errorf("the directory that isn't empty was changed: %v", err)
	}
}

// answering listens on a socket and answers each connection with what
// answer returns, then closes it; a nil answer holds the connection
// open until the test ends.
func answering(t testing.TB, answer func() []byte) string {
	t.Helper()
	socket := socketPath(t)
	l, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	held := make(chan struct{})
	t.Cleanup(func() {
		close(held)
		l.Close()
	})
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				if a := answer(); a != nil {
					conn.Write(a)
					return
				}
				<-held
			}()
		}
	}()
	return socket
}

func TestATokenRequestTakesOnlyAToken(t *testing.T) {
	const token = "ABCDEFGHIJKLMNOPQRSTUVWXYA"
	for answer, want := range map[string]string{
		token + "\n":                      token,
		token:                             token,
		"":                                "",
		"\n":                              "",
		"ABCDEFGHIJ\n":                    "",
		token + "A\n":                     "",
		token + "\nsecond line\x1b[31m\n": "",
		strings.ToLower(token) + "\n":     "",
		" " + token + "\n":                "",
		strings.Repeat("A", 500):          "",
		"ABCDEFGHIJKLMNOPQRSTUVWXYZ\n":    "",
	} {
		socket := answering(t, func() []byte { return []byte(answer) })
		got, err := console.RequestToken(context.Background(), socket)
		if got != want || (err == nil) != (want != "") {
			t.Errorf("a socket that answers %q: %q, %v; want %q", answer, got, err, want)
		}
		if want == "" && err != nil && !strings.Contains(err.Error(), "gave no token") {
			t.Errorf("a socket that answers %q: %v, want an error that the console gave no token", answer, err)
		}
	}
}

func FuzzATokenRequestTakesOnlyAToken(f *testing.F) {
	for _, seed := range []string{"ABCDEFGHIJKLMNOPQRSTUVWXYA\n", "ABCDEFGHIJKLMNOPQRSTUVWXYA", "", "\n", "abc", "ABCDEFGHIJKLMNOPQRSTUVWXYZ\n", "\x00\x1b[31m"} {
		f.Add([]byte(seed))
	}
	var answer atomic.Pointer[[]byte]
	empty := []byte{}
	answer.Store(&empty)
	socket := answering(f, func() []byte { return *answer.Load() })
	form := regexp.MustCompile(`^[A-Z2-7]{26}$`)
	f.Fuzz(func(t *testing.T, a []byte) {
		answer.Store(&a)
		token, err := console.RequestToken(context.Background(), socket)
		if err == nil && (!form.MatchString(token) || (string(a) != token && string(a) != token+"\n")) {
			t.Errorf("a socket that answers %q gave the token %q", a, token)
		}
		if err != nil && token != "" {
			t.Errorf("a socket that answers %q: %q with %v", a, token, err)
		}
	})
}

func TestATokenRequestGivesUpAfter5SecondsOnASocketThatDoesntAnswer(t *testing.T) {
	socket := answering(t, func() []byte { return nil })
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	start := time.Now()
	token, err := console.RequestToken(ctx, socket)
	if err == nil || !strings.Contains(err.Error(), "reading the token from "+socket) || token != "" {
		t.Errorf("RequestToken on a socket that doesn't answer: %q, %v", token, err)
	}
	if took := time.Since(start); took < 4900*time.Millisecond || took > 7*time.Second {
		t.Errorf("it gave up after %v, want 5 seconds", took)
	}
}

func TestATokenRequestEndsAtOnceWhenItsContextEnds(t *testing.T) {
	socket := answering(t, func() []byte { return nil })
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(100*time.Millisecond, cancel)
	start := time.Now()
	_, err := console.RequestToken(ctx, socket)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("RequestToken when its context ends: %v, want context.Canceled", err)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Errorf("it ended %v after its context, want at once", took)
	}
}

func TestATokenRequestsPathMustBeAbsolute(t *testing.T) {
	for _, path := range []string{"", "@console", "console.sock"} {
		token, err := console.RequestToken(context.Background(), path)
		if err == nil || !strings.Contains(err.Error(), "isn't absolute") || token != "" {
			t.Errorf("RequestToken(%q): %q, %v, want a refusal of a path that isn't absolute", path, token, err)
		}
	}
}

func TestATokenRequestReadsNoMoreThanATokenNeeds(t *testing.T) {
	socket := socketPath(t)
	l, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	written := make(chan int, 1)
	go func() {
		conn, err := l.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(10 * time.Second))
		n := 0
		for chunk := bytes.Repeat([]byte("A"), 4096); ; {
			m, err := conn.Write(chunk)
			n += m
			if err != nil {
				written <- n
				return
			}
		}
	}()
	start := time.Now()
	token, err := console.RequestToken(context.Background(), socket)
	if err == nil || !strings.Contains(err.Error(), "gave no token") || token != "" {
		t.Errorf("RequestToken on a socket that answers without end: %q, %v", token, err)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Errorf("it read for %v, want it to stop once it has more than a token", took)
	}
	if n := <-written; n > 1<<20 {
		t.Errorf("it took %d bytes of the answer", n)
	}
}

func TestASocketThatCantBeDialledIsNotReplaced(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("fills a listen queue of one, which only Linux refuses dials on")
	}
	socket := socketPath(t)
	fd, err := syscall.Socket(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(fd)
	if err := syscall.Bind(fd, &syscall.SockaddrUnix{Name: socket}); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Listen(fd, 0); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		conn, err := net.DialTimeout("unix", socket, time.Second)
		if err != nil {
			break
		}
		defer conn.Close()
	}
	before, err := os.Stat(socket)
	if err != nil {
		t.Fatal(err)
	}

	l, err := console.ListenTokenSocket(socket)
	if err == nil {
		l.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "is in use") {
		t.Errorf("listening on a live socket that can't be dialled: %v, want a refusal", err)
	}
	if after, err := os.Stat(socket); err != nil || !os.SameFile(before, after) {
		t.Errorf("the live socket's file was replaced: %v", err)
	}
}

func TestOfConsolesStartingAtOnceOneGetsTheSocket(t *testing.T) {
	umask := syscall.Umask(0)
	syscall.Umask(umask)
	for round := range 200 {
		socket := socketPath(t)
		left, err := net.ListenUnix("unix", &net.UnixAddr{Name: socket, Net: "unix"})
		if err != nil {
			t.Fatal(err)
		}
		left.SetUnlinkOnClose(false)
		left.Close()

		var wg sync.WaitGroup
		var mu sync.Mutex
		var got []net.Listener
		release := make(chan struct{})
		for range 4 {
			wg.Go(func() {
				<-release
				if l, err := console.ListenTokenSocket(socket); err == nil {
					mu.Lock()
					got = append(got, l)
					mu.Unlock()
				}
			})
		}
		close(release)
		wg.Wait()
		if len(got) != 1 {
			t.Fatalf("round %d: %d consoles got the socket, want 1", round, len(got))
		}
		conn, err := net.DialTimeout("unix", socket, time.Second)
		if err != nil {
			t.Fatalf("round %d: the socket of the console that got it can't be reached: %v", round, err)
		}
		conn.Close()
		got[0].Close()
	}
	after := syscall.Umask(umask)
	if after != umask {
		t.Errorf("the process's umask was left at %04o, not %04o", after, umask)
	}
}
