package console_test

import (
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/moostackhq/moostack/internal/console"
)

func TestTheLoginPageAsksForTheToken(t *testing.T) {
	res, body := send(t, console.New(cluster), http.MethodGet, "/console/login", nil, nil)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200", res.StatusCode)
	}
	if got := res.Header.Get("Cache-Control"); got != "no-store" {
		t.Errorf("Cache-Control %q, want no-store", got)
	}
	golden(t, "login.golden.html", body)
}

func TestATokenLogsIn(t *testing.T) {
	c := console.New(cluster)
	res, _, session := login(t, c, c.IssueToken())
	if res.StatusCode != http.StatusSeeOther || res.Header.Get("Location") != "/console/" {
		t.Errorf("status %d to %q, want 303 to /console/", res.StatusCode, res.Header.Get("Location"))
	}
	if session == nil {
		t.Fatal("no session cookie")
	}
	if session.Name != "__Secure-console" || session.Path != "/console" || !session.Secure || !session.HttpOnly ||
		session.SameSite != http.SameSiteStrictMode || session.MaxAge != 12*60*60 || session.Domain != "" {
		t.Errorf("the cookie is %+v, want __Secure-console for /console, Secure, HttpOnly, SameSite=Strict, for 12 hours, with no domain", session)
	}
	if len(session.Value) < 43 {
		t.Errorf("the session ID has %d characters, want 256 random bits", len(session.Value))
	}
	if res, body := send(t, c, http.MethodGet, "/console/", session, nil); res.StatusCode != http.StatusOK || !strings.Contains(body, "coredns") {
		t.Errorf("the page with the session: status %d:\n%s", res.StatusCode, body)
	}
}

func TestATokenInTheURLIsNotAccepted(t *testing.T) {
	c := console.New(cluster)
	token := c.IssueToken()
	for _, method := range []string{http.MethodPost, http.MethodGet} {
		res, _ := send(t, c, method, "/console/login?token="+token, nil, nil)
		if len(res.Cookies()) != 0 || res.StatusCode == http.StatusSeeOther {
			t.Errorf("%s with the token in the URL: status %d with %d cookies", method, res.StatusCode, len(res.Cookies()))
		}
	}
	if _, _, session := login(t, c, token); session == nil {
		t.Error("the token doesn't log in after it was sent in a URL")
	}
}

func TestAnOversizedLoginIsRefused(t *testing.T) {
	c := console.New(cluster)
	token := c.IssueToken()
	res, _ := send(t, c, http.MethodPost, "/console/login", nil, url.Values{"padding": {strings.Repeat("x", 1<<20)}, "token": {token}})
	if res.StatusCode != http.StatusRequestEntityTooLarge || len(res.Cookies()) != 0 {
		t.Errorf("status %d with %d cookies, want 413 and none", res.StatusCode, len(res.Cookies()))
	}
}

func TestLogoutEndsThatSessionAndNoOther(t *testing.T) {
	c := console.New(cluster)
	mine, theirs := newSession(t, c), newSession(t, c)
	res, _ := send(t, c, http.MethodPost, "/console/logout", mine, nil)
	if res.StatusCode != http.StatusSeeOther || res.Header.Get("Location") != "/console/login" {
		t.Errorf("status %d to %q, want 303 to /console/login", res.StatusCode, res.Header.Get("Location"))
	}
	cookies := res.Cookies()
	if len(cookies) != 1 || cookies[0].Name != "__Secure-console" || cookies[0].Value != "" || cookies[0].MaxAge >= 0 || cookies[0].Path != "/console" {
		t.Errorf("logout sets %+v, want the cookie removed", cookies)
	}
	if res, _ := send(t, c, http.MethodGet, "/console/", mine, nil); res.StatusCode != http.StatusSeeOther {
		t.Errorf("the session after logout: status %d, want 303", res.StatusCode)
	}
	if res, _ := send(t, c, http.MethodGet, "/console/", theirs, nil); res.StatusCode != http.StatusOK {
		t.Errorf("another session after logout: status %d, want 200", res.StatusCode)
	}
}

func TestLogoutWithoutASessionGoesToTheLoginPageAndSetsNoCookie(t *testing.T) {
	res, _ := send(t, console.New(cluster), http.MethodPost, "/console/logout", nil, nil)
	if res.StatusCode != http.StatusSeeOther || res.Header.Get("Location") != "/console/login" {
		t.Errorf("status %d to %q, want 303 to /console/login", res.StatusCode, res.Header.Get("Location"))
	}
	if got := res.Header.Get("Set-Cookie"); got != "" {
		t.Errorf("logout without a session sets %q, want no cookie", got)
	}
}

func TestAMalformedLoginIsRefused(t *testing.T) {
	c := console.New(cluster)
	token := c.IssueToken()
	for _, body := range []string{"token=" + token + "&x=%zz", "x=%zz&token=" + token, "token=" + token + "%"} {
		req := httptest.NewRequest(http.MethodPost, "/console/login", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		rec := httptest.NewRecorder()
		c.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest || len(rec.Result().Cookies()) != 0 {
			t.Errorf("a login with the body %q: status %d with %d cookies, want 400 and none", strings.ReplaceAll(body, token, "<token>"), rec.Code, len(rec.Result().Cookies()))
		}
	}
	if _, _, session := login(t, c, token); session == nil {
		t.Error("the token no longer logs in after malformed logins with it")
	}
}

func TestARestartedConsoleKnowsNoTokenOrSessionFromBefore(t *testing.T) {
	before := console.New(cluster)
	token, session := before.IssueToken(), newSession(t, before)

	restarted := console.New(cluster)
	res, body, got := login(t, restarted, token)
	refused(t, "a token from before the restart", res, body, got)
	if res, _ := send(t, restarted, http.MethodGet, "/console/", session, nil); res.StatusCode != http.StatusSeeOther {
		t.Errorf("a session from before the restart: status %d, want 303", res.StatusCode)
	}
}

func TestLogoutTakesOnlyPost(t *testing.T) {
	c := console.New(cluster)
	session := newSession(t, c)
	if res, _ := send(t, c, http.MethodGet, "/console/logout", session, nil); res.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("GET /console/logout: status %d, want 405", res.StatusCode)
	}
	if res, _ := send(t, c, http.MethodGet, "/console/", session, nil); res.StatusCode != http.StatusOK {
		t.Errorf("the session after GET /console/logout: status %d, want 200", res.StatusCode)
	}
}

func TestTokensAndSessionIDsAreNotLogged(t *testing.T) {
	written := recordOutput(t)

	c := console.New(cluster)
	used, unused := c.IssueToken(), c.IssueToken()
	session := newSessionWith(t, c, used)
	login(t, c, used)
	login(t, c, "ABCDEFGHIJKLMNOPQRSTUVWXYZ")
	send(t, c, http.MethodGet, "/console/", session, nil)
	send(t, c, http.MethodPost, "/console/logout", session, nil)
	for what, secret := range map[string]string{"a used token": used, "an unused token": unused, "a wrong token": "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "a session ID": session.Value} {
		if strings.Contains(written(), secret) {
			t.Errorf("the log or the output holds %s:\n%s", what, written())
		}
	}
}

// TestSequencesOfLoginsMatchAModel plays generated sequences of issuing
// tokens, logging in, viewing the page, logging out and waiting, and
// checks each answer against a model of the tokens and sessions.
func TestSequencesOfLoginsMatchAModel(t *testing.T) {
	type modelToken struct {
		issued time.Time
		used   bool
	}
	type modelSession struct {
		cookie *http.Cookie
		start  time.Time
		ended  bool
	}
	for seed := range uint64(200) {
		synctest.Test(t, func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, 1))
			c := console.New(cluster)
			tokens := map[string]*modelToken{}
			var names []string
			var sessions []*modelSession
			for step := range 80 {
				switch op := rng.IntN(6); {
				case op == 0:
					token := c.IssueToken()
					tokens[token] = &modelToken{issued: time.Now()}
					names = append(names, token)
				case op == 1 && len(names) > 0:
					token := names[rng.IntN(len(names))]
					m := tokens[token]
					usedBefore := m.used
					want := !m.used && time.Since(m.issued) < 5*time.Minute
					m.used = true
					_, _, cookie := login(t, c, token)
					if (cookie != nil) != want {
						t.Fatalf("seed %d step %d: a token %v old, used before: %v; logged in: %v", seed, step, time.Since(m.issued), usedBefore, cookie != nil)
					}
					if cookie != nil {
						sessions = append(sessions, &modelSession{cookie: cookie, start: time.Now()})
					}
				case op == 2 && len(sessions) > 0:
					s := sessions[rng.IntN(len(sessions))]
					want := !s.ended && time.Since(s.start) < 12*time.Hour
					res, _ := send(t, c, http.MethodGet, "/console/", s.cookie, nil)
					if (res.StatusCode == http.StatusOK) != want {
						t.Fatalf("seed %d step %d: a session %v old, ended: %v; status %d", seed, step, time.Since(s.start), s.ended, res.StatusCode)
					}
				case op == 3 && len(sessions) > 0:
					s := sessions[rng.IntN(len(sessions))]
					s.ended = true
					send(t, c, http.MethodPost, "/console/logout", s.cookie, nil)
				case op == 4:
					time.Sleep(time.Duration(rng.IntN(240)) * time.Second)
				case op == 5 && rng.IntN(8) == 0:
					time.Sleep(time.Duration(1+rng.IntN(7)) * time.Hour)
				}
			}
		})
	}
}

func TestALoginBodyOf4KiBIsTakenAndOneByteMoreIsRefused(t *testing.T) {
	c := console.New(cluster)
	post := func(length int, token string) *http.Response {
		body := "token=" + token + "&padding="
		body += strings.Repeat("x", length-len(body))
		req := httptest.NewRequest(http.MethodPost, "/console/login", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		rec := httptest.NewRecorder()
		c.ServeHTTP(rec, req)
		return rec.Result()
	}
	if res := post(4096, c.IssueToken()); res.StatusCode != http.StatusSeeOther || len(res.Cookies()) != 1 {
		t.Errorf("a login of 4096 bytes: status %d with %d cookies, want 303 with the session", res.StatusCode, len(res.Cookies()))
	}
	if res := post(4097, c.IssueToken()); res.StatusCode != http.StatusRequestEntityTooLarge || len(res.Cookies()) != 0 {
		t.Errorf("a login of 4097 bytes: status %d with %d cookies, want 413 and none", res.StatusCode, len(res.Cookies()))
	}
}

// twoCookies sends c a request with a stray cookie of the session's
// name ahead of the session's.
func twoCookies(c *console.Console, method, path string, session *http.Cookie) *http.Response {
	req := httptest.NewRequest(method, path, nil)
	req.AddCookie(&http.Cookie{Name: session.Name, Value: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"})
	req.AddCookie(session)
	rec := httptest.NewRecorder()
	c.ServeHTTP(rec, req)
	return rec.Result()
}

func TestAStrayCookieOfTheSessionsNameDoesntHideTheSession(t *testing.T) {
	c := console.New(cluster)
	session := newSession(t, c)
	if res := twoCookies(c, http.MethodGet, "/console/", session); res.StatusCode != http.StatusOK {
		t.Errorf("the page with a stray cookie ahead of the session's: status %d, want 200", res.StatusCode)
	}
}

func TestLogoutWithAStrayCookieEndsTheSession(t *testing.T) {
	c := console.New(cluster)
	session := newSession(t, c)
	twoCookies(c, http.MethodPost, "/console/logout", session)
	if res, _ := send(t, c, http.MethodGet, "/console/", session, nil); res.StatusCode != http.StatusSeeOther {
		t.Errorf("the session after a logout sent with a stray cookie ahead of it: status %d, want 303", res.StatusCode)
	}
}
