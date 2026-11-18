package console_test

import (
	"encoding/base32"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/moostackhq/moostack/internal/console"
)

func TestATokenIs26LettersAndDigitsAndNoTwoAreTheSame(t *testing.T) {
	c := console.New(cluster)
	form := regexp.MustCompile(`^[A-Z2-7]{26}$`)
	seen := map[string]bool{}
	for range 1000 {
		token := c.IssueToken()
		if !form.MatchString(token) {
			t.Fatalf("the token %q isn't 26 letters and digits", token)
		}
		if seen[token] {
			t.Fatalf("the token %q was issued twice", token)
		}
		seen[token] = true
	}
}

func TestATokenPastedWithSpaceAroundItLogsIn(t *testing.T) {
	c := console.New(cluster)
	if _, _, session := login(t, c, "  "+c.IssueToken()+"\n"); session == nil {
		t.Error("no session")
	}
}

func TestATokenLogsInOnce(t *testing.T) {
	c := console.New(cluster)
	token := c.IssueToken()
	first := newSessionWith(t, c, token)
	res, body, session := login(t, c, token)
	refused(t, "the token again", res, body, session)
	if res, _ := send(t, c, http.MethodGet, "/console/", first, nil); res.StatusCode != http.StatusOK {
		t.Errorf("the first session after the second try: status %d, want 200", res.StatusCode)
	}
}

func TestAWrongTokenIsRefused(t *testing.T) {
	c := console.New(cluster)
	real := c.IssueToken()
	for what, token := range map[string]string{
		"no token":             "",
		"spaces":               "   ",
		"another token":        "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
		"the token cut short":  real[:25],
		"the token and more":   real + "A",
		"the token lower case": strings.ToLower(real),
		"markup":               `"><script>alert(1)</script>`,
	} {
		res, body, session := login(t, c, token)
		refused(t, what, res, body, session)
		if strings.Contains(body, "<script>alert(1)") {
			t.Errorf("%s: the page holds the token unescaped", what)
		}
	}
	if _, _, session := login(t, c, real); session == nil {
		t.Error("the real token no longer logs in after wrong ones")
	}
}

func TestATokenIsAcceptedFor5Minutes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := console.New(cluster)
		early, late := c.IssueToken(), c.IssueToken()
		time.Sleep(5*time.Minute - time.Second)
		if _, _, session := login(t, c, early); session == nil {
			t.Error("a token of 4m59s wasn't accepted")
		}
		time.Sleep(time.Second)
		res, body, session := login(t, c, late)
		refused(t, "a token of 5 minutes", res, body, session)
	})
}

func TestOneTokenUsedByManyAtOnceLogsInOne(t *testing.T) {
	c := console.New(cluster)
	for range 300 {
		token := c.IssueToken()
		var wg sync.WaitGroup
		var sessions atomic.Int32
		release := make(chan struct{})
		for range 64 {
			wg.Go(func() {
				<-release
				if _, _, session := login(t, c, token); session != nil {
					sessions.Add(1)
				}
			})
		}
		close(release)
		wg.Wait()
		if got := sessions.Load(); got != 1 {
			t.Fatalf("%d sessions from one token, want 1", got)
		}
	}
}

func TestIssuingLoggingInViewingAndLoggingOutAtOnce(t *testing.T) {
	c := console.New(cluster)
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			for range 50 {
				session := newSession(t, c)
				if res, _ := send(t, c, http.MethodGet, "/console/", session, nil); res.StatusCode != http.StatusOK {
					t.Errorf("the page with a new session: status %d", res.StatusCode)
				}
				send(t, c, http.MethodPost, "/console/logout", session, nil)
				if res, _ := send(t, c, http.MethodGet, "/console/", session, nil); res.StatusCode != http.StatusSeeOther {
					t.Errorf("the page after logout: status %d", res.StatusCode)
				}
				c.IssueToken()
			}
		})
	}
	wg.Wait()
	if _, sessions := c.Kept(); sessions != 0 {
		t.Errorf("%d sessions are kept after every one logged out", sessions)
	}
}
func TestUsedTokensAndThosePastTheirLifeAreDropped(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := console.New(cluster)
		for range 100 {
			c.IssueToken()
		}
		newSession(t, c)
		if tokens, _ := c.Kept(); tokens != 100 {
			t.Fatalf("%d tokens are kept after 100 unused ones and a used one, want 100", tokens)
		}
		time.Sleep(5 * time.Minute)
		c.IssueToken()
		if tokens, _ := c.Kept(); tokens != 1 {
			t.Errorf("%d tokens are kept after 5 minutes, want the new one", tokens)
		}
	})
}

func TestEveryByteOfATokenIsRandom(t *testing.T) {
	c := console.New(cluster)
	var seen [16]map[byte]bool
	for i := range seen {
		seen[i] = map[byte]bool{}
	}
	for range 1000 {
		secret, err := base32.StdEncoding.WithPadding(base32.NoPadding).DecodeString(c.IssueToken())
		if err != nil || len(secret) != len(seen) {
			t.Fatalf("a token decodes to %d bytes, %v; want 16", len(secret), err)
		}
		for i, b := range secret {
			seen[i][b] = true
		}
	}
	for i, values := range seen {
		if len(values) < 200 {
			t.Errorf("byte %d of 1000 tokens took %d values, want about 250", i, len(values))
		}
	}
}
