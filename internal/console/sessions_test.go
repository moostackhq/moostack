package console_test

import (
	"encoding/base64"
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"github.com/moostackhq/moostack/internal/console"
)

func TestASessionLasts12Hours(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := console.New(cluster)
		session := newSession(t, c)
		time.Sleep(12*time.Hour - time.Second)
		if res, _ := send(t, c, http.MethodGet, "/console/", session, nil); res.StatusCode != http.StatusOK {
			t.Errorf("after 11h59m59s: status %d, want 200", res.StatusCode)
		}
		time.Sleep(time.Second)
		if res, _ := send(t, c, http.MethodGet, "/console/", session, nil); res.StatusCode != http.StatusSeeOther {
			t.Errorf("after 12h: status %d, want 303", res.StatusCode)
		}
	})
}

func TestEndedSessionsAreDropped(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := console.New(cluster)
		for range 100 {
			newSession(t, c)
		}
		send(t, c, http.MethodPost, "/console/logout", newSession(t, c), nil)
		if _, sessions := c.Kept(); sessions != 100 {
			t.Fatalf("%d sessions are kept after 100 logins and a logout, want 100", sessions)
		}
		time.Sleep(12 * time.Hour)
		newSession(t, c)
		if _, sessions := c.Kept(); sessions != 1 {
			t.Errorf("%d sessions are kept after 12 hours, want the new one", sessions)
		}
	})
}

func TestEveryByteOfASessionIDIsRandom(t *testing.T) {
	c := console.New(cluster)
	var seen [32]map[byte]bool
	for i := range seen {
		seen[i] = map[byte]bool{}
	}
	for range 1000 {
		secret, err := base64.RawURLEncoding.DecodeString(newSession(t, c).Value)
		if err != nil || len(secret) != len(seen) {
			t.Fatalf("a session ID decodes to %d bytes, %v; want 32", len(secret), err)
		}
		for i, b := range secret {
			seen[i][b] = true
		}
	}
	for i, values := range seen {
		if len(values) < 200 {
			t.Errorf("byte %d of 1000 session IDs took %d values, want about 250", i, len(values))
		}
	}
}
