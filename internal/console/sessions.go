package console

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"maps"
	"net/http"
	"sync"
	"time"
)

const sessionLife = 12 * time.Hour

const sessionCookie = "__Secure-console"

// sessions holds the sessions: the hash of each one's ID, with the time
// it ends.
type sessions struct {
	mu   sync.Mutex
	ends map[[sha256.Size]byte]time.Time
}

func newSessions() *sessions {
	return &sessions{ends: map[[sha256.Size]byte]time.Time{}}
}

// start returns the ID of a new session: 256 random bits.
func (s *sessions) start() string {
	var secret [32]byte
	rand.Read(secret[:])
	id := base64.RawURLEncoding.EncodeToString(secret[:])

	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	maps.DeleteFunc(s.ends, func(_ [sha256.Size]byte, end time.Time) bool { return !now.Before(end) })
	s.ends[sha256.Sum256([]byte(id))] = now.Add(sessionLife)
	return id
}

// active reports whether id is a session that hasn't ended.
func (s *sessions) active(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	end, ok := s.ends[sha256.Sum256([]byte(id))]
	return ok && time.Now().Before(end)
}

func (s *sessions) end(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.ends, sha256.Sum256([]byte(id)))
}

// hasSession reports whether one of the request's session cookies is a
// session that hasn't ended.
func (c *Console) hasSession(r *http.Request) bool {
	for _, cookie := range r.CookiesNamed(sessionCookie) {
		if c.sessions.active(cookie.Value) {
			return true
		}
	}
	return false
}

// setSessionCookie sets the session's cookie to id for maxAge seconds;
// a negative maxAge removes it.
func setSessionCookie(w http.ResponseWriter, id string, maxAge int) {
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookie,
		Value:    id,
		Path:     "/console",
		MaxAge:   maxAge,
		Secure:   true,
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
	})
}
