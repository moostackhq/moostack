package console

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base32"
	"maps"
	"strings"
	"sync"
	"time"
)

const tokenLife = 5 * time.Minute

var tokenEncoding = base32.StdEncoding.WithPadding(base32.NoPadding)

// isToken reports whether s has the form of a login token.
func isToken(s string) bool {
	secret, err := tokenEncoding.DecodeString(s)
	return err == nil && len(secret) == 16 && tokenEncoding.EncodeToString(secret) == s
}

// tokens holds the login tokens that were issued and not used: the hash
// of each, with the time it stops being accepted.
type tokens struct {
	mu   sync.Mutex
	ends map[[sha256.Size]byte]time.Time
}

func newTokens() *tokens {
	return &tokens{ends: map[[sha256.Size]byte]time.Time{}}
}

// issue returns a new login token: 128 random bits as 26 letters and
// digits.
func (t *tokens) issue() string {
	var secret [16]byte
	rand.Read(secret[:])
	token := tokenEncoding.EncodeToString(secret[:])

	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	maps.DeleteFunc(t.ends, func(_ [sha256.Size]byte, end time.Time) bool { return !now.Before(end) })
	t.ends[sha256.Sum256([]byte(token))] = now.Add(tokenLife)
	return token
}

// redeem uses up token. It reports false for a token that is unknown,
// used or older than tokenLife.
func (t *tokens) redeem(token string) bool {
	key := sha256.Sum256([]byte(strings.TrimSpace(token)))

	t.mu.Lock()
	defer t.mu.Unlock()
	end, ok := t.ends[key]
	delete(t.ends, key)
	return ok && time.Now().Before(end)
}
