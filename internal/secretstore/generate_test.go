package secretstore_test

import (
	"encoding/base64"
	"encoding/hex"
	"regexp"
	"strings"
	"testing"

	"github.com/moostackhq/moostack/internal/secretstore"
)

func TestGeneratePasswordDefaultsTo32LettersAndDigits(t *testing.T) {
	v := newValue(t, secretstore.Generate{Type: "password"})
	if !regexp.MustCompile(`^[A-Za-z0-9]{32}$`).MatchString(v) {
		t.Errorf("password %q: want 32 letters and digits", v)
	}
}

func TestGeneratePasswordTakesLengthAndCharset(t *testing.T) {
	v := newValue(t, secretstore.Generate{Type: "password", Length: 50, Charset: "ab!é"})
	if !regexp.MustCompile(`^[ab!é]{50}$`).MatchString(v) {
		t.Errorf("password %q: want 50 characters of ab!é", v)
	}
}

func TestGeneratePasswordsDiffer(t *testing.T) {
	g := secretstore.Generate{Type: "password"}
	if newValue(t, g) == newValue(t, g) {
		t.Error("two passwords are the same")
	}
}

func TestGenerateHexAndBase64TakeLengthInBytes(t *testing.T) {
	tests := []struct {
		g      secretstore.Generate
		decode func(string) ([]byte, error)
		bytes  int
	}{
		{secretstore.Generate{Type: "hex"}, hex.DecodeString, 32},
		{secretstore.Generate{Type: "hex", Length: 5}, hex.DecodeString, 5},
		{secretstore.Generate{Type: "base64"}, base64.StdEncoding.DecodeString, 32},
		{secretstore.Generate{Type: "base64", Length: 7}, base64.StdEncoding.DecodeString, 7},
	}
	for _, tt := range tests {
		v := newValue(t, tt.g)
		raw, err := tt.decode(v)
		if err != nil || len(raw) != tt.bytes {
			t.Errorf("%+v gave %q: want %d bytes, got %d (%v)", tt.g, v, tt.bytes, len(raw), err)
		}
	}
}

func TestGenerateUUIDIsVersion4(t *testing.T) {
	v := newValue(t, secretstore.Generate{Type: "uuid"})
	if !regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`).MatchString(v) {
		t.Errorf("uuid %q isn't a v4 UUID", v)
	}
}

func TestGenerateRefusesBadSettings(t *testing.T) {
	tests := []struct {
		g    secretstore.Generate
		want string
	}{
		{secretstore.Generate{}, "needs a type"},
		{secretstore.Generate{Type: "pin"}, `unknown generator type "pin"`},
		{secretstore.Generate{Type: "password", Length: -1}, "length"},
		{secretstore.Generate{Type: "password", Charset: "aaa"}, "at least 2 different characters"},
		{secretstore.Generate{Type: "hex", Charset: "abc"}, "charset"},
		{secretstore.Generate{Type: "uuid", Length: 16}, "length"},
	}
	for _, tt := range tests {
		err := tt.g.Check()
		if err == nil || !strings.Contains(err.Error(), tt.want) {
			t.Errorf("%+v: got %v, want an error containing %q", tt.g, err, tt.want)
		}
		if _, err := tt.g.NewValue(); err == nil {
			t.Errorf("%+v: NewValue succeeded", tt.g)
		}
	}
}

// newValue generates a value with g, which must be valid.
func newValue(t *testing.T, g secretstore.Generate) string {
	t.Helper()
	if err := g.Check(); err != nil {
		t.Fatal(err)
	}
	v, err := g.NewValue()
	if err != nil {
		t.Fatal(err)
	}
	return v
}
