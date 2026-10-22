package controlplanev1_test

import (
	"encoding/base64"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	controlplanev1 "github.com/moostackhq/moostack/api/controlplane/v1"
)

const fingerprint = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestATokenReadsBackAsItWasWritten(t *testing.T) {
	for _, address := range []string{"cp.example.com:7444", "192.0.2.1:443", "[2001:db8::1]:7444"} {
		want := &controlplanev1.JoinToken{Address: address, CaFingerprint: fingerprint, Secret: "s3cret"}
		token := encodeToken(t, want)
		if !strings.HasPrefix(token, "moostack-join:") {
			t.Errorf("the token %q doesn't start with moostack-join:", token)
		}
		got, err := controlplanev1.ParseToken(token)
		if err != nil {
			t.Fatalf("parsing %q: %v", token, err)
		}
		if !proto.Equal(got, want) {
			t.Errorf("parsed %v, want %v", got, want)
		}
	}
}

// encode writes a token of tok's fields, as a control plane would.
func encode(tok *controlplanev1.JoinToken) string {
	data, err := proto.Marshal(tok)
	if err != nil {
		panic(err)
	}
	return "moostack-join:" + base64.RawURLEncoding.EncodeToString(data)
}

func TestParseTokenRefusesABadToken(t *testing.T) {
	good := func() *controlplanev1.JoinToken {
		return &controlplanev1.JoinToken{Address: "cp.example.com:7444", CaFingerprint: fingerprint, Secret: "s3cret"}
	}
	with := func(edit func(*controlplanev1.JoinToken)) string {
		tok := good()
		edit(tok)
		return encode(tok)
	}
	for _, c := range []struct{ name, token, want string }{
		{"empty", "", "moostack-join:"},
		{"no prefix", strings.TrimPrefix(encode(good()), "moostack-join:"), "moostack-join:"},
		{"padded base64", encode(good()) + "=", "base64"},
		{"not base64", "moostack-join:!!!", "base64"},
		{"not protobuf", "moostack-join:" + base64.RawURLEncoding.EncodeToString([]byte{0xff, 0xff}), "join token"},
		{"no address", with(func(t *controlplanev1.JoinToken) { t.Address = "" }), "address"},
		{"no port", with(func(t *controlplanev1.JoinToken) { t.Address = "cp.example.com" }), "address"},
		{"no host", with(func(t *controlplanev1.JoinToken) { t.Address = ":7444" }), "address"},
		{"bad port", with(func(t *controlplanev1.JoinToken) { t.Address = "cp.example.com:http" }), "address"},
		{"port 0", with(func(t *controlplanev1.JoinToken) { t.Address = "cp.example.com:0" }), "address"},
		{"no fingerprint", with(func(t *controlplanev1.JoinToken) { t.CaFingerprint = "" }), "fingerprint"},
		{"fingerprint without sha256:", with(func(t *controlplanev1.JoinToken) { t.CaFingerprint = strings.TrimPrefix(fingerprint, "sha256:") }), "fingerprint"},
		{"short fingerprint", with(func(t *controlplanev1.JoinToken) { t.CaFingerprint = fingerprint[:len(fingerprint)-1] }), "fingerprint"},
		{"upper-case fingerprint", with(func(t *controlplanev1.JoinToken) { t.CaFingerprint = strings.ToUpper(fingerprint) }), "fingerprint"},
		{"no secret", with(func(t *controlplanev1.JoinToken) { t.Secret = "" }), "secret"},
	} {
		_, err := controlplanev1.ParseToken(c.token)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: ParseToken(%q) = %v, want an error about %s", c.name, c.token, err, c.want)
		}
	}
}

// Any token ParseToken reads reads back the same once encoded again, and
// nothing makes it panic.
func FuzzParseToken(f *testing.F) {
	f.Add(encodeToken(f, &controlplanev1.JoinToken{Address: "cp.example.com:7444", CaFingerprint: fingerprint, Secret: "s3cret"}))
	f.Add(encodeToken(f, &controlplanev1.JoinToken{Address: "[2001:db8::1]:443", CaFingerprint: fingerprint, Secret: "x"}))
	f.Add("moostack-join:")
	f.Add("moostack-join:!!!")
	f.Add("")
	f.Fuzz(func(t *testing.T, token string) {
		tok, err := controlplanev1.ParseToken(token)
		if err != nil {
			return
		}
		again, err := controlplanev1.ParseToken(encodeToken(t, tok))
		if err != nil || !proto.Equal(again, tok) {
			t.Errorf("%q parsed as %v, which reads back as %v, %v", token, tok, again, err)
		}
	})
}

// encodeToken is tok encoded, failing t if it can't be.
func encodeToken(t testing.TB, tok *controlplanev1.JoinToken) string {
	t.Helper()
	token, err := controlplanev1.EncodeToken(tok)
	if err != nil {
		t.Fatal(err)
	}
	return token
}

func TestCheckAddress(t *testing.T) {
	for addr, want := range map[string]string{
		"cp.example.com:7444":  "",
		"[2001:db8::1]:443":    "",
		"[fe80::1%eth0]:7444":  "",
		"\x94:1":               "UTF-8",
		"\x1b:1":               "control character",
		"a b:1":                "space",
		" x:1":                 "space",
		"cp.example.com":       "port",
		":7444":                "no host",
		"cp.example.com:0":     "bad port",
		"cp.example.com:65536": "bad port",
		"cp.example.com:http":  "bad port",
	} {
		err := controlplanev1.CheckAddress(addr)
		switch {
		case want == "" && err != nil:
			t.Errorf("CheckAddress(%q) = %v, want it taken", addr, err)
		case want != "" && (err == nil || !strings.Contains(err.Error(), want)):
			t.Errorf("CheckAddress(%q) = %v, want an error about %s", addr, err, want)
		}
	}
}
