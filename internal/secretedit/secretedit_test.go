//go:build unix

package secretedit_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"filippo.io/age/agessh"
	"golang.org/x/crypto/ssh"

	"github.com/moostackhq/moostack/internal/secretedit"
	"github.com/moostackhq/moostack/internal/secretstore"
)

// newStore makes a store with secrets, and returns it opened.
func newStore(t *testing.T, secrets map[string]secretstore.Secret) *secretstore.Store {
	t.Helper()
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	pub, err := ssh.NewPublicKey(private.Public())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), secretstore.FileName)
	s, err := secretstore.Init(path, secretstore.Recipient{Key: pub, Comment: "alice"})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range slices.Sorted(maps.Keys(secrets)) {
		if err := s.Put(name, secrets[name]); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Save(); err != nil {
		t.Fatal(err)
	}
	id, err := agessh.NewEd25519Identity(private)
	if err != nil {
		t.Fatal(err)
	}
	if secrets != nil {
		// Reopen, so that values written by hand below are read.
		breakValues(t, path, secrets)
	}
	s, err = secretstore.Open(path, id)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// breakValues makes the value of every secret whose value is "broken"
// impossible to decrypt.
func breakValues(t *testing.T, path string, secrets map[string]secretstore.Secret) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for name, secret := range secrets {
		if secret.Value != "broken" {
			continue
		}
		re := regexp.MustCompile(`(?m)(^  - name: ` + regexp.QuoteMeta(name) + `\n(?:    .*\n)*?    value: ).*$`)
		data = re.ReplaceAll(data, []byte("${1}enc:v1:0000000000000000:AAAA"))
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

// edit runs Edit on s, with an editor that replaces the file with text.
// There is no terminal, so an edit that isn't valid fails.
func edit(t *testing.T, s *secretstore.Store, text string) (secretedit.Result, error) {
	t.Helper()
	dir := t.TempDir()
	src := filepath.Join(dir, "text")
	if err := os.WriteFile(src, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(dir, "editor")
	if err := os.WriteFile(script, []byte("#!/bin/sh\ncat '"+src+"' > \"$1\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("VISUAL", "")
	t.Setenv("EDITOR", script)
	return secretedit.Edit(s, secretedit.Options{Errors: &strings.Builder{}, Exit: func(int) { t.Error("Edit exited") }})
}

// wantRefused fails unless err is an error containing want and s is
// unchanged, holding only names.
func wantRefused(t *testing.T, err error, want string, s *secretstore.Store, names ...string) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("got %v, want an error containing %q", err, want)
	}
	if got := s.Names(); strings.Join(got, ",") != strings.Join(names, ",") {
		t.Errorf("secrets are %v, want %v", got, names)
	}
}

func TestEditRefusesASecondDocument(t *testing.T) {
	for name, text := range map[string]string{
		"after a mapping":        "a:\n  value: x\n---\nb:\n  value: y\n",
		"after an empty one":     "---\n---\nb:\n  value: y\n",
		"after an explicit null": "~\n---\nb:\n  value: y\n",
	} {
		t.Run(name, func(t *testing.T) {
			s := newStore(t, nil)

			_, err := edit(t, s, text)

			wantRefused(t, err, "one YAML document", s)
		})
	}
}

func TestEditRefusesNamesThatArentPlainStrings(t *testing.T) {
	for name, text := range map[string]string{
		"merge key": "<<: {b: {value: x}}\n",
		"null":      "~: {value: x}\n",
		"number":    "123: {value: x}\n",
		"list":      "? [x]\n: {value: y}\n",
	} {
		t.Run(name, func(t *testing.T) {
			s := newStore(t, nil)

			_, err := edit(t, s, text)

			wantRefused(t, err, "plain name", s)
		})
	}
}

func TestEditTakesAQuotedName(t *testing.T) {
	s := newStore(t, nil)

	if _, err := edit(t, s, "\"123\": {value: x}\n\"<<\": {value: y}\n"); err != nil {
		t.Fatal(err)
	}

	if got := s.Names(); strings.Join(got, ",") != "123,<<" {
		t.Errorf("secrets are %v", got)
	}
}

func TestEditRefusesRemovingEveryReadableSecret(t *testing.T) {
	s := newStore(t, map[string]secretstore.Secret{"a": {Value: "one"}, "b": {Value: "broken"}})

	_, err := edit(t, s, "# nothing\n")

	wantRefused(t, err, "leaves no secret that can be read", s, "a", "b")
}

func TestEditRefusesRemovingTheLastReadableSecretBesideABrokenOne(t *testing.T) {
	s := newStore(t, map[string]secretstore.Secret{"ca": {Value: "key"}, "b": {Value: "broken"}})

	_, err := edit(t, s, "b:\n  value: \"\"\n")

	wantRefused(t, err, "leaves no secret that can be read", s, "b", "ca")
}

func TestEditAcceptsATrailingEmptyDocument(t *testing.T) {
	s := newStore(t, nil)

	if _, err := edit(t, s, "a:\n  value: x\n---\n"); err != nil {
		t.Fatal(err)
	}

	if got := s.Names(); len(got) != 1 || got[0] != "a" {
		t.Errorf("secrets are %v", got)
	}
}

func TestEditRemovesTheLastUnreadableSecret(t *testing.T) {
	s := newStore(t, map[string]secretstore.Secret{"b": {Value: "broken"}})

	result, err := edit(t, s, "# nothing\n")

	if err != nil || !result.Changed {
		t.Fatalf("got %+v, %v; want b removed", result, err)
	}
	if got := s.Names(); len(got) != 0 {
		t.Errorf("secrets are %v, want none", got)
	}
}

func TestViewFailsOnAnUnreadableValue(t *testing.T) {
	s := newStore(t, map[string]secretstore.Secret{"a": {Value: "one"}, "b": {Value: "broken"}})

	_, err := secretedit.View(s)

	if !errors.Is(err, secretedit.ErrUnreadable) || !strings.Contains(err.Error(), `secret "b"`) {
		t.Errorf("got %v, want ErrUnreadable naming b", err)
	}
}
