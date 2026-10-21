package secretstore_test

import (
	"errors"
	"slices"
	"testing"

	"github.com/moostackhq/moostack/internal/secretstore"
)

// rotationSecrets are a marked and an unmarked generated secret, and a
// marked secret without generate settings.
var rotationSecrets = map[string]secretstore.Secret{
	"marked":   {Value: "m", Generate: &secretstore.Generate{Type: "hex", Length: 4}, Rotate: true},
	"unmarked": {Value: "u", Generate: &secretstore.Generate{Type: "uuid"}},
	"typed":    {Value: "t", Rotate: true},
}

// breakValue makes the value of the secret called name in the store at
// path impossible to decrypt.
func breakValue(t *testing.T, path, name string) {
	t.Helper()
	f := readStoreFile(t, path)
	f.Secrets[secretIndex(t, f, name)].Value = "enc:v1:0000000000000000:AAAA"
	writeStoreFile(t, path, f)
}

func TestRotateByMarkRotatesMarkedGeneratedSecrets(t *testing.T) {
	alice := newOperator(t, "alice")
	s := open(t, initStore(t, alice, rotationSecrets), alice)

	rotated, byHand, err := s.Rotate()

	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(rotated, []string{"marked"}) || !slices.Equal(byHand, []string{"typed"}) {
		t.Errorf("rotated %v, by hand %v; want [marked], [typed]", rotated, byHand)
	}
	got, err := s.Get("marked")
	if err != nil || len(got.Value) != 8 || got.Value == "m" || got.Rotate {
		t.Errorf("marked is %+v, %v", got, err)
	}
	if v := value(t, s, "unmarked"); v != "u" {
		t.Errorf("unmarked is %q", v)
	}
}

func TestRotateByMarkSkipsUnreadableUnmarkedSecrets(t *testing.T) {
	alice := newOperator(t, "alice")
	path := initStore(t, alice, rotationSecrets)
	breakValue(t, path, "unmarked")
	s := open(t, path, alice)

	rotated, _, err := s.Rotate()

	if err != nil || !slices.Equal(rotated, []string{"marked"}) {
		t.Errorf("rotated %v, %v; want [marked]", rotated, err)
	}
}

func TestRotateByNameRotatesEachOnce(t *testing.T) {
	alice := newOperator(t, "alice")
	s := open(t, initStore(t, alice, rotationSecrets), alice)

	rotated, byHand, err := s.Rotate("unmarked", "unmarked")

	if err != nil || !slices.Equal(rotated, []string{"unmarked"}) || byHand != nil {
		t.Errorf("rotated %v, by hand %v, %v; want [unmarked]", rotated, byHand, err)
	}
	if v := value(t, s, "marked"); v != "m" {
		t.Errorf("marked is %q", v)
	}
}

func TestRotateRefusesANamedSecretWithoutGenerateSettings(t *testing.T) {
	alice := newOperator(t, "alice")
	s := open(t, initStore(t, alice, rotationSecrets), alice)

	_, _, err := s.Rotate("marked", "typed")

	wantErr(t, err, `"typed"`, "by hand")
	if v := value(t, s, "marked"); v != "m" {
		t.Errorf("marked was rotated: %q", v)
	}
}

func TestSettingsCanBeReadWhenTheValueCant(t *testing.T) {
	alice := newOperator(t, "alice")
	path := initStore(t, alice, rotationSecrets)
	breakValue(t, path, "marked")
	s := open(t, path, alice)

	generate, rotate, err := s.Settings("marked")

	if err != nil || !rotate || generate == nil || *generate != (secretstore.Generate{Type: "hex", Length: 4}) {
		t.Errorf("settings are %+v, %v, %v", generate, rotate, err)
	}
	if _, _, err := s.Settings("nope"); !errors.Is(err, secretstore.ErrNotFound) {
		t.Errorf("settings of a missing secret: %v", err)
	}
}

func TestRemoveRecipientReturnsTheSecretsItMarked(t *testing.T) {
	alice, bob := newOperator(t, "alice"), newOperator(t, "bob")
	s := open(t, initStore(t, alice, rotationSecrets), alice)
	if err := s.AddRecipient(bob.recipient); err != nil {
		t.Fatal(err)
	}

	marked, err := s.RemoveRecipient(bob.recipient.Key)

	if err != nil || !slices.Equal(marked, []string{"unmarked"}) {
		t.Errorf("marked %v, %v; want [unmarked]", marked, err)
	}
	if got := s.MarkedForRotation(); !slices.Equal(got, []string{"marked", "typed", "unmarked"}) {
		t.Errorf("marked for rotation: %v", got)
	}
}
