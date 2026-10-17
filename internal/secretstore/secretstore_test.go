package secretstore_test

import (
	"bytes"
	"crypto"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"filippo.io/age"
	"filippo.io/age/agessh"
	"filippo.io/age/armor"
	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/crypto/ssh"
	"gopkg.in/yaml.v3"

	"github.com/moostackhq/moostack/internal/secretstore"
)

// operator is a recipient and the identity that reads the store as them.
type operator struct {
	recipient secretstore.Recipient
	identity  age.Identity
}

// newOperator makes an operator with a new ed25519 key.
func newOperator(t *testing.T, comment string) operator {
	t.Helper()
	return newKeyOperator(t, newEd25519(t), comment)
}

// newKeyOperator makes an operator with key, an ed25519 or RSA key.
func newKeyOperator(t *testing.T, key crypto.Signer, comment string) operator {
	t.Helper()
	pub, err := ssh.NewPublicKey(key.Public())
	if err != nil {
		t.Fatal(err)
	}
	var id age.Identity
	switch key := key.(type) {
	case ed25519.PrivateKey:
		id, err = agessh.NewEd25519Identity(key)
	case *rsa.PrivateKey:
		id, err = agessh.NewRSAIdentity(key)
	default:
		t.Fatalf("no identity for a %T", key)
	}
	if err != nil {
		t.Fatal(err)
	}
	return operator{recipient: secretstore.Recipient{Key: pub, Comment: comment}, identity: id}
}

// storePath is a path for a new store in a temporary directory.
func storePath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "secrets.enc.yml")
}

func TestWrittenSecretReadsBack(t *testing.T) {
	alice := newOperator(t, "alice@laptop")
	path := storePath(t)
	s, err := secretstore.Init(path, alice.recipient)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Put("db-password", secretstore.Secret{Value: "hunter2"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(); err != nil {
		t.Fatal(err)
	}

	s, err = secretstore.Open(path, alice.identity)
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.Get("db-password")
	if err != nil {
		t.Fatal(err)
	}
	if got.Value != "hunter2" {
		t.Errorf("value = %q, want %q", got.Value, "hunter2")
	}
}

// initStore makes a store at a temporary path with owner as its only
// recipient, holding secrets, and saves it.
func initStore(t *testing.T, owner operator, secrets map[string]secretstore.Secret) string {
	t.Helper()
	path := storePath(t)
	s, err := secretstore.Init(path, owner.recipient)
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
	return path
}

// open opens the store at path as op.
func open(t *testing.T, path string, op operator) *secretstore.Store {
	t.Helper()
	s, err := secretstore.Open(path, op.identity)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// save saves s.
func save(t *testing.T, s *secretstore.Store) {
	t.Helper()
	if err := s.Save(); err != nil {
		t.Fatal(err)
	}
}

// value gets the value of the secret called name.
func value(t *testing.T, s *secretstore.Store, name string) string {
	t.Helper()
	got, err := s.Get(name)
	if err != nil {
		t.Fatal(err)
	}
	return got.Value
}

// storeFile is the store's file, decoded as it is written.
type storeFile struct {
	Version    int      `yaml:"version"`
	Recipients []string `yaml:"recipients"`
	DataKey    string   `yaml:"data_key"`
	Secrets    []struct {
		Name     string         `yaml:"name"`
		Generate map[string]any `yaml:"generate,omitempty"`
		Rotate   bool           `yaml:"rotate,omitempty"`
		Value    string         `yaml:"value"`
	} `yaml:"secrets"`
}

// readStoreFile reads the store's file at path.
func readStoreFile(t *testing.T, path string) storeFile {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var f storeFile
	if err := yaml.Unmarshal(data, &f); err != nil {
		t.Fatal(err)
	}
	return f
}

// writeStoreFile writes f as the store's file at path.
func writeStoreFile(t *testing.T, path string, f storeFile) {
	t.Helper()
	data, err := yaml.Marshal(f)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

// secretIndex is the index of the secret called name in f.
func secretIndex(t *testing.T, f storeFile, name string) int {
	t.Helper()
	for i, s := range f.Secrets {
		if s.Name == name {
			return i
		}
	}
	t.Fatalf("no secret %q in the file", name)
	return -1
}

// replaceDataKey has owner add leaving as a recipient of the store at
// path, then remove them again, which replaces the data key. It returns
// the file from between the two, whose values are under the old data key.
func replaceDataKey(t *testing.T, path string, owner, leaving operator) (old storeFile) {
	t.Helper()
	s := open(t, path, owner)
	if err := s.AddRecipient(leaving.recipient); err != nil {
		t.Fatal(err)
	}
	save(t, s)
	old = readStoreFile(t, path)
	s = open(t, path, owner)
	if _, err := s.RemoveRecipient(leaving.recipient.Key); err != nil {
		t.Fatal(err)
	}
	save(t, s)
	return old
}

// wantErr fails unless err is an error whose message has every part.
func wantErr(t *testing.T, err error, parts ...string) {
	t.Helper()
	if err == nil {
		t.Fatalf("err = nil, want an error with %q", parts)
	}
	for _, p := range parts {
		if !strings.Contains(err.Error(), p) {
			t.Errorf("err = %q, want it to contain %q", err, p)
		}
	}
}

func TestOverwrittenSecretReadsNewValue(t *testing.T) {
	alice := newOperator(t, "alice")
	path := initStore(t, alice, map[string]secretstore.Secret{"token": {Value: "old"}})
	s := open(t, path, alice)
	if err := s.Put("token", secretstore.Secret{Value: "new"}); err != nil {
		t.Fatal(err)
	}
	save(t, s)

	if got := value(t, open(t, path, alice), "token"); got != "new" {
		t.Errorf("value = %q, want %q", got, "new")
	}
}

func TestDeletedSecretIsGone(t *testing.T) {
	alice := newOperator(t, "alice")
	path := initStore(t, alice, map[string]secretstore.Secret{"a": {Value: "1"}, "b": {Value: "2"}})
	s := open(t, path, alice)
	if err := s.Delete("a"); err != nil {
		t.Fatal(err)
	}
	save(t, s)

	s = open(t, path, alice)
	if _, err := s.Get("a"); !errors.Is(err, secretstore.ErrNotFound) {
		t.Errorf("Get(a) err = %v, want ErrNotFound", err)
	}
	if got := value(t, s, "b"); got != "2" {
		t.Errorf("b = %q, want %q", got, "2")
	}
	if names := s.Names(); !slices.Equal(names, []string{"b"}) {
		t.Errorf("names = %q, want [b]", names)
	}
}

func TestMissingSecretIsNotFound(t *testing.T) {
	alice := newOperator(t, "alice")
	s := open(t, initStore(t, alice, nil), alice)
	if _, err := s.Get("nope"); !errors.Is(err, secretstore.ErrNotFound) {
		t.Errorf("Get err = %v, want ErrNotFound", err)
	}
	if err := s.Delete("nope"); !errors.Is(err, secretstore.ErrNotFound) {
		t.Errorf("Delete err = %v, want ErrNotFound", err)
	}
}

func TestGenerateSettingsReadBack(t *testing.T) {
	alice := newOperator(t, "alice")
	want := secretstore.Secret{Value: "v", Generate: &secretstore.Generate{Type: "password", Length: 20, Charset: "abc"}, Rotate: true}
	path := initStore(t, alice, map[string]secretstore.Secret{"pw": want})
	got, err := open(t, path, alice).Get("pw")
	if err != nil {
		t.Fatal(err)
	}
	if got.Value != want.Value || got.Rotate != want.Rotate || got.Generate == nil || *got.Generate != *want.Generate {
		t.Errorf("got %+v (generate %+v), want %+v (generate %+v)", got, got.Generate, want, want.Generate)
	}
}

func TestInitRefusesExistingFile(t *testing.T) {
	alice := newOperator(t, "alice")
	path := initStore(t, alice, map[string]secretstore.Secret{"keep": {Value: "me"}})
	before, _ := os.ReadFile(path)

	_, err := secretstore.Init(path, alice.recipient)
	wantErr(t, err, "already exists")
	if !errors.Is(err, os.ErrExist) {
		t.Errorf("err = %v, want os.ErrExist", err)
	}
	after, _ := os.ReadFile(path)
	if !bytes.Equal(before, after) {
		t.Error("Init changed the existing file")
	}
}

func TestWrongKeyIsNotARecipient(t *testing.T) {
	alice, mallory := newOperator(t, "alice"), newOperator(t, "mallory")
	path := initStore(t, alice, map[string]secretstore.Secret{"a": {Value: "1"}})
	_, err := secretstore.Open(path, mallory.identity)
	if !errors.Is(err, secretstore.ErrNotRecipient) {
		t.Errorf("err = %v, want ErrNotRecipient", err)
	}
}

func TestSaveLeavesNoTempFiles(t *testing.T) {
	alice := newOperator(t, "alice")
	path := initStore(t, alice, map[string]secretstore.Secret{"a": {Value: "1"}})
	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	if !slices.Equal(names, []string{secretstore.LockFileName, filepath.Base(path)}) {
		t.Errorf("directory holds %v, want only the store and its lock", names)
	}
}

func TestTamperedCiphertextFailsNamingSecret(t *testing.T) {
	alice := newOperator(t, "alice")
	path := initStore(t, alice, map[string]secretstore.Secret{"token": {Value: "secret value"}})
	f := readStoreFile(t, path)
	i := secretIndex(t, f, "token")
	v := []byte(f.Secrets[i].Value)
	// Change a base64 character in the middle of the ciphertext.
	if v[len(v)-10] == 'A' {
		v[len(v)-10] = 'B'
	} else {
		v[len(v)-10] = 'A'
	}
	f.Secrets[i].Value = string(v)
	writeStoreFile(t, path, f)

	_, err := open(t, path, alice).Get("token")
	wantErr(t, err, `"token"`, "changed without the key")
}

func TestSwappedCiphertextFailsNamingSecret(t *testing.T) {
	alice := newOperator(t, "alice")
	path := initStore(t, alice, map[string]secretstore.Secret{"admin": {Value: "admin-pw"}, "guest": {Value: "guest-pw"}})
	f := readStoreFile(t, path)
	a, g := secretIndex(t, f, "admin"), secretIndex(t, f, "guest")
	f.Secrets[a].Value = f.Secrets[g].Value
	writeStoreFile(t, path, f)

	_, err := open(t, path, alice).Get("admin")
	wantErr(t, err, `"admin"`, "changed without the key")
}

func TestTamperedGenerateSettingsFailNamingSecret(t *testing.T) {
	tests := []struct {
		name   string
		tamper func(map[string]any) map[string]any
	}{
		{"weaker length", func(g map[string]any) map[string]any { g["length"] = 4; return g }},
		{"other type", func(g map[string]any) map[string]any { g["type"] = "uuid"; return g }},
		{"charset added", func(g map[string]any) map[string]any { g["charset"] = "a"; return g }},
		{"removed", func(map[string]any) map[string]any { return nil }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			alice := newOperator(t, "alice")
			path := initStore(t, alice, map[string]secretstore.Secret{
				"pw": {Value: "v", Generate: &secretstore.Generate{Type: "password", Length: 32}},
			})
			f := readStoreFile(t, path)
			i := secretIndex(t, f, "pw")
			f.Secrets[i].Generate = tt.tamper(f.Secrets[i].Generate)
			writeStoreFile(t, path, f)

			_, err := open(t, path, alice).Get("pw")
			wantErr(t, err, `"pw"`, "changed without the key")
		})
	}
}

func TestTamperedRotateFlagFailsNamingSecret(t *testing.T) {
	alice := newOperator(t, "alice")
	path := initStore(t, alice, map[string]secretstore.Secret{
		"pw": {Value: "v", Generate: &secretstore.Generate{Type: "hex"}, Rotate: true},
	})
	f := readStoreFile(t, path)
	f.Secrets[secretIndex(t, f, "pw")].Rotate = false
	writeStoreFile(t, path, f)

	_, err := open(t, path, alice).Get("pw")
	wantErr(t, err, `"pw"`, "changed without the key")
}

func TestTamperedRecipientListFailsToOpen(t *testing.T) {
	alice, mallory := newOperator(t, "alice"), newOperator(t, "mallory")
	tests := []struct {
		name   string
		tamper func([]string) []string
	}{
		{"recipient added", func(r []string) []string { return append(r, mallory.recipient.String()) }},
		{"recipient replaced", func([]string) []string { return []string{mallory.recipient.String(), alice.recipient.String()} }},
		{"comment changed", func(r []string) []string { return []string{strings.Replace(r[0], "alice", "bob", 1)} }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := initStore(t, alice, map[string]secretstore.Secret{"a": {Value: "1"}})
			f := readStoreFile(t, path)
			f.Recipients = tt.tamper(f.Recipients)
			writeStoreFile(t, path, f)

			_, err := secretstore.Open(path, alice.identity)
			wantErr(t, err, "may have been tampered with")
		})
	}
}

func TestUnchangedValuesKeepTheirCiphertext(t *testing.T) {
	alice := newOperator(t, "alice")
	gen := &secretstore.Generate{Type: "password"}
	path := initStore(t, alice, map[string]secretstore.Secret{"a": {Value: "1"}, "b": {Value: "2", Generate: gen}})
	before, _ := os.ReadFile(path)

	s := open(t, path, alice)
	if err := s.Put("a", secretstore.Secret{Value: "1"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("b", secretstore.Secret{Value: "2", Generate: &secretstore.Generate{Type: "password"}}); err != nil {
		t.Fatal(err)
	}
	save(t, s)

	after, _ := os.ReadFile(path)
	if !bytes.Equal(before, after) {
		t.Errorf("file changed:\n%s\nwant:\n%s", after, before)
	}
}

func TestChangedValueKeepsOthersCiphertext(t *testing.T) {
	alice := newOperator(t, "alice")
	path := initStore(t, alice, map[string]secretstore.Secret{"a": {Value: "1"}, "b": {Value: "2"}})
	before := readStoreFile(t, path)

	s := open(t, path, alice)
	if err := s.Put("b", secretstore.Secret{Value: "3"}); err != nil {
		t.Fatal(err)
	}
	save(t, s)

	after := readStoreFile(t, path)
	if after.Secrets[secretIndex(t, after, "a")].Value != before.Secrets[secretIndex(t, before, "a")].Value {
		t.Error("a's ciphertext changed")
	}
	if after.Secrets[secretIndex(t, after, "b")].Value == before.Secrets[secretIndex(t, before, "b")].Value {
		t.Error("b's ciphertext didn't change")
	}
	if after.DataKey != before.DataKey {
		t.Error("the data key changed")
	}
}

func TestAddedRecipientCanRead(t *testing.T) {
	alice, bob := newOperator(t, "alice"), newOperator(t, "bob")
	path := initStore(t, alice, map[string]secretstore.Secret{"a": {Value: "1"}})
	s := open(t, path, alice)
	if err := s.AddRecipient(bob.recipient); err != nil {
		t.Fatal(err)
	}
	save(t, s)

	if got := value(t, open(t, path, bob), "a"); got != "1" {
		t.Errorf("bob reads %q, want %q", got, "1")
	}
	if got := value(t, open(t, path, alice), "a"); got != "1" {
		t.Errorf("alice reads %q, want %q", got, "1")
	}
}

func TestAddingRecipientReencryptsOnlyTheDataKey(t *testing.T) {
	alice, bob := newOperator(t, "alice"), newOperator(t, "bob")
	path := initStore(t, alice, map[string]secretstore.Secret{"a": {Value: "1"}, "b": {Value: "2"}})
	before := readStoreFile(t, path)

	s := open(t, path, alice)
	if err := s.AddRecipient(bob.recipient); err != nil {
		t.Fatal(err)
	}
	save(t, s)

	after := readStoreFile(t, path)
	if !reflect.DeepEqual(after.Secrets, before.Secrets) {
		t.Errorf("secrets changed:\n%+v\nwant:\n%+v", after.Secrets, before.Secrets)
	}
	if after.DataKey == before.DataKey {
		t.Error("the data key wasn't encrypted again")
	}
	want := []string{alice.recipient.String(), bob.recipient.String()}
	if !slices.Equal(after.Recipients, want) {
		t.Errorf("recipients = %q, want %q", after.Recipients, want)
	}
}

func TestAddingExistingRecipientFails(t *testing.T) {
	alice := newOperator(t, "alice")
	s := open(t, initStore(t, alice, nil), alice)
	// The same key with another comment is the same recipient.
	again := secretstore.Recipient{Key: alice.recipient.Key, Comment: "alice@other"}
	wantErr(t, s.AddRecipient(again), "already a recipient")
}

func TestOnlyEd25519AndRSAKeysCanBeRecipients(t *testing.T) {
	alice := newOperator(t, "alice")
	s := open(t, initStore(t, alice, nil), alice)

	ecdsaPub, err := ssh.NewPublicKey(newECDSA(t).Public())
	if err != nil {
		t.Fatal(err)
	}
	wantErr(t, s.AddRecipient(secretstore.Recipient{Key: ecdsaPub}), "only ed25519 and RSA")
	_, err = secretstore.Init(storePath(t), secretstore.Recipient{Key: ecdsaPub})
	wantErr(t, err, "only ed25519 and RSA")

	if err := s.AddRecipient(newKeyOperator(t, newRSA(t), "carol").recipient); err != nil {
		t.Errorf("adding an RSA key: %v", err)
	}
}

func TestRSARecipientCanRead(t *testing.T) {
	carol := newKeyOperator(t, newRSA(t), "carol")
	path := initStore(t, carol, map[string]secretstore.Secret{"a": {Value: "1"}})
	if got := value(t, open(t, path, carol), "a"); got != "1" {
		t.Errorf("value = %q, want %q", got, "1")
	}
}

func TestRemovedRecipientCantReadValuesWrittenAfterwards(t *testing.T) {
	alice, bob := newOperator(t, "alice"), newOperator(t, "bob")
	path := initStore(t, alice, map[string]secretstore.Secret{"a": {Value: "1"}})
	// bob keeps a copy of the file from while he was a recipient.
	bobsCopy := replaceDataKey(t, path, alice, bob)
	s := open(t, path, alice)
	if err := s.Put("b", secretstore.Secret{Value: "written after"}); err != nil {
		t.Fatal(err)
	}
	save(t, s)

	if _, err := secretstore.Open(path, bob.identity); !errors.Is(err, secretstore.ErrNotRecipient) {
		t.Errorf("bob opens the store: err = %v, want ErrNotRecipient", err)
	}
	// The old data key, which bob can read, doesn't read the new value.
	now := readStoreFile(t, path)
	bobsCopy.Secrets = append(bobsCopy.Secrets, now.Secrets[secretIndex(t, now, "b")])
	writeStoreFile(t, path+".bob", bobsCopy)
	if _, err := open(t, path+".bob", bob).Get("b"); err == nil {
		t.Error("bob's old data key reads b")
	}
	if got := value(t, open(t, path, alice), "b"); got != "written after" {
		t.Errorf("alice reads %q, want %q", got, "written after")
	}
}

func TestRemovingRecipientMarksGeneratedSecretsForRotation(t *testing.T) {
	alice, bob := newOperator(t, "alice"), newOperator(t, "bob")
	gen := &secretstore.Generate{Type: "password", Length: 24}
	path := initStore(t, alice, map[string]secretstore.Secret{
		"generated": {Value: "gen-value", Generate: gen},
		"manual":    {Value: "manual-value"},
	})
	s := open(t, path, alice)
	if err := s.AddRecipient(bob.recipient); err != nil {
		t.Fatal(err)
	}
	save(t, s)
	before := readStoreFile(t, path)

	s = open(t, path, bob)
	if _, err := s.RemoveRecipient(alice.recipient.Key); err != nil {
		t.Fatal(err)
	}
	save(t, s)

	s = open(t, path, bob)
	got, err := s.Get("generated")
	if err != nil {
		t.Fatal(err)
	}
	if got.Value != "gen-value" || !got.Rotate || got.Generate == nil || *got.Generate != *gen {
		t.Errorf("generated = %+v, want its value kept and marked rotate", got)
	}
	got, err = s.Get("manual")
	if err != nil {
		t.Fatal(err)
	}
	if got.Value != "manual-value" || got.Rotate {
		t.Errorf("manual = %+v, want its value kept and not marked rotate", got)
	}
	after := readStoreFile(t, path)
	for _, name := range []string{"generated", "manual"} {
		if after.Secrets[secretIndex(t, after, name)].Value == before.Secrets[secretIndex(t, before, name)].Value {
			t.Errorf("%s wasn't encrypted again", name)
		}
	}
	if !slices.Equal(after.Recipients, []string{bob.recipient.String()}) {
		t.Errorf("recipients = %q, want only bob", after.Recipients)
	}
}

func TestRemovingLastRecipientIsRefused(t *testing.T) {
	alice := newOperator(t, "alice")
	s := open(t, initStore(t, alice, nil), alice)
	_, err := s.RemoveRecipient(alice.recipient.Key)
	wantErr(t, err, "last recipient")
}

func TestRemovingUnknownRecipientFails(t *testing.T) {
	alice, bob := newOperator(t, "alice"), newOperator(t, "bob")
	s := open(t, initStore(t, alice, nil), alice)
	_, err := s.RemoveRecipient(bob.recipient.Key)
	wantErr(t, err, "not a recipient")
}

func TestValueFromOldDataKeyFailsNamingSecret(t *testing.T) {
	// A git merge of a recipient removal on one branch with a new secret
	// on another gives a value under the old data key.
	alice, bob := newOperator(t, "alice"), newOperator(t, "bob")
	path := initStore(t, alice, map[string]secretstore.Secret{"a": {Value: "1"}})
	base := replaceDataKey(t, path, alice, bob)

	// The other branch adds a secret under the old data key.
	branchPath := path + ".branch"
	writeStoreFile(t, branchPath, base)
	s := open(t, branchPath, alice)
	if err := s.Put("added", secretstore.Secret{Value: "new"}); err != nil {
		t.Fatal(err)
	}
	save(t, s)
	branch := readStoreFile(t, branchPath)

	merged := readStoreFile(t, path)
	merged.Secrets = append(merged.Secrets, branch.Secrets[secretIndex(t, branch, "added")])
	writeStoreFile(t, path, merged)

	s = open(t, path, alice)
	_, err := s.Get("added")
	wantErr(t, err, `"added"`, "another data key", "remove it and set it again")
	if got := value(t, s, "a"); got != "1" {
		t.Errorf("a = %q, want %q", got, "1")
	}
	// It can be removed and set again.
	if err := s.Delete("added"); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("added", secretstore.Secret{Value: "new"}); err != nil {
		t.Fatal(err)
	}
	save(t, s)
	if got := value(t, open(t, path, alice), "added"); got != "new" {
		t.Errorf("added = %q, want %q", got, "new")
	}
}

func TestRemovingRecipientFailsOnValueFromOldDataKey(t *testing.T) {
	alice, bob, carol := newOperator(t, "alice"), newOperator(t, "bob"), newOperator(t, "carol")
	path := initStore(t, alice, map[string]secretstore.Secret{"a": {Value: "1"}})
	old := replaceDataKey(t, path, alice, bob)
	stale := old.Secrets[secretIndex(t, old, "a")]
	stale.Name = "merged"
	s := open(t, path, alice)
	if err := s.AddRecipient(carol.recipient); err != nil {
		t.Fatal(err)
	}
	save(t, s)
	f := readStoreFile(t, path)
	f.Secrets = append(f.Secrets, stale)
	writeStoreFile(t, path, f)

	_, err := open(t, path, alice).RemoveRecipient(carol.recipient.Key)
	wantErr(t, err, `"merged"`, "remove it and set it again")
}

func TestPutReplacesValueThatCantBeDecrypted(t *testing.T) {
	alice := newOperator(t, "alice")
	path := initStore(t, alice, map[string]secretstore.Secret{"token": {Value: "old"}})
	f := readStoreFile(t, path)
	f.Secrets[secretIndex(t, f, "token")].Rotate = true
	writeStoreFile(t, path, f)

	s := open(t, path, alice)
	if _, err := s.Get("token"); err == nil {
		t.Fatal("the tampered value reads")
	}
	if err := s.Put("token", secretstore.Secret{Value: "new"}); err != nil {
		t.Fatal(err)
	}
	save(t, s)
	if got := value(t, open(t, path, alice), "token"); got != "new" {
		t.Errorf("token = %q, want %q", got, "new")
	}
}

func TestStoreWithNoValueUnderItsDataKeyOpensAndCanBeRepaired(t *testing.T) {
	// A merge can leave every value under an older data key, for example
	// when one branch removes a recipient and the other branch's values
	// win the conflicts. The store must still open, so that mooctl can
	// remove and set them again.
	alice, bob := newOperator(t, "alice"), newOperator(t, "bob")
	path := initStore(t, alice, map[string]secretstore.Secret{"a": {Value: "1"}})
	old := replaceDataKey(t, path, alice, bob)
	merged := readStoreFile(t, path)
	merged.Secrets = old.Secrets
	writeStoreFile(t, path, merged)

	s := open(t, path, alice)
	_, err := s.Get("a")
	wantErr(t, err, `"a"`, "remove it and set it again")
	if err := s.Put("a", secretstore.Secret{Value: "1"}); err != nil {
		t.Fatal(err)
	}
	save(t, s)
	if got := value(t, open(t, path, alice), "a"); got != "1" {
		t.Errorf("a = %q, want %q", got, "1")
	}
}

func TestRecipientMustReadBackAsWritten(t *testing.T) {
	// The file keeps each recipient as an authorized_keys line, and its
	// sealed copy must match the line that is read back, or nobody can
	// open the store after the next save.
	alice := newOperator(t, "alice")
	for _, comment := range []string{"bob\nssh-ed25519 AAAA mallory", "bob\r", "bob ", " bob", " ", "a\tb\t", "jos\xe9@host"} {
		bad := newOperator(t, comment).recipient
		_, err := secretstore.Init(storePath(t), bad)
		wantErr(t, err, "comment")
		s := open(t, initStore(t, alice, nil), alice)
		wantErr(t, s.AddRecipient(bad), "comment")
	}
	// A .pub file in Latin-1 parses, but can't be written as it reads.
	pub := newOperator(t, "").recipient.String()
	_, err := secretstore.ParseRecipient(pub + " jos\xe9@host")
	wantErr(t, err, "comment")

	good := newOperator(t, "bob at  the desk")
	path := initStore(t, alice, nil)
	s := open(t, path, alice)
	if err := s.AddRecipient(good.recipient); err != nil {
		t.Fatal(err)
	}
	save(t, s)
	s = open(t, path, good)
	if err := s.Put("a", secretstore.Secret{Value: "1"}); err != nil {
		t.Fatal(err)
	}
	save(t, s)
	if got := value(t, open(t, path, alice), "a"); got != "1" {
		t.Errorf("a = %q, want %q", got, "1")
	}
}

func TestRecipientLineNotWrittenAsTheStoreWritesItFailsToOpen(t *testing.T) {
	// No call writes such a line, but whoever seals a data key by hand
	// can. A line that saving would rewrite must fail now, rather than
	// lock everyone out after the next save.
	alice := newOperator(t, "alice")
	path := initStore(t, alice, nil)
	line := strings.Replace(alice.recipient.String(), " ", "  ", 1)
	f := readStoreFile(t, path)
	f.Recipients = []string{line}
	f.DataKey = sealByHand(t, alice.recipient, line)
	writeStoreFile(t, path, f)

	_, err := secretstore.Open(path, alice.identity)
	wantErr(t, err, "isn't written as the secret store writes it")
}

// sealByHand seals a new data key and the recipient line to recipient,
// as the package doc gives the format, without going through the store.
func sealByHand(t *testing.T, recipient secretstore.Recipient, line string) string {
	t.Helper()
	ageRecipient, err := agessh.ParseRecipient(recipient.String())
	if err != nil {
		t.Fatal(err)
	}
	key := make([]byte, chacha20poly1305.KeySize)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	plain, err := json.Marshal(map[string]any{"id": "0011223344556677", "key": key, "recipients": []string{line}})
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	a := armor.NewWriter(&buf)
	w, err := age.Encrypt(a, ageRecipient)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(plain); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.String()
}

func TestSaveRefusesAFileChangedSinceOpen(t *testing.T) {
	alice := newOperator(t, "alice")
	path := initStore(t, alice, nil)
	first, second := open(t, path, alice), open(t, path, alice)
	if err := first.Put("a", secretstore.Secret{Value: "1"}); err != nil {
		t.Fatal(err)
	}
	save(t, first)
	if err := second.Put("b", secretstore.Secret{Value: "2"}); err != nil {
		t.Fatal(err)
	}

	err := second.Save()

	if !errors.Is(err, secretstore.ErrChanged) {
		t.Fatalf("got %v, want ErrChanged", err)
	}
	if v := value(t, open(t, path, alice), "a"); v != "1" {
		t.Errorf("a is %q", v)
	}
	if err := first.Put("c", secretstore.Secret{Value: "3"}); err != nil {
		t.Fatal(err)
	}
	save(t, first)
}

func TestConcurrentSavesNeverBothSucceed(t *testing.T) {
	alice := newOperator(t, "alice")
	path := initStore(t, alice, nil)
	for round := range 20 {
		stores := []*secretstore.Store{open(t, path, alice), open(t, path, alice)}
		errs := make(chan error, len(stores))
		for i, s := range stores {
			if err := s.Put(fmt.Sprint(round, i), secretstore.Secret{Value: "x"}); err != nil {
				t.Fatal(err)
			}
			go func() { errs <- s.Save() }()
		}
		var failed int
		for range stores {
			if err := <-errs; errors.Is(err, secretstore.ErrChanged) {
				failed++
			} else if err != nil {
				t.Fatal(err)
			}
		}
		if failed != 1 {
			t.Fatalf("%d of 2 saves failed, want 1", failed)
		}
	}
}

func TestFindRecipientByFingerprintOrComment(t *testing.T) {
	alice, bob, other := newOperator(t, "alice"), newOperator(t, "ops"), newOperator(t, "ops")
	s := open(t, initStore(t, alice, nil), alice)
	for _, r := range []secretstore.Recipient{bob.recipient, other.recipient} {
		if err := s.AddRecipient(r); err != nil {
			t.Fatal(err)
		}
	}

	if r, err := s.FindRecipient("alice"); err != nil || r.String() != alice.recipient.String() {
		t.Errorf("by comment: %v, %v", r, err)
	}
	if r, err := s.FindRecipient(bob.recipient.Fingerprint()); err != nil || r.String() != bob.recipient.String() {
		t.Errorf("by fingerprint: %v, %v", r, err)
	}
	_, err := s.FindRecipient("ops")
	wantErr(t, err, "more than one", bob.recipient.Fingerprint(), other.recipient.Fingerprint())
	if _, err := s.FindRecipient("carol"); !errors.Is(err, secretstore.ErrNotFound) {
		t.Errorf("an unknown recipient: %v", err)
	}
}
