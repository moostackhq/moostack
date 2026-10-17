package secretstore_test

import (
	"bytes"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"encoding/pem"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/crypto/ssh"

	"github.com/moostackhq/moostack/internal/secretstore"
)

// terminal is a scripted terminal. It answers lines and passwords from
// its queues and records what was written to it.
type terminal struct {
	out       bytes.Buffer
	lines     []string
	passwords []string
	// passwordReads counts the passwords asked for.
	passwordReads int
}

func (t *terminal) Write(p []byte) (int, error) { return t.out.Write(p) }

func (t *terminal) ReadLine() (string, error) {
	if len(t.lines) == 0 {
		return "", io.EOF
	}
	line := t.lines[0]
	t.lines = t.lines[1:]
	return line, nil
}

func (t *terminal) ReadPassword() ([]byte, error) {
	t.passwordReads++
	if len(t.passwords) == 0 {
		return nil, io.EOF
	}
	p := t.passwords[0]
	t.passwords = t.passwords[1:]
	return []byte(p), nil
}

// keyFile is a key written to disk.
type keyFile struct {
	// path is the private key's file.
	path      string
	recipient secretstore.Recipient
}

// writeKey writes key's private key to path and its public key to
// path.pub.
func writeKey(t *testing.T, path string, key crypto.Signer, comment string) keyFile {
	t.Helper()
	return writeProtectedKey(t, path, key, comment, "")
}

// writeProtectedKey writes key's private key to path, protected by
// passphrase unless it is empty, and its public key to path.pub.
func writeProtectedKey(t *testing.T, path string, key crypto.Signer, comment, passphrase string) keyFile {
	t.Helper()
	writePrivateKey(t, path, key, comment, passphrase)
	pub, err := ssh.NewPublicKey(key.Public())
	if err != nil {
		t.Fatal(err)
	}
	r := secretstore.Recipient{Key: pub, Comment: comment}
	if err := os.WriteFile(path+".pub", []byte(r.String()+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return keyFile{path: path, recipient: r}
}

// writePrivateKey writes key's private key alone to path, protected by
// passphrase unless it is empty.
func writePrivateKey(t *testing.T, path string, key crypto.Signer, comment, passphrase string) {
	t.Helper()
	var (
		block *pem.Block
		err   error
	)
	if passphrase == "" {
		block, err = ssh.MarshalPrivateKey(key, comment)
	} else {
		block, err = ssh.MarshalPrivateKeyWithPassphrase(key, comment, []byte(passphrase))
	}
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, path, pem.EncodeToMemory(block))
}

// writeFile writes data to path, making its directory.
func writeFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

// pubFile is the contents of the .pub file for k.
func pubFile(t *testing.T, k keyFile) []byte {
	t.Helper()
	data, err := os.ReadFile(k.path + ".pub")
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// newEd25519 makes an ed25519 key.
func newEd25519(t *testing.T) ed25519.PrivateKey {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

// newRSA makes a 2048-bit RSA key.
func newRSA(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

// newECDSA makes a P-256 ECDSA key.
func newECDSA(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

// sshDir is ~/.ssh in home.
func sshDir(home string) string { return filepath.Join(home, ".ssh") }

func TestChooseKeyAsksAmongUsableKeys(t *testing.T) {
	home := t.TempDir()
	ed := writeKey(t, filepath.Join(sshDir(home), "id_ed25519"), newEd25519(t), "alice@laptop")
	rsaKey := writeKey(t, filepath.Join(sshDir(home), "work_rsa"), newRSA(t), "alice@work")
	writeKey(t, filepath.Join(sshDir(home), "id_ecdsa"), newECDSA(t), "ecdsa")
	writePrivateKey(t, filepath.Join(sshDir(home), "no_pub"), newEd25519(t), "no pub", "")
	orphan := writeKey(t, filepath.Join(t.TempDir(), "orphan"), newEd25519(t), "orphan")
	writeFile(t, filepath.Join(sshDir(home), "orphan.pub"), pubFile(t, orphan))

	term := &terminal{lines: []string{"2"}}
	var out bytes.Buffer
	got, err := secretstore.ChooseKey(storePath(t), secretstore.KeyOptions{Home: home, Terminal: term, Out: &out})
	if err != nil {
		t.Fatal(err)
	}
	if got.String() != rsaKey.recipient.String() {
		t.Errorf("chose %q, want %q", got, rsaKey.recipient)
	}
	shown := term.out.String()
	for _, want := range []string{ed.recipient.Fingerprint(), "alice@laptop", rsaKey.recipient.Fingerprint(), "alice@work"} {
		if !strings.Contains(shown, want) {
			t.Errorf("prompt doesn't show %q:\n%s", want, shown)
		}
	}
	for _, unwanted := range []string{"ecdsa", "no pub", "orphan"} {
		if strings.Contains(shown, unwanted) {
			t.Errorf("prompt shows %q:\n%s", unwanted, shown)
		}
	}
}

func TestChooseKeyAsksEvenWithOneKey(t *testing.T) {
	home := t.TempDir()
	ed := writeKey(t, filepath.Join(sshDir(home), "id_ed25519"), newEd25519(t), "alice@laptop")

	// The first answer isn't a key; it is asked again.
	term := &terminal{lines: []string{"7", "1"}}
	got, err := secretstore.ChooseKey(storePath(t), secretstore.KeyOptions{Home: home, Terminal: term, Out: io.Discard})
	if err != nil {
		t.Fatal(err)
	}
	if got.String() != ed.recipient.String() {
		t.Errorf("chose %q, want %q", got, ed.recipient)
	}
	if len(term.lines) != 0 {
		t.Error("ChooseKey didn't ask until it got a key")
	}
}

func TestChooseKeyWithSSHKeyDoesntAsk(t *testing.T) {
	home := t.TempDir()
	writeKey(t, filepath.Join(sshDir(home), "id_ed25519"), newEd25519(t), "alice@laptop")
	chosen := writeKey(t, filepath.Join(t.TempDir(), "elsewhere"), newEd25519(t), "alice@elsewhere")

	term := &terminal{}
	got, err := secretstore.ChooseKey(storePath(t), secretstore.KeyOptions{Home: home, SSHKey: chosen.path, Terminal: term, Out: io.Discard})
	if err != nil {
		t.Fatal(err)
	}
	if got.String() != chosen.recipient.String() {
		t.Errorf("chose %q, want %q", got, chosen.recipient)
	}
	if term.out.Len() != 0 {
		t.Errorf("ChooseKey asked:\n%s", term.out.String())
	}
}

func TestChooseKeyWithSSHKeyRefusesOtherKeyTypes(t *testing.T) {
	chosen := writeKey(t, filepath.Join(t.TempDir(), "id_ecdsa"), newECDSA(t), "ecdsa")
	_, err := secretstore.ChooseKey(storePath(t), secretstore.KeyOptions{Home: t.TempDir(), SSHKey: chosen.path, Out: io.Discard})
	wantErr(t, err, "only ed25519 and RSA")
}

func TestChooseKeyWithoutTerminalOrSSHKeyFails(t *testing.T) {
	home := t.TempDir()
	writeKey(t, filepath.Join(sshDir(home), "id_ed25519"), newEd25519(t), "alice@laptop")
	_, err := secretstore.ChooseKey(storePath(t), secretstore.KeyOptions{Home: home, Out: io.Discard})
	wantErr(t, err, "--ssh-key")
}

func TestChooseKeyWithoutUsableKeysFails(t *testing.T) {
	home := t.TempDir()
	writeKey(t, filepath.Join(sshDir(home), "id_ecdsa"), newECDSA(t), "ecdsa")
	_, err := secretstore.ChooseKey(storePath(t), secretstore.KeyOptions{Home: home, Terminal: &terminal{}, Out: io.Discard})
	wantErr(t, err, "no ed25519 or RSA key", "--ssh-key")
}

func TestChooseKeyWarnsThatLosingEveryKeyLosesTheStore(t *testing.T) {
	chosen := writeKey(t, filepath.Join(t.TempDir(), "id_ed25519"), newEd25519(t), "alice")
	var out bytes.Buffer
	if _, err := secretstore.ChooseKey(storePath(t), secretstore.KeyOptions{Home: t.TempDir(), SSHKey: chosen.path, Out: &out}); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"every\nrecipient loses", "second recipient", "only backup"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("warning doesn't say %q:\n%s", want, out.String())
		}
	}
	if n := strings.Count(out.String(), "only backup"); n != 1 {
		t.Errorf("warning printed %d times, want once", n)
	}
}

// storeFor makes a store with recipients, holding secret "a" = "1".
func storeFor(t *testing.T, recipients ...secretstore.Recipient) string {
	t.Helper()
	path := storePath(t)
	s, err := secretstore.Init(path, recipients[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range recipients[1:] {
		if err := s.AddRecipient(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Put("a", secretstore.Secret{Value: "1"}); err != nil {
		t.Fatal(err)
	}
	save(t, s)
	return path
}

// openWithKey finds the key for the store at path and opens it.
func openWithKey(t *testing.T, path string, opts secretstore.KeyOptions) *secretstore.Store {
	t.Helper()
	id, err := secretstore.FindKey(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	s, err := secretstore.Open(path, id)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestFindKeyMatchesRecipientInSSHDir(t *testing.T) {
	home := t.TempDir()
	writeKey(t, filepath.Join(sshDir(home), "a_other"), newEd25519(t), "not a recipient")
	mine := writeKey(t, filepath.Join(sshDir(home), "id_rsa"), newRSA(t), "alice")
	someoneElse := newOperator(t, "bob")
	path := storeFor(t, someoneElse.recipient, mine.recipient)

	s := openWithKey(t, path, secretstore.KeyOptions{Home: home})
	if got := value(t, s, "a"); got != "1" {
		t.Errorf("a = %q, want %q", got, "1")
	}
}

func TestFindKeyWithSSHKeyOverridesSSHDir(t *testing.T) {
	home := t.TempDir()
	inDir := writeKey(t, filepath.Join(sshDir(home), "id_ed25519"), newEd25519(t), "alice@laptop")
	chosen := writeKey(t, filepath.Join(t.TempDir(), "deploy"), newEd25519(t), "deploy")
	// Only inDir is a recipient, so the chosen key must not fall back to it.
	path := storeFor(t, inDir.recipient)

	_, err := secretstore.FindKey(path, secretstore.KeyOptions{Home: home, SSHKey: chosen.path})
	if !errors.Is(err, secretstore.ErrNotRecipient) {
		t.Fatalf("err = %v, want ErrNotRecipient", err)
	}
	wantErr(t, err, inDir.recipient.Fingerprint(), "alice@laptop", "ask one of them to add you")
	if strings.Contains(err.Error(), "--ssh-key") {
		t.Errorf("err = %q, want no advice to use --ssh-key, which was used", err)
	}

	path = storeFor(t, inDir.recipient, chosen.recipient)
	id, err := secretstore.FindKey(path, secretstore.KeyOptions{Home: home, SSHKey: chosen.path})
	if err != nil {
		t.Fatal(err)
	}
	// Only the chosen key reads the store once inDir is removed.
	s := open(t, path, operator{identity: id})
	if err := s.RemoveRecipient(inDir.recipient.Key); err != nil {
		t.Fatal(err)
	}
	save(t, s)
	if _, err := secretstore.Open(path, id); err != nil {
		t.Errorf("the --ssh-key identity doesn't read the store: %v", err)
	}
}

func TestFindKeyWithoutMatchListsRecipients(t *testing.T) {
	home := t.TempDir()
	writeKey(t, filepath.Join(sshDir(home), "id_ed25519"), newEd25519(t), "carol")
	alice, bob := newOperator(t, "alice@laptop"), newOperator(t, "bob@desk")
	path := storeFor(t, alice.recipient, bob.recipient)

	_, err := secretstore.FindKey(path, secretstore.KeyOptions{Home: home})
	wantErr(t, err,
		alice.recipient.Fingerprint(), "alice@laptop",
		bob.recipient.Fingerprint(), "bob@desk",
		"ask one of them to add you")
}

func TestPassphraseProtectedKeyPromptsOnce(t *testing.T) {
	home := t.TempDir()
	mine := writeProtectedKey(t, filepath.Join(sshDir(home), "id_ed25519"), newEd25519(t), "alice", "correct horse")
	path := storeFor(t, mine.recipient)

	term := &terminal{passwords: []string{"correct horse"}}
	id, err := secretstore.FindKey(path, secretstore.KeyOptions{Home: home, Terminal: term})
	if err != nil {
		t.Fatal(err)
	}
	// A command may open the store more than once.
	for range 2 {
		s, err := secretstore.Open(path, id)
		if err != nil {
			t.Fatal(err)
		}
		if got := value(t, s, "a"); got != "1" {
			t.Errorf("a = %q, want %q", got, "1")
		}
	}
	if term.passwordReads != 1 {
		t.Errorf("asked for the passphrase %d times, want once", term.passwordReads)
	}
	if !strings.Contains(term.out.String(), mine.path) {
		t.Errorf("the prompt doesn't name the key:\n%s", term.out.String())
	}
}

func TestPassphraseProtectedKeyWithWrongPassphraseFails(t *testing.T) {
	home := t.TempDir()
	mine := writeProtectedKey(t, filepath.Join(sshDir(home), "id_ed25519"), newEd25519(t), "alice", "correct horse")
	path := storeFor(t, mine.recipient)

	_, err := secretstore.FindKey(path, secretstore.KeyOptions{Home: home, Terminal: &terminal{passwords: []string{"wrong"}}})
	wantErr(t, err, "wrong passphrase", mine.path)
}

func TestPassphraseProtectedKeyWithoutTerminalFails(t *testing.T) {
	home := t.TempDir()
	mine := writeProtectedKey(t, filepath.Join(sshDir(home), "id_ed25519"), newEd25519(t), "alice", "correct horse")
	path := storeFor(t, mine.recipient)

	_, err := secretstore.FindKey(path, secretstore.KeyOptions{Home: home})
	wantErr(t, err, mine.path, "passphrase", "no terminal")
}

func TestChooseKeyRefusesExistingStoreBeforeAsking(t *testing.T) {
	home := t.TempDir()
	mine := writeKey(t, filepath.Join(sshDir(home), "id_ed25519"), newEd25519(t), "alice")
	path := storeFor(t, mine.recipient)

	term := &terminal{lines: []string{"1"}}
	var out bytes.Buffer
	_, err := secretstore.ChooseKey(path, secretstore.KeyOptions{Home: home, Terminal: term, Out: &out})
	wantErr(t, err, "already exists")
	if !errors.Is(err, os.ErrExist) {
		t.Errorf("err = %v, want os.ErrExist", err)
	}
	if term.out.Len() != 0 || out.Len() != 0 {
		t.Errorf("ChooseKey asked or warned:\n%s%s", term.out.String(), out.String())
	}
}

func TestFindKeyRefusesPubThatDoesntMatchPrivateKey(t *testing.T) {
	home := t.TempDir()
	mine := writeKey(t, filepath.Join(sshDir(home), "id_ed25519"), newEd25519(t), "alice")
	path := storeFor(t, mine.recipient)
	// The private key is replaced, and the .pub left stale.
	writePrivateKey(t, mine.path, newEd25519(t), "alice", "")

	_, err := secretstore.FindKey(path, secretstore.KeyOptions{Home: home})
	wantErr(t, err, "none of your keys", "ask one of them to add you")
	_, err = secretstore.FindKey(path, secretstore.KeyOptions{Home: home, SSHKey: mine.path})
	wantErr(t, err, mine.path, "doesn't match")
}

func TestChooseKeyListsOnlyKeysWhosePrivateKeyMatches(t *testing.T) {
	home := t.TempDir()
	good := writeKey(t, filepath.Join(sshDir(home), "id_ed25519"), newEd25519(t), "good")
	stale := writeKey(t, filepath.Join(sshDir(home), "stale"), newEd25519(t), "stale")
	writePrivateKey(t, stale.path, newEd25519(t), "stale", "")
	corrupt := writeKey(t, filepath.Join(sshDir(home), "corrupt"), newEd25519(t), "corrupt")
	writeFile(t, corrupt.path, []byte("not a key"))
	// A file that isn't a key, next to someone's public key.
	other := writeKey(t, filepath.Join(t.TempDir(), "other"), newEd25519(t), "other")
	writeFile(t, filepath.Join(sshDir(home), "notes"), []byte("notes"))
	writeFile(t, filepath.Join(sshDir(home), "notes.pub"), pubFile(t, other))

	term := &terminal{lines: []string{"1"}}
	got, err := secretstore.ChooseKey(storePath(t), secretstore.KeyOptions{Home: home, Terminal: term, Out: io.Discard})
	if err != nil {
		t.Fatal(err)
	}
	if got.String() != good.recipient.String() {
		t.Errorf("chose %q, want %q", got, good.recipient)
	}
	for _, k := range []keyFile{stale, corrupt, other} {
		if strings.Contains(term.out.String(), k.recipient.Fingerprint()) {
			t.Errorf("prompt lists %s:\n%s", k.recipient.Comment, term.out.String())
		}
	}
}

func TestChooseKeyListsPassphraseProtectedKeys(t *testing.T) {
	home := t.TempDir()
	mine := writeProtectedKey(t, filepath.Join(sshDir(home), "id_ed25519"), newEd25519(t), "alice", "correct horse")

	term := &terminal{lines: []string{"1"}}
	got, err := secretstore.ChooseKey(storePath(t), secretstore.KeyOptions{Home: home, Terminal: term, Out: io.Discard})
	if err != nil {
		t.Fatal(err)
	}
	if got.String() != mine.recipient.String() {
		t.Errorf("chose %q, want %q", got, mine.recipient)
	}
	if term.passwordReads != 0 {
		t.Error("ChooseKey asked for the passphrase")
	}
}

func TestChooseKeyWithSSHKeyRefusesKeyThatDoesntMatch(t *testing.T) {
	stale := writeKey(t, filepath.Join(t.TempDir(), "stale"), newEd25519(t), "stale")
	writePrivateKey(t, stale.path, newEd25519(t), "stale", "")
	_, err := secretstore.ChooseKey(storePath(t), secretstore.KeyOptions{Home: t.TempDir(), SSHKey: stale.path, Out: io.Discard})
	wantErr(t, err, stale.path, "doesn't match")

	corrupt := writeKey(t, filepath.Join(t.TempDir(), "corrupt"), newEd25519(t), "corrupt")
	writeFile(t, corrupt.path, []byte("not a key"))
	_, err = secretstore.ChooseKey(storePath(t), secretstore.KeyOptions{Home: t.TempDir(), SSHKey: corrupt.path, Out: io.Discard})
	wantErr(t, err, corrupt.path)
}

func TestChooseKeyFailsWhenStorePathCantBeChecked(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	dir := filepath.Join(t.TempDir(), "locked")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(dir, 0o700) })
	home := t.TempDir()
	writeKey(t, filepath.Join(sshDir(home), "id_ed25519"), newEd25519(t), "alice")

	term := &terminal{lines: []string{"1"}}
	var out bytes.Buffer
	_, err := secretstore.ChooseKey(filepath.Join(dir, secretstore.FileName), secretstore.KeyOptions{Home: home, Terminal: term, Out: &out})
	wantErr(t, err, "permission denied")
	if term.out.Len() != 0 || out.Len() != 0 {
		t.Errorf("ChooseKey asked or warned:\n%s%s", term.out.String(), out.String())
	}
}

func TestChooseKeyWithoutOutStillChooses(t *testing.T) {
	chosen := writeKey(t, filepath.Join(t.TempDir(), "id_ed25519"), newEd25519(t), "alice")
	if _, err := secretstore.ChooseKey(storePath(t), secretstore.KeyOptions{Home: t.TempDir(), SSHKey: chosen.path}); err != nil {
		t.Fatal(err)
	}
}

// writePubComment replaces k's .pub with one whose comment is comment,
// as written, with no checks.
func writePubComment(t *testing.T, k keyFile, comment string) {
	t.Helper()
	writeFile(t, k.path+".pub", []byte(secretstore.Recipient{Key: k.recipient.Key}.String()+" "+comment+"\n"))
}

// unkeepableComments are .pub comments that wouldn't read back as written,
// with what is kept of each.
var unkeepableComments = []struct{ raw, kept string }{
	{"jos\xe9@host", "jos\uFFFD@host"}, // Latin-1
	{"jose\r ", "jose"},                // a CR that parsing keeps
}

func TestFindKeyMatchesKeyWhoseCommentWontReadBack(t *testing.T) {
	// The recipient was added with a cleaned-up comment, but the key's
	// own .pub still has the original.
	for _, c := range unkeepableComments {
		home := t.TempDir()
		mine := writeKey(t, filepath.Join(sshDir(home), "id_ed25519"), newEd25519(t), "jose@host")
		path := storeFor(t, mine.recipient)
		writePubComment(t, mine, c.raw)

		for _, opts := range []secretstore.KeyOptions{{Home: home}, {Home: home, SSHKey: mine.path}} {
			openWithKey(t, path, opts)
		}
	}
}

func TestChooseKeyOffersKeyWhoseCommentWontReadBack(t *testing.T) {
	for _, c := range unkeepableComments {
		home := t.TempDir()
		mine := writeKey(t, filepath.Join(sshDir(home), "id_ed25519"), newEd25519(t), "jose")
		writePubComment(t, mine, c.raw)

		for _, opts := range []secretstore.KeyOptions{
			{Home: home, Terminal: &terminal{lines: []string{"1"}}, Out: io.Discard},
			{Home: home, SSHKey: mine.path, Out: io.Discard},
		} {
			path := storePath(t)
			r, err := secretstore.ChooseKey(path, opts)
			if err != nil {
				t.Fatal(err)
			}
			if r.Comment != c.kept {
				t.Errorf("comment = %q, want %q", r.Comment, c.kept)
			}
			if _, err := secretstore.Init(path, r); err != nil {
				t.Fatal(err)
			}
			openWithKey(t, path, secretstore.KeyOptions{Home: home})
		}
	}
}
