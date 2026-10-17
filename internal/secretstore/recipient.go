package secretstore

import (
	"bytes"
	"fmt"
	"os"
	"slices"
	"strings"
	"unicode/utf8"

	"filippo.io/age"
	"filippo.io/age/agessh"
	"golang.org/x/crypto/ssh"
)

// Recipient is an SSH public key that can read the store, with its
// comment.
type Recipient struct {
	Key     ssh.PublicKey
	Comment string
}

// ParseRecipient parses a recipient in authorized_keys format, as a .pub
// file holds it. Only ed25519 and RSA keys can be recipients.
func ParseRecipient(line string) (Recipient, error) {
	key, comment, _, rest, err := ssh.ParseAuthorizedKey([]byte(line))
	if err != nil {
		return Recipient{}, fmt.Errorf("parsing recipient %q: %w", line, err)
	}
	if len(bytes.TrimSpace(rest)) > 0 {
		return Recipient{}, fmt.Errorf("parsing recipient %q: more than one key", line)
	}
	r := Recipient{Key: key, Comment: comment}
	return r, r.check()
}

// ReadRecipient reads the recipient that arg gives: the path of a .pub
// file, if that file exists, or else a public key in authorized_keys
// format, which ParseRecipient reads. An arg that names no file and has
// no key type as a field of its own is a path that doesn't exist, so the
// error says so rather than that it isn't a key.
//
// The comment of a .pub file is cleaned up the way ChooseKey cleans up
// the operator's own, rather than refused if it wouldn't read back as
// written, such as a comment in Latin-1. A typed comment can be fixed
// where it is typed, so ParseRecipient refuses it.
func ReadRecipient(arg string) (Recipient, error) {
	if _, err := os.Stat(arg); err == nil || !hasKeyType(arg) {
		return readRecipientFile(arg)
	}
	return ParseRecipient(arg)
}

// keyTypes are the key types an authorized_keys line can have.
var keyTypes = []string{
	ssh.KeyAlgoRSA, ssh.KeyAlgoDSA, ssh.KeyAlgoED25519, ssh.KeyAlgoSKED25519,
	ssh.KeyAlgoECDSA256, ssh.KeyAlgoECDSA384, ssh.KeyAlgoECDSA521, ssh.KeyAlgoSKECDSA256,
	ssh.CertAlgoRSAv01, ssh.CertAlgoDSAv01, ssh.CertAlgoED25519v01, ssh.CertAlgoSKED25519v01,
	ssh.CertAlgoECDSA256v01, ssh.CertAlgoECDSA384v01, ssh.CertAlgoECDSA521v01, ssh.CertAlgoSKECDSA256v01,
}

// hasKeyType reports whether a field of s is a key type, as one of every
// authorized_keys line is.
func hasKeyType(s string) bool {
	return slices.ContainsFunc(strings.Fields(s), func(field string) bool {
		return slices.Contains(keyTypes, field)
	})
}

// readRecipientFile reads a recipient from a .pub file, cleaning up its
// comment.
func readRecipientFile(path string) (Recipient, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Recipient{}, err
	}
	key, comment, _, rest, err := ssh.ParseAuthorizedKey(data)
	if err != nil {
		return Recipient{}, fmt.Errorf("%s: %w", path, err)
	}
	if len(bytes.TrimSpace(rest)) > 0 {
		return Recipient{}, fmt.Errorf("%s: more than one key", path)
	}
	r := Recipient{Key: key, Comment: ownComment(key, comment)}
	if err := r.check(); err != nil {
		return Recipient{}, fmt.Errorf("%s: %w", path, err)
	}
	return r, nil
}

// String is the recipient in authorized_keys format.
func (r Recipient) String() string {
	s := strings.TrimSuffix(string(ssh.MarshalAuthorizedKey(r.Key)), "\n")
	if r.Comment != "" {
		s += " " + r.Comment
	}
	return s
}

// Fingerprint is the key's SHA256 fingerprint, as ssh-keygen -l shows it.
func (r Recipient) Fingerprint() string {
	return ssh.FingerprintSHA256(r.Key)
}

// check checks that the key can be a recipient, and that its line reads
// back as written.
func (r Recipient) check() error {
	if _, err := r.ageRecipient(); err != nil {
		return err
	}
	_, ok, err := readBack(r.String())
	if err != nil {
		return fmt.Errorf("key %s: %w", r.Fingerprint(), err)
	}
	if !ok {
		return fmt.Errorf("key %s: its comment %q doesn't read back as written; use UTF-8 with no line breaks or surrounding spaces", r.Fingerprint(), r.Comment)
	}
	return nil
}

// readBack parses line as a recipient. ok says whether writing that
// recipient gives line again. The file keeps each recipient as a line in
// YAML and seals a copy in JSON, and opening compares the two, so a line
// that any of these changes would lock everyone out. Parsing trims the
// comment and stops at a line break, and JSON replaces invalid UTF-8.
// YAML keeps any valid UTF-8, and yaml.v3 doesn't fold long lines.
func readBack(line string) (r Recipient, ok bool, err error) {
	key, comment, _, _, err := ssh.ParseAuthorizedKey([]byte(line))
	if err != nil {
		return Recipient{}, false, err
	}
	r = Recipient{Key: key, Comment: comment}
	return r, r.String() == line && utf8.ValidString(line), nil
}

// ageRecipient is the recipient for age.
func (r Recipient) ageRecipient() (age.Recipient, error) {
	var (
		ar  age.Recipient
		err error
	)
	switch t := r.Key.Type(); t {
	case ssh.KeyAlgoED25519:
		ar, err = agessh.NewEd25519Recipient(r.Key)
	case ssh.KeyAlgoRSA:
		ar, err = agessh.NewRSARecipient(r.Key)
	default:
		return nil, fmt.Errorf("%s key %s: only ed25519 and RSA keys can be recipients", t, r.Fingerprint())
	}
	if err != nil {
		return nil, fmt.Errorf("key %s: %w", r.Fingerprint(), err)
	}
	return ar, nil
}

// sameKey reports whether a and b are the same key.
func sameKey(a, b ssh.PublicKey) bool {
	return bytes.Equal(a.Marshal(), b.Marshal())
}
