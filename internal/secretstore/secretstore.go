// Package secretstore keeps secrets in a file that is encrypted to its
// recipients' SSH keys, so that it can be committed with the cluster
// directory.
//
// A random data key encrypts each value with XChaCha20-Poly1305. The data
// key is encrypted with age to every recipient, together with a copy of
// the recipient list: the age plaintext is the JSON object {"id": the
// data key's ID, "key": the key in base64, "recipients": the recipient
// lines}. Secret names, generate settings and recipients stay readable,
// so diffs show what changed and edits to different secrets merge
// cleanly.
//
// Anyone who can write the file can seal a new data key under any ID,
// to the recipients alone or to themselves as well, and put values of their
// own under it. A planted value reads like any other. So any change to data_key,
// with or without a change to the recipients, must be reviewed.
package secretstore

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"filippo.io/age"
	"filippo.io/age/armor"
	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/crypto/ssh"
	"gopkg.in/yaml.v3"

	"github.com/moostackhq/moostack/internal/filelock"
)

// FileName is the store's file name in the cluster directory.
const FileName = "secrets.enc.yml"

// LockFileName is the file next to the store that Save locks. It is
// never removed, and never committed.
const LockFileName = "." + FileName + ".lock"

// formatVersion is the version of the file format.
const formatVersion = 1

// ErrNotFound is returned for a secret that isn't in the store.
var ErrNotFound = errors.New("not found")

// ErrChanged is returned by Save when the file changed after the store
// was opened, as when another mooctl saved it meanwhile.
var ErrChanged = errors.New("changed since it was opened")

// ErrNotRecipient is returned when a key isn't one of the store's
// recipients.
var ErrNotRecipient = errors.New("not a recipient of the secret store")

// Secret is a secret's value and settings. The YAML tags are the form
// the operator sees and edits.
type Secret struct {
	Value string `yaml:"value"`
	// Generate says how to generate the value, if it is generated.
	Generate *Generate `yaml:"generate,omitempty"`
	// Rotate marks the value to be generated again.
	Rotate bool `yaml:"rotate,omitempty"`
}

// Generate are the settings to generate a value with. The store keeps
// them without checking them; Check does.
type Generate struct {
	Type    string `yaml:"type" json:"type"`
	Length  int    `yaml:"length,omitempty" json:"length,omitempty"`
	Charset string `yaml:"charset,omitempty" json:"charset,omitempty"`
}

// Store is an opened secret store. Changes stay in memory until Save.
type Store struct {
	path       string
	recipients []Recipient
	key        dataKey
	// sealedKey is the data key and recipients encrypted to the
	// recipients, or empty if they changed and must be encrypted again.
	sealedKey string
	secrets   []*entry
	// saved is the file as it was opened or last saved, which Save
	// checks it still is.
	saved []byte
}

// dataKey is the key that encrypts the values, and its ID.
type dataKey struct {
	id  string
	key []byte
}

// entry is a secret as the file holds it.
type entry struct {
	Name     string    `yaml:"name"`
	Generate *Generate `yaml:"generate,omitempty"`
	Rotate   bool      `yaml:"rotate,omitempty"`
	// Value is enc:v1:<data key ID>:<base64 of nonce and ciphertext>.
	Value string `yaml:"value"`
}

// file is the store's file.
type file struct {
	Version    int      `yaml:"version"`
	Recipients []string `yaml:"recipients"`
	// DataKey is the sealed data key, armored.
	DataKey string   `yaml:"data_key"`
	Secrets []*entry `yaml:"secrets"`
	// raw is the file as it was read.
	raw []byte
}

// sealed is what the data key's age encryption holds.
type sealed struct {
	ID         string   `json:"id"`
	Key        []byte   `json:"key"`
	Recipients []string `json:"recipients"`
}

// Init creates a store at path with one recipient. It refuses to
// overwrite an existing file.
func Init(path string, recipient Recipient) (*Store, error) {
	if err := recipient.check(); err != nil {
		return nil, err
	}
	key, err := newDataKey()
	if err != nil {
		return nil, err
	}
	s := &Store{path: path, recipients: []Recipient{recipient}, key: key}
	data, err := s.marshal()
	if err != nil {
		return nil, err
	}
	tmp, err := writeTemp(path, data)
	if err != nil {
		return nil, err
	}
	defer os.Remove(tmp)
	// A link fails if path exists, where a rename would replace it. So
	// Init needs a filesystem with hard links.
	if err := os.Link(tmp, path); err != nil {
		if errors.Is(err, os.ErrExist) {
			return nil, existsError(path)
		}
		return nil, err
	}
	s.saved = data
	return s, syncDir(path)
}

// Open opens the store at path, reading it with identity.
func Open(path string, identity age.Identity) (*Store, error) {
	f, recipients, err := readStore(path)
	if err != nil {
		return nil, err
	}
	r, err := age.Decrypt(armor.NewReader(strings.NewReader(f.DataKey)), identity)
	var noMatch *age.NoIdentityMatchError
	if errors.As(err, &noMatch) {
		return nil, fmt.Errorf("%s: your SSH key is %w", path, ErrNotRecipient)
	}
	if err != nil {
		return nil, fmt.Errorf("%s: decrypting the data key: %w", path, err)
	}
	plain, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("%s: decrypting the data key: %w", path, err)
	}
	var contents sealed
	if err := json.Unmarshal(plain, &contents); err != nil || len(contents.Key) != chacha20poly1305.KeySize {
		return nil, fmt.Errorf("%s: the data key is malformed", path)
	}
	if !slices.Equal(contents.Recipients, f.Recipients) {
		return nil, fmt.Errorf("%s: the recipient list doesn't match the copy sealed in the data key; the file may have been tampered with", path)
	}
	return &Store{
		path:       path,
		recipients: recipients,
		key:        dataKey{id: contents.ID, key: contents.Key},
		sealedKey:  f.DataKey,
		secrets:    f.Secrets,
		saved:      f.raw,
	}, nil
}

// existsError says that a store can't be created at path, because it
// exists. It is os.ErrExist.
func existsError(path string) error {
	return fmt.Errorf("%s: %w", path, os.ErrExist)
}

// readStore reads and checks the file at path, and parses its
// recipients.
func readStore(path string) (*file, []Recipient, error) {
	f, err := readFile(path)
	if err != nil {
		return nil, nil, err
	}
	recipients, err := parseRecipients(path, f.Recipients)
	if err != nil {
		return nil, nil, err
	}
	return f, recipients, nil
}

// readFile reads and checks the file at path.
func readFile(path string) (*file, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	var f file
	if err := dec.Decode(&f); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	f.raw = data
	if f.Version != formatVersion {
		return nil, fmt.Errorf("%s: unsupported format version %d", path, f.Version)
	}
	seen := map[string]bool{}
	for _, e := range f.Secrets {
		if e == nil || e.Name == "" {
			return nil, fmt.Errorf("%s: a secret has no name", path)
		}
		if seen[e.Name] {
			return nil, fmt.Errorf("%s: secret %q appears more than once", path, e.Name)
		}
		seen[e.Name] = true
	}
	return &f, nil
}

// parseRecipients parses the recipient list in the file at path.
func parseRecipients(path string, lines []string) ([]Recipient, error) {
	if len(lines) == 0 {
		return nil, fmt.Errorf("%s: the store has no recipients", path)
	}
	var recipients []Recipient
	for _, line := range lines {
		r, ok, err := readBack(line)
		if err != nil {
			return nil, fmt.Errorf("%s: recipient %q: %w", path, line, err)
		}
		if !ok {
			return nil, fmt.Errorf("%s: recipient %q isn't written as the secret store writes it; the file may have been tampered with", path, line)
		}
		// readBack has checked the line, so only the key type is left.
		if _, err := r.ageRecipient(); err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		recipients = append(recipients, r)
	}
	return recipients, nil
}

// Names are the names of the secrets, in the file's order.
func (s *Store) Names() []string {
	var names []string
	for _, e := range s.secrets {
		names = append(names, e.Name)
	}
	return names
}

// Get decrypts the secret called name.
func (s *Store) Get(name string) (Secret, error) {
	e := s.find(name)
	if e == nil {
		return Secret{}, fmt.Errorf("secret %q: %w", name, ErrNotFound)
	}
	return s.key.decrypt(e)
}

// Settings are the generate settings and rotate flag of the secret called
// name, as the file has them, without its value. They are checked only
// when the value is decrypted, so they can be read even when it can't.
func (s *Store) Settings(name string) (generate *Generate, rotate bool, err error) {
	e := s.find(name)
	if e == nil {
		return nil, false, fmt.Errorf("secret %q: %w", name, ErrNotFound)
	}
	return cloneGenerate(e.Generate), e.Rotate, nil
}

// Put sets the secret called name, adding it if it is new. A secret that
// doesn't change keeps its ciphertext. A value that can't be decrypted
// is replaced, which is how one from another data key is set again.
func (s *Store) Put(name string, secret Secret) error {
	if name == "" {
		return errors.New("a secret needs a name")
	}
	e := s.find(name)
	if e != nil {
		if old, err := s.key.decrypt(e); err == nil && old.Equal(secret) {
			return nil
		}
	}
	value, err := s.key.encrypt(name, secret)
	if err != nil {
		return err
	}
	if e == nil {
		e = &entry{Name: name}
		s.secrets = append(s.secrets, e)
	}
	e.Generate, e.Rotate, e.Value = cloneGenerate(secret.Generate), secret.Rotate, value
	return nil
}

// Delete removes the secret called name. It needn't be readable.
func (s *Store) Delete(name string) error {
	i := s.secretIndex(name)
	if i < 0 {
		return fmt.Errorf("secret %q: %w", name, ErrNotFound)
	}
	s.secrets = slices.Delete(s.secrets, i, i+1)
	return nil
}

// Recipients are the store's recipients.
func (s *Store) Recipients() []Recipient {
	return slices.Clone(s.recipients)
}

// FindRecipient finds the recipient whose fingerprint or comment is id.
// A comment that more than one recipient has is refused, with an error
// that lists their fingerprints.
func (s *Store) FindRecipient(id string) (Recipient, error) {
	var byComment []Recipient
	for _, r := range s.recipients {
		if r.Fingerprint() == id {
			return r, nil
		}
		if r.Comment == id {
			byComment = append(byComment, r)
		}
	}
	switch len(byComment) {
	case 1:
		return byComment[0], nil
	case 0:
		return Recipient{}, fmt.Errorf("no recipient has the fingerprint or comment %q: %w", id, ErrNotFound)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "more than one recipient has the comment %q; use the fingerprint instead:", id)
	for _, r := range byComment {
		fmt.Fprintf(&b, "\n  %s %s", r.Fingerprint(), r.Comment)
	}
	return Recipient{}, errors.New(b.String())
}

// AddRecipient adds a recipient. Only the data key is encrypted again;
// the values keep their ciphertext.
func (s *Store) AddRecipient(r Recipient) error {
	if err := r.check(); err != nil {
		return err
	}
	if recipientIndex(s.recipients, r.Key) >= 0 {
		return fmt.Errorf("key %s is already a recipient", r.Fingerprint())
	}
	s.recipients = append(s.recipients, r)
	s.sealedKey = ""
	return nil
}

// RemoveRecipient removes the recipient with key. The data key is
// replaced and every value encrypted again, so that the removed recipient
// can't read anything written afterwards. Every secret with generate
// settings is marked rotate, but nothing is generated again; marked are
// the secrets that weren't marked before. The last recipient can't be
// removed, nor can any recipient while a value can't be decrypted; the
// error names that secret.
func (s *Store) RemoveRecipient(key ssh.PublicKey) (marked []string, err error) {
	i := recipientIndex(s.recipients, key)
	if i < 0 {
		return nil, fmt.Errorf("key %s is %w", ssh.FingerprintSHA256(key), ErrNotRecipient)
	}
	if len(s.recipients) == 1 {
		return nil, errors.New("the last recipient can't be removed")
	}
	// Everything is decrypted and encrypted again before anything
	// changes, so that a failure leaves the store as it was.
	secrets := make([]Secret, len(s.secrets))
	for j, e := range s.secrets {
		secret, err := s.key.decrypt(e)
		if err != nil {
			return nil, err
		}
		if !secret.Rotate && secret.Generate != nil {
			secret.Rotate = true
			marked = append(marked, e.Name)
		}
		secrets[j] = secret
	}
	newKey, err := newDataKey()
	if err != nil {
		return nil, err
	}
	values := make([]string, len(s.secrets))
	for j, e := range s.secrets {
		if values[j], err = newKey.encrypt(e.Name, secrets[j]); err != nil {
			return nil, err
		}
	}
	for j, e := range s.secrets {
		e.Rotate, e.Value = secrets[j].Rotate, values[j]
	}
	s.key = newKey
	s.recipients = slices.Delete(s.recipients, i, i+1)
	s.sealedKey = ""
	return marked, nil
}

// recipientIndex is the index of the recipient with key, or -1.
func recipientIndex(recipients []Recipient, key ssh.PublicKey) int {
	return slices.IndexFunc(recipients, func(r Recipient) bool { return sameKey(r.Key, key) })
}

// Save writes the store to its file atomically. It refuses, with
// ErrChanged, if the file changed after the store was opened or last
// saved, so that it never overwrites someone else's change. From the
// check to the rename it holds a lock on LockFileName, next to the file,
// so that two saves at the same moment can't both pass the check. That
// holds for every user of the directory, and across machines where the
// filesystem supports the lock. On a system without file locks, Save
// refuses to save.
func (s *Store) Save() error {
	data, err := s.marshal()
	if err != nil {
		return err
	}
	unlock, err := filelock.Lock(filepath.Join(filepath.Dir(s.path), LockFileName))
	if err != nil {
		return err
	}
	defer unlock()
	current, err := os.ReadFile(s.path)
	if err != nil {
		return err
	}
	if !bytes.Equal(current, s.saved) {
		return fmt.Errorf("%s %w; nothing was saved", s.path, ErrChanged)
	}
	tmp, err := writeTemp(s.path, data)
	if err != nil {
		return err
	}
	if err := os.Rename(tmp, s.path); err != nil {
		os.Remove(tmp)
		return err
	}
	s.saved = data
	return syncDir(s.path)
}

// marshal encodes the store as its file, sealing the data key if it
// needs to be.
func (s *Store) marshal() ([]byte, error) {
	if s.sealedKey == "" {
		sealedKey, err := s.seal()
		if err != nil {
			return nil, err
		}
		s.sealedKey = sealedKey
	}
	f := file{Version: formatVersion, DataKey: s.sealedKey, Secrets: s.secrets}
	for _, r := range s.recipients {
		f.Recipients = append(f.Recipients, r.String())
	}
	if f.Secrets == nil {
		f.Secrets = []*entry{}
	}
	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(f); err != nil {
		return nil, err
	}
	if err := enc.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// seal encrypts the data key and a copy of the recipient list to the
// recipients.
func (s *Store) seal() (string, error) {
	contents := sealed{ID: s.key.id, Key: s.key.key}
	var ageRecipients []age.Recipient
	for _, r := range s.recipients {
		contents.Recipients = append(contents.Recipients, r.String())
		ar, err := r.ageRecipient()
		if err != nil {
			return "", err
		}
		ageRecipients = append(ageRecipients, ar)
	}
	plain, err := json.Marshal(contents)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	a := armor.NewWriter(&buf)
	w, err := age.Encrypt(a, ageRecipients...)
	if err != nil {
		return "", err
	}
	if _, err := w.Write(plain); err != nil {
		return "", err
	}
	if err := w.Close(); err != nil {
		return "", err
	}
	if err := a.Close(); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// secretIndex is the index of the secret called name, or -1.
func (s *Store) secretIndex(name string) int {
	return slices.IndexFunc(s.secrets, func(e *entry) bool { return e.Name == name })
}

// find is the secret called name, or nil.
func (s *Store) find(name string) *entry {
	i := s.secretIndex(name)
	if i < 0 {
		return nil
	}
	return s.secrets[i]
}

// valuePrefix starts every encrypted value.
const valuePrefix = "enc:v1:"

// encrypt encrypts secret's value under the data key, bound to name and
// secret's settings.
func (k dataKey) encrypt(name string, secret Secret) (string, error) {
	aead, err := chacha20poly1305.NewX(k.key)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, aead.NonceSize(), aead.NonceSize()+len(secret.Value)+aead.Overhead())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	ad := associatedData(k.id, name, secret)
	ciphertext := aead.Seal(nonce, nonce, []byte(secret.Value), ad)
	return valuePrefix + k.id + ":" + base64.StdEncoding.EncodeToString(ciphertext), nil
}

// decrypt decrypts e's value with the data key.
func (k dataKey) decrypt(e *entry) (Secret, error) {
	id, raw, ok := parseValue(e.Value)
	if !ok || len(raw) < chacha20poly1305.NonceSizeX {
		return Secret{}, fmt.Errorf("secret %q: the value is malformed", e.Name)
	}
	if id != k.id {
		return Secret{}, fmt.Errorf("secret %q was encrypted with another data key, as when a git merge combines a recipient change with a new secret; remove it and set it again", e.Name)
	}
	aead, err := chacha20poly1305.NewX(k.key)
	if err != nil {
		return Secret{}, err
	}
	secret := Secret{Generate: cloneGenerate(e.Generate), Rotate: e.Rotate}
	ad := associatedData(id, e.Name, secret)
	plain, err := aead.Open(nil, raw[:aead.NonceSize()], raw[aead.NonceSize():], ad)
	if err != nil {
		return Secret{}, fmt.Errorf("secret %q can't be decrypted: its value, name, generate settings or rotate flag were changed without the key", e.Name)
	}
	secret.Value = string(plain)
	return secret, nil
}

// parseValue splits an encrypted value into its data key ID and the
// nonce and ciphertext. ok is false if it is malformed.
func parseValue(value string) (id string, raw []byte, ok bool) {
	rest, prefixed := strings.CutPrefix(value, valuePrefix)
	id, encoded, hasID := strings.Cut(rest, ":")
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if !prefixed || !hasID || err != nil {
		return "", nil, false
	}
	return id, raw, true
}

// associatedData binds a value to its data key, its secret's name and its
// settings. secret's value isn't used.
func associatedData(keyID, name string, secret Secret) []byte {
	ad, _ := json.Marshal(struct {
		Key      string    `json:"key"`
		Name     string    `json:"name"`
		Generate *Generate `json:"generate"`
		Rotate   bool      `json:"rotate"`
	}{keyID, name, secret.Generate, secret.Rotate})
	return append([]byte("moostack secret store v1\n"), ad...)
}

// Equal reports whether s and o have the same value and settings.
func (s Secret) Equal(o Secret) bool {
	return s.Value == o.Value && s.SameSettings(o)
}

// SameSettings reports whether s and o have the same generate settings
// and rotate flag.
func (s Secret) SameSettings(o Secret) bool {
	return s.Rotate == o.Rotate && (s.Generate == nil) == (o.Generate == nil) &&
		(s.Generate == nil || *s.Generate == *o.Generate)
}

// cloneGenerate copies g, so that the caller can't change the store's.
func cloneGenerate(g *Generate) *Generate {
	if g == nil {
		return nil
	}
	c := *g
	return &c
}

// newDataKey makes a random data key with a random ID.
func newDataKey() (dataKey, error) {
	key := make([]byte, chacha20poly1305.KeySize)
	id := make([]byte, 8)
	if _, err := rand.Read(key); err != nil {
		return dataKey{}, err
	}
	if _, err := rand.Read(id); err != nil {
		return dataKey{}, err
	}
	return dataKey{id: hex.EncodeToString(id), key: key}, nil
}

// writeTemp writes data to a new temporary file next to path, synced,
// and returns its name.
func writeTemp(path string, data []byte) (string, error) {
	f, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*")
	if err != nil {
		return "", err
	}
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(f.Name())
		return "", err
	}
	return f.Name(), nil
}

// syncDir syncs the directory holding path, so that a new name in it is
// durable.
func syncDir(path string) error {
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
