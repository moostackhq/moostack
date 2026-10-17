package secretstore

import (
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"filippo.io/age"
	"filippo.io/age/agessh"
	"golang.org/x/crypto/ssh"
)

// KeyOptions say where to find the operator's SSH key and how to ask
// them about it. Nothing about the key is stored.
type KeyOptions struct {
	// Home is the operator's home directory. Keys are looked for in its
	// .ssh directory.
	Home string
	// SSHKey is the private key file to use instead, as --ssh-key names
	// it. Its public key is read from SSHKey.pub, else from the private
	// key.
	SSHKey string
	// Terminal asks the operator. It is nil when there is no terminal.
	Terminal Terminal
	// Out is where ChooseKey prints its warning. Nil means os.Stderr.
	Out io.Writer
}

// Terminal is the operator's terminal. Prompts are written to it.
type Terminal interface {
	io.Writer
	// ReadLine reads a line, without its line ending.
	ReadLine() (string, error)
	// ReadPassword reads a line without echoing it.
	ReadPassword() ([]byte, error)
}

// lossWarning is printed once, when a store is created.
const lossWarning = `The secret store can be read only with its recipients' SSH keys. If every
recipient loses their key, the store is lost, and the cluster CA key with it.
Adding a second recipient is the only backup.
`

// ChooseKey chooses the key that a new store at path is encrypted to:
// the one SSHKey names, else one of the ed25519 and RSA keys in ~/.ssh
// that have both a private key and a .pub, which the operator is asked to
// choose even if there is only one. It then warns, once, that losing
// every recipient's key loses the store. It refuses before asking if
// path exists, as Init would.
func ChooseKey(path string, opts KeyOptions) (Recipient, error) {
	if _, err := os.Lstat(path); err == nil {
		return Recipient{}, existsError(path)
	} else if !errors.Is(err, os.ErrNotExist) {
		return Recipient{}, err
	}
	var (
		r   Recipient
		err error
	)
	if opts.SSHKey != "" {
		r, err = recipientForKey(opts.SSHKey)
	} else {
		r, err = askForKey(opts)
	}
	if err != nil {
		return Recipient{}, err
	}
	out := opts.Out
	if out == nil {
		out = os.Stderr
	}
	fmt.Fprint(out, lossWarning)
	return r, nil
}

// askForKey asks the operator which of their keys in ~/.ssh to use.
func askForKey(opts KeyOptions) (Recipient, error) {
	if opts.Terminal == nil {
		return Recipient{}, errors.New("no terminal to ask which SSH key protects the secret store; choose one with --ssh-key")
	}
	keys := sshDirKeys(opts.Home)
	if len(keys) == 0 {
		return Recipient{}, fmt.Errorf("no ed25519 or RSA key with a .pub in %s; create one with ssh-keygen -t ed25519, or choose one with --ssh-key", sshDir(opts.Home))
	}
	t := opts.Terminal
	fmt.Fprintln(t, "Which SSH key protects the secret store?")
	for i, k := range keys {
		fmt.Fprintf(t, "  %d) %s %s (%s)\n", i+1, k.recipient.Fingerprint(), k.recipient.Comment, k.path)
	}
	for {
		fmt.Fprintf(t, "Key [1-%d]: ", len(keys))
		line, err := t.ReadLine()
		if err != nil {
			return Recipient{}, fmt.Errorf("asking which SSH key protects the secret store: %w", err)
		}
		if n, err := strconv.Atoi(strings.TrimSpace(line)); err == nil && n >= 1 && n <= len(keys) {
			return keys[n-1].recipient, nil
		}
	}
}

// FindKey finds the operator's key for the store at path: the one
// SSHKey names, else the first key in ~/.ssh whose .pub is a recipient.
// If a passphrase protects the key, FindKey asks for it on the terminal.
// It isn't kept, so each command that calls FindKey asks once.
func FindKey(path string, opts KeyOptions) (age.Identity, error) {
	_, recipients, err := readStore(path)
	if err != nil {
		return nil, err
	}
	isRecipient := func(r Recipient) bool { return recipientIndex(recipients, r.Key) >= 0 }
	if opts.SSHKey != "" {
		r, err := recipientForKey(opts.SSHKey)
		if err != nil {
			return nil, err
		}
		if !isRecipient(r) {
			problem := fmt.Sprintf("key %s in %s is %s", r.Fingerprint(), opts.SSHKey, ErrNotRecipient)
			return nil, noKeyError(path, problem, recipients, "")
		}
		return sshKey{path: opts.SSHKey, recipient: r}.identity(opts.Terminal)
	}
	for _, k := range sshDirKeys(opts.Home) {
		if isRecipient(k.recipient) {
			return k.identity(opts.Terminal)
		}
	}
	problem := fmt.Sprintf("none of your keys in %s is a recipient of the secret store", sshDir(opts.Home))
	return nil, noKeyError(path, problem, recipients, "choose your key with --ssh-key")
}

// noKeyError is the error for when the operator has no key for the store
// at path. It says what the problem is, lists the store's recipients, and
// says to ask one of them to add the operator, offering alternative as
// another way if it isn't empty.
func noKeyError(path, problem string, recipients []Recipient, alternative string) error {
	var b strings.Builder
	fmt.Fprintf(&b, "%s: %s. Its recipients are:\n", path, problem)
	for _, r := range recipients {
		fmt.Fprintf(&b, "  %s %s\n", r.Fingerprint(), r.Comment)
	}
	b.WriteString("ask one of them to add you")
	if alternative != "" {
		b.WriteString(", or " + alternative)
	}
	return &notRecipientError{msg: b.String()}
}

// notRecipientError is an error that is ErrNotRecipient, with its own
// message.
type notRecipientError struct{ msg string }

func (e *notRecipientError) Error() string        { return e.msg }
func (e *notRecipientError) Is(target error) bool { return target == ErrNotRecipient }

// sshKey is a key file and its public key.
type sshKey struct {
	// path is the private key's file.
	path      string
	recipient Recipient
}

// identity reads the key's private key, asking for its passphrase on t
// if one protects it. The private key must be the public key's.
func (k sshKey) identity(t Terminal) (age.Identity, error) {
	data, err := os.ReadFile(k.path)
	if err != nil {
		return nil, err
	}
	key, err := ssh.ParseRawPrivateKey(data)
	var missing *ssh.PassphraseMissingError
	if errors.As(err, &missing) {
		if t == nil {
			return nil, fmt.Errorf("%s is protected by a passphrase, and there is no terminal to ask for it", k.path)
		}
		fmt.Fprintf(t, "Passphrase for %s: ", k.path)
		passphrase, rerr := t.ReadPassword()
		fmt.Fprintln(t)
		if rerr != nil {
			return nil, fmt.Errorf("asking for the passphrase for %s: %w", k.path, rerr)
		}
		key, err = ssh.ParseRawPrivateKeyWithPassphrase(data, passphrase)
		if errors.Is(err, x509.IncorrectPasswordError) {
			return nil, fmt.Errorf("wrong passphrase for %s", k.path)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("%s: %w", k.path, err)
	}
	signer, err := ssh.NewSignerFromKey(key)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", k.path, err)
	}
	if !sameKey(signer.PublicKey(), k.recipient.Key) {
		return nil, fmt.Errorf("%s doesn't match its public key %s", k.path, k.recipient.Fingerprint())
	}
	var id age.Identity
	switch key := key.(type) {
	case *ed25519.PrivateKey:
		id, err = agessh.NewEd25519Identity(*key)
	case ed25519.PrivateKey:
		id, err = agessh.NewEd25519Identity(key)
	case *rsa.PrivateKey:
		id, err = agessh.NewRSAIdentity(key)
	default:
		// The public key, which matches, is ed25519 or RSA.
		return nil, fmt.Errorf("%s: unexpected private key type %T", k.path, key)
	}
	if err != nil {
		return nil, fmt.Errorf("%s: %w", k.path, err)
	}
	return id, nil
}

// ownComment is the comment from the operator's own .pub for key, made
// safe to keep: bytes that aren't UTF-8 are replaced, surrounding
// whitespace is trimmed, and a comment that still wouldn't read back as
// written is dropped. The comment is only shown, or kept with a new
// recipient, so it never makes a key unusable.
func ownComment(key ssh.PublicKey, comment string) string {
	comment = strings.TrimSpace(strings.ToValidUTF8(comment, "\uFFFD"))
	if _, ok, err := readBack(Recipient{Key: key, Comment: comment}.String()); err != nil || !ok {
		return ""
	}
	return comment
}

// sshDir is ~/.ssh.
func sshDir(home string) string {
	return filepath.Join(home, ".ssh")
}

// sshDirKeys are the keys in ~/.ssh that can be recipients and have both
// a private key and a .pub that match, in file name order.
func sshDirKeys(home string) []sshKey {
	entries, err := os.ReadDir(sshDir(home))
	if err != nil {
		return nil
	}
	var keys []sshKey
	for _, e := range entries {
		name, ok := strings.CutSuffix(e.Name(), ".pub")
		if !ok {
			continue
		}
		private := filepath.Join(sshDir(home), name)
		if info, err := os.Stat(private); err != nil || !info.Mode().IsRegular() {
			continue
		}
		r, err := recipientForKey(private)
		if err != nil {
			continue
		}
		keys = append(keys, sshKey{path: private, recipient: r})
	}
	return keys
}

// recipientForKey is the recipient for the private key file path, which
// must be an ed25519 or RSA key. Its public key is read from the private
// key and must match path.pub, if there is one, which gives the comment.
// A passphrase isn't needed, because OpenSSH's format keeps the public
// key unencrypted; a protected key in the older PEM format is refused.
func recipientForKey(path string) (Recipient, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Recipient{}, err
	}
	var key ssh.PublicKey
	signer, err := ssh.ParsePrivateKey(data)
	var missing *ssh.PassphraseMissingError
	switch {
	case err == nil:
		key = signer.PublicKey()
	case errors.As(err, &missing) && missing.PublicKey != nil:
		key = missing.PublicKey
	case errors.As(err, &missing):
		return Recipient{}, fmt.Errorf("%s: its public key can't be read without its passphrase; convert it to OpenSSH's format with ssh-keygen -p -f %s", path, path)
	default:
		return Recipient{}, fmt.Errorf("%s: %w", path, err)
	}
	r := Recipient{Key: key}
	pub, err := os.ReadFile(path + ".pub")
	switch {
	case err == nil:
		pubKey, comment, _, _, err := ssh.ParseAuthorizedKey(pub)
		if err != nil {
			return Recipient{}, fmt.Errorf("%s.pub: %w", path, err)
		}
		if !sameKey(pubKey, key) {
			return Recipient{}, fmt.Errorf("%s doesn't match its public key in %s.pub", path, path)
		}
		r.Comment = ownComment(key, comment)
	case !errors.Is(err, os.ErrNotExist):
		return Recipient{}, err
	}
	if err := r.check(); err != nil {
		return Recipient{}, fmt.Errorf("%s: %w", path, err)
	}
	return r, nil
}
