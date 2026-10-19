package cli_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"golang.org/x/crypto/ssh"

	"github.com/moostackhq/moostack/internal/cli"
	"github.com/moostackhq/moostack/internal/secretstore"
	"github.com/moostackhq/moostack/internal/terminal"
)

// operator is someone running mooctl, with a home directory holding
// their SSH key, and a scripted terminal. Each operator runs mooctl in a
// cluster directory of their own.
type operator struct {
	t    *testing.T
	home string
	dir  string
	// key is the private key file in ~/.ssh.
	key       string
	recipient secretstore.Recipient
	terminal  *scriptedTerminal
	// version is the release version mooctl was built for.
	version string
	// nodes, if set, answer for the nodes, by name, instead of SSH.
	nodes map[string]*fakeNode

	mu sync.Mutex
	// exits are the codes mooctl exited with after an interrupt.
	exits []int
	// tempFilesAtExit is how many files were in $TMPDIR at the last exit.
	tempFilesAtExit int
}

// exitCodes are the codes mooctl exited with so far.
func (o *operator) exitCodes() []int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return slices.Clone(o.exits)
}

// newOperator makes an operator with an ed25519 key in ~/.ssh, whose
// .pub has comment, and an empty cluster directory.
func newOperator(t *testing.T, comment string) *operator {
	t.Helper()
	o := &operator{t: t, home: t.TempDir(), dir: t.TempDir(), terminal: &scriptedTerminal{}, version: "1.2.3"}
	o.key = filepath.Join(o.home, ".ssh", "id_ed25519")
	o.recipient = writeKey(t, o.key, comment)
	return o
}

// writeKey writes a new ed25519 key to path and path.pub, and returns
// its recipient.
func writeKey(t *testing.T, path, comment string) secretstore.Recipient {
	t.Helper()
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	block, err := ssh.MarshalPrivateKey(private, comment)
	if err != nil {
		t.Fatal(err)
	}
	pub, err := ssh.NewPublicKey(private.Public())
	if err != nil {
		t.Fatal(err)
	}
	r := secretstore.Recipient{Key: pub, Comment: comment}
	writeFile(t, path, pem.EncodeToMemory(block))
	writeFile(t, path+".pub", []byte(r.String()+"\n"))
	return r
}

// readFile reads path.
func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
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

// result is what a mooctl run printed and returned.
type result struct {
	stdout, stderr string
	err            error
}

// run runs mooctl with args in the operator's cluster directory.
func (o *operator) run(args ...string) result {
	o.t.Helper()
	var stdout, stderr bytes.Buffer
	env := cli.Env{
		Version:   o.version,
		Home:      o.home,
		LocalUser: "alice",
		Machine:   "laptop",
		Stdout:    &stdout,
		Stderr:    &stderr,
		DialNode:  o.dialNode(),
		Terminal: func(func(int)) (terminal.Terminal, io.Closer) {
			if o.terminal == nil {
				return nil, nil
			}
			return o.terminal, io.NopCloser(nil)
		},
		Exit: func(code int) {
			entries, _ := os.ReadDir(os.Getenv("TMPDIR"))
			o.mu.Lock()
			defer o.mu.Unlock()
			o.exits = append(o.exits, code)
			o.tempFilesAtExit = len(entries)
		},
	}
	err := cli.New(env).Run(context.Background(), append([]string{"mooctl", "-C", o.dir}, args...))
	return result{stdout: stdout.String(), stderr: stderr.String(), err: err}
}

// mustRun runs mooctl with args, which must succeed.
func (o *operator) mustRun(args ...string) result {
	o.t.Helper()
	r := o.run(args...)
	if r.err != nil {
		o.t.Fatalf("mooctl %s: %v\nstdout:\n%s\nstderr:\n%s", strings.Join(args, " "), r.err, r.stdout, r.stderr)
	}
	return r
}

// storePath is the secret store in the operator's cluster directory.
func (o *operator) storePath() string {
	return filepath.Join(o.dir, secretstore.FileName)
}

// open opens the operator's secret store with their key.
func (o *operator) open() *secretstore.Store {
	o.t.Helper()
	id, err := secretstore.FindKey(o.storePath(), secretstore.KeyOptions{Home: o.home})
	if err != nil {
		o.t.Fatal(err)
	}
	s, err := secretstore.Open(o.storePath(), id)
	if err != nil {
		o.t.Fatal(err)
	}
	return s
}

// putSecret sets the secret called name to value in the operator's
// secret store.
func (o *operator) putSecret(name, value string) {
	o.t.Helper()
	s := o.open()
	if err := s.Put(name, secretstore.Secret{Value: value}); err != nil {
		o.t.Fatal(err)
	}
	if err := s.Save(); err != nil {
		o.t.Fatal(err)
	}
}

// scriptedTerminal is a scripted terminal. It answers lines from its queue and
// records what was written to it.
type scriptedTerminal struct {
	out   bytes.Buffer
	lines []string
	// onReadLine, if set, runs when a line is read, instead of answering.
	onReadLine func()
}

// Write records p.
func (t *scriptedTerminal) Write(p []byte) (int, error) { return t.out.Write(p) }

// ReadLine answers the next line in the queue, or runs onReadLine.
func (t *scriptedTerminal) ReadLine() (string, error) {
	if t.onReadLine != nil {
		t.onReadLine()
		return "", io.EOF
	}
	if len(t.lines) == 0 {
		return "", io.EOF
	}
	line := t.lines[0]
	t.lines = t.lines[1:]
	return line, nil
}

// ReadPassword fails: the test keys have no passphrase.
func (t *scriptedTerminal) ReadPassword() ([]byte, error) { return nil, io.EOF }

// wantContains fails unless s contains each of want.
func wantContains(t *testing.T, what, s string, want ...string) {
	t.Helper()
	for _, w := range want {
		if !strings.Contains(s, w) {
			t.Errorf("%s doesn't contain %q:\n%s", what, w, s)
		}
	}
}
