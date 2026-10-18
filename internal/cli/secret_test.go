//go:build unix

package cli_test

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/moostackhq/moostack/internal/secretstore"
)

func TestSecretInitCreatesAStoreForTheChosenKey(t *testing.T) {
	alice := newOperator(t, "alice@laptop")
	alice.terminal.lines = []string{"1"}

	r := alice.mustRun("secret", "init")

	wantContains(t, "terminal", alice.terminal.out.String(), "Which SSH key", alice.recipient.Fingerprint())
	wantContains(t, "stdout", r.stdout, "Created "+alice.storePath(), alice.recipient.Fingerprint(), "alice@laptop", "Added .secrets.enc.yml.lock to .gitignore")
	wantContains(t, ".gitignore", readFile(t, filepath.Join(alice.dir, ".gitignore")), "/.secrets.enc.yml.lock\n")
	wantContains(t, "stderr", r.stderr, "Adding a second recipient is the only backup")
	if got := alice.open().Recipients(); len(got) != 1 || got[0].String() != alice.recipient.String() {
		t.Errorf("recipients are %v, want only %v", got, alice.recipient)
	}
}

func TestSecretInitTakesSSHKey(t *testing.T) {
	alice := newOperator(t, "alice")
	other := filepath.Join(t.TempDir(), "deploy")
	deploy := writeKey(t, other, "deploy")
	alice.terminal = nil

	alice.mustRun("secret", "init", "--ssh-key", other)

	data, err := os.ReadFile(alice.storePath())
	if err != nil {
		t.Fatal(err)
	}
	wantContains(t, "store", string(data), deploy.String())
}

func TestSecretInitRefusesAnExistingStore(t *testing.T) {
	alice := newOperator(t, "alice")
	alice.mustRun("secret", "init", "--ssh-key", alice.key)
	before, _ := os.ReadFile(alice.storePath())

	r := alice.run("secret", "init", "--ssh-key", alice.key)

	if !errors.Is(r.err, os.ErrExist) {
		t.Errorf("got %v, want an error that the store exists", r.err)
	}
	after, _ := os.ReadFile(alice.storePath())
	if string(before) != string(after) {
		t.Error("the store changed")
	}
}

func TestSecretInitWithoutTerminalOrSSHKeyFails(t *testing.T) {
	alice := newOperator(t, "alice")
	alice.terminal = nil

	r := alice.run("secret", "init")

	if r.err == nil {
		t.Fatal("init succeeded")
	}
	wantContains(t, "error", r.err.Error(), "--ssh-key")
	if _, err := os.Stat(alice.storePath()); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("a store was written: %v", err)
	}
}

func TestSecretCommandsWithoutAStoreSayToInitOne(t *testing.T) {
	alice := newOperator(t, "alice")

	r := alice.run("secret", "view")

	if r.err == nil || !strings.Contains(r.err.Error(), "mooctl secret init") {
		t.Errorf("got %v, want an error that says to run mooctl secret init", r.err)
	}
}

func TestSecretCommandsCheckTheirArguments(t *testing.T) {
	alice := newStore(t, "alice")

	r := alice.run("secret", "recipients", "add")

	if r.err == nil || !strings.Contains(r.err.Error(), "mooctl secret recipients add takes 1 argument, not 0") {
		t.Errorf("got %v", r.err)
	}
}

// newStore makes an operator with a secret store encrypted to their key.
func newStore(t *testing.T, comment string) *operator {
	t.Helper()
	o := newOperator(t, comment)
	o.mustRun("secret", "init", "--ssh-key", o.key)
	return o
}

// teammate makes another operator, with their own home and key, who
// works in o's cluster directory.
func (o *operator) teammate(comment string) *operator {
	o.t.Helper()
	m := newOperator(o.t, comment)
	m.dir = o.dir
	return m
}

func TestSecretRecipientsLsShowsFingerprintTypeAndComment(t *testing.T) {
	alice := newStore(t, "alice@laptop")

	r := alice.mustRun("secret", "recipients", "ls")

	wantContains(t, "stdout", r.stdout, alice.recipient.Fingerprint(), "ssh-ed25519", "alice@laptop")
}

func TestSecretRecipientsAddTakesAPubFile(t *testing.T) {
	alice := newStore(t, "alice")
	bob := alice.teammate("bob@desk")

	r := alice.mustRun("secret", "recipients", "add", bob.key+".pub")

	wantContains(t, "stdout", r.stdout, "Added "+bob.recipient.String())
	bob.mustRun("secret", "view")
	wantContains(t, "ls", alice.mustRun("secret", "recipients", "ls").stdout, bob.recipient.Fingerprint(), "bob@desk")
}

func TestSecretRecipientsAddTakesALineAndShowsWhatWasAdded(t *testing.T) {
	alice := newStore(t, "alice")
	bob := alice.teammate("bob")

	r := alice.mustRun("secret", "recipients", "add", `from="10.0.0.1" `+bob.recipient.String())

	wantContains(t, "stdout", r.stdout, "Added "+bob.recipient.String()+"\n")
	if strings.Contains(r.stdout, "from=") {
		t.Errorf("stdout shows the options that weren't added:\n%s", r.stdout)
	}
	bob.mustRun("secret", "view")
}

func TestSecretRecipientsAddRefusesATypedCommentThatWontReadBack(t *testing.T) {
	alice := newStore(t, "alice")
	bob := alice.teammate("bob")
	line := strings.TrimSuffix(bob.recipient.String(), "bob") + "b\xe9b"

	r := alice.run("secret", "recipients", "add", line)

	if r.err == nil || !strings.Contains(r.err.Error(), "UTF-8") {
		t.Errorf("got %v, want an error that says to use UTF-8", r.err)
	}
}

func TestSecretRecipientsAddCleansUpTheCommentOfAPubFile(t *testing.T) {
	alice := newStore(t, "alice")
	bob := alice.teammate("bob")
	pub := filepath.Join(t.TempDir(), "bob.pub")
	line := strings.TrimSuffix(bob.recipient.String(), "bob") + " b\xe9b \r \n"
	writeFile(t, pub, []byte(line))

	r := alice.mustRun("secret", "recipients", "add", pub)

	wantContains(t, "stdout", r.stdout, "b�b")
	bob.mustRun("secret", "view")
}

func TestSecretRecipientsAddRefusesAnExistingRecipient(t *testing.T) {
	alice := newStore(t, "alice")

	r := alice.run("secret", "recipients", "add", alice.key+".pub")

	if r.err == nil || !strings.Contains(r.err.Error(), "already a recipient") {
		t.Errorf("got %v, want an error that the key is already a recipient", r.err)
	}
}

func TestSecretRecipientsRmByCommentMarksGeneratedSecretsAndWarns(t *testing.T) {
	alice := newStore(t, "alice")
	bob := alice.teammate("bob")
	alice.mustRun("secret", "recipients", "add", bob.key+".pub")
	s := alice.open()
	put(t, s, "db", secretstore.Secret{Value: "x", Generate: &secretstore.Generate{Type: "hex"}})
	put(t, s, "token", secretstore.Secret{Value: "y", Generate: &secretstore.Generate{Type: "uuid"}})
	put(t, s, "already", secretstore.Secret{Value: "z", Generate: &secretstore.Generate{Type: "hex"}, Rotate: true})
	put(t, s, "api", secretstore.Secret{Value: "typed"})
	save(t, s)

	r := alice.mustRun("secret", "recipients", "rm", "bob")

	wantContains(t, "stdout", r.stdout,
		"Removed "+bob.recipient.Fingerprint()+" bob",
		"Marked 2 secrets for rotation; 3 now marked")
	wantContains(t, "stderr", r.stderr, "may have copies of every current value, the cluster CA key included")
	if r := bob.run("secret", "view"); !errors.Is(r.err, secretstore.ErrNotRecipient) {
		t.Errorf("bob can still read the store: %v", r.err)
	}
	s = alice.open()
	for name, want := range map[string]bool{"db": true, "token": true, "already": true, "api": false} {
		if got := get(t, s, name); got.Rotate != want {
			t.Errorf("%s: rotate is %v, want %v", name, got.Rotate, want)
		}
	}
}

func TestSecretRecipientsRmByFingerprint(t *testing.T) {
	alice := newStore(t, "alice")
	bob := alice.teammate("bob")
	alice.mustRun("secret", "recipients", "add", bob.key+".pub")

	alice.mustRun("secret", "recipients", "rm", bob.recipient.Fingerprint())

	if got := alice.open().Recipients(); len(got) != 1 {
		t.Errorf("recipients are %v, want only alice", got)
	}
}

func TestSecretRecipientsRmRefusesACommentMatchingTwo(t *testing.T) {
	alice := newStore(t, "ops")
	bob := alice.teammate("ops")
	alice.mustRun("secret", "recipients", "add", bob.key+".pub")

	r := alice.run("secret", "recipients", "rm", "ops")

	if r.err == nil {
		t.Fatal("rm succeeded")
	}
	wantContains(t, "error", r.err.Error(), "more than one", "fingerprint")
	if got := alice.open().Recipients(); len(got) != 2 {
		t.Errorf("recipients are %v, want both", got)
	}
}

func TestSecretRecipientsRmRefusesAnUnknownRecipient(t *testing.T) {
	alice := newStore(t, "alice")
	before, _ := os.ReadFile(alice.storePath())

	r := alice.run("secret", "recipients", "rm", "carol")

	if r.err == nil || !strings.Contains(r.err.Error(), "carol") {
		t.Errorf("got %v, want an error naming carol", r.err)
	}
	after, _ := os.ReadFile(alice.storePath())
	if string(before) != string(after) {
		t.Error("the store changed")
	}
}

// put puts secret in s under name.
func put(t *testing.T, s *secretstore.Store, name string, secret secretstore.Secret) {
	t.Helper()
	if err := s.Put(name, secret); err != nil {
		t.Fatal(err)
	}
}

// get gets the secret called name from s.
func get(t *testing.T, s *secretstore.Store, name string) secretstore.Secret {
	t.Helper()
	secret, err := s.Get(name)
	if err != nil {
		t.Fatal(err)
	}
	return secret
}

// save saves s.
func save(t *testing.T, s *secretstore.Store) {
	t.Helper()
	if err := s.Save(); err != nil {
		t.Fatal(err)
	}
}

func TestSecretRecipientsAddSaysWhenAPubFileIsMissing(t *testing.T) {
	alice := newStore(t, "alice")

	r := alice.run("secret", "recipients", "add", "bob.pub")

	if !errors.Is(r.err, os.ErrNotExist) {
		t.Errorf("got %v, want an error that bob.pub doesn't exist", r.err)
	}
}

// editor is a scripted $EDITOR. On its nth run it records its arguments,
// the file it was given and that file's directory listing, then replaces
// the file with the nth text it was given, if there is one, and runs the
// nth hook, if there is one.
type editor struct {
	t   *testing.T
	log string
}

// scriptEditor makes a scripted editor called name and sets $EDITOR to
// it, followed by args. $VISUAL is cleared. It also points $TMPDIR at a
// directory of its own, so that the test can check that nothing is left
// behind.
func scriptEditor(t *testing.T, name string, args string, texts ...string) *editor {
	t.Helper()
	e := &editor{t: t, log: t.TempDir()}
	script := filepath.Join(t.TempDir(), name)
	writeFile(t, script, []byte(`#!/bin/sh
log='`+e.log+`'
n=$(cat "$log/runs" 2>/dev/null || echo 0)
n=$((n + 1))
echo $n > "$log/runs"
for a in "$@"; do printf '%s\n' "$a"; done > "$log/args.$n"
for file; do :; done
cp "$file" "$log/seen.$n"
ls -ld "$(dirname "$file")" > "$log/dir.$n"
if [ -f "$log/text.$n" ]; then cat "$log/text.$n" > "$file"; fi
if [ -f "$log/hook.$n" ]; then . "$log/hook.$n"; fi
`))
	if err := os.Chmod(script, 0o700); err != nil {
		t.Fatal(err)
	}
	for i, text := range texts {
		writeFile(t, filepath.Join(e.log, fmt.Sprintf("text.%d", i+1)), []byte(text))
	}
	t.Setenv("VISUAL", "")
	t.Setenv("EDITOR", strings.TrimSpace(script+" "+args))
	t.Setenv("TMPDIR", t.TempDir())
	return e
}

// hook makes the editor's nth run also run the shell code sh.
func (e *editor) hook(n int, sh string) {
	writeFile(e.t, filepath.Join(e.log, fmt.Sprintf("hook.%d", n)), []byte(sh))
}

// runs is how often the editor ran.
func (e *editor) runs() int {
	data, err := os.ReadFile(filepath.Join(e.log, "runs"))
	if err != nil {
		return 0
	}
	n, _ := strconv.Atoi(strings.TrimSpace(string(data)))
	return n
}

// record is what the editor recorded on its nth run: "args", "seen" or
// "dir".
func (e *editor) record(what string, n int) string {
	e.t.Helper()
	data, err := os.ReadFile(filepath.Join(e.log, fmt.Sprintf("%s.%d", what, n)))
	if err != nil {
		e.t.Fatal(err)
	}
	return string(data)
}

// wantNoTempFiles fails if anything is left in $TMPDIR.
func wantNoTempFiles(t *testing.T) {
	t.Helper()
	entries, err := os.ReadDir(os.Getenv("TMPDIR"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) > 0 {
		t.Errorf("left in the temporary directory: %v", entries)
	}
}

func TestSecretEditSavesValuesAndGeneratesEmptyOnes(t *testing.T) {
	alice := newStore(t, "alice")
	scriptEditor(t, "editor", "", `
api_key:
  value: typed
db_password:
  value: ""
  generate: {type: password, length: 20}
session:
  generate: {type: hex}
`)

	r := alice.mustRun("secret", "edit")

	wantContains(t, "stdout", r.stdout, "Generated db_password, session")
	s := alice.open()
	if got := get(t, s, "api_key"); got.Value != "typed" || got.Generate != nil {
		t.Errorf("api_key is %+v", got)
	}
	if got := get(t, s, "db_password"); !regexp.MustCompile(`^[A-Za-z0-9]{20}$`).MatchString(got.Value) ||
		*got.Generate != (secretstore.Generate{Type: "password", Length: 20}) {
		t.Errorf("db_password is %+v, %+v", got, got.Generate)
	}
	if got := get(t, s, "session"); !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(got.Value) {
		t.Errorf("session is %+v", got)
	}
	wantNoTempFiles(t)
}

func TestSecretViewPrintsWhatEditShows(t *testing.T) {
	alice := newStore(t, "alice")
	s := alice.open()
	put(t, s, "db", secretstore.Secret{Value: "pw", Generate: &secretstore.Generate{Type: "password", Length: 20}, Rotate: true})
	put(t, s, "ca", secretstore.Secret{Value: "line 1\nline 2\n"})
	save(t, s)
	e := scriptEditor(t, "editor", "")

	view := alice.mustRun("secret", "view").stdout
	alice.mustRun("secret", "edit")

	want := `db:
  value: pw
  generate: {type: password, length: 20}
  rotate: true
ca:
  value: |
    line 1
    line 2
`
	if view != want {
		t.Errorf("view printed:\n%s\nwant:\n%s", view, want)
	}
	if seen := e.record("seen", 1); !strings.HasSuffix(seen, "\n"+want) || !strings.HasPrefix(seen, "#") {
		t.Errorf("edit showed:\n%s\nwant a comment, then:\n%s", seen, want)
	}
}

func TestSecretEditKeepsUnchangedCiphertextAndNeverRegenerates(t *testing.T) {
	alice := newStore(t, "alice")
	s := alice.open()
	put(t, s, "a", secretstore.Secret{Value: "one"})
	put(t, s, "b", secretstore.Secret{Value: "two", Generate: &secretstore.Generate{Type: "hex"}})
	save(t, s)
	before := readFile(t, alice.storePath())
	scriptEditor(t, "editor", "", `
a:
  value: one
b:
  value: two
  generate: {type: hex, length: 8}
`)

	alice.mustRun("secret", "edit")

	after := readFile(t, alice.storePath())
	if valueLine(t, before, "a") != valueLine(t, after, "a") {
		t.Error("a's ciphertext changed")
	}
	if got := get(t, alice.open(), "b"); got.Value != "two" || got.Generate.Length != 8 {
		t.Errorf("b is %+v, %+v; want its value kept and its length changed", got, got.Generate)
	}
}

func TestSecretEditDeletesRemovedSecrets(t *testing.T) {
	alice := newStore(t, "alice")
	s := alice.open()
	put(t, s, "a", secretstore.Secret{Value: "one"})
	put(t, s, "b", secretstore.Secret{Value: "two"})
	save(t, s)
	scriptEditor(t, "editor", "", "b:\n  value: two\n")

	alice.mustRun("secret", "edit")

	if got := alice.open().Names(); len(got) != 1 || got[0] != "b" {
		t.Errorf("secrets are %v, want only b", got)
	}
}

func TestSecretEditWithoutChangesLeavesTheFile(t *testing.T) {
	alice := newStore(t, "alice")
	s := alice.open()
	put(t, s, "a", secretstore.Secret{Value: "one"})
	save(t, s)
	before := readFile(t, alice.storePath())
	scriptEditor(t, "editor", "")

	r := alice.mustRun("secret", "edit")

	wantContains(t, "stdout", r.stdout, "No changes")
	if readFile(t, alice.storePath()) != before {
		t.Error("the store changed")
	}
}

func TestSecretEditRefusesToRemoveEverySecret(t *testing.T) {
	alice := newStore(t, "alice")
	s := alice.open()
	put(t, s, "a", secretstore.Secret{Value: "one"})
	save(t, s)
	before := readFile(t, alice.storePath())
	scriptEditor(t, "editor", "", "# all gone\n")
	alice.terminal.lines = []string{"n"}

	r := alice.mustRun("secret", "edit")

	wantContains(t, "stderr", r.stderr, "leaves no secret that can be read, which is refused", "Nothing saved")
	if readFile(t, alice.storePath()) != before {
		t.Error("the store changed")
	}
}

func TestSecretEditReopensInvalidTextIntact(t *testing.T) {
	alice := newStore(t, "alice")
	invalid := "a:\n  value: [unclosed\n"
	e := scriptEditor(t, "editor", "", invalid, "a:\n  value: fixed\n")
	alice.terminal.lines = []string{"y"}

	r := alice.mustRun("secret", "edit")

	wantContains(t, "stderr", r.stderr, "line")
	wantContains(t, "terminal", alice.terminal.out.String(), "Reopen the editor?")
	if seen := e.record("seen", 2); seen != invalid {
		t.Errorf("the editor reopened with:\n%s\nwant:\n%s", seen, invalid)
	}
	if got := get(t, alice.open(), "a"); got.Value != "fixed" {
		t.Errorf("a is %q", got.Value)
	}
	wantNoTempFiles(t)
}

func TestSecretEditRefusesInvalidSecrets(t *testing.T) {
	tests := map[string]struct{ text, want string }{
		"unknown field":          {"a:\n  value: x\n  vaule: y\n", "vaule"},
		"unknown generate field": {"a:\n  generate: {type: hex, size: 3}\n", "size"},
		"unknown generator":      {"a:\n  generate: {type: pin}\n", `unknown generator type "pin"`},
		"duplicate name":         {"a:\n  value: x\na:\n  value: y\n", `"a"`},
		"no value":               {"a:\n  rotate: true\n", `secret "a" needs a value or generate settings`},
		"not a mapping":          {"- a\n", "mapping"},
		"second document":        {"a:\n  value: x\n---\nb:\n  value: y\n", "one YAML document"},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			alice := newStore(t, "alice")
			before := readFile(t, alice.storePath())
			e := scriptEditor(t, "editor", "", tt.text)
			alice.terminal.lines = []string{"n"}

			r := alice.mustRun("secret", "edit")

			wantContains(t, "stderr", r.stderr, tt.want, "Nothing saved")
			if e.runs() != 1 {
				t.Errorf("the editor ran %d times", e.runs())
			}
			if readFile(t, alice.storePath()) != before {
				t.Error("the store changed")
			}
			wantNoTempFiles(t)
		})
	}
}

func TestSecretEditWithoutTerminalFailsOnInvalidText(t *testing.T) {
	alice := newStore(t, "alice")
	before := readFile(t, alice.storePath())
	scriptEditor(t, "editor", "", "a: [\n")
	alice.terminal = nil

	r := alice.run("secret", "edit")

	if r.err == nil {
		t.Fatal("edit succeeded")
	}
	if readFile(t, alice.storePath()) != before {
		t.Error("the store changed")
	}
	wantNoTempFiles(t)
}

func TestSecretEditUsesAPrivateDirectory(t *testing.T) {
	alice := newStore(t, "alice")
	e := scriptEditor(t, "editor", "")

	alice.mustRun("secret", "edit")

	if dir := e.record("dir", 1); !strings.HasPrefix(dir, "drwx------") {
		t.Errorf("the file's directory is %s", dir)
	}
	wantNoTempFiles(t)
}

func TestSecretEditPrefersVisualAndTakesArguments(t *testing.T) {
	alice := newStore(t, "alice")
	e := scriptEditor(t, "code", "--wait")
	t.Setenv("VISUAL", os.Getenv("EDITOR"))
	t.Setenv("EDITOR", "false")

	alice.mustRun("secret", "edit")

	if args := e.record("args", 1); !strings.HasPrefix(args, "--wait\n") {
		t.Errorf("the editor got arguments:\n%s", args)
	}
}

func TestSecretEditTurnsOffVimFiles(t *testing.T) {
	for _, name := range []string{"vi", "vim", "nvim"} {
		t.Run(name, func(t *testing.T) {
			alice := newStore(t, "alice")
			e := scriptEditor(t, name, "")

			alice.mustRun("secret", "edit")

			wantContains(t, "arguments", e.record("args", 1), "-n\n", "-i\nNONE\n", "--cmd\nset nobackup nowritebackup noundofile\n", "-c\nset nobackup nowritebackup noundofile\n")
		})
	}
}

func TestSecretEditSurvivesCtrlCInTheEditor(t *testing.T) {
	alice := newStore(t, "alice")
	e := scriptEditor(t, "editor", "", "a:\n  value: kept\n")
	e.hook(1, fmt.Sprintf("kill -INT %d; sleep 0.2\n", os.Getpid()))

	alice.mustRun("secret", "edit")

	if got := get(t, alice.open(), "a"); got.Value != "kept" {
		t.Errorf("a is %q", got.Value)
	}
	if len(alice.exitCodes()) > 0 {
		t.Errorf("mooctl exited with %v", alice.exitCodes())
	}
	wantNoTempFiles(t)
}

func TestSecretEditRemovesThePlaintextOnCtrlCAtThePrompt(t *testing.T) {
	alice := newStore(t, "alice")
	before := readFile(t, alice.storePath())
	scriptEditor(t, "editor", "", "a: [\n")
	alice.terminal.onReadLine = func() {
		syscall.Kill(os.Getpid(), syscall.SIGINT)
		deadline := time.Now().Add(5 * time.Second)
		for len(alice.exitCodes()) == 0 && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
	}

	alice.run("secret", "edit")

	if got := alice.exitCodes(); len(got) != 1 || got[0] != 130 {
		t.Fatalf("mooctl exited with %v, want 130", got)
	}
	if alice.tempFilesAtExit != 0 {
		t.Errorf("%d files were left in the temporary directory when mooctl exited", alice.tempFilesAtExit)
	}
	if readFile(t, alice.storePath()) != before {
		t.Error("the store changed")
	}
}

func TestSecretEditRepairsAValueFromAnotherDataKey(t *testing.T) {
	alice := newStore(t, "alice")
	bob := alice.teammate("bob")
	alice.mustRun("secret", "recipients", "add", bob.key+".pub")
	s := alice.open()
	put(t, s, "kept", secretstore.Secret{Value: "k"})
	put(t, s, "stale", secretstore.Secret{Value: "old"})
	save(t, s)
	stale := valueLine(t, readFile(t, alice.storePath()), "stale")
	alice.mustRun("secret", "recipients", "rm", "bob")
	replaceValueLine(t, alice.storePath(), "stale", stale)

	r := alice.run("secret", "view")
	if r.err == nil || !strings.Contains(r.err.Error(), `secret "stale"`) {
		t.Fatalf("view gave %v, want an error naming stale", r.err)
	}

	e := scriptEditor(t, "editor", "")
	alice.mustRun("secret", "edit")
	wantContains(t, "editor", e.record("seen", 1), "# secret \"stale\" was encrypted with another data key", "stale:\n  value: \"\"\n")
	if valueLine(t, readFile(t, alice.storePath()), "stale") != stale {
		t.Error("an unchanged stale secret was changed")
	}

	scriptEditor(t, "editor", "", "kept:\n  value: k\nstale:\n  value: \"\"\n  rotate: true\n")
	alice.terminal.lines = []string{"n"}
	r = alice.mustRun("secret", "edit")
	wantContains(t, "stderr", r.stderr, `secret "stale" can't be decrypted, so its settings can't change without a value`)

	scriptEditor(t, "editor", "", "kept:\n  value: k\nstale:\n  value: new\n")
	alice.mustRun("secret", "edit")
	if got := get(t, alice.open(), "stale"); got.Value != "new" {
		t.Errorf("stale is %q", got.Value)
	}
}

// valueLine is the line holding the encrypted value of the secret called
// name, in a store's file.
func valueLine(t *testing.T, file, name string) string {
	t.Helper()
	m := regexp.MustCompile(`(?m)^  - name: ` + regexp.QuoteMeta(name) + `\n(?:    .*\n)*?(    value: .*)$`).FindStringSubmatch(file)
	if m == nil {
		t.Fatalf("no value for %s in:\n%s", name, file)
	}
	return m[1]
}

// replaceValueLine replaces the encrypted value of the secret called name
// in the store at path with line.
func replaceValueLine(t *testing.T, path, name, line string) {
	t.Helper()
	file := readFile(t, path)
	writeFile(t, path, []byte(strings.Replace(file, valueLine(t, file, name), line, 1)))
}

// rotationStore makes a store with a marked and an unmarked generated
// secret, and a marked secret without generate settings.
func rotationStore(t *testing.T) *operator {
	t.Helper()
	alice := newStore(t, "alice")
	s := alice.open()
	put(t, s, "marked", secretstore.Secret{Value: "m", Generate: &secretstore.Generate{Type: "hex", Length: 4}, Rotate: true})
	put(t, s, "unmarked", secretstore.Secret{Value: "u", Generate: &secretstore.Generate{Type: "uuid"}})
	put(t, s, "typed", secretstore.Secret{Value: "t", Rotate: true})
	save(t, s)
	return alice
}

func TestSecretRotateByMark(t *testing.T) {
	alice := rotationStore(t)

	r := alice.mustRun("secret", "rotate")

	wantContains(t, "stdout", r.stdout, "Rotated marked")
	wantContains(t, "stderr", r.stderr, `secret "typed" is marked for rotation, but it has no generate settings`, "change it by hand")
	s := alice.open()
	if got := get(t, s, "marked"); !regexp.MustCompile(`^[0-9a-f]{8}$`).MatchString(got.Value) || got.Rotate {
		t.Errorf("marked is %+v", got)
	}
	if got := get(t, s, "unmarked"); got.Value != "u" {
		t.Errorf("unmarked was rotated: %+v", got)
	}
	if got := get(t, s, "typed"); got.Value != "t" || !got.Rotate {
		t.Errorf("typed changed: %+v", got)
	}
}

func TestSecretRotateByName(t *testing.T) {
	alice := rotationStore(t)

	r := alice.mustRun("secret", "rotate", "unmarked", "unmarked")

	wantContains(t, "stdout", r.stdout, "Rotated unmarked\n")

	s := alice.open()
	if got := get(t, s, "unmarked"); !regexp.MustCompile(`^[0-9a-f-]{36}$`).MatchString(got.Value) {
		t.Errorf("unmarked is %+v", got)
	}
	if got := get(t, s, "marked"); got.Value != "m" || !got.Rotate {
		t.Errorf("marked changed: %+v", got)
	}
}

func TestSecretRotateRefusesASecretWithoutGenerate(t *testing.T) {
	alice := rotationStore(t)
	before := readFile(t, alice.storePath())

	r := alice.run("secret", "rotate", "marked", "typed")

	if r.err == nil {
		t.Fatal("rotate succeeded")
	}
	wantContains(t, "error", r.err.Error(), `"typed"`, "by hand", "rotate mark")
	if readFile(t, alice.storePath()) != before {
		t.Error("the store changed")
	}
}

func TestSecretRotateRefusesAnUnknownSecret(t *testing.T) {
	alice := rotationStore(t)

	r := alice.run("secret", "rotate", "nope")

	if r.err == nil || !strings.Contains(r.err.Error(), `"nope"`) {
		t.Errorf("got %v, want an error naming nope", r.err)
	}
}

func TestSecretRotateAfterRmRotatesEveryGeneratedSecret(t *testing.T) {
	alice := rotationStore(t)
	bob := alice.teammate("bob")
	alice.mustRun("secret", "recipients", "add", bob.key+".pub")
	alice.mustRun("secret", "recipients", "rm", "bob")

	r := alice.mustRun("secret", "rotate")

	wantContains(t, "stdout", r.stdout, "Rotated marked, unmarked")
	if marked := alice.open().MarkedForRotation(); len(marked) != 1 || marked[0] != "typed" {
		t.Errorf("still marked: %v, want only typed", marked)
	}
}

// gitRepo makes o's cluster directory a git repository with the secret
// store committed on main.
func gitRepo(t *testing.T, o *operator) {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git isn't installed")
	}
	o.git("init", "-q", "-b", "main")
	o.git("add", ".")
	o.git("commit", "-q", "-m", "secret store")
}

// git runs git in o's cluster directory, which must succeed.
func (o *operator) git(args ...string) string {
	o.t.Helper()
	cmd := exec.Command("git", append([]string{"-c", "user.name=test", "-c", "user.email=test@example.com", "-c", "commit.gpgsign=false"}, args...)...)
	cmd.Dir = o.dir
	cmd.Env = append(cmd.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		o.t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}

// commitEdit edits the secrets so that they become text, and commits.
func (o *operator) commitEdit(text string) {
	o.t.Helper()
	scriptEditor(o.t, "editor", "", text)
	o.mustRun("secret", "edit")
	o.git("commit", "-q", "-am", "edit secrets")
}

func TestSecretChangesOnTwoBranchesMergeCleanly(t *testing.T) {
	alice := newStore(t, "alice")
	gitRepo(t, alice)
	alice.commitEdit("a:\n  value: a1\nb:\n  value: b1\nc:\n  value: c1\n")

	alice.git("checkout", "-q", "-b", "other")
	alice.commitEdit("a:\n  value: a2\nb:\n  value: b1\nc:\n  value: c1\n")
	alice.git("checkout", "-q", "main")
	alice.commitEdit("a:\n  value: a1\nb:\n  value: b1\nc:\n  value: c2\n")
	alice.git("merge", "-q", "--no-edit", "other")

	got := alice.mustRun("secret", "view").stdout
	want := "a:\n  value: a2\nb:\n  value: b1\nc:\n  value: c2\n"
	if got != want {
		t.Errorf("after the merge, view printed:\n%s\nwant:\n%s", got, want)
	}
}

func TestSecretAddedAcrossARecipientChangeNamesTheSecret(t *testing.T) {
	alice := newStore(t, "alice")
	bob := alice.teammate("bob")
	alice.mustRun("secret", "recipients", "add", bob.key+".pub")
	gitRepo(t, alice)

	alice.git("checkout", "-q", "-b", "other")
	alice.mustRun("secret", "recipients", "rm", "bob")
	alice.git("commit", "-q", "-am", "remove bob")
	alice.git("checkout", "-q", "main")
	alice.commitEdit("new:\n  value: n\n")
	alice.git("merge", "-q", "--no-edit", "other")

	r := alice.run("secret", "view")

	if r.err == nil {
		t.Fatalf("view succeeded after the merge:\n%s", r.stdout)
	}
	wantContains(t, "error", r.err.Error(), `secret "new"`, "another data key", "mooctl secret edit")
}

func TestSecretEditKeepsTheSettingsOfAValueItSetsAgain(t *testing.T) {
	alice := newStore(t, "alice")
	s := alice.open()
	put(t, s, "db", secretstore.Secret{Value: "old", Generate: &secretstore.Generate{Type: "hex", Length: 4}, Rotate: true})
	save(t, s)
	replaceValueLine(t, alice.storePath(), "db", "    value: enc:v1:0000000000000000:AAAA")

	e := scriptEditor(t, "editor", "")
	alice.mustRun("secret", "edit")
	wantContains(t, "editor", e.record("seen", 1), "db:\n  value: \"\"\n  generate: {type: hex, length: 4}\n  rotate: true\n")

	scriptEditor(t, "editor", "", "db:\n  value: new\n  generate: {type: hex, length: 4}\n  rotate: true\n")
	alice.mustRun("secret", "edit")

	got := get(t, alice.open(), "db")
	if got.Value != "new" || !got.Rotate || got.Generate == nil || *got.Generate != (secretstore.Generate{Type: "hex", Length: 4}) {
		t.Errorf("db is %+v, %+v; want the new value with its settings kept", got, got.Generate)
	}
}

func TestSecretEditRemovesThePlaintextWhenTheSessionEnds(t *testing.T) {
	for name, sig := range map[string]syscall.Signal{"SIGTERM": syscall.SIGTERM, "SIGHUP": syscall.SIGHUP} {
		t.Run(name, func(t *testing.T) {
			alice := newStore(t, "alice")
			before := readFile(t, alice.storePath())
			e := scriptEditor(t, "editor", "", "a:\n  value: unsaved\n")
			e.hook(1, fmt.Sprintf("kill -%s %d; sleep 0.2\n", strings.TrimPrefix(name, "SIG"), os.Getpid()))

			alice.run("secret", "edit")

			if got := alice.exitCodes(); len(got) != 1 || got[0] != 128+int(sig) {
				t.Fatalf("mooctl exited with %v, want %d", got, 128+int(sig))
			}
			if alice.tempFilesAtExit != 0 {
				t.Errorf("%d files were left in the temporary directory when mooctl exited", alice.tempFilesAtExit)
			}
			if readFile(t, alice.storePath()) != before {
				t.Error("the store changed")
			}
		})
	}
}

func TestSecretEditRefusesToOverwriteAChangeMadeMeanwhile(t *testing.T) {
	alice := newStore(t, "alice")
	e := scriptEditor(t, "editor", "", "a:\n  value: mine\n")
	e.hook(1, fmt.Sprintf("printf '# a teammate was here\\n' >> '%s'\n", alice.storePath()))

	r := alice.run("secret", "edit")

	if !errors.Is(r.err, secretstore.ErrChanged) {
		t.Fatalf("got %v, want ErrChanged", r.err)
	}
	wantContains(t, "error", r.err.Error(), "your edit is lost")
	wantContains(t, "store", readFile(t, alice.storePath()), "# a teammate was here")
}

func TestSecretRecipientsRmWithNothingMarkedDoesntSuggestRotate(t *testing.T) {
	alice := newStore(t, "alice")
	bob := alice.teammate("bob")
	alice.mustRun("secret", "recipients", "add", bob.key+".pub")

	r := alice.mustRun("secret", "recipients", "rm", "bob")

	wantContains(t, "stdout", r.stdout, "Marked 0 secrets for rotation; 0 now marked.")
	if strings.Contains(r.stdout, "mooctl secret rotate") {
		t.Errorf("stdout suggests rotating nothing:\n%s", r.stdout)
	}
}
