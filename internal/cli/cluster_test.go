package cli_test

import (
	"bytes"
	"crypto"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/moostackhq/moostack/internal/filelock"
	"github.com/moostackhq/moostack/internal/secretstore"
)

func TestClusterInitPinsMooctlsRelease(t *testing.T) {
	o := newOperator(t, "op")
	o.version = "0.4.0"
	r := o.mustRun("cluster", "init", "--ssh-key", o.key, "--name", "prod")
	if got, want := readFile(t, filepath.Join(o.dir, "cluster.yml")), "name: prod\nrelease: 0.4.0\ngeneration: 1\n"; got != want {
		t.Errorf("cluster.yml is\n%s\nwant\n%s", got, want)
	}
	if want := "Created cluster prod on release 0.4.0 in " + o.dir + "\n"; !strings.HasPrefix(r.stdout, want) {
		t.Errorf("cluster init printed %q, want it to start with %q", r.stdout, want)
	}
}

func TestClusterInitNamesTheClusterAfterItsDirectory(t *testing.T) {
	o := newOperator(t, "op")
	o.dir = filepath.Join(t.TempDir(), "staging")
	o.mustRun("cluster", "init", "--ssh-key", o.key)
	if got := readFile(t, filepath.Join(o.dir, "cluster.yml")); !strings.HasPrefix(got, "name: staging\n") {
		t.Errorf("cluster.yml is\n%s\nwant the name staging", got)
	}
}

func TestClusterInitCreatesAMissingDirectory(t *testing.T) {
	o := newOperator(t, "op")
	o.dir = filepath.Join(t.TempDir(), "a", "prod")
	o.mustRun("cluster", "init", "--ssh-key", o.key)
	if _, err := os.Stat(filepath.Join(o.dir, "cluster.yml")); err != nil {
		t.Error(err)
	}
}

func TestClusterInitRefusesAnExistingClusterDirectory(t *testing.T) {
	o := newOperator(t, "op")
	o.mustRun("cluster", "init", "--ssh-key", o.key, "--name", "prod")
	before := readFile(t, filepath.Join(o.dir, "cluster.yml"))
	o.version = "9.9.9"
	r := o.run("cluster", "init", "--ssh-key", o.key, "--name", "other")
	if r.err == nil || !strings.Contains(r.err.Error(), "already a cluster directory") {
		t.Errorf("a second cluster init: %v, want an error saying it is already a cluster directory", r.err)
	}
	if got := readFile(t, filepath.Join(o.dir, "cluster.yml")); got != before {
		t.Errorf("a second cluster init changed cluster.yml to\n%s", got)
	}
}

func TestClusterInitRefusesAnInvalidName(t *testing.T) {
	o := newOperator(t, "op")
	r := o.run("cluster", "init", "--ssh-key", o.key, "--name", "Prod_1")
	if r.err == nil {
		t.Fatal("cluster init accepted the name Prod_1")
	}
	if _, err := os.Stat(filepath.Join(o.dir, "cluster.yml")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("cluster init with an invalid name left cluster.yml: %v", err)
	}
}

func TestClusterInitWithoutAReleaseFails(t *testing.T) {
	o := newOperator(t, "op")
	o.version = ""
	r := o.run("cluster", "init", "--ssh-key", o.key, "--name", "prod")
	if r.err == nil || !strings.Contains(r.err.Error(), "release build") {
		t.Errorf("cluster init from a build without a release: %v, want an error naming the release build", r.err)
	}
	if _, err := os.Stat(filepath.Join(o.dir, "cluster.yml")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("cluster init without a release left cluster.yml: %v", err)
	}
}

func TestClusterInitTakesNoArguments(t *testing.T) {
	o := newOperator(t, "op")
	if r := o.run("cluster", "init", "--ssh-key", o.key, "prod"); r.err == nil {
		t.Error("cluster init accepted an argument")
	}
}

func TestClusterInitAsksForANameWhenTheDirectorysWontDo(t *testing.T) {
	o := newOperator(t, "op")
	o.dir = filepath.Join(t.TempDir(), "My Cluster")
	r := o.run("cluster", "init", "--ssh-key", o.key)
	if r.err == nil || !strings.Contains(r.err.Error(), "--name") {
		t.Errorf("cluster init in %q: %v, want an error that suggests --name", o.dir, r.err)
	}
}

func TestClusterInitCreatesTheClusterCAWithItsKeyInTheSecretStore(t *testing.T) {
	o := newOperator(t, "op")
	o.mustRun("cluster", "init", "--ssh-key", o.key, "--name", "prod")

	block, _ := pem.Decode([]byte(readFile(t, filepath.Join(o.dir, "ca.crt"))))
	if block == nil || block.Type != "CERTIFICATE" {
		t.Fatalf("ca.crt holds no certificate")
	}
	ca, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if !ca.IsCA || ca.KeyUsage&x509.KeyUsageCertSign == 0 {
		t.Errorf("ca.crt isn't a CA that signs certificates: IsCA %v, key usage %b", ca.IsCA, ca.KeyUsage)
	}
	if err := ca.CheckSignatureFrom(ca); err != nil {
		t.Errorf("ca.crt isn't self-signed: %v", err)
	}
	if ca.NotAfter.Year() != 9999 {
		t.Errorf("the CA expires on %s, want it never to", ca.NotAfter)
	}
	if !strings.Contains(ca.Subject.CommonName, "prod") {
		t.Errorf("the CA is %q, want one that names the cluster prod", ca.Subject.CommonName)
	}

	secret, err := o.open().Get("cluster-ca-key")
	if err != nil {
		t.Fatalf("the secret store has no CA key: %v", err)
	}
	keyBlock, _ := pem.Decode([]byte(secret.Value))
	if keyBlock == nil {
		t.Fatalf("the CA key isn't PEM:\n%s", secret.Value)
	}
	key, err := x509.ParsePKCS8PrivateKey(keyBlock.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	signer, ok := key.(crypto.Signer)
	if !ok || !signer.Public().(interface{ Equal(crypto.PublicKey) bool }).Equal(ca.PublicKey) {
		t.Errorf("the stored key isn't the key of ca.crt")
	}

	// The key appears nowhere in the directory in plaintext.
	filepath.WalkDir(o.dir, func(path string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && bytes.Contains([]byte(readFile(t, path)), keyBlock.Bytes[len(keyBlock.Bytes)-32:]) {
			t.Errorf("%s holds the CA key", path)
		}
		if err == nil && !d.IsDir() && strings.Contains(readFile(t, path), "PRIVATE KEY") {
			t.Errorf("%s holds a private key", path)
		}
		return nil
	})
}

func TestClusterInitIgnoresTheSecretStoresLockFile(t *testing.T) {
	o := newOperator(t, "op")
	o.mustRun("cluster", "init", "--ssh-key", o.key, "--name", "prod")
	if got := readFile(t, filepath.Join(o.dir, ".gitignore")); got != "/.secrets.enc.yml.lock\n" {
		t.Errorf(".gitignore is %q, want the store's lock file", got)
	}
}

func TestClusterInitAsksWhichKeyProtectsTheSecretStore(t *testing.T) {
	o := newOperator(t, "op")
	o.terminal.lines = []string{"1"}
	o.mustRun("cluster", "init", "--name", "prod")
	wantContains(t, "the terminal", o.terminal.out.String(), "Which SSH key protects the secret store?")
	if got := o.open().Recipients(); len(got) != 1 || got[0].String() != o.recipient.String() {
		t.Errorf("the store's recipients are %v, want only %s", got, o.recipient)
	}
}

func TestClusterInitRefusesAnExistingClusterDirectoryBeforeAskingAnything(t *testing.T) {
	o := newOperator(t, "op")
	o.mustRun("cluster", "init", "--ssh-key", o.key, "--name", "prod")
	o.terminal.out.Reset()
	r := o.run("cluster", "init", "--name", "prod")
	if r.err == nil || !strings.Contains(r.err.Error(), "already a cluster directory") {
		t.Errorf("a second cluster init: %v, want an error saying it is already a cluster directory", r.err)
	}
	if o.terminal.out.Len() > 0 || r.stderr != "" {
		t.Errorf("a second cluster init asked or warned:\nterminal:\n%s\nstderr:\n%s", o.terminal.out.String(), r.stderr)
	}
}

func TestClusterInitAddsTheCAKeyToAnExistingSecretStore(t *testing.T) {
	o := newOperator(t, "op")
	o.mustRun("secret", "init", "--ssh-key", o.key)
	o.putSecret("db-password", "hunter2")

	// With no --ssh-key and nothing to answer, it finds the key in ~/.ssh.
	r := o.mustRun("cluster", "init", "--name", "prod")
	if strings.Contains(r.stderr, "only backup") {
		t.Errorf("adopting a store warned again about losing it:\n%s", r.stderr)
	}
	wantContains(t, "cluster init's output", r.stdout, "Added the cluster CA key to "+o.storePath())
	s := o.open()
	if got, err := s.Get("db-password"); err != nil || got.Value != "hunter2" {
		t.Errorf("the operator's secret is %v, %v, want it kept", got, err)
	}
	if _, err := s.Get("cluster-ca-key"); err != nil {
		t.Errorf("the store has no CA key: %v", err)
	}
}

func TestClusterInitRefusesASecretStoreThatHasACAKey(t *testing.T) {
	o := newOperator(t, "op")
	o.mustRun("secret", "init", "--ssh-key", o.key)
	o.putSecret("cluster-ca-key", "old")
	before := snapshot(t, o.dir)
	r := o.run("cluster", "init", "--name", "prod")
	if r.err == nil || !strings.Contains(r.err.Error(), "already has a cluster CA key") {
		t.Errorf("cluster init with a CA key in the store: %v, want an error saying it has one", r.err)
	}
	wantSnapshot(t, o.dir, before)
}

func TestClusterInitByANonRecipientListsTheRecipients(t *testing.T) {
	alice := newOperator(t, "alice")
	alice.mustRun("secret", "init", "--ssh-key", alice.key)
	bob := newOperator(t, "bob")
	bob.dir = alice.dir
	before := snapshot(t, alice.dir)
	r := bob.run("cluster", "init", "--name", "prod")
	if r.err == nil {
		t.Fatal("cluster init by someone who isn't a recipient succeeded")
	}
	wantContains(t, "the error", r.err.Error(), alice.recipient.Fingerprint(), "alice", "ask one of them to add you")
	wantSnapshot(t, alice.dir, before)
}

func TestAFailedClusterInitPutsTheDirectoryBack(t *testing.T) {
	for name, setup := range map[string]func(o *operator){
		"a new store":       func(o *operator) {},
		"a .gitignore":      func(o *operator) { writeFile(t, filepath.Join(o.dir, ".gitignore"), []byte("/build\n")) },
		"an existing store": func(o *operator) { o.mustRun("secret", "init", "--ssh-key", o.key) },
	} {
		t.Run(name, func(t *testing.T) {
			o := newOperator(t, "op")
			setup(o)
			// Save can't lock a directory, so the store can't be saved,
			// once the rest has been written.
			lock := filepath.Join(o.dir, ".secrets.enc.yml.lock")
			os.Remove(lock)
			if err := os.Mkdir(lock, 0o700); err != nil {
				t.Fatal(err)
			}
			before := snapshot(t, o.dir)
			if r := o.run("cluster", "init", "--ssh-key", o.key, "--name", "prod"); r.err == nil {
				t.Fatal("cluster init succeeded without a lock file")
			}
			wantSnapshot(t, o.dir, before)

			if err := os.Remove(lock); err != nil {
				t.Fatal(err)
			}
			o.mustRun("cluster", "init", "--ssh-key", o.key, "--name", "prod")
		})
	}
}

func TestAFailedClusterInitKeepsAnotherWritersSave(t *testing.T) {
	for name, setup := range map[string]func(o *operator){
		"a new store":       func(o *operator) {},
		"an existing store": func(o *operator) { o.mustRun("secret", "init", "--ssh-key", o.key) },
	} {
		t.Run(name, func(t *testing.T) {
			o := newOperator(t, "op")
			setup(o)
			store := filepath.Join(o.dir, secretstore.FileName)
			// Holding the store's lock, the test is the other writer:
			// cluster init waits for the lock to save, and finds the
			// other writer's save once it has it.
			release, err := filelock.Lock(filepath.Join(o.dir, secretstore.LockFileName))
			if err != nil {
				t.Fatal(err)
			}
			unlock := sync.OnceFunc(release)
			t.Cleanup(unlock)
			done := make(chan result, 1)
			go func() { done <- o.run("cluster", "init", "--ssh-key", o.key, "--name", "prod") }()
			waitFor(t, filepath.Join(o.dir, "cluster.yml"), store)
			theirs := []byte("another writer's save\n")
			writeFile(t, store, theirs)
			unlock()

			r := <-done
			if !errors.Is(r.err, secretstore.ErrChanged) {
				t.Fatalf("cluster init after another writer's save: %v, want %v", r.err, secretstore.ErrChanged)
			}
			if got := readFile(t, store); got != string(theirs) {
				t.Errorf("the failed cluster init undid another writer's save: the store is\n%s", got)
			}
			for _, name := range []string{"cluster.yml", "ca.crt"} {
				if _, err := os.Lstat(filepath.Join(o.dir, name)); !errors.Is(err, fs.ErrNotExist) {
					t.Errorf("the failed cluster init left %s behind: %v", name, err)
				}
			}
		})
	}
}

// waitFor waits until every one of paths exists.
func waitFor(t *testing.T, paths ...string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for _, p := range paths {
		for {
			if _, err := os.Lstat(p); err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s never appeared", p)
			}
			time.Sleep(time.Millisecond)
		}
	}
}

// snapshot is every path in dir, with each file's content.
func snapshot(t *testing.T, dir string) map[string]string {
	t.Helper()
	files := map[string]string{}
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			files[path] = "<dir>"
			return nil
		}
		data, err := os.ReadFile(path)
		files[path] = string(data)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}

// wantSnapshot fails unless dir is as it was in before.
func wantSnapshot(t *testing.T, dir string, before map[string]string) {
	t.Helper()
	after := snapshot(t, dir)
	for p := range after {
		if _, ok := before[p]; !ok {
			t.Errorf("%s was left behind", p)
		} else if after[p] != before[p] {
			t.Errorf("%s changed", p)
		}
	}
	for p := range before {
		if _, ok := after[p]; !ok {
			t.Errorf("%s was removed", p)
		}
	}
}
