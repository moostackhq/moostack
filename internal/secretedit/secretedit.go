// Package secretedit shows a secret store's secrets as a YAML document,
// and edits them as one in the operator's editor. The plaintext is only
// ever written to a private temporary directory, which is removed afterwards.
package secretedit

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/moostackhq/moostack/internal/interrupt"
	"github.com/moostackhq/moostack/internal/secretstore"
	"github.com/moostackhq/moostack/internal/terminal"
)

// ErrUnreadable is returned by View for a value it can't decrypt, which
// Edit can remove or set again.
var ErrUnreadable = errors.New("a value can't be decrypted")

// View is the secrets in s as the YAML document that Edit opens, without
// its explanatory comment. It fails with ErrUnreadable on a value it
// can't decrypt.
func View(s *secretstore.Store) ([]byte, error) {
	secrets, err := readSecrets(s)
	if err != nil {
		return nil, err
	}
	for _, secret := range secrets {
		if secret.err != nil {
			return nil, fmt.Errorf("%w: %w", ErrUnreadable, secret.err)
		}
	}
	return render(secrets)
}

// Options say how Edit talks to the operator.
type Options struct {
	// Terminal asks whether to reopen the editor after an edit that
	// isn't valid. It is nil when there is no terminal, and then such an
	// edit fails.
	Terminal terminal.Terminal
	// Errors is where the problem with an edit that isn't valid is
	// shown.
	Errors io.Writer
	// Exit ends the process after a signal, once the plaintext is
	// removed.
	Exit func(code int)
}

// Result is what an edit did.
type Result struct {
	// Changed says whether s changed, and must be saved.
	Changed bool
	// Generated are the secrets whose values were generated.
	Generated []string
	// Declined says that the operator didn't reopen the editor after an
	// edit that wasn't valid, so nothing changed.
	Declined bool
}

// Edit opens the secrets in s in the operator's editor, $VISUAL, else
// $EDITOR, else vi, and puts what they save into s. Unchanged values keep
// their ciphertext, and empty values with generate settings are
// generated. If what they save isn't valid, they are asked whether to
// reopen the editor with their text intact.
//
// A secret whose value can't be decrypted is shown with its settings and
// an empty value, under a comment. Deleting it removes it, giving it a
// value sets it again, and leaving it as it is keeps it.
func Edit(s *secretstore.Store, opts Options) (Result, error) {
	stored, err := readSecrets(s)
	if err != nil {
		return Result{}, err
	}
	text, err := render(stored)
	if err != nil {
		return Result{}, err
	}
	// The directory is named, and the guard set to remove it, before it
	// exists, so that a signal can't leave it behind.
	dir := filepath.Join(os.TempDir(), "mooctl-secrets-"+rand.Text())
	guard := interrupt.Catch(func() { os.RemoveAll(dir) }, opts.Exit)
	defer guard.Stop()
	if err := os.Mkdir(dir, 0o700); err != nil {
		return Result{}, err
	}
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "secrets.yml")
	text = append([]byte(header), text...)
	for {
		if err := os.WriteFile(path, text, 0o600); err != nil {
			return Result{}, err
		}
		if err := guard.Pass(func() error { return runEditor(path) }); err != nil {
			return Result{}, fmt.Errorf("%w; nothing saved", err)
		}
		if text, err = os.ReadFile(path); err != nil {
			return Result{}, err
		}
		p, err := planText(stored, text)
		if err == nil {
			if p.empty() {
				return Result{}, nil
			}
			generated, err := p.apply(s)
			return Result{Changed: true, Generated: generated}, err
		}
		if opts.Terminal == nil {
			return Result{}, fmt.Errorf("%w; nothing saved", err)
		}
		fmt.Fprintf(opts.Errors, "Error: %v\n", err)
		reopen, err := askReopen(opts.Terminal)
		if err != nil {
			return Result{}, err
		}
		if !reopen {
			fmt.Fprintln(opts.Errors, "Nothing saved.")
			return Result{Declined: true}, nil
		}
	}
}

// planText parses text and plans the edit from stored to it.
func planText(stored []storedSecret, text []byte) (plan, error) {
	edited, err := parse(text)
	if err != nil {
		return plan{}, err
	}
	return planEdit(stored, edited)
}

// askReopen asks on t whether to reopen the editor. The default is yes.
func askReopen(t terminal.Terminal) (bool, error) {
	fmt.Fprint(t, "Reopen the editor? [Y/n] ")
	line, err := t.ReadLine()
	if err != nil {
		return false, fmt.Errorf("asking whether to reopen the editor: %w; nothing saved", err)
	}
	switch strings.ToLower(strings.TrimSpace(line)) {
	case "", "y", "yes":
		return true, nil
	}
	return false, nil
}
