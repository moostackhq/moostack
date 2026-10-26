package cli

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"text/tabwriter"

	"github.com/urfave/cli/v3"

	"github.com/moostackhq/moostack/internal/secretedit"
	"github.com/moostackhq/moostack/internal/secretstore"
	"github.com/moostackhq/moostack/internal/terminal"
)

// secretCommand is mooctl secret.
func secretCommand(env Env) *cli.Command {
	return &cli.Command{
		Name:  "secret",
		Usage: "keep secrets in the cluster directory's secret store",
		Commands: []*cli.Command{
			{
				Name:   "init",
				Usage:  "create the secret store, encrypted to one of your SSH keys",
				Action: secretAction(env, 0, secretInit),
			},
			{
				Name:   "edit",
				Usage:  "edit the secrets in $VISUAL or $EDITOR",
				Action: secretAction(env, 0, secretEdit),
			},
			{
				Name:   "view",
				Usage:  "print the decrypted secrets",
				Action: secretAction(env, 0, secretView),
			},
			{
				Name:      "rotate",
				Usage:     "generate new values for the named secrets, or for every secret marked rotate",
				ArgsUsage: "[name...]",
				Action:    secretAction(env, anyArgs, secretRotate),
			},
			{
				Name:  "recipients",
				Usage: "manage who can read the secret store",
				Commands: []*cli.Command{
					{
						Name:   "ls",
						Usage:  "list the recipients",
						Action: secretAction(env, 0, recipientsLs),
					},
					{
						Name:      "add",
						Usage:     "add a recipient",
						ArgsUsage: `<key.pub | "ssh-ed25519 AAAA...">`,
						Action:    secretAction(env, 1, recipientsAdd),
					},
					{
						Name:      "rm",
						Usage:     "remove a recipient, and mark generated secrets for rotation",
						ArgsUsage: "<fingerprint | comment>",
						Action:    secretAction(env, 1, recipientsRm),
					},
				},
			},
		},
	}
}

// secretSession is what a secret command works with: the Env, the
// command, the store's path, the operator's terminal and the command's
// arguments.
type secretSession struct {
	env      Env
	cmd      *cli.Command
	path     string
	terminal terminal.Terminal
	args     []string
	// changed says the command changed the store.
	changed bool
}

// anyArgs is the nargs of a command that takes any number of arguments.
const anyArgs = -1

// secretAction is the action of a secret command that takes nargs
// arguments, or any number if nargs is anyArgs. It opens the terminal
// for run and closes it afterwards.
func secretAction(env Env, nargs int, run func(*secretSession) error) cli.ActionFunc {
	return func(ctx context.Context, cmd *cli.Command) error {
		if nargs != anyArgs {
			if err := wantArgs(cmd, nargs); err != nil {
				return err
			}
		}
		t, closeTerminal := openTerminal(env)
		defer closeTerminal()
		s := &secretSession{
			env:      env,
			cmd:      cmd,
			path:     filepath.Join(clusterDir(cmd), secretstore.FileName),
			terminal: t,
			args:     cmd.Args().Slice(),
		}
		if err := run(s); err != nil {
			return err
		}
		if s.changed {
			remindToCommit(env)
		}
		return nil
	}
}

// open finds the operator's key, asking for its passphrase if needed,
// and opens the store with it. Each command calls it once, so the
// passphrase is asked for once.
func (s *secretSession) open() (*secretstore.Store, error) {
	store, err := secretstore.Unlock(s.path, keyOptions(s.env, s.cmd, s.terminal))
	if errors.Is(err, secretstore.ErrNoStore) {
		return nil, fmt.Errorf("%w; create one with mooctl secret init", err)
	}
	return store, err
}

// runAgain is what to do when a command's save finds that someone else
// saved meanwhile.
const runAgain = "run the command again"

// save saves store. If someone else saved it meanwhile, the error ends
// with again, which says what the operator should do.
func (s *secretSession) save(store *secretstore.Store, again string) error {
	err := store.Save()
	if errors.Is(err, secretstore.ErrChanged) {
		return fmt.Errorf("%w; %s", err, again)
	}
	if err != nil {
		return err
	}
	s.changed = true
	return nil
}

// secretInit is mooctl secret init.
func secretInit(s *secretSession) error {
	r, err := secretstore.ChooseKey(s.path, keyOptions(s.env, s.cmd, s.terminal))
	if err != nil {
		return err
	}
	if _, err := secretstore.Init(s.path, r); err != nil {
		return err
	}
	s.changed = true
	fmt.Fprintf(s.env.Stdout, "Created %s for %s %s\n", s.path, r.Fingerprint(), r.Comment)
	changed, err := secretstore.IgnoreLockFile(filepath.Dir(s.path))
	if err != nil {
		return fmt.Errorf("adding %s to .gitignore: %w", secretstore.LockFileName, err)
	}
	if changed {
		fmt.Fprintf(s.env.Stdout, "Added %s to .gitignore\n", secretstore.LockFileName)
	}
	return nil
}

// secretEdit is mooctl secret edit.
func secretEdit(s *secretSession) error {
	store, err := s.open()
	if err != nil {
		return err
	}
	result, err := secretedit.Edit(store, secretedit.Options{Terminal: s.terminal, Errors: s.env.Stderr, Exit: s.env.Exit})
	if err != nil || result.Declined {
		return err
	}
	if !result.Changed {
		fmt.Fprintln(s.env.Stdout, "No changes.")
		return nil
	}
	if err := s.save(store, "your edit is lost, so make it again"); err != nil {
		return err
	}
	if len(result.Generated) > 0 {
		fmt.Fprintf(s.env.Stdout, "Generated %s\n", strings.Join(result.Generated, ", "))
	}
	fmt.Fprintf(s.env.Stdout, "Saved %s\n", s.path)
	return nil
}

// secretView is mooctl secret view.
func secretView(s *secretSession) error {
	store, err := s.open()
	if err != nil {
		return err
	}
	data, err := secretedit.View(store)
	if errors.Is(err, secretedit.ErrUnreadable) {
		return fmt.Errorf("%w with mooctl secret edit", err)
	}
	if err != nil {
		return err
	}
	_, err = s.env.Stdout.Write(data)
	return err
}

// secretRotate is mooctl secret rotate.
func secretRotate(s *secretSession) error {
	store, err := s.open()
	if err != nil {
		return err
	}
	rotated, byHand, err := store.Rotate(s.args...)
	if err != nil {
		return err
	}
	for _, name := range byHand {
		fmt.Fprintf(s.env.Stderr, "secret %q is marked for rotation, but %v\n", name, secretstore.ErrNoGenerate)
	}
	if len(rotated) == 0 {
		fmt.Fprintln(s.env.Stdout, "Nothing to rotate.")
		return nil
	}
	if err := s.save(store, runAgain); err != nil {
		return err
	}
	fmt.Fprintf(s.env.Stdout, "Rotated %s\n", strings.Join(rotated, ", "))
	return nil
}

// recipientsLs is mooctl secret recipients ls.
func recipientsLs(s *secretSession) error {
	store, err := s.open()
	if err != nil {
		return err
	}
	w := tabwriter.NewWriter(s.env.Stdout, 0, 0, 2, ' ', 0)
	for _, r := range store.Recipients() {
		fmt.Fprintf(w, "%s\t%s\t%s\n", r.Fingerprint(), r.Key.Type(), r.Comment)
	}
	return w.Flush()
}

// recipientsAdd is mooctl secret recipients add.
func recipientsAdd(s *secretSession) error {
	r, err := secretstore.ReadRecipient(s.args[0])
	if err != nil {
		return err
	}
	store, err := s.open()
	if err != nil {
		return err
	}
	if err := store.AddRecipient(r); err != nil {
		return err
	}
	if err := s.save(store, runAgain); err != nil {
		return err
	}
	// The line can differ from what the operator gave: options are
	// dropped, and a .pub file's comment is cleaned up.
	fmt.Fprintf(s.env.Stdout, "Added %s\n", r)
	return nil
}

// recipientsRm is mooctl secret recipients rm.
func recipientsRm(s *secretSession) error {
	store, err := s.open()
	if err != nil {
		return err
	}
	r, err := store.FindRecipient(s.args[0])
	if err != nil {
		return err
	}
	marked, err := store.RemoveRecipient(r.Key)
	if err != nil {
		return err
	}
	if err := s.save(store, runAgain); err != nil {
		return err
	}
	fmt.Fprintf(s.env.Stdout, "Removed %s %s\n", r.Fingerprint(), r.Comment)
	total := len(store.MarkedForRotation())
	fmt.Fprintf(s.env.Stdout, "Marked %s for rotation; %d now marked.\n", plural(len(marked), "secret"), total)
	if total > 0 {
		fmt.Fprintln(s.env.Stdout, "Run mooctl secret rotate when you are ready.")
	}
	fmt.Fprintln(s.env.Stderr, "The removed recipient can't read anything written from now on, but may have copies of every current value, the cluster CA key included. Secrets without generate settings aren't marked; change them by hand.")
	return nil
}
