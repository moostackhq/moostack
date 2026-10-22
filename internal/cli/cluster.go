package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/urfave/cli/v3"

	"github.com/moostackhq/moostack/internal/clusterdir"
	"github.com/moostackhq/moostack/internal/gitrepo"
	"github.com/moostackhq/moostack/internal/secretstore"
)

// clusterCommand is mooctl cluster.
func clusterCommand(env Env) *cli.Command {
	return &cli.Command{
		Name:  "cluster",
		Usage: "set up and inspect a cluster",
		Commands: []*cli.Command{
			{
				Name:      "init",
				Usage:     "create a cluster directory, with its secret store",
				ArgsUsage: "<cluster ref>",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "email", Usage: "the email of the Let's Encrypt account that apps' certificates are issued to; without it, they are self-signed"},
				},
				Action: func(ctx context.Context, cmd *cli.Command) error { return clusterInit(ctx, env, cmd) },
			},
		},
	}
}

// clusterInit is mooctl cluster init.
func clusterInit(ctx context.Context, env Env, cmd *cli.Command) error {
	if err := wantArgs(cmd, 1); err != nil {
		return err
	}
	ref, dir, err := refDir(env, cmd, cmd.Args().First())
	if err != nil {
		return err
	}
	if _, err := os.Lstat(dir); err == nil {
		return existsError(ctx, dir)
	}
	if err := (gitrepo.Repo{Dir: env.Home}).CheckIdentity(ctx); err != nil {
		return err
	}
	t, closeTerminal := openTerminal(env)
	defer closeTerminal()
	storePath := filepath.Join(dir, secretstore.FileName)
	recipient, err := secretstore.ChooseKey(storePath, keyOptions(env, cmd, t))
	if err != nil {
		return err
	}

	if _, err := clusterdir.Init(dir, clusterdir.Cluster{Ref: ref.String(), K3s: K3sVersion, Email: cmd.String("email")}); err != nil {
		if errors.Is(err, clusterdir.ErrExists) {
			return existsError(ctx, dir)
		}
		return err
	}
	// From here on, a failure removes the directory Init made.
	created := false
	defer func() {
		if !created {
			os.RemoveAll(dir)
		}
	}()
	if _, err := secretstore.Init(storePath, recipient); err != nil {
		return err
	}
	if _, err := secretstore.IgnoreLockFile(dir); err != nil {
		return err
	}
	if err := ignoreTemporaryFiles(dir); err != nil {
		return err
	}
	repo, err := gitrepo.Init(ctx, dir)
	if err != nil {
		return err
	}
	if _, err := repo.Commit(ctx, "cluster init "+ref.String()); err != nil {
		return err
	}
	created = true
	fmt.Fprintf(env.Stdout, "Created cluster %s\n", ref)
	if r, err := root(env); err == nil && ref.Dir(r) == dir {
		def, err := defaultCluster(env)
		if err != nil {
			return err
		}
		if def == "" {
			if err := setDefaultCluster(env, ref); err != nil {
				return err
			}
			def = ref.String()
		}
		fmt.Fprintf(env.Stdout, "Default cluster: %s\n", def)
	}
	return nil
}

// temporaryPatterns are the files a write leaves behind when it is cut
// off: clusterdir's and the secret store's.
const temporaryPatterns = "*.tmp\n." + secretstore.FileName + ".*\n"

// ignoreTemporaryFiles adds temporaryPatterns to dir's .gitignore.
func ignoreTemporaryFiles(dir string) error {
	f, err := os.OpenFile(filepath.Join(dir, ".gitignore"), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	_, err = f.WriteString(temporaryPatterns)
	return errors.Join(err, f.Close())
}

// existsError refuses dir, which exists, saying how to clear it if it
// has no commit, as when a cluster init was cut off.
func existsError(ctx context.Context, dir string) error {
	err := fmt.Errorf("%s %w", dir, clusterdir.ErrExists)
	if repo, oerr := gitrepo.Open(ctx, dir); oerr != nil {
		return fmt.Errorf("%w, without a commit; if a cluster init was cut off, remove it", err)
	} else if _, herr := repo.Head(ctx); herr != nil {
		return fmt.Errorf("%w, without a commit; if a cluster init was cut off, remove it", err)
	}
	return err
}
