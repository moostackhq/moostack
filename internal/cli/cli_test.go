package cli

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/crypto/ssh"

	"github.com/moostackhq/moostack/internal/clusterdir"
	"github.com/moostackhq/moostack/internal/secretstore"
)

// sshKey writes an ed25519 key without a passphrase into dir, and returns
// its path.
func sshKey(t testing.TB, dir string) string {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	block, err := ssh.MarshalPrivateKey(priv, "op@test")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "id_ed25519")
	if err := os.WriteFile(path, pem.EncodeToMemory(block), 0o600); err != nil {
		t.Fatal(err)
	}
	sp, _ := ssh.NewPublicKey(pub)
	if err := os.WriteFile(path+".pub", append(bytes.TrimSpace(ssh.MarshalAuthorizedKey(sp)), " op@test\n"...), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// mooctl runs mooctl with args in a home of its own, with git's identity
// set, in the directory wd.
func mooctl(t testing.TB, home, wd string, args ...string) (string, error) {
	t.Helper()
	t.Chdir(wd)
	var out, errOut bytes.Buffer
	env := Env{Home: home, LocalUser: "op", Stdout: &out, Stderr: &errOut, Root: filepath.Join(home, "clusters")}
	err := New(env).Run(t.Context(), append([]string{"mooctl"}, args...))
	return out.String() + errOut.String(), err
}

func newHome(t testing.TB) string {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")
	t.Setenv("MOOCTL_DIR", "")
	os.Unsetenv("MOOCTL_DIR")
	git(t, home, "config", "--global", "user.name", "Op")
	git(t, home, "config", "--global", "user.email", "op@example.com")
	return home
}

// git runs git with args in dir, and returns what it printed.
func git(t testing.TB, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v: %s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

func TestClusterInitCreatesTheDirectoryStoreAndFirstCommit(t *testing.T) {
	home := newHome(t)
	key := sshKey(t, home)
	out, err := mooctl(t, home, home, "--ssh-key", key, "cluster", "init", "acme/prod", "--email", "op@example.com")
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	dir := filepath.Join(home, "clusters", "acme", "prod")
	d, err := clusterdir.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if d.Cluster.Ref != "acme/prod" || d.Cluster.K3s != K3sVersion || d.Cluster.Email != "op@example.com" {
		t.Errorf("cluster = %+v", d.Cluster)
	}
	if _, err := os.Stat(filepath.Join(dir, secretstore.FileName)); err != nil {
		t.Errorf("no secret store: %v", err)
	}
	if msg := git(t, dir, "log", "--format=%s"); msg != "cluster init acme/prod" {
		t.Errorf("log = %q", msg)
	}
	if status := git(t, dir, "status", "--porcelain"); status != "" {
		t.Errorf("uncommitted: %s", status)
	}

	if out, err := mooctl(t, home, home, "--ssh-key", key, "cluster", "init", "acme/prod"); err == nil {
		t.Errorf("a second init passed:\n%s", out)
	}
}

func TestAFailedInitLeavesNothing(t *testing.T) {
	home := newHome(t)
	out, err := mooctl(t, home, home, "--ssh-key", filepath.Join(home, "missing"), "cluster", "init", "prod")
	if err == nil {
		t.Fatalf("init passed without a key:\n%s", out)
	}
	if _, err := os.Stat(filepath.Join(home, "clusters", "default", "prod")); !os.IsNotExist(err) {
		t.Errorf("the directory is there: %v", err)
	}
}

func TestWhichClusterACommandActsOn(t *testing.T) {
	home := newHome(t)
	key := sshKey(t, home)
	for _, ref := range []string{"prod", "acme/staging"} {
		if out, err := mooctl(t, home, home, "--ssh-key", key, "cluster", "init", ref); err != nil {
			t.Fatalf("%v\n%s", err, out)
		}
	}
	app := t.TempDir()
	os.WriteFile(filepath.Join(app, "Dockerfile"), []byte("FROM scratch\n"), 0o644)
	os.WriteFile(filepath.Join(app, "deploy.yaml"), []byte("cluster: acme/staging\nname: web\ncontainers: {web: {build: {dockerfile: .}, port: 80}}\n"), 0o644)

	status := func(wd string, args ...string) (string, error) {
		return mooctl(t, home, wd, append(args, "cluster", "status")...)
	}
	if out, err := status(app); err != nil || !strings.Contains(out, "Cluster acme/staging") {
		t.Errorf("from the app's directory: %v\n%s", err, out)
	}
	if out, err := status(app, "-c", "prod"); err != nil || !strings.Contains(out, "Cluster prod") {
		t.Errorf("-c over deploy.yaml: %v\n%s", err, out)
	}
	if out, err := status(home, "-C", filepath.Join(home, "clusters", "default", "prod"), "-c", "acme/staging"); err != nil || !strings.Contains(out, "Cluster prod") {
		t.Errorf("-C over -c: %v\n%s", err, out)
	}
	t.Setenv("MOOCTL_DIR", filepath.Join(home, "clusters", "default", "prod"))
	if out, err := status(app); err != nil || !strings.Contains(out, "Cluster prod") {
		t.Errorf("MOOCTL_DIR over deploy.yaml: %v\n%s", err, out)
	}
	t.Setenv("MOOCTL_DIR", "")
	os.Unsetenv("MOOCTL_DIR")

	// The first cluster made is the default.
	if out, err := status(home); err != nil || !strings.Contains(out, "Cluster prod") {
		t.Errorf("the default set by init: %v\n%s", err, out)
	}
	if out, err := mooctl(t, home, home, "cluster", "use", "acme/staging"); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	if out, err := mooctl(t, home, home, "cluster", "use"); err != nil || strings.TrimSpace(out) != "acme/staging" {
		t.Errorf("cluster use prints %q, %v", out, err)
	}
	if out, err := status(home); err != nil || !strings.Contains(out, "Cluster acme/staging") {
		t.Errorf("the default: %v\n%s", err, out)
	}
	os.WriteFile(filepath.Join(app, "deploy.yaml"), []byte("cluster: prod\nname: web\ncontainers: {web: {build: {dockerfile: .}, port: 80}}\n"), 0o644)
	if out, err := status(app); err != nil || !strings.Contains(out, "Cluster prod") {
		t.Errorf("deploy.yaml over the default: %v\n%s", err, out)
	}
	if out, err := status(home, "-c", "prod"); err != nil || !strings.Contains(out, "Cluster prod") {
		t.Errorf("-c over the default: %v\n%s", err, out)
	}
	if out, err := mooctl(t, home, home, "cluster", "use", "nope"); err == nil {
		t.Errorf("an unknown cluster became the default:\n%s", out)
	}
	if out, err := status(home, "-c", "nope"); err == nil || !strings.Contains(err.Error(), "no cluster directory") {
		t.Errorf("an unknown cluster: %v\n%s", err, out)
	}

	os.Remove(filepath.Join(home, "default-cluster"))
	if out, err := status(home); err == nil || !strings.Contains(err.Error(), "which cluster?") {
		t.Errorf("no cluster named and no default: %v\n%s", err, out)
	}
	if out, err := mooctl(t, home, home, "cluster", "use"); err == nil {
		t.Errorf("cluster use with no default:\n%s", out)
	}
}

func TestInitKeepsAnExistingDefault(t *testing.T) {
	home := newHome(t)
	key := sshKey(t, home)
	for _, ref := range []string{"first", "second"} {
		if out, err := mooctl(t, home, home, "--ssh-key", key, "cluster", "init", ref); err != nil {
			t.Fatalf("%v\n%s", err, out)
		}
	}
	if out, _ := mooctl(t, home, home, "cluster", "use"); strings.TrimSpace(out) != "first" {
		t.Errorf("default = %q, want first", out)
	}
}

func TestRemoteAndClone(t *testing.T) {
	home := newHome(t)
	key := sshKey(t, home)
	if out, err := mooctl(t, home, home, "--ssh-key", key, "cluster", "init", "acme/prod"); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	remote := filepath.Join(t.TempDir(), "prod.git")
	git(t, home, "init", "--bare", "--quiet", "--initial-branch=main", remote)
	if out, err := mooctl(t, home, home, "-c", "acme/prod", "cluster", "remote", remote); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}

	other := newHome(t)
	if out, err := mooctl(t, other, other, "cluster", "clone", "acme/prod", remote); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	if _, err := clusterdir.Open(filepath.Join(other, "clusters", "acme", "prod")); err != nil {
		t.Error(err)
	}
	if out, err := mooctl(t, other, other, "cluster", "clone", "acme/other", remote); err == nil || !strings.Contains(err.Error(), "not of acme/other") {
		t.Errorf("a clone under another ref: %v\n%s", err, out)
	}
	if _, err := os.Stat(filepath.Join(other, "clusters", "acme", "other")); !os.IsNotExist(err) {
		t.Error("a refused clone left its directory")
	}
}

func TestAnEmptyClusterDirectoryIsRefused(t *testing.T) {
	home := newHome(t)
	key := sshKey(t, home)
	if out, err := mooctl(t, home, home, "--ssh-key", key, "cluster", "init", "prod"); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	if _, err := mooctl(t, home, home, "-C", "", "cluster", "status"); err == nil || !strings.Contains(err.Error(), "-C is empty") {
		t.Errorf("-C '': %v", err)
	}
	if _, err := mooctl(t, home, home, "-c", "", "cluster", "status"); err == nil {
		t.Error("-c '' was taken")
	}
	t.Setenv("MOOCTL_DIR", "")
	if _, err := mooctl(t, home, home, "cluster", "status"); err == nil || !strings.Contains(err.Error(), "MOOCTL_DIR is set to nothing") {
		t.Errorf("MOOCTL_DIR='': %v", err)
	}
}

func TestInitIntoADirectoryNamedDirectlySetsNoDefault(t *testing.T) {
	home := newHome(t)
	key := sshKey(t, home)
	dir := filepath.Join(t.TempDir(), "prod")
	if out, err := mooctl(t, home, home, "--ssh-key", key, "-C", dir, "cluster", "init", "prod"); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	if _, err := os.Stat(filepath.Join(home, "default-cluster")); err == nil {
		t.Error("init into a directory outside the root set the default")
	}
	ignore, _ := os.ReadFile(filepath.Join(dir, ".gitignore"))
	for _, want := range []string{"*.tmp\n", ".secrets.enc.yml.*\n"} {
		if !strings.Contains(string(ignore), want) {
			t.Errorf(".gitignore lacks %q:\n%s", want, ignore)
		}
	}
}

func TestInitNamesADirectoryACutOffInitLeft(t *testing.T) {
	home := newHome(t)
	key := sshKey(t, home)
	os.MkdirAll(filepath.Join(home, "clusters", "default", "prod"), 0o755)
	_, err := mooctl(t, home, home, "--ssh-key", key, "cluster", "init", "prod")
	if err == nil || !strings.Contains(err.Error(), "if a cluster init was cut off, remove it") {
		t.Errorf("init = %v", err)
	}
	if out, err := mooctl(t, home, home, "--ssh-key", key, "cluster", "init", "other"); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	_, err = mooctl(t, home, home, "--ssh-key", key, "cluster", "init", "other")
	if err == nil || strings.Contains(err.Error(), "cut off") {
		t.Errorf("a second init of a whole cluster: %v", err)
	}
}

func TestCloneOntoAnExistingDirectoryLeavesIt(t *testing.T) {
	home := newHome(t)
	dir := filepath.Join(home, "clusters", "default", "prod")
	os.MkdirAll(dir, 0o755)
	os.WriteFile(filepath.Join(dir, "keep"), []byte("x"), 0o644)
	if _, err := mooctl(t, home, home, "cluster", "clone", "prod", filepath.Join(t.TempDir(), "nowhere.git")); err == nil {
		t.Error("clone onto an existing directory passed")
	}
	if _, err := os.Stat(filepath.Join(dir, "keep")); err != nil {
		t.Error("clone removed what was there")
	}
}
