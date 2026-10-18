package cli_test

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestClusterInitPinsMooctlsRelease(t *testing.T) {
	o := newOperator(t, "op")
	o.version = "0.4.0"
	r := o.mustRun("cluster", "init", "--name", "prod")
	if got, want := readFile(t, filepath.Join(o.dir, "cluster.yml")), "name: prod\nrelease: 0.4.0\ngeneration: 1\n"; got != want {
		t.Errorf("cluster.yml is\n%s\nwant\n%s", got, want)
	}
	if want := "Created cluster prod on release 0.4.0 in " + o.dir + "\n"; r.stdout != want {
		t.Errorf("cluster init printed %q, want %q", r.stdout, want)
	}
}

func TestClusterInitNamesTheClusterAfterItsDirectory(t *testing.T) {
	o := newOperator(t, "op")
	o.dir = filepath.Join(t.TempDir(), "staging")
	o.mustRun("cluster", "init")
	if got := readFile(t, filepath.Join(o.dir, "cluster.yml")); !strings.HasPrefix(got, "name: staging\n") {
		t.Errorf("cluster.yml is\n%s\nwant the name staging", got)
	}
}

func TestClusterInitCreatesAMissingDirectory(t *testing.T) {
	o := newOperator(t, "op")
	o.dir = filepath.Join(t.TempDir(), "a", "prod")
	o.mustRun("cluster", "init")
	if _, err := os.Stat(filepath.Join(o.dir, "cluster.yml")); err != nil {
		t.Error(err)
	}
}

func TestClusterInitRefusesAnExistingClusterDirectory(t *testing.T) {
	o := newOperator(t, "op")
	o.mustRun("cluster", "init", "--name", "prod")
	before := readFile(t, filepath.Join(o.dir, "cluster.yml"))
	o.version = "9.9.9"
	r := o.run("cluster", "init", "--name", "other")
	if r.err == nil || !strings.Contains(r.err.Error(), "already a cluster directory") {
		t.Errorf("a second cluster init: %v, want an error saying it is already a cluster directory", r.err)
	}
	if got := readFile(t, filepath.Join(o.dir, "cluster.yml")); got != before {
		t.Errorf("a second cluster init changed cluster.yml to\n%s", got)
	}
}

func TestClusterInitRefusesAnInvalidName(t *testing.T) {
	o := newOperator(t, "op")
	r := o.run("cluster", "init", "--name", "Prod_1")
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
	r := o.run("cluster", "init", "--name", "prod")
	if r.err == nil || !strings.Contains(r.err.Error(), "release build") {
		t.Errorf("cluster init from a build without a release: %v, want an error naming the release build", r.err)
	}
	if _, err := os.Stat(filepath.Join(o.dir, "cluster.yml")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("cluster init without a release left cluster.yml: %v", err)
	}
}

func TestClusterInitTakesNoArguments(t *testing.T) {
	o := newOperator(t, "op")
	if r := o.run("cluster", "init", "prod"); r.err == nil {
		t.Error("cluster init accepted an argument")
	}
}

func TestClusterInitAsksForANameWhenTheDirectorysWontDo(t *testing.T) {
	o := newOperator(t, "op")
	o.dir = filepath.Join(t.TempDir(), "My Cluster")
	r := o.run("cluster", "init")
	if r.err == nil || !strings.Contains(r.err.Error(), "--name") {
		t.Errorf("cluster init in %q: %v, want an error that suggests --name", o.dir, r.err)
	}
}
