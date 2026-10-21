package buildid_test

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/moostackhq/moostack/internal/buildid"
)

func sum(data string) string {
	s := sha256.Sum256([]byte(data))
	return hex.EncodeToString(s[:])
}

func TestTheBuildOfFilesOnDiskIsTheBuildOfTheirSums(t *testing.T) {
	dir := t.TempDir()
	files := map[string]string{"containerd": "containerd 2.4.1", "containerd-shim-runc-v2": "shim 2.4.1"}
	paths := map[string]string{}
	sums := map[string]string{}
	for name, content := range files {
		paths[name] = filepath.Join(dir, "on-disk-"+name)
		if err := os.WriteFile(paths[name], []byte(content), 0o755); err != nil {
			t.Fatal(err)
		}
		sums[name] = sum(content)
	}
	var c buildid.Cache
	got, err := c.Of(paths)
	if err != nil {
		t.Fatal(err)
	}
	if want := buildid.FromSums(sums); got != want {
		t.Errorf("the files' build is %s, want %s, the build of their sums", got, want)
	}
}

func TestBuildsDifferWithAnyFileOrItsName(t *testing.T) {
	base := buildid.FromSums(map[string]string{"a": sum("1"), "b": sum("2")})
	for name, sums := range map[string]map[string]string{
		"another content": {"a": sum("1"), "b": sum("3")},
		"another name":    {"a": sum("1"), "c": sum("2")},
		"a file fewer":    {"a": sum("1")},
	} {
		if buildid.FromSums(sums) == base {
			t.Errorf("a build with %s is the same as the build it differs from", name)
		}
	}
	if again := buildid.FromSums(map[string]string{"b": sum("2"), "a": sum("1")}); again != base {
		t.Errorf("the same sums give %s and %s", base, again)
	}
}

func TestTheCacheSeesAFileReplaced(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent")
	if err := os.WriteFile(path, []byte("one build"), 0o755); err != nil {
		t.Fatal(err)
	}
	var c buildid.Cache
	first, err := c.Of(map[string]string{"agent": path})
	if err != nil {
		t.Fatal(err)
	}
	// Replaced as an update does, by a rename, at the same size and with
	// the same modification time, as a copy that keeps it would be.
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	other := path + ".new"
	if err := os.WriteFile(other, []byte("two build"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(other, time.Now(), info.ModTime()); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(other, path); err != nil {
		t.Fatal(err)
	}
	second, err := c.Of(map[string]string{"agent": path})
	if err != nil {
		t.Fatal(err)
	}
	if second == first || second != buildid.FromSums(map[string]string{"agent": sum("two build")}) {
		t.Errorf("after the file was replaced, the build is %s, want the new file's", second)
	}
}

// A status call peeks, so that it never waits for a large file to be
// hashed: the build shows once the files are hashed, in the background.
func TestPeekSaysTheBuildOnceTheFilesAreHashedWithoutWaiting(t *testing.T) {
	path := filepath.Join(t.TempDir(), "containerd")
	if err := os.WriteFile(path, []byte("one build"), 0o755); err != nil {
		t.Fatal(err)
	}
	paths := map[string]string{"containerd": path}
	want := buildid.FromSums(map[string]string{"containerd": sum("one build")})
	var c buildid.Cache
	if got := c.Peek(paths); got != "" && got != want {
		t.Fatalf("the first Peek says %q, want nothing or %s", got, want)
	}
	deadline := time.Now().Add(5 * time.Second)
	for c.Peek(paths) != want {
		if time.Now().After(deadline) {
			t.Fatalf("Peek still says %q, want %s once the file is hashed", c.Peek(paths), want)
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Another build in its place shows once hashed too, never the old.
	if err := os.WriteFile(path+".new", []byte("two builds"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path+".new", path); err != nil {
		t.Fatal(err)
	}
	next := buildid.FromSums(map[string]string{"containerd": sum("two builds")})
	for {
		got := c.Peek(paths)
		if got == want {
			t.Fatalf("Peek says the old build after the file was replaced")
		}
		if got == next {
			break
		}
		if time.Now().After(deadline.Add(5 * time.Second)) {
			t.Fatalf("Peek still says %q, want %s", got, next)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestPeekDoesntWaitForALargeFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "containerd")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	// Sparse, so it costs no disk, but hashing it takes a while.
	if err := f.Truncate(1 << 30); err != nil {
		t.Fatal(err)
	}
	f.Close()
	var c buildid.Cache
	start := time.Now()
	if got := c.Peek(map[string]string{"containerd": path}); got != "" {
		t.Errorf("the first Peek of a file not yet hashed says %q, want nothing", got)
	}
	if took := time.Since(start); took > 100*time.Millisecond {
		t.Errorf("Peek took %s, want it not to wait for the file to be hashed", took)
	}
}
