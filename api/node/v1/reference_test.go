package nodev1_test

import (
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
)

func TestNormalizeReferenceNormalizesEachKindOfReference(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	for ref, want := range map[string]string{
		"nginx":                                   "docker.io/library/nginx:latest",
		"nginx:1.27":                              "docker.io/library/nginx:1.27",
		"library/nginx:1.27":                      "docker.io/library/nginx:1.27",
		"docker.io/nginx:1.27":                    "docker.io/library/nginx:1.27",
		"docker.io/library/nginx:1.27":            "docker.io/library/nginx:1.27",
		"bitnami/redis":                           "docker.io/bitnami/redis:latest",
		"ghcr.io/foo/bar:1":                       "ghcr.io/foo/bar:1",
		"ghcr.io/foo/bar":                         "ghcr.io/foo/bar:latest",
		"localhost:5000/app":                      "localhost:5000/app:latest",
		"[::1]:5000/app:1":                        "[::1]:5000/app:1",
		"registry.test:5000/team/app:v2":          "registry.test:5000/team/app:v2",
		"nginx@" + digest:                         "docker.io/library/nginx@" + digest,
		"nginx:1.27@" + digest:                    "docker.io/library/nginx@" + digest,
		"ghcr.io/foo/bar@" + digest:               "ghcr.io/foo/bar@" + digest,
		"docker.io/library/nginx:latest":          "docker.io/library/nginx:latest",
		"a" + strings.Repeat("/b", 10) + ":t":     "docker.io/a" + strings.Repeat("/b", 10) + ":t",
		"example.test/" + strings.Repeat("a", 50): "example.test/" + strings.Repeat("a", 50) + ":latest",
	} {
		got, err := nodev1.NormalizeReference(ref)
		if err != nil || got != want {
			t.Errorf("NormalizeReference(%q) = %q, %v, want %q", ref, got, err, want)
		}
	}
}

func TestNormalizeReferenceRefusesEachKindOfInvalidReference(t *testing.T) {
	for name, ref := range map[string]string{
		"empty":                   "",
		"capitals":                "NGINX",
		"a space":                 "nginx :1.27",
		"a line break":            "nginx\n:1.27",
		"an empty tag":            "nginx:",
		"a tag of 129":            "nginx:" + strings.Repeat("a", 129),
		"a short digest":          "nginx@sha256:abc",
		"an unknown algorithm":    "nginx@md5:" + strings.Repeat("a", 32),
		"a scheme":                "https://docker.io/library/nginx",
		"a trailing slash":        "nginx/",
		"a double slash":          "foo//bar",
		"an identifier alone":     strings.Repeat("a", 64),
		"over 1024 normalized":    "example.test/" + strings.Repeat("a/", 506) + "a",
		"a name over 255":         strings.Repeat("a", 256),
		"a leading dash":          "-nginx",
		"a tag starting with dot": "nginx:.1",
		"a registry not a host":   "[0]:0/0:0",
		"a bracketed name":        "[registry]/app:1",
	} {
		got, err := nodev1.NormalizeReference(ref)
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: NormalizeReference(%q) = %q, %v, want INVALID_ARGUMENT", name, ref, got, err)
		}
	}
}

func TestCheckReferenceTakesOnlyNormalizedReferences(t *testing.T) {
	for ref, want := range map[string]bool{
		"docker.io/library/nginx:1.27":    true,
		"ghcr.io/foo/bar:1":               true,
		"moostack.local/dns:1.0.0":        true,
		"example.test/app:1":              true,
		"nginx:1.27":                      false,
		"nginx":                           false,
		"docker.io/library/nginx":         false,
		"docker.io/nginx:1.27":            false,
		"index.docker.io/library/nginx:1": false,
		"":                                false,
	} {
		err := nodev1.CheckReference(ref)
		if (err == nil) != want {
			t.Errorf("CheckReference(%q) = %v, want it taken: %v", ref, err, want)
		}
		if err != nil && status.Code(err) != codes.InvalidArgument {
			t.Errorf("CheckReference(%q) = %v, want INVALID_ARGUMENT", ref, err)
		}
	}
}

// Normalizing is idempotent, and what it makes is exactly what the
// agent's check takes: a reference is taken if and only if normalizing
// it changes nothing.
func FuzzNormalizeReference(f *testing.F) {
	for _, seed := range []string{"nginx", "nginx:1.27", "docker.io/library/nginx:1.27", "ghcr.io/foo/bar@sha256:" + strings.Repeat("a", 64),
		"localhost:5000/app", "NGINX", "", "index.docker.io/library/nginx:1", "a/b/c:d", "nginx:1.27@sha256:" + strings.Repeat("b", 64)} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, ref string) {
		normalized, err := nodev1.NormalizeReference(ref)
		checkErr := nodev1.CheckReference(ref)
		if err != nil {
			if checkErr == nil {
				t.Fatalf("CheckReference took %q, which doesn't normalize: %v", ref, err)
			}
			return
		}
		if again, err := nodev1.NormalizeReference(normalized); err != nil || again != normalized {
			t.Fatalf("NormalizeReference(%q) = %q, but normalizing that gives %q, %v", ref, normalized, again, err)
		}
		if err := nodev1.CheckReference(normalized); err != nil {
			t.Fatalf("CheckReference refuses %q, which NormalizeReference made of %q: %v", normalized, ref, err)
		}
		if (checkErr == nil) != (normalized == ref) {
			t.Fatalf("CheckReference(%q) = %v, but it normalizes to %q", ref, checkErr, normalized)
		}
	})
}
