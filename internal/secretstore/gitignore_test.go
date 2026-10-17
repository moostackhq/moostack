package secretstore_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/moostackhq/moostack/internal/secretstore"
)

func TestIgnoreLockFile(t *testing.T) {
	tests := map[string]struct{ before, after string }{
		"no .gitignore":      {"", "/.secrets.enc.yml.lock\n"},
		"other lines":        {"*.tmp", "*.tmp\n/.secrets.enc.yml.lock\n"},
		"already there":      {"a\n/.secrets.enc.yml.lock\n", "a\n/.secrets.enc.yml.lock\n"},
		"there without root": {".secrets.enc.yml.lock\n", ".secrets.enc.yml.lock\n"},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, ".gitignore")
			if tt.before != "" {
				writeFile(t, path, []byte(tt.before))
			}

			changed, err := secretstore.IgnoreLockFile(dir)

			if err != nil {
				t.Fatal(err)
			}
			got, _ := os.ReadFile(path)
			if string(got) != tt.after || changed != (tt.before != tt.after) {
				t.Errorf("changed %v, .gitignore is %q; want %q", changed, got, tt.after)
			}
		})
	}
}
