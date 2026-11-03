package hivemind_test

import (
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// Every Go file is read, whatever its build constraints.
func TestImportsNothingFromMoostack(t *testing.T) {
	const module = "github.com/moostackhq/moostack/"
	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if n := d.Name(); path != "." && (n == "testdata" || n == "target" || strings.HasPrefix(n, ".") || strings.HasPrefix(n, "_")) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		f, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, s := range f.Imports {
			imp, err := strconv.Unquote(s.Path.Value)
			if err != nil {
				return err
			}
			if strings.HasPrefix(imp, module) && imp != module+"pkg/hivemind" && !strings.HasPrefix(imp, module+"pkg/hivemind/") {
				t.Errorf("%s imports %s", path, imp)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
