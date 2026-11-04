package hivemindadapter_test

import (
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// Only this package imports hivemind, apart from hivemind itself. Every Go
// file of the module is read, whatever its build constraints.
func TestOnlyTheAdapterImportsHivemind(t *testing.T) {
	const hivemind = "github.com/moostackhq/moostack/pkg/hivemind"
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	adapter := filepath.Join(root, "internal", "hivemindadapter")
	hivemindDir := filepath.Join(root, "pkg", "hivemind")
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			n := d.Name()
			if path != root && (n == "testdata" || path == filepath.Join(root, "target") || strings.HasPrefix(n, ".") || strings.HasPrefix(n, "_")) {
				return filepath.SkipDir
			}
			if path == hivemindDir {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || filepath.Dir(path) == adapter {
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
			if imp == hivemind || strings.HasPrefix(imp, hivemind+"/") {
				rel, _ := filepath.Rel(root, path)
				t.Errorf("%s imports %s", rel, imp)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
