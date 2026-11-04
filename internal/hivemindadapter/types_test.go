package hivemindadapter_test

import (
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// No exported function, method, type, field, variable or constant of the
// adapter names a type from hivemind.
func TestTheAdapterExportsNoHivemindType(t *testing.T) {
	const hivemind = "github.com/moostackhq/moostack/pkg/hivemind"
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(token.NewFileSet(), name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		// The names this file imports hivemind's packages under.
		hm := map[string]bool{}
		for _, s := range f.Imports {
			p, err := strconv.Unquote(s.Path.Value)
			if err != nil {
				t.Fatal(err)
			}
			if p != hivemind && !strings.HasPrefix(p, hivemind+"/") {
				continue
			}
			n := p[strings.LastIndex(p, "/")+1:]
			if s.Name != nil {
				n = s.Name.Name
			}
			// A dot import's names carry no package to tell them by.
			if n == "." {
				t.Errorf("%s: imports %s with a dot", name, p)
			}
			hm[n] = true
		}
		check := func(what string, n ast.Node) {
			if n == nil {
				return
			}
			ast.Inspect(n, func(n ast.Node) bool {
				if sel, ok := n.(*ast.SelectorExpr); ok {
					if x, ok := sel.X.(*ast.Ident); ok && hm[x.Name] {
						t.Errorf("%s: %s names hivemind's %s.%s", name, what, x.Name, sel.Sel.Name)
					}
				}
				return true
			})
		}
		for _, d := range f.Decls {
			switch d := d.(type) {
			case *ast.FuncDecl:
				if d.Name.IsExported() && (d.Recv == nil || exportedRecv(d.Recv)) {
					check(d.Name.Name, d.Type)
				}
			case *ast.GenDecl:
				for _, s := range d.Specs {
					switch s := s.(type) {
					case *ast.TypeSpec:
						if s.Name.IsExported() {
							check(s.Name.Name, exportedPart(s.Type))
						}
					case *ast.ValueSpec:
						for _, n := range s.Names {
							if n.IsExported() {
								check(n.Name, s.Type)
								for _, v := range s.Values {
									check(n.Name, v)
								}
							}
						}
					}
				}
			}
		}
	}
}

// exportedRecv says whether a method's receiver type is exported.
func exportedRecv(r *ast.FieldList) bool {
	t := r.List[0].Type
	if s, ok := t.(*ast.StarExpr); ok {
		t = s.X
	}
	if g, ok := t.(*ast.IndexExpr); ok {
		t = g.X
	}
	id, ok := t.(*ast.Ident)
	return ok && id.IsExported()
}

// exportedPart is what a type exposes: a struct's exported fields, an
// interface's methods, or the whole of any other type.
func exportedPart(t ast.Expr) ast.Node {
	st, ok := t.(*ast.StructType)
	if !ok {
		return t
	}
	var exposed []ast.Expr
	for _, f := range st.Fields.List {
		if len(f.Names) == 0 {
			exposed = append(exposed, f.Type) // embedded: its fields are promoted
			continue
		}
		for _, n := range f.Names {
			if n.IsExported() {
				exposed = append(exposed, f.Type)
				break
			}
		}
	}
	return &ast.CompositeLit{Elts: exposed}
}

// Nothing reachable from what the adapter exports is a type of hivemind,
// through an alias, an unexported type or an inferred variable's type.
func TestNothingTheAdapterExportsReachesAHivemindType(t *testing.T) {
	const hivemind = "github.com/moostackhq/moostack/pkg/hivemind"
	pkg, err := importer.ForCompiler(token.NewFileSet(), "source", nil).Import("github.com/moostackhq/moostack/internal/hivemindadapter")
	if err != nil {
		t.Fatal(err)
	}
	seen := map[types.Type]bool{}
	var walk func(what string, ty types.Type)
	walk = func(what string, ty types.Type) {
		ty = types.Unalias(ty)
		if seen[ty] {
			return
		}
		seen[ty] = true
		switch ty := ty.(type) {
		case *types.Named:
			if p := ty.Obj().Pkg(); p != nil {
				if p.Path() == hivemind || strings.HasPrefix(p.Path(), hivemind+"/") {
					t.Errorf("%s reaches hivemind's %s", what, ty)
				}
				if p != pkg {
					return
				}
			}
			for i := range ty.NumMethods() {
				if m := ty.Method(i); m.Exported() {
					walk(what+"."+m.Name(), m.Type())
				}
			}
			walk(what, ty.Underlying())
		case *types.Pointer:
			walk(what, ty.Elem())
		case *types.Slice:
			walk(what, ty.Elem())
		case *types.Array:
			walk(what, ty.Elem())
		case *types.Chan:
			walk(what, ty.Elem())
		case *types.Map:
			walk(what, ty.Key())
			walk(what, ty.Elem())
		case *types.Struct:
			for i := range ty.NumFields() {
				if f := ty.Field(i); f.Exported() || f.Embedded() {
					walk(what+"."+f.Name(), f.Type())
				}
			}
		case *types.Interface:
			for i := range ty.NumMethods() {
				walk(what+"."+ty.Method(i).Name(), ty.Method(i).Type())
			}
		case *types.Signature:
			for _, tuple := range []*types.Tuple{ty.Params(), ty.Results()} {
				for i := range tuple.Len() {
					walk(what, tuple.At(i).Type())
				}
			}
		}
	}
	scope := pkg.Scope()
	for _, name := range scope.Names() {
		if o := scope.Lookup(name); o.Exported() {
			walk(name, o.Type())
		}
	}
}
