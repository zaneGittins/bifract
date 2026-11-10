package sqlquote

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// Values reach SQL only through the constructors that quote them: String for a
// string literal and Ident for an identifier (or a package's own wrapper of one).
// Writing a quote by hand around a value is how an escape gets forgotten, so no
// code in the repository may: no %s or %v inside quotes or backticks in a format
// string, and no concatenation or WriteString of a literal that opens a quote it
// does not close. Error and log messages are text, not SQL.
func TestValuesEnterSQLOnlyThroughConstructors(t *testing.T) {
	var files []string
	for _, root := range []string{"../../pkg", "../../cmd"} {
		err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
			if err == nil && d.IsDir() && notSQL[filepath.ToSlash(path)] {
				return filepath.SkipDir
			}
			if err == nil && !d.IsDir() && strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, "_test.go") {
				files = append(files, path)
			}
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	fset := token.NewFileSet()
	for _, path := range files {
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		f, err := parser.ParseFile(fset, path, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range f.Decls {
			if declName(d) != "" && sqlConstructors[declName(d)] {
				continue
			}
			ast.Inspect(d, func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.CallExpr:
					if isMessageCall(n) {
						return false // an error or log message, not SQL
					}
					if name := callName(n); strings.HasSuffix(name, "printf") || name == "WriteString" {
						for _, a := range n.Args {
							if s, ok := constString(a); ok {
								if name == "WriteString" && unbalanced(s) {
									t.Errorf("%s: WriteString opens a quote by hand: %q", fset.Position(a.Pos()), s)
								} else if verbInQuotes(s) {
									t.Errorf("%s: a value is formatted inside quotes: %q", fset.Position(a.Pos()), s)
								}
							}
						}
					}
				case *ast.BinaryExpr:
					if n.Op == token.ADD {
						for _, side := range []ast.Expr{n.X, n.Y} {
							if s, ok := stringLit(side); ok && unbalanced(s) {
								t.Errorf("%s: a value is concatenated inside quotes: %q", fset.Position(side.Pos()), s)
							}
						}
					}
				}
				return true
			})
		}
	}
}

// notSQL are the packages whose strings are prose, Markdown or BQL, never SQL.
var notSQL = map[string]bool{
	"../../pkg/aitools": true, "../../pkg/chat": true, "../../pkg/notebooks": true, "../../pkg/sigma": true,
}

// sqlConstructors are the functions that write quotes, given a value they escape.
var sqlConstructors = map[string]bool{
	"String": true, "Ident": true, "PGString": true,
	"sqlString": true, "backtickIdent": true, "quoteIdent": true,
	"sanitizeIdentifier": true, "jsonFieldRef": true,
	// Error messages naming a token or value as the user wrote it.
	"describeToken": true, "lexError": true, "declaredBindings": true, "tokenNames": true,
}

// declName is the function or the single variable a declaration declares.
func declName(d ast.Decl) string {
	switch d := d.(type) {
	case *ast.FuncDecl:
		return d.Name.Name
	case *ast.GenDecl:
		if len(d.Specs) == 1 {
			if v, ok := d.Specs[0].(*ast.ValueSpec); ok && len(v.Names) == 1 {
				return v.Names[0].Name
			}
		}
	}
	return ""
}

func isMessageCall(c *ast.CallExpr) bool {
	// A notification's title and text are shown to people.
	if sel, ok := c.Fun.(*ast.SelectorExpr); ok {
		if recv, ok := sel.X.(*ast.SelectorExpr); ok && recv.Sel.Name == "notifWriter" {
			return true
		}
	}
	switch callName(c) {
	case "Errorf", "New", "Printf", "Println", "Print", "Fatalf", "notConditionError":
		return true
	}
	return false
}

func callName(c *ast.CallExpr) string {
	switch f := c.Fun.(type) {
	case *ast.Ident:
		return f.Name
	case *ast.SelectorExpr:
		return f.Sel.Name
	}
	return ""
}

// constString is a string constant written as one literal or a + chain of them,
// as a long format string is split across lines.
func constString(e ast.Expr) (string, bool) {
	if b, ok := e.(*ast.BinaryExpr); ok && b.Op == token.ADD {
		x, okx := constString(b.X)
		y, oky := constString(b.Y)
		return x + y, okx && oky
	}
	return stringLit(e)
}

func stringLit(e ast.Expr) (string, bool) {
	lit, ok := e.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return "", false
	}
	s, err := strconv.Unquote(lit.Value)
	return s, err == nil
}

// sqlQuotes are the characters that open a string literal or a quoted identifier.
const sqlQuotes = "'`"

// unbalanced reports whether s leaves a quote open, so a value concatenated beside it
// lands inside the quotes.
func unbalanced(s string) bool {
	for _, q := range sqlQuotes {
		if strings.Count(strings.ReplaceAll(s, `\`+string(q), ""), string(q))%2 == 1 {
			return true
		}
	}
	return false
}

// verbInQuotes reports whether a format string puts a %s or %v between quotes.
func verbInQuotes(s string) bool {
	var open rune
	for i := 0; i < len(s); i++ {
		c := rune(s[i])
		switch {
		case c == '\\':
			i++
		case strings.ContainsRune(sqlQuotes, c) && (open == 0 || open == c):
			if open == 0 {
				open = c
			} else {
				open = 0
			}
		case c == '%' && i+1 < len(s):
			if s[i+1] == '%' {
				i++
				continue
			}
			j := i + 1
			for j < len(s) && strings.ContainsRune("+-# 0123456789.[]", rune(s[j])) {
				j++
			}
			if open != 0 && j < len(s) && strings.ContainsRune("svq", rune(s[j])) {
				return true
			}
		}
	}
	return false
}
