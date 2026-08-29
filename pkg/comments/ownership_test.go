package comments

import (
	"go/ast"
	"go/parser"
	"go/token"
	"slices"
	"testing"
)

// ownershipChecks are the storage calls that decide whether the caller may
// change a comment. They are matched by name rather than by argument position,
// which their signatures are free to change.
var ownershipChecks = []string{
	"UpdateComment",
	"DeleteComment",
	"BulkAddTagToComments",
	"BulkRemoveTagFromComments",
	"BulkDeleteComments",
}

// TestOwnershipIsCheckedAgainstTheIdentityThatWroteTheRow keeps the two halves of
// "author only" in agreement.
//
// A comment is written with auth.AttributionUsername, which for an API key is the
// person who created it. Checking ownership against the session principal instead
// compares that to the synthetic apikey_<id>, which never matches: a key could
// create a comment and then never edit or delete it.
func TestOwnershipIsCheckedAgainstTheIdentityThatWroteTheRow(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "handler.go", nil, 0)
	if err != nil {
		t.Fatalf("parsing handler.go: %v", err)
	}

	seen := map[string]bool{}
	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		name := selector.Sel.Name
		if !slices.Contains(ownershipChecks, name) {
			return true
		}
		seen[name] = true

		attributed := false
		for _, arg := range call.Args {
			switch render(arg) {
			case "auth.AttributionUsername(":
				attributed = true
			case "user.Username":
				t.Errorf("%s at %s matches ownership on the session principal, which for an API "+
					"key is apikey_<id> and never equals the author it wrote",
					name, fset.Position(call.Pos()))
			}
		}
		if !attributed {
			t.Errorf("%s at %s does not match ownership on auth.AttributionUsername",
				name, fset.Position(call.Pos()))
		}
		return true
	})

	for _, name := range ownershipChecks {
		if !seen[name] {
			t.Errorf("%s is no longer called from handler.go; update ownershipChecks", name)
		}
	}
}

// render prints the expression well enough to recognise the call, without
// pulling in a printer for what is only ever an identifier or a call.
func render(expr ast.Expr) string {
	switch node := expr.(type) {
	case *ast.Ident:
		return node.Name
	case *ast.SelectorExpr:
		return render(node.X) + "." + node.Sel.Name
	case *ast.CallExpr:
		return render(node.Fun) + "("
	}
	return "?"
}
