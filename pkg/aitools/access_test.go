package aitools

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"

	"bifract/pkg/api"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func tools(t *testing.T) []*mcp.Tool {
	t.Helper()
	defs := make([]*mcp.Tool, 0, len(All()))
	for _, tool := range All() {
		defs = append(defs, tool.Def)
	}
	if len(defs) == 0 {
		t.Fatal("no tools registered")
	}
	return defs
}

// TestNoToolReachesPastItsCeiling is the gate the chat tiering rests on.
//
// A tool's ceiling caps every request it makes, and chat decides from the same
// declaration whether to run the tool outright or ask the user first. If a tool
// marked read-only reaches a route that requires more than a viewer, the router
// refuses it at runtime and the user sees a tool that simply never works; worse,
// a write mislabelled as a read would be offered as a question the user is
// never asked. Both are caught here, against the routes the server actually
// serves rather than a list kept alongside.
func TestNoToolReachesPastItsCeiling(t *testing.T) {
	required := routeAccess(t)
	reaches := reachableCalls(t)
	handlers := toolHandlers(t)

	for _, tool := range All() {
		body, ok := handlers[tool.Name()]
		if !ok {
			t.Errorf("%s: could not find the function it is registered with", tool.Name())
			continue
		}
		for _, call := range reaches[body] {
			if call.method == "" {
				t.Errorf("%s: the verb of the call at %s is not readable, so its access cannot be checked",
					tool.Name(), call.where)
				continue
			}
			access, known := required[call.method+" "+flattenParams(call.path)]
			if !known {
				// An unserved path is TestEveryEndpointAToolCallsExists's to
				// report. A served path reached by the wrong verb is this one's:
				// nothing else would notice, and it skips the check.
				if servedPaths(t)[flattenParams(call.path)] {
					t.Errorf("%s calls %s %s (%s), which this build does not serve",
						tool.Name(), call.method, call.path, call.where)
				}
				continue
			}
			if !tool.Ceiling().Permits(access) {
				t.Errorf("%s runs at the %s ceiling but calls %s %s (%s), which requires %s",
					tool.Name(), tool.Ceiling(), call.method, call.path, call.where, access)
			}
		}
	}
}

// TestEveryWriteAsksTheUserFirst pins the confirmation rule itself. Only the
// exemptions noConfirm names may write without being approved.
func TestEveryWriteAsksTheUserFirst(t *testing.T) {
	exempt := map[string]bool{"cancel_archive_search": true}
	for _, tool := range All() {
		switch {
		case tool.ReadOnly() && tool.NeedsConfirmation():
			t.Errorf("%s only reads but is gated behind a confirmation", tool.Name())
		case !tool.ReadOnly() && !tool.NeedsConfirmation() && !exempt[tool.Name()]:
			t.Errorf("%s writes but runs without asking the user", tool.Name())
		case exempt[tool.Name()] && tool.NeedsConfirmation():
			t.Errorf("%s is listed as exempt but still asks", tool.Name())
		}
	}
}

// routeAccess reads the access each operation requires out of the served API
// description, keyed by method and flattened path.
func routeAccess(t *testing.T) map[string]api.Access {
	t.Helper()
	raw, err := os.ReadFile(specPath)
	if err != nil {
		t.Fatalf("reading %s: %v. Regenerate with: go test ./cmd/bifract-server -run TestOpenAPI -update", specPath, err)
	}
	var spec struct {
		Paths map[string]map[string]struct {
			Access api.Access `json:"x-bifract-access"`
		} `json:"paths"`
	}
	if err := json.Unmarshal(raw, &spec); err != nil {
		t.Fatalf("parsing %s: %v", specPath, err)
	}

	out := map[string]api.Access{}
	for path, operations := range spec.Paths {
		trimmed, found := strings.CutPrefix(path, "/api/v1")
		if !found {
			continue
		}
		for method, operation := range operations {
			out[strings.ToUpper(method)+" "+flattenParams(trimmed)] = operation.Access
		}
	}
	if len(out) == 0 {
		t.Fatal("the description carries no access levels")
	}
	return out
}

// toolHandlers maps each tool's name to the function it is registered with, by
// reading the add() calls rather than a list kept in step by hand.
func toolHandlers(t *testing.T) map[string]string {
	t.Helper()
	out := map[string]string{}
	forEachFunc(t, func(_ string, fn *ast.FuncDecl) {
		ast.Inspect(fn, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || len(call.Args) < 3 {
				return true
			}
			if name, ok := call.Fun.(*ast.Ident); !ok || name.Name != "add" {
				return true
			}
			literal, ok := call.Args[1].(*ast.UnaryExpr)
			if !ok {
				return true
			}
			composite, ok := literal.X.(*ast.CompositeLit)
			if !ok {
				return true
			}
			body, ok := call.Args[2].(*ast.Ident)
			if !ok {
				return true
			}
			for _, element := range composite.Elts {
				pair, ok := element.(*ast.KeyValueExpr)
				if !ok {
					continue
				}
				if key, ok := pair.Key.(*ast.Ident); !ok || key.Name != "Name" {
					continue
				}
				if value, ok := pair.Value.(*ast.BasicLit); ok {
					out[strings.Trim(value.Value, `"`)] = body.Name
				}
			}
			return true
		})
	})
	if len(out) == 0 {
		t.Fatal("recovered no tool registrations; the pattern this test relies on changed")
	}
	return out
}

// reachableCalls returns, for every function in the package, every API call it
// can make, following calls to other functions here. A tool body that reaches
// the API through a helper is covered as if it called it directly.
func reachableCalls(t *testing.T) map[string][]apiCall {
	t.Helper()
	direct := map[string][]apiCall{}
	for _, call := range calledPaths(t) {
		direct[call.fn] = append(direct[call.fn], call)
	}

	defined := map[string]bool{}
	callees := map[string][]string{}
	forEachFunc(t, func(_ string, fn *ast.FuncDecl) {
		defined[fn.Name.Name] = true
		ast.Inspect(fn, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok {
				if name, ok := call.Fun.(*ast.Ident); ok {
					callees[fn.Name.Name] = append(callees[fn.Name.Name], name.Name)
				}
			}
			return true
		})
	})

	out := map[string][]apiCall{}
	for name := range defined {
		seen := map[string]bool{}
		var walk func(string)
		walk = func(fn string) {
			if seen[fn] {
				return
			}
			seen[fn] = true
			out[name] = append(out[name], direct[fn]...)
			for _, callee := range callees[fn] {
				if defined[callee] {
					walk(callee)
				}
			}
		}
		walk(name)
	}
	return out
}

// forEachFunc visits every function declared in the package's non-test files.
func forEachFunc(t *testing.T, visit func(file string, fn *ast.FuncDecl)) {
	t.Helper()
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(f os.FileInfo) bool {
		return !strings.HasSuffix(f.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatalf("parsing the package: %v", err)
	}
	for _, pkg := range pkgs {
		for name, file := range pkg.Files {
			for _, decl := range file.Decls {
				if fn, ok := decl.(*ast.FuncDecl); ok {
					visit(name, fn)
				}
			}
		}
	}
}
