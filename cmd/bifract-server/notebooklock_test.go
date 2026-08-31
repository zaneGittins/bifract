package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

// lockExemptHandlers are the notebook writes that do not touch an existing
// notebook's content, with the reason each is exempt.
var lockExemptHandlers = map[string]string{
	"HandleLockNotebook":         "the lock itself",
	"HandleUnlockNotebook":       "the lock itself",
	"HandleUpdatePresence":       "records who is looking, not a change",
	"HandleCreateNotebook":       "creates a new notebook",
	"HandleImportNotebook":       "creates a new notebook",
	"HandleEnsureActiveNotebook": "returns or creates a scratch notebook, and the query skips locked ones",
}

// TestNotebookWritesCheckTheLock requires every mutating notebook route to pass
// through requireEditable, which is where the lock is enforced. A route added
// later that resolves a notebook and writes to it would otherwise edit a locked
// one, and nothing else in the suite would notice.
func TestNotebookWritesCheckTheLock(t *testing.T) {
	bodies := handlerBodies(t, "../../pkg/notebooks")

	mux, registry := buildRouter(testDeps())
	seen := 0
	for _, route := range walkRouter(t, mux, registry) {
		if route.method == http.MethodGet || !strings.HasPrefix(route.path, "/api/v1/notebooks") {
			continue
		}
		described, ok := registry.Lookup(route.method, route.path)
		if !ok || described.Handler == nil {
			t.Errorf("%s %s is mounted but not described", route.method, route.path)
			continue
		}
		name := handlerName(described.Handler)
		if _, exempt := lockExemptHandlers[name]; exempt {
			continue
		}
		if _, ok := bodies[name]; !ok {
			t.Errorf("%s %s resolves to %s, which is not a function in pkg/notebooks",
				route.method, route.path, name)
			continue
		}
		seen++
		if !reaches(bodies, name, "requireEditable") {
			t.Errorf("%s %s (%s) does not call requireEditable, so it would write to a locked notebook.\n"+
				"Gate it with requireEditable, or add it to lockExemptHandlers with a reason.",
				route.method, route.path, name)
		}
	}
	if seen == 0 {
		t.Fatal("no mutating notebook routes were checked; the walk found nothing")
	}
}

// TestFilingEvidenceChecksTheLock covers the capture path, which lives outside
// the notebook routes: a star posts a comment naming the notebook to file into.
func TestFilingEvidenceChecksTheLock(t *testing.T) {
	bodies := handlerBodies(t, "../../pkg/comments")
	body, ok := bodies["resolveEvidenceNotebook"]
	if !ok {
		t.Fatal("pkg/comments no longer has resolveEvidenceNotebook; move this check to whatever replaced it")
	}
	if !callsIn(body, "IsLocked") {
		t.Error("resolveEvidenceNotebook does not check IsLocked, so a star would file into a locked notebook")
	}
}

// handlerBodies parses a package and returns each function and method body by
// name. Reflection gives the handler a route points at; the source says what it
// checks.
func handlerBodies(t *testing.T, dir string) map[string]*ast.BlockStmt {
	t.Helper()
	pkgs, err := parser.ParseDir(token.NewFileSet(), dir, nil, 0)
	if err != nil {
		t.Fatalf("parsing %s: %v", dir, err)
	}
	bodies := map[string]*ast.BlockStmt{}
	for _, pkg := range pkgs {
		for _, file := range pkg.Files {
			for _, decl := range file.Decls {
				if fn, ok := decl.(*ast.FuncDecl); ok && fn.Body != nil {
					bodies[fn.Name.Name] = fn.Body
				}
			}
		}
	}
	return bodies
}

// handlerName is the bare method name behind a route's handler value, with the
// package path and Go's method-value "-fm" suffix stripped.
func handlerName(h http.HandlerFunc) string {
	full := runtime.FuncForPC(reflect.ValueOf(h).Pointer()).Name()
	name := full[strings.LastIndex(full, ".")+1:]
	return strings.TrimSuffix(name, "-fm")
}

// reaches reports whether fn calls method, directly or through package-local
// helpers. Handlers legitimately delegate the gate to a resolver, so following
// calls is what keeps the check from forcing every guard inline.
func reaches(bodies map[string]*ast.BlockStmt, fn, method string) bool {
	seen := map[string]bool{}
	var walk func(string) bool
	walk = func(name string) bool {
		if seen[name] {
			return false
		}
		seen[name] = true
		body, ok := bodies[name]
		if !ok {
			return false
		}
		found := false
		var callees []string
		ast.Inspect(body, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if sel.Sel.Name == method {
				found = true
				return false
			}
			callees = append(callees, sel.Sel.Name)
			return true
		})
		if found {
			return true
		}
		for _, c := range callees {
			if walk(c) {
				return true
			}
		}
		return false
	}
	return walk(fn)
}

// callsIn reports whether a body mentions the named method.
func callsIn(body *ast.BlockStmt, method string) bool {
	found := false
	ast.Inspect(body, func(n ast.Node) bool {
		if sel, ok := n.(*ast.SelectorExpr); ok && sel.Sel.Name == method {
			found = true
		}
		return !found
	})
	return found
}
