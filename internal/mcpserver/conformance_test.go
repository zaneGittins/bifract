package mcpserver

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"bifract/pkg/alerts"
)

// The API description the server generates, drift-gated by TestOpenAPISpecMatches.
const specPath = "../../openapi.json"

// clientMethods are the calls that reach the API. Their first path argument is
// what these tests recover.
var clientMethods = map[string]bool{
	"Get": true, "Post": true, "Put": true, "Delete": true, "Do": true, "Static": true,
}

// TestEveryEndpointAToolCallsExists fails when a route is renamed or removed,
// rather than leaving it to a user's first tool call. Paths come from the syntax
// tree, so a stale list cannot satisfy it. Only this direction is checked: the
// tool surface is curated, so unused routes are expected.
func TestEveryEndpointAToolCallsExists(t *testing.T) {
	served := servedPaths(t)
	for _, call := range calledPaths(t) {
		if !served[call.path] {
			t.Errorf("%s calls %s, which this build does not serve", call.where, call.path)
		}
	}
}

type apiCall struct {
	path  string
	where string
}

// calledPaths walks the package for Client calls and rebuilds the path each one
// requests. A segment that is not a literal (an id, a name) becomes {}.
func calledPaths(t *testing.T) []apiCall {
	t.Helper()
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(f os.FileInfo) bool {
		return !strings.HasSuffix(f.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatalf("parsing the package: %v", err)
	}

	var calls []apiCall
	for _, pkg := range pkgs {
		for name, file := range pkg.Files {
			ast.Inspect(file, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || !clientMethods[selector.Sel.Name] || len(call.Args) < 2 {
					return true
				}
				// Do takes (ctx, method, path, ...); the rest take (ctx, path, ...).
				index := 1
				if selector.Sel.Name == "Do" {
					index = 2
				}
				if index >= len(call.Args) {
					return true
				}
				path, ok := pathOf(call.Args[index])
				if !ok {
					return true
				}
				calls = append(calls, apiCall{
					path:  path,
					where: filepath.Base(name) + ":" + strconv.Itoa(fset.Position(call.Pos()).Line),
				})
				return true
			})
		}
	}
	if len(calls) == 0 {
		t.Fatal("recovered no API calls; the pattern this test relies on changed")
	}
	return calls
}

// pathOf flattens a string literal or a chain of + concatenations into a path
// pattern. It reports false when the expression does not start with a literal,
// which would mean the path is not knowable from the syntax alone.
func pathOf(expr ast.Expr) (string, bool) {
	var parts []string
	var walk func(ast.Expr)
	walk = func(e ast.Expr) {
		switch node := e.(type) {
		case *ast.BasicLit:
			if node.Kind == token.STRING {
				if value, err := strconv.Unquote(node.Value); err == nil {
					parts = append(parts, value)
					return
				}
			}
			parts = append(parts, "{}")
		case *ast.BinaryExpr:
			if node.Op == token.ADD {
				walk(node.X)
				walk(node.Y)
				return
			}
			parts = append(parts, "{}")
		default:
			parts = append(parts, "{}")
		}
	}
	walk(expr)

	joined := strings.Join(parts, "")
	if !strings.HasPrefix(joined, "/") {
		return "", false
	}
	return joined, true
}

// servedPaths is every route in the description, with its parameter names
// flattened so a path compares by shape rather than by what the segment is called.
func servedPaths(t *testing.T) map[string]bool {
	t.Helper()
	raw, err := os.ReadFile(specPath)
	if err != nil {
		t.Fatalf("reading %s: %v. Regenerate with: go test ./cmd/bifract-server -run TestOpenAPI -update", specPath, err)
	}
	var spec struct {
		Paths map[string]json.RawMessage `json:"paths"`
	}
	if err := json.Unmarshal(raw, &spec); err != nil {
		t.Fatalf("parsing %s: %v", specPath, err)
	}

	served := make(map[string]bool, len(spec.Paths))
	for path := range spec.Paths {
		trimmed, found := strings.CutPrefix(path, "/api/v1")
		if !found {
			continue
		}
		served[flattenParams(trimmed)] = true
	}
	if len(served) == 0 {
		t.Fatal("the description served no /api/v1 routes")
	}
	return served
}

// flattenParams turns /alerts/{id} into /alerts/{}.
func flattenParams(path string) string {
	segments := strings.Split(path, "/")
	for i, segment := range segments {
		if strings.HasPrefix(segment, "{") && strings.HasSuffix(segment, "}") {
			segments[i] = "{}"
		}
	}
	return strings.Join(segments, "/")
}

// TestEnumArgumentsCarryTheValuesTheAPIAccepts proves the linkage rather than the
// list: the schema is built from the server's own type, so a value added in Go
// appears here without anyone editing the tool.
func TestEnumArgumentsCarryTheValuesTheAPIAccepts(t *testing.T) {
	want := map[string]map[string][]string{
		"create_alert": {
			"alert_type": alerts.AlertType("").EnumValues(),
			"severity":   alerts.Severity("").EnumValues(),
		},
		"update_alert": {
			"alert_type": alerts.AlertType("").EnumValues(),
			"severity":   alerts.Severity("").EnumValues(),
		},
	}

	found := map[string]bool{}
	for _, tool := range tools(t) {
		expected, ok := want[tool.Name]
		if !ok {
			continue
		}
		found[tool.Name] = true

		schema, _ := json.Marshal(tool.InputSchema)
		var decoded struct {
			Properties map[string]struct {
				Enum []string `json:"enum"`
			} `json:"properties"`
		}
		if err := json.Unmarshal(schema, &decoded); err != nil {
			t.Fatalf("%s: %v", tool.Name, err)
		}
		for property, values := range expected {
			got := decoded.Properties[property].Enum
			if !equalStrings(got, values) {
				t.Errorf("%s argument %q offers %v, but the API accepts %v", tool.Name, property, got, values)
			}
		}
	}
	for name := range want {
		if !found[name] {
			t.Errorf("%s is not registered, so its enums cannot be checked", name)
		}
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	x, y := append([]string(nil), a...), append([]string(nil), b...)
	sort.Strings(x)
	sort.Strings(y)
	for i := range x {
		if x[i] != y[i] {
			return false
		}
	}
	return true
}

// docsPath is the page a user reads to decide what this server can do.
const docsPath = "../../docs/features/mcp-server.md"

// TestTheDocumentedToolListMatchesTheServer keeps the page honest. A tool added
// without documenting it, or documented after being removed, fails here.
func TestTheDocumentedToolListMatchesTheServer(t *testing.T) {
	page, err := os.ReadFile(docsPath)
	if err != nil {
		t.Fatalf("reading %s: %v", docsPath, err)
	}
	// Scoped to the tool tables: the environment-variable table upstream uses the
	// same row shape and would otherwise read as a tool named BIFRACT_URL.
	section := string(page)
	start := strings.Index(section, "## Available Tools")
	if start < 0 {
		t.Fatalf("%s has no '## Available Tools' section", docsPath)
	}
	section = section[start:]
	if end := strings.Index(section[len("## Available Tools"):], "\n## "); end >= 0 {
		section = section[:len("## Available Tools")+end]
	}

	documented := map[string]bool{}
	for _, match := range toolRow.FindAllStringSubmatch(section, -1) {
		documented[match[1]] = true
	}

	registered := map[string]bool{}
	for _, tool := range tools(t) {
		registered[tool.Name] = true
		if !documented[tool.Name] {
			t.Errorf("%s is registered but not in %s", tool.Name, docsPath)
		}
	}
	for name := range documented {
		if !registered[name] {
			t.Errorf("%s is documented in %s but not registered", name, docsPath)
		}
	}
}

// toolRow matches a leading `| `name` |` cell in the tool tables.
var toolRow = regexp.MustCompile("(?m)^\\|\\s*`([a-z_]+)`\\s*\\|")
