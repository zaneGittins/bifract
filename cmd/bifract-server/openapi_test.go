package main

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"bifract/pkg/api/openapi"
)

const specPath = "../../openapi.json"

// specVersion is fixed so the checked-in document does not churn with the build
// stamp; the served document carries the real version.
const specVersion = "dev"

// TestOpenAPISpecMatches pins the generated API description. It is the contract
// other people build against, so a change to it has to appear in a diff and be
// approved, not slip out with an unrelated commit.
func TestOpenAPISpecMatches(t *testing.T) {
	_, registry := buildRouter(testDeps())

	got, err := json.MarshalIndent(openapi.Generate(registry, specVersion), "", "  ")
	if err != nil {
		t.Fatalf("rendering the document: %v", err)
	}
	got = append(got, '\n')

	if *updateRoutes {
		if err := os.WriteFile(specPath, got, 0o644); err != nil {
			t.Fatalf("writing %s: %v", specPath, err)
		}
		t.Logf("wrote %d bytes to %s", len(got), specPath)
		return
	}

	want, err := os.ReadFile(specPath)
	if err != nil {
		t.Fatalf("reading %s (regenerate with: go test ./cmd/bifract-server -run TestOpenAPI -update): %v", specPath, err)
	}
	if string(got) != string(want) {
		t.Errorf("the API description changed. If intended, regenerate with:\n"+
			"  go test ./cmd/bifract-server -run TestOpenAPI -update\n"+
			"(generated %d bytes, checked-in %d)", len(got), len(want))
	}
}

// TestOpenAPICoversEveryRoute is the completeness check: the document must
// describe every route the router serves, minus the SPA catch-all which is not
// an API operation.
func TestOpenAPICoversEveryRoute(t *testing.T) {
	mux, registry := buildRouter(testDeps())
	doc := openapi.Generate(registry, specVersion)

	described := 0
	for _, item := range doc.Paths {
		for _, op := range []any{item.Get, item.Post, item.Put, item.Patch, item.Delete} {
			switch v := op.(type) {
			case *openapi.Operation:
				if v != nil {
					described++
				}
			}
		}
	}

	mounted := 0
	for _, route := range walkRouter(t, mux, registry) {
		if route.path != "/*" {
			mounted++
		}
	}
	if described != mounted {
		t.Errorf("document describes %d operations but the router mounts %d", described, mounted)
	}
}

// TestOpenAPIDocumentIsSound checks the things a generator gets wrong: a $ref
// pointing at a schema that was never emitted, an orphaned component, a
// duplicate operation id, or a path parameter the operation never declares.
func TestOpenAPIDocumentIsSound(t *testing.T) {
	_, registry := buildRouter(testDeps())
	doc := openapi.Generate(registry, specVersion)

	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	var tree any
	if err := json.Unmarshal(raw, &tree); err != nil {
		t.Fatal(err)
	}

	refs := map[string]bool{}
	var walk func(any)
	walk = func(n any) {
		switch v := n.(type) {
		case map[string]any:
			for k, child := range v {
				if k == "$ref" {
					if s, ok := child.(string); ok {
						refs[strings.TrimPrefix(s, "#/components/schemas/")] = true
					}
					continue
				}
				walk(child)
			}
		case []any:
			for _, child := range v {
				walk(child)
			}
		}
	}
	walk(tree)

	for name := range refs {
		if _, ok := doc.Components.Schemas[name]; !ok {
			t.Errorf("$ref points at %q, which is not in components", name)
		}
	}
	for name := range doc.Components.Schemas {
		if !refs[name] {
			t.Errorf("component %q is never referenced", name)
		}
	}

	seen := map[string]string{}
	for path, item := range doc.Paths {
		declared := map[string]bool{}
		for _, m := range pathParamNames(path) {
			declared[m] = true
		}
		for method, op := range map[string]*openapi.Operation{
			"GET": item.Get, "POST": item.Post, "PUT": item.Put,
			"PATCH": item.Patch, "DELETE": item.Delete,
		} {
			if op == nil {
				continue
			}
			if op.OperationID == "" {
				t.Errorf("%s %s has no operationId", method, path)
			}
			if prev, dup := seen[op.OperationID]; dup {
				t.Errorf("operationId %q is used by both %s and %s %s", op.OperationID, prev, method, path)
			}
			seen[op.OperationID] = method + " " + path

			got := map[string]bool{}
			for _, p := range op.Parameters {
				if p.In == "path" {
					got[p.Name] = true
				}
			}
			for name := range declared {
				if !got[name] {
					t.Errorf("%s %s does not declare path parameter %q", method, path, name)
				}
			}
		}
	}
}

func pathParamNames(path string) []string {
	var out []string
	for _, seg := range strings.Split(path, "/") {
		if strings.HasPrefix(seg, "{") && strings.HasSuffix(seg, "}") {
			out = append(out, strings.Trim(seg, "{}"))
		}
	}
	return out
}
