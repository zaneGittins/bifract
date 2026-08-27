package main

import (
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"testing"
)

var (
	handlerDeclRe = regexp.MustCompile(`^func (?:\([^)]*\) )?((?:H|h)andle\w+)\(`)
	queryGetRe    = regexp.MustCompile(`Query\(\)\.Get\("(\w+)"\)|\bq\.Get\("(\w+)"\)`)
)

// handlerQueryReads maps "package.HandlerName" to the query-string keys that
// handler reads. Handlers pull them straight off the URL, so this is the only
// way to know what a route accepts without being told.
func handlerQueryReads(t *testing.T) map[string]map[string]bool {
	t.Helper()

	reads := map[string]map[string]bool{}
	root := filepath.Join("..", "..")
	for _, dir := range []string{"pkg", "cmd"} {
		err := filepath.Walk(filepath.Join(root, dir), func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return err
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			pkg := "main"
			if rel, _ := filepath.Rel(filepath.Join(root, "pkg"), path); rel != "" && !strings.HasPrefix(rel, "..") {
				pkg = strings.Split(rel, string(filepath.Separator))[0]
			}

			var fn string
			for _, line := range strings.Split(string(data), "\n") {
				if m := handlerDeclRe.FindStringSubmatch(line); m != nil {
					fn = pkg + "." + m[1]
				}
				if fn == "" {
					continue
				}
				for _, m := range queryGetRe.FindAllStringSubmatch(line, -1) {
					key := m[1]
					if key == "" {
						key = m[2]
					}
					if reads[fn] == nil {
						reads[fn] = map[string]bool{}
					}
					reads[fn][key] = true
				}
				// PageParams reads limit and offset on the handler's behalf.
				if strings.Contains(line, "api.PageParams(r") {
					if reads[fn] == nil {
						reads[fn] = map[string]bool{}
					}
					reads[fn]["limit"], reads[fn]["offset"] = true, true
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walking %s: %v", dir, err)
		}
	}
	return reads
}

// handlerKey turns a mounted handler back into "package.HandlerName".
func handlerKey(h any) string {
	name := runtime.FuncForPC(reflect.ValueOf(h).Pointer()).Name()
	name = strings.TrimSuffix(name, "-fm")
	if i := strings.LastIndex(name, "/"); i >= 0 {
		name = name[i+1:]
	}
	parts := strings.Split(name, ".")
	if len(parts) < 2 {
		return ""
	}
	pkg := parts[0]
	if pkg == "bifract-server" {
		pkg = "main"
	}
	return pkg + "." + parts[len(parts)-1]
}

// TestQueryParamsAreDeclared is the completeness gate for query parameters. A
// handler that reads one the route does not declare is invisible to the
// generated description and to the API explorer, which is exactly the drift the
// registry exists to prevent.
func TestQueryParamsAreDeclared(t *testing.T) {
	mux, registry := buildRouter(testDeps())
	reads := handlerQueryReads(t)

	for _, route := range walkRouter(t, mux, registry) {
		described, ok := registry.Lookup(route.method, route.path)
		if !ok || described.Handler == nil {
			continue
		}
		read := reads[handlerKey(described.Handler)]
		if len(read) == 0 {
			continue
		}

		declared := map[string]bool{}
		for _, q := range described.Query {
			declared[q.Name] = true
		}
		var missing []string
		for name := range read {
			if !declared[name] {
				missing = append(missing, name)
			}
		}
		if len(missing) > 0 {
			sort.Strings(missing)
			t.Errorf("%s reads query parameters it does not declare: %s\n"+
				"Add them to the route's Query field so the description and the explorer know they exist.",
				route.key(), strings.Join(missing, ", "))
		}
	}
}
