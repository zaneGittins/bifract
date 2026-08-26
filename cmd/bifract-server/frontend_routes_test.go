package main

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// callRe finds an /api/v1 path in frontend source and captures the character
// that ended it. The path class stops at the first character a segment cannot
// contain, so a template literal like `/api/v1/fractals/${id}/alerts` yields
// "/api/v1/fractals/" and "$". That terminator is what separates a path the
// frontend wrote in full from one it builds at runtime.
var callRe = regexp.MustCompile(`["'` + "`" + `](/api/v1/[a-zA-Z0-9_/\-]*)(.?)`)

// TestFrontendAPIPathsHaveRoutes checks every /api/v1 path the frontend fetches
// against the routes the server actually mounts. A route deleted during a
// refactor while its caller stayed behind is invisible at compile time and at
// runtime shows up only as a silently broken widget (this test exists because
// the ClickHouse status dot polled a route that no longer existed and reported
// "disconnected" on every deployment for two releases).
//
// It reads the built router rather than server source text, so a path that
// appears in a comment or an unrelated string no longer satisfies it.
func TestFrontendAPIPathsHaveRoutes(t *testing.T) {
	mux, registry := buildRouter(testDeps())

	served := map[string]bool{}
	var servedPaths []string
	for _, route := range walkRouter(t, mux, registry) {
		if !served[route.path] {
			served[route.path] = true
			servedPaths = append(servedPaths, route.path)
		}
	}

	seen := map[string]bool{}
	for _, file := range frontendFiles(t) {
		src, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("reading %s: %v", file, err)
		}
		for _, m := range callRe.FindAllStringSubmatch(string(src), -1) {
			path, terminator := m[1], m[2]
			key := path + terminator
			if seen[key] {
				continue
			}
			seen[key] = true

			if pathIsServed(path, terminator, served, servedPaths) {
				continue
			}
			t.Errorf("%s calls %s but no server route matches it",
				filepath.Base(file), path)
		}
	}
}

// pathIsServed reports whether a frontend call can reach a mounted route.
// A path the frontend wrote in full must match one exactly; a partial path need
// only prefix a mounted route, since the rest is not visible in the source.
func pathIsServed(path, terminator string, served map[string]bool, servedPaths []string) bool {
	if isWholePath(path, terminator) {
		return served[path]
	}
	for _, mounted := range servedPaths {
		if strings.HasPrefix(mounted, path) {
			return true
		}
	}
	return false
}

// isWholePath reports whether the frontend wrote the entire path. A trailing
// slash always means a prefix: no route is addressed that way, but predicates
// like url.startsWith("/api/v1/") and interpolations like
// `/api/v1/fractals/${id}` both are. Otherwise the terminator decides, where a
// quote closes the literal and "?" or "#" begins a query or fragment. Anything
// else, notably "$" and "+", continues with a value known only at runtime.
func isWholePath(path, terminator string) bool {
	if strings.HasSuffix(path, "/") {
		return false
	}
	switch terminator {
	case `"`, "'", "`", "?", "#":
		return true
	}
	return false
}

// frontendFiles returns the web assets that can contain an API call.
func frontendFiles(t *testing.T) []string {
	t.Helper()

	var out []string
	root := filepath.Join("..", "..", "web")
	err := filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		if strings.HasSuffix(p, ".js") || strings.HasSuffix(p, ".html") {
			out = append(out, p)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s: %v", root, err)
	}
	if len(out) == 0 {
		t.Fatalf("no frontend files found under %s", root)
	}
	return out
}
