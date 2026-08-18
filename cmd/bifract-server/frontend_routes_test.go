package main

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// Every /api/v1/... path the frontend fetches must correspond to a route
// literal registered somewhere in the server. A route deleted during a refactor
// while its caller stayed behind is invisible at compile time and at runtime
// shows up only as a silently broken widget (this test exists because the
// ClickHouse status dot polled a route that no longer existed and reported
// "disconnected" on every deployment for two releases).
func TestFrontendAPIPathsHaveRoutes(t *testing.T) {
	root := filepath.Join("..", "..")

	routes := readAll(t, root, []string{"cmd", "pkg"}, func(p string) bool {
		return strings.HasSuffix(p, ".go") && !strings.HasSuffix(p, "_test.go")
	})
	frontend := readAll(t, root, []string{"web"}, func(p string) bool {
		return strings.HasSuffix(p, ".js") || strings.HasSuffix(p, ".html")
	})

	// Stops at the first character a path segment cannot contain, so a template
	// literal like `/api/v1/fractals/${id}/alerts` yields the "/fractals/" prefix.
	callRe := regexp.MustCompile(`["'` + "`" + `](/api/v1/[a-zA-Z0-9_/\-]*)`)

	seen := map[string]bool{}
	for _, m := range callRe.FindAllStringSubmatch(frontend, -1) {
		path := strings.TrimPrefix(m[1], "/api/v1")
		segs := splitSegments(path)
		if len(segs) == 0 || seen[path] {
			continue
		}
		seen[path] = true

		// Routes are registered relative to their chi.Route prefix, so try every
		// suffix of the called path. A trailing "/" or "{param}" may follow.
		matched := false
		for i := range segs {
			suffix := `"/` + strings.Join(segs[i:], "/")
			if strings.Contains(routes, suffix+`"`) || strings.Contains(routes, suffix+"/") {
				matched = true
				break
			}
		}
		if !matched {
			t.Errorf("frontend calls /api/v1%s but no server route registers it", path)
		}
	}
}

func splitSegments(path string) []string {
	var out []string
	for _, s := range strings.Split(path, "/") {
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}

func readAll(t *testing.T, root string, dirs []string, keep func(string) bool) string {
	t.Helper()
	var b strings.Builder
	for _, dir := range dirs {
		err := filepath.Walk(filepath.Join(root, dir), func(p string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() || !keep(p) {
				return err
			}
			data, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			b.Write(data)
			b.WriteByte('\n')
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", dir, err)
		}
	}
	return b.String()
}
